//! M9.20b differential harness: the exhaustive-search production wiring.
//!
//! Replays `test_ws/p9-go/data/exhaustive_production_wiring/corpus.json` and
//! writes one JSON line per case to `M920B_WIRING_OUT`; the Go oracles
//! (`internal/search/zz_m920b_wiring_config_oracle_test.go` and
//! `internal/object/zz_m920b_wiring_oracle_test.go`) write the same lines in
//! the same order (config cases, then select/store cases).
//!
//! Three sections:
//!
//! * `config_cases`: `SEARCH_JOBS_UPLOAD_*` env → `SearchJobsObjectStorageConfig`
//!   (`ObjectStorageConfig.Load` equivalent), including validation error text.
//! * `select_cases`: `StorageConfig.backend` → `object.create`'s store choice,
//!   without constructing a client.
//! * `store_cases`: lazy-init semantics and the upload/log adapters over an
//!   in-memory backend.
//!
//! No real S3/GCS endpoint is contacted and no database is used. Gated by
//! `SG_M920B_WIRING=1`; reads `M920B_WIRING_CORPUS` and writes
//! `M920B_WIRING_OUT`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sg_core::EnvReader;
use sg_objectstorage::{GcsConfig, S3Config, StorageConfig};
use sg_service_worker::jobs::exhaustive_search_janitor::{
    JobLogsBlobStore, SearchJobsObjectStorageConfig,
};
use sg_service_worker::jobs::exhaustive_search_job::assemble_object_storage;
use sg_service_worker::jobs::exhaustive_search_service::{
    select_object_storage_kind, LazyObjectStorage, ObjectStorageBackend, ObjectStorageConnector,
    ObjectStorageKind, ObjectStorageLogBlobStore, ObjectStorageUploadStore, UploadStore,
};
use sg_service_worker::jobs::JobConfig;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Corpus {
    #[serde(default)]
    config_cases: Vec<ConfigCase>,
    #[serde(default)]
    select_cases: Vec<SelectCase>,
    #[serde(default)]
    store_cases: Vec<StoreCase>,
}

#[derive(Debug, Deserialize)]
struct ConfigCase {
    name: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct SelectCase {
    name: String,
    config: ConfigWire,
}

#[derive(Debug, Deserialize)]
struct StoreCase {
    name: String,
    #[serde(default)]
    lazy: bool,
    #[serde(default)]
    init_fail_times: usize,
    #[serde(default)]
    via: String,
    #[serde(default)]
    seed: BTreeMap<String, String>,
    #[serde(default)]
    ops: Vec<Op>,
}

#[derive(Debug, Deserialize)]
struct Op {
    op: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    max_age_seconds: i64,
}

#[derive(Debug, Deserialize, Default)]
struct ConfigWire {
    #[serde(default)]
    backend: String,
    #[serde(default)]
    manage_bucket: bool,
    #[serde(default)]
    bucket: String,
    #[serde(default)]
    s3: S3Wire,
    #[serde(default)]
    gcs: GcsWire,
}

#[derive(Debug, Deserialize, Default)]
struct S3Wire {
    #[serde(default)]
    is_blobstore: bool,
    #[serde(default)]
    region: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    use_path_style: bool,
    #[serde(default)]
    access_key_id: String,
    #[serde(default)]
    secret_access_key: String,
    #[serde(default)]
    session_token: String,
}

#[derive(Debug, Deserialize, Default)]
struct GcsWire {
    #[serde(default)]
    project_id: String,
    #[serde(default)]
    credentials_file: String,
    #[serde(default)]
    credentials_file_contents: String,
}

impl ConfigWire {
    fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            backend: self.backend.clone(),
            manage_bucket: self.manage_bucket,
            bucket: self.bucket.clone(),
            s3: S3Config {
                is_blobstore: self.s3.is_blobstore,
                region: self.s3.region.clone(),
                endpoint: self.s3.endpoint.clone(),
                use_path_style: self.s3.use_path_style,
                access_key_id: self.s3.access_key_id.clone(),
                secret_access_key: self.s3.secret_access_key.clone(),
                session_token: self.s3.session_token.clone(),
            },
            gcs: GcsConfig {
                project_id: self.gcs.project_id.clone(),
                credentials_file: self.gcs.credentials_file.clone(),
                credentials_file_contents: self.gcs.credentials_file_contents.clone(),
            },
        }
    }
}

fn config_json(config: &StorageConfig) -> Value {
    json!({
        "backend": config.backend,
        "manage_bucket": config.manage_bucket,
        "bucket": config.bucket,
        "s3": {
            "is_blobstore": config.s3.is_blobstore,
            "region": config.s3.region,
            "endpoint": config.s3.endpoint,
            "use_path_style": config.s3.use_path_style,
            "access_key_id": config.s3.access_key_id,
            "secret_access_key": config.s3.secret_access_key,
            "session_token": config.s3.session_token,
        },
        "gcs": {
            "project_id": config.gcs.project_id,
            "credentials_file": config.gcs.credentials_file,
            "credentials_file_contents": config.gcs.credentials_file_contents,
        },
    })
}

// ---------------------------------------------------------------------------
// Fake backend
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeState {
    init_calls: usize,
    fail_init: usize,
    objects: BTreeMap<String, Vec<u8>>,
}

struct FakeBackend {
    state: Mutex<FakeState>,
}

impl FakeBackend {
    fn new(fail_init: usize, seed: &BTreeMap<String, String>) -> FakeBackend {
        let objects = seed
            .iter()
            .map(|(key, value)| (key.clone(), value.as_bytes().to_vec()))
            .collect();
        FakeBackend {
            state: Mutex::new(FakeState {
                init_calls: 0,
                fail_init,
                objects,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn init_calls(&self) -> usize {
        self.lock().init_calls
    }
}

#[async_trait]
impl ObjectStorageBackend for FakeBackend {
    async fn init(&self) -> Result<(), String> {
        let mut state = self.lock();
        state.init_calls += 1;
        if state.init_calls <= state.fail_init {
            return Err("bucket init failed".to_owned());
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.lock()
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| format!("object not found: {key}"))
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        let len = data.len() as i64;
        self.lock().objects.insert(key.to_owned(), data);
        Ok(len)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.lock().objects.remove(key);
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String> {
        Ok(self
            .lock()
            .objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn expire_objects(&self, prefix: &str, _max_age: Duration) -> Result<(), String> {
        self.lock()
            .objects
            .retain(|key, _| !key.starts_with(prefix));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Op output
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
struct OpOut {
    op: String,
    key: String,
    prefix: String,
    ok: bool,
    error: String,
    data: String,
    bytes: i64,
    keys: Vec<String>,
    max_age_seconds: i64,
}

impl OpOut {
    fn new(op: &str) -> OpOut {
        OpOut {
            op: op.to_owned(),
            ..OpOut::default()
        }
    }
}

async fn run_backend_ops(store: &dyn ObjectStorageBackend, ops: &[Op]) -> Vec<OpOut> {
    let mut out = Vec::new();
    for op in ops {
        let mut result = OpOut::new(&op.op);
        match op.op.as_str() {
            "init" => match store.init().await {
                Ok(()) => result.ok = true,
                Err(err) => result.error = err,
            },
            "get" => {
                result.key = op.key.clone();
                match store.get(&op.key).await {
                    Ok(data) => {
                        result.ok = true;
                        result.data = String::from_utf8_lossy(&data).into_owned();
                    }
                    Err(err) => result.error = err,
                }
            }
            "upload" => {
                result.key = op.key.clone();
                match store.upload(&op.key, op.data.as_bytes().to_vec()).await {
                    Ok(n) => {
                        result.ok = true;
                        result.bytes = n;
                    }
                    Err(err) => result.error = err,
                }
            }
            "delete" => {
                result.key = op.key.clone();
                match store.delete(&op.key).await {
                    Ok(()) => result.ok = true,
                    Err(err) => result.error = err,
                }
            }
            "list" => {
                result.prefix = op.prefix.clone();
                match store.list_objects(&op.prefix).await {
                    Ok(keys) => {
                        result.ok = true;
                        result.keys = keys;
                    }
                    Err(err) => result.error = err,
                }
            }
            "expire" => {
                result.prefix = op.prefix.clone();
                result.max_age_seconds = op.max_age_seconds;
                match store
                    .expire_objects(&op.prefix, Duration::from_secs(op.max_age_seconds as u64))
                    .await
                {
                    Ok(()) => result.ok = true,
                    Err(err) => result.error = err,
                }
            }
            other => result.error = format!("unknown op {other}"),
        }
        out.push(result);
    }
    out
}

async fn run_upload_ops(store: &dyn UploadStore, ops: &[Op]) -> Vec<OpOut> {
    let mut out = Vec::new();
    for op in ops {
        let mut result = OpOut::new(&op.op);
        match op.op.as_str() {
            "get" => {
                result.key = op.key.clone();
                match store.get(&op.key).await {
                    Ok(data) => {
                        result.ok = true;
                        result.data = String::from_utf8_lossy(&data).into_owned();
                    }
                    Err(err) => result.error = err,
                }
            }
            "upload" => {
                result.key = op.key.clone();
                match store.upload(&op.key, op.data.as_bytes()).await {
                    Ok(n) => {
                        result.ok = true;
                        result.bytes = n;
                    }
                    Err(err) => result.error = err,
                }
            }
            "delete" => {
                result.key = op.key.clone();
                match store.delete(&op.key).await {
                    Ok(()) => result.ok = true,
                    Err(err) => result.error = err,
                }
            }
            "list" => {
                result.prefix = op.prefix.clone();
                match store.list(&op.prefix).await {
                    Ok(list) => {
                        if let Some(err) = list.error {
                            result.error = err;
                        } else {
                            result.ok = true;
                            result.keys = list.keys;
                        }
                    }
                    Err(err) => result.error = err,
                }
            }
            other => result.error = format!("unknown op {other}"),
        }
        out.push(result);
    }
    out
}

async fn run_log_ops(store: &dyn JobLogsBlobStore, ops: &[Op]) -> Vec<OpOut> {
    let mut out = Vec::new();
    for op in ops {
        let mut result = OpOut::new(&op.op);
        match op.op.as_str() {
            "upload" => {
                result.key = op.key.clone();
                match store.upload(&op.key, op.data.as_bytes().to_vec()).await {
                    Ok(n) => {
                        result.ok = true;
                        result.bytes = n;
                    }
                    Err(err) => result.error = err,
                }
            }
            "delete" => {
                result.key = op.key.clone();
                match store.delete(&op.key).await {
                    Ok(()) => result.ok = true,
                    Err(err) => result.error = err,
                }
            }
            other => result.error = format!("unknown op {other}"),
        }
        out.push(result);
    }
    out
}

// ---------------------------------------------------------------------------
// Config env handling
// ---------------------------------------------------------------------------

static ENV_LOCK: Mutex<()> = Mutex::new(());

const STORAGE_ENV: &[&str] = &[
    "SEARCH_JOBS_UPLOAD_BACKEND",
    "SEARCH_JOBS_UPLOAD_MANAGE_BUCKET",
    "SEARCH_JOBS_UPLOAD_BUCKET",
    "SEARCH_JOBS_UPLOAD_AWS_REGION",
    "SEARCH_JOBS_UPLOAD_AWS_ENDPOINT",
    "SEARCH_JOBS_UPLOAD_AWS_USE_PATH_STYLE",
    "SEARCH_JOBS_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
    "SEARCH_JOBS_UPLOAD_AWS_ACCESS_KEY_ID",
    "SEARCH_JOBS_UPLOAD_AWS_SECRET_ACCESS_KEY",
    "SEARCH_JOBS_UPLOAD_AWS_SESSION_TOKEN",
    "SEARCH_JOBS_UPLOAD_GCP_PROJECT_ID",
    "SEARCH_JOBS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE",
    "SEARCH_JOBS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
    "DEPLOY_TYPE",
];

fn apply_env(env: &BTreeMap<String, String>) {
    for name in STORAGE_ENV {
        std::env::remove_var(name);
    }
    for (name, value) in env {
        std::env::set_var(name, value);
    }
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exhaustive_production_wiring_diff() {
    if std::env::var("SG_M920B_WIRING").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M920B_WIRING_CORPUS").expect("M920B_WIRING_CORPUS");
    let out_path = std::env::var("M920B_WIRING_OUT").expect("M920B_WIRING_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut lines = Vec::new();

    for case in &corpus.config_cases {
        let (storage, error) = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
            apply_env(&case.env);
            let config = SearchJobsObjectStorageConfig::default();
            config.load(&mut EnvReader::new());
            let error = match config.validate() {
                Ok(()) => String::new(),
                Err(err) => err.to_string(),
            };
            (config.storage(), error)
        };
        let storage_json = storage.as_ref().map(config_json).unwrap_or(Value::Null);
        lines.push(json!({
            "name": case.name,
            "kind": "config",
            "ok": error.is_empty(),
            "error": error,
            "storage": storage_json,
        }));
    }

    for case in &corpus.select_cases {
        let (ok, error, selected) = match select_object_storage_kind(&case.config.storage_config())
        {
            Ok(ObjectStorageKind::S3) => (true, String::new(), "s3".to_owned()),
            Ok(ObjectStorageKind::Gcs) => (true, String::new(), "gcs".to_owned()),
            Err(err) => (false, err, String::new()),
        };
        lines.push(json!({
            "name": case.name,
            "kind": "select",
            "ok": ok,
            "error": error,
            "selected": selected,
        }));
    }

    for case in &corpus.store_cases {
        let fake = Arc::new(FakeBackend::new(case.init_fail_times, &case.seed));
        let ops = if case.lazy {
            let lazy = LazyObjectStorage::new(Arc::clone(&fake));
            run_backend_ops(&lazy, &case.ops).await
        } else if case.via == "upload_store" {
            let store = ObjectStorageUploadStore::new(Arc::clone(&fake));
            run_upload_ops(&store, &case.ops).await
        } else if case.via == "log_store" {
            let store = ObjectStorageLogBlobStore::new(Arc::clone(&fake));
            run_log_ops(&store, &case.ops).await
        } else {
            run_backend_ops(fake.as_ref(), &case.ops).await
        };
        lines.push(json!({
            "name": case.name,
            "kind": "store",
            "init_calls": fake.init_calls(),
            "ops": ops,
        }));
    }

    let body: String = lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("serialize") + "\n")
        .collect();
    std::fs::write(out_path, body).expect("write output");
}

/// `search.NewObjectStorage` returns one shared `object.Storage` for the
/// revision writer and the janitor. This checks the Rust assembly hands both
/// adapters the same backend (no database/network involved).
#[tokio::test]
async fn assemble_object_storage_shares_one_backend() {
    struct FakeConnector(Arc<FakeBackend>);

    #[async_trait]
    impl ObjectStorageConnector for FakeConnector {
        async fn connect(
            &self,
            _config: StorageConfig,
        ) -> Result<Arc<dyn ObjectStorageBackend>, String> {
            Ok(self.0.clone())
        }
    }

    let config = {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        apply_env(&BTreeMap::new());
        let config = SearchJobsObjectStorageConfig::default();
        config.load(&mut EnvReader::new());
        config
    };
    let fake = Arc::new(FakeBackend::new(0, &BTreeMap::new()));
    let connector = FakeConnector(Arc::clone(&fake));
    let (uploads, blobs) = assemble_object_storage(&connector, &config)
        .await
        .expect("assemble storage");

    assert_eq!(
        blobs.upload("log-9.csv", b"repo,rev\n".to_vec()).await,
        Ok(9)
    );
    assert_eq!(
        uploads.get("log-9.csv").await.expect("shared get"),
        b"repo,rev\n"
    );
    assert_eq!(
        uploads.list("log-").await.expect("shared list").keys,
        vec!["log-9.csv".to_owned()]
    );
    blobs.delete("log-9.csv").await.expect("shared delete");
    assert!(uploads.get("log-9.csv").await.is_err());
}
