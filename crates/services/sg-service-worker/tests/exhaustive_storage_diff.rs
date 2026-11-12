//! Differential harness (M9.17c) for the exhaustive-search object-storage
//! seam: `object.normalizeConfig`/`object.create`/`lazyStore` and the
//! search-job key naming.
//!
//! Replays `test_ws/p9-go/data/exhaustive_storage/corpus.json` and writes one
//! JSON line per case to `M917C_STORAGE_OUT`; the Go oracles
//! (`internal/object/zz_m917c_storage_oracle_test.go` and
//! `internal/search/exhaustive/service/zz_m917c_keys_oracle_test.go`) write the
//! same lines. `run_exhaustive_storage_diff.sh` compares them.
//!
//! No real S3/GCS endpoint is used: the lazy-store cases inject a fake
//! [`ObjectStorageBackend`], and the create cases only exercise the unknown
//! backend rejection (matching `object.create`, which fails before any client
//! construction).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sg_objectstorage::{GcsConfig, S3Config, StorageConfig};
use sg_service_worker::jobs::exhaustive_search_service::{
    get_log_key, get_prefix, new_object_storage, LazyObjectStorage, ObjectStorageBackend,
};

#[derive(Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    object_cases: Vec<ObjectCase>,
    #[serde(default)]
    service_cases: Vec<ServiceCase>,
}

#[derive(Deserialize)]
struct ObjectCase {
    name: String,
    op: String,
    #[serde(default)]
    config: ConfigWire,
    #[serde(default)]
    backend: String,
    #[serde(default)]
    init_error: bool,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Deserialize, Default)]
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

#[derive(Deserialize, Default)]
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

#[derive(Deserialize, Default)]
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

#[derive(Deserialize)]
struct Step {
    op: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    max_age_seconds: Option<i64>,
}

#[derive(Deserialize)]
struct ServiceCase {
    id: i64,
}

struct FakeBackend {
    init_error: bool,
    init_calls: AtomicUsize,
    calls: Mutex<Vec<String>>,
}

impl FakeBackend {
    fn new(init_error: bool) -> Self {
        Self {
            init_error,
            init_calls: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, op: &str) {
        self.calls.lock().expect("calls").push(op.to_owned());
    }
}

#[async_trait]
impl ObjectStorageBackend for FakeBackend {
    async fn init(&self) -> Result<(), String> {
        self.init_calls.fetch_add(1, Ordering::SeqCst);
        self.record("init");
        if self.init_error {
            Err("init boom".to_owned())
        } else {
            Ok(())
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.record("get");
        Ok(format!("value:{key}").into_bytes())
    }

    async fn upload(&self, _key: &str, data: Vec<u8>) -> Result<i64, String> {
        self.record("upload");
        Ok(data.len() as i64)
    }

    async fn delete(&self, _key: &str) -> Result<(), String> {
        self.record("delete");
        Ok(())
    }

    async fn list_objects(&self, _prefix: &str) -> Result<Vec<String>, String> {
        self.record("list");
        Ok(vec!["k1".to_owned(), "k2".to_owned()])
    }

    async fn expire_objects(&self, _prefix: &str, _max_age: Duration) -> Result<(), String> {
        self.record("expire");
        Ok(())
    }
}

async fn lazy_case(case: &ObjectCase) -> Value {
    let fake = Arc::new(FakeBackend::new(case.init_error));
    let store = LazyObjectStorage::new(Arc::clone(&fake));
    let mut results = Vec::new();
    for step in &case.steps {
        match step.op.as_str() {
            "get" => match store.get(&step.key).await {
                Ok(data) => results.push(json!({
                    "op": "get",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": true,
                    "error": Value::Null,
                    "data": String::from_utf8_lossy(&data),
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
                Err(err) => results.push(json!({
                    "op": "get",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": false,
                    "error": err,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
            },
            "upload" => match store.upload(&step.key, step.data.as_bytes().to_vec()).await {
                Ok(n) => results.push(json!({
                    "op": "upload",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": true,
                    "error": Value::Null,
                    "data": Value::Null,
                    "n": n,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
                Err(err) => results.push(json!({
                    "op": "upload",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": false,
                    "error": err,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
            },
            "delete" => match store.delete(&step.key).await {
                Ok(()) => results.push(json!({
                    "op": "delete",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": true,
                    "error": Value::Null,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
                Err(err) => results.push(json!({
                    "op": "delete",
                    "key": step.key,
                    "prefix": Value::Null,
                    "ok": false,
                    "error": err,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
            },
            "list" => match store.list_objects(&step.prefix).await {
                Ok(keys) => results.push(json!({
                    "op": "list",
                    "key": Value::Null,
                    "prefix": step.prefix,
                    "ok": true,
                    "error": Value::Null,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": keys,
                })),
                Err(err) => results.push(json!({
                    "op": "list",
                    "key": Value::Null,
                    "prefix": step.prefix,
                    "ok": false,
                    "error": err,
                    "data": Value::Null,
                    "n": Value::Null,
                    "max_age_seconds": Value::Null,
                    "keys": Value::Null,
                })),
            },
            "expire" => {
                let seconds = step.max_age_seconds.unwrap_or(0);
                match store
                    .expire_objects(&step.prefix, Duration::from_secs(seconds as u64))
                    .await
                {
                    Ok(()) => results.push(json!({
                        "op": "expire",
                        "key": Value::Null,
                        "prefix": step.prefix,
                        "ok": true,
                        "error": Value::Null,
                        "data": Value::Null,
                        "n": Value::Null,
                        "max_age_seconds": seconds,
                        "keys": Value::Null,
                    })),
                    Err(err) => results.push(json!({
                        "op": "expire",
                        "key": Value::Null,
                        "prefix": step.prefix,
                        "ok": false,
                        "error": err,
                        "data": Value::Null,
                        "n": Value::Null,
                        "max_age_seconds": seconds,
                        "keys": Value::Null,
                    })),
                }
            }
            other => panic!("case {}: unknown step op {other:?}", case.name),
        }
    }
    json!({
        "kind": "lazy",
        "init_calls": fake.init_calls.load(Ordering::SeqCst),
        "calls": *fake.calls.lock().expect("calls"),
        "steps": results,
    })
}

#[test]
fn exhaustive_storage_diff() {
    if std::env::var("SG_M917C_STORAGE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M917C_STORAGE_CORPUS").expect("M917C_STORAGE_CORPUS");
    let out_path = std::env::var("M917C_STORAGE_OUT").expect("M917C_STORAGE_OUT");
    let corpus: Corpus = serde_json::from_str(
        &std::fs::read_to_string(&corpus_path).expect("read corpus"),
    )
    .expect("parse corpus");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut lines = Vec::new();
    for case in &corpus.object_cases {
        let value = runtime.block_on(async {
            match case.op.as_str() {
                "normalize" => json!({
                    "kind": "normalize",
                    "config": config_json(&case.config.storage_config().normalize()),
                }),
                "create" => {
                    let error = new_object_storage(StorageConfig {
                        backend: case.backend.clone(),
                        ..StorageConfig::default()
                    })
                    .await
                    .err();
                    json!({"kind": "create", "error": error})
                }
                "lazy" => lazy_case(case).await,
                other => panic!("case {}: unknown op {other:?}", case.name),
            }
        });
        lines.push(serde_json::to_string(&value).expect("encode case"));
    }
    for case in &corpus.service_cases {
        lines.push(
            serde_json::to_string(&json!({
                "kind": "keys",
                "log": get_log_key(case.id),
                "prefix": get_prefix(case.id),
            }))
            .expect("encode keys"),
        );
    }

    let mut output = lines.join("\n");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");
}
