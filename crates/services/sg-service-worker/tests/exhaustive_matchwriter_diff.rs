#![forbid(unsafe_code)]

//! M9.8id differential harness (Rust side): the exhaustive-search object
//! storage adapter and the synchronous `MatchWriter` factory.
//!
//! Mirrors the untracked Go oracle
//! `internal/search/exhaustive/service/zz_m9id_oracle_test.go`. Both sides use
//! an in-memory `object.Storage` fake and compare:
//!
//! * `NewJSONWriter`/`MatchJSONWriter` chunk keys and exact bytes
//!   (`prefix`, `prefix-2`, ...), the default 100 MiB flush size and flush
//!   boundaries;
//! * `Upload`/`Get`/`Delete`/`List`/`ExpireObjects` semantics;
//! * the janitor's `log-<id>.csv` upload/delete.
//!
//! Only runs when `SG_M9ID_DIFF=1`; reads `M9ID_CORPUS`, writes `M9ID_OUT`.
//! `test_ws/p9-go/run_exhaustive_matchwriter_diff.sh` compares the two JSON
//! files byte for byte.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_service_worker::jobs::exhaustive_search_janitor::{log_key, JobLogsBlobStore};
use sg_service_worker::jobs::exhaustive_search_job::MatchWriterFactory;
use sg_service_worker::jobs::exhaustive_search_service::{
    ObjectStorageBackend, ObjectStorageLogBlobStore, ObjectStorageMatchWriterFactory,
    ObjectStorageUploadStore, UploadStore, BUFFERED_JSON_WRITER_FLUSH_SIZE,
};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    flush_size: usize,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    values: Vec<Value>,
    #[serde(default)]
    fail_upload: Vec<String>,
    #[serde(default)]
    objects: BTreeMap<String, SeedObject>,
    #[serde(default)]
    ops: Vec<Op>,
    #[serde(default)]
    action: String,
    #[serde(default)]
    search_job_id: i64,
    #[serde(default)]
    contents: String,
}

#[derive(Debug, Deserialize, Default)]
struct SeedObject {
    #[serde(default)]
    data: String,
    #[serde(default)]
    last_modified: i64,
}

#[derive(Debug, Deserialize, Default)]
struct Op {
    #[serde(default)]
    op: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    max_age: i64,
    #[serde(default)]
    now: i64,
}

// ---------------------------------------------------------------------------
// In-memory object.Storage fake
// ---------------------------------------------------------------------------

struct FakeObject {
    data: Vec<u8>,
    last_modified: i64,
}

#[derive(Default)]
struct FakeBackend {
    objects: Mutex<BTreeMap<String, FakeObject>>,
    uploads: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    fail_upload: Mutex<BTreeSet<String>>,
    now: Mutex<i64>,
}

impl FakeBackend {
    fn new(case: &Case) -> FakeBackend {
        let objects = case
            .objects
            .iter()
            .map(|(key, seed)| {
                (
                    key.clone(),
                    FakeObject {
                        data: seed.data.clone().into_bytes(),
                        last_modified: seed.last_modified,
                    },
                )
            })
            .collect();
        FakeBackend {
            objects: Mutex::new(objects),
            uploads: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
            fail_upload: Mutex::new(case.fail_upload.iter().cloned().collect()),
            now: Mutex::new(0),
        }
    }

    fn objects_value(&self) -> Value {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let mut map = Map::new();
        for (key, object) in objects.iter() {
            map.insert(
                key.clone(),
                Value::String(String::from_utf8_lossy(&object.data).into_owned()),
            );
        }
        Value::Object(map)
    }

    fn uploads_value(&self) -> Value {
        Value::Array(
            self.uploads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        )
    }

    fn deletes_value(&self) -> Value {
        Value::Array(
            self.deletes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        )
    }
}

#[async_trait]
impl ObjectStorageBackend for FakeBackend {
    async fn init(&self) -> Result<(), String> {
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        objects
            .get(key)
            .map(|object| object.data.clone())
            .ok_or_else(|| "key not found".to_owned())
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        if self
            .fail_upload
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(key)
        {
            return Err(format!("upload {key}: boom"));
        }
        let len = data.len() as i64;
        self.objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                key.to_owned(),
                FakeObject {
                    data,
                    last_modified: *self.now.lock().unwrap_or_else(PoisonError::into_inner),
                },
            );
        self.uploads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key.to_owned());
        Ok(len)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
        self.deletes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key.to_owned());
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String> {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), String> {
        let now = *self.now.lock().unwrap_or_else(PoisonError::into_inner);
        let max_age = max_age.as_secs() as i64;
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let expired: Vec<String> = objects
            .iter()
            .filter(|(key, object)| {
                key.starts_with(prefix) && now - object.last_modified >= max_age
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            objects.remove(&key);
            self.deletes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(key);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

fn case_map(case: &Case) -> Map<String, Value> {
    let mut value = Map::new();
    value.insert("name".to_owned(), Value::String(case.name.clone()));
    value.insert("kind".to_owned(), Value::String(case.kind.clone()));
    value
}

fn run_writer(case: &Case) -> Value {
    let backend = Arc::new(FakeBackend::new(case));
    let upload_store: Arc<dyn UploadStore> =
        Arc::new(ObjectStorageUploadStore::new(Arc::clone(&backend)));
    let factory =
        ObjectStorageMatchWriterFactory::new(upload_store).with_flush_size(case.flush_size);
    let writer = factory.new_writer(&case.prefix).expect("valid writer");

    let mut error = None;
    for value in &case.values {
        if let Err(err) = writer.write(value.clone()) {
            error = Some(err);
            break;
        }
    }
    if error.is_none() {
        if let Err(err) = writer.flush() {
            error = Some(err);
        }
    }

    let mut out = case_map(case);
    out.insert("objects".to_owned(), backend.objects_value());
    out.insert("uploads".to_owned(), backend.uploads_value());
    if let Some(error) = error {
        out.insert("error".to_owned(), Value::String(error));
    }
    Value::Object(out)
}

fn run_default_flush_size(case: &Case) -> Value {
    let mut out = case_map(case);
    out.insert(
        "flush_size".to_owned(),
        Value::from(BUFFERED_JSON_WRITER_FLUSH_SIZE as u64),
    );
    Value::Object(out)
}

async fn run_object_ops(case: &Case) -> Value {
    let backend = Arc::new(FakeBackend::new(case));
    let upload_store: Arc<dyn UploadStore> =
        Arc::new(ObjectStorageUploadStore::new(Arc::clone(&backend)));

    let mut results = Vec::new();
    for op in &case.ops {
        let mut result = Map::new();
        result.insert("op".to_owned(), Value::String(op.op.clone()));
        match op.op.as_str() {
            "get" => {
                result.insert("key".to_owned(), Value::String(op.key.clone()));
                match upload_store.get(&op.key).await {
                    Ok(data) => {
                        result.insert(
                            "data".to_owned(),
                            Value::String(String::from_utf8_lossy(&data).into_owned()),
                        );
                    }
                    Err(err) => {
                        result.insert("error".to_owned(), Value::String(err));
                    }
                }
            }
            "list" => {
                result.insert("prefix".to_owned(), Value::String(op.prefix.clone()));
                match upload_store.list(&op.prefix).await {
                    Ok(list) => {
                        result.insert(
                            "keys".to_owned(),
                            Value::Array(list.keys.into_iter().map(Value::String).collect()),
                        );
                    }
                    Err(err) => {
                        result.insert("error".to_owned(), Value::String(err));
                    }
                }
            }
            "delete" => {
                result.insert("key".to_owned(), Value::String(op.key.clone()));
                if let Err(err) = upload_store.delete(&op.key).await {
                    result.insert("error".to_owned(), Value::String(err));
                }
            }
            "expire" => {
                result.insert("prefix".to_owned(), Value::String(op.prefix.clone()));
                result.insert("max_age".to_owned(), Value::from(op.max_age));
                *backend.now.lock().unwrap_or_else(PoisonError::into_inner) = op.now;
                if let Err(err) = backend
                    .expire_objects(&op.prefix, Duration::from_secs(op.max_age as u64))
                    .await
                {
                    result.insert("error".to_owned(), Value::String(err));
                }
            }
            other => {
                result.insert(
                    "error".to_owned(),
                    Value::String(format!("unknown op {other:?}")),
                );
            }
        }
        results.push(Value::Object(result));
    }

    let mut out = case_map(case);
    out.insert("objects".to_owned(), backend.objects_value());
    out.insert("deletes".to_owned(), backend.deletes_value());
    out.insert("results".to_owned(), Value::Array(results));
    Value::Object(out)
}

async fn run_log_blob(case: &Case) -> Value {
    let backend = Arc::new(FakeBackend::new(case));
    let log_store = ObjectStorageLogBlobStore::new(Arc::clone(&backend));
    let key = log_key(case.search_job_id);

    let error = match case.action.as_str() {
        "upload" => log_store
            .upload(&key, case.contents.clone().into_bytes())
            .await
            .err(),
        "delete" => log_store.delete(&key).await.err(),
        other => Some(format!("unknown action {other:?}")),
    };

    let mut out = case_map(case);
    out.insert("action".to_owned(), Value::String(case.action.clone()));
    out.insert("key".to_owned(), Value::String(key));
    out.insert("objects".to_owned(), backend.objects_value());
    out.insert("uploads".to_owned(), backend.uploads_value());
    out.insert("deletes".to_owned(), backend.deletes_value());
    if let Some(error) = error {
        out.insert("error".to_owned(), Value::String(error));
    }
    Value::Object(out)
}

async fn run_case(case: &Case) -> Value {
    match case.kind.as_str() {
        "writer" => run_writer(case),
        "default_flush_size" => run_default_flush_size(case),
        "object_ops" => run_object_ops(case).await,
        "log_blob" => run_log_blob(case).await,
        other => json!({
            "name": case.name,
            "kind": case.kind,
            "error": format!("unknown case kind {other:?}"),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhaustive_matchwriter_diff() {
    if std::env::var("SG_M9ID_DIFF").as_deref() != Ok("1") {
        return;
    }

    let corpus_path = std::env::var("M9ID_CORPUS").expect("M9ID_CORPUS");
    let out_path = std::env::var("M9ID_OUT").expect("M9ID_OUT");
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let mut output = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        output.push(run_case(case).await);
    }

    let mut encoded = serde_json::to_string_pretty(&output).expect("encode output");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");
}
