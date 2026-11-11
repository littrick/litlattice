#![forbid(unsafe_code)]

//! M9.8hy differential harness (Rust side): the exhaustive-search service layer
//! and `matchjson.go` JSON writer.
//!
//! Mirrors the untracked Go oracle
//! `internal/search/exhaustive/service/zz_m9hy_oracle_test.go`. Both sides
//! seed the same fixtures in a scratch frontend database and run the same
//! cases. The Go `Service` takes a concrete `*store.Store`, so the oracle uses
//! the real store; this side does the same through [`PgSearchJobStore`].
//! Object storage and the query searcher are fakes, and the actor is injected
//! through [`ActorContext`].
//!
//! Only runs when `SG_M9HY_DIFF=1`; reads `M9HY_CORPUS`, writes `M9HY_OUT` and
//! connects to `SG_M9HY_TEST_DSN`. `test_ws/p9-go/
//! run_exhaustive_search_service_diff.sh` compares the two JSON files byte for
//! byte.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sg_db::exhaustive_search as ex;
use sg_service_worker::jobs::exhaustive_search_job::{MatchWriter, NewSearcher, SearchQuery};
use sg_service_worker::jobs::exhaustive_search_service::{
    ActorContext, BlobUploader, BufferedWriter, PgSearchJobStore, Service, UploadList, UploadStore,
};
use sqlx::postgres::{PgPool, PgPoolOptions};

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
    actor_uid: i32,
    #[serde(default)]
    actor_internal: bool,
    #[serde(default)]
    query: String,
    #[serde(default)]
    id: i64,
    #[serde(default)]
    search_error: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    uploads: BTreeMap<String, String>,
    #[serde(default)]
    list_keys: Option<Vec<String>>,
    #[serde(default)]
    list_error: String,
    #[serde(default)]
    list_terminal_error: String,
    #[serde(default)]
    get_errors: BTreeMap<String, String>,
    #[serde(default)]
    delete_errors: BTreeMap<String, String>,
    #[serde(default)]
    upload_error: String,
    #[serde(default)]
    flush_size: usize,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    values: Vec<Value>,
    #[serde(default)]
    create_seq: i64,
    #[serde(default)]
    args: Args,
    #[serde(default)]
    setup: Setup,
}

#[derive(Debug, Deserialize, Default)]
struct Args {
    #[serde(default)]
    query: String,
    #[serde(default)]
    states: Vec<String>,
    #[serde(default)]
    user_ids: Vec<i32>,
}

#[derive(Debug, Deserialize, Default)]
struct Setup {
    #[serde(default)]
    search_jobs: Vec<JobSeed>,
    #[serde(default)]
    repo_jobs: Vec<RepoJobSeed>,
    #[serde(default)]
    rev_jobs: Vec<RevJobSeed>,
}

#[derive(Debug, Deserialize)]
struct JobSeed {
    id: i64,
    initiator_id: i32,
    query: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct RepoJobSeed {
    id: i64,
    search_job_id: i64,
    repo_id: i32,
    #[serde(default)]
    ref_spec: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct RevJobSeed {
    id: i64,
    search_repo_job_id: i64,
    revision: String,
    state: String,
    #[serde(default)]
    failure_message: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    finished_at: Option<String>,
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

struct StaticActor {
    actor: ex::Actor,
}

impl ActorContext for StaticActor {
    fn actor(&self) -> ex::Actor {
        self.actor
    }
}

struct DummyQuery;

#[async_trait]
impl SearchQuery for DummyQuery {
    async fn repository_rev_specs(&self) -> Result<Vec<ex::RepositoryRevSpecs>, String> {
        Err("unused".to_owned())
    }

    async fn resolve_repository_rev_spec(
        &self,
        _repo_rev_spec: &ex::RepositoryRevSpecs,
    ) -> Result<Vec<ex::RepositoryRevision>, String> {
        Err("unused".to_owned())
    }

    async fn search(
        &self,
        _repo_rev: &ex::RepositoryRevision,
        _match_writer: &dyn MatchWriter,
    ) -> Result<(), String> {
        Err("unused".to_owned())
    }
}

struct FakeSearcher {
    error: Option<String>,
    calls: AtomicUsize,
    user: Mutex<Option<i32>>,
    query: Mutex<Option<String>>,
}

impl FakeSearcher {
    fn new(error: &str) -> FakeSearcher {
        FakeSearcher {
            error: if error.is_empty() {
                None
            } else {
                Some(error.to_owned())
            },
            calls: AtomicUsize::new(0),
            user: Mutex::new(None),
            query: Mutex::new(None),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn user(&self) -> Option<i32> {
        *self.user.lock().unwrap()
    }

    fn query(&self) -> Option<String> {
        self.query.lock().unwrap().clone()
    }
}

#[async_trait]
impl NewSearcher for FakeSearcher {
    async fn new_search(&self, user_id: i32, query: &str) -> Result<Box<dyn SearchQuery>, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.user.lock().unwrap() = Some(user_id);
        *self.query.lock().unwrap() = Some(query.to_owned());

        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(Box::new(DummyQuery)),
        }
    }
}

struct FakeUploadStore {
    bucket: Mutex<BTreeMap<String, Vec<u8>>>,
    upload_error: Option<String>,
    get_errors: BTreeMap<String, String>,
    delete_errors: BTreeMap<String, String>,
    list_error: Option<String>,
    list_keys: Option<Vec<String>>,
    list_terminal_error: Option<String>,
}

impl FakeUploadStore {
    fn from_case(case: &Case) -> FakeUploadStore {
        let mut bucket = BTreeMap::new();
        for (key, value) in &case.uploads {
            bucket.insert(key.clone(), value.as_bytes().to_vec());
        }

        FakeUploadStore {
            bucket: Mutex::new(bucket),
            upload_error: none_if_empty(&case.upload_error),
            get_errors: case.get_errors.clone(),
            delete_errors: case.delete_errors.clone(),
            list_error: none_if_empty(&case.list_error),
            list_keys: case.list_keys.clone(),
            list_terminal_error: none_if_empty(&case.list_terminal_error),
        }
    }

    fn snapshot(&self) -> Value {
        let bucket = self.bucket.lock().unwrap();
        Value::Array(
            bucket
                .iter()
                .map(|(key, content)| {
                    json!({
                        "key": key,
                        "content": String::from_utf8_lossy(content),
                    })
                })
                .collect(),
        )
    }
}

fn none_if_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

#[async_trait]
impl UploadStore for FakeUploadStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        if let Some(message) = self.get_errors.get(key) {
            return Err(message.clone());
        }
        self.bucket
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| "key not found".to_owned())
    }

    async fn upload(&self, key: &str, data: &[u8]) -> Result<i64, String> {
        if let Some(message) = &self.upload_error {
            return Err(message.clone());
        }
        self.bucket
            .lock()
            .unwrap()
            .insert(key.to_owned(), data.to_vec());
        Ok(data.len() as i64)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        if let Some(message) = self.delete_errors.get(key) {
            return Err(message.clone());
        }
        self.bucket.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<UploadList, String> {
        if let Some(message) = &self.list_error {
            return Err(message.clone());
        }
        let keys = match &self.list_keys {
            Some(keys) => keys.clone(),
            None => self
                .bucket
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect(),
        };
        Ok(UploadList {
            keys,
            error: self.list_terminal_error.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn seed(pool: &PgPool, case: &Case) {
    for query in [
        "TRUNCATE TABLE exhaustive_search_jobs RESTART IDENTITY CASCADE",
        "DELETE FROM users WHERE id IN (101, 102)",
        "DELETE FROM repo WHERE id IN (201, 202)",
    ] {
        sqlx::query(query)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("seed {query:?}: {err}"));
    }

    sqlx::query(
        "INSERT INTO users (id, username, display_name, site_admin, deleted_at) VALUES
            (101, 'm9hy-admin', 'Admin', TRUE, NULL),
            (102, 'm9hy-user', 'User', FALSE, NULL)",
    )
    .execute(pool)
    .await
    .expect("insert users");

    sqlx::query(
        "INSERT INTO repo (id, name) VALUES
            (201, 'github.com/acme/alpha'),
            (202, 'github.com/acme/beta')",
    )
    .execute(pool)
    .await
    .expect("insert repos");

    for job in &case.setup.search_jobs {
        sqlx::query(
            "INSERT INTO exhaustive_search_jobs
              (id, initiator_id, query, state, cancel, started_at, finished_at, num_resets,
               num_failures, worker_hostname, created_at, updated_at)
            VALUES ($1, $2, $3, $4, FALSE,
                    CASE WHEN $4 IN ('queued') THEN NULL
                         ELSE '2026-01-01T00:00:00Z'::timestamptz END,
                    CASE WHEN $4 IN ('queued', 'processing', 'errored') THEN NULL
                         ELSE '2026-01-01T00:10:00Z'::timestamptz END,
                    0, 0, 'seed-host', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .bind(job.id)
        .bind(job.initiator_id)
        .bind(job.query.as_str())
        .bind(job.state.as_str())
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert search job {}: {err}", job.id));
    }

    for job in &case.setup.repo_jobs {
        sqlx::query(
            "INSERT INTO exhaustive_search_repo_jobs
              (id, search_job_id, repo_id, ref_spec, state, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .bind(job.id)
        .bind(job.search_job_id)
        .bind(job.repo_id)
        .bind(job.ref_spec.as_str())
        .bind(job.state.as_str())
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert repo job {}: {err}", job.id));
    }

    for job in &case.setup.rev_jobs {
        sqlx::query(
            "INSERT INTO exhaustive_search_repo_revision_jobs
              (id, search_repo_job_id, revision, state, failure_message, started_at,
               finished_at, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NULLIF($5::text, ''), ($6::text)::timestamptz,
                    ($7::text)::timestamptz, '2026-01-01T00:00:00Z',
                    '2026-01-01T00:00:00Z')",
        )
        .bind(job.id)
        .bind(job.search_repo_job_id)
        .bind(job.revision.as_str())
        .bind(job.state.as_str())
        .bind(job.failure_message.as_str())
        .bind(job.started_at.as_deref())
        .bind(job.finished_at.as_deref())
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert rev job {}: {err}", job.id));
    }

    let sequence = if case.create_seq == 0 {
        1000
    } else {
        case.create_seq
    };
    sqlx::query("SELECT setval('exhaustive_search_jobs_id_seq', $1)")
        .bind(sequence)
        .execute(pool)
        .await
        .expect("setval");
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

fn time_marker(time: Option<time::OffsetDateTime>) -> &'static str {
    if time.is_some() {
        "T"
    } else {
        "-"
    }
}

fn job_map(job: &ex::ExhaustiveSearchJob) -> Value {
    json!({
        "agg_state": job.agg_state.as_str(),
        "cancel": job.worker.cancel,
        "failure_message": job.worker.failure_message,
        "finished_at": time_marker(job.worker.finished_at),
        "id": job.id,
        "initiator_id": job.initiator_id,
        "is_aggregated": job.is_aggregated,
        "query": job.query,
        "started_at": time_marker(job.worker.started_at),
        "state": job.worker.state.as_str(),
    })
}

// ---------------------------------------------------------------------------
// Case runner
// ---------------------------------------------------------------------------

async fn run_case(pool: &PgPool, case: &Case) -> Value {
    let store = Arc::new(PgSearchJobStore::new(pool.clone()));
    let upload_store = Arc::new(FakeUploadStore::from_case(case));
    let searcher = Arc::new(FakeSearcher::new(&case.search_error));
    let actor = ex::Actor {
        uid: case.actor_uid,
        internal: case.actor_internal,
    };

    let service = Service::new(
        store,
        upload_store.clone(),
        searcher.clone(),
        Arc::new(StaticActor { actor }),
    );

    let mut out = serde_json::Map::new();
    out.insert("name".to_owned(), json!(case.name));

    let mut call_err: Option<String> = None;
    let mut write_err: Option<String> = None;

    match case.kind.as_str() {
        "validate" => {
            call_err = service.validate_search_job(&case.query).await.err();
        }

        "create" => match service.create_search_job(&case.query).await {
            Ok(job) => {
                out.insert("job".to_owned(), job_map(&job));
            }
            Err(err) => call_err = Some(err),
        },

        "cancel" => {
            call_err = service.cancel_search_job(case.id).await.err();
            if call_err.is_none() {
                match service.get_search_job(case.id).await {
                    Ok(job) => {
                        out.insert("job_after".to_owned(), job_map(&job));
                    }
                    Err(err) => {
                        out.insert("job_after_error".to_owned(), json!(err));
                    }
                }
            }
        }

        "get" => match service.get_search_job(case.id).await {
            Ok(job) => {
                out.insert("job".to_owned(), job_map(&job));
            }
            Err(err) => call_err = Some(err),
        },

        "list" => {
            let args = ex::ListArgs {
                query: case.args.query.clone(),
                states: case.args.states.clone(),
                user_ids: case.args.user_ids.clone(),
            };
            match service.list_search_jobs(&args).await {
                Ok(mut jobs) => {
                    jobs.sort_by_key(|job| job.id);
                    let jobs: Vec<Value> = jobs.iter().map(job_map).collect();
                    out.insert("jobs".to_owned(), Value::Array(jobs));
                }
                Err(err) => call_err = Some(err),
            }
        }

        "delete_search_job" => {
            call_err = service.delete_search_job(case.id).await.err();
            if call_err.is_none() {
                let after = match service.get_search_job(case.id).await {
                    Ok(_) => String::new(),
                    Err(err) => err,
                };
                out.insert("get_after_error".to_owned(), json!(after));
            }
        }

        "upload_logs" => {
            let mut uploaded = 0;
            match service.upload_job_logs(case.id, case.data.as_bytes()).await {
                Ok(n) => uploaded = n,
                Err(err) => call_err = Some(err),
            }
            out.insert("uploaded".to_owned(), json!(uploaded));
        }

        "get_logs" => match service.get_job_logs(case.id).await {
            Ok(content) => {
                out.insert(
                    "content".to_owned(),
                    json!(String::from_utf8_lossy(&content)),
                );
            }
            Err(err) => call_err = Some(err),
        },

        "delete_logs" => {
            call_err = service.delete_job_logs(case.id).await.err();
        }

        "logs_writer_to" => match service.get_search_job_logs_writer_to(case.id).await {
            Ok(writer_to) => {
                let mut buffer = Vec::new();
                let outcome = writer_to.write_to(&mut buffer).await;
                out.insert("csv".to_owned(), json!(String::from_utf8_lossy(&buffer)));
                out.insert("bytes_written".to_owned(), json!(outcome.bytes_written));
                write_err = outcome.error;
            }
            Err(err) => call_err = Some(err),
        },

        "results_writer_to" => match service.get_search_job_results_writer_to(case.id).await {
            Ok(writer_to) => {
                let mut buffer = Vec::new();
                let outcome = writer_to.write_to(&mut buffer).await;
                out.insert("output".to_owned(), json!(String::from_utf8_lossy(&buffer)));
                out.insert("bytes_written".to_owned(), json!(outcome.bytes_written));
                write_err = outcome.error;
            }
            Err(err) => call_err = Some(err),
        },

        "aggregate" => match service.get_aggregate_repo_rev_state(case.id).await {
            Ok(stats) => {
                out.insert(
                    "stats".to_owned(),
                    json!({
                        "completed": stats.completed,
                        "failed": stats.failed,
                        "in_progress": stats.in_progress,
                        "total": stats.total,
                    }),
                );
            }
            Err(err) => call_err = Some(err),
        },

        "json_writer" => {
            let uploader = Arc::new(BlobUploader::new(upload_store.clone(), case.prefix.clone()));
            let mut writer = BufferedWriter::new(case.flush_size, uploader);
            for value in &case.values {
                if let Err(err) = writer.append(value).await {
                    call_err = Some(err);
                    break;
                }
            }
            if call_err.is_none() {
                call_err = writer.flush().await.err();
            }
        }

        other => panic!("case {}: unknown kind {other}", case.name),
    }

    let calls = searcher.calls();
    out.insert("search_calls".to_owned(), json!(calls));
    if calls > 0 {
        out.insert("searched_user".to_owned(), json!(searcher.user()));
        out.insert("searched_query".to_owned(), json!(searcher.query()));
    }

    out.insert(
        "ok".to_owned(),
        json!(call_err.is_none() && write_err.is_none()),
    );
    if let Some(err) = call_err {
        out.insert("error".to_owned(), json!(err));
    }
    if let Some(err) = write_err {
        out.insert("write_error".to_owned(), json!(err));
    }
    out.insert("uploads".to_owned(), upload_store.snapshot());

    Value::Object(out)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exhaustive_search_service_diff() {
    if std::env::var("SG_M9HY_DIFF").as_deref() != Ok("1") {
        return;
    }

    let corpus_path = std::env::var("M9HY_CORPUS").expect("M9HY_CORPUS is not set");
    let out_path = std::env::var("M9HY_OUT").expect("M9HY_OUT is not set");
    let dsn = std::env::var("SG_M9HY_TEST_DSN").expect("SG_M9HY_TEST_DSN is not set");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");

    let mut results = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        seed(&pool, case).await;
        results.push(run_case(&pool, case).await);
    }

    let mut output = serde_json::to_string_pretty(&Value::Array(results)).expect("encode output");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");

    let cases = corpus.cases.len();
    eprintln!("m9hy rust wrote {cases} cases");
}
