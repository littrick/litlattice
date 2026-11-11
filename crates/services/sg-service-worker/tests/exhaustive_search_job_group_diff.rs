//! M9.8ik differential harness: the `exhaustive-search-job` group assembly
//! and the three handlers it wires.
//!
//! Mirrors `cmd/worker/internal/search/zz_m9ik_oracle_test.go` (UNTRACKED, Go
//! package `search`). Both sides consume the same corpus
//! (`test_ws/p9-go/data/exhaustive_search_job_group_corpus.json`), write one
//! normalized JSON array to `M9IK_OUT`, and `test_ws/p9-go/
//! run_exhaustive_search_job_group_diff.sh` compares them byte for byte.
//!
//! Group cases compare the enabled flag, the config count, error, and the
//! seven routines' metadata (names, intervals, worker heartbeat and handler
//! count). Handler cases seed the scratch frontend database with the same rows
//! as the Go oracle and drive `DbWorkerHandler::handle` with the equivalent of
//! `service.NewSearcherFake`.
//!
//! Gated by `SG_M9IK_DIFF=1`; without it the test returns immediately.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sg_config::LiveConfig;
use sg_core::EnvReader;
use sg_db::exhaustive_search::{
    ExhaustiveSearchJob, ExhaustiveSearchRepoJob, ExhaustiveSearchRepoRevisionJob, JobState,
    RepositoryRevSpecs, RepositoryRevision, WorkerJob,
};
use sg_db::OffsetDateTime;
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::jobs::exhaustive_search_job::{
    ExhaustiveSearchHandler, ExhaustiveSearchRepoHandler, ExhaustiveSearchRepoRevisionHandler,
    MatchWriter, MatchWriterFactory, NewSearcher, SearchJob, SearchJobRoutines, SearchQuery,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct FakeSearcher;

#[async_trait]
impl NewSearcher for FakeSearcher {
    async fn new_search(&self, _user_id: i32, query: &str) -> Result<Box<dyn SearchQuery>, String> {
        let mut repo_revs = Vec::new();
        for part in query.split_whitespace() {
            let Some((repo, revision)) = part.split_once('@') else {
                continue;
            };
            let Ok(repository) = repo.parse::<i32>() else {
                continue;
            };
            if revision.is_empty() {
                continue;
            }
            repo_revs.push(RepositoryRevision {
                repository_rev_specs: RepositoryRevSpecs {
                    repository,
                    revision_specifiers: "spec".to_owned(),
                },
                revision: revision.to_owned(),
            });
        }
        if repo_revs.is_empty() {
            return Err(format!("no repository revisions found in {query:?}"));
        }
        Ok(Box::new(FakeSearchQuery { repo_revs }))
    }
}

struct FakeSearchQuery {
    repo_revs: Vec<RepositoryRevision>,
}

#[async_trait]
impl SearchQuery for FakeSearchQuery {
    async fn repository_rev_specs(&self) -> Result<Vec<RepositoryRevSpecs>, String> {
        let mut specs: Vec<RepositoryRevSpecs> = Vec::new();
        for repo_rev in &self.repo_revs {
            if !specs.contains(&repo_rev.repository_rev_specs) {
                specs.push(repo_rev.repository_rev_specs.clone());
            }
        }
        Ok(specs)
    }

    async fn resolve_repository_rev_spec(
        &self,
        repo_rev_spec: &RepositoryRevSpecs,
    ) -> Result<Vec<RepositoryRevision>, String> {
        Ok(self
            .repo_revs
            .iter()
            .filter(|repo_rev| &repo_rev.repository_rev_specs == repo_rev_spec)
            .cloned()
            .collect())
    }

    async fn search(
        &self,
        repo_rev: &RepositoryRevision,
        match_writer: &dyn MatchWriter,
    ) -> Result<(), String> {
        let repository_id = repo_rev.repository_rev_specs.repository;
        match_writer.write(json!({
            "type": "path",
            "path": "path/to/file.go",
            "repositoryID": repository_id,
            "repository": format!("repo{repository_id}"),
            "commit": repo_rev.revision,
            "language": "Go",
        }))
    }
}

#[derive(Clone, Default)]
struct FakeWriters {
    uploads: Arc<Mutex<BTreeMap<String, Vec<Value>>>>,
}

struct FakeWriter {
    key: String,
    buffer: Mutex<Vec<Value>>,
    uploads: Arc<Mutex<BTreeMap<String, Vec<Value>>>>,
}

impl MatchWriter for FakeWriter {
    fn write(&self, r#match: Value) -> Result<(), String> {
        self.buffer.lock().expect("buffer lock").push(r#match);
        Ok(())
    }

    fn flush(&self) -> Result<(), String> {
        let buffer = std::mem::take(&mut *self.buffer.lock().expect("buffer lock"));
        self.uploads
            .lock()
            .expect("uploads lock")
            .insert(self.key.clone(), buffer);
        Ok(())
    }
}

impl MatchWriterFactory for FakeWriters {
    fn new_writer(&self, key: &str) -> Result<Box<dyn MatchWriter>, String> {
        Ok(Box::new(FakeWriter {
            key: key.to_owned(),
            buffer: Mutex::new(Vec::new()),
            uploads: Arc::clone(&self.uploads),
        }))
    }
}

fn worker_job() -> WorkerJob {
    WorkerJob {
        state: JobState::new("processing"),
        failure_message: String::new(),
        started_at: None,
        finished_at: None,
        process_after: None,
        num_resets: 0,
        num_failures: 0,
        worker_hostname: String::new(),
        cancel: false,
    }
}

fn job_record(id: i64, initiator_id: i32, query: &str) -> ExhaustiveSearchJob {
    let now = OffsetDateTime::now_utc();
    ExhaustiveSearchJob {
        worker: worker_job(),
        id,
        initiator_id,
        query: query.to_owned(),
        created_at: now,
        updated_at: now,
        agg_state: JobState::new(""),
        is_aggregated: false,
    }
}

fn repo_record(
    id: i64,
    repo_id: i32,
    ref_spec: &str,
    search_job_id: i64,
) -> ExhaustiveSearchRepoJob {
    let now = OffsetDateTime::now_utc();
    ExhaustiveSearchRepoJob {
        worker: worker_job(),
        id,
        repo_id,
        ref_spec: ref_spec.to_owned(),
        search_job_id,
        created_at: now,
        updated_at: now,
    }
}

fn rev_record(id: i64, search_repo_job_id: i64, revision: &str) -> ExhaustiveSearchRepoRevisionJob {
    let now = OffsetDateTime::now_utc();
    ExhaustiveSearchRepoRevisionJob {
        worker: worker_job(),
        id,
        search_repo_job_id,
        revision: revision.to_owned(),
        created_at: now,
        updated_at: now,
    }
}

fn time_text(value: Option<OffsetDateTime>) -> &'static str {
    if value.is_some() {
        "T"
    } else {
        "-"
    }
}

async fn repo_rows(pool: &sqlx::PgPool) -> Result<Vec<Value>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, repo_id, ref_spec, search_job_id, state, failure_message,
                worker_hostname, num_resets, num_failures, cancel, created_at, updated_at
         FROM exhaustive_search_repo_jobs ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(json!({
            "id": row.try_get::<i32, _>("id")?,
            "repo_id": row.try_get::<i32, _>("repo_id")?,
            "ref_spec": row.try_get::<String, _>("ref_spec")?,
            "search_job_id": row.try_get::<i32, _>("search_job_id")?,
            "state": row.try_get::<String, _>("state")?,
            "failure_message": row.try_get::<Option<String>, _>("failure_message")?.unwrap_or_default(),
            "worker_hostname": row.try_get::<String, _>("worker_hostname")?,
            "num_resets": row.try_get::<i32, _>("num_resets")?,
            "num_failures": row.try_get::<i32, _>("num_failures")?,
            "cancel": row.try_get::<bool, _>("cancel")?,
            "created_at": time_text(row.try_get("created_at")?),
            "updated_at": time_text(row.try_get("updated_at")?),
        }));
    }
    Ok(out)
}

async fn rev_rows(pool: &sqlx::PgPool) -> Result<Vec<Value>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, search_repo_job_id, revision, state, failure_message,
                worker_hostname, num_resets, num_failures, cancel, created_at, updated_at
         FROM exhaustive_search_repo_revision_jobs ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(json!({
            "id": row.try_get::<i32, _>("id")?,
            "search_repo_job_id": row.try_get::<i32, _>("search_repo_job_id")?,
            "revision": row.try_get::<String, _>("revision")?,
            "state": row.try_get::<String, _>("state")?,
            "failure_message": row.try_get::<Option<String>, _>("failure_message")?.unwrap_or_default(),
            "worker_hostname": row.try_get::<String, _>("worker_hostname")?,
            "num_resets": row.try_get::<i32, _>("num_resets")?,
            "num_failures": row.try_get::<i32, _>("num_failures")?,
            "cancel": row.try_get::<bool, _>("cancel")?,
            "created_at": time_text(row.try_get("created_at")?),
            "updated_at": time_text(row.try_get("updated_at")?),
        }));
    }
    Ok(out)
}

fn uploads(writers: &FakeWriters) -> Value {
    let uploads = writers.uploads.lock().expect("uploads lock");
    let entries: Vec<Value> = uploads
        .iter()
        .map(|(key, matches)| json!({"key": key, "matches": matches}))
        .collect();
    Value::Array(entries)
}

async fn seed(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    for statement in [
        "TRUNCATE TABLE exhaustive_search_repo_revision_jobs, exhaustive_search_repo_jobs,
            exhaustive_search_jobs RESTART IDENTITY CASCADE",
        "DELETE FROM users WHERE id IN (101)",
        "DELETE FROM repo WHERE id IN (201, 202)",
        "INSERT INTO users (id, username, display_name, site_admin, deleted_at)
            VALUES (101, 'm9ik-user', 'User', FALSE, NULL)",
        "INSERT INTO repo (id, name) VALUES (201, 'github.com/acme/alpha'), (202, 'github.com/acme/beta')",
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES
                (1, 101, '201@rev1 201@rev2 202@rev3', 'processing'),
                (2, 101, 'bogus', 'processing')",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

fn record_i64(record: &Value, key: &str) -> Result<i64, String> {
    record
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("record.{key} is not an integer"))
}

fn record_str<'a>(record: &'a Value, key: &str) -> Result<&'a str, String> {
    record
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("record.{key} is not a string"))
}

/// The routine metadata the Go oracle reads through reflection: worker names,
/// descriptions, intervals, heartbeat and handler count, resetter names and
/// intervals, and the janitor's name/description/interval.
fn routine_metadata(assembled: &SearchJobRoutines) -> (Vec<String>, Vec<Value>) {
    let names: Vec<String> = assembled
        .routines
        .iter()
        .map(|r| r.name().to_owned())
        .collect();
    if assembled.routines.is_empty() {
        return (names, Vec::new());
    }

    let mut routines = Vec::with_capacity(names.len());
    for options in &assembled.worker_options {
        routines.push(json!({
            "kind": "worker",
            "name": options.name,
            "description": options.description,
            "interval_ms": options.interval.as_millis() as u64,
            "heartbeat_ms": options.heartbeat_interval.as_millis() as u64,
            "num_handlers": options.num_handlers,
        }));
    }
    for options in &assembled.resetter_options {
        routines.push(json!({
            "kind": "resetter",
            "name": options.name,
            "interval_ms": options.interval.as_millis() as u64,
        }));
    }
    let janitor = assembled
        .routines
        .last()
        .expect("the group always appends the janitor");
    routines.push(json!({
        "kind": "periodic",
        "name": janitor.name(),
        "description": janitor.description(),
        "interval_ms": janitor.interval().as_millis() as u64,
    }));

    (names, routines)
}

async fn test_body(pool: sqlx::PgPool, corpus_path: &str, out_path: &str) -> Result<(), String> {
    let raw = std::fs::read_to_string(corpus_path)
        .map_err(|err| format!("read corpus {corpus_path}: {err}"))?;
    let corpus: Value =
        serde_json::from_str(&raw).map_err(|err| format!("decode corpus: {err}"))?;
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .ok_or_else(|| "corpus.cases is not an array".to_owned())?;

    let ctx = CancellationToken::new();
    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };

    let mut results: Vec<Value> = Vec::with_capacity(cases.len());
    for case in cases {
        let name = case
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "case.name is not a string".to_owned())?;
        let kind = case
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| "case.kind is not a string".to_owned())?;

        match kind {
            "group" => {
                let disabled = case
                    .get("env")
                    .and_then(Value::as_object)
                    .is_some_and(|env| env.contains_key("DISABLE_SEARCH_JOBS"));

                let job = SearchJob::with_searcher_factory(Arc::new(|| Arc::new(FakeSearcher)));
                if disabled {
                    std::env::set_var("DISABLE_SEARCH_JOBS", "true");
                } else {
                    std::env::remove_var("DISABLE_SEARCH_JOBS");
                    job.config().load(&mut EnvReader::new());
                }
                let assembled = job.assemble(&deps).await;
                if disabled {
                    std::env::remove_var("DISABLE_SEARCH_JOBS");
                }

                let (error, names, routines) = match &assembled {
                    Ok(assembled) => {
                        let (names, routines) = routine_metadata(assembled);
                        ("-".to_owned(), names, routines)
                    }
                    Err(err) => (err.clone(), Vec::new(), Vec::new()),
                };
                results.push(json!({
                    "name": name,
                    "kind": "group",
                    "enabled": assembled.as_ref().is_ok_and(|a| !a.routines.is_empty()),
                    "error": error,
                    "config_count": job.configs().len(),
                    "routine_names": names,
                    "routines": routines,
                }));
            }

            "job_handle" | "repo_handle" | "rev_handle" => {
                seed(&pool).await.map_err(|err| err.to_string())?;
                if let Some(statements) = case.get("sql").and_then(Value::as_array) {
                    for statement in statements {
                        let statement = statement
                            .as_str()
                            .ok_or_else(|| "case.sql entries must be strings".to_owned())?;
                        sqlx::query(statement)
                            .execute(&pool)
                            .await
                            .map_err(|err| err.to_string())?;
                    }
                }
                let record = case
                    .get("record")
                    .ok_or_else(|| format!("{name}: record is missing"))?;

                let (error, output) = match kind {
                    "job_handle" => {
                        let handler =
                            ExhaustiveSearchHandler::new(pool.clone(), Arc::new(FakeSearcher));
                        let err = handler
                            .handle(
                                ctx.clone(),
                                &job_record(
                                    record_i64(record, "id")?,
                                    record_i64(record, "initiator_id")? as i32,
                                    record_str(record, "query")?,
                                ),
                            )
                            .await
                            .err();
                        (
                            err.unwrap_or_else(|| "-".to_owned()),
                            json!({"repo_jobs": repo_rows(&pool).await.map_err(|e| e.to_string())?}),
                        )
                    }
                    "repo_handle" => {
                        let handler =
                            ExhaustiveSearchRepoHandler::new(pool.clone(), Arc::new(FakeSearcher));
                        let err = handler
                            .handle(
                                ctx.clone(),
                                &repo_record(
                                    record_i64(record, "id")?,
                                    record_i64(record, "repo_id")? as i32,
                                    record_str(record, "ref_spec")?,
                                    record_i64(record, "search_job_id")?,
                                ),
                            )
                            .await
                            .err();
                        (
                            err.unwrap_or_else(|| "-".to_owned()),
                            json!({"rev_jobs": rev_rows(&pool).await.map_err(|e| e.to_string())?}),
                        )
                    }
                    _ => {
                        let writers = FakeWriters::default();
                        let handler = ExhaustiveSearchRepoRevisionHandler::new(
                            pool.clone(),
                            Arc::new(FakeSearcher),
                            Arc::new(writers.clone()),
                        );
                        let err = handler
                            .handle(
                                ctx.clone(),
                                &rev_record(
                                    record_i64(record, "id")?,
                                    record_i64(record, "search_repo_job_id")?,
                                    record_str(record, "revision")?,
                                ),
                            )
                            .await
                            .err();
                        (
                            err.unwrap_or_else(|| "-".to_owned()),
                            json!({"uploads": uploads(&writers)}),
                        )
                    }
                };

                results.push(json!({
                    "name": name,
                    "kind": kind,
                    "error": error,
                    "output": output,
                }));
            }

            other => return Err(format!("unknown case kind {other:?}")),
        }
    }

    let mut encoded =
        serde_json::to_string_pretty(&results).map_err(|err| format!("encode output: {err}"))?;
    encoded.push('\n');
    std::fs::write(out_path, encoded).map_err(|err| format!("write {out_path}: {err}"))?;
    Ok(())
}

#[tokio::test]
async fn exhaustive_search_job_group_harness() {
    if std::env::var("SG_M9IK_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9IK_CORPUS").expect("M9IK_CORPUS is required when SG_M9IK_DIFF=1");
    let out_path = std::env::var("M9IK_OUT").expect("M9IK_OUT is required when SG_M9IK_DIFF=1");
    let dsn = std::env::var("SG_M9IK_TEST_DSN")
        .expect("SG_M9IK_TEST_DSN is required when SG_M9IK_DIFF=1");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");
    test_body(pool, &corpus_path, &out_path)
        .await
        .expect("harness body");
}
