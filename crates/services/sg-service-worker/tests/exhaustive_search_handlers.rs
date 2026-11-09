//! M9.8cv differential harness: the three exhaustive search worker handlers
//! (`ExhaustiveSearchHandler`, `ExhaustiveSearchRepoHandler`,
//! `ExhaustiveSearchRepoRevisionHandler`).
//!
//! Mirrors `cmd/worker/internal/search/exhaustive_handlers_harness_test.go`.
//! Both sides seed identical rows in the scratch frontend database and call the
//! handlers directly with a fake searcher equivalent to
//! `service.NewSearcherFake`. Each case prints one canonical `exhj ` JSON line;
//! `test_ws/p9-go/run_exhaustive_search_handlers_diff.sh` compares them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sg_db::exhaustive_search::{
    ExhaustiveSearchJob, ExhaustiveSearchRepoJob, ExhaustiveSearchRepoRevisionJob, JobState,
    RepositoryRevSpecs, RepositoryRevision, WorkerJob,
};
use sg_db::OffsetDateTime;
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::jobs::exhaustive_search_job::{
    ExhaustiveSearchHandler, ExhaustiveSearchRepoHandler, ExhaustiveSearchRepoRevisionHandler,
    MatchWriter, MatchWriterFactory, NewSearcher, SearchQuery,
};
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

fn emit(case: &str, error: Option<String>, output: Value) {
    let line = json!({
        "case": case,
        "error": error.unwrap_or_else(|| "-".to_owned()),
        "output": output,
    });
    println!("exhj {}", serde_json::to_string(&line).expect("encode"));
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
            VALUES (101, 'exhj-user', 'User', FALSE, NULL)",
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

async fn test_body(pool: sqlx::PgPool) -> Result<(), String> {
    let ctx = CancellationToken::new();
    let job_handler = ExhaustiveSearchHandler::new(pool.clone(), Arc::new(FakeSearcher));
    let repo_handler = ExhaustiveSearchRepoHandler::new(pool.clone(), Arc::new(FakeSearcher));

    // 1. Job handler: one repo job per unique repo/rev-spec.
    seed(&pool).await.map_err(|err| err.to_string())?;
    let err = job_handler
        .handle(
            ctx.clone(),
            &job_record(1, 101, "201@rev1 201@rev2 202@rev3"),
        )
        .await
        .err();
    emit(
        "job-handle",
        err,
        json!({"repo_jobs": repo_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 2. Job handler: the fake searcher rejects a query without repo@rev.
    seed(&pool).await.map_err(|err| err.to_string())?;
    let err = job_handler
        .handle(ctx.clone(), &job_record(2, 101, "bogus"))
        .await
        .err();
    emit(
        "job-handle-searcher-error",
        err,
        json!({"repo_jobs": repo_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 3. Repo handler: resolves rev1/rev2 for repo 201.
    seed(&pool).await.map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id)
         VALUES (31, 201, 'spec', 1)",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    let err = repo_handler
        .handle(ctx.clone(), &repo_record(31, 201, "spec", 1))
        .await
        .err();
    emit(
        "repo-handle",
        err,
        json!({"rev_jobs": rev_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 4. Repo handler: missing parent search job.
    seed(&pool).await.map_err(|err| err.to_string())?;
    let err = repo_handler
        .handle(ctx.clone(), &repo_record(35, 201, "spec", 999))
        .await
        .err();
    emit(
        "repo-handle-missing-parent",
        err,
        json!({"rev_jobs": rev_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 5. Repo handler: parent query rejected by the searcher.
    seed(&pool).await.map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id)
         VALUES (36, 201, 'spec', 2)",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    let err = repo_handler
        .handle(ctx.clone(), &repo_record(36, 201, "spec", 2))
        .await
        .err();
    emit(
        "repo-handle-searcher-error",
        err,
        json!({"rev_jobs": rev_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 6. Repo handler: the fake has no revisions for this repo/spec.
    seed(&pool).await.map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id)
         VALUES (33, 202, 'other', 1)",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    let err = repo_handler
        .handle(ctx.clone(), &repo_record(33, 202, "other", 1))
        .await
        .err();
    emit(
        "repo-handle-no-results",
        err,
        json!({"rev_jobs": rev_rows(&pool).await.map_err(|e| e.to_string())?}),
    );

    // 7. Revision handler: one match uploaded under "<job>-<rev-job>".
    seed(&pool).await.map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id)
         VALUES (31, 201, 'spec', 1)",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_revision_jobs
         (id, search_repo_job_id, revision) VALUES (41, 31, 'revA')",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    let writers = FakeWriters::default();
    let rev_handler = ExhaustiveSearchRepoRevisionHandler::new(
        pool.clone(),
        Arc::new(FakeSearcher),
        Arc::new(writers.clone()),
    );
    let err = rev_handler
        .handle(ctx.clone(), &rev_record(41, 31, "revA"))
        .await
        .err();
    emit("rev-handle", err, json!({"uploads": uploads(&writers)}));

    // 8. Revision handler: missing repo job.
    seed(&pool).await.map_err(|err| err.to_string())?;
    let writers = FakeWriters::default();
    let rev_handler = ExhaustiveSearchRepoRevisionHandler::new(
        pool.clone(),
        Arc::new(FakeSearcher),
        Arc::new(writers.clone()),
    );
    let err = rev_handler
        .handle(ctx.clone(), &rev_record(42, 999, "revA"))
        .await
        .err();
    emit(
        "rev-handle-missing-repo-job",
        err,
        json!({"uploads": uploads(&writers)}),
    );

    // 9. Revision handler: parent query rejected by the searcher.
    seed(&pool).await.map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id)
         VALUES (36, 201, 'spec', 2)",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    sqlx::query(
        "INSERT INTO exhaustive_search_repo_revision_jobs
         (id, search_repo_job_id, revision) VALUES (43, 36, 'revA')",
    )
    .execute(&pool)
    .await
    .map_err(|err| err.to_string())?;
    let writers = FakeWriters::default();
    let rev_handler = ExhaustiveSearchRepoRevisionHandler::new(
        pool.clone(),
        Arc::new(FakeSearcher),
        Arc::new(writers.clone()),
    );
    let err = rev_handler
        .handle(ctx.clone(), &rev_record(43, 36, "revA"))
        .await
        .err();
    emit(
        "rev-handle-searcher-error",
        err,
        json!({"uploads": uploads(&writers)}),
    );

    Ok(())
}

#[tokio::test]
async fn exhaustive_handlers_harness() {
    let Ok(dsn) = std::env::var("SG_EXHAUSTIVE_HANDLERS_TEST_DSN") else {
        return;
    };
    if dsn.is_empty() {
        return;
    }

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");
    test_body(pool).await.expect("harness body");
}
