//! M9.8cw differential harness: the exhaustive search janitor
//! (`SearchJobsJanitor` / `runJanitor`).
//!
//! Mirrors `cmd/worker/internal/search/janitor_harness_test.go`. Both sides
//! seed identical rows in the scratch frontend database, run the janitor
//! against the same in-memory blobstore, and print one canonical `exjn ` JSON
//! line per case: the run error plus the resulting job rows, cascade counts,
//! uploaded CSV contents and blobstore deletes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sg_service_worker::jobs::exhaustive_search_janitor::{
    JobLogsBlobStore, PgJanitorService, SearchJobsJanitor,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

#[derive(Default)]
struct FakeBlobs {
    uploads: Mutex<BTreeMap<String, String>>,
    upload_order: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    fail_uploads: Mutex<Vec<String>>,
}

impl FakeBlobs {
    fn fail_upload(&self, key: &str) {
        self.fail_uploads
            .lock()
            .expect("fail lock")
            .push(key.to_owned());
    }
}

#[async_trait]
impl JobLogsBlobStore for FakeBlobs {
    async fn upload(&self, key: &str, contents: Vec<u8>) -> Result<i64, String> {
        if self
            .fail_uploads
            .lock()
            .expect("fail lock")
            .iter()
            .any(|failed| failed == key)
        {
            return Err(format!("upload {key}: boom"));
        }
        let contents = String::from_utf8(contents).map_err(|err| err.to_string())?;
        let len = contents.len() as i64;
        self.uploads
            .lock()
            .expect("uploads lock")
            .insert(key.to_owned(), contents);
        self.upload_order
            .lock()
            .expect("order lock")
            .push(key.to_owned());
        Ok(len)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.deletes
            .lock()
            .expect("deletes lock")
            .push(key.to_owned());
        self.uploads.lock().expect("uploads lock").remove(key);
        Ok(())
    }
}

fn emit(case: &str, error: Option<String>, output: Value) {
    let line = json!({
        "case": case,
        "error": error.unwrap_or_else(|| "-".to_owned()),
        "output": output,
    });
    println!("exjn {}", serde_json::to_string(&line).expect("encode"));
}

async fn seed(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    for statement in [
        "TRUNCATE TABLE exhaustive_search_repo_revision_jobs, exhaustive_search_repo_jobs,
            exhaustive_search_jobs RESTART IDENTITY CASCADE",
        "DELETE FROM users WHERE id IN (101, 102)",
        "DELETE FROM repo WHERE id IN (201, 202)",
        "INSERT INTO users (id, username, display_name, site_admin, deleted_at)
            VALUES
                (101, 'exjn-user', 'User', FALSE, NULL),
                (102, 'exjn-other', 'Other', FALSE, NULL)",
        "INSERT INTO repo (id, name) VALUES
            (201, 'github.com/acme/alpha'), (202, 'github.com/acme/beta')",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

async fn exec(pool: &sqlx::PgPool, statement: &str) -> Result<(), sqlx::Error> {
    sqlx::query(statement).execute(pool).await.map(|_| ())
}

async fn snapshot(pool: &sqlx::PgPool, blobs: &FakeBlobs) -> Result<Value, sqlx::Error> {
    let rows =
        sqlx::query("SELECT id, state, is_aggregated FROM exhaustive_search_jobs ORDER BY id")
            .fetch_all(pool)
            .await?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        jobs.push(json!({
            "id": row.try_get::<i32, _>("id")?,
            "state": row.try_get::<String, _>("state")?,
            "is_aggregated": row.try_get::<bool, _>("is_aggregated")?,
        }));
    }

    let repo_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exhaustive_search_repo_jobs")
        .fetch_one(pool)
        .await?;
    let rev_jobs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM exhaustive_search_repo_revision_jobs")
            .fetch_one(pool)
            .await?;

    let uploads_guard = blobs.uploads.lock().expect("uploads lock");
    let order = blobs.upload_order.lock().expect("order lock");
    let uploads: Vec<Value> = order
        .iter()
        .map(|key| {
            json!({"key": key, "content": uploads_guard.get(key).cloned().unwrap_or_default()})
        })
        .collect();
    drop(uploads_guard);
    drop(order);

    let deletes = blobs.deletes.lock().expect("deletes lock");

    Ok(json!({
        "jobs": jobs,
        "repo_jobs": repo_jobs,
        "rev_jobs": rev_jobs,
        "uploads": uploads,
        "deletes": deletes.clone(),
    }))
}

async fn run_case(pool: &sqlx::PgPool, blobs: &Arc<FakeBlobs>) -> Option<String> {
    let blobs: Arc<dyn JobLogsBlobStore> = blobs.clone();
    let service = PgJanitorService::new(pool.clone(), blobs);
    let janitor = SearchJobsJanitor::new(pool.clone(), Arc::new(service));
    janitor.run().await.err()
}

async fn test_body(pool: sqlx::PgPool) -> Result<(), String> {
    let sql = |err: sqlx::Error| err.to_string();

    // 1. All rows completed: state update, log upload, cascade delete.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state, started_at, finished_at)
            VALUES
                (101, 11, 'rev1', 'completed', '2026-01-02 03:04:05+00', '2026-01-02 03:05:06+00'),
                (102, 11, 'rev2', 'completed', '2026-01-02 03:04:05+00', '2026-01-02 03:05:06+00')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "all-completed",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 2. One revision job failed: aggregate state is failed.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state, failure_message, started_at, finished_at)
            VALUES
                (101, 11, 'rev1', 'failed', 'boom',
                 '2026-01-02 03:04:05+00', '2026-01-02 03:05:06+00'),
                (102, 11, 'rev2', 'completed', '', NULL, NULL)",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "one-failed",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 3. Errored is retried, so the aggregate state is processing: untouched.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state) VALUES (101, 11, 'rev1', 'errored')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "still-processing",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 4. A canceled child makes the aggregate state canceled.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state) VALUES (101, 11, 'rev1', 'canceled')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit("canceled", err, snapshot(&pool, &blobs).await.map_err(sql)?);

    // 5. A job that has not started yet stays queued (not terminal).
    seed(&pool).await.map_err(sql)?;
    exec(
        &pool,
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
         VALUES (1, 101, 'q', 'queued')",
    )
    .await
    .map_err(sql)?;
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "queued-job",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 6. Only the terminal job of several unaggregated jobs is processed.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES
                (1, 101, 'done', 'completed'),
                (2, 102, 'busy', 'processing')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES
                (11, 201, 'spec', 1, 'completed'),
                (21, 202, 'spec', 2, 'processing')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state)
            VALUES
                (101, 11, 'rev1', 'completed'),
                (201, 21, 'rev1', 'processing')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "multiple-jobs",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 7. A failed upload rolls the transaction back and deletes the logs.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state, failure_message)
            VALUES (101, 11, 'rev1', 'failed', 'boom')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    blobs.fail_upload("log-1.csv");
    let err = run_case(&pool, &blobs).await;
    emit(
        "upload-failure",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 8. Two failing jobs render like errors.Append's MultiError.
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES
                (1, 101, 'q1', 'completed'),
                (2, 102, 'q2', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES
                (11, 201, 'spec', 1, 'completed'),
                (21, 202, 'spec', 2, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state, failure_message)
            VALUES
                (101, 11, 'rev1', 'failed', 'boom'),
                (201, 21, 'rev1', 'failed', 'boom')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    blobs.fail_upload("log-1.csv");
    blobs.fail_upload("log-2.csv");
    let err = run_case(&pool, &blobs).await;
    emit(
        "multiple-errors",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 9. CSV escaping matches encoding/csv (comma, quote, newline, leading
    //    space, carriage return).
    seed(&pool).await.map_err(sql)?;
    for statement in [
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
            VALUES (1, 101, 'q', 'completed')",
        "INSERT INTO exhaustive_search_repo_jobs (id, repo_id, ref_spec, search_job_id, state)
            VALUES (11, 201, 'spec', 1, 'completed')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, search_repo_job_id, revision, state, failure_message)
            VALUES
                (101, 11, 'rev1', 'failed', 'bad, \"quote\"' || chr(10) || 'line'),
                (102, 11, 'rev2', 'failed', ' leading'),
                (103, 11, 'rev3', 'failed', 'carriage' || chr(13) || 'return')",
    ] {
        exec(&pool, statement).await.map_err(sql)?;
    }
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "csv-escaping",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    // 10. A terminal job with no revision jobs uploads the header only.
    seed(&pool).await.map_err(sql)?;
    exec(
        &pool,
        "INSERT INTO exhaustive_search_jobs (id, initiator_id, query, state)
         VALUES (1, 101, 'q', 'completed')",
    )
    .await
    .map_err(sql)?;
    let blobs = Arc::new(FakeBlobs::default());
    let err = run_case(&pool, &blobs).await;
    emit(
        "empty-logs",
        err,
        snapshot(&pool, &blobs).await.map_err(sql)?,
    );

    Ok(())
}

#[tokio::test]
async fn exhaustive_janitor_harness() {
    let Ok(dsn) = std::env::var("SG_EXHAUSTIVE_JANITOR_TEST_DSN") else {
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
