#![forbid(unsafe_code)]

//! M9.8cu differential harness (Rust side): the three exhaustive search
//! dbworker stores in `sg_db::exhaustive_search`.
//!
//! Mirrors `internal/search/exhaustive/store/exh_workers_harness_test.go`.
//! Both sides re-seed identical rows in the scratch frontend database, then run
//! the same ordered set of generic-store calls (dequeue, heartbeat,
//! mark_complete, mark_errored, mark_failed, reset_stalled). Each case prints
//! one canonical `exhw ` JSON line: the store result plus normalized row
//! projections (timestamps as `T`/`-`; reset ages bucketed to whole minutes
//! because the stores read the wall clock).
//!
//! Only runs when `SG_EXHAUSTIVE_WORKERS_TEST_DSN` names the scratch database.
//! `test_ws/p9-go/run_exhaustive_search_workers_diff.sh` compares the two
//! outputs line by line.

use serde_json::{json, Value};
use sg_db::exhaustive_search as ex;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;
use std::time::Duration;
use time::OffsetDateTime;

fn emit(case: &str, error: Option<String>, output: Value) {
    let line = json!({
        "case": case,
        "error": error.unwrap_or_else(|| "-".to_owned()),
        "output": output,
    });
    println!("exhw {line}");
}

fn time_flag(time: Option<OffsetDateTime>) -> &'static str {
    match time {
        Some(_) => "T",
        None => "-",
    }
}

fn worker_json(worker: &ex::WorkerJob) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    out.insert("cancel".into(), json!(worker.cancel));
    out.insert("failure_message".into(), json!(worker.failure_message));
    out.insert("finished_at".into(), json!(time_flag(worker.finished_at)));
    out.insert("num_failures".into(), json!(worker.num_failures));
    out.insert("num_resets".into(), json!(worker.num_resets));
    out.insert(
        "process_after".into(),
        json!(time_flag(worker.process_after)),
    );
    out.insert("started_at".into(), json!(time_flag(worker.started_at)));
    out.insert("state".into(), json!(worker.state.as_str()));
    out.insert("worker_hostname".into(), json!(worker.worker_hostname));
    out
}

fn job_json(job: &ex::ExhaustiveSearchJob) -> Value {
    let mut out = worker_json(&job.worker);
    out.insert("id".into(), json!(job.id));
    out.insert("initiator_id".into(), json!(job.initiator_id));
    out.insert("is_aggregated".into(), json!(job.is_aggregated));
    out.insert("query".into(), json!(job.query));
    Value::Object(out)
}

fn repo_json(job: &ex::ExhaustiveSearchRepoJob) -> Value {
    let mut out = worker_json(&job.worker);
    out.insert("id".into(), json!(job.id));
    out.insert("ref_spec".into(), json!(job.ref_spec));
    out.insert("repo_id".into(), json!(job.repo_id));
    out.insert("search_job_id".into(), json!(job.search_job_id));
    Value::Object(out)
}

fn rev_json(job: &ex::ExhaustiveSearchRepoRevisionJob) -> Value {
    let mut out = worker_json(&job.worker);
    out.insert("id".into(), json!(job.id));
    out.insert("revision".into(), json!(job.revision));
    out.insert("search_repo_job_id".into(), json!(job.search_repo_job_id));
    Value::Object(out)
}

fn ages_json(ages: &[(i32, Duration)]) -> Value {
    let mut sorted = ages.to_vec();
    sorted.sort_by_key(|(id, _)| *id);
    Value::Array(
        sorted
            .into_iter()
            .map(|(id, age)| {
                json!({
                    "age_minutes": (age.as_secs() / 60) as i64,
                    "id": id,
                })
            })
            .collect(),
    )
}

async fn seed(pool: &PgPool) {
    for query in [
        "TRUNCATE TABLE exhaustive_search_jobs RESTART IDENTITY CASCADE",
        "DELETE FROM users WHERE id IN (101)",
        "DELETE FROM repo WHERE id IN (201)",
        "INSERT INTO users (id, username, display_name, site_admin, deleted_at)
            VALUES (101, 'exhw-admin', 'Admin', TRUE, NULL)",
        "INSERT INTO repo (id, name) VALUES (201, 'github.com/acme/alpha')",
        "INSERT INTO exhaustive_search_jobs
            (id, initiator_id, query, state, failure_message, started_at, finished_at,
             process_after, num_resets, num_failures, last_heartbeat_at, worker_hostname,
             cancel, created_at, updated_at)
         VALUES
            (1, 101, 'q-1', 'queued', NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-01'),
            (2, 101, 'q-2', 'queued', NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-05'),
            (3, 101, 'q-3', 'queued', NULL, NULL, NULL, now() + interval '1 hour', 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-06'),
            (5, 101, 'q-5', 'queued', NULL, NULL, NULL, now() - interval '1 hour', 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-02'),
            (6, 101, 'q-6', 'failed', NULL, '2026-01-01', '2026-01-02', NULL, 0, 1, NULL, 'seed-host', false, '2026-01-01', '2026-01-07'),
            (7, 101, 'q-7', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-01-08'),
            (8, 101, 'q-8', 'errored', NULL, '2026-01-01', now() - interval '10 seconds', NULL, 0, 3, NULL, 'seed-host', false, '2026-01-01', '2026-01-09'),
            (9, 101, 'q-9', 'errored', NULL, '2026-01-01', now() - interval '10 seconds', NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-04'),
            (10, 101, 'q-10', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', true, '2026-01-01', '2026-01-10'),
            (11, 101, 'q-11', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 2, now(), 'seed-host', false, '2026-01-01', '2026-01-11'),
            (12, 101, 'q-12', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', false, '2026-01-01', '2026-01-12'),
            (13, 101, 'q-13', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', true, '2026-01-01', '2026-01-13'),
            (21, 101, 'q-21', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-01-21'),
            (22, 101, 'q-22', 'processing', NULL, '2026-01-01', NULL, NULL, 5, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-01-22'),
            (23, 101, 'q-23', 'processing', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', false, '2026-01-01', '2026-01-23'),
            (24, 101, 'q-24', 'queued', NULL, NULL, NULL, now() + interval '1 hour', 0, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-01-24')",
        "INSERT INTO exhaustive_search_repo_jobs
            (id, state, repo_id, ref_spec, search_job_id, failure_message, started_at,
             finished_at, process_after, num_resets, num_failures, last_heartbeat_at,
             worker_hostname, cancel, created_at, updated_at)
         VALUES
            (31, 'queued', 201, 'HEAD', 1, NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-01'),
            (32, 'queued', 201, 'HEAD', 1, NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-05'),
            (33, 'queued', 201, 'HEAD', 1, NULL, NULL, NULL, now() + interval '1 hour', 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-06'),
            (34, 'processing', 201, 'HEAD', 1, NULL, '2026-01-01', NULL, NULL, 0, 2, now(), 'seed-host', false, '2026-01-01', '2026-02-04'),
            (35, 'processing', 201, 'HEAD', 1, NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', false, '2026-01-01', '2026-02-05'),
            (36, 'processing', 201, 'HEAD', 1, NULL, '2026-01-01', NULL, NULL, 0, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-02-06'),
            (37, 'processing', 201, 'HEAD', 1, NULL, '2026-01-01', NULL, NULL, 5, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-02-07')",
        "INSERT INTO exhaustive_search_repo_revision_jobs
            (id, state, search_repo_job_id, revision, failure_message, started_at,
             finished_at, process_after, num_resets, num_failures, last_heartbeat_at,
             worker_hostname, cancel, created_at, updated_at)
         VALUES
            (41, 'queued', 31, 'v1', NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-01'),
            (42, 'queued', 31, 'v2', NULL, NULL, NULL, NULL, 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-05'),
            (43, 'queued', 31, 'v3', NULL, NULL, NULL, now() + interval '1 hour', 0, 0, NULL, 'seed-host', false, '2026-01-01', '2026-01-06'),
            (44, 'processing', 31, 'v4', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', false, '2026-01-01', '2026-02-04'),
            (45, 'processing', 31, 'v5', NULL, '2026-01-01', NULL, NULL, 5, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-02-05'),
            (46, 'processing', 31, 'v6', NULL, '2026-01-01', NULL, NULL, 0, 0, now(), 'seed-host', false, '2026-01-01', '2026-02-06'),
            (47, 'processing', 31, 'v7', NULL, '2026-01-01', NULL, NULL, 0, 0, now() - interval '10 minutes', 'seed-host', false, '2026-01-01', '2026-02-07')",
    ] {
        sqlx::query(query).execute(pool).await.expect("seed reset");
    }
}

const JOB_SELECT: &str = "
SELECT id, initiator_id, state, query, failure_message, started_at, finished_at,
       process_after, num_resets, num_failures, worker_hostname, cancel,
       created_at, updated_at, is_aggregated
FROM exhaustive_search_jobs WHERE id = $1";

fn full_job_json(row: &PgRow) -> Value {
    json!({
        "cancel": row.try_get::<bool, _>("cancel").expect("cancel"),
        "created_at": time_flag(Some(row.try_get("created_at").expect("created_at"))),
        "failure_message": row
            .try_get::<Option<String>, _>("failure_message")
            .expect("failure_message")
            .unwrap_or_default(),
        "finished_at": time_flag(row.try_get("finished_at").expect("finished_at")),
        "id": i64::from(row.try_get::<i32, _>("id").expect("id")),
        "initiator_id": row.try_get::<i32, _>("initiator_id").expect("initiator_id"),
        "is_aggregated": row.try_get::<bool, _>("is_aggregated").expect("is_aggregated"),
        "num_failures": i64::from(row.try_get::<i32, _>("num_failures").expect("num_failures")),
        "num_resets": i64::from(row.try_get::<i32, _>("num_resets").expect("num_resets")),
        "process_after": time_flag(row.try_get("process_after").expect("process_after")),
        "query": row.try_get::<String, _>("query").expect("query"),
        "started_at": time_flag(row.try_get("started_at").expect("started_at")),
        "state": row.try_get::<String, _>("state").expect("state"),
        "updated_at": time_flag(Some(row.try_get("updated_at").expect("updated_at"))),
        "worker_hostname": row.try_get::<String, _>("worker_hostname").expect("worker_hostname"),
    })
}

async fn job_rows(pool: &PgPool, ids: &[i64]) -> Value {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    let mut out = Vec::new();
    for id in sorted {
        let row = sqlx::query(JOB_SELECT)
            .bind(id)
            .fetch_optional(pool)
            .await
            .expect("job row");
        if let Some(row) = row {
            out.push(full_job_json(&row));
        }
    }
    Value::Array(out)
}

async fn repo_rows(pool: &PgPool, search_job_id: i64) -> Value {
    let rows = sqlx::query(
        "
SELECT id, search_job_id, repo_id, ref_spec, state, failure_message, started_at,
       finished_at, process_after, num_resets, num_failures, worker_hostname, cancel
FROM exhaustive_search_repo_jobs WHERE search_job_id = $1 ORDER BY id",
    )
    .bind(search_job_id)
    .fetch_all(pool)
    .await
    .expect("repo rows");
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "cancel": row.try_get::<bool, _>("cancel").expect("cancel"),
                    "failure_message": row
                        .try_get::<Option<String>, _>("failure_message")
                        .expect("failure_message")
                        .unwrap_or_default(),
                    "finished_at": time_flag(row.try_get("finished_at").expect("finished_at")),
                    "id": i64::from(row.try_get::<i32, _>("id").expect("id")),
                    "num_failures": i64::from(row.try_get::<i32, _>("num_failures").expect("num_failures")),
                    "num_resets": i64::from(row.try_get::<i32, _>("num_resets").expect("num_resets")),
                    "process_after": time_flag(row.try_get("process_after").expect("process_after")),
                    "ref_spec": row.try_get::<String, _>("ref_spec").expect("ref_spec"),
                    "repo_id": row.try_get::<i32, _>("repo_id").expect("repo_id"),
                    "search_job_id": i64::from(row.try_get::<i32, _>("search_job_id").expect("search_job_id")),
                    "started_at": time_flag(row.try_get("started_at").expect("started_at")),
                    "state": row.try_get::<String, _>("state").expect("state"),
                    "worker_hostname": row.try_get::<String, _>("worker_hostname").expect("worker_hostname"),
                })
            })
            .collect(),
    )
}

async fn rev_rows(pool: &PgPool, search_repo_job_id: i64) -> Value {
    let rows = sqlx::query(
        "
SELECT id, search_repo_job_id, revision, state, failure_message, started_at,
       finished_at, process_after, num_resets, num_failures, worker_hostname, cancel
FROM exhaustive_search_repo_revision_jobs WHERE search_repo_job_id = $1 ORDER BY id",
    )
    .bind(search_repo_job_id)
    .fetch_all(pool)
    .await
    .expect("rev rows");
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "cancel": row.try_get::<bool, _>("cancel").expect("cancel"),
                    "failure_message": row
                        .try_get::<Option<String>, _>("failure_message")
                        .expect("failure_message")
                        .unwrap_or_default(),
                    "finished_at": time_flag(row.try_get("finished_at").expect("finished_at")),
                    "id": i64::from(row.try_get::<i32, _>("id").expect("id")),
                    "num_failures": i64::from(row.try_get::<i32, _>("num_failures").expect("num_failures")),
                    "num_resets": i64::from(row.try_get::<i32, _>("num_resets").expect("num_resets")),
                    "process_after": time_flag(row.try_get("process_after").expect("process_after")),
                    "revision": row.try_get::<String, _>("revision").expect("revision"),
                    "search_repo_job_id": i64::from(row.try_get::<i32, _>("search_repo_job_id").expect("search_repo_job_id")),
                    "started_at": time_flag(row.try_get("started_at").expect("started_at")),
                    "state": row.try_get::<String, _>("state").expect("state"),
                    "worker_hostname": row.try_get::<String, _>("worker_hostname").expect("worker_hostname"),
                })
            })
            .collect(),
    )
}

fn error_text<T>(result: Result<T, String>) -> (Option<String>, Option<T>) {
    match result {
        Ok(value) => (None, Some(value)),
        Err(error) => (Some(error), None),
    }
}

#[tokio::test]
async fn exhaustive_workers_harness() {
    let Ok(dsn) = std::env::var("SG_EXHAUSTIVE_WORKERS_TEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");
    seed(&pool).await;

    let job_store = ex::ExhaustiveSearchJobWorkerStore::new(pool.clone());
    let repo_store = ex::ExhaustiveSearchRepoJobWorkerStore::new(pool.clone());
    let rev_store = ex::ExhaustiveSearchRepoRevisionJobWorkerStore::new(pool.clone());

    const WORKER_HOSTNAME: &str = "harness-worker";

    for case in [
        "job-dequeue-1",
        "job-dequeue-2",
        "job-dequeue-3",
        "job-dequeue-4",
        "job-dequeue-empty",
    ] {
        let (error, record) = error_text(
            job_store
                .dequeue(WORKER_HOSTNAME, OffsetDateTime::now_utc())
                .await,
        );
        let output = match record.flatten() {
            Some(record) => json!({
                "record": job_json(&record),
                "rows": job_rows(&pool, &[record.id]).await,
            }),
            None => Value::Null,
        };
        emit(case, error, output);
    }

    let (error, result) = error_text(
        job_store
            .heartbeat(&[2, 7, 9, 10, 999], OffsetDateTime::now_utc())
            .await,
    );
    match result {
        Some((known, canceled)) => emit(
            "job-heartbeat",
            error,
            json!({"canceled": canceled, "known": known}),
        ),
        None => emit("job-heartbeat", error, Value::Null),
    }

    let (error, result) = error_text(job_store.heartbeat(&[], OffsetDateTime::now_utc()).await);
    match result {
        Some((known, canceled)) => emit(
            "job-heartbeat-empty",
            error,
            json!({"canceled": canceled, "known": known}),
        ),
        None => emit("job-heartbeat-empty", error, Value::Null),
    }

    for (case, id, kind) in [
        ("job-mark-complete", 2, "complete"),
        ("job-mark-complete-queued", 3, "complete"),
        ("job-mark-errored", 7, "errored"),
        ("job-mark-errored-max", 11, "errored"),
        ("job-mark-errored-cancel", 10, "errored"),
        ("job-mark-failed", 12, "failed"),
        ("job-mark-failed-cancel", 13, "failed"),
        ("job-mark-missing", 999, "complete"),
    ] {
        let (error, updated) = error_text(match kind {
            "complete" => job_store.mark_complete(id).await,
            "errored" => job_store.mark_errored(id, "boom").await,
            _ => job_store.mark_failed(id, "boom").await,
        });
        emit(
            case,
            error,
            json!({"rows": job_rows(&pool, &[i64::from(id)]).await, "updated": updated}),
        );
    }

    let (error, result) = error_text(job_store.reset_stalled_at(OffsetDateTime::now_utc()).await);
    match result {
        Some((reset, failed)) => emit(
            "job-reset-stalled",
            error,
            json!({
                "failed": ages_json(&failed),
                "reset": ages_json(&reset),
                "rows": job_rows(&pool, &[21, 22, 23, 24]).await,
            }),
        ),
        None => emit("job-reset-stalled", error, Value::Null),
    }

    for case in ["repo-dequeue-1", "repo-dequeue-2", "repo-dequeue-empty"] {
        let (error, record) = error_text(
            repo_store
                .dequeue(WORKER_HOSTNAME, OffsetDateTime::now_utc())
                .await,
        );
        let output = match record.flatten() {
            Some(record) => json!({
                "record": repo_json(&record),
                "rows": repo_rows(&pool, record.search_job_id).await,
            }),
            None => Value::Null,
        };
        emit(case, error, output);
    }

    let (error, result) = error_text(
        repo_store
            .heartbeat(&[32, 34, 35, 999], OffsetDateTime::now_utc())
            .await,
    );
    match result {
        Some((known, canceled)) => emit(
            "repo-heartbeat",
            error,
            json!({"canceled": canceled, "known": known}),
        ),
        None => emit("repo-heartbeat", error, Value::Null),
    }

    for (case, id, kind) in [
        ("repo-mark-complete", 32, "complete"),
        ("repo-mark-errored", 34, "errored"),
        ("repo-mark-failed", 35, "failed"),
    ] {
        let (error, updated) = error_text(match kind {
            "complete" => repo_store.mark_complete(id).await,
            "errored" => repo_store.mark_errored(id, "boom").await,
            _ => repo_store.mark_failed(id, "boom").await,
        });
        emit(
            case,
            error,
            json!({"rows": repo_rows(&pool, 1).await, "updated": updated}),
        );
    }

    let (error, result) = error_text(repo_store.reset_stalled_at(OffsetDateTime::now_utc()).await);
    match result {
        Some((reset, failed)) => emit(
            "repo-reset-stalled",
            error,
            json!({
                "failed": ages_json(&failed),
                "reset": ages_json(&reset),
                "rows": repo_rows(&pool, 1).await,
            }),
        ),
        None => emit("repo-reset-stalled", error, Value::Null),
    }

    for case in ["rev-dequeue-1", "rev-dequeue-2", "rev-dequeue-empty"] {
        let (error, record) = error_text(
            rev_store
                .dequeue(WORKER_HOSTNAME, OffsetDateTime::now_utc())
                .await,
        );
        let output = match record.flatten() {
            Some(record) => json!({
                "record": rev_json(&record),
                "rows": rev_rows(&pool, record.search_repo_job_id).await,
            }),
            None => Value::Null,
        };
        emit(case, error, output);
    }

    let (error, result) = error_text(
        rev_store
            .heartbeat(&[42, 44, 46, 999], OffsetDateTime::now_utc())
            .await,
    );
    match result {
        Some((known, canceled)) => emit(
            "rev-heartbeat",
            error,
            json!({"canceled": canceled, "known": known}),
        ),
        None => emit("rev-heartbeat", error, Value::Null),
    }

    for (case, id, kind) in [
        ("rev-mark-complete", 42, "complete"),
        ("rev-mark-errored", 44, "errored"),
        ("rev-mark-failed", 46, "failed"),
    ] {
        let (error, updated) = error_text(match kind {
            "complete" => rev_store.mark_complete(id).await,
            "errored" => rev_store.mark_errored(id, "boom").await,
            _ => rev_store.mark_failed(id, "boom").await,
        });
        emit(
            case,
            error,
            json!({"rows": rev_rows(&pool, 31).await, "updated": updated}),
        );
    }

    let (error, result) = error_text(rev_store.reset_stalled_at(OffsetDateTime::now_utc()).await);
    match result {
        Some((reset, failed)) => emit(
            "rev-reset-stalled",
            error,
            json!({
                "failed": ages_json(&failed),
                "reset": ages_json(&reset),
                "rows": rev_rows(&pool, 31).await,
            }),
        ),
        None => emit("rev-reset-stalled", error, Value::Null),
    }
}
