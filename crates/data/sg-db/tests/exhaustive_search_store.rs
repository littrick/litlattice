#![forbid(unsafe_code)]

//! M9.8ct differential harness (Rust side): the exhaustive search job store
//! (`sg_db::exhaustive_search`, ported from
//! `internal/search/exhaustive/store`).
//!
//! Mirrors `internal/search/exhaustive/store/exh_store_harness_test.go`.
//! Both sides re-seed the same users / repo / job rows in the scratch frontend
//! database, then run the same ordered set of store calls. Each case prints
//! one canonical `exh ` JSON line: the baseline error string plus a normalized
//! projection of the rows it produced (timestamps as `T`/`-`, list results
//! sorted by id).
//!
//! Only runs when `SG_EXHAUSTIVE_STORE_TEST_DSN` names the scratch database.
//! `test_ws/p9-go/run_exhaustive_search_store_diff.sh` compares the two
//! outputs line by line.

use serde_json::{json, Value};
use sg_db::exhaustive_search as ex;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn emit(case: &str, error: Option<String>, output: Value) {
    let line = json!({
        "case": case,
        "error": error.unwrap_or_else(|| "-".to_owned()),
        "output": output,
    });
    println!("exh {line}");
}

fn time_flag(time: Option<OffsetDateTime>) -> &'static str {
    match time {
        Some(_) => "T",
        None => "-",
    }
}

fn fixed(spec: &str) -> OffsetDateTime {
    OffsetDateTime::parse(spec, &Rfc3339).expect("parse fixed timestamp")
}

async fn seed(pool: &PgPool) {
    for query in [
        "TRUNCATE TABLE exhaustive_search_jobs RESTART IDENTITY CASCADE",
        "DELETE FROM users WHERE id IN (101, 102, 103)",
        "DELETE FROM repo WHERE id IN (201, 202)",
    ] {
        sqlx::query(query).execute(pool).await.expect("seed reset");
    }
    sqlx::query(
        "INSERT INTO users (id, username, display_name, site_admin, deleted_at) VALUES
            (101, 'exh-admin', 'Admin', TRUE, NULL),
            (102, 'exh-user', 'User', FALSE, NULL),
            (103, 'exh-deleted', 'Deleted', FALSE, now())",
    )
    .execute(pool)
    .await
    .expect("seed users");
    sqlx::query(
        "INSERT INTO repo (id, name) VALUES
            (201, 'github.com/acme/alpha'),
            (202, 'github.com/acme/beta')",
    )
    .execute(pool)
    .await
    .expect("seed repos");
    for sequence in [
        "exhaustive_search_jobs_id_seq",
        "exhaustive_search_repo_jobs_id_seq",
        "exhaustive_search_repo_revision_jobs_id_seq",
    ] {
        sqlx::query(&format!("SELECT setval('{sequence}', 100)"))
            .execute(pool)
            .await
            .expect("setval");
    }

    insert_job(pool, 1, 102, "type:commit foo", "completed", false).await;
    insert_repo_job(pool, 11, 1, 201, "HEAD", "completed").await;
    insert_repo_job(pool, 12, 1, 202, "refs/heads/main", "errored").await;
    insert_rev_job(pool, 111, 11, "v1", "completed").await;
    insert_rev_job(pool, 112, 11, "v2", "failed").await;

    insert_job(pool, 2, 102, "type:diff bar", "failed", false).await;
    insert_repo_job(pool, 21, 2, 201, "HEAD", "queued").await;

    insert_job(pool, 3, 101, "type:commit baz", "queued", false).await;

    insert_job(pool, 4, 101, "type:commit qux", "processing", true).await;
    insert_repo_job(pool, 41, 4, 202, "HEAD", "processing").await;
    insert_rev_job(pool, 411, 41, "HEAD", "queued").await;

    insert_job(pool, 6, 102, "type:commit processing", "processing", false).await;
    insert_repo_job(pool, 61, 6, 201, "HEAD", "processing").await;
    insert_rev_job(pool, 611, 61, "HEAD", "queued").await;

    insert_job(pool, 7, 102, "type:commit queued", "queued", false).await;
    insert_repo_job(pool, 71, 7, 202, "HEAD", "queued").await;
    insert_rev_job(pool, 711, 71, "HEAD", "queued").await;
}

async fn insert_job(
    pool: &PgPool,
    id: i64,
    initiator: i32,
    query: &str,
    state: &str,
    cancel: bool,
) {
    sqlx::query(
        "
INSERT INTO exhaustive_search_jobs
  (id, initiator_id, query, state, cancel, started_at, finished_at, num_resets, num_failures,
   worker_hostname, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5,
        CASE WHEN $4 IN ('queued') THEN NULL ELSE '2026-01-01T00:00:00Z'::timestamptz END,
        CASE WHEN $4 IN ('queued', 'processing', 'errored') THEN NULL ELSE '2026-01-01T00:10:00Z'::timestamptz END,
        0, 0, 'seed-host', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    )
    .bind(id)
    .bind(initiator)
    .bind(query)
    .bind(state)
    .bind(cancel)
    .execute(pool)
    .await
    .expect("insert job");
}

async fn insert_repo_job(
    pool: &PgPool,
    id: i64,
    search_job_id: i64,
    repo_id: i32,
    ref_spec: &str,
    state: &str,
) {
    sqlx::query(
        "
INSERT INTO exhaustive_search_repo_jobs
  (id, search_job_id, repo_id, ref_spec, state, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    )
    .bind(id)
    .bind(search_job_id)
    .bind(repo_id)
    .bind(ref_spec)
    .bind(state)
    .execute(pool)
    .await
    .expect("insert repo job");
}

async fn insert_rev_job(
    pool: &PgPool,
    id: i64,
    search_repo_job_id: i64,
    revision: &str,
    state: &str,
) {
    sqlx::query(
        "
INSERT INTO exhaustive_search_repo_revision_jobs
  (id, search_repo_job_id, revision, state, failure_message, created_at, updated_at)
VALUES ($1, $2, $3, $4,
        CASE WHEN $4 = 'failed' THEN 'revision failed' ELSE NULL END,
        '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    )
    .bind(id)
    .bind(search_repo_job_id)
    .bind(revision)
    .bind(state)
    .execute(pool)
    .await
    .expect("insert rev job");
}

fn list_job_json(job: &ex::ExhaustiveSearchJob) -> Value {
    json!({
        "agg_state": job.agg_state.as_str(),
        "cancel": job.worker.cancel,
        "failure_message": job.worker.failure_message,
        "finished_at": time_flag(job.worker.finished_at),
        "id": job.id,
        "initiator_id": job.initiator_id,
        "is_aggregated": job.is_aggregated,
        "num_failures": job.worker.num_failures,
        "num_resets": job.worker.num_resets,
        "process_after": time_flag(job.worker.process_after),
        "query": job.query,
        "started_at": time_flag(job.worker.started_at),
        "state": job.worker.state.as_str(),
        "worker_hostname": job.worker.worker_hostname,
    })
}

const JOB_SELECT: &str = "
SELECT id, initiator_id, state, query, failure_message, started_at, finished_at,
       process_after, num_resets, num_failures, worker_hostname, cancel,
       created_at, updated_at, is_aggregated
FROM exhaustive_search_jobs WHERE id = $1";

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

async fn job_ids(pool: &PgPool) -> Value {
    let ids = sqlx::query_scalar::<_, i32>("SELECT id FROM exhaustive_search_jobs ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("job ids");
    Value::Array(ids.into_iter().map(|id| json!(i64::from(id))).collect())
}

fn ids_of(jobs: &[ex::ExhaustiveSearchJob]) -> Value {
    let mut ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    ids.sort_unstable();
    Value::Array(ids.into_iter().map(Value::from).collect())
}

fn summaries(jobs: &[ex::ExhaustiveSearchJob]) -> Value {
    let mut sorted: Vec<&ex::ExhaustiveSearchJob> = jobs.iter().collect();
    sorted.sort_by_key(|job| job.id);
    Value::Array(sorted.iter().map(|job| list_job_json(job)).collect())
}

fn logs_json(logs: &[ex::SearchJobLog]) -> Value {
    Value::Array(
        logs.iter()
            .map(|log| {
                json!({
                    "failure_message": log.failure_message,
                    "finished_at": time_flag(log.finished_at),
                    "id": log.id,
                    "repo_name": log.repo_name,
                    "revision": log.revision,
                    "started_at": time_flag(log.started_at),
                    "state": log.state.as_str(),
                })
            })
            .collect(),
    )
}

fn error_text<T>(result: Result<T, ex::Error>) -> (Option<String>, Option<T>) {
    match result {
        Ok(value) => (None, Some(value)),
        Err(err) => (Some(err.to_string()), None),
    }
}

#[tokio::test]
async fn exhaustive_search_store_harness() {
    let Ok(dsn) = std::env::var("SG_EXHAUSTIVE_STORE_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");
    seed(&pool).await;

    let user = |uid: i32| ex::Actor::user(uid);
    let internal = ex::Actor::internal();
    let now = fixed("2026-02-01T00:00:00Z");

    // ---- creation ------------------------------------------------------
    let (error, id) = error_text(
        ex::create_exhaustive_search_job(&pool, user(102), "type:commit new", 102).await,
    );
    let id = id.unwrap_or(0);
    emit(
        "create-ok",
        error.clone(),
        json!({"id": id, "rows": job_rows(&pool, &[id]).await}),
    );

    let (error, _) = error_text(ex::create_exhaustive_search_job(&pool, user(102), "", 102).await);
    emit("create-missing-query", error, Value::Null);

    let (error, _) = error_text(ex::create_exhaustive_search_job(&pool, user(102), "x", 0).await);
    emit("create-missing-initiator", error, Value::Null);

    let (error, _) = error_text(
        ex::create_exhaustive_search_job(&pool, user(102), "type:commit forbidden", 101).await,
    );
    emit("create-forbidden", error, Value::Null);

    let (error, admin_id) = error_text(
        ex::create_exhaustive_search_job(&pool, user(101), "type:commit admin", 102).await,
    );
    let admin_id = admin_id.unwrap_or(0);
    emit(
        "create-admin",
        error,
        json!({"id": admin_id, "rows": job_rows(&pool, &[admin_id]).await}),
    );

    let (error, _) = error_text(
        ex::create_exhaustive_search_job(&pool, user(103), "type:commit unknown-actor", 102).await,
    );
    emit("create-unknown-actor", error, Value::Null);

    // ---- get / access --------------------------------------------------
    let (error, job) = error_text(ex::get_exhaustive_search_job(&pool, user(102), 1).await);
    emit(
        "get-owner",
        error,
        job.as_ref()
            .map(|job| {
                json!({
                    "agg_state": job.agg_state.as_str(),
                    "cancel": job.worker.cancel,
                    "failure_message": job.worker.failure_message,
                    "finished_at": time_flag(job.worker.finished_at),
                    "id": job.id,
                    "initiator_id": job.initiator_id,
                    "is_aggregated": job.is_aggregated,
                    "num_failures": job.worker.num_failures,
                    "num_resets": job.worker.num_resets,
                    "process_after": time_flag(job.worker.process_after),
                    "query": job.query,
                    "started_at": time_flag(job.worker.started_at),
                    "state": job.worker.state.as_str(),
                    "worker_hostname": job.worker.worker_hostname,
                })
            })
            .unwrap_or(Value::Null),
    );

    let (error, job) = error_text(ex::get_exhaustive_search_job(&pool, user(101), 2).await);
    emit(
        "get-admin",
        error,
        job.as_ref()
            .map(|job| {
                json!({
                    "agg_state": job.agg_state.as_str(),
                    "cancel": job.worker.cancel,
                    "failure_message": job.worker.failure_message,
                    "finished_at": time_flag(job.worker.finished_at),
                    "id": job.id,
                    "initiator_id": job.initiator_id,
                    "is_aggregated": job.is_aggregated,
                    "num_failures": job.worker.num_failures,
                    "num_resets": job.worker.num_resets,
                    "process_after": time_flag(job.worker.process_after),
                    "query": job.query,
                    "started_at": time_flag(job.worker.started_at),
                    "state": job.worker.state.as_str(),
                    "worker_hostname": job.worker.worker_hostname,
                })
            })
            .unwrap_or(Value::Null),
    );

    let (error, job) = error_text(ex::get_exhaustive_search_job(&pool, user(102), 3).await);
    emit(
        "get-forbidden",
        error,
        job.as_ref()
            .map(|job| {
                json!({
                    "agg_state": job.agg_state.as_str(),
                    "cancel": job.worker.cancel,
                    "failure_message": job.worker.failure_message,
                    "finished_at": time_flag(job.worker.finished_at),
                    "id": job.id,
                    "initiator_id": job.initiator_id,
                    "is_aggregated": job.is_aggregated,
                    "num_failures": job.worker.num_failures,
                    "num_resets": job.worker.num_resets,
                    "process_after": time_flag(job.worker.process_after),
                    "query": job.query,
                    "started_at": time_flag(job.worker.started_at),
                    "state": job.worker.state.as_str(),
                    "worker_hostname": job.worker.worker_hostname,
                })
            })
            .unwrap_or(Value::Null),
    );

    let (error, job) = error_text(ex::get_exhaustive_search_job(&pool, user(102), 9999).await);
    emit(
        "get-missing",
        error,
        job.as_ref()
            .map(|job| {
                json!({
                    "agg_state": job.agg_state.as_str(),
                    "cancel": job.worker.cancel,
                    "failure_message": job.worker.failure_message,
                    "finished_at": time_flag(job.worker.finished_at),
                    "id": job.id,
                    "initiator_id": job.initiator_id,
                    "is_aggregated": job.is_aggregated,
                    "num_failures": job.worker.num_failures,
                    "num_resets": job.worker.num_resets,
                    "process_after": time_flag(job.worker.process_after),
                    "query": job.query,
                    "started_at": time_flag(job.worker.started_at),
                    "state": job.worker.state.as_str(),
                    "worker_hostname": job.worker.worker_hostname,
                })
            })
            .unwrap_or(Value::Null),
    );

    let (error, _) = error_text(ex::user_has_access(&pool, user(102), 1).await);
    emit("access-ok", error, Value::Null);
    let (error, _) = error_text(ex::user_has_access(&pool, user(102), 3).await);
    emit("access-forbidden", error, Value::Null);

    // ---- cancel --------------------------------------------------------
    let (error, total) = error_text(ex::cancel_search_job(&pool, user(102), 6, now).await);
    emit(
        "cancel-processing",
        error,
        json!({
            "jobs": job_rows(&pool, &[6]).await,
            "repo_jobs": repo_rows(&pool, 6).await,
            "rev_jobs": rev_rows(&pool, 61).await,
            "total": total.unwrap_or(-1),
        }),
    );

    let (error, total) = error_text(ex::cancel_search_job(&pool, user(102), 7, now).await);
    emit(
        "cancel-queued",
        error,
        json!({
            "jobs": job_rows(&pool, &[7]).await,
            "repo_jobs": repo_rows(&pool, 7).await,
            "rev_jobs": rev_rows(&pool, 71).await,
            "total": total.unwrap_or(-1),
        }),
    );

    let (error, total) = error_text(ex::cancel_search_job(&pool, user(102), 3, now).await);
    emit(
        "cancel-forbidden",
        error,
        json!({"total": total.unwrap_or(-1)}),
    );

    // ---- list ----------------------------------------------------------
    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(&pool, ex::Actor::default(), &ex::ListArgs::default())
            .await,
    );
    emit(
        "list-unauthenticated",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(&pool, internal, &ex::ListArgs::default()).await,
    );
    emit(
        "list-internal",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(&pool, user(102), &ex::ListArgs::default()).await,
    );
    emit(
        "list-user",
        error,
        jobs.map(|jobs| summaries(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(&pool, user(101), &ex::ListArgs::default()).await,
    );
    emit(
        "list-admin",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(
            &pool,
            user(101),
            &ex::ListArgs {
                user_ids: vec![102],
                ..ex::ListArgs::default()
            },
        )
        .await,
    );
    emit(
        "list-admin-users",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(
            &pool,
            user(102),
            &ex::ListArgs {
                user_ids: vec![101],
                ..ex::ListArgs::default()
            },
        )
        .await,
    );
    emit(
        "list-user-users",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(
            &pool,
            user(102),
            &ex::ListArgs {
                query: "foo".to_owned(),
                ..ex::ListArgs::default()
            },
        )
        .await,
    );
    emit(
        "list-query",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, jobs) = error_text(
        ex::list_exhaustive_search_jobs(
            &pool,
            user(102),
            &ex::ListArgs {
                states: vec!["COMPLETED".to_owned(), "processing".to_owned()],
                ..ex::ListArgs::default()
            },
        )
        .await,
    );
    emit(
        "list-states",
        error,
        jobs.map(|jobs| ids_of(&jobs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    // ---- aggregate state / logs ---------------------------------------
    let (error, states) = error_text(ex::get_aggregate_repo_rev_state(&pool, user(102), 1).await);
    emit(
        "aggregate-state",
        error,
        states
            .map(|states| {
                Value::Object(
                    states
                        .into_iter()
                        .map(|(state, count)| (state, json!(count)))
                        .collect(),
                )
            })
            .unwrap_or(Value::Null),
    );

    let (error, states) = error_text(ex::get_aggregate_repo_rev_state(&pool, user(102), 3).await);
    emit(
        "aggregate-forbidden",
        error,
        states
            .map(|states| {
                Value::Object(
                    states
                        .into_iter()
                        .map(|(state, count)| (state, json!(count)))
                        .collect(),
                )
            })
            .unwrap_or(Value::Null),
    );

    let (error, logs) = error_text(ex::get_job_logs(&pool, user(102), 1, None).await);
    emit(
        "logs",
        error,
        logs.map(|logs| logs_json(&logs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, logs) = error_text(
        ex::get_job_logs(
            &pool,
            user(102),
            1,
            Some(ex::GetJobLogsOptions {
                from: 112,
                limit: 10,
            }),
        )
        .await,
    );
    emit(
        "logs-from-limit",
        error,
        logs.map(|logs| logs_json(&logs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    let (error, logs) = error_text(ex::get_job_logs(&pool, user(102), 3, None).await);
    emit(
        "logs-forbidden",
        error,
        logs.map(|logs| logs_json(&logs))
            .unwrap_or_else(|| Value::Array(vec![])),
    );

    // ---- delete --------------------------------------------------------
    let (error, _) = error_text(ex::delete_exhaustive_search_job(&pool, user(102), id).await);
    emit("delete-owner", error, job_ids(&pool).await);

    let (error, _) = error_text(ex::delete_exhaustive_search_job(&pool, user(102), 3).await);
    emit("delete-forbidden", error, job_ids(&pool).await);

    // ---- repo / revision jobs -----------------------------------------
    let (error, repo_job_id) =
        error_text(ex::create_exhaustive_search_repo_job(&pool, 202, 1, "HEAD").await);
    emit(
        "create-repo-job-ok",
        error,
        json!({
            "id": repo_job_id.unwrap_or(0),
            "rows": repo_rows(&pool, 1).await,
        }),
    );

    let (error, _) = error_text(ex::create_exhaustive_search_repo_job(&pool, 202, 0, "HEAD").await);
    emit("create-repo-job-missing-job", error, Value::Null);

    let (error, _) = error_text(ex::create_exhaustive_search_repo_job(&pool, 0, 1, "HEAD").await);
    emit("create-repo-job-missing-repo", error, Value::Null);

    let (error, _) = error_text(ex::create_exhaustive_search_repo_job(&pool, 202, 1, "").await);
    emit("create-repo-job-missing-ref", error, Value::Null);

    let (error, rev_job_id) =
        error_text(ex::create_exhaustive_search_repo_revision_job(&pool, 11, "v3").await);
    emit(
        "create-rev-job-ok",
        error,
        json!({
            "id": rev_job_id.unwrap_or(0),
            "rows": rev_rows(&pool, 11).await,
        }),
    );

    let (error, _) =
        error_text(ex::create_exhaustive_search_repo_revision_job(&pool, 0, "v3").await);
    emit("create-rev-job-missing-repo-job", error, Value::Null);

    let (error, _) =
        error_text(ex::create_exhaustive_search_repo_revision_job(&pool, 11, "").await);
    emit("create-rev-job-missing-revision", error, Value::Null);

    let (error, query_repo_rev) = error_text(ex::get_query_repo_rev(&pool, 11, "v3").await);
    emit(
        "query-repo-rev",
        error,
        query_repo_rev
            .map(|value| {
                json!({
                    "id": value.id,
                    "initiator_id": value.initiator_id,
                    "query": value.query,
                    "repo_id": value.repo_rev.repository_rev_specs.repository,
                    "revision": value.repo_rev.revision,
                    "revision_specs": value.repo_rev.repository_rev_specs.revision_specifiers,
                })
            })
            .unwrap_or(Value::Null),
    );

    let (error, _) = error_text(ex::get_query_repo_rev(&pool, 9999, "v3").await);
    emit("query-repo-rev-missing", error, Value::Null);
}
