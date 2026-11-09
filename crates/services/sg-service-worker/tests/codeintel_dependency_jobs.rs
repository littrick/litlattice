#![forbid(unsafe_code)]

//! M9.8dj differential harness (Rust side): the dbworker-store half of the
//! `codeintel-autoindexing-dependency-scheduler` job, mirrored from
//! `internal/codeintel/autoindexing/internal/background/dependencies/store_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_dependency_jobs_diff.sh` applies
//! `codeintel_dependency_jobs_fixtures.sql` immediately before each side and
//! diffs the canonical `djb ` JSON lines. Timestamps written from the
//! process/database clock are printed as `set`/`null`; fixture-provided and
//! explicitly requeued timestamps are printed verbatim.

use serde::Serialize;
use sg_db::codeintel_dependency_jobs::{
    dequeue_indexing_job, dequeue_syncing_job, heartbeat, mark_complete, mark_errored, mark_failed,
    requeue, reset_stalled, DEFAULT_RESET_FAILURE_MESSAGE, INDEXING_JOBS_TABLE, LSIF_INDEXES_TABLE,
    MAX_NUM_RESETS, MAX_NUM_RETRIES, STALLED_MAX_AGE_SECONDS, SYNCING_JOBS_TABLE,
};
use sg_db::OffsetDateTime;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::UtcOffset;

#[derive(Serialize)]
struct SyncingRecord {
    id: i32,
    state: String,
    failure_message: Option<String>,
    started_at: String,
    finished_at: Option<String>,
    process_after: Option<String>,
    num_resets: i32,
    num_failures: i32,
    upload_id: i32,
}

#[derive(Serialize)]
struct IndexingRecord {
    id: i32,
    state: String,
    failure_message: Option<String>,
    started_at: String,
    finished_at: Option<String>,
    process_after: Option<String>,
    num_resets: i32,
    num_failures: i32,
    upload_id: i32,
    external_service_kind: String,
    external_service_sync: Option<String>,
}

#[derive(Serialize)]
struct StateRow {
    id: i32,
    state: String,
    failure_message: Option<String>,
    started_at: String,
    finished_at: String,
    process_after: Option<String>,
    num_resets: i32,
    num_failures: i32,
    upload_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_service_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_service_sync: Option<String>,
    worker_hostname: String,
    cancel: bool,
    queued_at: String,
    last_heartbeat_at: String,
}

#[derive(Serialize)]
struct HeartbeatLine {
    case: &'static str,
    known: Vec<String>,
    canceled: Vec<String>,
}

#[derive(Serialize)]
struct OkLine {
    case: &'static str,
    ok: bool,
}

#[derive(Serialize)]
struct IdLine {
    case: &'static str,
    id: i64,
}

#[derive(Serialize)]
struct ResetLine {
    case: &'static str,
    reset: Vec<i64>,
    failed: Vec<i64>,
}

#[derive(Serialize)]
struct RowsLine {
    case: &'static str,
    rows: Vec<StateRow>,
}

fn print(value: &impl Serialize) {
    println!(
        "djb {}",
        serde_json::to_string(value).expect("json snapshot")
    );
}

fn marker(value: Option<OffsetDateTime>) -> String {
    if value.is_some() {
        "set".to_owned()
    } else {
        "null".to_owned()
    }
}

fn format_time(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(|value| {
        value
            .to_offset(UtcOffset::UTC)
            .format(&Rfc3339)
            .expect("format time")
    })
}

fn parse_time(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).unwrap_or_else(|err| panic!("parse {raw:?}: {err}"))
}

async fn state_rows(pool: &PgPool, query: &str, with_external_service: bool) -> Vec<StateRow> {
    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .expect("state query");
    rows.into_iter()
        .map(|row| state_row(row, with_external_service))
        .collect()
}

fn state_row(row: PgRow, with_external_service: bool) -> StateRow {
    StateRow {
        id: row.get("id"),
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        started_at: marker(row.get("started_at")),
        finished_at: marker(row.get("finished_at")),
        process_after: format_time(row.get("process_after")),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
        upload_id: row.get("upload_id"),
        external_service_kind: if with_external_service {
            row.get("external_service_kind")
        } else {
            None
        },
        external_service_sync: if with_external_service {
            format_time(row.get("external_service_sync"))
        } else {
            None
        },
        worker_hostname: row.get("worker_hostname"),
        cancel: row.get("cancel"),
        queued_at: marker(row.get("queued_at")),
        last_heartbeat_at: marker(row.get("last_heartbeat_at")),
    }
}

const SYNCING_STATE_QUERY: &str = "
SELECT id, state, failure_message, started_at, finished_at, process_after,
       num_resets, num_failures, upload_id, worker_hostname, cancel, queued_at, last_heartbeat_at
FROM lsif_dependency_syncing_jobs ORDER BY id";

const INDEXING_STATE_QUERY: &str = "
SELECT id, state, failure_message, started_at, finished_at, process_after,
       num_resets, num_failures, upload_id, external_service_kind, external_service_sync,
       worker_hostname, cancel, queued_at, last_heartbeat_at
FROM lsif_dependency_indexing_jobs ORDER BY id";

const INDEXES_STATE_QUERY: &str = "
SELECT id, state, failure_message, started_at, finished_at, process_after,
       num_resets, num_failures, worker_hostname, cancel, queued_at, last_heartbeat_at
FROM lsif_indexes ORDER BY id";

fn sorted_ids(mut ids: Vec<i32>) -> Vec<i32> {
    ids.sort_unstable();
    ids
}

fn sorted_reset_ids(mut ids: Vec<(i64, std::time::Duration)>) -> Vec<i64> {
    let mut ids: Vec<i64> = ids.drain(..).map(|(id, _)| id).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn dependency_jobs_store_harness() {
    if std::env::var("SG_CODEINTEL_DEPENDENCY_JOBS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_DEPENDENCY_JOBS_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    let now = OffsetDateTime::now_utc();
    let hostname = "djb-harness";

    // -- syncing queue ------------------------------------------------------
    let record = dequeue_syncing_job(&pool, now, hostname)
        .await
        .expect("syncing dequeue")
        .expect("syncing dequeue: empty queue");
    print(&SyncingRecord {
        id: record.id,
        state: record.state.clone(),
        failure_message: record.failure_message.clone(),
        started_at: marker(record.started_at),
        finished_at: format_time(record.finished_at),
        process_after: format_time(record.process_after),
        num_resets: record.num_resets,
        num_failures: record.num_failures,
        upload_id: record.upload_id.expect("upload_id"),
    });

    let ids = vec!["101".to_owned(), "102".to_owned(), "999".to_owned()];
    let (known, canceled) = heartbeat(&pool, SYNCING_JOBS_TABLE, now, &ids)
        .await
        .expect("syncing heartbeat");
    let known: Vec<String> = sorted_ids(known)
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    let canceled: Vec<String> = sorted_ids(canceled)
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    print(&HeartbeatLine {
        case: "syncing-heartbeat",
        known,
        canceled,
    });

    let updated = mark_complete(&pool, SYNCING_JOBS_TABLE, 101)
        .await
        .expect("syncing complete");
    print(&OkLine {
        case: "syncing-complete",
        ok: updated,
    });
    let updated = mark_complete(&pool, SYNCING_JOBS_TABLE, 101)
        .await
        .expect("syncing complete again");
    print(&OkLine {
        case: "syncing-complete-again",
        ok: updated,
    });

    let record = dequeue_syncing_job(&pool, now, hostname)
        .await
        .expect("syncing dequeue 2")
        .expect("syncing dequeue 2: empty queue");
    let updated = mark_errored(
        &pool,
        SYNCING_JOBS_TABLE,
        record.id,
        "sync boom",
        MAX_NUM_RETRIES,
    )
    .await
    .expect("syncing errored");
    print(&IdLine {
        case: "syncing-errored",
        id: i64::from(record.id),
    });
    assert!(updated, "syncing errored: not updated");

    let record = dequeue_syncing_job(&pool, now, hostname)
        .await
        .expect("syncing dequeue 3")
        .expect("syncing dequeue 3: empty queue");
    let updated = mark_failed(&pool, SYNCING_JOBS_TABLE, record.id, "sync failed")
        .await
        .expect("syncing failed");
    print(&IdLine {
        case: "syncing-failed",
        id: i64::from(record.id),
    });
    assert!(updated, "syncing failed: not updated");

    let record = dequeue_syncing_job(&pool, now, hostname)
        .await
        .expect("syncing dequeue 4")
        .expect("syncing dequeue 4: empty queue");
    let updated = mark_errored(
        &pool,
        SYNCING_JOBS_TABLE,
        record.id,
        "sync canceled",
        MAX_NUM_RETRIES,
    )
    .await
    .expect("syncing canceled errored");
    print(&IdLine {
        case: "syncing-errored-canceled",
        id: i64::from(record.id),
    });
    assert!(updated, "syncing canceled errored: not updated");

    let empty = dequeue_syncing_job(&pool, now, hostname)
        .await
        .expect("syncing dequeue 5");
    print(&OkLine {
        case: "syncing-dequeue-empty",
        ok: empty.is_some(),
    });

    let ids = vec!["101".to_owned()];
    let (known, canceled) = heartbeat(&pool, SYNCING_JOBS_TABLE, now, &ids)
        .await
        .expect("syncing heartbeat 2");
    print(&HeartbeatLine {
        case: "syncing-heartbeat-completed",
        known: sorted_ids(known)
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        canceled: sorted_ids(canceled)
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
    });

    print(&RowsLine {
        case: "syncing-state",
        rows: state_rows(&pool, SYNCING_STATE_QUERY, false).await,
    });

    // -- indexing queue -----------------------------------------------------
    let record = dequeue_indexing_job(&pool, now, hostname)
        .await
        .expect("indexing dequeue")
        .expect("indexing dequeue: empty queue");
    print(&IndexingRecord {
        id: record.id,
        state: record.state.clone(),
        failure_message: record.failure_message.clone(),
        started_at: marker(record.started_at),
        finished_at: format_time(record.finished_at),
        process_after: format_time(record.process_after),
        num_resets: record.num_resets,
        num_failures: record.num_failures,
        upload_id: record.upload_id.expect("upload_id"),
        external_service_kind: record.external_service_kind.clone(),
        external_service_sync: format_time(record.external_service_sync),
    });

    let requeue_at = parse_time("2030-06-01T00:00:00Z");
    requeue(&pool, INDEXING_JOBS_TABLE, record.id, requeue_at)
        .await
        .expect("indexing requeue");
    print(&IdLine {
        case: "indexing-requeued",
        id: i64::from(record.id),
    });

    let record = dequeue_indexing_job(&pool, now, hostname)
        .await
        .expect("indexing dequeue 2")
        .expect("indexing dequeue 2: empty queue");
    print(&IdLine {
        case: "indexing-dequeue-after-requeue",
        id: i64::from(record.id),
    });

    let ids = vec![record.id.to_string()];
    let (known, canceled) = heartbeat(&pool, INDEXING_JOBS_TABLE, now, &ids)
        .await
        .expect("indexing heartbeat");
    print(&HeartbeatLine {
        case: "indexing-heartbeat",
        known: sorted_ids(known)
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
        canceled: sorted_ids(canceled)
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
    });

    let updated = mark_complete(&pool, INDEXING_JOBS_TABLE, record.id)
        .await
        .expect("indexing complete");
    print(&OkLine {
        case: "indexing-complete",
        ok: updated,
    });

    let (reset, failed) = reset_stalled(
        &pool,
        INDEXING_JOBS_TABLE,
        now,
        STALLED_MAX_AGE_SECONDS,
        MAX_NUM_RESETS,
        DEFAULT_RESET_FAILURE_MESSAGE,
    )
    .await
    .expect("indexing reset");
    print(&ResetLine {
        case: "indexing-reset",
        reset: sorted_reset_ids(reset),
        failed: sorted_reset_ids(failed),
    });

    print(&RowsLine {
        case: "indexing-state",
        rows: state_rows(&pool, INDEXING_STATE_QUERY, true).await,
    });

    // -- lsif_indexes resetter ---------------------------------------------
    let (reset, failed) = reset_stalled(
        &pool,
        LSIF_INDEXES_TABLE,
        now,
        STALLED_MAX_AGE_SECONDS,
        MAX_NUM_RESETS,
        DEFAULT_RESET_FAILURE_MESSAGE,
    )
    .await
    .expect("indexes reset");
    print(&ResetLine {
        case: "indexes-reset",
        reset: sorted_reset_ids(reset),
        failed: sorted_reset_ids(failed),
    });

    print(&RowsLine {
        case: "indexes-state",
        rows: state_rows(&pool, INDEXES_STATE_QUERY, false).await,
    });
}
