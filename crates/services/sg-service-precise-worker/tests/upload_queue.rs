//! Differential harness for the `lsif_uploads` worker queue store (M9.7b).
//!
//! Run with `SG_QTEST_DSN` pointing at a scratch copy of the frontend schema,
//! after applying `test_ws/p9-go/queue_fixtures.sql`:
//!
//! ```text
//! SG_QTEST_DSN=postgres://... cargo test -p sg-service-precise-worker \
//!     --test upload_queue -- --nocapture
//! ```
//!
//! The Go counterpart is
//! `internal/codeintel/uploads/queueharness/harness_test.go`; both print the
//! same lines, and `test_ws/p9-go/run_queue_diff.sh` diffs them.

use std::time::Duration;

use sg_service_precise_worker::queue::{Upload, UploadQueueStore};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

#[tokio::test]
async fn queue_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = UploadQueueStore::new(pool.clone());

    // Step 1-3: the three highest-priority queued records.
    let first = dequeue_step(&store, "worker-A").await.expect("first");
    let second = dequeue_step(&store, "worker-A").await.expect("second");
    let third = dequeue_step(&store, "worker-A").await.expect("third");

    // Heartbeats: one unknown id, and the cancel-flagged record.
    let ids = [first.id, second.id, 999999];
    let result = store.heartbeat(&ids, None).await.expect("heartbeat");
    println!(
        "heartbeat ids={} known={} cancel={}",
        join_ints(&ids),
        join_i32(&result.known_ids),
        join_i32(&result.cancel_ids)
    );
    let result = store.heartbeat(&[108], None).await.expect("heartbeat 108");
    println!(
        "heartbeat ids=108 known={} cancel={}",
        join_i32(&result.known_ids),
        join_i32(&result.cancel_ids)
    );

    // Final-state transitions and their guards.
    let ok = store.mark_complete(first.id, None).await.expect("complete");
    println!("mark_complete id={} ok={}", first.id, ok);
    let ok = store.mark_complete(first.id, None).await.expect("complete");
    println!("mark_complete id={} ok={}", first.id, ok);
    let ok = store
        .mark_errored(second.id, "boom", None)
        .await
        .expect("errored");
    println!("mark_errored id={} ok={}", second.id, ok);
    let ok = store
        .mark_failed(third.id, "fatal", None)
        .await
        .expect("failed");
    println!("mark_failed id={} ok={}", third.id, ok);
    let ok = store
        .mark_failed(first.id, "too late", None)
        .await
        .expect("failed");
    println!("mark_failed id={} ok={}", first.id, ok);
    let ok = store.mark_complete(999999, None).await.expect("complete");
    println!("mark_complete id=999999 ok={}", ok);

    // Requeue pushes a record behind its process_after; the next dequeues
    // walk the remaining ordering.
    let fourth = dequeue_step(&store, "worker-A").await.expect("fourth");
    requeue_step(
        &store,
        fourth.id,
        OffsetDateTime::now_utc() + Duration::from_secs(3600),
    )
    .await;
    let fifth = dequeue_step(&store, "worker-B").await.expect("fifth");
    requeue_step(
        &store,
        fifth.id,
        OffsetDateTime::now_utc() - Duration::from_secs(1),
    )
    .await;
    dequeue_step(&store, "worker-C").await.expect("sixth");
    dequeue_step(&store, "worker-C").await.expect("seventh");
    dequeue_step(&store, "worker-C").await;

    // Stalled reset: 106 back to queued, 107 beyond MaxNumResets to failed.
    let reset = store.reset_stalled().await.expect("reset stalled");
    println!(
        "reset_stalled reset={} failed={}",
        join_ages(&reset.reset),
        join_ages(&reset.failed)
    );

    // Canonical table dump.
    let rows = sqlx::query(
        r#"
SELECT
	id, state, num_resets, num_failures, failure_message,
	started_at IS NOT NULL AS started_set,
	finished_at IS NOT NULL AS finished_set,
	process_after IS NOT NULL AS process_after_set,
	cancel, worker_hostname
FROM lsif_uploads
WHERE id BETWEEN 101 AND 112
ORDER BY id
"#,
    )
    .fetch_all(&pool)
    .await
    .expect("dump");

    println!("dump:");
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let state: String = row.try_get("state").expect("state");
        let num_resets: i32 = row.try_get("num_resets").expect("num_resets");
        let num_failures: i32 = row.try_get("num_failures").expect("num_failures");
        let failure_message: Option<String> = row.try_get("failure_message").expect("failure");
        let started_set: bool = row.try_get("started_set").expect("started");
        let finished_set: bool = row.try_get("finished_set").expect("finished");
        let process_after_set: bool = row.try_get("process_after_set").expect("process_after");
        let cancel: bool = row.try_get("cancel").expect("cancel");
        let worker_hostname: String = row.try_get("worker_hostname").expect("worker");
        println!(
            "row id={} state={} resets={} failures={} failure={} started={} finished={} process_after={} cancel={} worker={:?}",
            id,
            state,
            num_resets,
            num_failures,
            format_option_string(&failure_message),
            set_str(started_set),
            set_str(finished_set),
            set_str(process_after_set),
            cancel,
            worker_hostname
        );
    }
}

async fn dequeue_step(store: &UploadQueueStore, host: &str) -> Option<Upload> {
    match store.dequeue(host).await.expect("dequeue") {
        Some(upload) => {
            println!("dequeue host={} ok=1 {}", host, format_upload(&upload));
            Some(upload)
        }
        None => {
            println!("dequeue host={} ok=0", host);
            None
        }
    }
}

async fn requeue_step(store: &UploadQueueStore, id: i32, after: OffsetDateTime) {
    store.requeue(id, after).await.expect("requeue");
    let direction = if after > OffsetDateTime::now_utc() {
        "future"
    } else {
        "past"
    };
    println!("requeue id={} after={}", id, direction);
}

fn format_upload(u: &Upload) -> String {
    format!(
        "id={} state={} failure={} started={} finished={} process_after={} resets={} failures={} repo={:?} indexer={:?} indexer_version={:?} parts={} uploaded_parts={} size={} uncompressed={} assoc={} content_type={:?} reindex={} rank={} visible={}",
        u.id,
        u.state,
        format_option_string(&u.failure_message),
        set_str(u.started_at.is_some()),
        set_str(u.finished_at.is_some()),
        set_str(u.process_after.is_some()),
        u.num_resets,
        u.num_failures,
        u.repository_name,
        u.indexer,
        u.indexer_version,
        u.num_parts,
        format_ints(&u.uploaded_parts),
        format_option_i64(u.upload_size),
        format_option_i64(u.uncompressed_size),
        format_option_i64(u.associated_index_id),
        u.content_type,
        u.should_reindex,
        format_option_i32(u.rank),
        u.visible_at_tip,
    )
}

fn format_option_string(value: &Option<String>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => "null".to_string(),
    }
}

fn format_option_i32(value: Option<i32>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_i64(value: Option<i64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "null".to_string(),
    }
}

fn format_ints(values: &[i32]) -> String {
    let parts: Vec<String> = values.iter().map(|value| value.to_string()).collect();
    format!("[{}]", parts.join(","))
}

fn set_str(set: bool) -> &'static str {
    if set {
        "set"
    } else {
        "null"
    }
}

fn join_ints(values: &[i32]) -> String {
    let parts: Vec<String> = values.iter().map(|value| value.to_string()).collect();
    parts.join(",")
}

fn join_i32(values: &[i32]) -> String {
    join_ints(values)
}

fn join_ages(values: &[(i32, Duration)]) -> String {
    let parts: Vec<String> = values.iter().map(|(id, _)| id.to_string()).collect();
    parts.join(",")
}
