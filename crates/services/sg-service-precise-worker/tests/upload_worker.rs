//! Differential harness for the `workerutil.Worker` loop (M9.7c).
//!
//! Run with `SG_QTEST_DSN` pointing at a scratch copy of the frontend schema,
//! after applying `test_ws/p9-go/worker_fixtures.sql`:
//!
//! ```text
//! SG_QTEST_DSN=postgres://... cargo test -p sg-service-precise-worker \
//!     --test upload_worker -- --nocapture
//! ```
//!
//! The Go counterpart is
//! `internal/codeintel/uploads/queueharness/worker_harness_test.go`; both print
//! the same lines, and `test_ws/p9-go/run_worker_diff.sh` diffs them.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_service_precise_worker::queue::{Upload, UploadQueueStore};
use sg_service_precise_worker::worker::{
    HandlerError, UploadHandler, UploadWorker, UploadWorkerOptions,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

struct HarnessHandler;

#[async_trait]
impl UploadHandler for HarnessHandler {
    async fn handle(&self, upload: Upload, cancel: CancellationToken) -> Result<(), HandlerError> {
        match upload.id {
            201 | 204 => {
                sleep(Duration::from_millis(30)).await;
                Ok(())
            }
            202 => {
                sleep(Duration::from_millis(30)).await;
                Err(HandlerError::retryable("handler boom"))
            }
            203 => {
                sleep(Duration::from_millis(30)).await;
                Err(HandlerError::non_retryable("non retryable"))
            }
            // 205 runs past `maximum_runtime_per_job` (the outer timeout
            // fires); 207 is cancelled through the queue's cancel flag.
            205 | 207 => {
                cancel.cancelled().await;
                Err(HandlerError::canceled())
            }
            other => Err(HandlerError::retryable(format!(
                "unexpected upload {other}"
            ))),
        }
    }

    fn pre_handle(&self, upload: &Upload) {
        // Called synchronously from the dequeue loop, so this order is stable.
        println!("pre_handle id={}", upload.id);
    }
}

#[tokio::test]
async fn worker_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = Arc::new(UploadQueueStore::new(pool.clone()));

    let mut options = UploadWorkerOptions::for_service(
        "precise_code_intel_upload_worker_harness",
        "queue-harness",
        2,
        Duration::from_millis(10),
        Duration::from_millis(200),
    );
    options.heartbeat_interval = Duration::from_millis(20);
    options.num_total_jobs = Some(6);

    let worker = Arc::new(UploadWorker::new(
        Arc::clone(&store),
        Arc::new(HarnessHandler),
        options,
    ));
    worker.run(CancellationToken::new()).await;
    println!("worker done");

    // Budget probe: the upload processor's PreDequeue hint is an extra
    // condition `(upload_size IS NULL OR upload_size <= <budget>)`.
    let skipped = store
        .dequeue_with_budget("budget", Some(10))
        .await
        .expect("dequeue with budget");
    println!(
        "dequeue host=budget budget=10 ok={} id={}",
        skipped.is_some(),
        format_id(skipped.as_ref())
    );

    let dequeued = store
        .dequeue_with_budget("budget", None)
        .await
        .expect("dequeue without budget");
    println!(
        "dequeue host=budget budget=none ok={} id={}",
        dequeued.is_some(),
        format_id(dequeued.as_ref())
    );
    if let Some(upload) = dequeued {
        let ok = store
            .mark_failed(upload.id, "budget probe done", None)
            .await
            .expect("mark failed");
        println!("mark_failed id={} ok={ok}", upload.id);
    }

    let rows = sqlx::query(
        r#"
SELECT
	id, state, num_resets, num_failures, failure_message,
	started_at IS NOT NULL AS started_set,
	finished_at IS NOT NULL AS finished_set,
	process_after IS NOT NULL AS process_after_set,
	cancel, worker_hostname
FROM lsif_uploads
WHERE id BETWEEN 201 AND 208
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

fn format_id(upload: Option<&Upload>) -> String {
    match upload {
        Some(upload) => upload.id.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_string(value: &Option<String>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => "null".to_string(),
    }
}

fn set_str(set: bool) -> &'static str {
    if set {
        "set"
    } else {
        "null"
    }
}
