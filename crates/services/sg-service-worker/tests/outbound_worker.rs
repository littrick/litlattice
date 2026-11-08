#![forbid(unsafe_code)]

//! M9.8n differential harness (Rust side): the `outbound-webhook-sender`
//! worker loop and stalled-record resetter.
//!
//! Mirrors
//! `cmd/worker/internal/outboundwebhooks/outbound_worker_harness_test.go`.
//! The fixture `test_ws/p9-go/outbound_webhooks_loop_fixtures.sql` is applied
//! by `test_ws/p9-go/run_outbound_webhooks_loop_diff.sh` immediately before
//! each run. The worker runs end-to-end with short intervals (20ms poll,
//! 100ms heartbeat) against a local HTTP server; the resetter runs until the
//! stalled rows have been reset.

use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use sg_http::{HttpClient, HttpFuture, HttpHandler};
use sg_service_worker::framework::dbworker::{
    DbBackedRoutine, DbResetter, ResetterOptions, ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use sg_service_worker::jobs::outbound_webhooks::handler::WebhookSender;
use sg_service_worker::jobs::outbound_webhooks::store::{
    WebhookJobQueue, WebhookLogStore, WebhookStore,
};
use sg_service_worker::jobs::outbound_webhooks::urlcheck::DenyList;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn outbound_webhook_worker_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let shutdown = CancellationToken::new();
    let http: HttpHandler = Arc::new(|req| {
        Box::pin(async move {
            let path = req.uri().path().to_owned();
            let _ = req.into_body().collect().await;
            let (status, body): (u16, &'static str) = match path.as_str() {
                "/hook" => (200, r#"{"ok":true}"#),
                "/bad" => (500, "nope"),
                _ => (404, ""),
            };
            let xtest = if status == 200 { "yes" } else { "no" };
            http::Response::builder()
                .status(status)
                .header("X-Test", xtest)
                .body(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from_static(body.as_bytes()),
                )))
                .expect("response")
        }) as HttpFuture
    });
    let server = tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));

    rewrite_url(&pool, 9760, &format!("http://{addr}/hook")).await;
    rewrite_url(&pool, 9761, &format!("http://{addr}/bad")).await;

    let queue = Arc::new(WebhookJobQueue::new(pool.clone(), None));
    let sender = Arc::new(WebhookSender {
        client: HttpClient::new(),
        webhooks: WebhookStore::new(pool.clone()),
        logs: WebhookLogStore::new(pool.clone(), None),
        key: None,
        deny_list: DenyList::test_denylist(),
    });

    let worker = Arc::new(WorkerRoutine::new(
        Arc::clone(&queue),
        sender,
        WorkerOptions {
            name: "outbound_webhook_job_worker".to_owned(),
            description: String::new(),
            interval: Duration::from_millis(20),
            heartbeat_interval: Duration::from_millis(100),
            num_handlers: 1,
            worker_hostname: "harness".to_owned(),
        },
    ));

    let worker_shutdown = CancellationToken::new();
    let worker_task = tokio::spawn(Arc::clone(&worker).start_arc(worker_shutdown.clone()));
    wait_for_count(
        &pool,
        "SELECT COUNT(*) FROM outbound_webhook_jobs \
         WHERE id IN (9750, 9751, 9752, 9754) AND state NOT IN ('completed', 'failed')",
        0,
    )
    .await;
    worker_shutdown.cancel();
    worker_task.await.expect("worker join");

    for id in [9750i64, 9751, 9752, 9753, 9754] {
        dump_job(&pool, id).await;
    }
    print_worker_logs(&pool).await;

    let resetter = Arc::new(ResetterRoutine::new(
        Arc::clone(&queue) as Arc<dyn DbResetter>,
        ResetterOptions {
            name: "outbound_webhook_job_resetter".to_owned(),
            interval: Duration::from_millis(20),
        },
    ));
    let resetter_shutdown = CancellationToken::new();
    let resetter_task = tokio::spawn(Arc::clone(&resetter).start_arc(resetter_shutdown.clone()));
    wait_for_count(
        &pool,
        "SELECT COUNT(*) FROM outbound_webhook_jobs \
         WHERE id IN (9756, 9757) AND state = 'processing'",
        0,
    )
    .await;
    resetter_shutdown.cancel();
    resetter_task.await.expect("resetter join");

    for id in [9756i64, 9757, 9758] {
        dump_job(&pool, id).await;
    }

    shutdown.cancel();
    let _ = server.await;
}

/// Polls `query` until it returns `want`, panicking after 30s.
async fn wait_for_count(pool: &PgPool, query: &str, want: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(pool)
            .await
            .expect("count");
        if count == want {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still {count} (want {want}) after 30s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn dump_job(pool: &PgPool, id: i64) {
    let row = sqlx::query(
        r#"
SELECT state, num_failures, num_resets, failure_message, worker_hostname,
       finished_at IS NOT NULL AS finished
FROM outbound_webhook_jobs WHERE id = $1"#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("dump job");
    let state: String = row.get("state");
    let failures: i32 = row.get("num_failures");
    let resets: i32 = row.get("num_resets");
    let failure: Option<String> = row.get("failure_message");
    let worker: String = row.get("worker_hostname");
    let finished: bool = row.get("finished");
    println!(
        "wjob {id} state={state} failures={failures} resets={resets} failure={} worker={worker} finished={finished}",
        failure.as_deref().unwrap_or("<null>")
    );
}

async fn print_worker_logs(pool: &PgPool) {
    let rows = sqlx::query(
        r#"
SELECT job_id, count(*) AS count,
       string_agg(status_code::text, ',' ORDER BY status_code) AS statuses
FROM outbound_webhook_logs
WHERE job_id BETWEEN 9750 AND 9755
GROUP BY job_id ORDER BY job_id ASC"#,
    )
    .fetch_all(pool)
    .await
    .expect("worker logs");
    for row in rows {
        let job_id: i64 = row.get("job_id");
        let count: i64 = row.get("count");
        let statuses: Option<String> = row.get("statuses");
        println!(
            "wlogs job={job_id} count={count} statuses={}",
            statuses.unwrap_or_default()
        );
    }
}

async fn rewrite_url(pool: &PgPool, id: i64, url: &str) {
    sqlx::query("UPDATE outbound_webhooks SET url = $1 WHERE id = $2")
        .bind(url.as_bytes().to_vec())
        .bind(id)
        .execute(pool)
        .await
        .expect("rewrite url");
}
