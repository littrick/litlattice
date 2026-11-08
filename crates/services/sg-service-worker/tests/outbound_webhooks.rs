#![forbid(unsafe_code)]

//! M9.8m differential harness (Rust side): the `outbound-webhook-sender` core.
//!
//! Mirrors
//! `cmd/worker/internal/outboundwebhooks/outboundwebhooks_harness_test.go`.
//! The fixture `test_ws/p9-go/outbound_webhooks_fixtures.sql` is applied by
//! `test_ws/p9-go/run_outbound_webhooks_diff.sh` immediately before each run.
//!
//! The harness exercises the real queue store, the real webhook/log stores and
//! the real handler against a local HTTP server. The ring has no
//! `OutboundWebhookKey`, exactly like production, so all values stay
//! plaintext. The test denylist (loopback removed) is used for the handler,
//! matching `outbound.SetTestDenyList`.

use std::collections::BTreeMap;
use std::sync::Arc;

use http_body_util::BodyExt;
use sg_http::{HttpClient, HttpFuture, HttpHandler};
use sg_service_worker::jobs::outbound_webhooks::handler::{WebhookLogMessage, WebhookSender};
use sg_service_worker::jobs::outbound_webhooks::store::{
    RecordState, WebhookJob, WebhookJobQueue, WebhookLogStore, WebhookStore,
};
use sg_service_worker::jobs::outbound_webhooks::urlcheck::{check_url, DenyList};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn outbound_webhook_sender_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");

    print_url_checks().await;

    let queue = WebhookJobQueue::new(pool.clone(), None);

    let first = dequeue_and_print(&queue).await;
    let (known, cancel) = queue
        .heartbeat(
            &[9701, 9705, 9708, 9999],
            Some("harness"),
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("heartbeat");
    print_heartbeat("harness", &known, &cancel);

    if let Some(job) = &first {
        println!(
            "mark_complete {} {}",
            job.id,
            queue
                .mark_complete(job.id, None)
                .await
                .expect("mark complete")
        );
        println!(
            "mark_complete {} {}",
            job.id,
            queue
                .mark_complete(job.id, None)
                .await
                .expect("mark complete again")
        );
        dump_job(&pool, job.id).await;
    }

    let second = dequeue_and_print(&queue).await;
    if let Some(job) = &second {
        println!(
            "mark_errored {} {}",
            job.id,
            queue
                .mark_errored(job.id, "handler failed", None)
                .await
                .expect("mark errored")
        );
        dump_job(&pool, job.id).await;
    }

    let third = dequeue_and_print(&queue).await;
    if let Some(job) = &third {
        println!(
            "mark_failed {} {}",
            job.id,
            queue
                .mark_failed(job.id, "gave up", None)
                .await
                .expect("mark failed")
        );
        dump_job(&pool, job.id).await;
    }

    let fourth = dequeue_and_print(&queue).await;
    if let Some(job) = &fourth {
        queue
            .requeue(job.id, OffsetDateTime::now_utc())
            .await
            .expect("requeue");
        println!("requeue {} ok", job.id);
        dump_job(&pool, job.id).await;
    }

    dequeue_and_print(&queue).await;

    let (known, cancel) = queue
        .heartbeat(&[9705, 9708], None, OffsetDateTime::now_utc())
        .await
        .expect("heartbeat");
    print_heartbeat("any", &known, &cancel);

    let (reset, failed) = queue
        .reset_stalled(
            OffsetDateTime::now_utc(),
            std::time::Duration::from_secs(10),
            5,
        )
        .await
        .expect("reset stalled");
    println!(
        "reset stalled={} failed={}",
        join_ids(reset.iter().map(|(id, _)| *id)),
        join_ids(failed.iter().map(|(id, _)| *id))
    );
    for id in [9705i64, 9706, 9707] {
        dump_job(&pool, id).await;
    }

    // Handler runs against a local HTTP server.
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
            let response = http::Response::builder()
                .status(status)
                .header("X-Test", xtest)
                .body(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from_static(body.as_bytes()),
                )))
                .expect("response");
            response
        }) as HttpFuture
    });
    let server = tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));

    for id in [9720i64, 9721, 9722, 9723] {
        rewrite_url(&pool, id, &format!("http://{addr}/hook")).await;
    }
    rewrite_url(&pool, 9724, &format!("http://{addr}/bad")).await;

    let sender = WebhookSender {
        client: HttpClient::new(),
        webhooks: WebhookStore::new(pool.clone()),
        logs: WebhookLogStore::new(pool.clone(), None),
        key: None,
        deny_list: DenyList::test_denylist(),
    };
    for id in [9710i64, 9711, 9712, 9713, 9714, 9715] {
        let job = queue
            .get_by_id(id)
            .await
            .expect("get job")
            .unwrap_or_else(|| panic!("missing job {id}"));
        match sender.handle(&job).await {
            Ok(()) => println!("handle {id} ok"),
            Err(err) => println!("handle {id} err={err}"),
        }
    }

    print_logs(&pool).await;

    queue
        .delete_before(OffsetDateTime::now_utc() - time::Duration::hours(72))
        .await
        .expect("delete before");
    print_janitor_rows(&pool).await;

    shutdown.cancel();
    let _ = server.await;
}

async fn print_url_checks() {
    let default = DenyList::default_denylist();
    let cases = [
        ("public", "http://1.2.3.4/x"),
        ("ipv4_loopback", "http://127.0.0.1/x"),
        ("metadata", "http://169.254.169.254/x"),
        ("private", "http://10.0.0.1/x"),
        ("ipv6_loopback", "http://[::1]/x"),
        ("ftp", "ftp://1.2.3.4/x"),
        ("empty_host", "http://"),
        ("missing_scheme", "://bad"),
    ];
    for (name, dest) in cases {
        match check_url(dest, &default).await {
            Ok(()) => println!("urlcheck {name} ok"),
            Err(err) => println!("urlcheck {name} err={err}"),
        }
    }
    match check_url("http://127.0.0.1/x", &DenyList::test_denylist()).await {
        Ok(()) => println!("urlcheck test_loopback ok"),
        Err(err) => println!("urlcheck test_loopback err={err}"),
    }
}

async fn dequeue_and_print(queue: &WebhookJobQueue) -> Option<WebhookJob> {
    let job = queue
        .dequeue("harness", OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    match &job {
        None => println!("dequeue none"),
        Some(job) => println!(
            "dequeue {} worker={} state={} started={} finished={} failure={}",
            job.id,
            job.worker_hostname,
            job.state,
            job.started_at.is_some(),
            job.finished_at.is_some(),
            job.failure_message.as_deref().unwrap_or("<null>"),
        ),
    }
    job
}

fn print_heartbeat(host: &str, known: &[String], cancel: &[String]) {
    println!(
        "heartbeat {host} known={} cancel={}",
        join_strings(known),
        join_strings(cancel)
    );
}

fn join_strings(values: &[String]) -> String {
    if values.is_empty() {
        return "<>".to_owned();
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    sorted.join(",")
}

fn join_ids(values: impl Iterator<Item = i64>) -> String {
    let mut ids: Vec<i64> = values.collect();
    if ids.is_empty() {
        return "<>".to_owned();
    }
    ids.sort();
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

async fn dump_job(pool: &PgPool, id: i64) {
    let row = sqlx::query(
        r#"
SELECT state, num_failures, num_resets, failure_message, worker_hostname,
       finished_at IS NOT NULL AS finished, process_after IS NOT NULL AS has_process_after
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
    let process_after: bool = row.get("has_process_after");
    println!(
        "job {id} state={state} failures={failures} resets={resets} failure={} worker={worker} finished={finished} process_after={process_after}",
        failure.as_deref().unwrap_or("<null>")
    );
}

async fn rewrite_url(pool: &PgPool, id: i64, url: &str) {
    sqlx::query("UPDATE outbound_webhooks SET url = $1 WHERE id = $2")
        .bind(url.as_bytes().to_vec())
        .bind(id)
        .execute(pool)
        .await
        .expect("rewrite url");
}

async fn print_logs(pool: &PgPool) {
    let rows = sqlx::query(
        r#"
SELECT job_id, outbound_webhook_id, status_code, request, response, error
FROM outbound_webhook_logs
WHERE job_id BETWEEN 9710 AND 9719
ORDER BY job_id ASC, outbound_webhook_id ASC"#,
    )
    .fetch_all(pool)
    .await
    .expect("logs");

    for row in rows {
        let job_id: i64 = row.get("job_id");
        let webhook_id: i64 = row.get("outbound_webhook_id");
        let status: i32 = row.get("status_code");
        let raw_request: Vec<u8> = row.get("request");
        let raw_response: Vec<u8> = row.get("response");
        let raw_error: Vec<u8> = row.get("error");
        let request = WebhookLogMessage::from_json(&String::from_utf8_lossy(&raw_request))
            .expect("request json");
        let response = WebhookLogMessage::from_json(&String::from_utf8_lossy(&raw_response))
            .expect("response json");

        let signature = request
            .header
            .as_ref()
            .and_then(|header| header.get("X-Sourcegraph-Webhook-Signature"))
            .and_then(|values| values.first())
            .cloned()
            .unwrap_or_default();
        let path = path_of(&request.url);
        println!(
            "log job={job_id} webhook={webhook_id} status={status} error={} method={} path={} body={} sig={} resp_body={} req_headers={}",
            String::from_utf8_lossy(&raw_error),
            request.method,
            path,
            String::from_utf8_lossy(&request.body),
            signature,
            String::from_utf8_lossy(&response.body),
            header_names(request.header.as_ref()),
        );
    }
}

fn path_of(url: &str) -> String {
    match url.find("://") {
        Some(scheme_end) => match url[scheme_end + 3..].find('/') {
            Some(path_start) => url[scheme_end + 3 + path_start..].to_owned(),
            None => "/".to_owned(),
        },
        None => url.to_owned(),
    }
}

fn header_names(header: Option<&BTreeMap<String, Vec<String>>>) -> String {
    match header {
        None => String::new(),
        Some(header) => header.keys().cloned().collect::<Vec<_>>().join(","),
    }
}

async fn print_janitor_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id FROM outbound_webhook_jobs WHERE id BETWEEN 9730 AND 9732 ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await
    .expect("janitor rows");
    let ids: Vec<String> = rows
        .iter()
        .map(|row| row.get::<i64, _>("id").to_string())
        .collect();
    println!("janitor remaining={}", join_strings(&ids));
}

#[allow(dead_code)]
async fn count_queued(queue: &WebhookJobQueue) {
    let _ = queue
        .count_by_state(RecordState::QUEUED | RecordState::ERRORED)
        .await;
}
