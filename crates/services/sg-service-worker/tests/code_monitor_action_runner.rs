//! M9.8cn differential harness: `actionRunner` dispatch and handlers.
//!
//! Mirrors `internal/codemonitors/background/cm_action_runner_harness_test.go`.
//! The e-mail transport is injected through [`EmailSender`] (the baseline's
//! `MockSendEmailForNewSearchResult` seam); webhook and Slack delivery hit
//! minimal capture servers. Only runs when `SG_CM_ACTION_RUNNER_TEST_DSN`
//! names the scratch frontend database. Prints canonical `cmr ` lines.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use sg_db::code_monitors::ActionJob;
use sg_service_worker::jobs::code_monitor_action_runner::{ActionRunner, EmailSender};
use sg_service_worker::jobs::code_monitor_actions::TemplateDataNewSearchResults;
use sqlx::{PgPool, Row};
use url::Url;

fn escape_err(err: Option<String>) -> String {
    match err {
        None => "-".to_owned(),
        Some(err) => format!("{err:?}"),
    }
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).expect("quote")
}

#[derive(Serialize)]
struct CmpDisplay {
    #[serde(rename = "resultType")]
    result_type: String,
    #[serde(rename = "commitURL")]
    commit_url: String,
    #[serde(rename = "repoName")]
    repo_name: String,
    #[serde(rename = "commitID")]
    commit_id: String,
    content: String,
}

/// Mirror of `TemplateDataNewSearchResults` with the camelCase JSON names the
/// Go harness uses (the real struct has no JSON tags).
#[derive(Serialize)]
struct CmpData {
    priority: String,
    #[serde(rename = "codeMonitorURL")]
    code_monitor_url: String,
    #[serde(rename = "searchURL")]
    search_url: String,
    description: String,
    #[serde(rename = "includeResults")]
    include_results: bool,
    #[serde(rename = "truncatedResults")]
    truncated_results: Vec<CmpDisplay>,
    #[serde(rename = "totalCount")]
    total_count: usize,
    #[serde(rename = "truncatedCount")]
    truncated_count: usize,
    #[serde(rename = "resultPluralized")]
    result_pluralized: String,
    #[serde(rename = "truncatedResultPluralized")]
    truncated_result_pluralized: String,
    #[serde(rename = "displayMoreLink")]
    display_more_link: bool,
    #[serde(rename = "isTest")]
    is_test: bool,
}

fn cmp_data(data: &TemplateDataNewSearchResults) -> CmpData {
    CmpData {
        priority: data.priority.clone(),
        code_monitor_url: data.code_monitor_url.clone(),
        search_url: data.search_url.clone(),
        description: data.description.clone(),
        include_results: data.include_results,
        truncated_results: data
            .truncated_results
            .iter()
            .map(|display| CmpDisplay {
                result_type: display.result_type.clone(),
                commit_url: display.commit_url.clone(),
                repo_name: display.repo_name.clone(),
                commit_id: display.commit_id.clone(),
                content: display.content.clone(),
            })
            .collect(),
        total_count: data.total_count,
        truncated_count: data.truncated_count,
        result_pluralized: data.result_pluralized.clone(),
        truncated_result_pluralized: data.truncated_result_pluralized.clone(),
        display_more_link: data.display_more_link,
        is_test: data.is_test,
    }
}

struct CapturingSender {
    current_job: AtomicI32,
}

#[async_trait]
impl EmailSender for CapturingSender {
    async fn send(&self, user_id: i32, data: &TemplateDataNewSearchResults) -> Result<(), String> {
        let job = self.current_job.load(Ordering::SeqCst);
        println!(
            "cmr email job={job} user={user_id} data={}",
            serde_json::to_string(&cmp_data(data)).expect("serialize")
        );
        Ok(())
    }
}

struct Captured {
    content_type: String,
    body: String,
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Minimal HTTP/1.1 server: accepts one request, captures its Content-Type
/// and body, replies with the given status line/body.
fn spawn_capture(
    status_line: &'static str,
    response_body: &'static str,
) -> (String, mpsc::Receiver<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut data = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = stream.read(&mut buffer).expect("read");
            if read == 0 {
                break;
            }
            data.extend_from_slice(&buffer[..read]);
            if let Some(end) = find_header_end(&data) {
                let header_end = end + 4;
                let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
                let content_length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                if data.len() >= header_end + content_length {
                    break;
                }
            }
        }
        let header_end = find_header_end(&data).map(|end| end + 4).unwrap_or(0);
        let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
        let content_type = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-type") {
                    Some(value.trim().to_owned())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        let body = String::from_utf8_lossy(&data[header_end..]).to_string();
        let response = format!(
            "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = sender.send(Captured { content_type, body });
    });
    (format!("http://{addr}/hook"), receiver)
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("exec");
}

/// `cm_trigger_jobs.search_results` as `json.Marshal([]*result.CommitMatch)`
/// would store it: one MessagePreview match and one DiffPreview match.
const RESULTS_JSON: &str = r#"[
 {"repoID":1,"repoName":"github.com/acme/alpha","repoStars":0,"commitID":"a1b2c3d4e5f60718",
  "author":{"name":"Alice","email":"alice@example.com","date":"2020-01-01T00:00:00Z"},
  "message":"fix crash",
  "messagePreview":{"content":"line1\nline2 crash here\nline3\n","matchedRanges":[{"start":[6,1,0],"end":[11,1,5]},{"start":[17,1,11],"end":[22,1,16]}]}},
 {"repoID":2,"repoName":"github.com/acme/beta","repoStars":0,"commitID":"b1b2c3d4e5f60718",
  "author":{"name":"Bob","email":"bob@example.com","date":"2020-01-01T00:00:00Z"},
  "message":"tweak",
  "diffPreview":{"content":"x.go x.go\n@@ -1,2 +1,2 @@ func main()\n-panic()\n+recover()\n","matchedRanges":[{"start":[40,2,0],"end":[47,2,7]}]}}
]"#;

async fn seed(
    pool: &PgPool,
    webhook_ok1: &str,
    webhook_boom1: &str,
    webhook_ok2: &str,
    slack_ok: &str,
    slack_boom: &str,
) {
    exec(
        pool,
        "TRUNCATE TABLE cm_action_jobs, cm_trigger_jobs, cm_queries, cm_emails, cm_recipients, \
         cm_webhooks, cm_slack_webhooks, cm_monitors, users, orgs RESTART IDENTITY CASCADE",
    )
    .await;
    exec(
        pool,
        "INSERT INTO users (id, username, display_name) VALUES
         (1, 'owner', 'Owner Person'), (10, 'recipient-a', ''), (11, 'recipient-b', ''), (12, 'owner-two', '')",
    )
    .await;
    exec(
        pool,
        "INSERT INTO orgs (id, name, display_name) VALUES (5, 'org-five', 'Org Five')",
    )
    .await;
    exec(
        pool,
        "INSERT INTO cm_monitors (id, created_by, changed_by, description, enabled, namespace_user_id) VALUES
         (1, 1, 1, 'mon-runner', TRUE, 1), (2, 12, 12, 'mon-two', TRUE, 12)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO cm_queries (id, monitor, query, created_by, changed_by) VALUES
         (1, 1, 'type:commit', 1, 1), (2, 2, 'type:diff', 12, 12)",
    )
    .await;
    sqlx::query(
        "INSERT INTO cm_trigger_jobs (id, query, state, query_string, search_results) VALUES
         (900, 1, 'completed', 'type:commit after:2020-01-01', $1::jsonb),
         (901, 1, 'completed', 'type:commit after:2020-01-01', NULL),
         (902, 2, 'completed', 'type:diff after:2020-01-01', $1::jsonb)",
    )
    .bind(RESULTS_JSON)
    .execute(pool)
    .await
    .expect("trigger jobs");
    exec(
        pool,
        "INSERT INTO cm_emails (id, monitor, enabled, priority, header, created_by, changed_by, include_results) VALUES
         (1, 1, TRUE, 'CRITICAL', '', 1, 1, TRUE),
         (2, 1, TRUE, 'NORMAL', '', 1, 1, FALSE),
         (3, 1, TRUE, 'NORMAL', '', 1, 1, TRUE),
         (4, 1, TRUE, 'NORMAL', '', 1, 1, FALSE)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO cm_recipients (id, email, namespace_user_id, namespace_org_id) VALUES
         (1, 1, 10, NULL), (2, 1, NULL, 5), (3, 1, 11, NULL),
         (4, 2, NULL, NULL), (5, 3, 10, NULL), (6, 4, 10, NULL)",
    )
    .await;
    sqlx::query(
        "INSERT INTO cm_webhooks (id, monitor, url, enabled, created_by, changed_by, include_results) VALUES
         (1, 1, $1, TRUE, 1, 1, TRUE), (2, 1, $2, TRUE, 1, 1, FALSE),
         (3, 2, $3, TRUE, 12, 12, TRUE)",
    )
    .bind(webhook_ok1)
    .bind(webhook_boom1)
    .bind(webhook_ok2)
    .execute(pool)
    .await
    .expect("webhooks");
    sqlx::query(
        "INSERT INTO cm_slack_webhooks (id, monitor, url, enabled, created_by, changed_by, include_results) VALUES
         (1, 1, $1, TRUE, 1, 1, TRUE), (2, 1, $2, TRUE, 1, 1, FALSE)",
    )
    .bind(slack_ok)
    .bind(slack_boom)
    .execute(pool)
    .await
    .expect("slack webhooks");
    exec(
        pool,
        "INSERT INTO cm_action_jobs (id, email, webhook, slack_webhook, trigger_event) VALUES
         (910, 1, NULL, NULL, 900), (911, 2, NULL, NULL, 900), (912, 3, NULL, NULL, 901),
         (913, NULL, 1, NULL, 900), (914, NULL, 2, NULL, 900),
         (915, NULL, NULL, 1, 900), (916, NULL, NULL, 2, 900),
         (918, NULL, 3, NULL, 902), (919, 4, NULL, NULL, 900)",
    )
    .await;
}

fn job(id: i32, email: Option<i64>, webhook: Option<i64>, slack_webhook: Option<i64>) -> ActionJob {
    ActionJob {
        id,
        email,
        webhook,
        slack_webhook,
        trigger_event: 0,
        state: None,
        failure_message: None,
        started_at: None,
        finished_at: None,
        process_after: None,
        num_resets: 0,
        num_failures: 0,
        log_contents: None,
    }
}

async fn load_job(pool: &PgPool, id: i32) -> ActionJob {
    let row =
        sqlx::query("SELECT id, email, webhook, slack_webhook FROM cm_action_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("action job");
    job(
        row.try_get("id").expect("id"),
        row.try_get("email").expect("email"),
        row.try_get("webhook").expect("webhook"),
        row.try_get("slack_webhook").expect("slack_webhook"),
    )
}

async fn handle(
    runner: &ActionRunner,
    pool: &PgPool,
    sender: &CapturingSender,
    id: i32,
    capture: Option<&mpsc::Receiver<Captured>>,
    label: &str,
) {
    let record = load_job(pool, id).await;
    sender.current_job.store(id, Ordering::SeqCst);
    let result = runner.handle(&record).await;
    println!("cmr handle job={id} err={}", escape_err(result.err()));
    if let Some(capture) = capture {
        let captured = capture
            .recv_timeout(Duration::from_secs(10))
            .expect("capture");
        println!(
            "cmr {label} job={id} content_type={} body={}",
            quote(&captured.content_type),
            quote(&captured.body)
        );
    }
}

#[tokio::test]
async fn cm_action_runner_harness() {
    let Ok(dsn) = std::env::var("SG_CM_ACTION_RUNNER_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");

    let (webhook_ok1, capture_webhook_ok1) = spawn_capture("HTTP/1.1 200 OK", "ok");
    let (webhook_boom1, capture_webhook_boom1) =
        spawn_capture("HTTP/1.1 500 Internal Server Error", "boom");
    let (webhook_ok2, capture_webhook_ok2) = spawn_capture("HTTP/1.1 200 OK", "ok");
    let (slack_ok, capture_slack_ok) = spawn_capture("HTTP/1.1 200 OK", "ok");
    let (slack_boom, capture_slack_boom) =
        spawn_capture("HTTP/1.1 500 Internal Server Error", "boom");

    seed(
        &pool,
        &webhook_ok1,
        &webhook_boom1,
        &webhook_ok2,
        &slack_ok,
        &slack_boom,
    )
    .await;

    let sender = Arc::new(CapturingSender {
        current_job: AtomicI32::new(0),
    });
    let runner = ActionRunner::new(
        pool.clone(),
        Url::parse("https://sourcegraph.example.com").expect("external url"),
        reqwest::Client::new(),
        sender.clone(),
    );

    // E-mail: success (CRITICAL, org recipient skipped, users 10 and 11).
    handle(&runner, &pool, &sender, 910, None, "").await;
    // E-mail: recipient with neither namespace.
    handle(&runner, &pool, &sender, 911, None, "").await;
    // E-mail: trigger job without search_results (NULL).
    handle(&runner, &pool, &sender, 912, None, "").await;
    // Webhook: success.
    handle(
        &runner,
        &pool,
        &sender,
        913,
        Some(&capture_webhook_ok1),
        "webhook",
    )
    .await;
    // Webhook: non-200 response.
    handle(
        &runner,
        &pool,
        &sender,
        914,
        Some(&capture_webhook_boom1),
        "webhook",
    )
    .await;
    // Slack: success.
    handle(
        &runner,
        &pool,
        &sender,
        915,
        Some(&capture_slack_ok),
        "slack",
    )
    .await;
    // Slack: non-200 response.
    handle(
        &runner,
        &pool,
        &sender,
        916,
        Some(&capture_slack_boom),
        "slack",
    )
    .await;
    // Webhook for a monitor whose owner has no display name.
    handle(
        &runner,
        &pool,
        &sender,
        918,
        Some(&capture_webhook_ok2),
        "webhook",
    )
    .await;
    // E-mail: NORMAL priority, include_results=false.
    handle(&runner, &pool, &sender, 919, None, "").await;

    // Neither action type: unreachable through the schema's check
    // constraint, so the job is constructed directly.
    let no_type = job(9999, None, None, None);
    let result = runner.handle(&no_type).await;
    println!("cmr handle job=9999 err={}", escape_err(result.err()));
}
