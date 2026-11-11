//! M9.8ip differential harness: `codemonitors-job` action dispatch, payloads,
//! error handling and resetter metrics.
//!
//! Mirrors `internal/codemonitors/background/zz_m9ip_oracle_test.go`
//! (UNTRACKED, gated by `SG_M9IP_DIFF=1`). Both sides consume
//! `test_ws/p9-go/data/codemonitors_job_corpus.json`, drive
//! `handle_action` / [`ActionRunnerHandler`] with a fake [`ActionStore`], send
//! webhooks/Slack to a local HTTP capture server, and print the same
//! normalised JSON (2-space indent, sorted map keys, trailing newline).
//!
//! Runs only when `SG_M9IP_DIFF=1`; `M9IP_CORPUS` and `M9IP_OUT` name the
//! input corpus and the output file.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sg_db::code_monitors::{
    ActionJob, ActionJobMetadata, EmailAction, QueryTrigger, Recipient, SlackWebhookAction,
    TriggerJob, WebhookAction,
};
use sg_service_worker::framework::dbworker::DbResetter;
use sg_service_worker::jobs::code_monitor_action_runner::EmailSender;
use sg_service_worker::jobs::code_monitor_actions::TemplateDataNewSearchResults;
use sg_service_worker::jobs::code_monitor_email::template_data_json;
use sg_service_worker::jobs::codemonitors_job::{
    handle_action, CodeMonitorMetrics, HttpSlackSender, HttpWebhookSender, MetricsKind,
    MetricsResetter, SlackSender, WebhookSender,
};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
struct Corpus {
    external_url: String,
    cases: Vec<CaseSpec>,
    resetters: Vec<ResetterSpec>,
}

#[derive(Clone, Deserialize)]
struct JobSpec {
    id: i32,
    email: Option<i64>,
    webhook: Option<i64>,
    slack_webhook: Option<i64>,
    trigger_event: i32,
}

#[derive(Clone, Deserialize)]
struct MetadataSpec {
    description: String,
    query: String,
    monitor_id: i64,
    owner_name: String,
    #[serde(default)]
    results: serde_json::Value,
}

#[derive(Clone, Deserialize)]
struct EmailSpec {
    id: i64,
    monitor: i64,
    priority: String,
    include_results: bool,
}

#[derive(Clone, Deserialize)]
struct WebhookSpec {
    id: i64,
    monitor: i64,
    #[allow(dead_code)]
    url: String,
    include_results: bool,
}

#[derive(Clone, Deserialize)]
struct RecipientSpec {
    namespace_user_id: Option<i32>,
    namespace_org_id: Option<i32>,
}

#[derive(Clone, Default, Deserialize)]
struct StoreErrors {
    #[serde(default)]
    metadata: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    webhook: String,
    #[serde(default)]
    slack_webhook: String,
    #[serde(default)]
    recipients: String,
}

#[derive(Clone, Deserialize)]
struct CaseSpec {
    name: String,
    job: JobSpec,
    metadata: Option<MetadataSpec>,
    email: Option<EmailSpec>,
    webhook: Option<WebhookSpec>,
    slack_webhook: Option<WebhookSpec>,
    recipients: Vec<RecipientSpec>,
    #[serde(default)]
    store_errors: StoreErrors,
    #[serde(default)]
    email_error: String,
    #[serde(default)]
    http_status: u16,
    #[serde(default)]
    http_body: String,
}

#[derive(Deserialize)]
struct ResetterSpec {
    name: String,
    kind: String,
    resets: Vec<i64>,
    failures: Vec<i64>,
    #[serde(default)]
    error: String,
}

#[derive(Serialize)]
struct Output {
    cases: Vec<CaseOutput>,
    resetters: Vec<ResetterOutput>,
}

#[derive(Serialize)]
struct CaseOutput {
    name: String,
    error: Option<String>,
    email_sends: Vec<EmailSendOutput>,
    http: Option<HttpOutput>,
}

#[derive(Clone, Serialize)]
struct EmailSendOutput {
    user_id: i32,
    data: serde_json::Value,
}

#[derive(Serialize)]
struct HttpOutput {
    content_type: String,
    body: String,
}

#[derive(Serialize)]
struct ResetterOutput {
    name: String,
    resets: u64,
    reset_failures: u64,
    errors: u64,
}

struct CapturingEmailSender {
    sends: Mutex<Vec<EmailSendOutput>>,
    error: Option<String>,
}

#[async_trait]
impl EmailSender for CapturingEmailSender {
    async fn send(&self, user_id: i32, data: &TemplateDataNewSearchResults) -> Result<(), String> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.sends.lock().expect("sends").push(EmailSendOutput {
            user_id,
            data: template_data_json(data),
        });
        Ok(())
    }
}

struct FakeActionStore {
    case: CaseSpec,
    webhook_url: String,
    slack_url: String,
}

#[async_trait]
impl sg_service_worker::jobs::codemonitors_job::ActionStore for FakeActionStore {
    async fn get_action_job_metadata(&self, _job_id: i32) -> Result<ActionJobMetadata, String> {
        if !self.case.store_errors.metadata.is_empty() {
            return Err(self.case.store_errors.metadata.clone());
        }
        let metadata = self.case.metadata.clone().expect("metadata");
        Ok(ActionJobMetadata {
            description: metadata.description,
            query: Some(metadata.query),
            monitor_id: metadata.monitor_id,
            results: metadata.results,
            owner_name: metadata.owner_name,
        })
    }

    async fn get_email_action(&self, _email_id: i64) -> Result<EmailAction, String> {
        if !self.case.store_errors.email.is_empty() {
            return Err(self.case.store_errors.email.clone());
        }
        let email = self.case.email.clone().expect("email");
        Ok(EmailAction {
            id: email.id,
            monitor: email.monitor,
            priority: email.priority,
            include_results: email.include_results,
        })
    }

    async fn get_webhook_action(&self, _webhook_id: i64) -> Result<WebhookAction, String> {
        if !self.case.store_errors.webhook.is_empty() {
            return Err(self.case.store_errors.webhook.clone());
        }
        let webhook = self.case.webhook.clone().expect("webhook");
        Ok(WebhookAction {
            id: webhook.id,
            monitor: webhook.monitor,
            url: self.webhook_url.clone(),
            include_results: webhook.include_results,
        })
    }

    async fn get_slack_webhook_action(
        &self,
        _slack_webhook_id: i64,
    ) -> Result<SlackWebhookAction, String> {
        if !self.case.store_errors.slack_webhook.is_empty() {
            return Err(self.case.store_errors.slack_webhook.clone());
        }
        let slack = self.case.slack_webhook.clone().expect("slack webhook");
        Ok(SlackWebhookAction {
            id: slack.id,
            monitor: slack.monitor,
            url: self.slack_url.clone(),
            include_results: slack.include_results,
        })
    }

    async fn list_recipients(&self, _email_id: i64) -> Result<Vec<Recipient>, String> {
        if !self.case.store_errors.recipients.is_empty() {
            return Err(self.case.store_errors.recipients.clone());
        }
        Ok(self
            .case
            .recipients
            .iter()
            .enumerate()
            .map(|(index, recipient)| Recipient {
                id: index as i64 + 1,
                email: 0,
                namespace_user_id: recipient.namespace_user_id,
                namespace_org_id: recipient.namespace_org_id,
            })
            .collect())
    }
}

#[derive(Clone)]
struct Captured {
    content_type: String,
    body: String,
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Minimal HTTP/1.1 server: accepts one request, captures its Content-Type and
/// body, replies with the given status line/body.
fn spawn_capture(status: u16, response_body: String) -> (String, mpsc::Receiver<Captured>) {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|status| status.canonical_reason())
        .unwrap_or("");
    let status_line = format!("HTTP/1.1 {status} {reason}");
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

fn action_job(spec: &JobSpec) -> ActionJob {
    ActionJob {
        id: spec.id,
        email: spec.email,
        webhook: spec.webhook,
        slack_webhook: spec.slack_webhook,
        trigger_event: spec.trigger_event,
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

async fn run_case(corpus: &Corpus, case: CaseSpec) -> CaseOutput {
    let needs_http =
        case.http_status != 0 && (case.job.webhook.is_some() || case.job.slack_webhook.is_some());
    let (webhook_url, slack_url, capture) = if needs_http {
        let (url, receiver) = spawn_capture(case.http_status, case.http_body.clone());
        if case.job.webhook.is_some() {
            (url, String::new(), Some(receiver))
        } else {
            (String::new(), url, Some(receiver))
        }
    } else {
        (String::new(), String::new(), None)
    };

    let store = Arc::new(FakeActionStore {
        case: case.clone(),
        webhook_url,
        slack_url,
    });
    let email_sender = Arc::new(CapturingEmailSender {
        sends: Mutex::new(Vec::new()),
        error: if case.email_error.is_empty() {
            None
        } else {
            Some(case.email_error.clone())
        },
    });
    let client = reqwest::Client::new();
    let webhook_sender: Arc<dyn WebhookSender> = Arc::new(HttpWebhookSender::new(client.clone()));
    let slack_sender: Arc<dyn SlackSender> = Arc::new(HttpSlackSender::new(client));

    let job = action_job(&case.job);
    let result = handle_action(
        &job,
        store.as_ref(),
        &corpus.external_url,
        email_sender.as_ref(),
        webhook_sender.as_ref(),
        slack_sender.as_ref(),
    )
    .await;

    let http = capture.map(|receiver| {
        let captured = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("capture");
        HttpOutput {
            content_type: captured.content_type,
            body: captured.body,
        }
    });

    let sends = email_sender.sends.lock().expect("sends");
    let email_sends = sends.clone();

    CaseOutput {
        name: case.name,
        error: result.err(),
        email_sends,
        http,
    }
}

type ResetterOutcome = Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String>;

struct FakeResetter {
    outcome: Mutex<Option<ResetterOutcome>>,
}

#[async_trait]
impl DbResetter for FakeResetter {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        self.outcome
            .lock()
            .expect("outcome")
            .take()
            .expect("one call")
    }
}

async fn run_resetter(spec: ResetterSpec) -> ResetterOutput {
    let kind = if spec.kind == "action" {
        MetricsKind::Actions
    } else {
        MetricsKind::TriggerQueries
    };
    let metrics = Arc::new(CodeMonitorMetrics::new(kind));
    let outcome = if spec.error.is_empty() {
        Ok((
            spec.resets
                .iter()
                .map(|id| (*id, Duration::from_secs(*id as u64)))
                .collect(),
            spec.failures
                .iter()
                .map(|id| (*id, Duration::from_secs(*id as u64)))
                .collect(),
        ))
    } else {
        Err(spec.error)
    };
    let resetter = MetricsResetter::new(
        Arc::new(FakeResetter {
            outcome: Mutex::new(Some(outcome)),
        }),
        Arc::clone(&metrics),
    );
    let _ = resetter.reset_stalled().await;
    let snapshot = metrics.snapshot();
    ResetterOutput {
        name: spec.name,
        resets: snapshot.resets,
        reset_failures: snapshot.reset_failures,
        errors: snapshot.errors,
    }
}

#[tokio::test]
async fn codemonitors_job_diff() {
    if std::env::var("SG_M9IP_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M9IP_CORPUS").expect("M9IP_CORPUS");
    let output_path = std::env::var("M9IP_OUT").expect("M9IP_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Output {
        cases: Vec::with_capacity(corpus.cases.len()),
        resetters: Vec::with_capacity(corpus.resetters.len()),
    };
    for case in corpus.cases.clone() {
        output.cases.push(run_case(&corpus, case).await);
    }
    for spec in corpus.resetters {
        output.resetters.push(run_resetter(spec).await);
    }

    let mut encoded = serde_json::to_string_pretty(&output).expect("serialize");
    encoded.push('\n');
    std::fs::write(output_path, encoded).expect("write output");
}

// Keep the query-handler types in the harness' reach: the corpus currently
// covers the action half; the trigger pipeline is unit-tested in the module.
#[allow(dead_code)]
fn trigger_types_are_public(
    _store: &dyn sg_service_worker::jobs::codemonitors_job::TriggerStore,
    _searcher: &dyn sg_service_worker::jobs::codemonitors_job::TriggerSearcher,
    _query: QueryTrigger,
    _monitor: sg_db::code_monitors::Monitor,
    _trigger: TriggerJob,
    _cancel: CancellationToken,
) {
}
