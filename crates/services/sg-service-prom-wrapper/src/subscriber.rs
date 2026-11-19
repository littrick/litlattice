#![forbid(unsafe_code)]

//! Site-config subscriber and change application (`siteconfig.go`,
//! `change.go`).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response};
use hyper::body::Incoming;
use sg_config::{ConfigClient, SiteConfiguration};
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;

use crate::alertmanager::{json_response, problems_body, AlertmanagerClient, SilenceMatcher};
use crate::amconfig::AmConfig;
use crate::config::{DiffType, Problem, SiteEmailConfig, SubscribedSiteConfig};
use crate::silence::{matchers_from_silence, silence_from_matchers};

/// `conf.Watch` in Go reacts immediately; the Rust config client is polled.
const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// `configUpdateCtx` timeout in `siteconfig.go`.
const APPLY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct SubscriberState {
    config: SubscribedSiteConfig,
    problems: Vec<Problem>,
}

pub struct SiteConfigSubscriber {
    pub alertmanager: Arc<AlertmanagerClient>,
    client: ConfigClient,
    /// Shared with the HTTP handler, which runs concurrently with the
    /// subscriber loop.
    state: tokio::sync::Mutex<SubscriberState>,
}

impl SiteConfigSubscriber {
    pub fn new(alertmanager: Arc<AlertmanagerClient>) -> Self {
        Self::with_config_client(alertmanager, ConfigClient::from_env())
    }

    /// Test/embedding hook: use a specific configuration client.
    pub fn with_config_client(alertmanager: Arc<AlertmanagerClient>, client: ConfigClient) -> Self {
        Self {
            alertmanager,
            client,
            state: tokio::sync::Mutex::new(SubscriberState::default()),
        }
    }

    pub async fn handle(&self, _request: Request<Incoming>) -> Response<BoxBody> {
        self.handle_response().await
    }

    /// `GET /prom-wrapper/config-subscriber`.
    pub(crate) async fn handle_response(&self) -> Response<BoxBody> {
        let mut problems = {
            let state = self.state.lock().await;
            state.problems.clone()
        };

        // The baseline probes Alertmanager when the status is fetched and
        // surfaces a problem if it is unreachable.
        if self.alertmanager.status().await.is_err() {
            tracing::error!("unable to get Alertmanager status");
            problems.push(Problem::new(
                "`observability`: unable to reach Alertmanager - please refer to the Prometheus logs for more details",
            ));
        }

        json_response(http::StatusCode::OK, &problems_body(&problems))
    }

    /// Poll the frontend configuration and apply subscribed changes until
    /// shutdown (`Subscribe` + `conf.Watch`).
    pub async fn run(&self, shutdown: CancellationToken) {
        loop {
            if shutdown.is_cancelled() {
                return;
            }

            match self.client.fetch_raw().await {
                Ok(raw) => match SiteConfiguration::parse(&raw.site) {
                    Ok(site) => {
                        let new_config = SubscribedSiteConfig::from_site_config(site.raw());
                        let diffs = {
                            let state = self.state.lock().await;
                            new_config.diff(&state.config)
                        };

                        if diffs.is_empty() {
                            tracing::debug!(
                                "config update contained no relevant changes - ignoring"
                            );
                        } else {
                            let types = diffs.iter().map(|diff| diff.as_str()).collect::<Vec<_>>();
                            tracing::debug!(?types, "applying configuration diffs");
                            if tokio::time::timeout(
                                APPLY_TIMEOUT,
                                self.apply_diffs(&new_config, &diffs),
                            )
                            .await
                            .is_err()
                            {
                                tracing::error!("timed out applying configuration diffs");
                                let mut state = self.state.lock().await;
                                state.problems = vec![Problem::new(
                                    "`observability`: failed to update Alertmanager configuration (timed out)",
                                )];
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to parse site configuration");
                    }
                },
                Err(error) => {
                    // Services commonly start before the frontend is
                    // reachable; keep polling quietly (the Go watcher delays
                    // this log as well).
                    if error.is_unreachable() {
                        tracing::debug!(%error, "frontend configuration unavailable");
                    } else {
                        tracing::error!(%error, "failed to fetch site configuration");
                    }
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    /// `execDiffs`: read the current Alertmanager config, run the changes in
    /// order, then write/reload. Failures are reported as problems; the
    /// subscriber state is only advanced once Alertmanager accepts the new
    /// configuration.
    async fn apply_diffs(&self, new_config: &SubscribedSiteConfig, diffs: &[DiffType]) {
        tracing::debug!("reloading with new configuration");
        let mut am_config = match load_existing_config(&self.alertmanager.config_path) {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(%error, "failed to load Alertmanager configuration");
                let mut state = self.state.lock().await;
                state.problems = vec![Problem::new(
                    "`observability`: failed to load Alertmanager configuration, please refer to Prometheus logs for more details",
                )];
                return;
            }
        };

        let mut problems = Vec::new();
        for diff in diffs {
            tracing::info!(change = diff.as_str(), "applying changes for diff");
            match diff {
                DiffType::Alerts => crate::receivers::apply_receivers(
                    &mut am_config,
                    &new_config.alerts,
                    &new_config.external_url,
                    &mut problems,
                ),
                DiffType::Email => apply_email(&mut am_config, new_config.email.as_ref()),
                DiffType::Silences => {
                    sync_silences(
                        &self.alertmanager,
                        &new_config.silenced_alerts,
                        &mut problems,
                    )
                    .await
                }
            }
        }

        match self.alertmanager.apply(&am_config).await {
            Ok(()) => {
                let mut state = self.state.lock().await;
                state.config = new_config.clone();
                state.problems = problems;
                tracing::debug!("configuration diffs applied");
            }
            Err(error) => {
                tracing::error!(%error, "failed to apply new configuration");
                problems.push(Problem::new(format!(
                    "`observability`: failed to update Alertmanager configuration ({error})"
                )));
                self.state.lock().await.problems = problems;
            }
        }
    }
}

/// `changeSMTP`: update the Alertmanager `global` SMTP fields and apply
/// `email.smtp.additionalHeaders` to every email receiver.
pub(crate) fn apply_email(config: &mut AmConfig, email: Option<&SiteEmailConfig>) {
    let default_email = SiteEmailConfig::default();
    let email = email.unwrap_or(&default_email);
    let smtp = email.smtp.clone().unwrap_or_default();

    let global = config.global.get_or_insert_with(Default::default);
    global.smtp_from = Some(email.address.clone());
    global.smtp_hello = Some(smtp.domain.clone());
    global.smtp_smarthost = Some(format!("{}:{}", smtp.host, smtp.port));
    global.smtp_auth_username = Some(smtp.username.clone());
    match smtp.authentication.as_str() {
        "PLAIN" => global.smtp_auth_password = Some(smtp.password.clone()),
        "CRAM-MD5" => global.smtp_auth_secret = Some(smtp.password.clone()),
        _ => {}
    }
    global.smtp_require_tls = Some(!smtp.no_verify_tls);

    if !smtp.additional_headers.is_empty() {
        for receiver in &mut config.receivers {
            for email_config in &mut receiver.email_configs {
                for header in &smtp.additional_headers {
                    email_config
                        .headers
                        .insert(header.key.clone(), header.value.clone());
                }
            }
        }
    }
}

/// `changeSilences`: make Alertmanager's `src-prom-wrapper` silences match
/// `observability.silenceAlerts`.
async fn sync_silences(
    client: &AlertmanagerClient,
    configured: &[String],
    problems: &mut Vec<Problem>,
) {
    let mut active_silences: BTreeMap<String, Option<String>> = configured
        .iter()
        .map(|silence| (silence.clone(), None))
        .collect();

    let existing = match client.get_silences().await {
        Ok(existing) => existing,
        Err(error) => {
            problems.push(silence_problem(format!(
                "failed to get existing silences: {error}"
            )));
            return;
        }
    };

    for silence in existing {
        if silence.created_by != "src-prom-wrapper" || silence.status.state != "active" {
            continue;
        }
        let silenced_alert = silence_from_matchers(
            &silence
                .matchers
                .iter()
                .map(SilenceMatcher::to_matcher)
                .collect::<Vec<_>>(),
        );

        match active_silences.get_mut(&silenced_alert) {
            Some(existing_id) => *existing_id = Some(silence.id.clone()),
            None => {
                if let Err(error) = client.delete_silence(&silence.id).await {
                    problems.push(silence_problem(format!(
                        "failed to delete existing silence {:?}: {error}",
                        silence.id
                    )));
                    return;
                }
            }
        }
    }

    let active_names = active_silences.keys().cloned().collect::<Vec<_>>();
    tracing::info!(?active_names, "updating alert silences");

    for (alert, existing_id) in &active_silences {
        let matchers = matchers_from_silence(alert);
        if let Err(error) = client.post_silence(existing_id.as_deref(), &matchers).await {
            tracing::error!(%error, silence = %alert, "failed to update silence");
            problems.push(silence_problem(format!(
                "failed to update silence: {error}"
            )));
            return;
        }
    }
}

fn silence_problem(message: String) -> Problem {
    Problem::new(format!("`observability.silenceAlerts`: {message}"))
}

/// Read the current Alertmanager configuration; a missing file starts from
/// the default (the brief explicitly allows this even though Go's
/// `LoadFile` would report an error).
fn load_existing_config(path: &Path) -> Result<AmConfig, String> {
    if path.exists() {
        crate::amconfig::load_file(path)
    } else {
        Ok(AmConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alertmanager::test_support::MockServer;
    use crate::amconfig::{EmailConfig, Receiver};
    use crate::config::Header;
    use crate::config::SmtpConfig;
    use http::StatusCode;

    fn write_minimal_config(path: &Path) {
        std::fs::write(
            path,
            "route:\n  receiver: default\nreceivers:\n  - name: default\n",
        )
        .expect("write config");
    }

    #[test]
    fn email_updates_global_and_receiver_headers() {
        let mut config = AmConfig::default();
        config.receivers.push(Receiver {
            name: "src-critical".to_owned(),
            email_configs: vec![EmailConfig::default()],
            ..Receiver::default()
        });
        config.receivers.push(Receiver {
            name: "src-warning".to_owned(),
            email_configs: vec![EmailConfig::default()],
            ..Receiver::default()
        });
        let email = SiteEmailConfig {
            address: "alerts@example.com".to_owned(),
            smtp: Some(SmtpConfig {
                host: "smtp.example.com".to_owned(),
                port: 587,
                username: "user".to_owned(),
                password: "pass".to_owned(),
                authentication: "PLAIN".to_owned(),
                domain: "example.com".to_owned(),
                no_verify_tls: false,
                additional_headers: vec![Header {
                    key: "X-Custom".to_owned(),
                    value: "value".to_owned(),
                }],
            }),
        };

        apply_email(&mut config, Some(&email));

        let global = config.global.expect("global");
        assert_eq!(global.smtp_from.as_deref(), Some("alerts@example.com"));
        assert_eq!(
            global.smtp_smarthost.as_deref(),
            Some("smtp.example.com:587")
        );
        assert_eq!(global.smtp_auth_username.as_deref(), Some("user"));
        assert_eq!(global.smtp_auth_password.as_deref(), Some("pass"));
        assert_eq!(global.smtp_require_tls, Some(true));
        for receiver in &config.receivers {
            assert_eq!(
                receiver.email_configs[0].headers.get("X-Custom"),
                Some(&"value".to_owned())
            );
        }
    }

    #[test]
    fn email_zero_config_matches_go_zero_values() {
        let mut config = AmConfig::default();
        apply_email(&mut config, None);
        let global = config.global.expect("global");
        assert_eq!(global.smtp_from.as_deref(), Some(""));
        assert_eq!(global.smtp_smarthost.as_deref(), Some(":0"));
        assert_eq!(global.smtp_require_tls, Some(true));
    }

    #[tokio::test]
    async fn syncs_silences_deleting_unconfigured_and_creating_new() {
        let server = MockServer::start(|request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/alertmanager/api/v2/silences") => (
                StatusCode::OK,
                serde_json::json!([
                    {
                        "id": "old-id",
                        "createdBy": "src-prom-wrapper",
                        "status": {"state": "active"},
                        "matchers": [{"name": "alertname", "value": "^(warning_old)$", "isRegex": true}]
                    },
                    {
                        "id": "other-id",
                        "createdBy": "someone-else",
                        "status": {"state": "active"},
                        "matchers": [{"name": "alertname", "value": "^(warning_gone)$", "isRegex": true}]
                    }
                ])
                .to_string(),
            ),
            ("DELETE", "/alertmanager/api/v2/silence/old-id") => (StatusCode::OK, String::new()),
            ("POST", "/alertmanager/api/v2/silences") => (StatusCode::OK, String::new()),
            _ => (StatusCode::NOT_FOUND, String::new()),
        })
        .await;

        let client = AlertmanagerClient::new(server.base_url.clone(), "/tmp/alertmanager.yml");
        let mut problems = Vec::new();
        sync_silences(&client, &["warning_new".to_owned()], &mut problems).await;

        assert!(problems.is_empty(), "{problems:?}");
        let requests = server.requests();
        assert!(requests.iter().any(|request| {
            request.method == "DELETE" && request.path == "/alertmanager/api/v2/silence/old-id"
        }));
        let post = requests
            .iter()
            .find(|request| request.method == "POST")
            .expect("post request");
        let body: serde_json::Value = serde_json::from_str(&post.body).expect("json");
        assert!(body.get("id").is_none());
        assert_eq!(body["matchers"][0]["name"], "alertname");
        assert_eq!(body["matchers"][0]["value"], "^(warning_new)$");
        assert_eq!(body["matchers"][0]["isRegex"], true);
    }

    #[tokio::test]
    async fn syncs_silences_updating_existing() {
        let server = MockServer::start(|request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/alertmanager/api/v2/silences") => (
                StatusCode::OK,
                serde_json::json!([
                    {
                        "id": "keep-id",
                        "createdBy": "src-prom-wrapper",
                        "status": {"state": "active"},
                        "matchers": [{"name": "alertname", "value": "^(warning_new)$", "isRegex": true}]
                    }
                ])
                .to_string(),
            ),
            ("POST", "/alertmanager/api/v2/silences") => (StatusCode::OK, String::new()),
            _ => (StatusCode::NOT_FOUND, String::new()),
        })
        .await;

        let client = AlertmanagerClient::new(server.base_url.clone(), "/tmp/alertmanager.yml");
        let mut problems = Vec::new();
        sync_silences(&client, &["warning_new".to_owned()], &mut problems).await;

        assert!(problems.is_empty(), "{problems:?}");
        let requests = server.requests();
        assert!(!requests.iter().any(|request| request.method == "DELETE"));
        let post = requests
            .iter()
            .find(|request| request.method == "POST")
            .expect("post request");
        let body: serde_json::Value = serde_json::from_str(&post.body).expect("json");
        assert_eq!(body["id"], "keep-id");
    }

    #[tokio::test]
    async fn syncs_silences_reports_listing_errors() {
        let server =
            MockServer::start(|_| (StatusCode::INTERNAL_SERVER_ERROR, "cannot list".to_owned()))
                .await;
        let client = AlertmanagerClient::new(server.base_url.clone(), "/tmp/alertmanager.yml");
        let mut problems = Vec::new();
        sync_silences(&client, &[], &mut problems).await;

        assert_eq!(problems.len(), 1);
        assert!(problems[0]
            .message
            .starts_with("`observability.silenceAlerts`: failed to get existing silences: "));
    }

    #[tokio::test]
    async fn apply_diffs_updates_state_on_success() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("alertmanager.yml");
        write_minimal_config(&config_path);
        let subscriber = SiteConfigSubscriber::with_config_client(
            Arc::new(AlertmanagerClient::new(
                server.base_url.clone(),
                config_path.clone(),
            )),
            ConfigClient::new("127.0.0.1:1"),
        );

        let new_config = SubscribedSiteConfig {
            email: Some(SiteEmailConfig {
                address: "alerts@example.com".to_owned(),
                smtp: Some(SmtpConfig {
                    host: "smtp.example.com".to_owned(),
                    port: 25,
                    ..SmtpConfig::default()
                }),
            }),
            ..SubscribedSiteConfig::default()
        };

        subscriber
            .apply_diffs(&new_config, &[DiffType::Email])
            .await;

        let state = subscriber.state.lock().await;
        assert!(state.problems.is_empty(), "{:?}", state.problems);
        assert_eq!(state.config, new_config);
        assert!(config_path.exists());
        drop(state);
        assert!(server
            .requests()
            .iter()
            .any(|request| request.path == "/alertmanager/-/reload"));
    }

    #[tokio::test]
    async fn apply_diffs_keeps_state_on_failure() {
        let server = MockServer::start(|request| match request.method.as_str() {
            "POST" => (StatusCode::INTERNAL_SERVER_ERROR, "broken".to_owned()),
            _ => (StatusCode::OK, String::new()),
        })
        .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("alertmanager.yml");
        write_minimal_config(&config_path);
        let subscriber = SiteConfigSubscriber::with_config_client(
            Arc::new(AlertmanagerClient::new(
                server.base_url.clone(),
                config_path,
            )),
            ConfigClient::new("127.0.0.1:1"),
        );

        let new_config = SubscribedSiteConfig {
            email: Some(SiteEmailConfig {
                address: "alerts@example.com".to_owned(),
                ..SiteEmailConfig::default()
            }),
            ..SubscribedSiteConfig::default()
        };
        subscriber
            .apply_diffs(&new_config, &[DiffType::Email])
            .await;

        let state = subscriber.state.lock().await;
        assert_eq!(state.config, SubscribedSiteConfig::default());
        assert_eq!(state.problems.len(), 1);
        assert!(state.problems[0]
            .message
            .starts_with("`observability`: failed to update Alertmanager configuration ("));
    }

    #[tokio::test]
    async fn handle_reports_problems_and_alertmanager_reachability() {
        let server = MockServer::start(|_| (StatusCode::OK, "{}".to_owned())).await;
        let subscriber = SiteConfigSubscriber::with_config_client(
            Arc::new(AlertmanagerClient::new(
                server.base_url.clone(),
                "/tmp/alertmanager.yml",
            )),
            ConfigClient::new("127.0.0.1:1"),
        );
        {
            let mut state = subscriber.state.lock().await;
            state
                .problems
                .push(Problem::new("`observability.alerts`: boom"));
        }

        let response = subscriber.handle_response().await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("body")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            value,
            serde_json::json!({
                "problems": [
                    {"kind": "SiteConfig", "description": "`observability.alerts`: boom"}
                ]
            })
        );
    }
}
