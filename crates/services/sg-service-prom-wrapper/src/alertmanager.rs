#![forbid(unsafe_code)]

//! Alertmanager API client (`alertmanager.go` + v2 silence API).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;
use serde::{Deserialize, Serialize};
use tonic::body::BoxBody;

use crate::amconfig::AmConfig;
use crate::silence::Matcher;

/// Path prefix Alertmanager is served under (must match `prometheus.yml`).
const ALERTMANAGER_PATH_PREFIX: &str = "alertmanager";
const CREATED_BY: &str = "src-prom-wrapper";
const COMMENT: &str = "Applied via `observability.silenceAlerts` in site configuration";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct AlertmanagerClient {
    pub base_url: String,
    pub config_path: PathBuf,
    pub http: reqwest::Client,
}

impl AlertmanagerClient {
    pub fn new(base_url: impl Into<String>, config_path: impl Into<PathBuf>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.into(),
            config_path: config_path.into(),
            http,
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}/api/v2/{}",
            self.base_url.trim_end_matches('/'),
            ALERTMANAGER_PATH_PREFIX,
            path.trim_start_matches('/')
        )
    }

    fn reload_url(&self) -> String {
        format!(
            "{}/{}/-/reload",
            self.base_url.trim_end_matches('/'),
            ALERTMANAGER_PATH_PREFIX
        )
    }

    /// Poll the v2 status endpoint until it answers 2xx or `timeout` elapses.
    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let mut last_error;

        loop {
            match self.http.get(self.url("status")).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => last_error = format!("unexpected status {}", response.status()),
                Err(error) => last_error = error.to_string(),
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "alertmanager not reachable: timed out after {timeout:?} (last error: {last_error})"
                ));
            }
            tokio::time::sleep(READY_POLL_INTERVAL.min(deadline - now)).await;
        }
    }

    /// `POST /alertmanager/-/reload`.
    pub async fn reload(&self) -> Result<(), String> {
        let response = self
            .http
            .post(self.reload_url())
            .send()
            .await
            .map_err(|error| format!("reload request failed: {error}"))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("reload failed with status {status}: {body}"));
        }
        Ok(())
    }

    /// Render and validate the new configuration, write it to `config_path`
    /// (creating the parent directory), then reload Alertmanager.
    ///
    /// The caller reads/merges the existing file via [`crate::amconfig::load_file`]
    /// before calling this (Go's `execDiffs` does the same); `apply` must not
    /// re-read the file or the changes would be discarded.
    pub async fn apply(&self, config: &AmConfig) -> Result<(), String> {
        let rendered = crate::amconfig::render_yaml(config)
            .map_err(|error| format!("failed to generate Alertmanager configuration: {error}"))?;

        if let Some(parent) = self.config_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.map_err(|error| {
                    format!("failed to write Alertmanager configuration: {error}")
                })?;
            }
        }

        tokio::fs::write(&self.config_path, rendered.as_bytes())
            .await
            .map_err(|error| format!("failed to write Alertmanager configuration: {error}"))?;

        self.reload()
            .await
            .map_err(|error| format!("failed to reload Alertmanager configuration: {error}"))
    }

    /// `GET /alertmanager/api/v2/silences`.
    pub async fn get_silences(&self) -> Result<Vec<Silence>, String> {
        let response = self
            .http
            .get(self.url("silences"))
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let response = ensure_success(response, "getting silences").await?;

        let value: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        if value.is_null() {
            return Ok(Vec::new());
        }
        serde_json::from_value(value).map_err(|error| error.to_string())
    }

    /// `DELETE /alertmanager/api/v2/silence/{id}`.
    pub async fn delete_silence(&self, id: &str) -> Result<(), String> {
        let response = self
            .http
            .delete(self.url(&format!("silence/{id}")))
            .send()
            .await
            .map_err(|error| error.to_string())?;
        ensure_success(response, "deleting silence").await?;
        Ok(())
    }

    /// `POST /alertmanager/api/v2/silences`, creating or updating a silence.
    pub async fn post_silence(&self, id: Option<&str>, matchers: &[Matcher]) -> Result<(), String> {
        let body = PostableSilence {
            id,
            matchers: matchers.iter().map(SilenceMatcher::from).collect(),
            starts_at: timestamp(0),
            // 10 year expiry (expiry is required, but silences removed from
            // config are deleted from Alertmanager instead).
            ends_at: timestamp(10 * 365 * 24 * 60 * 60),
            created_by: CREATED_BY,
            comment: COMMENT,
        };

        let response = self
            .http
            .post(self.url("silences"))
            .json(&body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        ensure_success(response, "updating silence").await?;
        Ok(())
    }

    /// `GET /alertmanager/api/v2/alerts?active=true&inhibited=false`.
    pub async fn alert_status(&self) -> Result<serde_json::Value, String> {
        // `RequestBuilder::query` needs reqwest's `query` feature, which the
        // workspace does not enable; the parameter set is fixed.
        let url = format!("{}?active=true&inhibited=false", self.url("alerts"));
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let response = ensure_success(response, "getting alerts").await?;
        response.json().await.map_err(|error| error.to_string())
    }

    /// `GET /alertmanager/api/v2/status` (reachability probe).
    pub async fn status(&self) -> Result<serde_json::Value, String> {
        let response = self
            .http
            .get(self.url("status"))
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let response = ensure_success(response, "getting status").await?;
        response.json().await.map_err(|error| error.to_string())
    }
}

async fn ensure_success(
    response: reqwest::Response,
    operation: &str,
) -> Result<reqwest::Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Err(format!("{operation} failed with status {status}: {body}"))
}

/// RFC3339 UTC timestamp `offset_seconds` from now.
fn timestamp(offset_seconds: i64) -> String {
    let time = time::OffsetDateTime::now_utc() + time::Duration::seconds(offset_seconds);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        time.year(),
        time.month() as u8,
        time.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

#[derive(Debug, Clone, Deserialize)]
pub struct Silence {
    #[serde(default)]
    pub id: String,
    #[serde(default, rename = "createdBy")]
    pub created_by: String,
    #[serde(default)]
    pub status: SilenceStatus,
    #[serde(default)]
    pub matchers: Vec<SilenceMatcher>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SilenceStatus {
    #[serde(default)]
    pub state: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SilenceMatcher {
    pub name: String,
    pub value: String,
    #[serde(default, rename = "isRegex")]
    pub is_regex: bool,
    /// Absent in older Alertmanager responses; defaults to `true`. Only
    /// serialized when a matcher is negated (`isEqual: false`).
    #[serde(
        default = "default_true",
        rename = "isEqual",
        skip_serializing_if = "is_true"
    )]
    pub is_equal: bool,
}

impl SilenceMatcher {
    pub fn to_matcher(&self) -> Matcher {
        Matcher {
            name: self.name.clone(),
            value: self.value.clone(),
            is_regex: self.is_regex,
            is_equal: self.is_equal,
        }
    }
}

impl From<&Matcher> for SilenceMatcher {
    fn from(matcher: &Matcher) -> Self {
        Self {
            name: matcher.name.clone(),
            value: matcher.value.clone(),
            is_regex: matcher.is_regex,
            is_equal: matcher.is_equal,
        }
    }
}

#[derive(Debug, Serialize)]
struct PostableSilence<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    matchers: Vec<SilenceMatcher>,
    #[serde(rename = "startsAt")]
    starts_at: String,
    #[serde(rename = "endsAt")]
    ends_at: String,
    #[serde(rename = "createdBy")]
    created_by: &'a str,
    comment: &'a str,
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

/// `conf.Problems` JSON shape: objects with `kind` and `description`.
pub(crate) fn problems_body(problems: &[crate::config::Problem]) -> serde_json::Value {
    serde_json::json!({
        "problems": problems
            .iter()
            .map(|problem| serde_json::json!({
                "kind": "SiteConfig",
                "description": problem.message,
            }))
            .collect::<Vec<_>>()
    })
}

pub(crate) fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response<BoxBody> {
    match serde_json::to_vec(value) {
        Ok(body) => {
            let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
            *response.status_mut() = status;
            response
        }
        Err(error) => text_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use http::{Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RecordedRequest {
        pub method: String,
        pub path: String,
        pub query: String,
        pub body: String,
    }

    pub type Responder = Arc<dyn Fn(&RecordedRequest) -> (StatusCode, String) + Send + Sync>;

    pub struct MockServer {
        pub base_url: String,
        pub requests: Arc<Mutex<Vec<RecordedRequest>>>,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    impl MockServer {
        pub async fn start<F>(responder: F) -> Self
        where
            F: Fn(&RecordedRequest) -> (StatusCode, String) + Send + Sync + 'static,
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock server");
            let addr = listener.local_addr().expect("mock server addr");

            let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
            let responder: Responder = Arc::new(responder);
            let handler_requests = Arc::clone(&requests);

            let handler: sg_http::HttpHandler = Arc::new(move |request: Request<Incoming>| {
                let responder = Arc::clone(&responder);
                let requests = Arc::clone(&handler_requests);
                Box::pin(async move {
                    let recorded = RecordedRequest {
                        method: request.method().to_string(),
                        path: request.uri().path().to_owned(),
                        query: request.uri().query().unwrap_or_default().to_owned(),
                        body: request
                            .into_body()
                            .collect()
                            .await
                            .map(|body| String::from_utf8_lossy(&body.to_bytes()).into_owned())
                            .unwrap_or_default(),
                    };
                    let (status, body) = responder(&recorded);
                    requests.lock().expect("requests lock").push(recorded);

                    let mut response =
                        Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
                    *response.status_mut() = status;
                    response
                })
            });

            let shutdown = CancellationToken::new();
            let token = shutdown.clone();
            let task = tokio::spawn(async move {
                let _ = sg_http::serve_http(listener, handler, token).await;
            });

            Self {
                base_url: format!("http://{addr}"),
                requests,
                shutdown,
                task,
            }
        }

        pub fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.shutdown.cancel();
            self.task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{MockServer, RecordedRequest};
    use super::*;

    fn client(server: &MockServer) -> AlertmanagerClient {
        AlertmanagerClient::new(server.base_url.clone(), "/tmp/alertmanager.yml")
    }

    #[tokio::test]
    async fn waits_until_status_is_ok() {
        let server = MockServer::start(|_| (StatusCode::OK, "{}".to_owned())).await;
        client(&server)
            .wait_until_ready(Duration::from_secs(1))
            .await
            .expect("ready");
        let requests = server.requests();
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/alertmanager/api/v2/status");
    }

    #[tokio::test]
    async fn wait_until_ready_times_out() {
        let server = MockServer::start(|_| (StatusCode::SERVICE_UNAVAILABLE, String::new())).await;
        let error = client(&server)
            .wait_until_ready(Duration::from_millis(150))
            .await
            .expect_err("should time out");
        assert!(error.contains("alertmanager not reachable"), "{error}");
    }

    #[tokio::test]
    async fn reload_posts_to_reload_endpoint() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        client(&server).reload().await.expect("reload");
        let requests = server.requests();
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/alertmanager/-/reload");
    }

    #[tokio::test]
    async fn reload_reports_status_and_body() {
        let server =
            MockServer::start(|_| (StatusCode::BAD_REQUEST, "bad config".to_owned())).await;
        let error = client(&server).reload().await.expect_err("reload fails");
        assert_eq!(error, "reload failed with status 400: bad config");
    }

    #[tokio::test]
    async fn parses_silences_with_v2_field_names() {
        let server = MockServer::start(|_| {
            (
                StatusCode::OK,
                serde_json::json!([{
                    "id": "abc",
                    "createdBy": "src-prom-wrapper",
                    "status": {"state": "active"},
                    "matchers": [{"name": "alertname", "value": "^(foo)$", "isRegex": true}]
                }])
                .to_string(),
            )
        })
        .await;

        let silences = client(&server).get_silences().await.expect("silences");
        assert_eq!(silences.len(), 1);
        assert_eq!(silences[0].id, "abc");
        assert_eq!(silences[0].created_by, "src-prom-wrapper");
        assert_eq!(silences[0].status.state, "active");
        assert_eq!(
            silences[0].matchers[0].to_matcher(),
            Matcher::new("alertname", "^(foo)$", true)
        );
    }

    #[tokio::test]
    async fn deletes_silence_by_id() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        client(&server)
            .delete_silence("silence-id")
            .await
            .expect("delete");
        let requests = server.requests();
        assert_eq!(requests[0].method, "DELETE");
        assert_eq!(requests[0].path, "/alertmanager/api/v2/silence/silence-id");
    }

    #[tokio::test]
    async fn posts_silence_with_expected_body() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        let matchers = vec![
            Matcher::new("level", "warning", false),
            Matcher {
                is_equal: false,
                ..Matcher::new("service_name", "foo", true)
            },
        ];
        client(&server)
            .post_silence(Some("existing-id"), &matchers)
            .await
            .expect("post");

        let requests = server.requests();
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/alertmanager/api/v2/silences");
        let body: serde_json::Value = serde_json::from_str(&requests[0].body).expect("json body");
        assert_eq!(body["id"], "existing-id");
        assert_eq!(body["createdBy"], "src-prom-wrapper");
        assert_eq!(
            body["comment"],
            "Applied via `observability.silenceAlerts` in site configuration"
        );
        assert_eq!(body["matchers"][0]["name"], "level");
        assert_eq!(body["matchers"][0]["value"], "warning");
        assert_eq!(body["matchers"][0]["isRegex"], false);
        assert!(body["matchers"][0].get("isEqual").is_none());
        assert_eq!(body["matchers"][1]["isRegex"], true);
        assert_eq!(body["matchers"][1]["isEqual"], false);
        assert!(body["startsAt"].as_str().unwrap().ends_with('Z'));
        assert!(body["endsAt"].as_str().unwrap().ends_with('Z'));
    }

    #[tokio::test]
    async fn omits_id_when_creating_a_silence() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        client(&server)
            .post_silence(None, &[Matcher::new("level", "warning", false)])
            .await
            .expect("post");
        let body: serde_json::Value =
            serde_json::from_str(&server.requests()[0].body).expect("json body");
        assert!(body.get("id").is_none());
    }

    fn valid_config() -> AmConfig {
        AmConfig {
            route: crate::amconfig::Route {
                receiver: "default".to_owned(),
                ..Default::default()
            },
            receivers: vec![crate::amconfig::Receiver {
                name: "default".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn apply_writes_config_creating_parent_and_reloads() {
        let server = MockServer::start(|_| (StatusCode::OK, String::new())).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("nested").join("alertmanager.yml");
        let client = AlertmanagerClient::new(server.base_url.clone(), config_path.clone());

        client.apply(&valid_config()).await.expect("apply");

        let written = std::fs::read_to_string(&config_path).expect("written config");
        assert!(!written.is_empty());
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/alertmanager/-/reload");
    }

    #[tokio::test]
    async fn apply_reports_reload_failures() {
        let server = MockServer::start(|_| (StatusCode::BAD_REQUEST, "nope".to_owned())).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("alertmanager.yml");
        let client = AlertmanagerClient::new(server.base_url.clone(), config_path);

        let error = client
            .apply(&valid_config())
            .await
            .expect_err("apply fails");
        assert_eq!(
            error,
            "failed to reload Alertmanager configuration: reload failed with status 400: nope"
        );
    }

    #[tokio::test]
    async fn gets_active_non_inhibited_alerts() {
        let server = MockServer::start(|request: &RecordedRequest| {
            assert_eq!(request.query, "active=true&inhibited=false");
            (StatusCode::OK, "[]".to_owned())
        })
        .await;
        let value = client(&server).alert_status().await.expect("alerts");
        assert_eq!(value, serde_json::json!([]));
    }
}
