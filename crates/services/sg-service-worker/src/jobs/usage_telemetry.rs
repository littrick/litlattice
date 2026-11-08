#![forbid(unsafe_code)]

//! `cmd/worker/internal/telemetry`: the `export-usage-telemetry` job.
//!
//! The job only exists when `EXPORT_USAGE_DATA_ENABLED` is a Go-true value; it
//! then owns two routines:
//!
//! * `analytics.telemetry-export` (every minute) reads the allowlisted
//!   `event_logs` rows after the scrape bookmark, maps them to the BigQuery
//!   shape, publishes one Pub/Sub message and advances the bookmark. When
//!   `event_logs_scrape_state` is empty the bookmark row is seeded with
//!   `MAX(event_logs.id)`, so the first run exports nothing;
//! * `analytics.event-log-export-metrics` (every five minutes) publishes the
//!   backlog size and the configured throughput as gauges.
//!
//! Error wrapping follows the baseline exactly (`getTopicConfig`,
//! `getInstanceMetadata`, `GetBookmark`, `fetchEvents`, `sendBatch`, and the
//! unwrapped `UpdateBookmark`), as do the job/routine metadata strings.
//!
//! What is not ported: the handler's RED metrics/observation operations and
//! its logs (the baseline's `telemetryHandler` metrics and `logger.Info`
//! calls); only the two queue-size gauges are kept because they are the
//! observable output of the second routine. `sendEvents` keeps the baseline
//! contract (one JSON array per publish, Pub/Sub topic from
//! `EXPORT_USAGE_DATA_TOPIC_PROJECT`/`_TOPIC_NAME`) but publishes through the
//! Pub/Sub REST API with Application Default Credentials instead of the Go
//! client's gRPC channel; the Go client also retries and batches, neither of
//! which is reproduced here.
//!
//! `version.Version()` is the worker's build version (the baseline also reads
//! `/version.txt` at startup; the Rust services report the development
//! constant, see docs §39).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Serialize;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::EnvReader;
use sg_metrics::prometheus::Gauge;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime};
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

pub const JOB_DESCRIPTION: &str =
    "A background routine that exports usage telemetry to Sourcegraph";

pub const TELEMETRY_EXPORT_NAME: &str = "analytics.telemetry-export";
pub const TELEMETRY_EXPORT_DESCRIPTION: &str = "event logs telemetry sender";
pub const METRICS_NAME: &str = "analytics.event-log-export-metrics";
pub const METRICS_DESCRIPTION: &str = "event logs export backlog metrics";

/// `JobCooldownDuration`: the export routine's interval, and the denominator
/// of the throughput gauge.
pub const JOB_COOLDOWN_DURATION: Duration = Duration::from_secs(60);
/// The queue-size routine's interval (`time.Minute * 5`).
pub const METRICS_INTERVAL: Duration = Duration::from_secs(300);
/// `MaxEventsCountDefault`.
pub const MAX_EVENTS_COUNT_DEFAULT: i64 = 1000;

/// `disabledErr`.
pub const DISABLED_ERROR: &str = "Usage telemetry export is disabled, but the background job is attempting to execute. This means the configuration was disabled without restarting the worker service. This job is aborting, and no telemetry will be exported.";

pub const ENABLED_ENV_VAR: &str = "EXPORT_USAGE_DATA_ENABLED";
pub const TOPIC_NAME_ENV_VAR: &str = "EXPORT_USAGE_DATA_TOPIC_NAME";
pub const PROJECT_NAME_ENV_VAR: &str = "EXPORT_USAGE_DATA_TOPIC_PROJECT";

const ENABLED_DESCRIPTION: &str = "Export usage data from this Sourcegraph instance to centralized Sourcegraph analytics (requires restart).";
const TOPIC_NAME_DESCRIPTION: &str = "GCP pubsub topic name for event level data usage exporter";
const PROJECT_NAME_DESCRIPTION: &str =
    "GCP project name for pubsub topic for event level data usage exporter";

/// `src_telemetry_job_queue_size_total`.
pub const QUEUE_SIZE_METRIC: &str = "src_telemetry_job_queue_size_total";
/// `src_telemetry_job_max_throughput`.
pub const MAX_THROUGHPUT_METRIC: &str = "src_telemetry_job_max_throughput";

const PUBSUB_SCOPE: &str = "https://www.googleapis.com/auth/pubsub";
const PUBSUB_ENDPOINT: &str = "https://pubsub.googleapis.com/v1";

/// The package-level configuration the baseline reads from environment
/// variables at import time (`enabled`, `topicName`, `projectName`) plus the
/// process-wide `deploy.Type()` and `version.Version()` used for instance
/// metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSettings {
    pub enabled: bool,
    pub topic_name: String,
    pub project_name: String,
    pub deploy_type: String,
    pub version: String,
}

impl ExportSettings {
    /// Read the environment exactly like the baseline package variables.
    pub fn from_env() -> Self {
        Self::from_reader(&mut EnvReader::new())
    }

    /// `env.Get` (including the per-process override suffix) plus the
    /// package-level `strconv.ParseBool`.
    pub fn from_reader(env: &mut EnvReader) -> Self {
        let raw_enabled = env.get(ENABLED_ENV_VAR, "false", ENABLED_DESCRIPTION);
        Self {
            enabled: parse_go_bool(Some(&raw_enabled)),
            topic_name: env.get(TOPIC_NAME_ENV_VAR, "", TOPIC_NAME_DESCRIPTION),
            project_name: env.get(PROJECT_NAME_ENV_VAR, "", PROJECT_NAME_DESCRIPTION),
            deploy_type: deploy_type_from(std::env::var("DEPLOY_TYPE").ok().as_deref()),
            version: crate::telemetry::VERSION.to_owned(),
        }
    }
}

/// `strconv.ParseBool`: only these spellings are true; everything else
/// (including the empty string and unset) is false.
fn parse_go_bool(raw: Option<&str>) -> bool {
    matches!(raw, Some("1" | "t" | "T" | "TRUE" | "true" | "True"))
}

/// `deploy.Type()`: `DEPLOY_TYPE`, defaulting to `kubernetes`.
fn deploy_type_from(raw: Option<&str>) -> String {
    match raw {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => "kubernetes".to_owned(),
    }
}

/// `export-usage-telemetry`.
pub struct ExportUsageTelemetryJob {
    settings: ExportSettings,
}

impl ExportUsageTelemetryJob {
    pub fn new() -> Self {
        Self {
            settings: ExportSettings::from_env(),
        }
    }

    pub fn with_settings(settings: ExportSettings) -> Self {
        Self { settings }
    }
}

impl Default for ExportUsageTelemetryJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for ExportUsageTelemetryJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !self.settings.enabled {
            // `Routines` returns `nil, nil`; the baseline logs an info line
            // here, which is not ported.
            return Ok(Vec::new());
        }

        let handler = TelemetryHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.live_config),
            Arc::new(PubSubSender),
            self.settings.clone(),
        );
        let metrics = QueueSizeHandler::new(deps.frontend.clone(), Arc::clone(&deps.live_config));

        Ok(vec![
            PeriodicRoutine::new(Arc::new(handler) as Arc<dyn Handler>)
                .with_name(TELEMETRY_EXPORT_NAME)
                .with_description(TELEMETRY_EXPORT_DESCRIPTION)
                .with_interval(JOB_COOLDOWN_DURATION)
                .into(),
            PeriodicRoutine::new(Arc::new(metrics) as Arc<dyn Handler>)
                .with_name(METRICS_NAME)
                .with_description(METRICS_DESCRIPTION)
                .with_interval(METRICS_INTERVAL)
                .into(),
        ])
    }
}

/// `topicConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub project_name: String,
    pub topic_name: String,
}

/// `instanceMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceMetadata {
    pub deploy_type: String,
    pub version: String,
    pub site_id: String,
    pub license_key: String,
    pub initial_admin_email: String,
}

/// `bigQueryEvent`: the exported shape. Field order (and therefore JSON key
/// order) matches the baseline struct; `cohort_id`, `referrer`, `device_id`
/// and `insert_id` are omitted when empty, and `Version` is capitalised
/// exactly like the baseline.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BigQueryEvent {
    pub site_id: String,
    pub license_key: String,
    pub initial_admin_email: String,
    pub deploy_type: String,
    pub name: String,
    pub url: String,
    pub anonymous_user_id: String,
    pub first_source_url: String,
    pub last_source_url: String,
    pub user_id: i64,
    pub source: String,
    pub timestamp: String,
    #[serde(rename = "Version")]
    pub version: String,
    pub feature_flags: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cohort_id: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub referrer: String,
    pub public_argument: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert_id: Option<String>,
}

/// `sendEventsCallbackFunc`: the seam where the baseline builds a Pub/Sub
/// client and publishes.
#[async_trait]
pub trait EventSender: Send + Sync {
    async fn send(
        &self,
        events: &[BigQueryEvent],
        config: &TopicConfig,
        metadata: &InstanceMetadata,
    ) -> Result<(), String>;
}

/// The production sender: `pubsub.NewClient` + `topic.Publish(...).Get()` over
/// the Pub/Sub REST API.
pub struct PubSubSender;

#[async_trait]
impl EventSender for PubSubSender {
    async fn send(
        &self,
        events: &[BigQueryEvent],
        config: &TopicConfig,
        _metadata: &InstanceMetadata,
    ) -> Result<(), String> {
        // `pubsub.NewClient(ctx, projectName)`: Application Default Credentials.
        let credentials = google_cloud_auth::credentials::Builder::default()
            .with_scopes([PUBSUB_SCOPE])
            .build_access_token_credentials()
            .map_err(|err| format!("pubsub.NewClient: {err}"))?;
        let token = credentials
            .access_token()
            .await
            .map_err(|err| format!("pubsub.NewClient: {err}"))?
            .token;

        let payload = marshal_events(events).map_err(|err| format!("json.Marshal: {err}"))?;

        let body = serde_json::json!({
            "messages": [{
                "data": base64::engine::general_purpose::STANDARD.encode(payload.as_bytes()),
            }],
        });
        let url = format!(
            "{PUBSUB_ENDPOINT}/projects/{}/topics/{}:publish",
            config.project_name, config.topic_name
        );
        let response = reqwest::Client::new()
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("result.Get: {err}"))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "result.Get: rpc error: code = {} desc = {}",
                status.as_u16(),
                text
            ));
        }
        Ok(())
    }
}

/// `json.Marshal(toSend)` for the exporter's payload.
///
/// Go's encoder HTML-escapes `<`, `>`, `&`, U+2028 and U+2029; serde_json does
/// not, so the characters are escaped here to keep the wire payload
/// byte-identical.
pub fn marshal_events(events: &[BigQueryEvent]) -> Result<String, String> {
    let json = serde_json::to_string(events).map_err(|err| err.to_string())?;
    Ok(go_json_escape(json))
}

fn go_json_escape(json: String) -> String {
    if !json.contains(['<', '>', '&', '\u{2028}', '\u{2029}']) {
        return json;
    }
    let mut out = String::with_capacity(json.len() + 16);
    for ch in json.chars() {
        match ch {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(ch),
        }
    }
    out
}

/// `buildBigQueryObject`.
pub fn build_big_query_object(
    event: &sg_db::event_logs::EventRow,
    metadata: &InstanceMetadata,
) -> BigQueryEvent {
    BigQueryEvent {
        site_id: metadata.site_id.clone(),
        license_key: metadata.license_key.clone(),
        initial_admin_email: metadata.initial_admin_email.clone(),
        deploy_type: metadata.deploy_type.clone(),
        name: event.name.clone(),
        // "omitting URL intentionally"
        url: String::new(),
        anonymous_user_id: event.anonymous_user_id.clone(),
        first_source_url: event.first_source_url.clone().unwrap_or_default(),
        last_source_url: event.last_source_url.clone().unwrap_or_default(),
        user_id: i64::from(event.user_id),
        source: event.source.clone(),
        timestamp: rfc3339_seconds(event.timestamp),
        version: event.version.clone(),
        feature_flags: feature_flags_json(&event.feature_flags),
        cohort_id: event.cohort_id.map(cohort_date_string),
        referrer: event.referrer.clone().unwrap_or_default(),
        public_argument: event.public_argument.clone(),
        device_id: event.device_id.clone(),
        insert_id: event.insert_id.clone(),
    }
}

/// `string(event.EvaluatedFlagSet.Json())`: a nil map marshals to `null`.
fn feature_flags_json(flags: &Option<BTreeMap<String, bool>>) -> String {
    match flags {
        None => "null".to_owned(),
        Some(flags) => serde_json::to_string(flags).expect("flag map serializes"),
    }
}

/// `time.Time.Format(time.RFC3339)`: the layout has no fractional seconds, so
/// the sub-second part is dropped.
fn rfc3339_seconds(timestamp: OffsetDateTime) -> String {
    timestamp
        .replace_nanosecond(0)
        .expect("zero nanoseconds are valid")
        .format(&Rfc3339)
        .expect("valid RFC3339 timestamp")
}

/// A `date` column scanned into a Go string arrives as `time.RFC3339Nano`
/// (`2022-01-02T00:00:00Z`); reproduce that.
fn cohort_date_string(date: Date) -> String {
    format!("{date}T00:00:00Z")
}

/// `getBatchSize`: `exportUsageTelemetry.batchSize` when positive, else
/// [`MAX_EVENTS_COUNT_DEFAULT`].
pub fn batch_size(site: &SiteConfiguration) -> i64 {
    site.raw()
        .get("exportUsageTelemetry")
        .and_then(|config| config.get("batchSize"))
        .and_then(serde_json::Value::as_i64)
        .filter(|size| *size > 0)
        .unwrap_or(MAX_EVENTS_COUNT_DEFAULT)
}

/// `telemetryHandler`.
pub struct TelemetryHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    sender: Arc<dyn EventSender>,
    settings: ExportSettings,
}

impl TelemetryHandler {
    pub fn new(
        pool: PgPool,
        live_config: Arc<LiveConfig>,
        sender: Arc<dyn EventSender>,
        settings: ExportSettings,
    ) -> Self {
        Self {
            pool,
            live_config,
            sender,
            settings,
        }
    }

    /// `getTopicConfig`: the topic is checked before the project.
    pub fn topic_config(&self) -> Result<TopicConfig, String> {
        if self.settings.topic_name.is_empty() {
            return Err("missing topic name to export usage data".to_owned());
        }
        if self.settings.project_name.is_empty() {
            return Err("missing project name to export usage data".to_owned());
        }
        Ok(TopicConfig {
            project_name: self.settings.project_name.clone(),
            topic_name: self.settings.topic_name.clone(),
        })
    }

    /// `getInstanceMetadata`.
    pub async fn instance_metadata(&self) -> Result<InstanceMetadata, String> {
        let site_id = sg_db::global_state::get(&self.pool)
            .await
            .map_err(|err| format!("getInstanceMetadata.getSiteId: {err}"))?
            .site_id;
        let (initial_admin_email, _) = sg_db::user_emails::get_initial_site_admin_info(&self.pool)
            .await
            .map_err(|err| format!("getInstanceMetadata.getInitialAdminEmail: {err}"))?;
        Ok(InstanceMetadata {
            deploy_type: self.settings.deploy_type.clone(),
            version: self.settings.version.clone(),
            site_id,
            license_key: self.live_config.current().site.license_key(),
            initial_admin_email,
        })
    }
}

#[async_trait]
impl Handler for TelemetryHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        if !self.settings.enabled {
            return Err(HandlerError::new(DISABLED_ERROR));
        }

        let topic_config = self
            .topic_config()
            .map_err(|err| HandlerError::new(format!("getTopicConfig: {err}")))?;

        let metadata = self
            .instance_metadata()
            .await
            .map_err(|err| HandlerError::new(format!("getInstanceMetadata: {err}")))?;

        let batch_size = batch_size(&self.live_config.current().site);

        let bookmark = sg_db::event_logs::get_bookmark(&self.pool)
            .await
            .map_err(|err| HandlerError::new(format!("GetBookmark: {err}")))?;

        let events = sg_db::event_logs::list_exportable_events(&self.pool, bookmark, batch_size)
            .await
            .map_err(|err| HandlerError::new(format!("fetchEvents: {err}")))?;
        if events.is_empty() {
            return Ok(());
        }

        let max_id = events.last().expect("non-empty batch").id;
        let max_id = i32::try_from(max_id).map_err(|_| {
            HandlerError::new(format!(
                "maxId {max_id} does not fit the bookmark integer column"
            ))
        })?;

        let objects: Vec<BigQueryEvent> = events
            .iter()
            .map(|event| build_big_query_object(event, &metadata))
            .collect();
        self.sender
            .send(&objects, &topic_config, &metadata)
            .await
            .map_err(|err| HandlerError::new(format!("sendBatch: {err}")))?;

        sg_db::event_logs::update_bookmark(&self.pool, max_id)
            .await
            .map_err(HandlerError::new)?;
        Ok(())
    }
}

/// `queueSizeJob`: publishes the backlog and configured throughput gauges.
pub struct QueueSizeHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl QueueSizeHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

#[async_trait]
impl Handler for QueueSizeHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let bookmark = sg_db::event_logs::get_bookmark(&self.pool)
            .await
            .map_err(|err| HandlerError::new(format!("queueSizeJob.GetBookmark: {err}")))?;
        let size = sg_db::event_logs::count_export_queue(&self.pool, bookmark)
            .await
            .map_err(|err| HandlerError::new(format!("queueSizeJob.GetCount: {err}")))?;

        let (size_gauge, throughput_gauge) = queue_gauges();
        size_gauge.set(size as f64);
        let batch_size = batch_size(&self.live_config.current().site);
        throughput_gauge.set(batch_size as f64 / JOB_COOLDOWN_DURATION.as_secs() as f64);
        Ok(())
    }
}

/// `promauto.NewGauge` for the two `src_telemetry_job_*` gauges; created once
/// per process (registering the same name twice would panic).
fn queue_gauges() -> &'static (Gauge, Gauge) {
    static GAUGES: OnceLock<(Gauge, Gauge)> = OnceLock::new();
    GAUGES.get_or_init(|| {
        let gauge = |name: &str, help: &str| {
            let gauge = Gauge::new(name, help).expect("valid telemetry gauge");
            let _ = sg_metrics::registry().register(Box::new(gauge.clone()));
            gauge
        };
        (
            gauge(
                QUEUE_SIZE_METRIC,
                "Current number of events waiting to be scraped.",
            ),
            gauge(
                MAX_THROUGHPUT_METRIC,
                "Currently configured maximum throughput per second.",
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(enabled: bool, topic: &str, project: &str) -> ExportSettings {
        ExportSettings {
            enabled,
            topic_name: topic.to_owned(),
            project_name: project.to_owned(),
            deploy_type: "test-deploy".to_owned(),
            version: "test-version".to_owned(),
        }
    }

    #[test]
    fn go_bool_parsing() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert!(parse_go_bool(Some(raw)), "{raw}");
        }
        for raw in ["0", "f", "FALSE", "false", "", "yes", "on"] {
            assert!(!parse_go_bool(Some(raw)), "{raw}");
        }
        assert!(!parse_go_bool(None));
    }

    #[test]
    fn deploy_type_defaults_to_kubernetes() {
        assert_eq!(deploy_type_from(None), "kubernetes");
        assert_eq!(deploy_type_from(Some("")), "kubernetes");
        assert_eq!(
            deploy_type_from(Some("docker-container")),
            "docker-container"
        );
    }

    #[test]
    fn batch_size_defaults_and_overrides() {
        let default = SiteConfiguration::parse(r#"{"exportUsageTelemetry":{}}"#).expect("jsonc");
        assert_eq!(batch_size(&default), MAX_EVENTS_COUNT_DEFAULT);

        let zero =
            SiteConfiguration::parse(r#"{"exportUsageTelemetry":{"batchSize":0}}"#).expect("jsonc");
        assert_eq!(batch_size(&zero), MAX_EVENTS_COUNT_DEFAULT);

        let five =
            SiteConfiguration::parse(r#"{"exportUsageTelemetry":{"batchSize":5}}"#).expect("jsonc");
        assert_eq!(batch_size(&five), 5);

        assert_eq!(
            batch_size(&SiteConfiguration::default()),
            MAX_EVENTS_COUNT_DEFAULT
        );
    }

    fn metadata() -> InstanceMetadata {
        InstanceMetadata {
            deploy_type: "docker".to_owned(),
            version: "1.2.3".to_owned(),
            site_id: "site-id-1".to_owned(),
            license_key: "license-key-1".to_owned(),
            initial_admin_email: "admin@place.com".to_owned(),
        }
    }

    fn event() -> sg_db::event_logs::EventRow {
        sg_db::event_logs::EventRow {
            id: 1,
            name: "GREAT_EVENT".to_owned(),
            user_id: 5,
            anonymous_user_id: "anonymous".to_owned(),
            source: "src".to_owned(),
            public_argument: r#"{"foo": "bar"}"#.to_owned(),
            version: "1.1.1".to_owned(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_658_448_000).expect("timestamp"),
            feature_flags: Some(BTreeMap::from([("testflag".to_owned(), true)])),
            cohort_id: Some(Date::from_calendar_date(2022, time::Month::January, 2).expect("date")),
            first_source_url: Some("first_source_url".to_owned()),
            last_source_url: Some("last_source_url".to_owned()),
            referrer: Some("reff".to_owned()),
            device_id: Some("devid".to_owned()),
            insert_id: Some("insertid".to_owned()),
        }
    }

    #[test]
    fn big_query_object_matches_the_baseline_shape() {
        let object = build_big_query_object(&event(), &metadata());
        assert_eq!(object.site_id, "site-id-1");
        assert_eq!(object.version, "1.1.1");
        assert_eq!(object.url, "", "URL is omitted intentionally");
        assert_eq!(object.timestamp, "2022-07-22T00:00:00Z");
        assert_eq!(object.feature_flags, r#"{"testflag":true}"#);
        assert_eq!(object.cohort_id.as_deref(), Some("2022-01-02T00:00:00Z"));
        assert_eq!(object.referrer, "reff");
        assert_eq!(object.public_argument, r#"{"foo": "bar"}"#);

        let json = marshal_events(&[object]).expect("marshal");
        assert_eq!(
            json,
            r#"[{"site_id":"site-id-1","license_key":"license-key-1","initial_admin_email":"admin@place.com","deploy_type":"docker","name":"GREAT_EVENT","url":"","anonymous_user_id":"anonymous","first_source_url":"first_source_url","last_source_url":"last_source_url","user_id":5,"source":"src","timestamp":"2022-07-22T00:00:00Z","Version":"1.1.1","feature_flags":"{\"testflag\":true}","cohort_id":"2022-01-02T00:00:00Z","referrer":"reff","public_argument":"{\"foo\": \"bar\"}","device_id":"devid","insert_id":"insertid"}]"#
        );
    }

    #[test]
    fn nil_optional_fields_follow_go_omitempty() {
        let mut event = event();
        event.feature_flags = None;
        event.cohort_id = None;
        event.referrer = Some(String::new());
        event.first_source_url = None;
        event.last_source_url = None;
        event.device_id = None;
        event.insert_id = None;

        let object = build_big_query_object(&event, &metadata());
        assert_eq!(object.feature_flags, "null");
        assert_eq!(object.first_source_url, "");
        assert_eq!(object.referrer, "");
        assert!(object.cohort_id.is_none());
        assert!(object.device_id.is_none());

        let json = marshal_events(&[object]).expect("marshal");
        assert!(!json.contains("cohort_id"), "{json}");
        assert!(!json.contains("referrer"), "{json}");
        assert!(!json.contains("device_id"), "{json}");
        assert!(!json.contains("insert_id"), "{json}");
        assert!(json.contains(r#""first_source_url":"""#), "{json}");
    }

    #[test]
    fn go_json_html_escaping() {
        assert_eq!(
            go_json_escape(r#"{"a":"<b>"}"#.to_owned()),
            r#"{"a":"\u003cb\u003e"}"#
        );
        assert_eq!(
            go_json_escape(r#"{"a":"x&y"}"#.to_owned()),
            r#"{"a":"x\u0026y"}"#
        );
        let with_line_separator = format!("{{\"a\":\"{}\"}}", '\u{2028}');
        assert_eq!(go_json_escape(with_line_separator), r#"{"a":"\u2028"}"#);
        assert_eq!(go_json_escape("plain".to_owned()), "plain");
    }

    #[test]
    fn timestamp_drops_sub_second_precision() {
        let timestamp = OffsetDateTime::from_unix_timestamp(1_658_448_000)
            .expect("timestamp")
            .replace_nanosecond(123_000_000)
            .expect("nanoseconds");
        assert_eq!(rfc3339_seconds(timestamp), "2022-07-22T00:00:00Z");
    }

    #[tokio::test]
    async fn topic_configuration_order() {
        let live_config = Arc::new(LiveConfig::new(sg_config::Unified {
            site: SiteConfiguration::default(),
            service_connections: sg_config::ServiceConnections::default(),
        }));
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@127.0.0.1:1/db")
            .expect("lazy pool");

        let missing_both = TelemetryHandler::new(
            pool.clone(),
            Arc::clone(&live_config),
            Arc::new(PubSubSender),
            settings(true, "", ""),
        );
        assert_eq!(
            missing_both.topic_config().unwrap_err(),
            "missing topic name to export usage data"
        );

        let missing_project = TelemetryHandler::new(
            pool,
            live_config,
            Arc::new(PubSubSender),
            settings(true, "topic", ""),
        );
        assert_eq!(
            missing_project.topic_config().unwrap_err(),
            "missing project name to export usage data"
        );
    }
}
