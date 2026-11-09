#![forbid(unsafe_code)]

//! `cmd/worker/internal/telemetrygatewayexporter`: the
//! `telemetrygateway-exporter` job.
//!
//! The job owns `telemetrygatewayexporter.ConfigInst` and, when an export
//! address is configured, three `goroutine.NewPeriodicGoroutine` routines:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `telemetrygatewayexporter.exporter` | `TELEMETRY_GATEWAY_EXPORTER_EXPORT_INTERVAL` (5m) | export one batch from `telemetry_events_export_queue` |
//! | `telemetrygatewayexporter.queue_cleanup` | `TELEMETRY_GATEWAY_EXPORTER_QUEUE_CLEANUP_INTERVAL` (30m) | delete exported events older than the retention window |
//! | `telemetrygatewayexporter.queue_metrics_reporter` | 5m | report the backlog size and oldest unexported event age |
//!
//! The exporter follows `internal/telemetrygateway.Exporter`: it builds the
//! publisher identifier from `global_state` and the site configuration, opens
//! a `RecordEvents` client stream, sends the metadata message, chunks events
//! to stay below gRPC's 1 MiB message limit (`internal/grpc/chunk`), then
//! collects `succeeded_events` and marks them exported.
//!
//! Ported as-is: the config validation messages and ordering, the licensing
//! export-mode gate (`licensing.GetTelemetryEventsExportMode`, see
//! [`crate::telemetry::telemetry_events_export_mode`]), the identifier rules
//! (`cannot infer an identifer for this instance`, sic), the chunker boundary
//! (`itemSize + size >= 1 MiB` flushes first), the `request %q` error
//! wrapper, and the partial-success contract (`succeeded` is marked exported
//! even when the export call errors).
//!
//! Not ported (documented in docs §75.3): the RED metrics/observation
//! operations and the baseline's debug/info logs; the Go gRPC dialer's
//! blocking behavior (the Rust client dials lazily, so connection errors
//! surface on the first RPC as `start export: ...`, which is where the
//! baseline surfaces them too); and `env.MustGet*`'s panic on an invalid
//! value (the Rust config records an error and the worker refuses to start
//! instead of crashing).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sg_db::telemetry_events as export_queue;
use sg_metrics::prometheus::Gauge;
use sg_proto::telemetrygateway as tg;
use sqlx::PgPool;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::telemetry::{env_bool, env_exact_true, telemetry_events_export_mode};

/// `telemetryGatewayExporter.Description`.
pub const JOB_DESCRIPTION: &str =
    "A background routine that exports telemetry events to Sourcegraph's Telemetry Gateway";

pub const EXPORT_ADDRESS_VAR: &str = "TELEMETRY_GATEWAY_EXPORTER_EXPORT_ADDR";
pub const EXPORT_INTERVAL_VAR: &str = "TELEMETRY_GATEWAY_EXPORTER_EXPORT_INTERVAL";
pub const EXPORT_BATCH_SIZE_VAR: &str = "TELEMETRY_GATEWAY_EXPORTER_EXPORT_BATCH_SIZE";
pub const EXPORTED_RETENTION_VAR: &str = "TELEMETRY_GATEWAY_EXPORTER_EXPORTED_EVENTS_RETENTION";
pub const QUEUE_CLEANUP_INTERVAL_VAR: &str = "TELEMETRY_GATEWAY_EXPORTER_QUEUE_CLEANUP_INTERVAL";

const EXPORT_ADDRESS_DESCRIPTION: &str = "Target Telemetry Gateway address";
const EXPORT_INTERVAL_DESCRIPTION: &str = "Interval at which to export telemetry";
const EXPORT_BATCH_SIZE_DESCRIPTION: &str = "Maximum number of events to export in each batch";
const EXPORTED_RETENTION_DESCRIPTION: &str =
    "Duration to retain already-exported telemetry events before deleting";
const QUEUE_CLEANUP_INTERVAL_DESCRIPTION: &str =
    "Interval at which to clean up telemetry export queue";

/// `defaultTelemetryGatewayExporterAddress`.
pub const DEFAULT_EXPORT_ADDRESS: &str = "https://telemetry-gateway.sourcegraph.com:443";
/// `defaultExportInterval` (5 minutes).
pub const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// `defaultExportBatchSize`.
pub const DEFAULT_EXPORT_BATCH_SIZE: i64 = 10_000;
/// `ExportedEventsRetentionWindow`'s default (24 hours).
pub const DEFAULT_EXPORTED_RETENTION: Duration = Duration::from_secs(24 * 3600);
/// `QueueCleanupInterval`'s default (30 minutes).
pub const DEFAULT_QUEUE_CLEANUP_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// `newQueueMetricsJob`'s `time.Minute * 5`.
pub const QUEUE_METRICS_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `internal/grpc/chunk.maxMessageSize`.
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

pub const EXPORTER_NAME: &str = "telemetrygatewayexporter.exporter";
pub const EXPORTER_DESCRIPTION: &str = "telemetrygatewayexporter events export job";
pub const QUEUE_CLEANUP_NAME: &str = "telemetrygatewayexporter.queue_cleanup";
pub const QUEUE_CLEANUP_DESCRIPTION: &str = "telemetrygatewayexporter queue cleanup";
pub const QUEUE_METRICS_NAME: &str = "telemetrygatewayexporter.queue_metrics_reporter";
pub const QUEUE_METRICS_DESCRIPTION: &str = "telemetrygatewayexporter backlog metrics reporting";

pub const QUEUE_SIZE_METRIC: &str = "src_telemetrygatewayexporter_queue_size";
pub const OLDEST_UNEXPORTED_METRIC: &str = "src_telemetrygatewayexporter_oldest_unexported_event";

const INTERVAL_TOO_LONG: &str =
    "TELEMETRY_GATEWAY_EXPORTER_EXPORT_INTERVAL cannot be more than 1 hour";
const BATCH_SIZE_TOO_SMALL: &str =
    "TELEMETRY_GATEWAY_EXPORTER_EXPORT_BATCH_SIZE must be no less than 100";

/// `telemetrygatewayexporter.config` (`ConfigInst`).
#[derive(Debug, Default)]
pub struct TelemetryGatewayExporterConfig {
    export_address: Mutex<Option<url::Url>>,
    export_interval_nanos: Mutex<i64>,
    max_export_batch_size: Mutex<i64>,
    exported_retention_nanos: Mutex<i64>,
    queue_cleanup_interval_nanos: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl TelemetryGatewayExporterConfig {
    pub fn export_address(&self) -> Option<url::Url> {
        self.export_address.lock().expect("config mutex").clone()
    }

    pub fn export_interval(&self) -> Duration {
        nanos_to_duration(*self.export_interval_nanos.lock().expect("config mutex"))
    }

    pub fn export_interval_nanos(&self) -> i64 {
        *self.export_interval_nanos.lock().expect("config mutex")
    }

    pub fn max_export_batch_size(&self) -> i64 {
        *self.max_export_batch_size.lock().expect("config mutex")
    }

    pub fn exported_retention(&self) -> Duration {
        nanos_to_duration(*self.exported_retention_nanos.lock().expect("config mutex"))
    }

    pub fn exported_retention_nanos(&self) -> i64 {
        *self.exported_retention_nanos.lock().expect("config mutex")
    }

    pub fn queue_cleanup_interval(&self) -> Duration {
        nanos_to_duration(
            *self
                .queue_cleanup_interval_nanos
                .lock()
                .expect("config mutex"),
        )
    }

    pub fn queue_cleanup_interval_nanos(&self) -> i64 {
        *self
            .queue_cleanup_interval_nanos
            .lock()
            .expect("config mutex")
    }
}

fn nanos_to_duration(nanos: i64) -> Duration {
    Duration::from_nanos(nanos.max(0) as u64)
}

impl JobConfig for TelemetryGatewayExporterConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        // `env.Get`: an empty value disables export, and is not replaced by
        // the default.
        let raw_address = env.get(
            EXPORT_ADDRESS_VAR,
            DEFAULT_EXPORT_ADDRESS,
            EXPORT_ADDRESS_DESCRIPTION,
        );
        let address = if raw_address.is_empty() {
            None
        } else {
            match url::Url::parse(&raw_address) {
                Ok(url) => Some(url),
                Err(error) => {
                    errs.push(format!(
                        "TELEMETRY_GATEWAY_EXPORTER_EXPORT_ADDR value is invalid: {error}"
                    ));
                    None
                }
            }
        };
        *self.export_address.lock().expect("config mutex") = address;

        *self.export_interval_nanos.lock().expect("config mutex") = must_get_duration(
            env,
            EXPORT_INTERVAL_VAR,
            "5m0s",
            EXPORT_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self.max_export_batch_size.lock().expect("config mutex") = must_get_int(
            env,
            EXPORT_BATCH_SIZE_VAR,
            "10000",
            EXPORT_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.exported_retention_nanos.lock().expect("config mutex") = must_get_duration(
            env,
            EXPORTED_RETENTION_VAR,
            "24h0m0s",
            EXPORTED_RETENTION_DESCRIPTION,
            &mut errs,
        );
        *self
            .queue_cleanup_interval_nanos
            .lock()
            .expect("config mutex") = must_get_duration(
            env,
            QUEUE_CLEANUP_INTERVAL_VAR,
            "30m0s",
            QUEUE_CLEANUP_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        if self.export_interval() > Duration::from_secs(3600) {
            errs.push(INTERVAL_TOO_LONG.to_owned());
        }
        if self.max_export_batch_size() < 100 {
            errs.push(BATCH_SIZE_TOO_SMALL.to_owned());
        }

        *self.errs.lock().expect("config mutex") = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self.errs.lock().expect("config mutex").clone();
        match combine_errors(&errs) {
            None => Ok(()),
            Some(message) => Err(ConfigError::new(message)),
        }
    }
}

/// `env.MustGetDuration`: the baseline panics on an unparsable value. The
/// Rust worker records the same `parsing environment variable ...` message as
/// a validation error instead of crashing (docs §75.3).
fn must_get_duration(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match parse_go_duration_signed(&raw) {
        Ok(nanos) => nanos,
        Err(_) => {
            errors.push(format!(
                "parsing environment variable {name:?}. Expected valid time.Duration, got {raw:?}"
            ));
            0
        }
    }
}

/// `env.MustGetInt`.
fn must_get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!(
                "parsing environment variable {name:?}. Expected valid integer, got {raw:?}"
            ));
            0
        }
    }
}

/// `time.ParseDuration` with Go's signed `time.Duration` result in nanoseconds.
fn parse_go_duration_signed(raw: &str) -> Result<i64, String> {
    let (negative, unsigned) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let value = sg_core::parse_go_duration(unsigned)
        .map_err(|message| format!("time: invalid duration {raw:?}: {message}"))?;
    let magnitude = value.as_nanos() as i128;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).map_err(|_| format!("time: invalid duration {raw:?}"))
}

/// `telemetryGatewayExporter` (`NewJob`).
pub struct TelemetryGatewayExporter {
    config: Arc<TelemetryGatewayExporterConfig>,
}

impl TelemetryGatewayExporter {
    pub fn new() -> Self {
        Self {
            config: Arc::new(TelemetryGatewayExporterConfig::default()),
        }
    }
}

impl Default for TelemetryGatewayExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for TelemetryGatewayExporter {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        // `Routines`: a missing address disables export entirely.
        let Some(address) = self.config.export_address() else {
            return Ok(Vec::new());
        };

        let pool = deps.frontend.clone();
        let live_config = Arc::clone(&deps.live_config);

        let exporter = ExporterHandler {
            pool: pool.clone(),
            live_config: live_config.clone(),
            address,
            max_batch_size: self.config.max_export_batch_size(),
        };
        let cleanup = QueueCleanupHandler {
            pool: pool.clone(),
            retention: self.config.exported_retention(),
        };
        let metrics = QueueMetricsHandler { pool };

        Ok(vec![
            PeriodicRoutine::new(Arc::new(exporter) as Arc<dyn Handler>)
                .with_name(EXPORTER_NAME)
                .with_description(EXPORTER_DESCRIPTION)
                .with_interval(self.config.export_interval())
                .into(),
            PeriodicRoutine::new(Arc::new(cleanup) as Arc<dyn Handler>)
                .with_name(QUEUE_CLEANUP_NAME)
                .with_description(QUEUE_CLEANUP_DESCRIPTION)
                .with_interval(self.config.queue_cleanup_interval())
                .into(),
            PeriodicRoutine::new(Arc::new(metrics) as Arc<dyn Handler>)
                .with_name(QUEUE_METRICS_NAME)
                .with_description(QUEUE_METRICS_DESCRIPTION)
                .with_interval(QUEUE_METRICS_INTERVAL)
                .into(),
        ])
    }
}

/// `exporterJob.Handle`.
struct ExporterHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    address: url::Url,
    max_batch_size: i64,
}

#[async_trait]
impl Handler for ExporterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let site = self.live_config.current().site.clone();
        let mode = telemetry_events_export_mode(
            &site.license_key(),
            env_bool("SOURCEGRAPHDOTCOM_MODE"),
            env_exact_true("EXPORT_USAGE_DATA_ENABLED"),
        );
        if mode == crate::telemetry::TelemetryEventsExportMode::Disabled {
            return Ok(());
        }

        let rows = export_queue::list_for_export(&self.pool, self.max_batch_size)
            .await
            .map_err(|error| HandlerError::new(format!("ListForExport: {error}")))?;

        // The baseline unmarshals each payload and skips corrupt rows, leaving
        // them in the queue for investigation.
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            match tg::Event::decode(row.payload_pb.as_slice()) {
                Ok(event) => events.push(event),
                Err(error) => {
                    tracing::error!(
                        id = %row.id,
                        error = %error,
                        "failed to unmarshal telemetry event payload"
                    );
                }
            }
        }
        if events.is_empty() {
            return Ok(());
        }

        let (succeeded, export_error) =
            export_events(&self.pool, &self.live_config, &self.address, &events).await;

        // `MarkAsExported` receives the event IDs the server acknowledged. The
        // baseline marks every succeeded event, and logs a failure to do so
        // without failing the routine.
        if !succeeded.is_empty() {
            if let Err(error) = export_queue::mark_as_exported(&self.pool, &succeeded).await {
                tracing::error!(
                    error = %error,
                    count = succeeded.len(),
                    "failed to mark exported events as exported"
                );
            }
        }

        match export_error {
            Some(message) => Err(HandlerError::new(message)),
            None => Ok(()),
        }
    }
}

/// `exporterJob.Handle`'s call into `telemetrygateway.NewExporter` +
/// `ExportEvents`. Returns the succeeded event IDs and, separately, the
/// error message so the caller can still mark partial successes (the
/// baseline does the same).
pub async fn export_events(
    pool: &PgPool,
    live_config: &LiveConfig,
    address: &url::Url,
    events: &[tg::Event],
) -> (Vec<String>, Option<String>) {
    let identifier = match new_identifier(pool, live_config).await {
        Ok(identifier) => identifier,
        Err(message) => return (Vec::new(), Some(message)),
    };

    // `ExportEvents`: without a recording trace the request ID is a random
    // UUID.
    let request_id = uuid::Uuid::new_v4().to_string();

    match do_export_events(address, &request_id, identifier, events).await {
        Ok(succeeded) => (succeeded, None),
        Err(message) => {
            // `errors.Wrapf(err, "request %q", requestID)`.
            let succeeded = message.succeeded;
            (
                succeeded,
                Some(format!("request {request_id:?}: {}", message.error)),
            )
        }
    }
}

struct ExportFailure {
    succeeded: Vec<String>,
    error: String,
}

/// `internal/telemetrygateway.newIdentifier`.
pub async fn new_identifier(
    pool: &PgPool,
    live_config: &LiveConfig,
) -> Result<tg::Identifier, String> {
    let global_state = sg_db::global_state::get(pool)
        .await
        .map_err(|error| format!("get instance ID: {error}"))?;
    let site = &live_config.current().site;
    let external_url = site
        .raw()
        .get("externalURL")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();

    let license_key = site.license_key();
    if !license_key.is_empty() {
        return Ok(tg::Identifier {
            identifier: Some(tg::identifier::Identifier::LicensedInstance(
                tg::identifier::LicensedInstanceIdentifier {
                    license_key,
                    instance_id: global_state.site_id,
                    external_url,
                },
            )),
        });
    }

    if !global_state.site_id.is_empty() {
        return Ok(tg::Identifier {
            identifier: Some(tg::identifier::Identifier::UnlicensedInstance(
                tg::identifier::UnlicensedInstanceIdentifier {
                    instance_id: global_state.site_id,
                    external_url,
                },
            )),
        });
    }

    Err("cannot infer an identifer for this instance".to_owned())
}

/// `exporter.doExportEvents`.
async fn do_export_events(
    address: &url::Url,
    request_id: &str,
    identifier: tg::Identifier,
    events: &[tg::Event],
) -> Result<Vec<String>, ExportFailure> {
    let channel = exporter_channel(address)
        .await
        .map_err(|error| ExportFailure {
            succeeded: Vec::new(),
            error,
        })?;

    let mut client =
        tg::telemetery_gateway_service_client::TelemeteryGatewayServiceClient::new(channel);

    let chunks = chunk_events(events);
    let (tx, rx) = tokio::sync::mpsc::channel::<tg::RecordEventsRequest>(4);

    // The client-streaming call returns the response stream once the server
    // has accepted the request; the send future fills the request stream. Both
    // must be polled together or the bounded channel deadlocks.
    let (call_result, send_result) = tokio::join!(
        client.record_events(tonic::Request::new(ReceiverStream::new(rx))),
        send_requests(tx, request_id.to_owned(), identifier, chunks)
    );

    let response = match (call_result, send_result) {
        (Ok(response), Ok(())) => response,
        (Ok(response), Err(error)) => {
            // `collectResults` still drains whatever the server recorded; the
            // missing-count error it may return is discarded here, exactly
            // like the baseline.
            let mut stream = response.into_inner();
            let mut succeeded = Vec::new();
            let _ = collect_results(&mut stream, events.len(), &mut succeeded).await;
            return Err(ExportFailure { succeeded, error });
        }
        (Err(status), _) => {
            return Err(ExportFailure {
                succeeded: Vec::new(),
                error: format!("start export: {status}"),
            });
        }
    };

    let mut stream = response.into_inner();
    let mut succeeded = Vec::new();
    match collect_results(&mut stream, events.len(), &mut succeeded).await {
        Ok(()) => Ok(succeeded),
        Err(error) => Err(ExportFailure { succeeded, error }),
    }
}

/// The baseline's `chunk.Chunker`: chunks are flushed when
/// `itemSize + sizeBytes >= 1 MiB`; the final buffer is flushed at the end.
pub fn chunk_events(events: &[tg::Event]) -> Vec<Vec<tg::Event>> {
    let mut chunks = Vec::new();
    let mut buffer: Vec<tg::Event> = Vec::new();
    let mut size_bytes = 0usize;
    for event in events {
        let item_size = event.encoded_len();
        if size_bytes == 0 {
            buffer.clear();
        }
        if item_size + size_bytes >= MAX_MESSAGE_SIZE {
            chunks.push(std::mem::take(&mut buffer));
            size_bytes = 0;
        }
        buffer.push(event.clone());
        size_bytes += item_size;
    }
    if size_bytes != 0 {
        chunks.push(buffer);
    }
    chunks
}

/// Stream metadata + chunks, exactly in the baseline's order.
async fn send_requests(
    tx: tokio::sync::mpsc::Sender<tg::RecordEventsRequest>,
    request_id: String,
    identifier: tg::Identifier,
    chunks: Vec<Vec<tg::Event>>,
) -> Result<(), String> {
    send_request(
        &tx,
        tg::RecordEventsRequest {
            payload: Some(tg::record_events_request::Payload::Metadata(
                tg::RecordEventsRequestMetadata {
                    request_id,
                    identifier: Some(identifier),
                },
            )),
        },
    )
    .await
    .map_err(|error| format!("send initial metadata: {error}"))?;

    for chunk in chunks {
        send_request(
            &tx,
            tg::RecordEventsRequest {
                payload: Some(tg::record_events_request::Payload::Events(
                    tg::record_events_request::EventsPayload { events: chunk },
                )),
            },
        )
        .await
        .map_err(|error| format!("chunk and send events: {error}"))?;
    }
    Ok(())
}

async fn send_request(
    tx: &tokio::sync::mpsc::Sender<tg::RecordEventsRequest>,
    request: tg::RecordEventsRequest,
) -> Result<(), String> {
    tx.send(request)
        .await
        .map_err(|_| "stream closed".to_owned())
}

/// `collectResults`: drain `succeeded_events` and enforce
/// `len(succeeded) >= len(events)`.
async fn collect_results(
    stream: &mut tonic::Streaming<tg::RecordEventsResponse>,
    event_count: usize,
    succeeded: &mut Vec<String>,
) -> Result<(), String> {
    loop {
        match stream.message().await {
            Ok(Some(response)) => succeeded.extend(response.succeeded_events),
            Ok(None) => break,
            Err(status) => return Err(status.to_string()),
        }
    }
    if succeeded.len() < event_count {
        return Err(format!(
            "{} events did not get recorded successfully",
            event_count - succeeded.len()
        ));
    }
    Ok(())
}

/// `telemetrygateway.NewExporter`'s transport setup: insecure addresses are
/// only allowed in insecure dev mode; connections are lazy, matching the
/// baseline's non-blocking `grpc.DialContext`.
async fn exporter_channel(address: &url::Url) -> Result<Channel, String> {
    let insecure = address.scheme() != "https";
    if insecure && !insecure_dev() {
        return Err("insecure export address used outside of dev mode".to_owned());
    }
    let host = address
        .host_str()
        .ok_or_else(|| "dialing telemetry gateway: missing host".to_owned())?;
    let endpoint = match address.port() {
        Some(port) => format!("{}://{host}:{port}", address.scheme()),
        None => format!("{}://{host}", address.scheme()),
    };
    Ok(Channel::from_shared(endpoint)
        .map_err(|error| format!("dialing telemetry gateway: {error}"))?
        .connect_lazy())
}

/// `env.InsecureDev`: `strconv.ParseBool(INSECURE_DEV)`.
fn insecure_dev() -> bool {
    std::env::var("INSECURE_DEV")
        .map(|raw| matches!(raw.as_str(), "1" | "t" | "T" | "TRUE" | "true" | "True"))
        .unwrap_or(false)
}

/// `queueCleanupJob.Handle`.
struct QueueCleanupHandler {
    pool: PgPool,
    retention: Duration,
}

#[async_trait]
impl Handler for QueueCleanupHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let before = sg_db::OffsetDateTime::now_utc()
            - time::Duration::nanoseconds(self.retention.as_nanos() as i64);
        export_queue::deleted_exported(&self.pool, before)
            .await
            .map(|_| ())
            .map_err(|error| HandlerError::new(format!("store.DeletedExported: {error}")))
    }
}

/// `queueMetricsJob.Handle`.
struct QueueMetricsHandler {
    pool: PgPool,
}

#[async_trait]
impl Handler for QueueMetricsHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let (count, oldest) = export_queue::count_unexported(&self.pool)
            .await
            .map_err(|error| HandlerError::new(format!("store.CountUnexported: {error}")))?;

        let (size_gauge, oldest_gauge) = queue_gauges();
        size_gauge.set(count as f64);
        match oldest {
            Some(oldest) => {
                let age = sg_db::OffsetDateTime::now_utc() - oldest;
                oldest_gauge.set(age.as_seconds_f64());
            }
            None => oldest_gauge.set(0.0),
        }
        Ok(())
    }
}

/// `promauto.NewGauge` for the two queue gauges; created once per process
/// (registering the same name twice would panic).
fn queue_gauges() -> &'static (Gauge, Gauge) {
    static GAUGES: OnceLock<(Gauge, Gauge)> = OnceLock::new();
    GAUGES.get_or_init(|| {
        let gauge = |name: &str, help: &str| {
            let gauge = Gauge::new(name, help).expect("valid telemetry exporter gauge");
            let _ = sg_metrics::registry().register(Box::new(gauge.clone()));
            gauge
        };
        (
            gauge(
                QUEUE_SIZE_METRIC,
                "Current number of events waiting to be exported.",
            ),
            gauge(
                OLDEST_UNEXPORTED_METRIC,
                "Age of the oldest unexported event in seconds.",
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Environment variables are process-global; serialize the tests that
    /// mutate them.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_env() {
        for name in [
            EXPORT_ADDRESS_VAR,
            EXPORT_INTERVAL_VAR,
            EXPORT_BATCH_SIZE_VAR,
            EXPORTED_RETENTION_VAR,
            QUEUE_CLEANUP_INTERVAL_VAR,
        ] {
            std::env::remove_var(name);
        }
    }

    fn load() -> TelemetryGatewayExporterConfig {
        let config = TelemetryGatewayExporterConfig::default();
        config.load(&mut EnvReader::new());
        config
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = env_guard();
        clear_env();
        let config = load();
        assert_eq!(
            config.export_address().map(|url| url.to_string()),
            Some(
                url::Url::parse(DEFAULT_EXPORT_ADDRESS)
                    .expect("default address")
                    .to_string()
            )
        );
        assert_eq!(config.export_interval(), DEFAULT_EXPORT_INTERVAL);
        assert_eq!(config.max_export_batch_size(), DEFAULT_EXPORT_BATCH_SIZE);
        assert_eq!(config.exported_retention(), DEFAULT_EXPORTED_RETENTION);
        assert_eq!(
            config.queue_cleanup_interval(),
            DEFAULT_QUEUE_CLEANUP_INTERVAL
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn an_empty_address_disables_export() {
        let _guard = env_guard();
        clear_env();
        std::env::set_var(EXPORT_ADDRESS_VAR, "");
        let config = load();
        clear_env();
        assert_eq!(config.export_address(), None);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn out_of_range_values_are_validation_errors() {
        let _guard = env_guard();
        clear_env();
        std::env::set_var(EXPORT_INTERVAL_VAR, "2h");
        std::env::set_var(EXPORT_BATCH_SIZE_VAR, "99");
        let config = load();
        clear_env();
        let error = config.validate().expect_err("validate").to_string();
        assert_eq!(
            error,
            format!("2 errors occurred:\n\t* {INTERVAL_TOO_LONG}\n\t* {BATCH_SIZE_TOO_SMALL}")
        );
    }

    #[test]
    fn unparsable_duration_is_reported_instead_of_panicking() {
        let _guard = env_guard();
        clear_env();
        std::env::set_var(EXPORT_INTERVAL_VAR, "5");
        let config = load();
        clear_env();
        let error = config.validate().expect_err("validate").to_string();
        assert_eq!(
            error,
            "parsing environment variable \"TELEMETRY_GATEWAY_EXPORTER_EXPORT_INTERVAL\". \
             Expected valid time.Duration, got \"5\""
        );
    }

    #[test]
    fn chunker_flushes_at_one_mebibyte() {
        let event = |id: usize| tg::Event {
            id: format!("event-{id}"),
            ..tg::Event::default()
        };
        // Two events that together cross the boundary end up in separate
        // chunks; the trailing buffer is flushed.
        let big = tg::Event {
            id: "big".to_owned(),
            feature: "x".repeat(600 * 1024),
            ..tg::Event::default()
        };
        let chunks = chunk_events(&[big.clone(), big.clone()]);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);

        let small = chunk_events(&[event(1), event(2), event(3)]);
        assert_eq!(small.len(), 1);
        assert_eq!(small[0].len(), 3);
        assert!(chunk_events(&[]).is_empty());
    }

    #[tokio::test]
    async fn insecure_addresses_require_insecure_dev() {
        std::env::remove_var("INSECURE_DEV");
        let address = url::Url::parse("http://127.0.0.1:1234").expect("url");
        let error = exporter_channel(&address).await.expect_err("insecure");
        assert_eq!(error, "insecure export address used outside of dev mode");

        std::env::set_var("INSECURE_DEV", "true");
        assert!(exporter_channel(&address).await.is_ok());
        std::env::remove_var("INSECURE_DEV");
    }
}
