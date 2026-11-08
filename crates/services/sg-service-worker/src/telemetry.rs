#![forbid(unsafe_code)]

//! `internal/telemetry` + `internal/telemetry/telemetryrecorder`: the minimal
//! Telemetry V2 event recorder needed by backend jobs.
//!
//! An event is built exactly like `newTelemetryGatewayEvent` (feature, action,
//! server version, parameters), serialized with the pinned
//! `telemetrygateway.v1.Event` protobuf and stored by
//! `telemetrystore/teestore.StoreEvents`, which tees it into
//! `telemetry_events_export_queue` (for export to the Telemetry Gateway) and
//! the legacy `event_logs` table.
//!
//! Covered by the differential harness: event construction, feature/action
//! validation, the export-mode gate (`licensing.GetTelemetryEventsExportMode`,
//! defaulting to export-all without a license key), and both writes.
//!
//! Not ported (unused by background jobs, which record from a bare
//! `context.Background()`): trace/interaction/user/feature-flag hydration from
//! the context, private metadata and billing metadata, the sensitive-metadata
//! allowlist's redaction (it only touches private metadata), the RED metrics
//! and the `5m` insert timeouts.

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message;
use sg_config::LiveConfig;
use sg_db::telemetry_events::{
    bulk_insert_event_logs, queue_events_for_export, EventLogRow, QueuedEvent,
};
use sg_proto::telemetrygateway::{event_source, Event, EventParameters as ProtoParameters};
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

/// `version.Version()`: the baseline reads `/version.txt` over this default.
pub const VERSION: &str = sg_metrics::VERSION;

/// `telemetrygatewayv1.featureActionMaxLength`.
pub const FEATURE_ACTION_MAX_LENGTH: usize = 64;

/// `telemetrygatewayv1.ValidateEventFeatureAction`.
pub fn validate_event_feature_action(feature: &str, action: &str) -> Result<(), String> {
    if feature.is_empty() || action.is_empty() {
        return Err("'feature', 'action' must both be provided".to_owned());
    }
    if feature.len() > FEATURE_ACTION_MAX_LENGTH {
        return Err("'feature' must be less than 64 characters".to_owned());
    }
    if action.len() > FEATURE_ACTION_MAX_LENGTH {
        return Err("'action' must be less than 64 characters".to_owned());
    }
    if !matches_feature_action_regex(feature) {
        return Err(
            "'feature' must start with a lowercase letter and contain only letters, dashes, and dots"
                .to_owned(),
        );
    }
    if !matches_feature_action_regex(action) {
        return Err(
            "'action' must start with a lowercase letter and contain only letters, dashes, and dots"
                .to_owned(),
        );
    }
    Ok(())
}

/// `^[a-z][a-zA-Z-\.]+$`: a lowercase first letter followed by one or more
/// ASCII letters, dashes or dots (the trailing `+` means a single-character
/// name does not match).
fn matches_feature_action_regex(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    let mut rest = 0;
    for ch in chars {
        if !(ch.is_ascii_alphabetic() || ch == '-' || ch == '.') {
            return false;
        }
        rest += 1;
    }
    rest > 0
}

/// `telemetry.EventParameters`, restricted to the fields background jobs use.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EventParameters {
    /// `parameters.Version`.
    pub version: i32,
    /// `parameters.Metadata`, a PII-free map of numbers.
    pub metadata: BTreeMap<String, f64>,
}

/// `telemetrygatewayevent.New(ctx, now, newEventID)` for a context with no
/// trace, actor or evaluated feature flags: those fields stay unset.
pub fn new_event(
    now: OffsetDateTime,
    new_event_id: impl FnOnce() -> String,
    feature: &str,
    action: &str,
    parameters: &EventParameters,
) -> Event {
    Event {
        id: new_event_id(),
        timestamp: Some(prost_types::Timestamp {
            seconds: now.unix_timestamp(),
            nanos: now.nanosecond() as i32,
        }),
        feature: feature.to_owned(),
        action: action.to_owned(),
        source: Some(sg_proto::telemetrygateway::EventSource {
            server: Some(event_source::Server {
                version: VERSION.to_owned(),
            }),
            client: None,
        }),
        parameters: Some(ProtoParameters {
            version: parameters.version,
            legacy_metadata: Default::default(),
            metadata: parameters
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect(),
            private_metadata: None,
            billing_metadata: None,
        }),
        user: None,
        feature_flags: None,
        marketing_tracking: None,
        interaction: None,
    }
}

/// `telemetrygatewayv1.DefaultEventIDFunc`: a time-ordered (v7) UUID.
pub fn new_event_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// `licensing.TelemetryEventsExportMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryEventsExportMode {
    /// No events are queued for export.
    Disabled,
    /// All events are queued.
    All,
    /// Only features named `cody` or `cody.*` are queued.
    CodyOnly,
}

/// `licensing.newTelemetryEventsExportMode`.
///
/// The `licenseKey == ""`, dotcom and unparseable-license short-circuits are
/// ported as-is; a valid license is evaluated with the same tag rules
/// (`allow-air-gapped` disables export, `disable-telemetry-events-export`
/// keeps it cody-only, pre-2023-10-04 licenses are cody-only).
pub fn telemetry_events_export_mode(
    license_key: &str,
    dotcom: bool,
    legacy_export_usage_data: bool,
) -> TelemetryEventsExportMode {
    if env_bool("SRC_TELEMETRY_EVENTS_EXPORT_ALL") {
        return TelemetryEventsExportMode::All;
    }

    // `without licensing`.
    if license_key.is_empty() {
        return TelemetryEventsExportMode::All;
    }

    // `dotcom mode`.
    if dotcom {
        return TelemetryEventsExportMode::All;
    }

    // `without a valid license key`.
    let Ok(info) = sg_licensing::configured_license_info(license_key) else {
        return TelemetryEventsExportMode::All;
    };

    // The only way to disable export entirely.
    if info.has_tag("allow-air-gapped") {
        return TelemetryEventsExportMode::Disabled;
    }

    // License-based opt-out mechanism.
    if info
        .tags
        .iter()
        .any(|tag| tag == "disable-telemetry-events-export")
    {
        return TelemetryEventsExportMode::CodyOnly;
    }

    // Match legacy configuration.
    if legacy_export_usage_data {
        return TelemetryEventsExportMode::All;
    }

    // Pre-cutoff policy: 2023-10-04T00:00:00Z. A license without a creation
    // timestamp is treated as created at the zero time, i.e. pre-cutoff.
    let cutoff = OffsetDateTime::from_unix_timestamp(1_696_377_600)
        .expect("the cutoff timestamp is in range");
    let created_at = info
        .created_at
        .as_deref()
        .and_then(|raw| {
            OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339).ok()
        })
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    if created_at < cutoff {
        return TelemetryEventsExportMode::CodyOnly;
    }

    TelemetryEventsExportMode::All
}

/// `strconv.ParseBool` for the environment flags read by the baseline.
fn parse_go_bool(raw: &str) -> bool {
    matches!(raw, "1" | "t" | "T" | "TRUE" | "true" | "True")
}

/// `env.MustGetBool` / `dotcom.SourcegraphDotComMode` (toleration of Go's
/// boolean spellings).
fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .map(|raw| parse_go_bool(&raw))
        .unwrap_or(false)
}

/// `os.Getenv(name) == "true"` (the legacy export toggle is exact-match).
fn env_exact_true(name: &str) -> bool {
    std::env::var(name).as_deref() == Ok("true")
}

/// `telemetryrecorder.New(db)` + `EventRecorder.Record`.
#[derive(Clone)]
pub struct EventRecorder {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl EventRecorder {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }

    /// `EventRecorder.Record(ctx, feature, action, parameters)`.
    pub async fn record(
        &self,
        feature: &str,
        action: &str,
        parameters: &EventParameters,
    ) -> Result<(), String> {
        validate_event_feature_action(feature, action)
            .map_err(|error| format!("invalid event feature or action: {error}"))?;
        let event = new_event(
            OffsetDateTime::now_utc(),
            new_event_id,
            feature,
            action,
            parameters,
        );
        self.store_events(&[event]).await
    }

    /// `teestore.Store.StoreEvents`: tee into the export queue and the legacy
    /// `event_logs` table.
    pub async fn store_events(&self, events: &[Event]) -> Result<(), String> {
        let site = &self.live_config.current().site;
        let mode = telemetry_events_export_mode(
            &site.license_key(),
            env_bool("SOURCEGRAPHDOTCOM_MODE"),
            env_exact_true("EXPORT_USAGE_DATA_ENABLED"),
        );

        let queued = events_for_export(mode, events);

        let now = OffsetDateTime::now_utc();
        let rows = to_event_logs(now, events);

        // `StoreEvents` writes to both stores at the same time (`pool.New()
        // .WithErrors()`); a failure in either makes the record fail.
        let (queued_result, logs_result) = tokio::join!(
            queue_events_for_export(&self.pool, &queued),
            bulk_insert_event_logs(&self.pool, &rows),
        );
        queued_result.map_err(|error| format!("bulk inserting telemetry events: {error}"))?;
        logs_result.map_err(|error| format!("bulk inserting events logs: {error}"))?;
        Ok(())
    }
}

/// `telemetryEventsExportQueueStore.QueueForExport`'s export-mode filter plus
/// payload serialization.
pub fn events_for_export(mode: TelemetryEventsExportMode, events: &[Event]) -> Vec<QueuedEvent> {
    let filtered: Vec<&Event> = match mode {
        TelemetryEventsExportMode::Disabled => return Vec::new(),
        TelemetryEventsExportMode::All => events.iter().collect(),
        TelemetryEventsExportMode::CodyOnly => events
            .iter()
            .filter(|event| event.feature == "cody" || event.feature.starts_with("cody."))
            .collect(),
    };

    filtered
        .into_iter()
        .map(|event| QueuedEvent {
            id: event.id.clone(),
            timestamp: event
                .timestamp
                .as_ref()
                .map(|timestamp| {
                    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                        + time::Duration::nanoseconds(i64::from(timestamp.nanos))
                })
                .unwrap_or_else(OffsetDateTime::now_utc),
            payload_pb: event.encode_to_vec(),
        })
        .collect()
}

/// `teestore.toEventLogs` for events recorded outside a request context.
pub fn to_event_logs(now: OffsetDateTime, events: &[Event]) -> Vec<EventLogRow> {
    events
        .iter()
        .map(|event| {
            let user_id = event
                .user
                .as_ref()
                .and_then(|user| user.user_id)
                .unwrap_or(0);
            let anonymous_user_id = match event
                .user
                .as_ref()
                .and_then(|user| user.anonymous_user_id.as_deref())
            {
                Some(anonymous) if !(user_id == 0 && anonymous.is_empty()) => anonymous.to_owned(),
                _ => "unknown".to_owned(),
            };

            let public_argument = {
                let mut payload = serde_json::Map::new();
                if let Some(parameters) = &event.parameters {
                    for (key, value) in &parameters.metadata {
                        payload.insert(key.clone(), json_number(*value));
                    }
                }
                payload.insert(
                    "telemetry.event.exportable".to_owned(),
                    serde_json::Value::Bool(true),
                );
                if let Some(interaction) = &event.interaction {
                    payload.insert(
                        "interaction.traceID".to_owned(),
                        interaction
                            .trace_id
                            .clone()
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                    );
                }
                serde_json::Value::Object(payload)
            };

            let argument = match &event.parameters {
                Some(parameters) => match &parameters.private_metadata {
                    Some(private) => serde_json::Value::Object(
                        private
                            .fields
                            .iter()
                            .map(|(key, value)| (key.clone(), structpb_to_json(value)))
                            .collect(),
                    ),
                    None => serde_json::json!({}),
                },
                None => serde_json::json!({}),
            };

            let feature_flags = match &event.feature_flags {
                Some(flags) => serde_json::Value::Object(
                    flags
                        .flags
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.clone(),
                                serde_json::Value::Bool(value.parse::<bool>().unwrap_or(false)),
                            )
                        })
                        .collect(),
                ),
                None => serde_json::json!({}),
            };

            let (source, client) = match event
                .source
                .as_ref()
                .and_then(|source| source.client.as_ref())
            {
                Some(client) => (
                    client.name.clone(),
                    Some(format!(
                        "{}:{}",
                        client.name,
                        client.version.clone().unwrap_or_default()
                    )),
                ),
                None => ("BACKEND".to_owned(), None),
            };

            EventLogRow {
                name: format!("{}.{}", event.feature, event.action),
                // `SanitizeEventURL`: background events carry no URL.
                url: String::new(),
                user_id: user_id as i32,
                anonymous_user_id,
                source,
                argument,
                public_argument,
                version: event
                    .source
                    .as_ref()
                    .and_then(|source| source.server.as_ref())
                    .map(|server| server.version.clone())
                    .unwrap_or_default(),
                timestamp: event
                    .timestamp
                    .as_ref()
                    .map(|timestamp| {
                        OffsetDateTime::from_unix_timestamp(timestamp.seconds).unwrap_or(now)
                            + time::Duration::nanoseconds(i64::from(timestamp.nanos))
                    })
                    .unwrap_or(now),
                feature_flags,
                cohort_id: None,
                first_source_url: None,
                last_source_url: None,
                referrer: None,
                device_id: new_uuid_v4(),
                insert_id: new_uuid_v4(),
                client,
                billing_product_category: None,
                billing_event_id: None,
            }
        })
        .collect()
}

fn new_uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// `json.Marshal(float64)` prints whole numbers without a fractional part;
/// `serde_json` would print `12.0`, which is a different jsonb value.
///
/// Public so the differential harness renders decoded payload metadata with
/// the same representation as `encoding/json`.
pub fn json_number(value: f64) -> serde_json::Value {
    if value.is_finite() && value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
        serde_json::Value::Number(serde_json::Number::from(value as i64))
    } else {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

/// `structpb.Value` -> `any`, as `AsMap` would produce it.
fn structpb_to_json(value: &prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind;
    match value.kind.as_ref() {
        Some(Kind::NullValue(_)) | None => serde_json::Value::Null,
        Some(Kind::NumberValue(number)) => json_number(*number),
        Some(Kind::StringValue(string)) => serde_json::Value::String(string.clone()),
        Some(Kind::BoolValue(boolean)) => serde_json::Value::Bool(*boolean),
        Some(Kind::StructValue(object)) => serde_json::Value::Object(
            object
                .fields
                .iter()
                .map(|(key, value)| (key.clone(), structpb_to_json(value)))
                .collect(),
        ),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.iter().map(structpb_to_json).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_matches_the_baseline() {
        assert!(validate_event_feature_action("cody.llmTokenCounter", "record").is_ok());
        assert_eq!(
            validate_event_feature_action("", "record").unwrap_err(),
            "'feature', 'action' must both be provided"
        );
        assert_eq!(
            validate_event_feature_action("Cody", "record").unwrap_err(),
            "'feature' must start with a lowercase letter and contain only letters, dashes, and dots"
        );
        assert_eq!(
            validate_event_feature_action("c", "record").unwrap_err(),
            "'feature' must start with a lowercase letter and contain only letters, dashes, and dots"
        );
        assert!(validate_event_feature_action("a-b.c", "xy").is_ok());
        // The regex's trailing `+` rejects single-character names.
        assert!(validate_event_feature_action("a-b.c", "x").is_err());
        assert_eq!(
            validate_event_feature_action(&"a".repeat(65), "record").unwrap_err(),
            "'feature' must be less than 64 characters"
        );
    }

    #[test]
    fn events_carry_the_baseline_defaults() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        let event = new_event(
            now,
            || "id".to_owned(),
            "cody.llmTokenCounter",
            "record",
            &EventParameters {
                version: 0,
                metadata: BTreeMap::from([("FinalFetchAndSync".to_owned(), 0.0)]),
            },
        );

        assert_eq!(event.id, "id");
        assert_eq!(event.feature, "cody.llmTokenCounter");
        assert_eq!(
            event
                .source
                .as_ref()
                .and_then(|source| source.server.as_ref())
                .map(|server| server.version.as_str()),
            Some(sg_metrics::VERSION)
        );
        let parameters = event.parameters.expect("parameters");
        assert_eq!(parameters.version, 0);
        assert_eq!(parameters.metadata.get("FinalFetchAndSync"), Some(&0.0));
        assert!(parameters.private_metadata.is_none());
        assert!(event.user.is_none());
        assert!(event.interaction.is_none());
        assert!(event.feature_flags.is_none());
    }

    #[test]
    fn event_logs_rows_match_the_baseline_translation() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        let event = new_event(
            now,
            || "id".to_owned(),
            "cody.llmTokenCounter",
            "record",
            &EventParameters {
                version: 0,
                metadata: BTreeMap::from([
                    ("FinalFetchAndSync".to_owned(), 0.0),
                    ("openai:gpt-4:chat:input".to_owned(), 12.0),
                ]),
            },
        );

        let rows = to_event_logs(now, &[event]);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.name, "cody.llmTokenCounter.record");
        assert_eq!(row.url, "");
        assert_eq!(row.user_id, 0);
        assert_eq!(row.anonymous_user_id, "unknown");
        assert_eq!(row.source, "BACKEND");
        assert_eq!(row.argument, serde_json::json!({}));
        assert_eq!(
            row.public_argument,
            serde_json::json!({
                "FinalFetchAndSync": 0,
                "openai:gpt-4:chat:input": 12,
                "telemetry.event.exportable": true,
            })
        );
        assert_eq!(row.version, sg_metrics::VERSION);
        assert_eq!(row.feature_flags, serde_json::json!({}));
        assert!(row.cohort_id.is_none());
        assert!(row.client.is_none());
    }

    #[test]
    fn export_mode_follows_the_baseline_gate() {
        // No license key (the default OSS instance) exports everything.
        assert_eq!(
            telemetry_events_export_mode("", false, false),
            TelemetryEventsExportMode::All
        );
        // Dotcom exports everything even with a key.
        assert_eq!(
            telemetry_events_export_mode("not-a-license", true, false),
            TelemetryEventsExportMode::All
        );
        // An unparseable key is treated as "without a valid license key".
        assert_eq!(
            telemetry_events_export_mode("not-a-license", false, false),
            TelemetryEventsExportMode::All
        );
    }

    #[test]
    fn cody_only_keeps_cody_features() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        let cody = new_event(
            now,
            || "a".to_owned(),
            "cody.llmTokenCounter",
            "record",
            &EventParameters::default(),
        );
        let other = new_event(
            now,
            || "b".to_owned(),
            "search.something",
            "record",
            &EventParameters::default(),
        );

        let queued = events_for_export(TelemetryEventsExportMode::CodyOnly, &[cody.clone(), other]);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, "a");

        assert!(events_for_export(TelemetryEventsExportMode::Disabled, &[cody]).is_empty());
    }

    #[test]
    fn queued_payload_round_trips_through_the_proto() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        let event = new_event(
            now,
            || "payload-id".to_owned(),
            "cody.llmTokenCounter",
            "record",
            &EventParameters {
                version: 0,
                metadata: BTreeMap::from([("k".to_owned(), 3.0)]),
            },
        );

        let queued =
            events_for_export(TelemetryEventsExportMode::All, std::slice::from_ref(&event));
        assert_eq!(queued.len(), 1);
        let decoded = Event::decode(queued[0].payload_pb.as_slice()).expect("decode");
        assert_eq!(decoded.id, "payload-id");
        assert_eq!(decoded.feature, "cody.llmTokenCounter");
        assert_eq!(
            decoded
                .parameters
                .and_then(|parameters| parameters.metadata.get("k").copied()),
            Some(3.0)
        );
    }
}
