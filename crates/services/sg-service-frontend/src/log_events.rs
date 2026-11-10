#![forbid(unsafe_code)]

//! `Mutation.logEvent` / `Mutation.logEvents` / `Mutation.logUserEvent`
//! (`cmd/frontend/graphqlbackend/user_usage_stats.go:88-352`).
//!
//! These are the deprecated write paths still present in the SDL. They do not
//! create new storage: each event is either classified as an aggregate-only
//! `search.latencies.frontend.*` / `search.ranking.*` event (Prometheus
//! histogram, never persisted) or appended to the legacy `event_logs` table
//! through [`sg_db::telemetry_events::bulk_insert_event_logs`] (M6 contract).
//! `SearchResultsQueried` and `findReferences` additionally set the
//! `user_activity:searchoccurred` / `user_activity:findrefsoccurred` Redis
//! flags, which already exist in the baseline contract.
//!
//! `logUserEvent` has been a no-op since PR 38826 and needs no prefetch.

use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Value};
use sg_config::SiteConfiguration;
use sg_db::telemetry_events::{bulk_insert_event_logs, EventLogRow};
use sg_graphql::exec::{Arguments, InputValue, RootField};
use sqlx::PgPool;
use time::format_description::well_known::Iso8601;
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::log_events_metrics;

/// `conf.EventLoggingEnabled()` (`internal/conf/computed.go:555-561`): the
/// empty string and `"enabled"` enable logging, every other value disables
/// it.
pub fn event_logging_enabled(site: &SiteConfiguration) -> bool {
    let value = site
        .raw()
        .get("experimentalFeatures")
        .and_then(|features| features.get("eventLogging"))
        .and_then(Value::as_str)
        .unwrap_or("");
    value.is_empty() || value == "enabled"
}

/// `conf.ExternalURL()`: the site configuration's `externalURL` (empty when
/// unset; `schema.go:3514` removes it from the defaults).
pub fn external_url(site: &SiteConfiguration) -> &str {
    site.raw()
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// One `Event` GraphQL input (`schema.graphql:903-1013`).
#[derive(Debug, Clone, Default)]
pub struct LogEventInput {
    pub event: String,
    pub user_cookie_id: String,
    pub first_source_url: Option<String>,
    pub last_source_url: Option<String>,
    pub url: String,
    pub source: String,
    pub cohort_id: Option<String>,
    pub referrer: Option<String>,
    pub original_referrer: Option<String>,
    pub session_referrer: Option<String>,
    pub session_first_url: Option<String>,
    pub device_session_id: Option<String>,
    pub argument: Option<String>,
    pub public_argument: Option<String>,
    pub user_properties: Option<String>,
    pub device_id: Option<String>,
    pub event_id: Option<i64>,
    pub insert_id: Option<String>,
    pub client: Option<String>,
    pub billing_product_category: Option<String>,
    pub billing_event_id: Option<String>,
    pub connected_site_id: Option<String>,
    pub hashed_license_key: Option<String>,
}

fn required<'a>(arguments: &'a Arguments, name: &str) -> Result<&'a str, String> {
    arguments
        .string(name)
        .ok_or_else(|| format!("missing required argument `{name}`"))
}

fn optional_string(value: Option<&InputValue>) -> Option<String> {
    value.and_then(InputValue::as_str).map(str::to_owned)
}

fn from_object(
    map: &std::collections::BTreeMap<String, InputValue>,
) -> Result<LogEventInput, String> {
    let required = |name: &str| {
        map.get(name)
            .and_then(InputValue::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("missing required argument `{name}`"))
    };
    Ok(LogEventInput {
        event: required("event")?,
        user_cookie_id: required("userCookieID")?,
        first_source_url: optional_string(map.get("firstSourceURL")),
        last_source_url: optional_string(map.get("lastSourceURL")),
        url: required("url")?,
        source: required("source")?,
        cohort_id: optional_string(map.get("cohortID")),
        referrer: optional_string(map.get("referrer")),
        original_referrer: optional_string(map.get("originalReferrer")),
        session_referrer: optional_string(map.get("sessionReferrer")),
        session_first_url: optional_string(map.get("sessionFirstURL")),
        device_session_id: optional_string(map.get("deviceSessionID")),
        argument: optional_string(map.get("argument")),
        public_argument: optional_string(map.get("publicArgument")),
        user_properties: optional_string(map.get("userProperties")),
        device_id: optional_string(map.get("deviceID")),
        event_id: map.get("eventID").and_then(InputValue::as_i64),
        insert_id: optional_string(map.get("insertID")),
        client: optional_string(map.get("client")),
        billing_product_category: optional_string(map.get("billingProductCategory")),
        billing_event_id: optional_string(map.get("billingEventID")),
        connected_site_id: optional_string(map.get("connectedSiteID")),
        hashed_license_key: optional_string(map.get("hashedLicenseKey")),
    })
}

impl LogEventInput {
    /// `logEvent(...)`'s flat argument list (graph-gophers decodes it into the
    /// `Event` struct).
    pub fn from_arguments(arguments: &Arguments) -> Result<Self, String> {
        Ok(LogEventInput {
            event: required(arguments, "event")?.to_owned(),
            user_cookie_id: required(arguments, "userCookieID")?.to_owned(),
            first_source_url: arguments.string("firstSourceURL").map(str::to_owned),
            last_source_url: arguments.string("lastSourceURL").map(str::to_owned),
            url: required(arguments, "url")?.to_owned(),
            source: required(arguments, "source")?.to_owned(),
            cohort_id: arguments.string("cohortID").map(str::to_owned),
            referrer: arguments.string("referrer").map(str::to_owned),
            original_referrer: arguments.string("originalReferrer").map(str::to_owned),
            session_referrer: arguments.string("sessionReferrer").map(str::to_owned),
            session_first_url: arguments.string("sessionFirstURL").map(str::to_owned),
            device_session_id: arguments.string("deviceSessionID").map(str::to_owned),
            argument: arguments.string("argument").map(str::to_owned),
            public_argument: arguments.string("publicArgument").map(str::to_owned),
            user_properties: arguments.string("userProperties").map(str::to_owned),
            device_id: arguments.string("deviceID").map(str::to_owned),
            event_id: arguments.i64("eventID"),
            insert_id: arguments.string("insertID").map(str::to_owned),
            client: arguments.string("client").map(str::to_owned),
            billing_product_category: arguments
                .string("billingProductCategory")
                .map(str::to_owned),
            billing_event_id: arguments.string("billingEventID").map(str::to_owned),
            connected_site_id: arguments.string("connectedSiteID").map(str::to_owned),
            hashed_license_key: arguments.string("hashedLicenseKey").map(str::to_owned),
        })
    }

    /// `logEvents(events: [Event!])`: `args.Events == nil` (argument absent or
    /// `null`) is `Ok(None)`, which the baseline treats as a no-op.
    pub fn from_batch_arguments(arguments: &Arguments) -> Result<Option<Vec<Self>>, String> {
        match arguments.get("events") {
            None | Some(InputValue::Null) => Ok(None),
            Some(InputValue::List(items)) => {
                let mut events = Vec::with_capacity(items.len());
                for item in items {
                    let InputValue::Object(map) = item else {
                        return Err("events: expected an object".to_owned());
                    };
                    events.push(from_object(map)?);
                }
                Ok(Some(events))
            }
            Some(_) => Err("events: expected a list".to_owned()),
        }
    }
}

/// The database, Redis and configuration seams of the log-event writes.
pub struct LogEventsDeps<'a> {
    pub pool: &'a PgPool,
    /// `redispool.Store`; the frontend passes `None` when Redis was
    /// unreachable at startup, which makes the all-time flags fail like a
    /// broken redigo pool would.
    pub store: Option<&'a sg_redis::KeyValue>,
    pub site: &'a SiteConfiguration,
    /// `actor.FromContext(ctx).UID` (0 for anonymous).
    pub uid: i32,
}

/// `user_activity:searchoccurred` (`internal/usagestats/all_time_stats.go`).
const SEARCH_OCCURRED_KEY: &str = "user_activity:searchoccurred";
/// `user_activity:findrefsoccurred`.
const FIND_REFS_OCCURRED_KEY: &str = "user_activity:findrefsoccurred";

static SEARCH_OCCURRED: AtomicBool = AtomicBool::new(false);
static FIND_REFS_OCCURRED: AtomicBool = AtomicBool::new(false);

/// `logSiteSearchOccurred`: the process-wide CAS means the key is written at
/// most once per process; a failed write still consumes the CAS (the baseline
/// sets the `int32` before checking the Redis error).
async fn log_site_search_occurred(store: Option<&sg_redis::KeyValue>) -> Result<(), String> {
    if SEARCH_OCCURRED.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    set_flag(store, SEARCH_OCCURRED_KEY).await
}

/// `logSiteFindRefsOccurred`, same contract.
async fn log_site_find_refs_occurred(store: Option<&sg_redis::KeyValue>) -> Result<(), String> {
    if FIND_REFS_OCCURRED.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    set_flag(store, FIND_REFS_OCCURRED_KEY).await
}

async fn set_flag(store: Option<&sg_redis::KeyValue>, key: &str) -> Result<(), String> {
    let Some(store) = store else {
        return Err("redis store is not available".to_owned());
    };
    store.set(key, "true").await.map_err(|err| err.to_string())
}

/// `decode(v *string)`: nil stays nil; any other string must be valid JSON.
/// The error class (invalid JSON aborts the whole mutation) is preserved; the
/// exact text comes from serde rather than Go's `encoding/json`.
fn decode(raw: Option<&str>) -> Result<Option<Value>, String> {
    match raw {
        None => Ok(None),
        Some(text) => serde_json::from_str(text)
            .map(Some)
            .map_err(|err| err.to_string()),
    }
}

/// `SanitizeEventURL` (`internal/database/event_logs.go:184-202`).
pub fn sanitize_event_url(raw: &str, external_url: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let Ok(parsed) = url::Url::parse(raw) else {
        return String::new();
    };
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return String::new();
    }
    let normalized = parsed.as_str();
    let on_site = !external_url.is_empty() && normalized.starts_with(external_url);
    let sourcegraph_host = parsed
        .host_str()
        .is_some_and(|host| host.ends_with("sourcegraph.com"));
    if on_site || sourcegraph_host {
        normalized.to_owned()
    } else {
        String::new()
    }
}

/// `ensureUuid`: a missing or empty ID gets a fresh v4 UUID.
fn ensure_uuid(raw: Option<&str>) -> String {
    match raw {
        None | Some("") => Uuid::new_v4().to_string(),
        Some(value) => value.to_owned(),
    }
}

/// The `date` column; the baseline hands the string to Postgres.
fn parse_cohort(raw: Option<&str>) -> Result<Option<Date>, String> {
    match raw {
        None => Ok(None),
        Some(value) => Date::parse(value, &Iso8601::DATE)
            .map(Some)
            .map_err(|err| format!("cohortID: {err}")),
    }
}

/// A decoded non-metric event, before the Redis side effects and row build.
struct PreparedEvent<'a> {
    input: &'a LogEventInput,
    argument: Option<Value>,
    public_argument: Option<Value>,
}

/// `schemaResolver.LogEvents` + `usagestats.LogEvents` + `logLocalEvents`:
///
/// 1. the configuration gate;
/// 2. `search.latencies.frontend.*` / `search.ranking.*` events are validated
///    and exported to Prometheus, then skipped (never persisted);
/// 3. every other event's arguments are JSON-validated (an error aborts the
///    whole mutation before any side effect);
/// 4. in a second pass the Redis all-time flags are written and the
///    `event_logs` rows are built;
/// 5. all rows are inserted in one statement.
pub async fn log_events(deps: &LogEventsDeps<'_>, events: &[LogEventInput]) -> Result<(), String> {
    if !event_logging_enabled(deps.site) {
        return Ok(());
    }

    let mut prepared: Vec<PreparedEvent<'_>> = Vec::with_capacity(events.len());
    for input in events {
        if input.event.starts_with("search.latencies.frontend.") {
            let argument = decode(input.argument.as_deref())?;
            log_events_metrics::observe_search_latencies(&input.event, &argument);
            continue;
        }
        if input.event.starts_with("search.ranking.") {
            let argument = decode(input.argument.as_deref())?;
            log_events_metrics::observe_search_ranking(&argument);
            continue;
        }
        let argument = decode(input.argument.as_deref())?;
        let public_argument = decode(input.public_argument.as_deref())?;
        // `userProperties` is validated but never persisted.
        decode(input.user_properties.as_deref())?;
        prepared.push(PreparedEvent {
            input,
            argument,
            public_argument,
        });
    }

    let external_url = external_url(deps.site);
    let mut rows = Vec::with_capacity(prepared.len());
    for prepared_event in &prepared {
        let input = prepared_event.input;
        if input.event == "SearchResultsQueried" {
            log_site_search_occurred(deps.store).await?;
        }
        if input.event == "findReferences" {
            log_site_find_refs_occurred(deps.store).await?;
        }
        rows.push(EventLogRow {
            name: input.event.clone(),
            url: sanitize_event_url(&input.url, external_url),
            user_id: deps.uid,
            anonymous_user_id: input.user_cookie_id.clone(),
            source: input.source.clone(),
            argument: prepared_event.argument.clone().unwrap_or_else(|| json!({})),
            public_argument: prepared_event
                .public_argument
                .clone()
                .unwrap_or_else(|| json!({})),
            version: sg_metrics::VERSION.to_owned(),
            timestamp: OffsetDateTime::now_utc(),
            // `json.Marshal(nil EvaluatedFlagSet)` is the JSON literal `null`;
            // the frontend has no feature-flag middleware to evaluate.
            feature_flags: Value::Null,
            cohort_id: parse_cohort(input.cohort_id.as_deref())?,
            first_source_url: input.first_source_url.clone(),
            last_source_url: input.last_source_url.clone(),
            referrer: input.referrer.clone(),
            device_id: ensure_uuid(input.device_id.as_deref()),
            insert_id: ensure_uuid(input.insert_id.as_deref()),
            client: input.client.clone(),
            billing_product_category: input.billing_product_category.clone(),
            billing_event_id: input.billing_event_id.clone(),
        });
    }

    bulk_insert_event_logs(deps.pool, &rows)
        .await
        .map_err(|err| err.to_string())
}

/// A root-field outcome the [`crate::settings_mutation::MutationRoot`] replays
/// during execution.
pub type LogEventsOutcome = Result<(), String>;

/// `Mutation.logEvent` / `Mutation.logEvents` prefetch: the executor resolves
/// fields synchronously, so the writes happen before execution. Results are
/// keyed by the root field name.
pub async fn prefetch_log_events(
    pool: &PgPool,
    store: Option<&sg_redis::KeyValue>,
    site: &SiteConfiguration,
    uid: i32,
    fields: &[RootField],
) -> Vec<(String, LogEventsOutcome)> {
    let mut outcomes = Vec::new();
    let deps = LogEventsDeps {
        pool,
        store,
        site,
        uid,
    };
    for field in fields {
        match field.name.as_str() {
            "logEvent" => {
                let outcome = match LogEventInput::from_arguments(&field.arguments) {
                    Ok(event) => log_events(&deps, std::slice::from_ref(&event)).await,
                    Err(err) => Err(err),
                };
                outcomes.push(("logEvent".to_owned(), outcome));
            }
            "logEvents" => {
                let outcome = match LogEventInput::from_batch_arguments(&field.arguments) {
                    Ok(Some(events)) => log_events(&deps, &events).await,
                    Ok(None) => Ok(()),
                    Err(err) => Err(err),
                };
                outcomes.push(("logEvents".to_owned(), outcome));
            }
            _ => {}
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(raw: Value) -> SiteConfiguration {
        SiteConfiguration::parse(&raw.to_string()).expect("site config")
    }

    #[test]
    fn event_logging_gate_matches_the_baseline() {
        assert!(event_logging_enabled(&site(json!({}))));
        assert!(event_logging_enabled(&site(
            json!({"experimentalFeatures": {}})
        )));
        assert!(event_logging_enabled(&site(
            json!({"experimentalFeatures": {"eventLogging": ""}})
        )));
        assert!(event_logging_enabled(&site(
            json!({"experimentalFeatures": {"eventLogging": "enabled"}})
        )));
        assert!(!event_logging_enabled(&site(
            json!({"experimentalFeatures": {"eventLogging": "disabled"}})
        )));
    }

    #[test]
    fn sanitize_keeps_only_this_site_and_sourcegraph_com_urls() {
        let external = "https://sourcegraph.example.com";
        assert_eq!(
            sanitize_event_url("https://sourcegraph.example.com/search?q=x", external),
            "https://sourcegraph.example.com/search?q=x"
        );
        assert_eq!(
            sanitize_event_url("https://foo.sourcegraph.com/search?q=x", ""),
            "https://foo.sourcegraph.com/search?q=x"
        );
        assert_eq!(
            sanitize_event_url("https://example.com/search?q=x", external),
            ""
        );
        // The baseline checks a plain string prefix (`strings.HasPrefix`), so
        // this URL is kept even though it is a different host.
        assert_eq!(
            sanitize_event_url("https://sourcegraph.example.com.evil.com/x", external),
            "https://sourcegraph.example.com.evil.com/x"
        );
        assert_eq!(
            sanitize_event_url("ftp://sourcegraph.example.com/x", external),
            ""
        );
        assert_eq!(sanitize_event_url("javascript:alert(1)", external), "");
        assert_eq!(sanitize_event_url("not a url", external), "");
        assert_eq!(sanitize_event_url("", external), "");
    }

    #[test]
    fn ensure_uuid_reuses_given_ids() {
        assert_eq!(ensure_uuid(Some("given-id")), "given-id");
        let generated = ensure_uuid(None);
        assert!(Uuid::parse_str(&generated).is_ok());
        let empty = ensure_uuid(Some(""));
        assert!(Uuid::parse_str(&empty).is_ok());
        assert_ne!(generated, empty);
    }

    #[test]
    fn decode_rejects_invalid_json() {
        assert_eq!(decode(None).expect("nil"), None);
        assert_eq!(decode(Some("null")).expect("null"), Some(Value::Null));
        assert!(decode(Some("")).is_err());
        assert!(decode(Some("{")).is_err());
        assert_eq!(
            decode(Some(r#"{"a": 1}"#)).expect("object"),
            Some(json!({"a": 1}))
        );
    }

    #[test]
    fn cohort_dates_parse_strictly() {
        assert_eq!(parse_cohort(None).expect("nil"), None);
        assert_eq!(
            parse_cohort(Some("2022-01-02")).expect("date"),
            Some(Date::from_calendar_date(2022, time::Month::January, 2).expect("date"))
        );
        assert!(parse_cohort(Some("2022-13-02")).is_err());
        assert!(parse_cohort(Some("")).is_err());
        assert!(parse_cohort(Some("not-a-date")).is_err());
    }

    #[test]
    fn event_objects_map_graphql_field_names() {
        let mut object = std::collections::BTreeMap::new();
        object.insert("event".to_owned(), InputValue::Str("e".to_owned()));
        object.insert("userCookieID".to_owned(), InputValue::Str("u".to_owned()));
        object.insert("url".to_owned(), InputValue::Str("https://x/y".to_owned()));
        object.insert("source".to_owned(), InputValue::Enum("WEB".to_owned()));
        object.insert(
            "cohortID".to_owned(),
            InputValue::Str("2022-01-02".to_owned()),
        );
        object.insert(
            "deviceSessionID".to_owned(),
            InputValue::Str("ds".to_owned()),
        );
        object.insert("eventID".to_owned(), InputValue::Int(7));
        let input = from_object(&object).expect("event");
        assert_eq!(input.event, "e");
        assert_eq!(input.user_cookie_id, "u");
        assert_eq!(input.source, "WEB");
        assert_eq!(input.cohort_id.as_deref(), Some("2022-01-02"));
        assert_eq!(input.device_session_id.as_deref(), Some("ds"));
        assert_eq!(input.event_id, Some(7));

        let mut missing = std::collections::BTreeMap::new();
        missing.insert("event".to_owned(), InputValue::Str("e".to_owned()));
        assert!(from_object(&missing).is_err());
    }

    #[tokio::test]
    async fn disabled_logging_and_metric_events_never_touch_the_database() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
            .expect("lazy pool");
        let disabled = site(json!({"experimentalFeatures": {"eventLogging": "disabled"}}));
        let deps = LogEventsDeps {
            pool: &pool,
            store: None,
            site: &disabled,
            uid: 0,
        };
        log_events(&deps, &[LogEventInput::default()])
            .await
            .expect("disabled logging is a no-op");

        let enabled = site(json!({}));
        let deps = LogEventsDeps {
            pool: &pool,
            store: None,
            site: &enabled,
            uid: 0,
        };
        let metric_event = LogEventInput {
            event: "search.latencies.frontend.code-load".to_owned(),
            user_cookie_id: "cookie".to_owned(),
            url: "https://sourcegraph.example.com/search".to_owned(),
            source: "WEB".to_owned(),
            argument: Some(r#"{"durationMs": 25}"#.to_owned()),
            ..LogEventInput::default()
        };
        log_events(&deps, &[metric_event])
            .await
            .expect("metric events skip the database");
    }
}
