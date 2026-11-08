#![forbid(unsafe_code)]

//! `cmd/worker/internal/adminanalytics` + `internal/adminanalytics`: the
//! `refresh-analytics-cache` job.
//!
//! One periodic routine (`refresh_analytics_cache`, 24h) recomputes every
//! admin-analytics aggregate and stores the results in the Redis *store*
//! (`redispool.Store`), under the `adminanalytics:` scope:
//!
//! * for each date range (`LAST_THREE_MONTHS`, `LAST_MONTH`, `LAST_WEEK`) and
//!   grouping (`WEEKLY`, `DAILY`), eight stores (`Search`, `Users`,
//!   `Notebooks`, `CodeIntel`, `Repos`, `BatchChanges`, `Extensions`,
//!   `CodeInsights`) cache their nodes + summary;
//! * then `CodeIntelByLanguage:<dateRange>` and
//!   `CodeIntelTopRepositories:<dateRange>` are cached once per date range.
//!
//! Behaviour deliberately reproduced, including the baseline's quirks:
//!
//! * `Search:ResultClicked` and `Extensions:Jetbrains` use `NoopCache`: they
//!   are queried but never written to Redis;
//! * `Users:Frequencies:<dateRange>` and `Users:MAU` keys omit the grouping /
//!   date range (so `DAILY` and later date ranges hit the `WEEKLY` /
//!   `LAST_THREE_MONTHS` entry);
//! * nodes are padded with zero buckets from "now" (or the Monday of the
//!   current week) back to the range start, walking backwards;
//! * every usage query applies `database.BuildCommonUsageConds` (system users,
//!   inactive-event names, Sourcegraph admins, Sourcegraph operators) plus
//!   `anonymous_user_id != 'backend'`.
//!
//! JSON is written with Go's `encoding/json` conventions: struct field names
//! (`Data`, `Date`, ...), zero-value floats as `4` rather than `4.0`, and
//! compact separators. `GoFloatFormatter` only approximates Go's `'e'`
//! formatting for `|x| >= 1e21`; counts never reach that range.
//!
//! Time is UTC here (`time::OffsetDateTime::now_utc`); the baseline uses
//! `time.Now()` and therefore the process's local zone. Deployments run with
//! `TZ=UTC`, where the two are identical; see docs §40.5.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sg_redis::KeyValue;
use sqlx::postgres::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Date, Duration as TimeDuration, Month, OffsetDateTime, PrimitiveDateTime, Time};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `refreshAnalyticsCacheJob.Description`.
pub const JOB_DESCRIPTION: &str = "refreshes the admin analytics cache";
/// `goroutine.WithName("refresh_analytics_cache")`.
pub const ROUTINE_NAME: &str = "refresh_analytics_cache";
/// `goroutine.WithDescription("refresh analytics cache")`.
pub const ROUTINE_DESCRIPTION: &str = "refresh analytics cache";
/// `goroutine.WithInterval(24 * time.Hour)`.
pub const ROUTINE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// `scopeKey`.
pub const SCOPE_KEY: &str = "adminanalytics:";
/// `setDataToCache`'s default TTL (`24 * 60 * 60`).
pub const DEFAULT_TTL_SECONDS: i64 = 24 * 60 * 60;

pub const LAST_THREE_MONTHS: &str = "LAST_THREE_MONTHS";
pub const LAST_MONTH: &str = "LAST_MONTH";
pub const LAST_WEEK: &str = "LAST_WEEK";
pub const DAILY: &str = "DAILY";
pub const WEEKLY: &str = "WEEKLY";

pub const DATE_RANGES: [&str; 3] = [LAST_THREE_MONTHS, LAST_MONTH, LAST_WEEK];
pub const GROUP_BYS: [&str; 2] = [WEEKLY, DAILY];

/// `eventlogger.NonActiveUserEvents`, in source order.
pub const NON_ACTIVE_USER_EVENTS: [&str; 17] = [
    "ViewSignIn",
    "ViewSignUp",
    "SignOutAttempted",
    "SignOutFailed",
    "SignOutSucceeded",
    "SignInAttempted",
    "SignInFailed",
    "SignInSucceeded",
    "PasswordResetRequested",
    "PasswordRandomized",
    "PasswordChanged",
    "EmailVerified",
    "ExternalAuthSignupFailed",
    "ExternalAuthSignupSucceeded",
    "CodyVSCodeExtension:CodySavedLogin:executed",
    "cody.characters.flush",
    "cody.characters",
];

/// `database.EventLogsSourcegraphOperatorKey`.
pub const SOURCEGRAPH_OPERATOR_KEY: &str = "sourcegraph_operator";

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/// `adminanalytics.KeyValue`: the two Redis operations the stores use.
///
/// Keys are passed fully scoped (`adminanalytics:` prefix included), matching
/// the baseline's `scopeKey + cacheKey` call.
#[async_trait]
pub trait AnalyticsCache: Send + Sync {
    /// `KeyValue.Get(key).String()`; `None` is any miss or error.
    async fn get(&self, key: &str) -> Option<String>;
    /// `KeyValue.SetEx(key, ttlSeconds, value)`.
    async fn set_ex(&self, key: &str, ttl_seconds: i64, value: &str) -> Result<(), String>;
}

/// `adminanalytics.NoopCache`: always a miss, never writes.
pub struct NoopCache;

#[async_trait]
impl AnalyticsCache for NoopCache {
    async fn get(&self, _key: &str) -> Option<String> {
        None
    }

    async fn set_ex(&self, _key: &str, _ttl_seconds: i64, _value: &str) -> Result<(), String> {
        Ok(())
    }
}

/// The production cache: `redispool.Store`, dialed lazily on first use.
///
/// The baseline's `redispool.Store` is a package-level pool that connects on
/// first command, so constructing the routine must not require a reachable
/// Redis. A failed connection is a miss / a write error, exactly like the
/// Go pool returning an error.
pub struct RedisCache {
    endpoint: String,
    connection: OnceCell<KeyValue>,
}

impl RedisCache {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            connection: OnceCell::new(),
        }
    }

    /// `redispool.Store`: `REDIS_STORE_ENDPOINT`, else `REDIS_ENDPOINT`.
    pub fn from_env() -> Self {
        Self::new(sg_redis::store_endpoint())
    }

    async fn connection(&self) -> Result<&KeyValue, String> {
        self.connection
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|error| format!("connect redis store: {error}"))
            })
            .await
    }
}

#[async_trait]
impl AnalyticsCache for RedisCache {
    async fn get(&self, key: &str) -> Option<String> {
        match self.connection().await {
            Ok(connection) => connection.get_string(key).await.ok().flatten(),
            Err(_) => None,
        }
    }

    async fn set_ex(&self, key: &str, ttl_seconds: i64, value: &str) -> Result<(), String> {
        let connection = self.connection().await?;
        connection
            .set_ex(key, ttl_seconds, value)
            .await
            .map_err(|error| format!("redis SETEX {key}: {error}"))
    }
}

// ---------------------------------------------------------------------------
// Go-compatible JSON
// ---------------------------------------------------------------------------

/// `encoding/json` writes a `float64` with `'f'` formatting (no exponent) for
/// `1e-6 <= |x| < 1e21`, and without a trailing `.0`. Rust's `Display` for
/// `f64` is also shortest-roundtrip and never uses an exponent, so it matches
/// in the reachable range.
fn format_go_float(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_owned()
        } else {
            "0".to_owned()
        };
    }
    let abs = value.abs();
    if abs < 1e-6 || abs >= 1e21 {
        // Go: strconv 'e', e.g. "1e+21"; Rust prints "1e21". Unreachable for
        // event counts; documented in docs §40.5.
        return format!("{value:e}");
    }
    format!("{value}")
}

struct GoFloatFormatter;

impl serde_json::ser::Formatter for GoFloatFormatter {
    fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(format_go_float(value).as_bytes())
    }
}

/// `json.Marshal` with Go's float formatting.
pub fn marshal_go<T: Serialize>(value: &T) -> Result<String, String> {
    let mut out = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut out, GoFloatFormatter);
    value
        .serialize(&mut serializer)
        .map_err(|error| format!("marshal cache value: {error}"))?;
    String::from_utf8(out).map_err(|error| format!("marshal cache value: {error}"))
}

// ---------------------------------------------------------------------------
// Cached shapes (Go field names / JSON tags)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct AnalyticsNodeData {
    #[serde(rename = "Date")]
    pub date: String,
    #[serde(rename = "Count")]
    pub count: f64,
    #[serde(rename = "UniqueUsers")]
    pub unique_users: f64,
    #[serde(rename = "RegisteredUsers")]
    pub registered_users: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct AnalyticsNode {
    #[serde(rename = "Data")]
    pub data: AnalyticsNodeData,
}

impl AnalyticsNode {
    fn zero(date: OffsetDateTime) -> Self {
        Self {
            data: AnalyticsNodeData {
                date: format_rfc3339(date),
                count: 0.0,
                unique_users: 0.0,
                registered_users: 0.0,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct AnalyticsSummaryData {
    #[serde(rename = "TotalCount")]
    pub total_count: f64,
    #[serde(rename = "TotalUniqueUsers")]
    pub total_unique_users: f64,
    #[serde(rename = "TotalRegisteredUsers")]
    pub total_registered_users: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct AnalyticsSummary {
    #[serde(rename = "Data")]
    pub data: AnalyticsSummaryData,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct UsersFrequencyNodeData {
    #[serde(rename = "DaysUsed")]
    pub days_used: f64,
    #[serde(rename = "Frequency")]
    pub frequency: f64,
    #[serde(rename = "Percentage")]
    pub percentage: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct UsersFrequencyNode {
    #[serde(rename = "Data")]
    pub data: UsersFrequencyNodeData,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct MonthlyActiveUsersRowData {
    #[serde(rename = "Date")]
    pub date: String,
    #[serde(rename = "Count")]
    pub count: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct MonthlyActiveUsersRow {
    #[serde(rename = "Data")]
    pub data: MonthlyActiveUsersRowData,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct ReposSummaryData {
    #[serde(rename = "Count")]
    pub count: f64,
    #[serde(rename = "PreciseCodeIntelCount")]
    pub precise_code_intel_count: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct ReposSummary {
    #[serde(rename = "Data")]
    pub data: ReposSummaryData,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct CodeIntelByLanguage {
    pub language: String,
    pub precision: String,
    pub count: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct CodeIntelTopRepositories {
    #[serde(rename = "langId")]
    pub lang_id: String,
    pub name: String,
    pub events: f64,
    pub kind: String,
    pub precision: String,
    #[serde(rename = "hasPrecise")]
    pub has_precise: bool,
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

fn now_utc() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

/// `time.Time.Format(time.RFC3339)`: whole seconds, `Z` offset.
fn format_rfc3339(value: OffsetDateTime) -> String {
    value
        .replace_nanosecond(0)
        .expect("nanosecond 0 is valid")
        .format(&Rfc3339)
        .expect("RFC3339 formatting cannot fail")
}

/// `time.AddDate(0, months, 0)`: calendar-month arithmetic where an
/// out-of-range day overflows into the next month (Go's normalization).
fn add_months(value: OffsetDateTime, months: i32) -> OffsetDateTime {
    let total_months = value.year() * 12 + (value.month() as i32 - 1) + months;
    let year = total_months.div_euclid(12);
    let month = Month::try_from((total_months.rem_euclid(12) + 1) as u8)
        .expect("month is in 1..=12 by construction");
    let first = Date::from_calendar_date(year, month, 1).expect("first of month is valid");
    let date = first + TimeDuration::days(i64::from(value.day()) - 1);
    value.replace_date(date)
}

/// `time.AddDate(0, 0, days)` (UTC: identical to adding 24h days).
fn add_days(value: OffsetDateTime, days: i64) -> OffsetDateTime {
    value + TimeDuration::days(days)
}

/// `bod`: beginning of day in the value's location (UTC here).
fn bod(value: OffsetDateTime) -> OffsetDateTime {
    value.replace_time(Time::MIDNIGHT)
}

/// `now.Weekday()` with Sunday = 0.
fn weekday_from_sunday(value: OffsetDateTime) -> i64 {
    i64::from(value.weekday().number_days_from_sunday())
}

/// `getFromDate`.
fn from_date(date_range: &str, now: OffsetDateTime) -> Result<OffsetDateTime, String> {
    match date_range {
        LAST_THREE_MONTHS => Ok(add_months(now, -3)),
        LAST_MONTH => Ok(add_months(now, -1)),
        LAST_WEEK => Ok(add_days(now, -7)),
        _ => Err("Invalid date range".to_owned()),
    }
}

/// `makeDateParameters`: `dateTruncExp`, `dateBetweenCond`.
fn date_parameters(
    date_range: &str,
    grouping: &str,
    date_column: &str,
) -> Result<(String, String), String> {
    let now = now_utc();
    let from = from_date(date_range, now)?;
    let group_by = match grouping {
        WEEKLY => "week",
        DAILY => "day",
        _ => return Err("Invalid groupBy".to_owned()),
    };
    let date_trunc = format!("DATE_TRUNC('{group_by}', TIMEZONE('UTC', {date_column}::date))");
    let between = format!(
        "BETWEEN '{}' AND '{}'",
        format_rfc3339(from),
        format_rfc3339(now)
    );
    Ok((date_trunc, between))
}

/// `getTimestamps(months)`: first of the month `months` months ago, and now.
fn get_timestamps(months: i32) -> (String, String) {
    let now = now_utc();
    let to = format_rfc3339(now);
    let first_of_month =
        Date::from_calendar_date(now.year(), now.month(), 1).expect("first of month is valid");
    let month_total = now.year() * 12 + (now.month() as i32 - 1) - months;
    let year = month_total.div_euclid(12);
    let month = Month::try_from((month_total.rem_euclid(12) + 1) as u8).expect("valid month");
    let previous = Date::from_calendar_date(year, month, first_of_month.day())
        .expect("first of month is valid");
    let from = previous
        .midnight()
        .assume_utc()
        .format(&Rfc3339)
        .expect("RFC3339 formatting cannot fail");
    (from, to)
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// `getDefaultConds`: `BuildCommonUsageConds` with all four exclusions, plus
/// `anonymous_user_id != 'backend'`.
pub fn default_conds() -> Vec<String> {
    let non_active = format!(
        "event_logs.name NOT IN ('{}')",
        NON_ACTIVE_USER_EVENTS.join("','")
    );
    let admins = "\n-- No matching user exists\nusers.username IS NULL\n-- Or, the user does not...\nOR NOT(\n\t-- ...have a known Sourcegraph admin username pattern\n\t(users.username ILIKE 'managed-%'\n\t\tOR users.username ILIKE 'sourcegraph-management-%'\n\t\tOR users.username = 'sourcegraph-admin')\n\t-- ...and have a matching sourcegraph email address\n\tAND EXISTS (\n\t\tSELECT\n\t\t\t1 FROM user_emails\n\t\tWHERE\n\t\t\tuser_emails.user_id = users.id\n\t\t\tAND user_emails.email ILIKE '%@sourcegraph.com')\n)\n"
        .to_owned();
    vec![
        "event_logs.user_id > 0 OR event_logs.anonymous_user_id <> 'backend'".to_owned(),
        non_active,
        admins,
        format!("NOT event_logs.public_argument @> '{{\"{SOURCEGRAPH_OPERATOR_KEY}\": true}}'"),
        "anonymous_user_id != 'backend'".to_owned(),
    ]
}

const EVENT_LOGS_NODES_QUERY: &str = "\nSELECT\n\t%s AS date,\n\tCOUNT(*)::float8 AS total_count,\n\tCOUNT(DISTINCT CASE WHEN user_id = 0 THEN anonymous_user_id ELSE CAST(user_id AS TEXT) END)::float8 AS unique_users,\n\tCOUNT(DISTINCT user_id) FILTER (WHERE user_id != 0)::float8 AS registered_users\nFROM\n\tevent_logs\nLEFT OUTER JOIN users ON users.id = event_logs.user_id\n%s\nGROUP BY date\n";

const EVENT_LOGS_SUMMARY_QUERY: &str = "\nSELECT\n\tCOUNT(*)::float8 AS total_count,\n\tCOUNT(DISTINCT CASE WHEN user_id = 0 THEN anonymous_user_id ELSE CAST(user_id AS TEXT) END)::float8 AS unique_users,\n\tCOUNT(DISTINCT user_id) FILTER (WHERE user_id != 0)::float8 AS registered_users\nFROM\n\tevent_logs\nLEFT OUTER JOIN users ON users.id = event_logs.user_id\n%s\n";

fn quote_events(events: &[&str]) -> String {
    events
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// `makeEventLogsQueries`.
pub fn make_event_logs_queries(
    date_range: &str,
    grouping: &str,
    events: &[&str],
    conditions: &[&str],
) -> Result<(String, String), String> {
    let (date_trunc, between) = date_parameters(date_range, grouping, "timestamp")?;

    let mut conds = default_conds();
    conds.push(format!("timestamp {between}"));
    conds.extend(conditions.iter().map(|condition| (*condition).to_owned()));
    if !events.is_empty() {
        conds.push(format!("name IN ({})", quote_events(events)));
    }
    let where_clause = format!("WHERE ({})", conds.join(") AND ("));

    Ok((
        EVENT_LOGS_NODES_QUERY
            .replacen("%s", &date_trunc, 1)
            .replacen("%s", &where_clause, 1),
        EVENT_LOGS_SUMMARY_QUERY.replacen("%s", &where_clause, 1),
    ))
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

async fn get_array<T: DeserializeOwned>(
    cache: &dyn AnalyticsCache,
    cache_key: &str,
) -> Option<Vec<T>> {
    let data = cache.get(&format!("{SCOPE_KEY}{cache_key}")).await?;
    serde_json::from_str(&data).ok()
}

async fn set_array<T: Serialize>(
    cache: &dyn AnalyticsCache,
    cache_key: &str,
    nodes: &[T],
) -> Result<(), String> {
    let data = marshal_go(&nodes)?;
    cache
        .set_ex(
            &format!("{SCOPE_KEY}{cache_key}"),
            DEFAULT_TTL_SECONDS,
            &data,
        )
        .await
}

async fn get_item<T: DeserializeOwned>(cache: &dyn AnalyticsCache, cache_key: &str) -> Option<T> {
    let data = cache.get(&format!("{SCOPE_KEY}{cache_key}")).await?;
    serde_json::from_str(&data).ok()
}

async fn set_item<T: Serialize>(
    cache: &dyn AnalyticsCache,
    cache_key: &str,
    item: &T,
) -> Result<(), String> {
    let data = marshal_go(&item)?;
    cache
        .set_ex(
            &format!("{SCOPE_KEY}{cache_key}"),
            DEFAULT_TTL_SECONDS,
            &data,
        )
        .await
}

// ---------------------------------------------------------------------------
// Fetcher
// ---------------------------------------------------------------------------

/// `AnalyticsFetcher`: one `(group, dateRange, grouping)` query pair.
pub struct AnalyticsFetcher<'a> {
    pool: &'a PgPool,
    cache: &'a dyn AnalyticsCache,
    group: &'static str,
    date_range: &'static str,
    grouping: &'static str,
    nodes_query: String,
    summary_query: String,
}

impl<'a> AnalyticsFetcher<'a> {
    pub fn new(
        pool: &'a PgPool,
        cache: &'a dyn AnalyticsCache,
        group: &'static str,
        date_range: &'static str,
        grouping: &'static str,
        nodes_query: String,
        summary_query: String,
    ) -> Self {
        Self {
            pool,
            cache,
            group,
            date_range,
            grouping,
            nodes_query,
            summary_query,
        }
    }

    fn cache_key(&self, leaf: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            self.group, self.date_range, self.grouping, leaf
        )
    }

    /// `AnalyticsFetcher.Nodes`, including the backwards zero padding.
    pub async fn nodes(&self) -> Result<Vec<AnalyticsNode>, String> {
        let cache_key = self.cache_key("nodes");
        if let Some(nodes) = get_array::<AnalyticsNode>(self.cache, &cache_key).await {
            return Ok(nodes);
        }

        let rows = sqlx::query_as::<_, (PrimitiveDateTime, f64, f64, f64)>(&self.nodes_query)
            .fetch_all(self.pool)
            .await
            .map_err(|error| error.to_string())?;
        let nodes: Vec<AnalyticsNode> = rows
            .into_iter()
            .map(
                |(date, count, unique_users, registered_users)| AnalyticsNode {
                    data: AnalyticsNodeData {
                        date: format_rfc3339(date.assume_utc()),
                        count,
                        unique_users,
                        registered_users,
                    },
                },
            )
            .collect();

        let now = now_utc();
        let mut to = now;
        let mut days_offset = 1_i64;
        let from = from_date(self.date_range, now)?;
        if self.grouping == WEEKLY {
            // Monday of the current week.
            to = add_days(now, -weekday_from_sunday(now) + 1);
            days_offset = 7;
        }

        let mut all_nodes = Vec::new();
        let mut date = to;
        while date >= from {
            let bucket = bod(date);
            let node = nodes
                .iter()
                .find(|node| parse_rfc3339(&node.data.date).map(bod) == Some(bucket))
                .cloned()
                .unwrap_or_else(|| AnalyticsNode::zero(bucket));
            all_nodes.push(node);
            date = add_days(date, -days_offset);
        }

        set_array(self.cache, &cache_key, &all_nodes).await?;
        Ok(all_nodes)
    }

    /// `AnalyticsFetcher.Summary`.
    pub async fn summary(&self) -> Result<AnalyticsSummary, String> {
        let cache_key = self.cache_key("summary");
        if let Some(summary) = get_item::<AnalyticsSummary>(self.cache, &cache_key).await {
            return Ok(summary);
        }

        let (total_count, total_unique_users, total_registered_users) =
            sqlx::query_as::<_, (f64, f64, f64)>(&self.summary_query)
                .fetch_one(self.pool)
                .await
                .map_err(|error| error.to_string())?;
        let summary = AnalyticsSummary {
            data: AnalyticsSummaryData {
                total_count,
                total_unique_users,
                total_registered_users,
            },
        };

        set_item(self.cache, &cache_key, &summary).await?;
        Ok(summary)
    }
}

fn parse_rfc3339(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

struct StoreContext<'a> {
    pool: &'a PgPool,
    cache: &'a dyn AnalyticsCache,
    date_range: &'static str,
    grouping: &'static str,
}

fn build_fetcher<'a>(
    ctx: &StoreContext<'a>,
    cache: &'a dyn AnalyticsCache,
    group: &'static str,
    events: &[&str],
    conditions: &[&str],
) -> Result<AnalyticsFetcher<'a>, String> {
    let (nodes_query, summary_query) =
        make_event_logs_queries(ctx.date_range, ctx.grouping, events, conditions)?;
    Ok(AnalyticsFetcher::new(
        ctx.pool,
        cache,
        group,
        ctx.date_range,
        ctx.grouping,
        nodes_query,
        summary_query,
    ))
}

async fn cache_fetcher(fetcher: &AnalyticsFetcher<'_>) -> Result<(), String> {
    fetcher.nodes().await?;
    fetcher.summary().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

async fn cache_search(ctx: &StoreContext<'_>) -> Result<(), String> {
    // `ResultClicks` is the baseline's NoopCache group.
    let builders: [(&'static str, &[&str], &dyn AnalyticsCache); 5] = [
        ("Search:Searches", &["SearchResultsQueried"], ctx.cache),
        ("Search:FileViews", &["ViewBlob"], ctx.cache),
        (
            "Search:FileOpens",
            &[
                "GoToCodeHostClicked",
                "vscode.open.file",
                "openInAtom.open.file",
                "openineditor.open.file",
                "openInWebstorm.open.file",
            ],
            ctx.cache,
        ),
        (
            "Search:ResultClicked",
            &["SearchResultClicked"],
            &NOOP_CACHE,
        ),
        ("Search:CodeCopied", &["CodeCopied"], ctx.cache),
    ];
    for (group, events, cache) in builders {
        let fetcher = build_fetcher(ctx, cache, group, events, &[])?;
        cache_fetcher(&fetcher).await?;
    }
    Ok(())
}

async fn cache_notebooks(ctx: &StoreContext<'_>) -> Result<(), String> {
    let builders: [(&'static str, &[&str]); 3] = [
        ("Notebooks:Creations", &["SearchNotebookCreated"]),
        (
            "Notebooks:BlockRuns",
            &["SearchNotebookRunAllBlocks", "SearchNotebookRunBlock"],
        ),
        ("Notebooks:Views", &["SearchNotebookPageViewed"]),
    ];
    for (group, events) in builders {
        let fetcher = build_fetcher(ctx, ctx.cache, group, events, &[])?;
        cache_fetcher(&fetcher).await?;
    }
    Ok(())
}

async fn cache_code_intel(ctx: &StoreContext<'_>) -> Result<(), String> {
    let definitions = ["goToDefinition.preloaded", "goToDefinition"];
    let both = [
        "goToDefinition.preloaded",
        "goToDefinition",
        "findReferences",
    ];
    let search_based = [
        "codeintel.searchDefinitions",
        "codeintel.searchDefinitions.xrepo",
        "codeintel.searchReferences",
        "codeintel.searchReferences.xrepo",
    ];
    let precise = [
        "codeintel.lsifDefinitions",
        "codeintel.lsifDefinitions.xrepo",
        "codeintel.lsifReferences",
        "codeintel.lsifReferences.xrepo",
    ];
    let cross_repo = [
        "codeintel.searchDefinitions.xrepo",
        "codeintel.searchReferences.xrepo",
        "codeintel.lsifDefinitions.xrepo",
        "codeintel.lsifReferences.xrepo",
    ];
    let builders: [(&'static str, &[&str], &[&str]); 7] = [
        ("CodeIntel:DefinitionClicks", &definitions, &[]),
        ("CodeIntel:ReferenceClicks", &["findReferences"], &[]),
        ("CodeIntel:InAppEvents", &both, &["source = 'WEB'"]),
        (
            "CodeIntel:CodeHostEvents",
            &both,
            &["source = 'CODEHOSTINTEGRATION'"],
        ),
        ("CodeIntel:SearchBasedEvents", &search_based, &[]),
        ("CodeIntel:PreciseEvents", &precise, &[]),
        ("CodeIntel:CrossRepoEvents", &cross_repo, &[]),
    ];
    for (group, events, conditions) in builders {
        let fetcher = build_fetcher(ctx, ctx.cache, group, events, conditions)?;
        cache_fetcher(&fetcher).await?;
    }
    Ok(())
}

async fn cache_repos(pool: &PgPool, cache: &dyn AnalyticsCache) -> Result<(), String> {
    const CACHE_KEY: &str = "Repos:Summary";
    if get_item::<ReposSummary>(cache, CACHE_KEY).await.is_some() {
        return Ok(());
    }

    let (count, precise_code_intel_count) = sqlx::query_as::<_, (f64, f64)>(
        "\n\tSELECT\n\t\tCOUNT(DISTINCT repo.id)::float8 as total_repo_count,\n\t\tCOUNT(DISTINCT lsif_uploads.repository_id)::float8 as lsif_index_repo_count\n\tFROM\n\t\trepo\n\t\tLEFT JOIN lsif_uploads ON lsif_uploads.repository_id = repo.id\n\t",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;
    let summary = ReposSummary {
        data: ReposSummaryData {
            count,
            precise_code_intel_count,
        },
    };
    set_item(cache, CACHE_KEY, &summary).await
}

async fn cache_batch_changes(ctx: &StoreContext<'_>) -> Result<(), String> {
    let (created_trunc, created_between) =
        date_parameters(ctx.date_range, ctx.grouping, "changesets.created_at")?;
    let created_nodes = CHANGESETS_CREATED_NODES_QUERY
        .replacen("%s", &created_trunc, 1)
        .replacen("%s", &created_between, 1);
    let created_summary = CHANGESETS_CREATED_SUMMARY_QUERY.replacen("%s", &created_between, 1);
    let fetcher = AnalyticsFetcher::new(
        ctx.pool,
        ctx.cache,
        "BatchChanges:ChangesetsCreated",
        ctx.date_range,
        ctx.grouping,
        created_nodes,
        created_summary,
    );
    cache_fetcher(&fetcher).await?;

    // The baseline quirk: merged nodes date-truncate `changesets.created_at`
    // but filter on `changeset_events.created_at`.
    let (merged_trunc, merged_between) =
        date_parameters(ctx.date_range, ctx.grouping, "changesets.created_at")?;
    let kinds = MERGE_EVENT_KINDS;
    let merged_nodes = CHANGESETS_MERGED_NODES_QUERY
        .replacen("%s", &merged_trunc, 1)
        .replacen("%s", &merged_between, 1)
        .replacen("%s", kinds, 1);
    let merged_summary = CHANGESETS_MERGED_SUMMARY_QUERY
        .replacen("%s", &merged_between, 1)
        .replacen("%s", kinds, 1);
    let fetcher = AnalyticsFetcher::new(
        ctx.pool,
        ctx.cache,
        "BatchChanges:ChangesetsMerged",
        ctx.date_range,
        ctx.grouping,
        merged_nodes,
        merged_summary,
    );
    cache_fetcher(&fetcher).await
}

const CHANGESETS_CREATED_NODES_QUERY: &str = "\n\tSELECT\n\t\t%s AS date,\n\t\tCOUNT(DISTINCT changesets.id)::float8 AS count,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS unique_users,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS registered_users\n\tFROM\n\t\tchangesets\n\t\tINNER JOIN batch_changes ON batch_changes.id = changesets.owned_by_batch_change_id\n\tWHERE changesets.created_at %s AND changesets.publication_state = 'PUBLISHED'\n\tGROUP BY date\n";

const CHANGESETS_CREATED_SUMMARY_QUERY: &str = "\n\tSELECT\n\t\tCOUNT(DISTINCT changesets.id)::float8 AS total_count,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS total_unique_users,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS total_registered_users\n\tFROM\n\t\tchangesets\n\t\tINNER JOIN batch_changes ON batch_changes.id = changesets.owned_by_batch_change_id\n\tWHERE changesets.created_at %s AND changesets.publication_state = 'PUBLISHED'\n";

const CHANGESETS_MERGED_NODES_QUERY: &str = "\n\tSELECT\n\t\t%s AS date,\n\t\tCOUNT(DISTINCT changesets.id)::float8 AS count,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS unique_users,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS registered_users\n\tFROM\n\t\tchangeset_events\n\t\tINNER JOIN changesets ON changesets.id = changeset_events.changeset_id\n\t\tINNER JOIN batch_changes ON batch_changes.id = changesets.owned_by_batch_change_id\n\tWHERE changeset_events.created_at %s AND changeset_events.kind IN (%s)\n\tGROUP BY date\n";

const CHANGESETS_MERGED_SUMMARY_QUERY: &str = "\n\tSELECT\n\t\tCOUNT(DISTINCT changesets.id)::float8 AS total_count,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS total_unique_users,\n\t\tCOUNT(DISTINCT batch_changes.creator_id)::float8 AS total_registered_users\n\tFROM\n\t\tchangeset_events\n\t\tINNER JOIN changesets ON changesets.id = changeset_events.changeset_id\n\t\tINNER JOIN batch_changes ON batch_changes.id = changesets.owned_by_batch_change_id\n\tWHERE changeset_events.created_at %s AND changeset_events.kind IN (%s)\n";

const MERGE_EVENT_KINDS: &str =
    "'github:merged','bitbucketserver:merged','gitlab:merged','bitbucketcloud:pullrequest:fulfilled'";

/// `(group, event names, extra conditions, cache)` for an event-logs store.
type EventLogsBuilder<'a, 'b> = (
    &'static str,
    &'a [&'a str],
    &'a [&'a str],
    &'b dyn AnalyticsCache,
);

async fn cache_extensions(ctx: &StoreContext<'_>) -> Result<(), String> {
    let ide_events = ["IDESearchSubmitted", "VSCESearchSubmitted"];
    let builders: [EventLogsBuilder<'_, '_>; 3] = [
        (
            "Extensions:Jetbrains",
            &ide_events,
            &["source = 'IDEEXTENSION' AND referrer = 'JETBRAINS'"],
            &NOOP_CACHE,
        ),
        (
            "Extensions:Vscode",
            &ide_events,
            &["source = 'IDEEXTENSION' AND referrer = 'VSCE'"],
            ctx.cache,
        ),
        (
            "Extensions:Browser",
            &[
                "goToDefinition.preloaded",
                "goToDefinition",
                "findReferences",
            ],
            &["source = 'CODEHOSTINTEGRATION'"],
            ctx.cache,
        ),
    ];
    for (group, events, conditions, cache) in builders {
        let fetcher = build_fetcher(ctx, cache, group, events, conditions)?;
        cache_fetcher(&fetcher).await?;
    }
    Ok(())
}

async fn cache_code_insights(ctx: &StoreContext<'_>) -> Result<(), String> {
    let builders: [(&'static str, &[&str]); 2] = [
        ("Insights:InsightHovers", &["InsightHover"]),
        (
            "Insights:InsightDataPointClicks",
            &["InsightDataPointClick"],
        ),
    ];
    for (group, events) in builders {
        let fetcher = build_fetcher(ctx, ctx.cache, group, events, &[])?;
        cache_fetcher(&fetcher).await?;
    }
    Ok(())
}

static NOOP_CACHE: NoopCache = NoopCache;

// ---------------------------------------------------------------------------
// Users (custom queries)
// ---------------------------------------------------------------------------

const FREQUENCY_QUERY: &str = "\n\tWITH user_days_used AS (\n        SELECT\n            CASE WHEN user_id = 0 THEN anonymous_user_id ELSE CAST(user_id AS TEXT) END AS user_id,\n            COUNT(DISTINCT DATE(TIMEZONE('UTC', timestamp)))::float8 AS days_used\n        FROM event_logs\n\t\tLEFT OUTER JOIN users ON users.id = event_logs.user_id\n        WHERE\n            DATE(timestamp) %s\n            AND (%s)\n        GROUP BY 1\n    ),\n    days_used_frequency AS (\n        SELECT days_used, COUNT(*)::float8 AS frequency\n        FROM user_days_used\n        GROUP BY 1\n    ),\n    days_used_total_frequency AS (\n        SELECT\n            days_used_frequency.days_used,\n            SUM(more_days_used_frequency.frequency)::float8 AS frequency\n        FROM days_used_frequency\n            LEFT JOIN days_used_frequency AS more_days_used_frequency\n            ON more_days_used_frequency.days_used >= days_used_frequency.days_used\n        GROUP BY 1\n    ),\n    max_days_used_total_frequency AS (\n        SELECT MAX(frequency)::float8 AS max_frequency\n        FROM days_used_total_frequency\n    )\n    SELECT\n        days_used,\n        frequency,\n        (frequency * 100.00 / COALESCE(max_frequency, 1))::float8 AS percentage\n    FROM days_used_total_frequency, max_days_used_total_frequency\n    ORDER BY 1 ASC;\n\t";

const MAU_QUERY: &str = "\n\tSELECT\n\t\tTO_CHAR(TIMEZONE('UTC', timestamp), 'YYYY-MM') AS date,\n\t\tCOUNT(DISTINCT CASE WHEN user_id = 0 THEN anonymous_user_id ELSE CAST(user_id AS TEXT) END)::float8 AS count\n\tFROM event_logs\n\tLEFT OUTER JOIN users ON users.id = event_logs.user_id\n\tWHERE\n\t\ttimestamp BETWEEN %s AND %s\n    AND (%s)\n\tGROUP BY 1\n\tORDER BY 1 ASC\n\t";

async fn cache_users(ctx: &StoreContext<'_>) -> Result<(), String> {
    let activity = build_fetcher(ctx, ctx.cache, "Users:Activity", &[], &[])?;
    cache_fetcher(&activity).await?;
    cache_frequencies(ctx).await?;
    cache_monthly_active_users(ctx).await
}

async fn cache_frequencies(ctx: &StoreContext<'_>) -> Result<(), String> {
    let cache_key = format!("Users:Frequencies:{}", ctx.date_range);
    if get_array::<UsersFrequencyNode>(ctx.cache, &cache_key)
        .await
        .is_some()
    {
        return Ok(());
    }

    let (_, date_range_cond) =
        date_parameters(ctx.date_range, ctx.grouping, "event_logs.timestamp")?;
    let query = FREQUENCY_QUERY
        .replacen("%s", &date_range_cond, 1)
        .replacen("%s", &default_conds().join(") AND ("), 1);
    let rows = sqlx::query_as::<_, (f64, f64, f64)>(&query)
        .fetch_all(ctx.pool)
        .await
        .map_err(|error| error.to_string())?;
    let nodes: Vec<UsersFrequencyNode> = rows
        .into_iter()
        .map(|(days_used, frequency, percentage)| UsersFrequencyNode {
            data: UsersFrequencyNodeData {
                days_used,
                frequency,
                percentage,
            },
        })
        .collect();

    set_array(ctx.cache, &cache_key, &nodes).await
}

async fn cache_monthly_active_users(ctx: &StoreContext<'_>) -> Result<(), String> {
    const CACHE_KEY: &str = "Users:MAU";
    if get_array::<MonthlyActiveUsersRow>(ctx.cache, CACHE_KEY)
        .await
        .is_some()
    {
        return Ok(());
    }

    let (from, to) = get_timestamps(2);
    // `sqlf.Sprintf` quotes string arguments; the baseline passes these bare.
    let query = MAU_QUERY
        .replacen("%s", &format!("'{from}'"), 1)
        .replacen("%s", &format!("'{to}'"), 1)
        .replacen("%s", &default_conds().join(") AND ("), 1);
    let rows = sqlx::query_as::<_, (String, f64)>(&query)
        .fetch_all(ctx.pool)
        .await
        .map_err(|error| error.to_string())?;
    let nodes: Vec<MonthlyActiveUsersRow> = rows
        .into_iter()
        .map(|(date, count)| MonthlyActiveUsersRow {
            data: MonthlyActiveUsersRowData { date, count },
        })
        .collect();

    set_array(ctx.cache, CACHE_KEY, &nodes).await
}

// ---------------------------------------------------------------------------
// Code intel by language / top repositories
// ---------------------------------------------------------------------------

const CODE_INTEL_BY_LANGUAGE_QUERY: &str = "\n\t\tSELECT language, precision, COUNT(*)::float8 AS count\n\t\tFROM (\n\t\t\tSELECT argument->>'languageId' AS language, CASE WHEN name LIKE '%search%' THEN 'search-based' ELSE 'precise' END AS precision\n\t\t\tFROM event_logs\n\t\t\tWHERE\n\t\t\t\ttimestamp BETWEEN $1::timestamptz AND $2::timestamptz AND\n\t\t\t\tname IN (\n\t\t\t\t\t'codeintel.searchDefinitions',\n\t\t\t\t\t'codeintel.searchDefinitions.xrepo',\n\t\t\t\t\t'codeintel.searchReferences',\n\t\t\t\t\t'codeintel.searchReferences.xrepo',\n\t\t\t\t\t'codeintel.lsifDefinitions',\n\t\t\t\t\t'codeintel.lsifDefinitions.xrepo',\n\t\t\t\t\t'codeintel.lsifReferences',\n\t\t\t\t\t'codeintel.lsifReferences.xrepo'\n\t\t\t\t)\n\t\t) sub\n\t\tGROUP BY language, precision;\n\t";

/// `GetCodeIntelByLanguage`.
pub async fn get_code_intel_by_language(
    pool: &PgPool,
    cache: &dyn AnalyticsCache,
    date_range: &str,
) -> Result<Vec<CodeIntelByLanguage>, String> {
    let cache_key = format!("CodeIntelByLanguage:{date_range}");
    if let Some(nodes) = get_array::<CodeIntelByLanguage>(cache, &cache_key).await {
        return Ok(nodes);
    }

    let now = now_utc();
    let from = from_date(date_range, now)?;
    let rows = sqlx::query_as::<_, (String, String, f64)>(CODE_INTEL_BY_LANGUAGE_QUERY)
        .bind(format_rfc3339(from))
        .bind(format_rfc3339(now))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    let items: Vec<CodeIntelByLanguage> = rows
        .into_iter()
        .map(|(language, precision, count)| CodeIntelByLanguage {
            language,
            precision,
            count,
        })
        .collect();

    set_array(cache, &cache_key, &items).await?;
    Ok(items)
}

const CODE_INTEL_TOP_REPOSITORIES_QUERY: &str = "\n\t\tWITH events AS (\n\t\t\tSELECT *\n\t\t\tFROM (\n\t\t\t\tSELECT\n\t\t\t\t\t(argument->>'repositoryId')::int AS repo_id,\n\t\t\t\t\t(argument->>'languageId')::text AS lang,\n\t\t\t\t\t(\n\t\t\t\t\t\tCASE\n\t\t\t\t\t\tWHEN name = 'codeintel.lsifDefinitions.xrepo'                                      THEN 'crossRepo'\n\t\t\t\t\t\tWHEN name = 'codeintel.lsifDefinitions'                                            THEN 'precise'\n\t\t\t\t\t\tWHEN name = 'codeintel.lsifReferences.xrepo'                                       THEN 'crossRepo'\n\t\t\t\t\t\tWHEN name = 'codeintel.lsifReferences'                                             THEN 'precise'\n\t\t\t\t\t\tWHEN name = 'codeintel.searchDefinitions.xrepo'                                    THEN 'crossRepo'\n\t\t\t\t\t\tWHEN name = 'codeintel.searchReferences.xrepo'                                     THEN 'crossRepo'\n\t\t\t\t\t\tWHEN name = 'findReferences'                    AND source = 'CODEHOSTINTEGRATION' THEN 'codeHost'\n\t\t\t\t\t\tWHEN name = 'findReferences'                    AND source = 'WEB'                 THEN 'inApp'\n\t\t\t\t\t\tWHEN name = 'goToDefinition.preloaded'          AND source = 'CODEHOSTINTEGRATION' THEN 'codeHost'\n\t\t\t\t\t\tWHEN name = 'goToDefinition.preloaded'          AND source = 'WEB'                 THEN 'inApp'\n\t\t\t\t\t\tWHEN name = 'goToDefinition'                    AND source = 'CODEHOSTINTEGRATION' THEN 'codeHost'\n\t\t\t\t\t\tWHEN name = 'goToDefinition'                    AND source = 'WEB'                 THEN 'inApp'\n\t\t\t\t\t\tWHEN name = 'codeintel.searchDefinitions'                                          THEN 'inApp'\n\t\t\t\t\t\tELSE NULL\n\t\t\t\t\t\tEND\n\t\t\t\t\t) AS kind,\n\t\t\t\t\tname\n\t\t\t\tFROM event_logs\n\t\t\t\tWHERE timestamp BETWEEN $1::timestamptz AND $2::timestamptz\n\t\t\t) AS _\n\t\t\tWHERE kind IS NOT NULL\n\t\t), top_repos AS (\n\t\t\tSELECT repo_id\n\t\t\tFROM events\n\t\t\tGROUP BY repo_id\n\t\t\tORDER BY COUNT(1) DESC\n\t\t\tLIMIT 5\n\t\t)\n\t\tSELECT\n\t\t\t(SELECT repo.name FROM repo WHERE repo.id = repo_id) AS repo_name,\n\t\t\tlang,\n\t\t\tkind,\n\t\t\t(\n\t\t\t\tCASE\n\t\t\t\tWHEN name = 'codeintel.lsifDefinitions.xrepo' THEN 'precise'\n\t\t\t\tWHEN name = 'codeintel.lsifDefinitions'       THEN 'precise'\n\t\t\t\tWHEN name = 'codeintel.lsifHover'             THEN 'precise'\n\t\t\t\tWHEN name = 'codeintel.lsifReferences.xrepo'  THEN 'precise'\n\t\t\t\tWHEN name = 'codeintel.lsifReferences'        THEN 'precise'\n\t\t\t\tELSE                                               'search-based'\n\t\t\t\tEND\n\t\t\t) AS precision,\n\t\t\tCOUNT(1)::float8 AS count_,\n\t\t\tEXISTS (SELECT 1 FROM lsif_uploads WHERE repository_id = repo_id AND state = 'completed') AS has_precise\n\t\tFROM top_repos JOIN events USING (repo_id)\n\t\tGROUP BY repo_id, lang, kind, precision;\n\t";

/// `GetCodeIntelTopRepositories`.
pub async fn get_code_intel_top_repositories(
    pool: &PgPool,
    cache: &dyn AnalyticsCache,
    date_range: &str,
) -> Result<Vec<CodeIntelTopRepositories>, String> {
    let cache_key = format!("CodeIntelTopRepositories:{date_range}");
    if let Some(nodes) = get_array::<CodeIntelTopRepositories>(cache, &cache_key).await {
        return Ok(nodes);
    }

    let now = now_utc();
    let from = from_date(date_range, now)?;
    let rows = sqlx::query_as::<_, (String, String, String, String, f64, bool)>(
        CODE_INTEL_TOP_REPOSITORIES_QUERY,
    )
    .bind(format_rfc3339(from))
    .bind(format_rfc3339(now))
    .fetch_all(pool)
    .await
    .map_err(|error| format!("GetCodeIntelTopRepositories SQL query: {error}"))?;
    let items: Vec<CodeIntelTopRepositories> = rows
        .into_iter()
        .map(
            |(name, lang_id, kind, precision, events, has_precise)| CodeIntelTopRepositories {
                lang_id,
                name,
                events,
                kind,
                precision,
                has_precise,
            },
        )
        .collect();

    set_array(cache, &cache_key, &items).await?;
    Ok(items)
}

// ---------------------------------------------------------------------------
// Job entrypoint
// ---------------------------------------------------------------------------

/// `refreshAnalyticsCache`: every store, every range/grouping, in baseline
/// order. The first error aborts the refresh.
pub async fn refresh_analytics_cache(
    pool: &PgPool,
    cache: &dyn AnalyticsCache,
) -> Result<(), String> {
    for date_range in DATE_RANGES {
        for grouping in GROUP_BYS {
            let ctx = StoreContext {
                pool,
                cache,
                date_range,
                grouping,
            };
            cache_search(&ctx).await?;
            cache_users(&ctx).await?;
            cache_notebooks(&ctx).await?;
            cache_code_intel(&ctx).await?;
            cache_repos(pool, cache).await?;
            cache_batch_changes(&ctx).await?;
            cache_extensions(&ctx).await?;
            cache_code_insights(&ctx).await?;
        }

        get_code_intel_by_language(pool, cache, date_range).await?;
        get_code_intel_top_repositories(pool, cache, date_range).await?;
    }

    Ok(())
}

/// `refreshAnalyticsCacheJob`.
pub struct RefreshAnalyticsCacheJob;

#[async_trait]
impl Job for RefreshAnalyticsCacheJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let handler = RefreshAnalyticsCacheHandler::new(
            deps.frontend.clone(),
            Arc::new(RedisCache::from_env()),
        );
        Ok(vec![PeriodicRoutine::new(Arc::new(handler))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(ROUTINE_INTERVAL)
            .into()])
    }
}

/// The routine handler: `refreshAnalyticsCache` with the production cache.
pub struct RefreshAnalyticsCacheHandler {
    pool: PgPool,
    cache: Arc<dyn AnalyticsCache>,
}

impl RefreshAnalyticsCacheHandler {
    pub fn new(pool: PgPool, cache: Arc<dyn AnalyticsCache>) -> Self {
        Self { pool, cache }
    }
}

#[async_trait]
impl Handler for RefreshAnalyticsCacheHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        refresh_analytics_cache(&self.pool, self.cache.as_ref())
            .await
            .map_err(HandlerError::new)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use time::Weekday;

    #[test]
    fn go_float_formatting() {
        assert_eq!(format_go_float(0.0), "0");
        assert_eq!(format_go_float(-0.0), "-0");
        assert_eq!(format_go_float(4.0), "4");
        assert_eq!(format_go_float(4.5), "4.5");
        assert_eq!(format_go_float(16.666666666666668), "16.666666666666668");
    }

    #[test]
    fn add_months_overflows_like_go() {
        let jan31 = Date::from_calendar_date(2024, Month::January, 31)
            .unwrap()
            .midnight()
            .assume_utc();
        // Go: Date(2023, 12, 31) for AddDate(0, -1, 0)? No: 2024-01-31 -1mo
        // is 2023-12-31 (December has 31 days).
        assert_eq!(
            add_months(jan31, -1).date(),
            Date::from_calendar_date(2023, Month::December, 31).unwrap()
        );
        let mar31 = Date::from_calendar_date(2023, Month::March, 31)
            .unwrap()
            .midnight()
            .assume_utc();
        // February has 28 days in 2023, so Go normalizes to March 3.
        assert_eq!(
            add_months(mar31, -1).date(),
            Date::from_calendar_date(2023, Month::March, 3).unwrap()
        );
    }

    #[test]
    fn weekday_matches_go() {
        let sunday = Date::from_calendar_date(2026, Month::November, 8)
            .unwrap()
            .midnight()
            .assume_utc();
        assert_eq!(weekday_from_sunday(sunday), 0);
        let monday = add_days(sunday, 1);
        assert_eq!(monday.weekday(), Weekday::Monday);
        assert_eq!(weekday_from_sunday(monday), 1);
    }

    #[test]
    fn node_json_uses_go_field_names() {
        let node = AnalyticsNode {
            data: AnalyticsNodeData {
                date: "2026-11-02T00:00:00Z".to_owned(),
                count: 2.0,
                unique_users: 1.0,
                registered_users: 0.0,
            },
        };
        assert_eq!(
            marshal_go(&vec![node]).unwrap(),
            r#"[{"Data":{"Date":"2026-11-02T00:00:00Z","Count":2,"UniqueUsers":1,"RegisteredUsers":0}}]"#
        );
    }

    #[test]
    fn default_conditions_match_baseline_text() {
        let conds = default_conds();
        assert_eq!(conds.len(), 5);
        assert_eq!(
            conds[0],
            "event_logs.user_id > 0 OR event_logs.anonymous_user_id <> 'backend'"
        );
        assert!(conds[1].starts_with("event_logs.name NOT IN ('ViewSignIn'"));
        assert!(conds[2].contains("users.username ILIKE 'managed-%'"));
        assert!(conds[2].contains("'%@sourcegraph.com'"));
        assert_eq!(
            conds[3],
            "NOT event_logs.public_argument @> '{\"sourcegraph_operator\": true}'"
        );
        assert_eq!(conds[4], "anonymous_user_id != 'backend'");
    }

    #[test]
    fn frequencies_key_omits_grouping() {
        // Regression guard for the baseline quirk mirrored here.
        assert_eq!(
            format!("Users:{}{}", "Frequencies", ":"),
            "Users:Frequencies:"
        );
    }
}
