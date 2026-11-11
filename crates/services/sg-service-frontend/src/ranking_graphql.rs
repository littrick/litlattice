#![forbid(unsafe_code)]

//! Port of the ranking GraphQL root resolver
//! (`internal/codeintel/ranking/transport/graphql/root_resolver.go`,
//! `iface.go`, `observability.go`) plus the `shared` result types
//! (`internal/codeintel/ranking/shared/types.go`) and graph key encoding
//! (`internal/codeintel/ranking/internal/shared/keys.go`).
//!
//! The GraphQL schema wiring, the gRPC `RankingService` client and the store
//! are out of scope. [`RootResolver`] takes the ranking service, the site-admin
//! checker and the observation hook as injected traits, so the resolver order
//! and error propagation can be exercised without a frontend process.
//!
//! Deviations from the baseline (documented in
//! `docs/overviews/m9_8hd_ranking_graphql_overview.md`):
//! - the configured root graph key is injected instead of read from the global
//!   site configuration;
//! - observation only reports the operation name (no RED metrics registry);
//! - [`DateTime`] implements Go's `time.RFC3339` parse/format semantics locally
//!   because this crate has no direct `time` dependency;
//! - [`GlobalRankingSummary::to_json`] and friends return `serde_json::Value`
//!   instead of using `serde::Serialize` derives for the same dependency reason.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use sg_graphql::schema::{assemble, Schema, SchemaError};

/// `conf.CodeIntelRankingDocumentReferenceCountsGraphKey`'s default when the
/// site setting is empty (`internal/conf/computed.go:423-428`).
pub const DEFAULT_GRAPH_KEY_ROOT: &str = "dev";

/// `//go:embed` equivalent for
/// `cmd/frontend/graphqlbackend/codeintel.ranking.graphql` (M9.8ic), byte
/// identical to the baseline file.
pub const CODEINTEL_RANKING_SCHEMA: &str = include_str!("codeintel.ranking.graphql");

/// Append [`CODEINTEL_RANKING_SCHEMA`] to an already assembled schema, exactly
/// like `schemas = append(schemas, codeintelRankingSchema)` followed by
/// `strings.Join(schemas, "\n")` (`graphqlbackend.go:440-447`).
pub fn append_ranking_schema(base: &Schema) -> Result<Schema, SchemaError> {
    assemble(&[base.schema_string.as_str(), CODEINTEL_RANKING_SCHEMA])
}

// ---------------------------------------------------------------------------
// Graph keys (`internal/codeintel/ranking/internal/shared/keys.go`)
// ---------------------------------------------------------------------------

/// `encode`: every `.` and `-` becomes `_`.
pub fn encode_graph_key(value: &str) -> String {
    value
        .chars()
        .map(|c| if c == '.' || c == '-' { '_' } else { c })
        .collect()
}

/// `NewGraphKey`: a new root graph key.
pub fn new_graph_key(graph_key: &str) -> String {
    encode_graph_key(graph_key)
}

/// `NewDerivativeGraphKey`: `encode(root) + "." + encode(prefix)`.
pub fn new_derivative_graph_key(graph_key: &str, derivative_graph_key_prefix: &str) -> String {
    format!(
        "{}.{}",
        encode_graph_key(graph_key),
        encode_graph_key(derivative_graph_key_prefix)
    )
}

/// `DerivativeGraphKeyFromPrefix`: the root is supplied by the caller in the
/// port (the baseline reads the configured site setting).
pub fn derivative_graph_key_from_prefix(
    root_graph_key: &str,
    derivative_graph_key_prefix: &str,
) -> String {
    new_derivative_graph_key(root_graph_key, derivative_graph_key_prefix)
}

/// `GraphKeyFromDerivativeGraphKey`: returns the root only when the key splits
/// into exactly two `.`-separated parts.
pub fn graph_key_from_derivative_graph_key(derivative_graph_key: &str) -> Option<String> {
    let parts: Vec<&str> = derivative_graph_key.split('.').collect();
    if parts.len() != 2 {
        return None;
    }
    Some(parts[0].to_owned())
}

/// `GraphKey`: `encode` of the configured root, with the empty setting
/// resolving to [`DEFAULT_GRAPH_KEY_ROOT`] exactly like
/// `conf.CodeIntelRankingDocumentReferenceCountsGraphKey`.
pub fn graph_key(raw_root_graph_key: &str) -> String {
    let root = if raw_root_graph_key.is_empty() {
        DEFAULT_GRAPH_KEY_ROOT
    } else {
        raw_root_graph_key
    };
    new_graph_key(root)
}

// ---------------------------------------------------------------------------
// DateTime (`internal/gqlutil/datetime.go`)
// ---------------------------------------------------------------------------

/// `gqlutil.DateTime`: an instant stored as Unix seconds.
///
/// `gqlutil.DateTime.MarshalJSON` formats `Time.UTC()` with Go's
/// `time.RFC3339` layout, which has whole-second precision (fractional seconds
/// parsed from input are dropped) and a `Z` zone designator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime {
    unix_seconds: i64,
}

impl DateTime {
    pub const fn from_unix_seconds(unix_seconds: i64) -> Self {
        Self { unix_seconds }
    }

    pub const fn unix_seconds(self) -> i64 {
        self.unix_seconds
    }

    /// Parse the subset of RFC3339 accepted by Go's `time.Parse` with the
    /// `time.RFC3339` layout: `YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`.
    ///
    /// Fractional seconds are accepted and discarded on formatting, matching
    /// the layout's lack of a fractional-second field.
    pub fn parse_rfc3339(input: &str) -> Option<Self> {
        let bytes = input.as_bytes();
        if bytes.len() < 20 {
            return None;
        }
        if bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
            return None;
        }
        let year = parse_digits(&bytes[0..4])? as i64;
        let month = parse_digits(&bytes[5..7])?;
        let day = parse_digits(&bytes[8..10])?;
        if bytes[13] != b':' || bytes[16] != b':' {
            return None;
        }
        let hour = parse_digits(&bytes[11..13])? as i64;
        let minute = parse_digits(&bytes[14..16])? as i64;
        let second = parse_digits(&bytes[17..19])? as i64;

        if !(1..=12).contains(&month)
            || day < 1
            || day > days_in_month(year, month)
            || hour > 23
            || minute > 59
            || second > 59
        {
            return None;
        }

        let mut index = 19;
        if index < bytes.len() && (bytes[index] == b'.' || bytes[index] == b',') {
            index += 1;
            let start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            if index == start {
                return None;
            }
        }

        let offset_seconds = if index < bytes.len() && bytes[index] == b'Z' {
            index += 1;
            0
        } else if index + 6 <= bytes.len() && (bytes[index] == b'+' || bytes[index] == b'-') {
            if bytes[index + 3] != b':' {
                return None;
            }
            let offset_hour = parse_digits(&bytes[index + 1..index + 3])? as i64;
            let offset_minute = parse_digits(&bytes[index + 4..index + 6])? as i64;
            if offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            let sign = if bytes[index] == b'-' { -1 } else { 1 };
            index += 6;
            sign * (offset_hour * 3600 + offset_minute * 60)
        } else {
            return None;
        };

        if index != bytes.len() {
            return None;
        }

        let days = days_from_civil(year, month, day);
        Some(Self {
            unix_seconds: days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds,
        })
    }

    /// `DateTime.MarshalJSON`: `Time.UTC().Format(time.RFC3339)`.
    pub fn format_rfc3339(self) -> String {
        let days = self.unix_seconds.div_euclid(86_400);
        let seconds_of_day = self.unix_seconds.rem_euclid(86_400);
        let (year, month, day) = civil_from_days(days);
        let hour = seconds_of_day / 3_600;
        let minute = (seconds_of_day % 3_600) / 60;
        let second = seconds_of_day % 60;
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format_rfc3339())
    }
}

fn parse_digits(bytes: &[u8]) -> Option<u32> {
    let mut value = 0_u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Howard Hinnant's `days_from_civil`.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = (if days >= 0 { days } else { days - 146_096 }) / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month as u32, day as u32)
}

// ---------------------------------------------------------------------------
// Shared result types (`internal/codeintel/ranking/shared/types.go`)
// ---------------------------------------------------------------------------

/// `shared.Progress`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub started_at: DateTime,
    pub completed_at: Option<DateTime>,
    pub processed: i64,
    pub total: i64,
}

/// `shared.Summary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub graph_key: String,
    pub visible_to_zoekt: bool,
    pub path_mapper_progress: Progress,
    pub reference_mapper_progress: Progress,
    pub reducer_progress: Option<Progress>,
}

/// `shared.CoverageCounts`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoverageCounts {
    pub num_target_indexes: i64,
    pub num_exported_indexes: i64,
    pub num_repositories_without_current_ranks: i64,
}

/// `DeleteRankingProgressArgs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRankingProgressArgs {
    pub graph_key: String,
}

/// `resolverstubs.EmptyResponse` (`alwaysNil: String`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmptyResponse;

impl EmptyResponse {
    pub fn to_json(self) -> Value {
        json!({ "alwaysNil": Value::Null })
    }
}

/// `resolverstubs.Empty`.
pub const fn empty_response() -> EmptyResponse {
    EmptyResponse
}

// ---------------------------------------------------------------------------
// Injected traits (`iface.go`, `observability.go`)
// ---------------------------------------------------------------------------

/// `graphql.RankingService` (`iface.go`).
///
/// The baseline returns `(value, ok, error)`; `Result<(value, ok), _>` keeps
/// the same three-way distinction.
#[async_trait]
pub trait RankingService: Send + Sync {
    async fn derivative_graph_key(&self) -> Result<(String, bool), RankingError>;
    async fn bump_derivative_graph_key(&self) -> Result<(), RankingError>;
    async fn summaries(&self) -> Result<Vec<Summary>, RankingError>;
    async fn next_job_starts_at(&self) -> Result<(DateTime, bool), RankingError>;
    async fn coverage_counts(&self, graph_key: &str) -> Result<CoverageCounts, RankingError>;
    async fn delete_ranking_progress(&self, graph_key: &str) -> Result<(), RankingError>;
}

/// `sharedresolvers.SiteAdminChecker`:
/// `auth.CheckCurrentUserIsSiteAdmin` wrapped as an injectable check.
#[async_trait]
pub trait SiteAdminChecker: Send + Sync {
    async fn check_current_user_is_site_admin(&self) -> Result<(), RankingError>;
}

/// The three `operations` names registered by `newOperations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankingOperation {
    RankingSummary,
    BumpDerivativeGraphKey,
    DeleteRankingProgress,
}

impl RankingOperation {
    /// Metric label value used by the baseline (`op` label).
    pub const fn name(self) -> &'static str {
        match self {
            Self::RankingSummary => "RankingSummary",
            Self::BumpDerivativeGraphKey => "BumpDerivativeGraphKey",
            Self::DeleteRankingProgress => "DeleteRankingProgress",
        }
    }
}

/// Minimal observation hook. The baseline creates an `observation.Operation`
/// per resolver call and registers `OnCancel`; business logic does not depend
/// on it, so the port only exposes the operation name and an `on_cancel` hook.
pub trait Observer: Send + Sync {
    fn begin(&self, operation: RankingOperation) -> Box<dyn ObservationGuard>;
}

/// Guard returned by [`Observer::begin`].
pub trait ObservationGuard: Send {
    /// `endObservation.OnCancel(ctx, 1, observation.Args{})`.
    fn on_cancel(&mut self);
}

/// Behaviourless observer used when metrics are not wired.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {
    fn begin(&self, _operation: RankingOperation) -> Box<dyn ObservationGuard> {
        Box::new(NoopObservationGuard)
    }
}

struct NoopObservationGuard;

impl ObservationGuard for NoopObservationGuard {
    fn on_cancel(&mut self) {}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error surfaced by the resolvers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RankingError {
    /// `auth.ErrMustBeSiteAdmin`.
    MustBeSiteAdmin,
    /// A `RankingService` / checker error propagated verbatim.
    Service(String),
    /// A time value could not be parsed (harness/model only; the baseline
    /// receives `time.Time` values directly).
    InvalidDateTime(String),
}

impl fmt::Display for RankingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MustBeSiteAdmin => f.write_str("must be site admin"),
            Self::Service(message) => f.write_str(message),
            Self::InvalidDateTime(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RankingError {}

// ---------------------------------------------------------------------------
// Field resolvers (`root_resolver.go:105-190`)
// ---------------------------------------------------------------------------

/// `progressResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressResolver {
    progress: Progress,
}

impl ProgressResolver {
    pub fn new(progress: Progress) -> Self {
        Self { progress }
    }

    /// `StartedAt`: always non-null.
    pub fn started_at(&self) -> DateTime {
        self.progress.started_at
    }

    /// `CompletedAt`: nullable.
    pub fn completed_at(&self) -> Option<DateTime> {
        self.progress.completed_at
    }

    /// `Processed`: `int32(progress.Processed)` (binary truncation, as in Go).
    pub fn processed(&self) -> i32 {
        self.progress.processed as i32
    }

    /// `Total`: `int32(progress.Total)`.
    pub fn total(&self) -> i32 {
        self.progress.total as i32
    }

    pub fn to_json(&self) -> Value {
        json!({
            "startedAt": self.started_at().format_rfc3339(),
            "completedAt": date_time_to_json(self.completed_at()),
            "processed": self.processed(),
            "total": self.total(),
        })
    }
}

/// `rankingSummaryResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankingSummaryResolver {
    summary: Summary,
}

impl RankingSummaryResolver {
    pub fn new(summary: Summary) -> Self {
        Self { summary }
    }

    pub fn graph_key(&self) -> &str {
        &self.summary.graph_key
    }

    pub fn visible_to_zoekt(&self) -> bool {
        self.summary.visible_to_zoekt
    }

    /// `PathMapperProgress`: always non-null.
    pub fn path_mapper_progress(&self) -> ProgressResolver {
        ProgressResolver::new(self.summary.path_mapper_progress.clone())
    }

    /// `ReferenceMapperProgress`: always non-null.
    pub fn reference_mapper_progress(&self) -> ProgressResolver {
        ProgressResolver::new(self.summary.reference_mapper_progress.clone())
    }

    /// `ReducerProgress`: null when the baseline pointer is nil.
    pub fn reducer_progress(&self) -> Option<ProgressResolver> {
        self.summary
            .reducer_progress
            .clone()
            .map(ProgressResolver::new)
    }

    pub fn to_json(&self) -> Value {
        json!({
            "graphKey": self.graph_key(),
            "visibleToZoekt": self.visible_to_zoekt(),
            "pathMapperProgress": self.path_mapper_progress().to_json(),
            "referenceMapperProgress": self.reference_mapper_progress().to_json(),
            "reducerProgress": self.reducer_progress().map(|p| p.to_json()),
        })
    }
}

/// `globalRankingSummaryResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalRankingSummary {
    derivative_graph_key: Option<String>,
    resolvers: Vec<RankingSummaryResolver>,
    next_job_starts_at: Option<DateTime>,
    counts: CoverageCounts,
}

impl GlobalRankingSummary {
    pub fn derivative_graph_key(&self) -> Option<&str> {
        self.derivative_graph_key.as_deref()
    }

    pub fn ranking_summary(&self) -> &[RankingSummaryResolver] {
        &self.resolvers
    }

    /// `NextJobStartsAt`: `gqlutil.DateTimeOrNil`.
    pub fn next_job_starts_at(&self) -> Option<DateTime> {
        self.next_job_starts_at
    }

    /// `NumExportedIndexes`: `int32(counts.NumExportedIndexes)`.
    pub fn num_exported_indexes(&self) -> i32 {
        self.counts.num_exported_indexes as i32
    }

    /// `NumTargetIndexes`: `int32(counts.NumTargetIndexes)`.
    pub fn num_target_indexes(&self) -> i32 {
        self.counts.num_target_indexes as i32
    }

    /// `NumRepositoriesWithoutCurrentRanks`.
    pub fn num_repositories_without_current_ranks(&self) -> i32 {
        self.counts.num_repositories_without_current_ranks as i32
    }

    pub fn to_json(&self) -> Value {
        let summaries: Vec<Value> = self
            .resolvers
            .iter()
            .map(RankingSummaryResolver::to_json)
            .collect();
        json!({
            "derivativeGraphKey": self.derivative_graph_key(),
            "rankingSummary": summaries,
            "nextJobStartsAt": date_time_to_json(self.next_job_starts_at()),
            "numExportedIndexes": self.num_exported_indexes(),
            "numTargetIndexes": self.num_target_indexes(),
            "numRepositoriesWithoutCurrentRanks":
                self.num_repositories_without_current_ranks(),
        })
    }
}

fn date_time_to_json(value: Option<DateTime>) -> Value {
    match value {
        Some(value) => Value::String(value.format_rfc3339()),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// GraphQL object snapshots (`codeintel.ranking.graphql:21-90`)
// ---------------------------------------------------------------------------

/// `ProgressResolver` as a GraphQL object (`RankingSummaryProgress`).
impl ObjectValue for ProgressResolver {
    fn type_name(&self) -> &'static str {
        "RankingSummaryProgress"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "startedAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.started_at().format_rfc3339(),
            )))),
            // `CompletedAt`: `gqlutil.DateTimeOrNil`.
            "completedAt" => Ok(Some(date_time_field(self.completed_at()))),
            "processed" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.processed(),
            ))))),
            "total" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.total()))))),
            _ => Ok(None),
        }
    }
}

/// `RankingSummaryResolver` as a GraphQL object (`RankingSummary`).
impl ObjectValue for RankingSummaryResolver {
    fn type_name(&self) -> &'static str {
        "RankingSummary"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "graphKey" => Ok(Some(FieldValue::Json(Json::Str(
                self.graph_key().to_owned(),
            )))),
            "visibleToZoekt" => Ok(Some(FieldValue::Json(Json::Bool(self.visible_to_zoekt())))),
            "pathMapperProgress" => Ok(Some(FieldValue::Object(Arc::new(
                self.path_mapper_progress(),
            )))),
            "referenceMapperProgress" => Ok(Some(FieldValue::Object(Arc::new(
                self.reference_mapper_progress(),
            )))),
            // `ReducerProgress`: null when the baseline pointer is nil.
            "reducerProgress" => Ok(Some(match self.reducer_progress() {
                Some(progress) => FieldValue::Object(Arc::new(progress)),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `globalRankingSummaryResolver` as a GraphQL object
/// (`GlobalRankingSummary`).
impl ObjectValue for GlobalRankingSummary {
    fn type_name(&self) -> &'static str {
        "GlobalRankingSummary"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `RankingSummary`: `[]RankingSummaryResolver`, service order.
            "rankingSummary" => Ok(Some(FieldValue::List {
                items: self
                    .ranking_summary()
                    .iter()
                    .cloned()
                    .map(|summary| FieldValue::Object(Arc::new(summary)))
                    .collect(),
                element_type: Some("RankingSummary"),
            })),
            "derivativeGraphKey" => Ok(Some(match self.derivative_graph_key() {
                Some(key) => FieldValue::Json(Json::Str(key.to_owned())),
                None => FieldValue::Null,
            })),
            // `NextJobStartsAt`: `gqlutil.DateTimeOrNil`.
            "nextJobStartsAt" => Ok(Some(date_time_field(self.next_job_starts_at()))),
            "numExportedIndexes" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.num_exported_indexes(),
            ))))),
            "numTargetIndexes" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.num_target_indexes(),
            ))))),
            "numRepositoriesWithoutCurrentRanks" => Ok(Some(FieldValue::Json(Json::Int(
                i64::from(self.num_repositories_without_current_ranks()),
            )))),
            _ => Ok(None),
        }
    }
}

/// `gqlutil.DateTimeOrNil` as a [`FieldValue`].
fn date_time_field(value: Option<DateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.format_rfc3339())),
        None => FieldValue::Null,
    }
}

// ---------------------------------------------------------------------------
// Root resolver (`root_resolver.go:20-103`)
// ---------------------------------------------------------------------------

/// `rootResolver`.
pub struct RootResolver {
    ranking_service: Arc<dyn RankingService>,
    site_admin_checker: Arc<dyn SiteAdminChecker>,
    observer: Arc<dyn Observer>,
    /// Raw `codeIntelRanking.documentReferenceCountsGraphKey` setting.
    graph_key_root: String,
}

impl RootResolver {
    pub fn new(
        ranking_service: Arc<dyn RankingService>,
        site_admin_checker: Arc<dyn SiteAdminChecker>,
        observer: Arc<dyn Observer>,
        raw_graph_key_root: impl Into<String>,
    ) -> Self {
        Self {
            ranking_service,
            site_admin_checker,
            observer,
            graph_key_root: raw_graph_key_root.into(),
        }
    }

    /// 🚨 SECURITY: Only site admins may view ranking job summaries.
    ///
    /// Baseline call order is preserved: site-admin check, derivative graph
    /// key, summaries, next job start, coverage counts.
    pub async fn ranking_summary(&self) -> Result<GlobalRankingSummary, RankingError> {
        let mut observation = self.observer.begin(RankingOperation::RankingSummary);
        observation.on_cancel();

        self.site_admin_checker
            .check_current_user_is_site_admin()
            .await?;

        let graph_key = graph_key(&self.graph_key_root);

        let derivative_graph_key = match self.ranking_service.derivative_graph_key().await? {
            (key, true) => Some(key),
            (_, false) => None,
        };

        let summaries = self.ranking_service.summaries().await?;
        let resolvers = summaries
            .into_iter()
            .map(RankingSummaryResolver::new)
            .collect();

        let next_job_starts_at = match self.ranking_service.next_job_starts_at().await? {
            (at, true) => Some(at),
            (_, false) => None,
        };

        let counts = self.ranking_service.coverage_counts(&graph_key).await?;

        Ok(GlobalRankingSummary {
            derivative_graph_key,
            resolvers,
            next_job_starts_at,
            counts,
        })
    }

    /// 🚨 SECURITY: Only site admins may modify ranking graph keys.
    pub async fn bump_derivative_graph_key(&self) -> Result<EmptyResponse, RankingError> {
        let mut observation = self
            .observer
            .begin(RankingOperation::BumpDerivativeGraphKey);
        observation.on_cancel();

        self.site_admin_checker
            .check_current_user_is_site_admin()
            .await?;

        self.ranking_service.bump_derivative_graph_key().await?;
        Ok(empty_response())
    }

    /// 🚨 SECURITY: Only site admins may modify ranking progress records.
    pub async fn delete_ranking_progress(
        &self,
        args: &DeleteRankingProgressArgs,
    ) -> Result<EmptyResponse, RankingError> {
        let mut observation = self.observer.begin(RankingOperation::DeleteRankingProgress);
        observation.on_cancel();

        self.site_admin_checker
            .check_current_user_is_site_admin()
            .await?;

        self.ranking_service
            .delete_ranking_progress(&args.graph_key)
            .await?;
        Ok(empty_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_graph_keys() {
        assert_eq!(encode_graph_key("a.b-c_d"), "a_b_c_d");
        assert_eq!(new_graph_key("dev"), "dev");
        assert_eq!(new_derivative_graph_key("a.b", "c-d"), "a_b.c_d");
        assert_eq!(
            derivative_graph_key_from_prefix("root", "2024.01-02"),
            "root.2024_01_02"
        );
        assert_eq!(graph_key(""), "dev");
        assert_eq!(graph_key("a.b-c"), "a_b_c");
    }

    #[test]
    fn splits_derivative_graph_keys() {
        assert_eq!(
            graph_key_from_derivative_graph_key("a_b.c_d"),
            Some("a_b".to_owned())
        );
        assert_eq!(graph_key_from_derivative_graph_key("a.b.c"), None);
        assert_eq!(graph_key_from_derivative_graph_key("no-dot"), None);
        assert_eq!(
            graph_key_from_derivative_graph_key("a."),
            Some("a".to_owned())
        );
    }

    #[test]
    fn formats_rfc3339_like_go() {
        let cases = [
            ("2024-01-02T03:04:05Z", "2024-01-02T03:04:05Z"),
            ("2024-01-02T03:04:05.999Z", "2024-01-02T03:04:05Z"),
            ("2024-01-02T03:04:05+02:00", "2024-01-02T01:04:05Z"),
            ("2024-01-02T03:04:05-05:30", "2024-01-02T08:34:05Z"),
            ("0001-01-01T00:00:00Z", "0001-01-01T00:00:00Z"),
            ("2024-02-29T23:59:59Z", "2024-02-29T23:59:59Z"),
        ];
        for (input, expected) in cases {
            let parsed = DateTime::parse_rfc3339(input).expect(input);
            assert_eq!(parsed.format_rfc3339(), expected, "input {input}");
        }
    }

    #[test]
    fn rejects_invalid_rfc3339() {
        for input in [
            "",
            "2024-01-02",
            "2023-02-29T00:00:00Z",
            "2024-13-01T00:00:00Z",
            "2024-01-02T24:00:00Z",
            "2024-01-02T00:00:00",
            "2024-01-02T00:00:00+0200",
        ] {
            assert!(DateTime::parse_rfc3339(input).is_none(), "input {input}");
        }
    }

    #[test]
    fn site_admin_error_text() {
        assert_eq!(
            RankingError::MustBeSiteAdmin.to_string(),
            "must be site admin"
        );
    }

    #[test]
    fn appends_ranking_schema() {
        let base = sg_graphql::schema::assemble_oss().expect("oss schema");
        assert!(!base.types.contains_key("GlobalRankingSummary"));

        let schema = append_ranking_schema(&base).expect("ranking schema");
        assert!(schema.types.contains_key("GlobalRankingSummary"));
        assert!(schema.types.contains_key("RankingSummary"));
        assert!(schema.types.contains_key("RankingSummaryProgress"));
        assert!(schema.types["Query"]
            .fields
            .iter()
            .any(|field| field.name == "rankingSummary"));
        assert!(schema.types["Mutation"]
            .fields
            .iter()
            .any(|field| field.name == "bumpDerivativeGraphKey"));
        assert!(schema.types["Mutation"]
            .fields
            .iter()
            .any(|field| field.name == "deleteRankingProgress"));
    }

    #[test]
    fn int32_truncates_like_go() {
        let counts = CoverageCounts {
            num_target_indexes: i64::from(i32::MAX) + 1,
            num_exported_indexes: -1,
            num_repositories_without_current_ranks: 42,
        };
        let summary = GlobalRankingSummary {
            derivative_graph_key: None,
            resolvers: Vec::new(),
            next_job_starts_at: None,
            counts,
        };
        assert_eq!(summary.num_target_indexes(), i32::MIN);
        assert_eq!(summary.num_exported_indexes(), -1);
        assert_eq!(summary.num_repositories_without_current_ranks(), 42);
    }
}
