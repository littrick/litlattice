#![forbid(unsafe_code)]

//! Search aggregations (M9.33a).
//!
//! Ports `searchAggregateResolver` from
//! `cmd/frontend/internal/insights/resolvers/aggregates_resolvers.go` and the
//! pure parts of `internal/insights/aggregation` (`LimitedAggregator`,
//! availability checks, default mode, drilldown query construction and result
//! shaping). The streaming search itself is behind [`AggregationSearchSeam`];
//! the production adapter reuses the frontend search combination root, while
//! tests inject a fake tabulator. No table/column/Redis key is added.
//!
//! See `dev/rs/docs/overviews/m9_33a_insights_preview_production_overview.md`.

use std::collections::BTreeMap;

use sg_insights_query::{
    add_author_filter, add_file_filter, add_repo_filter, add_repo_metadata_filter,
    aggregation_query, detect_search_type, is_single_repo_query, new_pattern_replacer,
    parameters_from_query_plan, parse_query, set_case_sensitivity, MULTIPLE_PATTERN_ERR,
    UNSUPPORTED_PATTERN_TYPE_ERR,
};
use sg_search_query::fields::{FIELD_SELECT, FIELD_TYPE};
use sg_search_query::SearchType;

// ---------------------------------------------------------------------------
// Constants (`aggregates_resolvers.go:27-49`)
// ---------------------------------------------------------------------------

pub const DEFAULT_AGGREGATION_BUFFER_SIZE: usize = 500;
pub const DEFAULT_SEARCH_TIME_LIMIT_SECONDS: i32 = 2;
pub const EXTENDED_SEARCH_TIME_LIMIT_SECONDS_DEFAULT: i32 = 55;
pub const DEFAULT_PROACTIVE_RESULTS_LIMIT: i64 = 50_000;
pub const MAX_PROACTIVE_RESULTS_LIMIT: i64 = 200_000;

pub const INVALID_QUERY_MSG: &str = "Grouping is disabled because the search query is not valid.";
pub const SHARD_TIMEOUT_MSG: &str = "The query was unable to complete in the allocated time.";
pub const GENERAL_TIMEOUT_MSG: &str = "The query was unable to complete in the allocated time.";
pub const PROACTIVE_RESULT_LIMIT_MSG: &str =
    "The query exceeded the number of results allowed over this time period.";
pub const UNKNOWN_AGGREGATION_MODE_MSG: &str = "The requested grouping is not supported.";
pub const UNABLE_TO_MODIFY_QUERY_MSG: &str =
    "The search query was unable to be updated to support grouping.";
pub const UNABLE_TO_COUNT_GROUPS_MSG: &str =
    "The search results were unable to be grouped successfully.";
pub const AUTH_NOT_COMMIT_DIFF_MSG: &str =
    "Grouping by author is only available for diff and commit searches.";
pub const REPO_METADATA_NOT_REPO_SELECT_MSG: &str =
    "Grouping by repo metadata is only available for repository searches.";
pub const CG_INVALID_QUERY_MSG: &str =
    "Grouping by capture group is only available for regexp searches that contain a capturing group.";
pub const CG_MULTIPLE_QUERY_PATTERN_MSG: &str =
    "Grouping by capture group does not support search patterns with the following: and, or, negation.";
pub const FILE_UNSUPPORTED_FIELD_VALUE_FMT: &str =
    "Grouping by file is not available for searches with \"%s:%s\".";
pub const CG_UNSUPPORTED_SELECT_FMT: &str =
    "Grouping by capture group is not available for searches with \"%s:%s\".";

// ---------------------------------------------------------------------------
// Enums / reason types (`internal/insights/types/types.go:209-228`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SearchAggregationMode {
    Repo,
    Path,
    Author,
    CaptureGroup,
    RepoMetadata,
}

impl SearchAggregationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Repo => "REPO",
            Self::Path => "PATH",
            Self::Author => "AUTHOR",
            Self::CaptureGroup => "CAPTURE_GROUP",
            Self::RepoMetadata => "REPO_METADATA",
        }
    }

    pub fn from_raw(raw: &str) -> Option<Self> {
        match raw {
            "REPO" => Some(Self::Repo),
            "PATH" => Some(Self::Path),
            "AUTHOR" => Some(Self::Author),
            "CAPTURE_GROUP" => Some(Self::CaptureGroup),
            "REPO_METADATA" => Some(Self::RepoMetadata),
            _ => None,
        }
    }
}

/// `types.SearchAggregationModes`.
pub const SEARCH_AGGREGATION_MODES: [SearchAggregationMode; 5] = [
    SearchAggregationMode::Repo,
    SearchAggregationMode::Path,
    SearchAggregationMode::Author,
    SearchAggregationMode::CaptureGroup,
    SearchAggregationMode::RepoMetadata,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationNotAvailableReasonType {
    InvalidQuery,
    InvalidAggregationModeForQuery,
    TimeoutExtensionAvailable,
    TimeoutNoExtensionAvailable,
    ErrorOccurred,
    /// `OTHER_ERROR` is declared by `insights_aggregations.graphql` but never
    /// produced by the baseline resolvers (`types` only defines the five above
    /// and uses the out-of-enum `ERROR_OCCURRED`). Kept for completeness so a
    /// future producer can serialize it.
    OtherError,
}

impl AggregationNotAvailableReasonType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidQuery => "INVALID_QUERY",
            Self::InvalidAggregationModeForQuery => "INVALID_AGGREGATION_MODE_FOR_QUERY",
            Self::TimeoutExtensionAvailable => "TIMEOUT_EXTENSION_AVAILABLE",
            Self::TimeoutNoExtensionAvailable => "TIMEOUT_NO_EXTENSION_AVAILABLE",
            Self::ErrorOccurred => "ERROR_OCCURRED",
            Self::OtherError => "OTHER_ERROR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotAvailableReason {
    pub reason: String,
    pub reason_type: AggregationNotAvailableReasonType,
}

impl NotAvailableReason {
    fn new(reason: impl Into<String>, reason_type: AggregationNotAvailableReasonType) -> Self {
        Self {
            reason: reason.into(),
            reason_type,
        }
    }
}

// ---------------------------------------------------------------------------
// Seam
// ---------------------------------------------------------------------------

/// One `(group, count)` pair the baseline tabulator emits
/// (`aggregation.AggregationMatchResult`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationMatchResult {
    pub group: String,
    pub count: i32,
}

/// The outcome of one aggregation search.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregationSearchOutcome {
    pub matches: Vec<AggregationMatchResult>,
    /// `searchAggregationResults.ShardTimeoutOccurred()`.
    pub shard_timeout: bool,
    /// `searchAggregationResults.ResultLimitHit(limit)` input: total result count.
    pub result_count: i32,
    /// The request context hit its deadline.
    pub timed_out: bool,
    /// Non-timeout terminal error (`Aggregations` returns it as a GraphQL error).
    pub error: Option<String>,
}

/// The search execution and configuration seam. The production implementation
/// wraps the frontend streaming search client; tests inject a fake.
pub trait AggregationSearchSeam: Send + Sync + std::fmt::Debug {
    /// `streaming.NewInsightsSearchClient(...).Search` materialized through
    /// `aggregation.NewSearchResultsAggregatorWithContext`.
    fn search(
        &self,
        modified_query: &str,
        mode: SearchAggregationMode,
    ) -> AggregationSearchOutcome;

    /// `conf.Get().InsightsAggregationsBufferSize`.
    fn buffer_size(&self) -> i32;

    /// `getProactiveResultLimit`.
    fn proactive_result_limit(&self) -> i64;

    /// `getExtendedTimeout`: user setting or the 55s default.
    fn extended_timeout_seconds(&self) -> i32;
}

// ---------------------------------------------------------------------------
// LimitedAggregator (`internal/insights/aggregation/limited_aggregator.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub label: String,
    pub count: i32,
}

impl Aggregate {
    fn less(&self, other: &Aggregate) -> bool {
        if self.count == other.count {
            return self.label <= other.label;
        }
        self.count < other.count
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtherCount {
    pub result_count: i32,
    pub group_count: i32,
}

#[derive(Debug, Clone)]
pub struct LimitedAggregator {
    result_buffer_size: usize,
    smallest_result: Option<Aggregate>,
    results: BTreeMap<String, i32>,
    other_count: OtherCount,
}

impl LimitedAggregator {
    /// `NewLimitedAggregator`.
    pub fn new(buffer_size: usize) -> Self {
        Self {
            result_buffer_size: buffer_size,
            smallest_result: None,
            results: BTreeMap::new(),
            other_count: OtherCount::default(),
        }
    }

    /// `Add`: best-effort aggregation for one `(label, count)` result.
    pub fn add(&mut self, label: &str, count: i32) {
        if self.result_buffer_size == 0 {
            return;
        }
        if !self.results.contains_key(label) {
            let new_result = Aggregate {
                label: label.to_owned(),
                count,
            };
            if self.results.len() < self.result_buffer_size {
                self.results.insert(label.to_owned(), count);
                if self
                    .smallest_result
                    .as_ref()
                    .is_none_or(|smallest| new_result.less(smallest))
                {
                    self.smallest_result = Some(new_result);
                }
            } else {
                let smallest = self.smallest_result.clone().expect("non-empty buffer");
                if smallest.less(&new_result) {
                    self.update_other_count(smallest.count, 1);
                    self.results.remove(&smallest.label);
                    self.results.insert(label.to_owned(), count);
                    self.update_smallest_aggregate();
                } else {
                    self.update_other_count(count, 1);
                }
            }
        } else {
            *self.results.get_mut(label).expect("present") += count;
            if self
                .smallest_result
                .as_ref()
                .is_none_or(|smallest| label == smallest.label)
            {
                self.update_smallest_aggregate();
            }
        }
    }

    fn find_smallest_aggregate(&self) -> Option<Aggregate> {
        let mut smallest: Option<Aggregate> = None;
        for (label, count) in &self.results {
            let candidate = Aggregate {
                label: label.clone(),
                count: *count,
            };
            if smallest.as_ref().is_none_or(|current| candidate.less(current)) {
                smallest = Some(candidate);
            }
        }
        smallest
    }

    fn update_smallest_aggregate(&mut self) {
        if let Some(smallest) = self.find_smallest_aggregate() {
            self.smallest_result = Some(smallest);
        }
    }

    fn update_other_count(&mut self, result_count: i32, group_count: i32) {
        self.other_count.result_count += result_count;
        self.other_count.group_count += group_count;
    }

    /// `SortAggregate`: descending by count, ties by label descending.
    pub fn sort_aggregate(&self) -> Vec<Aggregate> {
        let mut slice: Vec<Aggregate> = self
            .results
            .iter()
            .map(|(label, count)| Aggregate {
                label: label.clone(),
                count: *count,
            })
            .collect();
        slice.sort_by(|a, b| {
            if b.less(a) {
                std::cmp::Ordering::Less
            } else if a.less(b) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        slice
    }

    pub fn other_counts(&self) -> OtherCount {
        self.other_count
    }
}

// ---------------------------------------------------------------------------
// Availability / default mode
// ---------------------------------------------------------------------------

type CanAggregateBy = fn(&str, &str) -> (bool, Option<NotAvailableReason>, Option<String>);

fn get_aggregate_by(mode: SearchAggregationMode) -> CanAggregateBy {
    match mode {
        SearchAggregationMode::Repo => can_aggregate_by_repo,
        SearchAggregationMode::Path => can_aggregate_by_path,
        SearchAggregationMode::Author => can_aggregate_by_author,
        SearchAggregationMode::CaptureGroup => can_aggregate_by_capture_group,
        SearchAggregationMode::RepoMetadata => can_aggregate_by_repo_metadata,
    }
}

fn invalid_query(err: String) -> (bool, Option<NotAvailableReason>, Option<String>) {
    (
        false,
        Some(NotAvailableReason::new(
            INVALID_QUERY_MSG,
            AggregationNotAvailableReasonType::InvalidQuery,
        )),
        Some(err),
    )
}

fn can_aggregate_by_repo(query: &str, pattern_type: &str) -> (bool, Option<NotAvailableReason>, Option<String>) {
    match parse_query(query, pattern_type) {
        Ok(_) => (true, None, None),
        Err(err) => invalid_query(format!("ParseQuery: {err}")),
    }
}

fn can_aggregate_by_path(query: &str, pattern_type: &str) -> (bool, Option<NotAvailableReason>, Option<String>) {
    let plan = match parse_query(query, pattern_type) {
        Ok(plan) => plan,
        Err(err) => return invalid_query(format!("ParseQuery: {err}")),
    };
    for parameter in parameters_from_query_plan(&plan).iter() {
        if parameter.field == FIELD_SELECT || parameter.field == FIELD_TYPE {
            let value = parameter.value.to_lowercase();
            if value == "commit" || value == "diff" || value == "repo" {
                return (
                    false,
                    Some(NotAvailableReason::new(
                        FILE_UNSUPPORTED_FIELD_VALUE_FMT
                            .replacen("%s", &parameter.field, 1)
                            .replacen("%s", &parameter.value, 1),
                        AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                    )),
                    None,
                );
            }
        }
    }
    (true, None, None)
}

fn can_aggregate_by_author(query: &str, pattern_type: &str) -> (bool, Option<NotAvailableReason>, Option<String>) {
    let plan = match parse_query(query, pattern_type) {
        Ok(plan) => plan,
        Err(err) => return invalid_query(format!("ParseQuery: {err}")),
    };
    for parameter in parameters_from_query_plan(&plan).iter() {
        if parameter.field == FIELD_SELECT || parameter.field == FIELD_TYPE {
            if parameter.value == "diff" || parameter.value == "commit" {
                return (true, None, None);
            }
        }
    }
    (
        false,
        Some(NotAvailableReason::new(
            AUTH_NOT_COMMIT_DIFF_MSG,
            AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
        )),
        None,
    )
}

fn can_aggregate_by_capture_group(
    query: &str,
    pattern_type: &str,
) -> (bool, Option<NotAvailableReason>, Option<String>) {
    let plan = match parse_query(query, pattern_type) {
        Ok(plan) => plan,
        Err(err) => return invalid_query(format!("ParseQuery: {err}")),
    };

    let search_type = match detect_search_type(query, pattern_type) {
        Ok(search_type) => search_type,
        Err(err) => {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_INVALID_QUERY_MSG,
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                Some(err),
            )
        }
    };
    if !(search_type == SearchType::Regex || search_type == SearchType::Standard) {
        return (
            false,
            Some(NotAvailableReason::new(
                CG_INVALID_QUERY_MSG,
                AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
            )),
            None,
        );
    }

    let replacer = match new_pattern_replacer(query, search_type) {
        Ok(replacer) => replacer,
        Err(err) if err == UNSUPPORTED_PATTERN_TYPE_ERR => {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_INVALID_QUERY_MSG,
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                None,
            )
        }
        Err(err) if err == MULTIPLE_PATTERN_ERR => {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_MULTIPLE_QUERY_PATTERN_MSG,
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                None,
            )
        }
        Err(err) => {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_INVALID_QUERY_MSG,
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                Some(format!("pattern parsing: {err}")),
            )
        }
    };

    if !replacer.has_capture_groups() {
        return (
            false,
            Some(NotAvailableReason::new(
                CG_INVALID_QUERY_MSG,
                AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
            )),
            None,
        );
    }

    let not_allowed_select = ["repo", "file", "commit", "symbol"];
    let not_allowed_type = ["diff", "symbol"];
    for parameter in parameters_from_query_plan(&plan).iter() {
        let value = parameter.value.to_lowercase();
        if parameter.field.eq_ignore_ascii_case(FIELD_SELECT) && not_allowed_select.contains(&value.as_str()) {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_UNSUPPORTED_SELECT_FMT
                        .replacen("%s", &parameter.field.to_lowercase(), 1)
                        .replacen("%s", &value, 1),
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                None,
            );
        }
        if parameter.field.eq_ignore_ascii_case(FIELD_TYPE) && not_allowed_type.contains(&value.as_str()) {
            return (
                false,
                Some(NotAvailableReason::new(
                    CG_UNSUPPORTED_SELECT_FMT
                        .replacen("%s", &parameter.field.to_lowercase(), 1)
                        .replacen("%s", &value, 1),
                    AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
                )),
                None,
            );
        }
    }

    (true, None, None)
}

fn can_aggregate_by_repo_metadata(
    query: &str,
    pattern_type: &str,
) -> (bool, Option<NotAvailableReason>, Option<String>) {
    let plan = match parse_query(query, pattern_type) {
        Ok(plan) => plan,
        Err(err) => return invalid_query(format!("ParseQuery: {err}")),
    };
    for parameter in parameters_from_query_plan(&plan).iter() {
        if parameter.field == FIELD_SELECT && parameter.value == "repo" {
            return (true, None, None);
        }
    }
    (
        false,
        Some(NotAvailableReason::new(
            REPO_METADATA_NOT_REPO_SELECT_MSG,
            AggregationNotAvailableReasonType::InvalidAggregationModeForQuery,
        )),
        None,
    )
}

/// `getNotAvailableReason`.
pub fn get_not_available_reason(
    query: &str,
    pattern_type: &str,
    mode: SearchAggregationMode,
) -> (Option<NotAvailableReason>, Option<String>) {
    let aggregate_by = get_aggregate_by(mode);
    let (_, reason, err) = aggregate_by(query, pattern_type);
    (reason, err)
}

/// `getDefaultAggregationMode`.
pub fn get_default_aggregation_mode(query: &str, pattern_type: &str) -> SearchAggregationMode {
    let (capture_group, _, _) = can_aggregate_by_capture_group(query, pattern_type);
    if capture_group {
        return SearchAggregationMode::CaptureGroup;
    }
    let (author, _, _) = can_aggregate_by_author(query, pattern_type);
    if author {
        return SearchAggregationMode::Author;
    }
    let (path, _, _) = can_aggregate_by_path(query, pattern_type);
    let targets_single_repo = is_single_repo_query(query).unwrap_or(false);
    if path && targets_single_repo {
        return SearchAggregationMode::Path;
    }
    SearchAggregationMode::Repo
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModeAvailability {
    pub mode: SearchAggregationMode,
    pub available: bool,
    pub reason_unavailable: Option<String>,
}

/// `searchAggregateResolver.ModeAvailability`.
pub fn mode_availability(query: &str, pattern_type: &str) -> Vec<ModeAvailability> {
    SEARCH_AGGREGATION_MODES
        .iter()
        .map(|&mode| {
            let (available, reason, _) = get_aggregate_by(mode)(query, pattern_type);
            ModeAvailability {
                mode,
                available,
                reason_unavailable: reason.map(|reason| reason.reason),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Drilldown query / results
// ---------------------------------------------------------------------------

/// `buildDrilldownQuery`.
pub fn build_drilldown_query(
    mode: SearchAggregationMode,
    original_query: &str,
    drilldown: &str,
    pattern_type: &str,
) -> Result<String, String> {
    match mode {
        SearchAggregationMode::Repo => add_repo_filter(original_query, drilldown),
        SearchAggregationMode::RepoMetadata => {
            add_repo_metadata_filter(original_query, drilldown)
        }
        SearchAggregationMode::Path => add_file_filter(original_query, drilldown),
        SearchAggregationMode::Author => add_author_filter(original_query, drilldown),
        SearchAggregationMode::CaptureGroup => {
            let replacer = new_pattern_replacer(
                original_query,
                sg_insights_query::search_type_from_string(pattern_type)?,
            )?;
            set_case_sensitivity(&replacer.replace(drilldown), true)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationGroup {
    pub label: String,
    pub count: i32,
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationResults {
    pub groups: Vec<AggregationGroup>,
    pub other_result_count: i32,
    pub other_group_count: i32,
    pub total_count: u32,
}

/// `buildResults`.
pub fn build_results(
    aggregator: &LimitedAggregator,
    limit: usize,
    mode: SearchAggregationMode,
    original_query: &str,
    pattern_type: &str,
) -> AggregationResults {
    let sorted = aggregator.sort_aggregate();
    let mut groups = Vec::with_capacity(limit.min(sorted.len()));
    let mut other_results = aggregator.other_counts().result_count;
    let mut other_groups = aggregator.other_counts().group_count;
    let mut total_count = 0u32;

    for (index, aggregate) in sorted.iter().enumerate() {
        if index < limit {
            let drilldown_query = build_drilldown_query(
                mode,
                original_query,
                &aggregate.label,
                pattern_type,
            )
            .unwrap_or_else(|_| original_query.to_owned());
            groups.push(AggregationGroup {
                label: aggregate.label.clone(),
                count: aggregate.count,
                query: Some(drilldown_query),
            });
        } else {
            other_groups += 1;
            other_results += aggregate.count;
        }
        total_count += aggregate.count as u32;
    }

    AggregationResults {
        groups,
        other_result_count: other_results,
        other_group_count: other_groups,
        total_count,
    }
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchAggregationsInput {
    pub query: String,
    pub pattern_type: String,
    pub mode: Option<SearchAggregationMode>,
    pub limit: i32,
    pub extended_timeout: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationResult {
    Exhaustive {
        results: AggregationResults,
        mode: SearchAggregationMode,
    },
    NonExhaustive {
        results: AggregationResults,
        mode: SearchAggregationMode,
    },
    NotAvailable {
        reason: String,
        reason_type: AggregationNotAvailableReasonType,
        mode: SearchAggregationMode,
    },
}

/// `getProactiveResultLimit` with the configured value.
pub fn proactive_result_limit(configured: i64) -> i64 {
    let value = if configured <= 0 {
        DEFAULT_PROACTIVE_RESULTS_LIMIT
    } else {
        configured
    };
    value.min(MAX_PROACTIVE_RESULTS_LIMIT)
}

/// `searchSuccessful`.
pub fn search_successful(
    tabulation_errors: &[String],
    shard_timeout_occurred: bool,
    running_with_extended_timeout: bool,
    result_limit_hit: bool,
) -> (bool, Option<NotAvailableReason>) {
    if !tabulation_errors.is_empty() {
        return (
            false,
            Some(NotAvailableReason::new(
                UNABLE_TO_COUNT_GROUPS_MSG,
                AggregationNotAvailableReasonType::ErrorOccurred,
            )),
        );
    }
    if shard_timeout_occurred {
        let reason_type = if running_with_extended_timeout {
            AggregationNotAvailableReasonType::TimeoutNoExtensionAvailable
        } else {
            AggregationNotAvailableReasonType::TimeoutExtensionAvailable
        };
        return (
            false,
            Some(NotAvailableReason::new(SHARD_TIMEOUT_MSG, reason_type)),
        );
    }
    if !running_with_extended_timeout && result_limit_hit {
        return (
            false,
            Some(NotAvailableReason::new(
                PROACTIVE_RESULT_LIMIT_MSG,
                AggregationNotAvailableReasonType::TimeoutExtensionAvailable,
            )),
        );
    }
    (true, None)
}

/// `searchAggregateResolver.Aggregations`.
pub fn search_aggregations(
    seam: &dyn AggregationSearchSeam,
    input: &SearchAggregationsInput,
) -> Result<AggregationResult, String> {
    let mode = match input.mode {
        Some(mode) => mode,
        None => get_default_aggregation_mode(&input.query, &input.pattern_type),
    };

    if let (Some(reason), _) = get_not_available_reason(&input.query, &input.pattern_type, mode) {
        return Ok(AggregationResult::NotAvailable {
            reason: reason.reason,
            reason_type: reason.reason_type,
            mode,
        });
    }

    let proactive_limit = seam.proactive_result_limit().max(0);
    let proactive_limit = proactive_result_limit(proactive_limit);
    let (search_time_limit, count_value) = if input.extended_timeout {
        (seam.extended_timeout_seconds().max(1), "all".to_owned())
    } else {
        (
            DEFAULT_SEARCH_TIME_LIMIT_SECONDS,
            proactive_limit.to_string(),
        )
    };

    let modified_query = match aggregation_query(
        &input.query,
        search_time_limit + 1,
        &count_value,
    ) {
        Ok(query) => query,
        Err(_err) => {
            return Ok(AggregationResult::NotAvailable {
                reason: UNABLE_TO_MODIFY_QUERY_MSG.to_owned(),
                reason_type: AggregationNotAvailableReasonType::ErrorOccurred,
                mode,
            });
        }
    };

    let buffer_size = seam.buffer_size();
    let buffer_size = if buffer_size <= 0 {
        DEFAULT_AGGREGATION_BUFFER_SIZE
    } else {
        buffer_size as usize
    };
    let mut aggregator = LimitedAggregator::new(buffer_size);

    let outcome = seam.search(&modified_query, mode);
    if outcome.timed_out {
        let reason_type = if input.extended_timeout {
            AggregationNotAvailableReasonType::TimeoutNoExtensionAvailable
        } else {
            AggregationNotAvailableReasonType::TimeoutExtensionAvailable
        };
        return Ok(AggregationResult::NotAvailable {
            reason: GENERAL_TIMEOUT_MSG.to_owned(),
            reason_type,
            mode,
        });
    }
    if let Some(error) = outcome.error {
        return Err(error);
    }

    for matched in &outcome.matches {
        aggregator.add(&matched.group, matched.count);
    }

    let result_limit_hit = proactive_limit > 0
        && i64::from(outcome.result_count) >= proactive_limit;
    let (successful, failure_reason) = search_successful(
        &[],
        outcome.shard_timeout,
        input.extended_timeout,
        result_limit_hit,
    );
    if !successful {
        let reason = failure_reason.expect("reason on failure");
        return Ok(AggregationResult::NotAvailable {
            reason: reason.reason,
            reason_type: reason.reason_type,
            mode,
        });
    }

    let results = build_results(
        &aggregator,
        input.limit.max(0) as usize,
        mode,
        &input.query,
        &input.pattern_type,
    );
    if aggregator.other_counts().group_count == 0 {
        Ok(AggregationResult::Exhaustive { results, mode })
    } else {
        Ok(AggregationResult::NonExhaustive { results, mode })
    }
}

