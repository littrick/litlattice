#![forbid(unsafe_code)]

//! Code Insights live preview and aggregation (M9.32a).
//!
//! Ports the four GraphQL query fields that were defined but not wired:
//!
//! - `Query.searchInsightLivePreview` and `Query.searchInsightPreview`
//!   (`live_preview_resolvers.go`);
//! - `Query.validateScopedInsightQuery` and `Query.previewRepositoriesFromQuery`
//!   (`scoped_insight_resolvers.go`).
//!
//! The search / repository / gitserver side is behind the injectable
//! [`PreviewSearchSeam`]. The production `InsightsDatabase` does not provide a
//! connected seam yet, so [`UnavailablePreviewSeam`] reports
//! `search execution is not connected` exactly where the baseline would surface
//! a search-execution error; it never fabricates aggregation results.
//!
//! Query construction reuses `sg-insights-query`
//! (`repository_scope_query`, `with_count`, `single_repo_query`,
//! `single_repo_query_indexed`, `compute_insight_command_query_with_map_type`,
//! `point_diff_query`, `point_in_time_query`, `new_pattern_replacer`); the
//! sample-time window reuses `sg-insights`
//! (`timeseries::build_sample_times`, `compression::uncompressed_plan`).
//!
//! See `dev/rs/docs/overviews/m9_32a_insights_preview_overview.md`.

use std::collections::BTreeMap;
use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_insights::compression::{uncompressed_plan, BackfillPlan};
use sg_insights::timeseries::{build_sample_times, TimeInterval};
use sg_insights::types::IntervalUnit;
use sg_insights_query::{
    code_insights_query_defaults, compute_insight_command_query_with_map_type,
    new_pattern_replacer, point_diff_query, point_in_time_query, repository_scope_query,
    single_repo_query, single_repo_query_indexed, with_count, PointDiffQueryOpts,
};
use sg_search_query::SearchType;
use time::OffsetDateTime;

/// `maxPreviewRepos` (`live_preview_resolvers.go:20`).
pub const MAX_PREVIEW_REPOS: usize = 20;

/// `repoLimitExceededErrorCode` / `noDataErrorCode` / `invalidArgsErrorCode`.
pub const REPO_LIMIT_EXCEEDED: &str = "RepoLimitExceeded";
pub const NO_DATA: &str = "NoData";
pub const INVALID_ARGS: &str = "InvalidArgs";

// ---------------------------------------------------------------------------
// Seam
// ---------------------------------------------------------------------------

/// One repository returned by a repo search (`types.MinimalRepo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRepo {
    pub name: String,
}

/// One `(capture group value, count)` pair from a compute stream
/// (`query.GroupedResults`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedMatch {
    pub value: String,
    pub count: i64,
}

/// Injectable search/repository/gitserver seam.
///
/// `Err` strings are wrapped by the executor exactly like the Go `error`
/// values (`errors.Wrap`/`Wrapf`).
pub trait PreviewSearchSeam: Send + Sync + std::fmt::Debug {
    /// `time.Now()` used for sample times and the preview clock.
    fn now(&self) -> OffsetDateTime;

    /// `StreamingRepoQueryExecutor.ExecuteRepoList`.
    fn repo_search(&self, query: &str) -> Result<Vec<PreviewRepo>, String>;

    /// `db.Repos().GetByName` (the id is unused; `Err` carries the store error).
    fn repo_by_name(&self, name: &str) -> Result<i32, String>;

    /// `insights/gitserver.GitFirstEverCommit`; `Ok(None)` is an empty repo.
    fn first_commit(&self, repo: &str) -> Result<Option<OffsetDateTime>, String>;

    /// `GitCommitClient.RecentCommits(...)`; `Ok(None)` means no commit.
    fn recent_commit(&self, repo: &str, before: OffsetDateTime)
        -> Result<Option<String>, String>;

    /// Compute stream decoder (`streamCompute` / `MatchContextComputeDecoder`).
    fn capture_group_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String>;

    /// Compute text-extra stream (`streamTextExtraCompute` / `ComputeTextDecoder`).
    fn compute_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String>;

    /// Streaming tabulation decoder; the total match count.
    fn streaming_search(&self, query: &str) -> Result<i64, String>;
}

/// Default seam: search execution is not wired to the frontend combination
/// root yet. Every call fails instead of returning fabricated data.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailablePreviewSeam;

const NOT_CONNECTED: &str = "search execution is not connected";

impl PreviewSearchSeam for UnavailablePreviewSeam {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
    fn repo_search(&self, _query: &str) -> Result<Vec<PreviewRepo>, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn repo_by_name(&self, _name: &str) -> Result<i32, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn first_commit(&self, _repo: &str) -> Result<Option<OffsetDateTime>, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn recent_commit(
        &self,
        _repo: &str,
        _before: OffsetDateTime,
    ) -> Result<Option<String>, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn capture_group_search(&self, _query: &str) -> Result<Vec<GroupedMatch>, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn compute_search(&self, _query: &str) -> Result<Vec<GroupedMatch>, String> {
        Err(NOT_CONNECTED.to_owned())
    }
    fn streaming_search(&self, _query: &str) -> Result<i64, String> {
        Err(NOT_CONNECTED.to_owned())
    }
}

/// Production seam assembled by the frontend search combination root.
///
/// The frontend keeps search execution behind a synchronous [`search_stream`]
/// tree runner, so the combination root injects already-wired executors here
/// instead of re-implementing the streaming/gitserver clients. Operations left
/// `None` keep the baseline's `search execution is not connected` error.
#[derive(Default)]
pub struct ProductionPreviewSeam {
    now: Option<Box<dyn Fn() -> OffsetDateTime + Send + Sync>>,
    repo_search: Option<Box<dyn Fn(&str) -> Result<Vec<PreviewRepo>, String> + Send + Sync>>,
    repo_by_name: Option<Box<dyn Fn(&str) -> Result<i32, String> + Send + Sync>>,
    first_commit:
        Option<Box<dyn Fn(&str) -> Result<Option<OffsetDateTime>, String> + Send + Sync>>,
    recent_commit:
        Option<Box<dyn Fn(&str, OffsetDateTime) -> Result<Option<String>, String> + Send + Sync>>,
    capture_group_search:
        Option<Box<dyn Fn(&str) -> Result<Vec<GroupedMatch>, String> + Send + Sync>>,
    compute_search: Option<Box<dyn Fn(&str) -> Result<Vec<GroupedMatch>, String> + Send + Sync>>,
    streaming_search: Option<Box<dyn Fn(&str) -> Result<i64, String> + Send + Sync>>,
}

impl std::fmt::Debug for ProductionPreviewSeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProductionPreviewSeam")
            .field("repo_search", &self.repo_search.is_some())
            .field("capture_group_search", &self.capture_group_search.is_some())
            .field("compute_search", &self.compute_search.is_some())
            .field("streaming_search", &self.streaming_search.is_some())
            .finish()
    }
}

impl ProductionPreviewSeam {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_now(mut self, now: impl Fn() -> OffsetDateTime + Send + Sync + 'static) -> Self {
        self.now = Some(Box::new(now));
        self
    }
    pub fn with_repo_search(
        mut self,
        repo_search: impl Fn(&str) -> Result<Vec<PreviewRepo>, String> + Send + Sync + 'static,
    ) -> Self {
        self.repo_search = Some(Box::new(repo_search));
        self
    }
    pub fn with_repo_by_name(
        mut self,
        repo_by_name: impl Fn(&str) -> Result<i32, String> + Send + Sync + 'static,
    ) -> Self {
        self.repo_by_name = Some(Box::new(repo_by_name));
        self
    }
    pub fn with_first_commit(
        mut self,
        first_commit: impl Fn(&str) -> Result<Option<OffsetDateTime>, String> + Send + Sync + 'static,
    ) -> Self {
        self.first_commit = Some(Box::new(first_commit));
        self
    }
    pub fn with_recent_commit(
        mut self,
        recent_commit: impl Fn(&str, OffsetDateTime) -> Result<Option<String>, String>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.recent_commit = Some(Box::new(recent_commit));
        self
    }
    pub fn with_capture_group_search(
        mut self,
        capture_group_search: impl Fn(&str) -> Result<Vec<GroupedMatch>, String>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.capture_group_search = Some(Box::new(capture_group_search));
        self
    }
    pub fn with_compute_search(
        mut self,
        compute_search: impl Fn(&str) -> Result<Vec<GroupedMatch>, String> + Send + Sync + 'static,
    ) -> Self {
        self.compute_search = Some(Box::new(compute_search));
        self
    }
    pub fn with_streaming_search(
        mut self,
        streaming_search: impl Fn(&str) -> Result<i64, String> + Send + Sync + 'static,
    ) -> Self {
        self.streaming_search = Some(Box::new(streaming_search));
        self
    }
}

impl PreviewSearchSeam for ProductionPreviewSeam {
    fn now(&self) -> OffsetDateTime {
        self.now
            .as_ref()
            .map_or_else(OffsetDateTime::now_utc, |now| now())
    }
    fn repo_search(&self, query: &str) -> Result<Vec<PreviewRepo>, String> {
        match &self.repo_search {
            Some(repo_search) => repo_search(query),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn repo_by_name(&self, name: &str) -> Result<i32, String> {
        match &self.repo_by_name {
            Some(repo_by_name) => repo_by_name(name),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn first_commit(&self, repo: &str) -> Result<Option<OffsetDateTime>, String> {
        match &self.first_commit {
            Some(first_commit) => first_commit(repo),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn recent_commit(
        &self,
        repo: &str,
        before: OffsetDateTime,
    ) -> Result<Option<String>, String> {
        match &self.recent_commit {
            Some(recent_commit) => recent_commit(repo, before),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn capture_group_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String> {
        match &self.capture_group_search {
            Some(capture_group_search) => capture_group_search(query),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn compute_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String> {
        match &self.compute_search {
            Some(compute_search) => compute_search(query),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
    fn streaming_search(&self, query: &str) -> Result<i64, String> {
        match &self.streaming_search {
            Some(streaming_search) => streaming_search(query),
            None => Err(NOT_CONNECTED.to_owned()),
        }
    }
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepositoryScopeInput {
    pub repositories: Vec<String>,
    pub repository_criteria: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepIntervalInput {
    pub unit: String,
    pub value: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimeScopeInput {
    pub step_interval: Option<StepIntervalInput>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchSeriesPreviewInput {
    pub query: String,
    pub label: String,
    pub generated_from_capture_groups: bool,
    pub group_by: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchInsightPreviewInput {
    pub repository_scope: RepositoryScopeInput,
    pub time_scope: TimeScopeInput,
    pub series: Vec<SearchSeriesPreviewInput>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchInsightLivePreviewInput {
    pub query: String,
    pub label: String,
    pub repository_scope: RepositoryScopeInput,
    pub time_scope: TimeScopeInput,
    pub generated_from_capture_groups: bool,
    pub group_by: Option<String>,
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

/// `livePreviewError`: an optional GraphQL extension `code` plus the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewError {
    pub code: Option<&'static str>,
    pub message: String,
}

impl PreviewError {
    fn plain(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }
    fn coded(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: Some(code),
            message: message.into(),
        }
    }
}

/// `query.GeneratedTimeSeries`.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedTimeSeries {
    pub label: String,
    pub series_id: String,
    pub points: Vec<TimeDataPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeDataPoint {
    pub time: OffsetDateTime,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreviewPoint {
    pub time: OffsetDateTime,
    pub value: f64,
    pub diff_query: Option<String>,
    pub point_in_time_query: Option<String>,
}

/// `searchInsightLivePreviewSeriesResolver`.
#[derive(Debug, Clone, PartialEq)]
pub struct PreviewSeries {
    pub label: String,
    pub points: Vec<PreviewPoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedInsightQueryPayload {
    pub query: String,
    pub is_valid: bool,
    pub invalid_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPreviewPayload {
    pub query: String,
    pub number_of_repositories: Option<i32>,
}

// ---------------------------------------------------------------------------
// Validation / scope helpers
// ---------------------------------------------------------------------------

/// `isValidPreviewArgs`.
pub fn is_valid_preview_args(input: &SearchInsightPreviewInput) -> Result<(), PreviewError> {
    if input.time_scope.step_interval.is_none() {
        return Err(PreviewError::coded(
            INVALID_ARGS,
            "live preview currently only supports a time interval time scope",
        ));
    }
    let has_repo_criteria = input.repository_scope.repository_criteria.is_some();
    if has_repo_criteria && !input.repository_scope.repositories.is_empty() {
        return Err(PreviewError::coded(
            INVALID_ARGS,
            "can not specify both a repository list and a repository search",
        ));
    }
    if has_repo_criteria && input.series.iter().any(|series| series.group_by.is_some()) {
        return Err(PreviewError::coded(
            INVALID_ARGS,
            "group by insights do not support selecting repositories using a search",
        ));
    }
    Ok(())
}

/// `getPreviewRepos`.
pub fn get_preview_repos(
    seam: &dyn PreviewSearchSeam,
    scope: &RepositoryScopeInput,
) -> Result<Vec<String>, String> {
    let Some(criteria) = &scope.repository_criteria else {
        return Ok(scope.repositories.clone());
    };
    let repo_query = repository_scope_query(criteria)?;
    let limited = with_count(&repo_query, &(MAX_PREVIEW_REPOS + 1).to_string())?;
    let repos = seam.repo_search(&limited)?;
    Ok(repos.into_iter().map(|repo| repo.name).collect())
}

/// `ValidateScopedInsightQuery`.
pub fn validate_scoped_insight_query(query: &str) -> ScopedInsightQueryPayload {
    match sg_insights_query::parse_query(query, "literal") {
        Err(err) => ScopedInsightQueryPayload {
            query: query.to_owned(),
            is_valid: false,
            invalid_reason: Some(format!("the input query is invalid: {err}")),
        },
        Ok(plan) => {
            let (reason, valid) = sg_insights_query::is_valid_scope_query(&plan);
            if !valid {
                ScopedInsightQueryPayload {
                    query: query.to_owned(),
                    is_valid: false,
                    invalid_reason: Some(reason),
                }
            } else {
                ScopedInsightQueryPayload {
                    query: query.to_owned(),
                    is_valid: true,
                    invalid_reason: None,
                }
            }
        }
    }
}

/// `PreviewRepositoriesFromQuery`.
pub fn preview_repositories_from_query(
    seam: &dyn PreviewSearchSeam,
    query: &str,
) -> Result<RepositoryPreviewPayload, String> {
    let plan = sg_insights_query::parse_query(query, "literal")
        .map_err(|err| format!("the input query is invalid: {err}"))?;
    let (reason, valid) = sg_insights_query::is_valid_scope_query(&plan);
    if !valid {
        return Err(format!(
            "the input query cannot be used for previewing repositories: {reason}"
        ));
    }
    let repo_scope_query = repository_scope_query(query)
        .map_err(|err| format!("could not build repository scope query: {err}"))?;
    let repos = seam
        .repo_search(&repo_scope_query)
        .map_err(|err| format!("executing the repository search errored: {err}"))?;
    Ok(RepositoryPreviewPayload {
        query: repo_scope_query,
        number_of_repositories: Some(repos.len() as i32),
    })
}

// ---------------------------------------------------------------------------
// Executors
// ---------------------------------------------------------------------------

fn interval_from(step: &StepIntervalInput) -> TimeInterval {
    TimeInterval {
        unit: IntervalUnit::from_str_value(&step.unit),
        value: i64::from(step.value),
    }
}

/// `generateTimes(plan)`.
fn generate_times(plan: &BackfillPlan) -> BTreeMap<OffsetDateTime, i64> {
    let mut times = BTreeMap::new();
    for execution in &plan.executions {
        times.insert(execution.recording_time, 0);
        for recording in &execution.shared_recordings {
            times.insert(*recording, 0);
        }
    }
    times
}

/// `CaptureGroupExecutor.Execute`.
pub fn execute_capture_group(
    seam: &dyn PreviewSearchSeam,
    query: &str,
    repositories: &[String],
    interval: &TimeInterval,
    now: OffsetDateTime,
) -> Result<Vec<GeneratedTimeSeries>, String> {
    for repository in repositories {
        seam.repo_by_name(repository).map_err(|err| {
            format!("failed to fetch repository information for repository name: {repository}: {err}")
        })?;
    }

    let sample_times = build_sample_times(7, interval, now);
    let mut pivoted: BTreeMap<String, BTreeMap<OffsetDateTime, i64>> = BTreeMap::new();

    for repository in repositories {
        let first_commit = match seam.first_commit(repository) {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(err) => return Err(format!("FirstEverCommit: {err}")),
        };
        let plan = uncompressed_plan(sample_times.clone());

        for execution in &plan.executions {
            if execution.recording_time < first_commit {
                continue;
            }
            let commit = match seam
                .recent_commit(repository, execution.recording_time)
                .map_err(|err| format!("git.Commits: {err}"))?
            {
                Some(commit) => commit,
                None => continue,
            };

            let modified = single_repo_query(
                query,
                repository,
                &commit,
                &code_insights_query_defaults(false),
            )
            .map_err(|err| format!("query validation: {err}"))?;

            let mut grouped = seam.capture_group_search(&modified).map_err(|err| {
                let mut message =
                    format!("failed to execute capture group search for repository:{repository}");
                if !execution.revision.is_empty() {
                    message.push_str(&format!(" commit:{}", execution.revision));
                }
                format!("{message}: {err}")
            })?;
            grouped.sort_by(|a, b| a.value.cmp(&b.value));

            for element in &grouped {
                let entry = pivoted
                    .entry(element.value.clone())
                    .or_insert_with(|| generate_times(&plan));
                *entry.entry(execution.recording_time).or_insert(0) += element.count;
                for children in &execution.shared_recordings {
                    *entry.entry(*children).or_insert(0) += element.count;
                }
            }
        }
    }

    Ok(make_time_series(pivoted))
}

/// `makeTimeSeries`.
fn make_time_series(
    pivoted: BTreeMap<String, BTreeMap<OffsetDateTime, i64>>,
) -> Vec<GeneratedTimeSeries> {
    pivoted
        .into_iter()
        .enumerate()
        .map(|(index, (label, counts))| GeneratedTimeSeries {
            label,
            series_id: format!("dynamic-series-{}", index + 1),
            points: counts
                .into_iter()
                .map(|(time, count)| TimeDataPoint { time, count })
                .collect(),
        })
        .collect()
}

/// `ComputeExecutor.Execute`.
pub fn execute_compute(
    seam: &dyn PreviewSearchSeam,
    query: &str,
    group_by: &str,
    repositories: &[String],
    now: OffsetDateTime,
) -> Result<Vec<GeneratedTimeSeries>, String> {
    for repository in repositories {
        seam.repo_by_name(repository).map_err(|err| {
            format!("failed to fetch repository information for repository name: {repository}: {err}")
        })?;
    }

    let map_type = group_by.to_lowercase();
    let mut grouped_values: BTreeMap<String, i64> = BTreeMap::new();
    for repository in repositories {
        let modified = single_repo_query_indexed(query, repository);
        let final_query = compute_insight_command_query_with_map_type(&modified, &map_type)
            .map_err(|err| format!("query validation: {err}"))?;
        let mut grouped = seam
            .compute_search(&final_query)
            .map_err(|err| format!("failed to execute capture group search for repository:{repository}: {err}"))?;
        grouped.sort_by(|a, b| a.value.cmp(&b.value));
        for group in grouped {
            *grouped_values.entry(group.value).or_insert(0) += group.count;
        }
    }

    let series: Vec<GeneratedTimeSeries> = grouped_values
        .into_iter()
        .enumerate()
        .map(|(index, (label, value))| GeneratedTimeSeries {
            label,
            series_id: format!("captured-series-{}", index + 1),
            points: vec![TimeDataPoint { time: now, count: value }],
        })
        .collect();
    Ok(sort_and_limit_computed_groups(series))
}

/// `sortAndLimitComputedGroups`.
fn sort_and_limit_computed_groups(mut series: Vec<GeneratedTimeSeries>) -> Vec<GeneratedTimeSeries> {
    series.sort_by(|a, b| {
        let left = a.points.first().map(|point| point.count);
        let right = b.points.first().map(|point| point.count);
        match (left, right) {
            (Some(left), Some(right)) => right.cmp(&left),
            _ => std::cmp::Ordering::Equal,
        }
    });
    let limit = 20.min(series.len());
    series.truncate(limit);
    series
}

/// `StreamingQueryExecutor.Execute`.
pub fn execute_streaming(
    seam: &dyn PreviewSearchSeam,
    query: &str,
    series_label: &str,
    series_id: &str,
    repositories: &[String],
    interval: &TimeInterval,
    now: OffsetDateTime,
) -> Result<Vec<GeneratedTimeSeries>, String> {
    for repository in repositories {
        seam.repo_by_name(repository).map_err(|err| {
            format!("failed to fetch repository information for repository name: {repository}: {err}")
        })?;
    }

    // Go truncates the clock to the minute for this executor.
    let truncated = truncate_to_minute(now);
    let sample_times = build_sample_times(7, interval, truncated);
    let mut points: BTreeMap<OffsetDateTime, i64> = BTreeMap::new();

    for repository in repositories {
        let first_commit = match seam.first_commit(repository) {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(err) => return Err(format!("FirstEverCommit: {err}")),
        };
        let plan = uncompressed_plan(sample_times.clone());
        for execution in &plan.executions {
            if execution.recording_time < first_commit {
                continue;
            }
            let commit = match seam
                .recent_commit(repository, execution.recording_time)
                .map_err(|err| format!("git.Commits: {err}"))?
            {
                Some(commit) => commit,
                None => continue,
            };
            let modified = single_repo_query(
                query,
                repository,
                &commit,
                &code_insights_query_defaults(false),
            )
            .map_err(|err| format!("query validation: {err}"))?;
            let total = seam
                .streaming_search(&modified)
                .map_err(|err| format!("streaming.Search: {err}"))?;
            *points.entry(execution.recording_time).or_insert(0) += total;
        }
    }

    Ok(vec![GeneratedTimeSeries {
        label: series_label.to_owned(),
        series_id: series_id.to_owned(),
        points: points
            .into_iter()
            .map(|(time, count)| TimeDataPoint { time, count })
            .collect(),
    }])
}

fn truncate_to_minute(now: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(now.unix_timestamp() - now.unix_timestamp().rem_euclid(60))
        .unwrap_or(now)
        .replace_offset(now.offset())
}

// ---------------------------------------------------------------------------
// SearchInsightPreview
// ---------------------------------------------------------------------------

/// `SearchInsightLivePreview`.
pub fn search_insight_live_preview(
    seam: &dyn PreviewSearchSeam,
    input: &SearchInsightLivePreviewInput,
) -> Result<Vec<PreviewSeries>, PreviewError> {
    if !input.generated_from_capture_groups {
        return Err(PreviewError::plain(
            "live preview is currently only supported for generated series from capture groups",
        ));
    }
    let preview = SearchInsightPreviewInput {
        repository_scope: input.repository_scope.clone(),
        time_scope: input.time_scope.clone(),
        series: vec![SearchSeriesPreviewInput {
            query: input.query.clone(),
            label: input.label.clone(),
            generated_from_capture_groups: input.generated_from_capture_groups,
            group_by: input.group_by.clone(),
        }],
    };
    search_insight_preview(seam, &preview)
}

/// `SearchInsightPreview`.
pub fn search_insight_preview(
    seam: &dyn PreviewSearchSeam,
    input: &SearchInsightPreviewInput,
) -> Result<Vec<PreviewSeries>, PreviewError> {
    is_valid_preview_args(input)?;

    let preview_time = seam.now();
    let step = input
        .time_scope
        .step_interval
        .clone()
        .expect("validated step interval");
    let interval = interval_from(&step);

    let repos = get_preview_repos(seam, &input.repository_scope).map_err(PreviewError::plain)?;
    if repos.len() > MAX_PREVIEW_REPOS {
        return Err(PreviewError::coded(
            REPO_LIMIT_EXCEEDED,
            format!("live preview is limited to {MAX_PREVIEW_REPOS} repositories"),
        ));
    }

    let mut found_data = false;
    let mut resolvers: Vec<PreviewSeries> = Vec::new();

    for series_args in &input.series {
        let series = if series_args.generated_from_capture_groups {
            if let Some(group_by) = &series_args.group_by {
                execute_compute(seam, &series_args.query, group_by, &repos, preview_time)
            } else {
                execute_capture_group(seam, &series_args.query, &repos, &interval, preview_time)
            }
        } else {
            execute_streaming(
                seam,
                &series_args.query,
                &series_args.label,
                &series_args.label,
                &repos,
                &interval,
                preview_time,
            )
        }
        .map_err(PreviewError::plain)?;

        for generated in &series {
            found_data = found_data || !generated.points.is_empty();
            let mut series_query = series_args.query.clone();
            if series_args.generated_from_capture_groups && !generated.points.is_empty() {
                if let Ok(replacer) = new_pattern_replacer(&series_query, SearchType::Regex) {
                    series_query = replacer.replace(&generated.label);
                }
            }
            resolvers.push(preview_series(
                generated,
                &input.repository_scope.repositories,
                input.repository_scope.repository_criteria.as_deref(),
                &series_query,
            ));
        }
    }

    if !found_data {
        return Err(PreviewError::coded(
            NO_DATA,
            format!(
                "Data for {} not found",
                pluralize("this repository", "these repositories", repos.len())
            ),
        ));
    }

    Ok(resolvers)
}

/// `pluralize`.
fn pluralize<'a>(singular: &'a str, plural: &'a str, n: usize) -> &'a str {
    if n == 1 {
        singular
    } else {
        plural
    }
}

/// `searchInsightLivePreviewSeriesResolver` construction: label plus points.
pub fn preview_series(
    series: &GeneratedTimeSeries,
    repo_list: &[String],
    repo_search: Option<&str>,
    search_query: &str,
) -> PreviewSeries {
    PreviewSeries {
        label: series.label.clone(),
        points: preview_points(series, repo_list, repo_search, search_query),
    }
}

/// `searchInsightLivePreviewSeriesResolver.Points`.
fn preview_points(
    series: &GeneratedTimeSeries,
    repo_list: &[String],
    repo_search: Option<&str>,
    search_query: &str,
) -> Vec<PreviewPoint> {
    series
        .points
        .iter()
        .enumerate()
        .map(|(index, point)| {
            let after = if index > 0 {
                Some(series.points[index - 1].time)
            } else {
                None
            };
            let opts = PointDiffQueryOpts {
                before: Some(point.time),
                after,
                filter_repo_includes: Vec::new(),
                filter_repo_excludes: Vec::new(),
                repo_list: repo_list.to_vec(),
                repo_search: repo_search.map(str::to_owned),
                search_query: search_query.to_owned(),
            };
            // The baseline swallows diff-query build errors (`nil, nil`).
            let diff_query = point_diff_query(opts.clone())
                .ok()
                .map(|query| query.to_string());
            let point_in_time_query = point_in_time_query(opts)
                .ok()
                .map(|query| query.to_string());
            PreviewPoint {
                time: point.time,
                value: point.count as f64,
                diff_query,
                point_in_time_query,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// GraphQL input parsing / root dispatch
// ---------------------------------------------------------------------------

fn object_string(object: &std::collections::BTreeMap<String, InputValue>, name: &str) -> Option<String> {
    match object.get(name) {
        Some(InputValue::Str(value)) | Some(InputValue::Enum(value)) => Some(value.clone()),
        _ => None,
    }
}

fn object_bool(object: &std::collections::BTreeMap<String, InputValue>, name: &str) -> Option<bool> {
    match object.get(name) {
        Some(InputValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn object_int(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<i32> {
    match object.get(name) {
        Some(InputValue::Int(value)) => Some(*value as i32),
        _ => None,
    }
}

fn input_object<'a>(
    arguments: &'a Arguments,
    name: &str,
) -> Option<&'a std::collections::BTreeMap<String, InputValue>> {
    match arguments.get(name) {
        Some(InputValue::Object(value)) => Some(value),
        _ => None,
    }
}

fn parse_repository_scope(object: &std::collections::BTreeMap<String, InputValue>) -> RepositoryScopeInput {
    let repositories = match object.get("repositories") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| match item {
                InputValue::Str(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    RepositoryScopeInput {
        repositories,
        repository_criteria: object_string(object, "repositoryCriteria"),
    }
}

fn parse_time_scope(object: &std::collections::BTreeMap<String, InputValue>) -> TimeScopeInput {
    let step_interval = match object.get("stepInterval") {
        Some(InputValue::Object(step)) => {
            let unit = object_string(step, "unit").unwrap_or_else(|| "MONTH".to_owned());
            Some(StepIntervalInput {
                unit: unit.to_uppercase(),
                value: object_int(step, "value").unwrap_or(0),
            })
        }
        _ => None,
    };
    TimeScopeInput { step_interval }
}

fn parse_series(object: &std::collections::BTreeMap<String, InputValue>) -> SearchSeriesPreviewInput {
    SearchSeriesPreviewInput {
        query: object_string(object, "query").unwrap_or_default(),
        label: object_string(object, "label").unwrap_or_default(),
        generated_from_capture_groups: object_bool(object, "generatedFromCaptureGroups")
            .unwrap_or(false),
        group_by: object_string(object, "groupBy"),
    }
}

/// Parse `SearchInsightLivePreviewInput`.
pub fn parse_live_preview_input(arguments: &Arguments) -> SearchInsightLivePreviewInput {
    let input = input_object(arguments, "input").cloned().unwrap_or_default();
    let repository_scope = match input.get("repositoryScope") {
        Some(InputValue::Object(object)) => parse_repository_scope(object),
        _ => RepositoryScopeInput::default(),
    };
    let time_scope = match input.get("timeScope") {
        Some(InputValue::Object(object)) => parse_time_scope(object),
        _ => TimeScopeInput::default(),
    };
    SearchInsightLivePreviewInput {
        query: object_string(&input, "query").unwrap_or_default(),
        label: object_string(&input, "label").unwrap_or_default(),
        repository_scope,
        time_scope,
        generated_from_capture_groups: object_bool(&input, "generatedFromCaptureGroups")
            .unwrap_or(false),
        group_by: object_string(&input, "groupBy"),
    }
}

/// Parse `SearchInsightPreviewInput`.
pub fn parse_preview_input(arguments: &Arguments) -> SearchInsightPreviewInput {
    let input = input_object(arguments, "input").cloned().unwrap_or_default();
    let repository_scope = match input.get("repositoryScope") {
        Some(InputValue::Object(object)) => parse_repository_scope(object),
        _ => RepositoryScopeInput::default(),
    };
    let time_scope = match input.get("timeScope") {
        Some(InputValue::Object(object)) => parse_time_scope(object),
        _ => TimeScopeInput::default(),
    };
    let series = match input.get("series") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| match item {
                InputValue::Object(object) => Some(parse_series(object)),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    SearchInsightPreviewInput {
        repository_scope,
        time_scope,
        series,
    }
}

/// Fields implemented by this module at the `Query` root.
pub fn is_root_field(field: &str) -> bool {
    matches!(
        field,
        "searchInsightLivePreview"
            | "searchInsightPreview"
            | "validateScopedInsightQuery"
            | "previewRepositoriesFromQuery"
    )
}

/// Dispatch the preview root fields. `seam` comes from `InsightsDatabase`.
pub fn root_field(
    seam: &dyn PreviewSearchSeam,
    field: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "searchInsightLivePreview" => {
            let input = parse_live_preview_input(arguments);
            let series = search_insight_live_preview(seam, &input).map_err(|err| ExecError::new(err.message))?;
            Ok(Some(preview_series_list(series)))
        }
        "searchInsightPreview" => {
            let input = parse_preview_input(arguments);
            let series = search_insight_preview(seam, &input).map_err(|err| ExecError::new(err.message))?;
            Ok(Some(preview_series_list(series)))
        }
        "validateScopedInsightQuery" => {
            let query = arguments.string("query").unwrap_or_default();
            let payload = validate_scoped_insight_query(query);
            Ok(Some(FieldValue::Object(Arc::new(ScopedInsightQueryPayloadObject { payload }))))
        }
        "previewRepositoriesFromQuery" => {
            let query = arguments.string("query").unwrap_or_default();
            let payload = preview_repositories_from_query(seam, query).map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(RepositoryPreviewPayloadObject { payload }))))
        }
        _ => Ok(None),
    }
}

fn preview_series_list(series: Vec<PreviewSeries>) -> FieldValue {
    FieldValue::List {
        element_type: Some("SearchInsightLivePreviewSeries"),
        items: series
            .into_iter()
            .map(|series| FieldValue::Object(Arc::new(SearchInsightLivePreviewSeriesObject { series })))
            .collect(),
    }
}

struct SearchInsightLivePreviewSeriesObject {
    series: PreviewSeries,
}

impl ObjectValue for SearchInsightLivePreviewSeriesObject {
    fn type_name(&self) -> &'static str {
        "SearchInsightLivePreviewSeries"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "label" => Ok(Some(FieldValue::Json(Json::Str(self.series.label.clone())))),
            "points" => Ok(Some(FieldValue::List {
                element_type: Some("InsightDataPoint"),
                items: self
                    .series
                    .points
                    .iter()
                    .map(|point| {
                        FieldValue::Object(Arc::new(PreviewDataPointObject {
                            time: point.time,
                            value: point.value,
                            diff_query: point.diff_query.clone(),
                            point_in_time_query: point.point_in_time_query.clone(),
                        }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

struct PreviewDataPointObject {
    time: OffsetDateTime,
    value: f64,
    diff_query: Option<String>,
    point_in_time_query: Option<String>,
}

impl ObjectValue for PreviewDataPointObject {
    fn type_name(&self) -> &'static str {
        "InsightDataPoint"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let optional_query = |query: &Option<String>| match query {
            Some(query) => FieldValue::Json(Json::Str(query.clone())),
            None => FieldValue::Null,
        };
        match field {
            "dateTime" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::datetime_rfc3339(self.time),
            )))),
            "value" => Ok(Some(FieldValue::Json(Json::Float(self.value)))),
            "diffQuery" => Ok(Some(optional_query(&self.diff_query))),
            "pointInTimeQuery" => Ok(Some(optional_query(&self.point_in_time_query))),
            _ => Ok(None),
        }
    }
}

struct ScopedInsightQueryPayloadObject {
    payload: ScopedInsightQueryPayload,
}

impl ObjectValue for ScopedInsightQueryPayloadObject {
    fn type_name(&self) -> &'static str {
        "ScopedInsightQueryPayload"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "query" => Ok(Some(FieldValue::Json(Json::Str(
                self.payload.query.clone(),
            )))),
            "isValid" => Ok(Some(FieldValue::Json(Json::Bool(self.payload.is_valid)))),
            "invalidReason" => Ok(Some(match &self.payload.invalid_reason {
                Some(reason) => FieldValue::Json(Json::Str(reason.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct RepositoryPreviewPayloadObject {
    payload: RepositoryPreviewPayload,
}

impl ObjectValue for RepositoryPreviewPayloadObject {
    fn type_name(&self) -> &'static str {
        "RepositoryPreviewPayload"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "query" => Ok(Some(FieldValue::Json(Json::Str(
                self.payload.query.clone(),
            )))),
            "numberOfRepositories" => Ok(Some(match self.payload.number_of_repositories {
                Some(count) => FieldValue::Json(Json::Int(i64::from(count))),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}


/// RFC3339 UTC formatting used by the preview payload (Go `time.RFC3339`).
pub fn format_rfc3339(time: OffsetDateTime) -> String {
    crate::graphql_resolvers::datetime_rfc3339(time)
}
