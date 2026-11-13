//! M9.32a differential of the Code Insights live-preview / aggregation surface
//! against the Go baseline (`cmd/frontend/internal/insights/resolvers`).
//!
//! Consumes `data/insights_preview/corpus.json`, drives the ported production
//! functions through an injected [`PreviewSearchSeam`] fake, and serializes the
//! preview payload (labels, points, diff/point-in-time queries) or the error
//! message/code. The Go oracle is `zz_m932a_preview_oracle_test.go`
//! (untracked); both are compared by `run_insights_preview_diff.sh`.

#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::Deserialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_insights::timeseries::TimeInterval;
use sg_insights::types::IntervalUnit;
use sg_insights_query::new_pattern_replacer;
use sg_search_query::SearchType;
use sg_service_frontend::insights_aggregations::{
    mode_availability, search_aggregations, AggregationMatchResult, AggregationResult,
    AggregationSearchOutcome, AggregationSearchSeam, SearchAggregationMode,
    SearchAggregationsInput,
};
use sg_service_frontend::insights_preview::{
    execute_capture_group, execute_compute, format_rfc3339, preview_repositories_from_query,
    preview_series, search_insight_live_preview, search_insight_preview,
    validate_scoped_insight_query, GeneratedTimeSeries, GroupedMatch, PreviewRepo,
    PreviewSearchSeam, RepositoryScopeInput, SearchInsightLivePreviewInput,
    SearchInsightPreviewInput, SearchSeriesPreviewInput, StepIntervalInput, TimeScopeInput,
};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct Step {
    #[serde(default)]
    unit: String,
    #[serde(default)]
    value: i32,
}

#[derive(Deserialize, Default)]
struct TimeScopeFixture {
    #[serde(default, rename = "stepInterval")]
    step_interval: Option<Step>,
}

#[derive(Deserialize, Default)]
struct ScopeFixture {
    #[serde(default)]
    repositories: Vec<String>,
    #[serde(default, rename = "repositoryCriteria")]
    repository_criteria: Option<String>,
}

#[derive(Deserialize, Default)]
struct SeriesFixture {
    #[serde(default)]
    query: String,
    #[serde(default)]
    label: String,
    #[serde(default, rename = "generatedFromCaptureGroups")]
    generated_from_capture_groups: bool,
    #[serde(default, rename = "groupBy")]
    group_by: Option<String>,
}

#[derive(Deserialize, Default)]
struct GroupFixture {
    #[serde(default)]
    value: String,
    #[serde(default)]
    count: i64,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    label: String,
    #[serde(default, rename = "generatedFromCaptureGroups")]
    generated_from_capture_groups: bool,
    #[serde(default, rename = "groupBy")]
    group_by: Option<String>,
    #[serde(default, rename = "repositoryScope")]
    repository_scope: Option<ScopeFixture>,
    #[serde(default, rename = "timeScope")]
    time_scope: Option<TimeScopeFixture>,
    #[serde(default)]
    series: Vec<SeriesFixture>,
    #[serde(default, rename = "repoSearchResult")]
    repo_search_result: Vec<String>,
    #[serde(default, rename = "repoSearchError")]
    repo_search_error: Option<String>,
    #[serde(default)]
    repositories: Vec<String>,
    #[serde(default)]
    clock: Option<String>,
    #[serde(default, rename = "firstCommit")]
    first_commit: Option<String>,
    #[serde(default, rename = "emptyRepos")]
    empty_repos: Vec<String>,
    #[serde(default, rename = "recentCommit")]
    recent_commit: Option<String>,
    #[serde(default, rename = "searchGroups")]
    search_groups: Vec<GroupFixture>,
    #[serde(default, rename = "searchError")]
    search_error: Option<String>,
    #[serde(default, rename = "maskDateTime")]
    mask_date_time: bool,

    // Aggregation (`searchQueryAggregate`).
    #[serde(default, rename = "patternType")]
    pattern_type: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default = "default_agg_limit")]
    limit: i32,
    #[serde(default, rename = "extendedTimeout")]
    extended_timeout: bool,
    #[serde(default, rename = "bufferSize")]
    buffer_size: i32,
    #[serde(default, rename = "proactiveResultLimit")]
    proactive_result_limit: i64,
    #[serde(default = "default_extended_seconds", rename = "extendedTimeoutSeconds")]
    extended_timeout_seconds: i32,
    #[serde(default, rename = "aggregationMatches")]
    aggregation_matches: Vec<GroupFixture>,
    #[serde(default, rename = "shardTimeout")]
    shard_timeout: bool,
    #[serde(default, rename = "resultCount")]
    result_count: i32,
    #[serde(default, rename = "timedOut")]
    timed_out: bool,
}

fn default_agg_limit() -> i32 {
    50
}

fn default_extended_seconds() -> i32 {
    55
}

struct FakeAggregationSeam {
    matches: Vec<AggregationMatchResult>,
    shard_timeout: bool,
    result_count: i32,
    timed_out: bool,
    error: Option<String>,
    buffer_size: i32,
    proactive_result_limit: i64,
    extended_timeout_seconds: i32,
}

impl std::fmt::Debug for FakeAggregationSeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeAggregationSeam")
    }
}

impl FakeAggregationSeam {
    fn from_case(case: &Case) -> Self {
        Self {
            matches: case
                .aggregation_matches
                .iter()
                .map(|group| AggregationMatchResult {
                    group: group.value.clone(),
                    count: group.count as i32,
                })
                .collect(),
            shard_timeout: case.shard_timeout,
            result_count: case.result_count,
            timed_out: case.timed_out,
            error: case.search_error.clone(),
            buffer_size: case.buffer_size,
            proactive_result_limit: case.proactive_result_limit,
            extended_timeout_seconds: case.extended_timeout_seconds,
        }
    }
}

impl AggregationSearchSeam for FakeAggregationSeam {
    fn search(
        &self,
        _modified_query: &str,
        _mode: SearchAggregationMode,
    ) -> AggregationSearchOutcome {
        AggregationSearchOutcome {
            matches: self.matches.clone(),
            shard_timeout: self.shard_timeout,
            result_count: self.result_count,
            timed_out: self.timed_out,
            error: self.error.clone(),
        }
    }

    fn buffer_size(&self) -> i32 {
        self.buffer_size
    }

    fn proactive_result_limit(&self) -> i64 {
        self.proactive_result_limit
    }

    fn extended_timeout_seconds(&self) -> i32 {
        self.extended_timeout_seconds
    }
}

struct FakeSeam {
    now: OffsetDateTime,
    repo_list: Vec<String>,
    repo_error: Option<String>,
    groups: Vec<GroupedMatch>,
    search_error: Option<String>,
    empty_repos: Vec<String>,
    first_commit: Option<OffsetDateTime>,
    recent_commit: Option<String>,
    queries: Mutex<Vec<String>>,
}

impl std::fmt::Debug for FakeSeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeSeam")
    }
}

impl FakeSeam {
    fn from_case(case: &Case) -> Self {
        let parse = |value: &Option<String>| {
            value
                .as_deref()
                .and_then(|raw| OffsetDateTime::parse(raw, &Rfc3339).ok())
        };
        Self {
            now: parse(&case.clock).unwrap_or(OffsetDateTime::UNIX_EPOCH),
            repo_list: case.repo_search_result.clone(),
            repo_error: case.repo_search_error.clone(),
            groups: case
                .search_groups
                .iter()
                .map(|group| GroupedMatch {
                    value: group.value.clone(),
                    count: group.count,
                })
                .collect(),
            search_error: case.search_error.clone(),
            empty_repos: case.empty_repos.clone(),
            first_commit: parse(&case.first_commit),
            recent_commit: case.recent_commit.clone(),
            queries: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, query: &str) {
        self.queries.lock().unwrap().push(query.to_owned());
    }

    fn take_queries(&self) -> Vec<String> {
        std::mem::take(&mut *self.queries.lock().unwrap())
    }
}

impl PreviewSearchSeam for FakeSeam {
    fn now(&self) -> OffsetDateTime {
        self.now
    }

    fn repo_search(&self, _query: &str) -> Result<Vec<PreviewRepo>, String> {
        if let Some(err) = &self.repo_error {
            return Err(format!("streaming repo search: errors: [{err}]"));
        }
        Ok(self
            .repo_list
            .iter()
            .map(|name| PreviewRepo { name: name.clone() })
            .collect())
    }

    fn repo_by_name(&self, _name: &str) -> Result<i32, String> {
        Ok(1)
    }

    fn first_commit(&self, repo: &str) -> Result<Option<OffsetDateTime>, String> {
        if self.empty_repos.iter().any(|empty| empty == repo) {
            return Ok(None);
        }
        Ok(self.first_commit)
    }

    fn recent_commit(&self, _repo: &str, _before: OffsetDateTime) -> Result<Option<String>, String> {
        Ok(self.recent_commit.clone())
    }

    fn capture_group_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String> {
        self.record(query);
        if let Some(err) = &self.search_error {
            return Err(format!("compute streaming search: errors: [{err}]"));
        }
        Ok(self.groups.clone())
    }

    fn compute_search(&self, query: &str) -> Result<Vec<GroupedMatch>, String> {
        self.record(query);
        if let Some(err) = &self.search_error {
            return Err(format!("compute streaming search: errors: [{err}]"));
        }
        Ok(self.groups.clone())
    }

    fn streaming_search(&self, _query: &str) -> Result<i64, String> {
        Ok(0)
    }
}

fn scope(case: &Case) -> RepositoryScopeInput {
    match &case.repository_scope {
        Some(scope) => RepositoryScopeInput {
            repositories: scope.repositories.clone(),
            repository_criteria: scope.repository_criteria.clone(),
        },
        None => RepositoryScopeInput::default(),
    }
}

fn time_scope(case: &Case) -> TimeScopeInput {
    let step = case
        .time_scope
        .as_ref()
        .and_then(|scope| scope.step_interval.as_ref())
        .map(|step| StepIntervalInput {
            unit: step.unit.clone(),
            value: step.value,
        });
    TimeScopeInput { step_interval: step }
}

fn interval(case: &Case) -> TimeInterval {
    let step = time_scope(case).step_interval.unwrap_or(StepIntervalInput {
        unit: "MONTH".to_owned(),
        value: 1,
    });
    TimeInterval {
        unit: IntervalUnit::from_str_value(&step.unit),
        value: i64::from(step.value),
    }
}

fn point_doc(point: &sg_service_frontend::insights_preview::PreviewPoint, mask: bool) -> Value {
    let formatted = format_rfc3339(point.time);
    let (date_time, diff_query, point_in_time_query) = if mask {
        let mask_query = |query: &Option<String>| match query {
            Some(query) => Some(query.replace(&formatted, "<masked>")),
            None => None,
        };
        (
            "<masked>".to_owned(),
            mask_query(&point.diff_query),
            mask_query(&point.point_in_time_query),
        )
    } else {
        (
            formatted,
            point.diff_query.clone(),
            point.point_in_time_query.clone(),
        )
    };
    json!({
        "dateTime": date_time,
        "value": point.value,
        "diffQuery": diff_query,
        "pointInTimeQuery": point_in_time_query,
    })
}

fn series_doc(
    series: &sg_service_frontend::insights_preview::PreviewSeries,
    mask: bool,
) -> Value {
    json!({
        "label": series.label,
        "points": series.points.iter().map(|point| point_doc(point, mask)).collect::<Vec<_>>(),
    })
}

fn sorted_series_docs(
    series: Vec<sg_service_frontend::insights_preview::PreviewSeries>,
    mask: bool,
) -> Vec<Value> {
    let mut docs: Vec<Value> = series.iter().map(|item| series_doc(item, mask)).collect();
    docs.sort_by(|left, right| {
        left["label"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["label"].as_str().unwrap_or_default())
    });
    docs
}

fn run_series(case: &Case, seam: &FakeSeam) -> Value {
    let repositories = case.repositories.clone();
    let interval = interval(case);
    let now = seam.now();
    let generated: Result<Vec<GeneratedTimeSeries>, String> = if let Some(group_by) = &case.group_by {
        execute_compute(seam, &case.query, group_by, &repositories, now)
    } else {
        execute_capture_group(seam, &case.query, &repositories, &interval, now)
    };
    let generated = match generated {
        Ok(generated) => generated,
        Err(err) => return json!({"error": err, "code": Value::Null}),
    };
    let queries = seam.take_queries();

    let series = generated
        .iter()
        .map(|item| {
            let mut series_query = case.query.clone();
            if case.generated_from_capture_groups && !item.points.is_empty() {
                if let Ok(replacer) = new_pattern_replacer(&series_query, SearchType::Regex) {
                    series_query = replacer.replace(&item.label);
                }
            }
            preview_series(item, &repositories, None, &series_query)
        })
        .collect();
    json!({
        "series": sorted_series_docs(series, case.mask_date_time),
        "queries": queries,
    })
}

fn run_case(case: &Case) -> Value {
    let seam = FakeSeam::from_case(case);
    match case.kind.as_str() {
        "validate" => {
            let payload = validate_scoped_insight_query(&case.query);
            json!({
                "query": payload.query,
                "isValid": payload.is_valid,
                "invalidReason": payload.invalid_reason,
            })
        }
        "repoPreview" => match preview_repositories_from_query(&seam, &case.query) {
            Ok(payload) => json!({
                "query": payload.query,
                "numberOfRepositories": payload.number_of_repositories,
            }),
            Err(err) => json!({"error": err}),
        },
        "live" => {
            let input = SearchInsightLivePreviewInput {
                query: case.query.clone(),
                label: case.label.clone(),
                repository_scope: scope(case),
                time_scope: time_scope(case),
                generated_from_capture_groups: case.generated_from_capture_groups,
                group_by: case.group_by.clone(),
            };
            match search_insight_live_preview(&seam, &input) {
                Ok(series) => json!({"series": sorted_series_docs(series, case.mask_date_time)}),
                Err(err) => json!({"error": err.message, "code": err.code}),
            }
        }
        "preview" => {
            let input = SearchInsightPreviewInput {
                repository_scope: scope(case),
                time_scope: time_scope(case),
                series: case
                    .series
                    .iter()
                    .map(|item| SearchSeriesPreviewInput {
                        query: item.query.clone(),
                        label: item.label.clone(),
                        generated_from_capture_groups: item.generated_from_capture_groups,
                        group_by: item.group_by.clone(),
                    })
                    .collect(),
            };
            match search_insight_preview(&seam, &input) {
                Ok(series) => json!({"series": sorted_series_docs(series, case.mask_date_time)}),
                Err(err) => json!({"error": err.message, "code": err.code}),
            }
        }
        "series" => run_series(case, &seam),
        "modeAvailability" => {
            let modes: Vec<Value> = mode_availability(&case.query, &case.pattern_type)
                .into_iter()
                .map(|item| {
                    json!({
                        "mode": item.mode.as_str(),
                        "available": item.available,
                        "reasonUnavailable": item.reason_unavailable,
                    })
                })
                .collect();
            json!({"modes": modes})
        }
        "aggregation" => {
            let seam = FakeAggregationSeam::from_case(case);
            let input = SearchAggregationsInput {
                query: case.query.clone(),
                pattern_type: case.pattern_type.clone(),
                mode: case
                    .mode
                    .as_deref()
                    .and_then(SearchAggregationMode::from_raw),
                limit: case.limit,
                extended_timeout: case.extended_timeout,
            };
            match search_aggregations(&seam, &input) {
                Err(err) => json!({"error": err}),
                Ok(AggregationResult::NotAvailable {
                    reason,
                    reason_type,
                    mode,
                }) => json!({
                    "notAvailable": {
                        "reason": reason,
                        "reasonType": reason_type.as_str(),
                        "mode": mode.as_str(),
                    }
                }),
                Ok(result) => {
                    let (results, mode, exhaustive) = match result {
                        AggregationResult::Exhaustive { results, mode } => (results, mode, true),
                        AggregationResult::NonExhaustive { results, mode } => {
                            (results, mode, false)
                        }
                        AggregationResult::NotAvailable { .. } => unreachable!(),
                    };
                    let groups: Vec<Value> = results
                        .groups
                        .iter()
                        .map(|group| {
                            json!({
                                "label": group.label,
                                "count": group.count,
                                "query": group.query,
                            })
                        })
                        .collect();
                    let mut doc = json!({
                        "mode": mode.as_str(),
                        "exhaustive": exhaustive,
                        "groups": groups,
                        "otherResultCount": results.other_result_count,
                        "totalCount": results.total_count,
                    });
                    if exhaustive {
                        doc["otherGroupCount"] = json!(results.other_group_count);
                    } else {
                        doc["approximateOtherGroupCount"] = json!(results.other_group_count);
                    }
                    doc
                }
            }
        }
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn insights_preview_diff() {
    if std::env::var("SG_M932A_PREVIEW_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M932A_CORPUS").expect("M932A_CORPUS");
    let out_dir = PathBuf::from(std::env::var("M932A_OUT").expect("M932A_OUT"));
    fs::create_dir_all(&out_dir).unwrap();

    let corpus: Corpus =
        serde_json::from_str(&fs::read_to_string(&corpus_path).unwrap()).unwrap();
    for case in &corpus.cases {
        let document = run_case(case);
        let encoded = serde_json::to_string_pretty(&document).unwrap();
        fs::write(out_dir.join(format!("{}.json", case.name)), encoded).unwrap();
    }
}
