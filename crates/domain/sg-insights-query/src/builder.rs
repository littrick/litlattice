#![forbid(unsafe_code)]

//! `internal/insights/query/querybuilder/builder.go`: build Sourcegraph search
//! queries for Code Insights by parsing and reconstructing a query plan.

use time::{OffsetDateTime, UtcOffset};

use sg_search_query::basic::{map_plan, Plan};
use sg_search_query::fields::{
    FIELD_AFTER, FIELD_ARCHIVED, FIELD_AUTHOR, FIELD_BEFORE, FIELD_CASE, FIELD_CONTENT,
    FIELD_COUNT, FIELD_FILE, FIELD_FORK, FIELD_PATTERN_TYPE, FIELD_REPO, FIELD_REV, FIELD_TIMEOUT,
    FIELD_TYPE,
};
use sg_search_query::node::{regexp_quote_meta, Parameter};
use sg_search_query::{
    add_regexp_field, pipeline_query, plan_to_q, string_human, Parameters, SearchType,
};

use crate::parser::parse_query;
use crate::repo::{exactly_one_repo, parameter, repositories};

/// `BasicQuery`.
pub type BasicQuery = String;
/// `ComputeInsightQuery`.
pub type ComputeInsightQuery = String;

/// `types.NO_REPO_METADATA_TEXT`.
pub const NO_REPO_METADATA_TEXT: &str = "No metadata";

/// `QueryNotSupported`.
pub const QUERY_NOT_SUPPORTED: &str = "query not supported";

/// `MapType`: the compute grouping key for a command insight series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapType {
    Lang,
    Repo,
    Path,
    Author,
    Date,
}

impl MapType {
    pub fn as_str(self) -> &'static str {
        match self {
            MapType::Lang => "lang",
            MapType::Repo => "repo",
            MapType::Path => "path",
            MapType::Author => "author",
            MapType::Date => "date",
        }
    }
}

/// The compute command that corresponds to the execution for Code Insights.
const INSIGHTS_COMPUTE_COMMAND: &str = "output.extra";

/// `ComputeInsightCommandQuery`: convert a search query into a compute
/// `output.extra(...)` query that groups matches by `map_type`.
pub fn compute_insight_command_query(
    query: &str,
    map_type: MapType,
) -> Result<ComputeInsightQuery, String> {
    let parsed = crate::compute::parse(query).map_err(|err| format!("compute.Parse: {err}"))?;
    let pattern = parsed.command.to_search_pattern();
    Ok(add_regexp_field(
        &parsed.parameters,
        FIELD_CONTENT,
        &format!(
            "{INSIGHTS_COMPUTE_COMMAND}({pattern} -> ${})",
            map_type.as_str()
        ),
    ))
}

/// `withDefaults`: insert default parameters that the query does not specify,
/// preserving user-supplied values. `count:all` becomes `count:99999999` via
/// the pipeline's normalization.
pub fn with_defaults(input_query: &str, defaults: &Parameters) -> Result<BasicQuery, String> {
    if defaults.0.is_empty() {
        return Ok(input_query.to_owned());
    }

    let plan = pipeline_query(input_query, SearchType::Literal)
        .map_err(|err| format!("Pipeline: {err}"))?;

    let modified: Plan = plan
        .into_iter()
        .map(|basic| {
            let mut parameters: Vec<Parameter> =
                Vec::with_capacity(basic.parameters.0.len() + defaults.0.len());
            for default_param in &defaults.0 {
                if !basic.parameters.exists(&default_param.field) {
                    parameters.push(default_param.clone());
                }
            }
            parameters.extend(basic.parameters.0.iter().cloned());
            basic.map_parameters(Parameters::new(parameters))
        })
        .collect();

    Ok(string_human(&plan_to_q(&modified)))
}

/// `AggregationQuery`: upsert `count:<count>` and `timeout:<n>s`.
pub fn aggregation_query(
    input_query: &str,
    timeout_seconds: i32,
    count: &str,
) -> Result<BasicQuery, String> {
    let upsert_params = Parameters::new(vec![
        parameter(FIELD_COUNT, count, false),
        parameter(FIELD_TIMEOUT, format!("{timeout_seconds}s"), false),
    ]);

    let plan = pipeline_query(input_query, SearchType::Literal)
        .map_err(|err| format!("Pipeline: {err}"))?;

    let modified: Plan = plan
        .into_iter()
        .map(|basic| {
            let mut parameters: Vec<Parameter> =
                Vec::with_capacity(basic.parameters.0.len() + upsert_params.0.len());
            for param in &basic.parameters.0 {
                if upsert_params.exists(&param.field) {
                    continue;
                }
                parameters.push(param.clone());
            }
            parameters.extend(upsert_params.0.iter().cloned());
            basic.map_parameters(Parameters::new(parameters))
        })
        .collect();

    Ok(string_human(&plan_to_q(&modified)))
}

/// `CodeInsightsQueryDefaults`.
pub fn code_insights_query_defaults(all_repos_insight: bool) -> Parameters {
    let fork_archive_value = if all_repos_insight { "no" } else { "yes" };
    Parameters::new(vec![
        parameter(FIELD_FORK, fork_archive_value, false),
        parameter(FIELD_ARCHIVED, fork_archive_value, false),
        parameter(FIELD_PATTERN_TYPE, "literal", false),
    ])
}

/// `withCountAll`: append `count:all` when the query has no `count:` filter.
pub fn with_count_all(query: &str) -> BasicQuery {
    if query.contains("count:") {
        return query.to_owned();
    }
    format!("{query} count:all")
}

/// `forRepoRevision`.
pub fn for_repo_revision(query: &str, repo: &str, revision: &str) -> BasicQuery {
    format!("{query} repo:^{}$@{}", regexp_quote_meta(repo), revision)
}

/// `forRepos`: one `repo:^(a|b)$` filter over quoted repo names.
pub fn for_repos(query: &str, repos: &[String]) -> BasicQuery {
    let escaped_repos: Vec<String> = repos.iter().map(|repo| regexp_quote_meta(repo)).collect();
    format!("{query} repo:^({})$", escaped_repos.join("|"))
}

/// `PointDiffQueryOpts`.
#[derive(Debug, Clone, Default)]
pub struct PointDiffQueryOpts {
    pub before: Option<OffsetDateTime>,
    pub after: Option<OffsetDateTime>,
    /// Repos included from a selected context.
    pub filter_repo_includes: Vec<String>,
    /// Repos excluded from a selected context.
    pub filter_repo_excludes: Vec<String>,
    pub repo_list: Vec<String>,
    pub repo_search: Option<String>,
    pub search_query: BasicQuery,
}

/// Build the repo/date filters shared by `PointDiffQuery` and
/// `PointInTimeQuery`, up to and including the `before` filter.
fn point_filters(diff_info: &PointDiffQueryOpts) -> Vec<Parameter> {
    let mut new_filters = Vec::new();

    if !diff_info.filter_repo_includes.is_empty() {
        new_filters.push(parameter(
            FIELD_REPO,
            diff_info.filter_repo_includes.join("|"),
            false,
        ));
    }
    if !diff_info.filter_repo_excludes.is_empty() {
        new_filters.push(parameter(
            FIELD_REPO,
            diff_info.filter_repo_excludes.join("|"),
            true,
        ));
    }
    if !diff_info.repo_list.is_empty() {
        let escaped_repos: Vec<String> = diff_info
            .repo_list
            .iter()
            .map(|repo| regexp_quote_meta(repo))
            .collect();
        new_filters.push(parameter(
            FIELD_REPO,
            format!("^({})$", escaped_repos.join("|")),
            false,
        ));
    }
    new_filters
}

/// Prepend `filters` to each basic query's parameters and rebuild the plan.
fn prepend_filters(plan: Plan, filters: &[Parameter]) -> Plan {
    plan.into_iter()
        .map(|step| {
            let mut parameters = filters.to_vec();
            parameters.extend(step.parameters.0.iter().cloned());
            step.map_parameters(Parameters::new(parameters))
        })
        .collect()
}

/// `PointDiffQuery`.
pub fn point_diff_query(diff_info: PointDiffQueryOpts) -> Result<BasicQuery, String> {
    let mut new_filters = point_filters(&diff_info);

    if let Some(after) = diff_info.after {
        new_filters.push(parameter(FIELD_AFTER, rfc3339_utc(after), false));
    }
    let Some(before) = diff_info.before else {
        // The baseline dereferences `Before` unconditionally; callers always
        // set it.
        return Err("before is required".to_owned());
    };
    new_filters.push(parameter(FIELD_BEFORE, rfc3339_utc(before), false));
    new_filters.push(parameter(FIELD_TYPE, "diff", false));

    let query_plan = parse_query(&diff_info.search_query, "literal")?;
    let modified_plan = prepend_filters(query_plan, &new_filters);
    let mut query = string_human(&plan_to_q(&modified_plan));

    // Treat a repo search as its own query to preserve compound groupings.
    if let Some(repo_search) = &diff_info.repo_search {
        query = make_query_with_repo_filters(repo_search, &query, false, &[])?;
    }
    Ok(query)
}

/// `PointInTimeQuery`.
pub fn point_in_time_query(diff_info: PointDiffQueryOpts) -> Result<BasicQuery, String> {
    let mut new_filters = point_filters(&diff_info);

    let Some(before) = diff_info.before else {
        return Err("before is required".to_owned());
    };
    new_filters.push(parameter(
        FIELD_REV,
        format!("at.time({})", rfc3339_utc(before)),
        false,
    ));

    let query_plan = parse_query(&diff_info.search_query, "literal")?;
    let modified_plan = prepend_filters(query_plan, &new_filters);
    let mut query = string_human(&plan_to_q(&modified_plan));

    if let Some(repo_search) = &diff_info.repo_search {
        query = make_query_with_repo_filters(repo_search, &query, false, &[])?;
    }
    Ok(query)
}

/// `SingleRepoQuery`.
pub fn single_repo_query(
    query: &str,
    repo: &str,
    revision: &str,
    default_params: &Parameters,
) -> Result<BasicQuery, String> {
    let modified = with_count_all(query);
    let modified =
        with_defaults(&modified, default_params).map_err(|err| format!("WithDefaults: {err}"))?;
    Ok(for_repo_revision(&modified, repo, revision))
}

/// `SingleRepoQueryIndexed`.
pub fn single_repo_query_indexed(query: &str, repo: &str) -> BasicQuery {
    let modified = with_count_all(query);
    for_repos(&modified, &[repo.to_owned()])
}

/// `GlobalQuery`.
pub fn global_query(query: &str, default_params: &Parameters) -> Result<BasicQuery, String> {
    let modified = with_count_all(query);
    with_defaults(&modified, default_params).map_err(|err| format!("WithDefaults: {err}"))
}

/// `MultiRepoQuery`.
pub fn multi_repo_query(
    query: &str,
    repos: &[String],
    default_params: &Parameters,
) -> Result<BasicQuery, String> {
    let modified = with_count_all(query);
    let modified =
        with_defaults(&modified, default_params).map_err(|err| format!("WithDefaults: {err}"))?;
    Ok(for_repos(&modified, repos))
}

/// `BasicQuery.WithCount`.
pub fn with_count(query: &str, count: &str) -> Result<BasicQuery, String> {
    let upsert_params = Parameters::new(vec![parameter(FIELD_COUNT, count, false)]);

    let plan =
        pipeline_query(query, SearchType::Literal).map_err(|err| format!("Pipeline: {err}"))?;

    let modified: Plan = plan
        .into_iter()
        .map(|basic| {
            let mut parameters: Vec<Parameter> =
                Vec::with_capacity(basic.parameters.0.len() + upsert_params.0.len());
            for param in &basic.parameters.0 {
                if upsert_params.exists(&param.field) {
                    continue;
                }
                parameters.push(param.clone());
            }
            parameters.extend(upsert_params.0.iter().cloned());
            basic.map_parameters(Parameters::new(parameters))
        })
        .collect();

    Ok(string_human(&plan_to_q(&modified)))
}

/// `IsSingleRepoQuery`.
pub fn is_single_repo_query(query: &str) -> Result<bool, String> {
    // The search type is irrelevant for this check.
    let plan_steps = pipeline_query(query, SearchType::Literal)?;
    if plan_steps.len() > 1 {
        return Err(QUERY_NOT_SUPPORTED.to_owned());
    }
    for step in &plan_steps {
        let (repo_filters, _negated) = repositories(&step.parameters);
        if !exactly_one_repo(&repo_filters) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `AddAuthorFilter`: only commit/diff searches accept an author filter.
pub fn add_author_filter(query: &str, author: &str) -> Result<BasicQuery, String> {
    let plan = pipeline_query(query, SearchType::Literal)?;

    let mutated_query = map_plan(plan, &mut |basic| {
        let mut modified: Vec<Parameter> = Vec::with_capacity(basic.parameters.0.len() + 1);
        let mut is_commit_diff_type = false;
        for parameter in &basic.parameters.0 {
            modified.push(parameter.clone());
            if parameter.field == FIELD_TYPE
                && (parameter.value == "commit" || parameter.value == "diff")
            {
                is_commit_diff_type = true;
            }
        }
        if !is_commit_diff_type {
            // Cannot add an author filter without committing to a result type.
            return basic;
        }
        modified.push(parameter(FIELD_AUTHOR, build_filter_text(author), false));
        basic.map_parameters(Parameters::new(modified))
    });

    Ok(string_human(&plan_to_q(&mutated_query)))
}

/// `AddRepoFilter`.
pub fn add_repo_filter(query: &str, repo: &str) -> Result<BasicQuery, String> {
    add_filter_simple(query, FIELD_REPO, repo)
}

/// `AddFileFilter`.
pub fn add_file_filter(query: &str, file: &str) -> Result<BasicQuery, String> {
    add_filter_simple(query, FIELD_FILE, file)
}

/// `AddRepoMetadataFilter`.
pub fn add_repo_metadata_filter(query: &str, repo_meta: &str) -> Result<BasicQuery, String> {
    if repo_meta == NO_REPO_METADATA_TEXT {
        return Err("Can't search for no metadata key".to_owned());
    }
    let plan = pipeline_query(query, SearchType::Literal)?;

    let mutated_query = map_plan(plan, &mut |basic| {
        let mut modified: Vec<Parameter> = Vec::with_capacity(basic.parameters.0.len() + 1);
        modified.extend(basic.parameters.0.iter().cloned());
        // The baseline splits on ':' and rebuilds `has.meta(key:value)`, which
        // is byte-identical to `has.meta(repoMeta)` for exactly one colon.
        modified.push(parameter(
            FIELD_REPO,
            format!("has.meta({repo_meta})"),
            false,
        ));
        basic.map_parameters(Parameters::new(modified))
    });

    Ok(string_human(&plan_to_q(&mutated_query)))
}

/// `buildFilterText`.
pub fn build_filter_text(raw: &str) -> String {
    let quoted = regexp_quote_meta(raw);
    if raw.contains(' ') {
        format!("(^{quoted}$)")
    } else {
        format!("^{quoted}$")
    }
}

/// `AddFilter`.
pub fn add_filter(
    query: &str,
    field: &str,
    value: &str,
    negated: bool,
) -> Result<BasicQuery, String> {
    let plan = pipeline_query(query, SearchType::Literal)?;

    let mutated_query = map_plan(plan, &mut |basic| {
        let mut modified: Vec<Parameter> = Vec::with_capacity(basic.parameters.0.len() + 1);
        modified.extend(basic.parameters.0.iter().cloned());
        modified.push(parameter(field, build_filter_text(value), negated));
        basic.map_parameters(Parameters::new(modified))
    });
    Ok(string_human(&plan_to_q(&mutated_query)))
}

/// `addFilterSimple`.
fn add_filter_simple(query: &str, field: &str, value: &str) -> Result<BasicQuery, String> {
    add_filter(query, field, value, false)
}

/// `SetCaseSensitivity`.
pub fn set_case_sensitivity(query: &str, sensitive: bool) -> Result<BasicQuery, String> {
    let plan = pipeline_query(query, SearchType::Literal)?;

    let mutated_query = map_plan(plan, &mut |basic| {
        let mut params: Vec<Parameter> = Vec::with_capacity(basic.parameters.0.len());
        for parameter in &basic.parameters.0 {
            if parameter.field == FIELD_CASE {
                continue;
            }
            params.push(parameter.clone());
        }
        let value = if sensitive { "yes" } else { "no" };
        params.push(parameter(FIELD_CASE, value, false));
        basic.map_parameters(Parameters::new(params))
    });
    Ok(string_human(&plan_to_q(&mutated_query)))
}

/// `RepositoryScopeQuery`: prepend `fork:yes archived:yes count:all`,
/// overwriting any user-supplied values for those fields.
pub fn repository_scope_query(query: &str) -> Result<BasicQuery, String> {
    let repository_scope_parameters = Parameters::new(vec![
        parameter(FIELD_FORK, "yes", false),
        parameter(FIELD_ARCHIVED, "yes", false),
        parameter(FIELD_COUNT, "all", false),
    ]);

    let plan =
        pipeline_query(query, SearchType::Literal).map_err(|err| format!("Pipeline: {err}"))?;

    let modified: Plan = plan
        .into_iter()
        .map(|basic| {
            let mut p = repository_scope_parameters.0.clone();
            for param in &basic.parameters.0 {
                if !repository_scope_parameters.exists(&param.field) {
                    p.push(param.clone());
                }
            }
            basic.map_parameters(Parameters::new(p))
        })
        .collect();
    Ok(string_human(&plan_to_q(&modified)))
}

/// `MakeQueryWithRepoFilters`.
pub fn make_query_with_repo_filters(
    repository_criteria: &str,
    query: &str,
    count_all: bool,
    defaults: &[Parameter],
) -> Result<BasicQuery, String> {
    let query = if count_all {
        with_count_all(query)
    } else {
        query.to_owned()
    };
    let modified_query = with_defaults(&query, &Parameters::new(defaults.to_vec()))
        .map_err(|err| format!("error parsing search query: {err}"))?;
    let repository_plan = parse_query(repository_criteria, "literal")
        .map_err(|err| format!("error parsing repository filters: {err}"))?;
    Ok(format!(
        "{} {}",
        string_human(&plan_to_q(&repository_plan)),
        modified_query
    ))
}

/// `time.Time.UTC().Format(time.RFC3339)` for a UTC instant. The Go layout has
/// no fractional-second component, so subseconds are truncated.
fn rfc3339_utc(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults(spec: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            spec.iter()
                .map(|(field, value)| parameter(field, *value, false))
                .collect(),
        )
    }

    #[test]
    fn with_defaults_inserts_missing_fields() {
        assert_eq!(
            with_defaults("repo:myrepo testquery", &defaults(&[("archived", "yes")])).unwrap(),
            "archived:yes repo:myrepo testquery"
        );
        assert_eq!(
            with_defaults(
                "repo:myrepo testquery fork:no",
                &defaults(&[("archived", "yes")])
            )
            .unwrap(),
            "archived:yes repo:myrepo fork:no testquery"
        );
    }

    #[test]
    fn with_defaults_keeps_user_values() {
        assert_eq!(
            with_defaults(
                "patterntype:standard repo:myrepo testquery",
                &defaults(&[("patterntype", "literal")])
            )
            .unwrap(),
            "patterntype:standard repo:myrepo testquery"
        );
    }

    #[test]
    fn multi_repo_query_quotes_repos() {
        assert_eq!(
            multi_repo_query(
                "testquery",
                &["github.com/myrepos/repo1".to_owned(), "github.com/myrepos/repo2".to_owned()],
                &defaults(&[("archived", "no"), ("fork", "no")])
            )
            .unwrap(),
            "archived:no fork:no count:99999999 testquery repo:^(github\\.com/myrepos/repo1|github\\.com/myrepos/repo2)$"
        );
    }

    #[test]
    fn count_all_is_converted_only_when_the_plan_is_rebuilt() {
        // With no defaults `withDefaults` returns the input untouched, so the
        // literal `count:all` survives.
        assert_eq!(
            global_query("testquery", &Parameters::default()).unwrap(),
            "testquery count:all"
        );
        // A default forces the parse/rebuild, which normalizes count:all.
        assert_eq!(
            global_query("testquery", &defaults(&[("fork", "no")])).unwrap(),
            "fork:no count:99999999 testquery"
        );
        assert!(is_single_repo_query("repo:^github\\.com/a/b$ test").unwrap());
        assert!(!is_single_repo_query("repo:^(a|b)$ test").unwrap());
        assert_eq!(
            is_single_repo_query("(repo:^a$ x) OR (repo:^b$ y)").unwrap_err(),
            QUERY_NOT_SUPPORTED
        );
    }

    #[test]
    fn aggregation_upserts_count_and_timeout() {
        assert_eq!(
            aggregation_query("repo:x test count:10 timeout:5s", 30, "100").unwrap(),
            "repo:x count:100 timeout:30s test"
        );
        // Aggregation does not rebuild the plan after inserting, so a
        // `count:all` value is emitted verbatim.
        assert_eq!(
            aggregation_query("test", 2, "all").unwrap(),
            "count:all timeout:2s test"
        );
    }

    #[test]
    fn repository_scope_prepends_defaults() {
        assert_eq!(
            repository_scope_query("repo:repo fork:no").unwrap(),
            "fork:yes archived:yes count:all repo:repo"
        );
    }

    #[test]
    fn point_in_time_uses_at_time() {
        let before = OffsetDateTime::from_unix_timestamp(1_577_836_800).unwrap(); // 2020-01-01T00:00:00Z
        let query = point_in_time_query(PointDiffQueryOpts {
            before: Some(before),
            search_query: "testquery".to_owned(),
            ..PointDiffQueryOpts::default()
        })
        .unwrap();
        assert_eq!(query, "rev:at.time(2020-01-01T00:00:00Z) testquery");
    }

    #[test]
    fn add_filter_quotes_value() {
        assert_eq!(
            add_repo_filter("test", "github.com/a/b").unwrap(),
            "repo:^github\\.com/a/b$ test"
        );
        assert_eq!(
            add_author_filter("type:commit test", "Jane Doe").unwrap(),
            "type:commit author:(^Jane Doe$) test"
        );
        assert_eq!(add_author_filter("test", "Jane").unwrap(), "test");
        assert_eq!(
            add_repo_metadata_filter("test", "k:v").unwrap(),
            "repo:has.meta(k:v) test"
        );
        assert_eq!(
            add_repo_metadata_filter("test", NO_REPO_METADATA_TEXT).unwrap_err(),
            "Can't search for no metadata key"
        );
    }

    #[test]
    fn make_query_with_repo_filters_concatenates() {
        // No defaults: `withDefaults` returns the query untouched, so the
        // `count:all` appended by `withCountAll` is not normalized.
        assert_eq!(
            make_query_with_repo_filters("repo:a or repo:b", "test", true, &[]).unwrap(),
            "(repo:a OR repo:b) test count:all"
        );
        assert_eq!(
            make_query_with_repo_filters(
                "repo:a or repo:b",
                "test",
                true,
                &[parameter(FIELD_FORK, "no", false)]
            )
            .unwrap(),
            "(repo:a OR repo:b) fork:no count:99999999 test"
        );
    }
}
