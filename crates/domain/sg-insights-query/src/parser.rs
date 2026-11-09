#![forbid(unsafe_code)]

//! `internal/insights/query/querybuilder/parser.go`: search-type detection and
//! query-plan parsing as Code Insights performs it.

use sg_search_query::fields::{FIELD_PATTERN_TYPE, FIELD_REPO};
use sg_search_query::node::{go_quote, visit_field};
use sg_search_query::transformer::lowercase_field_names;
use sg_search_query::{parse, pipeline_query, Parameters, Plan, SearchType};

use crate::repo::parse_repository_revisions;

const CONTAINS_PATTERN: &str =
    "the query cannot be used for scoping because it contains a pattern: `%s`.";
const CONTAINS_DISALLOWED_FILTER: &str =
    "the query cannot be used for scoping because it contains a disallowed filter: `%s`.";
const CONTAINS_DISALLOWED_REVISION: &str =
    "the query cannot be used for scoping because it contains a revision.";
const CONTAINS_INVALID_EXPRESSION: &str =
    "the query cannot be used for scoping because it is not a valid regular expression.";

/// `client.SearchTypeFromString`.
pub fn search_type_from_string(pattern_type: &str) -> Result<SearchType, String> {
    match pattern_type {
        "standard" => Ok(SearchType::Standard),
        "literal" => Ok(SearchType::Literal),
        "regexp" => Ok(SearchType::Regex),
        "structural" => Ok(SearchType::Structural),
        "codycontext" => Ok(SearchType::CodyContext),
        "keyword" => Ok(SearchType::Keyword),
        // The "lucky" pattern type is deprecated and remapped to standard.
        "lucky" => Ok(SearchType::Standard),
        other => Err(format!("unrecognized patternType {}", go_quote(other))),
    }
}

/// `SearchType.String`.
pub fn search_type_name(search_type: SearchType) -> &'static str {
    match search_type {
        SearchType::Standard => "standard",
        SearchType::Regex => "regex",
        SearchType::Literal => "literal",
        SearchType::Structural => "structural",
        SearchType::CodyContext => "codycontext",
        SearchType::Keyword => "keyword",
    }
}

/// `DetectSearchType`: the `patternType` parameter selects the initial type, a
/// non-empty `patterntype:` filter overrides it.
pub fn detect_search_type(raw_query: &str, pattern_type: &str) -> Result<SearchType, String> {
    let search_type = search_type_from_string(pattern_type)
        .map_err(|err| format!("client.SearchTypeFromString: {err}"))?;

    let nodes = parse(raw_query, search_type).map_err(|err| format!("query.Parse: {err}"))?;
    let nodes = lowercase_field_names(&nodes);

    // `VisitField` keeps visiting; the last non-empty `patterntype:` value
    // decides (including its error).
    let mut override_result: Option<Result<SearchType, String>> = None;
    visit_field(
        &nodes,
        FIELD_PATTERN_TYPE,
        &mut |value, _negated, _annotation| {
            if !value.is_empty() {
                override_result = Some(search_type_from_string(value));
            }
        },
    );

    match override_result {
        Some(Ok(search_type)) => Ok(search_type),
        Some(Err(err)) => Err(err),
        None => Ok(search_type),
    }
}

/// `ParseQuery`.
pub fn parse_query(query: &str, pattern_type: &str) -> Result<Plan, String> {
    let search_type = detect_search_type(query, pattern_type)
        .map_err(|err| format!("overrideSearchType: {err}"))?;
    pipeline_query(query, search_type).map_err(|err| format!("query.Pipeline: {err}"))
}

/// `ParametersFromQueryPlan`: concatenate every basic query's parameters.
pub fn parameters_from_query_plan(plan: &Plan) -> Parameters {
    let mut parameters = Vec::new();
    for basic in plan {
        parameters.extend(basic.parameters.0.iter().cloned());
    }
    Parameters::new(parameters)
}

/// `ContainsField`.
pub fn contains_field(raw_query: &str, field: &str) -> Result<bool, String> {
    let plan = parse_query(raw_query, "literal").map_err(|err| format!("ParseQuery: {err}"))?;
    for basic in &plan {
        if basic.parameters.exists(field) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `IsValidScopeQuery`: only `repo:` filters (including repo predicates) and
/// boolean operators are allowed, and repository revisions are disallowed.
pub fn is_valid_scope_query(plan: &Plan) -> (String, bool) {
    for basic in plan {
        if basic.pattern.is_some() {
            return (
                CONTAINS_PATTERN.replace("%s", &basic.pattern_string()),
                false,
            );
        }
        for parameter in &basic.parameters.0 {
            let field = parameter.field.to_lowercase();
            // The only allowed filter is repo (including repo:has predicates).
            if field != FIELD_REPO {
                return (
                    CONTAINS_DISALLOWED_FILTER.replace("%s", &parameter.field),
                    false,
                );
            }
            match parse_repository_revisions(&parameter.value) {
                Err(_) => return (CONTAINS_INVALID_EXPRESSION.to_owned(), false),
                Ok(filter) => {
                    if !filter.revs.is_empty() {
                        return (CONTAINS_DISALLOWED_REVISION.to_owned(), false);
                    }
                }
            }
        }
    }
    (String::new(), true)
}

/// Convenience for callers that only have the query string.
pub fn parse_scope_query(query: &str) -> Result<(String, bool), String> {
    let plan = parse_query(query, "literal")?;
    Ok(is_valid_scope_query(&plan))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_search_type_follows_patterntype_filter() {
        assert_eq!(
            detect_search_type("test patterntype:regexp", "literal").unwrap(),
            SearchType::Regex
        );
        assert_eq!(
            detect_search_type("[a] patterntype:structural", "regexp").unwrap(),
            SearchType::Structural
        );
    }

    #[test]
    fn unknown_pattern_type_is_an_error() {
        assert_eq!(
            detect_search_type("a", "nope").unwrap_err(),
            "client.SearchTypeFromString: unrecognized patternType \"nope\""
        );
    }

    #[test]
    fn scope_queries() {
        assert_eq!(
            parse_scope_query("repo:sourcegraph or repo:about").unwrap(),
            (String::new(), true)
        );
        assert_eq!(
            parse_scope_query("repo:sourcegraph pattern").unwrap(),
            (
                "the query cannot be used for scoping because it contains a pattern: `pattern`."
                    .to_owned(),
                false
            )
        );
        assert_eq!(
            parse_scope_query("repo:sourcegraph rev:mybranch").unwrap(),
            (
                "the query cannot be used for scoping because it contains a revision.".to_owned(),
                false
            )
        );
        assert_eq!(
            parse_scope_query("file:x repo:handbook").unwrap(),
            (
                "the query cannot be used for scoping because it contains a disallowed filter: `file`."
                    .to_owned(),
                false
            )
        );
    }
}
