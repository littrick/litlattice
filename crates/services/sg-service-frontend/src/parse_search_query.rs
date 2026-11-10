#![forbid(unsafe_code)]

//! `Query.parseSearchQuery` (`cmd/frontend/graphqlbackend/parse_search_query.go`).
//!
//! Only the `PARSE_TREE` output phase is ported: it parses the query with
//! `query.Pipeline(query.Init(...))` and prints the plan's parse tree with
//! `query.ToJSON`. `JOB_TREE` needs the search job tree
//! (`jobutil.NewPlanJob`, settings and feature flags), which is not ported
//! yet, so it reports an explicit error instead of a fabricated result.
//!
//! The `patternType` switch intentionally falls back to the *literal* search
//! type for every value other than `literal`/`structural`/`regexp`/`regex`
//! — including the schema default `standard` (`parse_search_query.go:43-53`).

use sg_search_query::{pipeline_query, plan_to_q, to_json, SearchType};

/// `ParseTree` (`parse_search_query.go:23`).
pub const PARSE_TREE: &str = "PARSE_TREE";
/// `JobTree` (`:24`).
pub const JOB_TREE: &str = "JOB_TREE";

/// `Json` (`:28`).
pub const JSON: &str = "JSON";
/// `Basic` (`:33`).
pub const BASIC: &str = "BASIC";

/// `outputParseTree`'s option error (`:63-64`).
pub const UNSUPPORTED_PARSE_TREE_OPTIONS: &str =
    "unsupported output options for PARSE_TREE, only JSON output with BASIC verbosity is supported";

/// Known deviation: the `JOB_TREE` phase is not ported (overview §2).
pub const JOB_TREE_UNPORTED: &str = "parseSearchQuery: JOB_TREE output is not ported yet";

/// The GraphQL arguments, after schema default resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseSearchQueryArgs {
    pub query: String,
    pub pattern_type: String,
    pub output_phase: String,
    pub output_format: String,
    pub output_verbosity: String,
}

impl Default for ParseSearchQueryArgs {
    /// `schema.graphql:1782-1800` defaults.
    fn default() -> Self {
        Self {
            query: String::new(),
            pattern_type: "standard".to_owned(),
            output_phase: PARSE_TREE.to_owned(),
            output_format: JSON.to_owned(),
            output_verbosity: BASIC.to_owned(),
        }
    }
}

/// `ParseSearchQuery`'s `patternType` switch (`:43-53`).
pub fn search_type(pattern_type: &str) -> SearchType {
    match pattern_type {
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "regexp" | "regex" => SearchType::Regex,
        _ => SearchType::Literal,
    }
}

/// `schemaResolver.ParseSearchQuery` (`:49-61`).
pub fn parse_search_query(args: &ParseSearchQueryArgs) -> Result<String, String> {
    let search_type = search_type(&args.pattern_type);
    match args.output_phase.as_str() {
        PARSE_TREE => output_parse_tree(search_type, args),
        JOB_TREE => Err(JOB_TREE_UNPORTED.to_owned()),
        _ => Ok(String::new()),
    }
}

/// `outputParseTree` (`:55-69`).
fn output_parse_tree(
    search_type: SearchType,
    args: &ParseSearchQueryArgs,
) -> Result<String, String> {
    let plan = pipeline_query(&args.query, search_type)?;
    if args.output_format != JSON || args.output_verbosity != BASIC {
        return Err(UNSUPPORTED_PARSE_TREE_OPTIONS.to_owned());
    }
    Ok(to_json(&plan_to_q(&plan)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(query: &str, pattern_type: &str) -> ParseSearchQueryArgs {
        ParseSearchQueryArgs {
            query: query.to_owned(),
            pattern_type: pattern_type.to_owned(),
            ..ParseSearchQueryArgs::default()
        }
    }

    #[test]
    fn defaults_match_the_schema() {
        let defaults = ParseSearchQueryArgs::default();
        assert_eq!(defaults.pattern_type, "standard");
        assert_eq!(defaults.output_phase, PARSE_TREE);
        assert_eq!(defaults.output_format, JSON);
        assert_eq!(defaults.output_verbosity, BASIC);
    }

    #[test]
    fn standard_pattern_type_is_parsed_as_literal() {
        assert_eq!(search_type("standard"), SearchType::Literal);
        assert_eq!(search_type("literal"), SearchType::Literal);
        assert_eq!(search_type("regexp"), SearchType::Regex);
        assert_eq!(search_type("regex"), SearchType::Regex);
        assert_eq!(search_type("structural"), SearchType::Structural);
        assert_eq!(search_type("keyword"), SearchType::Literal);
    }

    #[test]
    fn empty_query_is_null() {
        assert_eq!(parse_search_query(&args("", "literal")).unwrap(), "null");
    }

    #[test]
    fn parse_tree_json_matches_the_baseline_shape() {
        let json = parse_search_query(&args("repo:a b", "literal")).unwrap();
        assert_eq!(
            json,
            r#"[{"and":[{"field":"repo","value":"a","negated":false,"labels":["None"],"range":{"start":{"line":0,"column":0},"end":{"line":0,"column":6}}},{"value":"b","negated":false,"labels":["Literal"],"range":{"start":{"line":0,"column":7},"end":{"line":0,"column":8}}}]}]"#
        );
    }

    #[test]
    fn unsupported_output_options_report_the_baseline_error() {
        let mut request = args("foo", "literal");
        request.output_format = "SEXP".to_owned();
        assert_eq!(
            parse_search_query(&request).unwrap_err(),
            UNSUPPORTED_PARSE_TREE_OPTIONS
        );
        let mut request = args("foo", "literal");
        request.output_verbosity = "MAXIMAL".to_owned();
        assert_eq!(
            parse_search_query(&request).unwrap_err(),
            UNSUPPORTED_PARSE_TREE_OPTIONS
        );
    }

    #[test]
    fn unknown_phase_is_empty_and_job_tree_reports_unported() {
        let mut request = args("foo", "literal");
        request.output_phase = "OTHER".to_owned();
        assert_eq!(parse_search_query(&request).unwrap(), "");
        request.output_phase = JOB_TREE.to_owned();
        assert_eq!(parse_search_query(&request).unwrap_err(), JOB_TREE_UNPORTED);
    }

    #[test]
    fn pipeline_errors_are_returned_verbatim() {
        let error = parse_search_query(&args("(foo or bar) baz", "literal")).unwrap_err();
        assert_eq!(
            error,
            "i'm having trouble understanding that query. The combination of parentheses is the problem. Try using the content: filter to quote patterns that contain parentheses"
        );
    }
}
