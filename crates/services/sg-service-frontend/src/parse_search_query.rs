#![forbid(unsafe_code)]

//! `Query.parseSearchQuery` (`cmd/frontend/graphqlbackend/parse_search_query.go`).
//!
//! Both output phases are ported:
//!
//! * `PARSE_TREE` parses the query with `query.Pipeline(query.Init(...))` and
//!   prints the plan's parse tree with `query.ToJSON`;
//! * `JOB_TREE` builds the job tree with `search_jobutil_tree::new_plan_job`
//!   and prints it with `search_job_printer` as JSON, SEXP or Mermaid at
//!   MINIMAL/BASIC/MAXIMAL verbosity.
//!
//! The `patternType` switch intentionally falls back to the *literal* search
//! type for every value other than `literal`/`structural`/`regexp`/`regex`
//! — including the schema default `standard` (`parse_search_query.go:43-53`).

use crate::search_client::{Features, Protocol};
use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_job_printer::{json_verbose, mermaid_verbose, sexp_verbose};
use crate::search_jobutil_job_builders as jb;
use crate::search_jobutil_tree::{new_plan_job, JobNode, TreeInputs};
use crate::search_stream::progress::SearchEvent;
use sg_search_query::{pipeline_query, plan_to_q, to_json, SearchType};

/// `ParseTree` (`parse_search_query.go:23`).
pub const PARSE_TREE: &str = "PARSE_TREE";
/// `JobTree` (`:24`).
pub const JOB_TREE: &str = "JOB_TREE";

/// `Json` (`:28`).
pub const JSON: &str = "JSON";
/// `Sexp` (`:30`).
pub const SEXP: &str = "SEXP";
/// `Mermaid` (`:32`).
pub const MERMAID: &str = "MERMAID";

/// `Minimal` (`:34`).
pub const MINIMAL: &str = "MINIMAL";
/// `Basic` (`:37`).
pub const BASIC: &str = "BASIC";
/// `Maximal` (`:39`).
pub const MAXIMAL: &str = "MAXIMAL";

/// `outputParseTree`'s option error (`:63-64`).
pub const UNSUPPORTED_PARSE_TREE_OPTIONS: &str =
    "unsupported output options for PARSE_TREE, only JSON output with BASIC verbosity is supported";

/// Injected dependencies of [`parse_search_query_with_deps`]'s `JOB_TREE`
/// phase. The baseline reads user settings and feature flags from the request
/// context; here they are plain closures. The defaults match the baseline for
/// an anonymous request with no feature flags: empty settings, all flags off
/// and `dotcom.SourcegraphDotComMode()` false.
pub struct JobTreeDeps {
    /// `settings.CurrentUserFinal(ctx, db)`.
    pub user_settings: Box<dyn Fn() -> Result<jb::UserSettings, String>>,
    /// `client.ToFeatures(featureflag.FromContext(ctx), logger)`.
    pub features: Box<dyn Fn() -> Features>,
    /// `dotcom.SourcegraphDotComMode()`.
    pub on_sourcegraph_dot_com: bool,
}

impl Default for JobTreeDeps {
    fn default() -> Self {
        Self {
            user_settings: Box::new(|| Ok(jb::UserSettings::default())),
            features: Box::new(Features::default),
            on_sourcegraph_dot_com: false,
        }
    }
}

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

/// `schemaResolver.ParseSearchQuery` (`:49-61`) with the default (empty)
/// `JOB_TREE` dependencies.
pub fn parse_search_query(args: &ParseSearchQueryArgs) -> Result<String, String> {
    parse_search_query_with_deps(args, &JobTreeDeps::default())
}

/// `schemaResolver.ParseSearchQuery` (`:49-61`).
pub fn parse_search_query_with_deps(
    args: &ParseSearchQueryArgs,
    deps: &JobTreeDeps,
) -> Result<String, String> {
    let search_type = search_type(&args.pattern_type);
    match args.output_phase.as_str() {
        PARSE_TREE => output_parse_tree(search_type, args),
        JOB_TREE => output_job_tree(search_type, args, deps),
        _ => Ok(String::new()),
    }
}

/// `outputParseTree` (`:71-85`).
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

/// A read-only view of the [`JobNode`] tree for printing.
///
/// `search_jobutil_tree::new_alert_job` renders its `query` attribute from the
/// plan (`inputs.Query = plan.ToQ()` in the M9.8ho harness), but the real
/// `outputJobTree` never populates `search.Inputs.Query`, so the baseline
/// prints an empty `AlertJob.query`. This adapter matches the resolver by
/// clearing that one attribute; the tree itself is untouched.
struct JobTreeView {
    name: &'static str,
    attributes: Vec<(String, String)>,
    children: Vec<JobTreeView>,
}

impl JobTreeView {
    fn new(node: &JobNode) -> Self {
        let (name, mut attributes, children) = node.describe();
        if name == "AlertJob" {
            for (key, value) in &mut attributes {
                if key == "query" {
                    value.clear();
                }
            }
        }
        Self {
            name,
            attributes,
            children: children.into_iter().map(Self::new).collect(),
        }
    }
}

impl SearchJob for JobTreeView {
    fn run(
        &mut self,
        _ctx: &JobContext,
        _clients: &RuntimeClients<'_>,
        _stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        Err(JobError(
            "job tree printing does not execute jobs (M9.8ht)".to_owned(),
        ))
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        self.children
            .iter()
            .map(|child| child as &dyn SearchJob)
            .collect()
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        match verbosity {
            Verbosity::None => Vec::new(),
            Verbosity::Basic | Verbosity::Max => self.attributes.clone(),
        }
    }
}

/// `outputJobTree` (`:87-138`).
fn output_job_tree(
    search_type: SearchType,
    args: &ParseSearchQueryArgs,
    deps: &JobTreeDeps,
) -> Result<String, String> {
    let plan = pipeline_query(&args.query, search_type)?;
    let user_settings = (deps.user_settings)()?;
    let inputs = TreeInputs {
        protocol: Protocol::Streaming,
        pattern_type: search_type,
        user_settings,
        features: (deps.features)(),
        context_lines: 0,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: deps.on_sourcegraph_dot_com,
        // `outputJobTree` does not set `Inputs.OriginalQuery` or
        // `Inputs.ContextLines`, so they keep their zero values.
        original_query: String::new(),
        sub_repo_enabled: false,
        cody_context_hook: None,
    };
    let job = new_plan_job(&inputs, &plan).map_err(|error| error.0)?;
    let view = JobTreeView::new(&job);

    let verbosity = verbosity(&args.output_verbosity);
    let output = match args.output_format.as_str() {
        JSON => json_verbose(&view, verbosity),
        SEXP => sexp_verbose(&view, verbosity, true),
        MERMAID => mermaid_verbose(&view, verbosity),
        _ => return Ok(String::new()),
    };
    Ok(output)
}

/// `outputJobTree`'s verbosity switch (`:116-124`). An unrecognized value
/// keeps the `job.Verbosity` zero value (`VerbosityNone`), like the baseline.
fn verbosity(output_verbosity: &str) -> Verbosity {
    match output_verbosity {
        MINIMAL => Verbosity::None,
        BASIC => Verbosity::Basic,
        MAXIMAL => Verbosity::Max,
        _ => Verbosity::None,
    }
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

    fn job_tree_args(query: &str, format: &str, verbosity: &str) -> ParseSearchQueryArgs {
        ParseSearchQueryArgs {
            query: query.to_owned(),
            pattern_type: "literal".to_owned(),
            output_phase: JOB_TREE.to_owned(),
            output_format: format.to_owned(),
            output_verbosity: verbosity.to_owned(),
        }
    }

    #[test]
    fn unknown_phase_is_empty() {
        let mut request = args("foo", "literal");
        request.output_phase = "OTHER".to_owned();
        assert_eq!(parse_search_query(&request).unwrap(), "");
    }

    #[test]
    fn job_tree_empty_plan_prints_the_log_noop_tree() {
        assert_eq!(
            parse_search_query(&job_tree_args("", JSON, MINIMAL)).unwrap(),
            "{\n  \"LogJob\": {\n    \"NoopJob\": {}\n  }\n}"
        );
        assert_eq!(
            parse_search_query(&job_tree_args("", SEXP, BASIC)).unwrap(),
            "(LOG\n  NOOP)"
        );
        assert_eq!(
            parse_search_query(&job_tree_args("", MERMAID, MAXIMAL)).unwrap(),
            "flowchart TB\n0[\"LOG\"]\n  0---1\n  1[\"NOOP\"]\n    "
        );
    }

    #[test]
    fn job_tree_unknown_format_is_empty_and_unknown_verbosity_is_minimal() {
        assert_eq!(
            parse_search_query(&job_tree_args("foo", "YAML", BASIC)).unwrap(),
            ""
        );
        assert_eq!(
            parse_search_query(&job_tree_args("foo", JSON, "VERBOSE")).unwrap(),
            parse_search_query(&job_tree_args("foo", JSON, MINIMAL)).unwrap()
        );
    }

    #[test]
    fn job_tree_errors_are_returned_verbatim() {
        assert_eq!(
            parse_search_query(&job_tree_args("(foo or bar) baz", JSON, BASIC)).unwrap_err(),
            "i'm having trouble understanding that query. The combination of parentheses is the problem. Try using the content: filter to quote patterns that contain parentheses"
        );
        assert_eq!(
            parse_search_query(&job_tree_args("type:symbol not foo", JSON, BASIC)).unwrap_err(),
            "symbol search does not support negation."
        );
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
