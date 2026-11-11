#![forbid(unsafe_code)]

//! `internal/search/codycontext`: `NewSearchJob` and its query/term helpers.
//!
//! The Go package turns a `codycontext` query into three bounded keyword
//! searches (symbols, code, text) by lowercasing, stripping punctuation,
//! dropping stop words, Snowball-stemming the remaining terms and OR-ing
//! them. This module ports the construction side faithfully; the blocking
//! `searchJob.Run` (result collection, `result.Key` de-duplication and the
//! `fileMatcher` filter) is not ported because the result layer is not
//! available in the tree execution model (see the M9.8ju overview).
//!
//! Determinism note: the baseline `textFileFilter` is built from a Go map,
//! whose iteration order is randomized. [`text_file_filter`] uses the same
//! extension set in sorted order; the differential oracle pins the Go global
//! to the same order.

mod punctuation;
mod snowball;
mod stop_words;

use std::sync::{Arc, OnceLock};

use regex::Regex;
use sg_search_query::basic::{Basic, Parameters};
use sg_search_query::fields::{FIELD_CASE, FIELD_CONTENT, FIELD_FILE, FIELD_TYPE};
use sg_search_query::labels::Labels;
use sg_search_query::node::{new_operator, Annotation, Node, OperatorKind, Parameter};
use sg_search_query::parser::{parse, SearchType};
use sg_search_query::pipeline::for_search_type;
use sg_search_query::printer::string_human;
use sg_search_query::transformer::to_basic_query;

use crate::search_job::JobError;

/// `DefaultCodeResultsCount`.
pub const DEFAULT_CODE_RESULTS_COUNT: i32 = 12;
/// `DefaultTextResultsCount`.
pub const DEFAULT_TEXT_RESULTS_COUNT: i32 = 3;
/// `SymbolResultsCount` (only used by the unported `Run`).
pub const SYMBOL_RESULTS_COUNT: i32 = 3;
/// `maxKeywords`.
pub const MAX_KEYWORDS: usize = 10;

/// The fixed error shared by `NewSearchJob` and `parseQuery`.
pub const MULTI_CLAUSE_ERROR: &str =
    "The 'codycontext' patterntype does not support multiple clauses";

/// `embed.TextFileExtensions` keys, sorted so the filter is deterministic.
pub const TEXT_FILE_EXTENSIONS: &[&str] = &[
    "adoc",
    "asciidoc",
    "creole",
    "markdown",
    "md",
    "mediawiki",
    "muse",
    "org",
    "pod",
    "pod6",
    "rdoc",
    "rest",
    "rmd",
    "rst",
    "textile",
    "txt",
    "wiki",
    "wikitext",
];

/// `textFileFilter`: `(ext1|ext2|...)$`.
pub fn text_file_filter() -> &'static str {
    static FILTER: OnceLock<String> = OnceLock::new();
    FILTER.get_or_init(|| format!("({})$", TEXT_FILE_EXTENSIONS.join("|")))
}

/// `codyFileMatcher` (`func(api.RepoID, string) bool`).
pub type FileMatcherFn = dyn Fn(i32, &str) -> bool + Send + Sync;

/// The baseline's `search.Features.CodyFileMatcher` seam.
#[derive(Clone)]
pub enum CodyFileMatcher {
    /// `inputs.Features == nil || inputs.Features.CodyFileMatcher == nil`.
    AcceptAll,
    /// `inputs.Features.CodyFileMatcher`.
    Custom(Arc<FileMatcherFn>),
}

impl std::fmt::Debug for CodyFileMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodyFileMatcher::AcceptAll => f.write_str("CodyFileMatcher::AcceptAll"),
            CodyFileMatcher::Custom(_) => f.write_str("CodyFileMatcher::Custom"),
        }
    }
}

impl PartialEq for CodyFileMatcher {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CodyFileMatcher::AcceptAll, CodyFileMatcher::AcceptAll) => true,
            (CodyFileMatcher::Custom(left), CodyFileMatcher::Custom(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }
}

impl CodyFileMatcher {
    pub fn accepts(&self, repo_id: i32, path: &str) -> bool {
        match self {
            CodyFileMatcher::AcceptAll => true,
            CodyFileMatcher::Custom(matcher) => matcher(repo_id, path),
        }
    }
}

/// The `search.Features` fields `codycontext` reads.
#[derive(Clone, Default)]
pub struct CodyContextOptions {
    pub code_count: i32,
    pub text_count: i32,
    pub file_matcher: Option<Arc<FileMatcherFn>>,
}

impl std::fmt::Debug for CodyContextOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodyContextOptions")
            .field("code_count", &self.code_count)
            .field("text_count", &self.text_count)
            .field("file_matcher", &self.file_matcher.is_some())
            .finish()
    }
}

/// What `NewSearchJob` produces: either the shared no-op or the three
/// sub-queries plus the attributes the job advertises.
#[allow(clippy::large_enum_variant)] // Mirrors the baseline's two return shapes; `Jobs` is the common path.
#[derive(Debug, Clone, PartialEq)]
pub enum CodyContextJobParts {
    /// `len(patterns) == 0` → `newNoopJob()`.
    Noop,
    Jobs {
        symbol_query: Option<Basic>,
        code_query: Basic,
        text_query: Basic,
        patterns: Vec<String>,
        code_count: i32,
        text_count: i32,
        file_matcher: CodyFileMatcher,
    },
}

/// `getResultLimits`.
pub fn get_result_limits(options: Option<&CodyContextOptions>) -> (i32, i32) {
    let mut code_count = DEFAULT_CODE_RESULTS_COUNT;
    let mut text_count = DEFAULT_TEXT_RESULTS_COUNT;
    if let Some(options) = options {
        if options.code_count > 0 {
            code_count = options.code_count;
        }
        if options.text_count > 0 {
            text_count = options.text_count;
        }
    }
    (code_count, text_count)
}

/// `getFileMatcher`.
pub fn get_file_matcher(options: Option<&CodyContextOptions>) -> CodyFileMatcher {
    match options.and_then(|options| options.file_matcher.clone()) {
        Some(matcher) => CodyFileMatcher::Custom(matcher),
        None => CodyFileMatcher::AcceptAll,
    }
}

/// `NewSearchJob` (`job.go:34-89`), minus the `newJob` callback: the caller
/// maps [`CodyContextJobParts`] to real job nodes in the baseline order.
pub fn new_search_job(
    plan: &[Basic],
    options: Option<&CodyContextOptions>,
) -> Result<CodyContextJobParts, JobError> {
    if plan.len() > 1 {
        return Err(JobError(MULTI_CLAUSE_ERROR.to_owned()));
    }

    let (code_count, text_count) = get_result_limits(options);
    let file_matcher = get_file_matcher(options);

    // The baseline indexes `plan[0]` and would panic on an empty plan; the
    // pipeline never produces one for an accepted query.
    let basic_query = &plan[0];
    let q = parse_query(&string_human(&basic_query.to_parse_tree()))?;
    let patterns = q.patterns;

    if patterns.is_empty() {
        return Ok(CodyContextJobParts::Noop);
    }

    let params = q.keyword_query.parameters.0.clone();
    let filter = text_file_filter().to_owned();

    let symbol_query = if q.symbols.is_empty() {
        None
    } else {
        let mut parameters = params.clone();
        parameters.push(parameter(FIELD_TYPE, "symbol", false));
        parameters.push(parameter(FIELD_FILE, &filter, true));
        Some(q.symbol_query.map_parameters(Parameters::new(parameters)))
    };

    let mut code_parameters = params.clone();
    code_parameters.push(parameter(FIELD_TYPE, "path", false));
    code_parameters.push(parameter(FIELD_TYPE, "file", false));
    code_parameters.push(parameter(FIELD_FILE, &filter, true));
    let code_query = q
        .keyword_query
        .map_parameters(Parameters::new(code_parameters));

    let mut text_parameters = params;
    text_parameters.push(parameter(FIELD_TYPE, "path", false));
    text_parameters.push(parameter(FIELD_TYPE, "file", false));
    text_parameters.push(parameter(FIELD_FILE, &filter, false));
    let text_query = q
        .keyword_query
        .map_parameters(Parameters::new(text_parameters));

    Ok(CodyContextJobParts::Jobs {
        symbol_query,
        code_query,
        text_query,
        patterns,
        code_count,
        text_count,
        file_matcher,
    })
}

fn parameter(field: &str, value: &str, negated: bool) -> Parameter {
    Parameter {
        field: field.to_owned(),
        value: value.to_owned(),
        negated,
        annotation: Annotation::default(),
    }
}

/// `contextQuery`.
#[derive(Debug, Clone)]
pub struct ContextQuery {
    pub symbol_query: Basic,
    pub symbols: Vec<String>,
    pub keyword_query: Basic,
    /// `contextQuery.patterns`: the truncated keyword list.
    pub patterns: Vec<String>,
}

/// `parseQuery` (`query_parser.go:21-52`).
pub fn parse_query(query_string: &str) -> Result<ContextQuery, JobError> {
    let raw_parse_tree =
        parse(query_string, SearchType::Standard).map_err(|error| JobError(error.to_string()))?;

    if raw_parse_tree.len() != 1 {
        return Err(JobError(MULTI_CLAUSE_ERROR.to_owned()));
    }

    let (patterns, parameters) = node_to_patterns_and_parameters(&raw_parse_tree[0]);

    let symbols = find_symbols(&patterns);
    let mut keywords = expand_query(query_string);
    keywords.extend(transform_patterns(&patterns));

    if keywords.len() > MAX_KEYWORDS {
        keywords.truncate(MAX_KEYWORDS);
    }

    let symbol_query = new_basic_query(&parameters, &symbols)?;
    let keyword_query = new_basic_query(&parameters, &keywords)?;

    Ok(ContextQuery {
        symbol_query,
        symbols,
        keyword_query,
        patterns: keywords,
    })
}

/// `newBasicQuery` (`query_parser.go:54-81`).
pub fn new_basic_query(parameters: &[Parameter], patterns: &[String]) -> Result<Basic, JobError> {
    let mut nodes: Vec<Node> = parameters.iter().cloned().map(Node::Parameter).collect();

    let pattern_nodes: Vec<Node> = patterns
        .iter()
        .map(|pattern| {
            let mut annotation = Annotation::default();
            annotation.labels.set(Labels::LITERAL);
            Node::Pattern(sg_search_query::node::Pattern {
                value: pattern.clone(),
                negated: false,
                annotation,
            })
        })
        .collect();

    if !pattern_nodes.is_empty() {
        nodes.extend(new_operator(pattern_nodes, OperatorKind::Or));
    }

    let new_nodes = for_search_type(SearchType::Standard)(&nodes).map_err(JobError)?;
    to_basic_query(&new_nodes).map_err(JobError)
}

/// `concatNodeToPatterns`.
fn concat_node_to_patterns(operator: &sg_search_query::node::Operator) -> Vec<String> {
    operator
        .operands
        .iter()
        .filter_map(|operand| match operand {
            Node::Pattern(pattern) => Some(pattern.value.clone()),
            _ => None,
        })
        .collect()
}

/// `nodeToPatternsAndParameters` (`query_parser.go:94-129`).
pub fn node_to_patterns_and_parameters(root: &Node) -> (Vec<String>, Vec<Parameter>) {
    let Node::Operator(operator) = root else {
        return (Vec::new(), Vec::new());
    };

    let mut patterns = Vec::new();
    let mut parameters = Vec::new();

    match operator.kind {
        OperatorKind::And => {
            for operand in &operator.operands {
                match operand {
                    Node::Operator(op) if op.kind == OperatorKind::Concat => {
                        patterns.extend(concat_node_to_patterns(op));
                    }
                    Node::Parameter(op) => {
                        if op.field == FIELD_CONTENT {
                            // Split any content field on white space into a
                            // set of patterns.
                            patterns.extend(op.value.split_whitespace().map(str::to_owned));
                        } else if op.field != FIELD_CASE && op.field != FIELD_TYPE {
                            parameters.push(op.clone());
                        }
                    }
                    Node::Pattern(op) => patterns.push(op.value.clone()),
                    Node::Operator(_) => {}
                }
            }
        }
        OperatorKind::Concat => patterns = concat_node_to_patterns(operator),
        OperatorKind::Or => {}
    }

    (patterns, parameters)
}

/// `transformPatterns` (`query_parser.go:131-171`).
pub fn transform_patterns(patterns: &[String]) -> Vec<String> {
    let mut transformed = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for pattern in patterns {
        let pattern = pattern.to_lowercase();
        let pattern = remove_punctuation(&pattern);

        for term in tokenize(&pattern) {
            if term.len() < 3 || is_common_term(&term) {
                continue;
            }
            let term = stem_term(&term);
            if seen.insert(term.clone()) {
                transformed.push(term);
            }
        }
    }

    transformed
}

fn namespace_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"::|->").expect("namespace regex"))
}

fn separator_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[:.]|->").expect("separator regex"))
}

/// `findSymbols` (`query_parser.go:173-192`).
pub fn find_symbols(patterns: &[String]) -> Vec<String> {
    let mut symbols = Vec::new();
    for pattern in patterns {
        let split: Vec<&str> = namespace_regex().split(pattern).collect();
        if split.len() > 1 {
            symbols.extend(split.into_iter().map(str::to_owned));
        } else if pattern.contains('.') {
            for part in pattern.split('.') {
                if is_likely_symbol(part) {
                    symbols.push(part.to_owned());
                }
            }
        } else if is_likely_symbol(pattern) {
            symbols.push(pattern.clone());
        }
    }
    symbols
}

/// `isLikelySymbol`: contains `_` or matches `[a-z][A-Z]`.
pub fn is_likely_symbol(pattern: &str) -> bool {
    if pattern.contains('_') {
        return true;
    }
    let bytes = pattern.as_bytes();
    bytes
        .windows(2)
        .any(|pair| pair[0].is_ascii_lowercase() && pair[1].is_ascii_uppercase())
}

/// `questionSignifiers`.
const QUESTION_SIGNIFIERS: &[&str] = &["what", "how", "describe", "explain"];
/// `projectSignifiers`.
const PROJECT_SIGNIFIERS: &[&str] = &[
    "code",
    "codebase",
    "library",
    "module",
    "package",
    "program",
    "project",
    "repo",
    "repository",
];

/// `needsReadmeContext`.
pub fn needs_readme_context(input: &str) -> bool {
    let lowered = input.to_lowercase();
    if !QUESTION_SIGNIFIERS
        .iter()
        .any(|signifier| lowered.contains(signifier))
    {
        return false;
    }
    PROJECT_SIGNIFIERS
        .iter()
        .any(|signifier| lowered.contains(signifier))
}

/// `kwReadme`.
pub const KW_README: &str = "readme";

/// `expandQuery`.
pub fn expand_query(query_string: &str) -> Vec<String> {
    if needs_readme_context(query_string) {
        vec![KW_README.to_owned()]
    } else {
        Vec::new()
    }
}

/// `removePunctuation` (`strings.TrimFunc(input, unicode.IsPunct)`).
pub fn remove_punctuation(input: &str) -> String {
    input
        .trim_matches(|c: char| punctuation::is_go_punct(c))
        .to_owned()
}

/// `tokenize` (`separatorRegex.Split(input, -1)`).
pub fn tokenize(input: &str) -> Vec<String> {
    separator_regex().split(input).map(str::to_owned).collect()
}

/// `stemTerm`: keep the stem only when it is a prefix of the input.
pub fn stem_term(input: &str) -> String {
    let stemmed = snowball::stem(input, false);
    if !input.starts_with(&stemmed) {
        return input.to_owned();
    }
    stemmed
}

/// `isCommonTerm`.
pub fn is_common_term(input: &str) -> bool {
    stop_words::is_common_code_search_term(input) || stop_words::is_stop_word(input)
}
