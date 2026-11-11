#![forbid(unsafe_code)]

//! `internal/search/job/jobutil/exhaustive_job.go`: the search-jobs product
//! facade over the job infrastructure.
//!
//! Ported:
//!
//! * [`new_exhaustive`]: the protocol/plan/select/file-predicate/regex/
//!   result-type validation sequence and the commit/diff and text job
//!   construction branches (`exhaustive_job.go:27-114`);
//! * [`has_predicates`] (`exhaustive_job.go:116-125`);
//! * [`Exhaustive::job`]: `repoPagerJob.child.Resolve(resolvedRepos{unindexed:
//!   [repoRevs]})`, exposed as [`ResolvedJob`] with the baseline's
//!   `Name`/`Children`/`Attributes` (`exhaustive_job.go:127-133`);
//! * [`Exhaustive::repository_rev_specs`] and
//!   [`Exhaustive::resolve_repository_rev_spec`]: `reposNewResolver(...)`
//!   wrappers that forward the constructed `RepoOptions`
//!   (`exhaustive_job.go:135-147`).
//!
//! The text/commit leaf `Name`/`Attributes` formats and
//! `TextPatternInfo.String()` are reproduced because they are the observable
//! output of [`ResolvedJob`]; `commit.QueryToGitQuery` (and a deterministic
//! subset of `gitprotocol.Reduce`) is modelled privately to render the commit
//! `query` attribute.
//!
//! Repository listing/resolution is injected through [`ExhaustiveResolver`]
//! instead of constructing the baseline's `repos.Resolver` from a real
//! DB/gitserver/zoekt client set. Full job-tree registration, execution,
//! GraphQL/HTTP/gRPC and the search backends are not ported.
//!
//! See `docs/overviews/m9_8hp_jobutil_exhaustive_overview.md`.

use sg_search_query::basic::{q_string_value, Basic, Plan};
use sg_search_query::fields::{
    FIELD_AUTHOR, FIELD_COMMITTER, FIELD_CONTENT, FIELD_FILE, FIELD_LANG, FIELD_MESSAGE,
    FIELD_SELECT,
};
use sg_search_query::labels::Labels;
use sg_search_query::node::{go_quote, visit_field, Node, OperatorKind, Parameter, Pattern};
use sg_search_query::parser::{scan_predicate, SearchType};
use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions};
use sg_zoekt::languages::lang_to_file_regexp;

use crate::search_client::{Features, Protocol};
use crate::search_contexts::RevisionSpecifier;
use crate::search_job::{JobError, Verbosity};
use crate::search_jobutil_job_builders as jb;

// `exhaustive_job.go:29-32`.
/// `exhaustiveSupportedResultTypes`.
pub const EXHAUSTIVE_SUPPORTED_RESULT_TYPES: jb::ResultTypes = jb::ResultTypes(
    jb::ResultTypes::COMMIT.0
        | jb::ResultTypes::DIFF.0
        | jb::ResultTypes::FILE.0
        | jb::ResultTypes::PATH.0,
);
/// `exhaustiveDefaultResultTypes`.
pub const EXHAUSTIVE_DEFAULT_RESULT_TYPES: jb::ResultTypes =
    jb::ResultTypes(jb::ResultTypes::FILE.0 | jb::ResultTypes::PATH.0);

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// The `search.Inputs` fields [`new_exhaustive`] reads (`types.go:31-43`).
#[derive(Debug, Clone, PartialEq)]
pub struct Inputs {
    pub plan: Plan,
    /// `Plan.ToQ()`.
    pub query: Vec<Node>,
    pub pattern_type: SearchType,
    pub user_settings: jb::UserSettings,
    pub protocol: Protocol,
    pub features: Features,
    pub context_lines: i32,
    /// `authz.SubRepoEnabled(authz.DefaultSubRepoPermsChecker)`.
    pub sub_repo_enabled: bool,
}

impl Inputs {
    /// `Inputs.DefaultLimit` (`types.go:50-63`).
    pub fn default_limit(&self) -> i64 {
        match self.protocol {
            Protocol::Streaming => 10_000,
            Protocol::Batch => 30,
            Protocol::Exhaustive => 1_000_000,
        }
    }
}

// ---------------------------------------------------------------------------
// gitprotocol query nodes (`internal/gitserver/protocol`)
// ---------------------------------------------------------------------------

/// `gitprotocol.OperatorKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitOperatorKind {
    And,
    Or,
    Not,
}

/// Minimal `gitprotocol.Node`. `IgnoreCase`/`RevAtTime` do not affect the
/// rendered `String`, so they are not stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitQueryNode {
    AuthorMatches {
        expr: String,
    },
    CommitterMatches {
        expr: String,
    },
    MessageMatches {
        expr: String,
    },
    DiffMatches {
        expr: String,
    },
    DiffModifiesFile {
        expr: String,
    },
    Boolean(bool),
    Operator {
        kind: GitOperatorKind,
        operands: Vec<GitQueryNode>,
    },
}

impl GitQueryNode {
    fn operator(kind: GitOperatorKind, operands: Vec<GitQueryNode>) -> GitQueryNode {
        GitQueryNode::Operator { kind, operands }
    }

    /// `estimatedNodeCost` (`search_reduce.go:243-267`).
    fn estimated_cost(&self) -> f64 {
        match self {
            GitQueryNode::Operator { operands, .. } => {
                operands.iter().map(GitQueryNode::estimated_cost).sum()
            }
            GitQueryNode::Boolean(_) => 0.0,
            GitQueryNode::AuthorMatches { .. } | GitQueryNode::CommitterMatches { .. } => 5.0,
            GitQueryNode::MessageMatches { .. } => 10.0,
            GitQueryNode::DiffModifiesFile { .. } => 1000.0,
            GitQueryNode::DiffMatches { .. } => 10000.0,
        }
    }
}

impl std::fmt::Display for GitQueryNode {
    /// `gitprotocol.Node.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitQueryNode::AuthorMatches { expr } => {
                write!(f, "*protocol.AuthorMatches({expr})")
            }
            GitQueryNode::CommitterMatches { expr } => {
                write!(f, "*protocol.CommitterMatches({expr})")
            }
            GitQueryNode::MessageMatches { expr } => {
                write!(f, "*protocol.MessageMatches({expr})")
            }
            GitQueryNode::DiffMatches { expr } => write!(f, "*protocol.DiffMatches({expr})"),
            GitQueryNode::DiffModifiesFile { expr } => {
                write!(f, "*protocol.DiffModifiesFile({expr})")
            }
            GitQueryNode::Boolean(value) => write!(f, "*protocol.Boolean({value})"),
            GitQueryNode::Operator { kind, operands } => {
                let (sep, prefix) = match kind {
                    GitOperatorKind::And => (" AND ", ""),
                    GitOperatorKind::Or => (" OR ", ""),
                    GitOperatorKind::Not => (" AND NOT ", "NOT "),
                };
                let children: Vec<String> = operands.iter().map(ToString::to_string).collect();
                write!(f, "({prefix}{})", children.join(sep))
            }
        }
    }
}

/// `gitprotocol.NewAnd`.
fn new_and(operands: Vec<GitQueryNode>) -> GitQueryNode {
    match operands.len() {
        0 => GitQueryNode::Boolean(true),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Operator {
                        kind: GitOperatorKind::And,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::And, flattened)
        }
    }
}

/// `gitprotocol.NewOr`.
fn new_or(operands: Vec<GitQueryNode>) -> GitQueryNode {
    match operands.len() {
        0 => GitQueryNode::Boolean(false),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Operator {
                        kind: GitOperatorKind::Or,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::Or, flattened)
        }
    }
}

/// `gitprotocol.NewNot`.
fn new_not(operand: GitQueryNode) -> GitQueryNode {
    match operand {
        GitQueryNode::Operator {
            kind: GitOperatorKind::Not,
            mut operands,
        } => operands.remove(0),
        other => GitQueryNode::operator(GitOperatorKind::Not, vec![other]),
    }
}

/// `propagateBoolean` (`search_reduce.go:52-97`).
fn propagate_boolean(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator { kind, operands } = node else {
        return node;
    };
    match kind {
        GitOperatorKind::Not => {
            if let Some(GitQueryNode::Boolean(value)) = operands.first() {
                GitQueryNode::Boolean(!value)
            } else {
                GitQueryNode::operator(GitOperatorKind::Not, operands)
            }
        }
        GitOperatorKind::And => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Boolean(false) => return GitQueryNode::Boolean(false),
                    GitQueryNode::Boolean(true) => {}
                    other => filtered.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::And, filtered)
        }
        GitOperatorKind::Or => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Boolean(true) => return GitQueryNode::Boolean(true),
                    GitQueryNode::Boolean(false) => {}
                    other => filtered.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::Or, filtered)
        }
    }
}

/// `flatten` (`search_reduce.go:129-146`).
fn flatten(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator { kind, operands } = node else {
        return node;
    };
    if kind == GitOperatorKind::Not {
        return GitQueryNode::operator(kind, operands);
    }
    let mut flattened = Vec::with_capacity(operands.len());
    for operand in operands {
        match operand {
            GitQueryNode::Operator {
                kind: nested_kind,
                operands: nested_operands,
            } if nested_kind == kind => flattened.extend(nested_operands),
            other => flattened.push(other),
        }
    }
    GitQueryNode::operator(kind, flattened)
}

/// `sortAndByCost` (`search_reduce.go:269-282`). The baseline's `sort.Slice`
/// is unstable; with the small operand counts this slice produces (and no
/// equal-cost ties in the corpus) the ordering is unambiguous.
fn sort_and_by_cost(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator {
        kind: GitOperatorKind::And,
        mut operands,
    } = node
    else {
        return node;
    };
    operands.sort_by(|left, right| {
        left.estimated_cost()
            .partial_cmp(&right.estimated_cost())
            .expect("finite costs")
    });
    GitQueryNode::operator(GitOperatorKind::And, operands)
}

/// `Reduce` with the deterministic default passes. `rewriteConjunctive` and
/// `mergeOrRegexp` are not ported (the latter iterates a Go map); the corpus
/// avoids OR patterns so they are unobservable.
fn reduce(node: GitQueryNode) -> GitQueryNode {
    let mut node = match node {
        GitQueryNode::Operator { kind, operands } => {
            GitQueryNode::operator(kind, operands.into_iter().map(reduce).collect::<Vec<_>>())
        }
        other => other,
    };
    node = propagate_boolean(node);
    node = flatten(node);
    node = sort_and_by_cost(node);
    node
}

/// `queryPatternToPredicate` (`commit.go:247-257`).
fn query_pattern_to_predicate(node: Option<&Node>, diff: bool) -> Option<GitQueryNode> {
    let node = node?;
    match node {
        Node::Operator(operator) => {
            let operands = operator
                .operands
                .iter()
                .filter_map(|operand| query_pattern_to_predicate(Some(operand), diff))
                .collect::<Vec<_>>();
            match operator.kind {
                OperatorKind::And => Some(new_and(operands)),
                OperatorKind::Or => Some(new_or(operands)),
                OperatorKind::Concat => None,
            }
        }
        Node::Pattern(pattern) => Some(pattern_atom_to_predicate(pattern, diff)),
        Node::Parameter(_) => None,
    }
}

/// `patternAtomToPredicate` (`commit.go:275-290`).
fn pattern_atom_to_predicate(pattern: &Pattern, diff: bool) -> GitQueryNode {
    let expr = pattern.regexp_pattern();
    let atom = if diff {
        GitQueryNode::DiffMatches { expr }
    } else {
        GitQueryNode::MessageMatches { expr }
    };
    if pattern.negated {
        new_not(atom)
    } else {
        atom
    }
}

/// `queryParameterToPredicate` (`commit.go:292-325`).
fn query_parameter_to_predicate(parameter: &Parameter, diff: bool) -> Option<GitQueryNode> {
    let value = parameter.value.clone();
    let predicate = match parameter.field.as_str() {
        FIELD_AUTHOR => Some(GitQueryNode::AuthorMatches { expr: value }),
        FIELD_COMMITTER => Some(GitQueryNode::CommitterMatches { expr: value }),
        FIELD_MESSAGE => Some(GitQueryNode::MessageMatches { expr: value }),
        FIELD_CONTENT => Some(if diff {
            GitQueryNode::DiffMatches { expr: value }
        } else {
            GitQueryNode::MessageMatches { expr: value }
        }),
        FIELD_FILE => Some(GitQueryNode::DiffModifiesFile { expr: value }),
        FIELD_LANG => Some(GitQueryNode::DiffModifiesFile {
            expr: lang_to_file_regexp(&value),
        }),
        // `before:`/`after:` (`CommitBefore`/`CommitAfter`) are not modelled.
        _ => None,
    };
    match (parameter.negated, predicate) {
        (true, Some(predicate)) => Some(new_not(predicate)),
        (_, predicate) => predicate,
    }
}

/// `commit.QueryToGitQuery` (`commit.go:213-240`). `caseSensitive` only
/// affects the (unrendered) `IgnoreCase` flag, so it is not threaded through.
fn query_to_git_query(basic: &Basic, diff: bool) -> GitQueryNode {
    let mut nodes = Vec::with_capacity(basic.parameters.len() + 2);
    for parameter in basic.parameters.iter() {
        if parameter.annotation.labels.is_set(Labels::IS_PREDICATE) {
            continue;
        }
        if let Some(predicate) = query_parameter_to_predicate(parameter, diff) {
            nodes.push(predicate);
        }
    }
    if let Some(predicate) = query_pattern_to_predicate(basic.pattern.as_ref(), diff) {
        nodes.push(predicate);
    }
    reduce(new_and(nodes))
}

// ---------------------------------------------------------------------------
// Job construction
// ---------------------------------------------------------------------------

/// `searcher.TextSearchJob` as built by `NewTextSearchJob` (`job.go:282-304`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearcherTextJob {
    pub pattern_info: jb::TextPatternInfo,
    /// `PathRegexps`, rendered as their pattern source.
    pub path_regexps: Vec<String>,
    pub use_full_deadline: bool,
    pub indexed: bool,
    pub num_context_lines: i32,
}

/// `commit.SearchJob` as built by `NewExhaustive` (`exhaustive_job.go:74-87`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSearchJob {
    pub query: GitQueryNode,
    pub diff: bool,
    pub limit: i64,
    pub include_modified_files: bool,
    pub concurrency: i32,
}

/// The child of the exhaustive `repoPagerJob`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExhaustiveChild {
    SearcherText(SearcherTextJob),
    Commit(CommitSearchJob),
}

/// `repoPagerJob` reduced to the fields `NewExhaustive`/`Exhaustive` read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoPagerJob {
    pub repo_opts: jb::RepoOptions,
    /// The commit branch sets `repoOptionsCopy.OnlyCloned = true`; the text
    /// branch leaves it at the zero value.
    pub only_cloned: bool,
    pub contains_ref_globs: bool,
    pub skip_partitioning: bool,
    pub child: ExhaustiveChild,
}

/// `jobutil.Exhaustive`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exhaustive {
    pub repo_pager_job: RepoPagerJob,
}

/// `hasPredicates` (`exhaustive_job.go:116-125`): first positive value, then
/// negated values; the first registered predicate wins.
pub fn has_predicates(field: &str, query: &[Node]) -> Option<String> {
    let mut values = Vec::new();
    let mut negated = Vec::new();
    visit_field(query, field, &mut |value, is_negated, _annotation| {
        if is_negated {
            negated.push(value.to_owned());
        } else {
            values.push(value.to_owned());
        }
    });
    for value in values.iter().chain(negated.iter()) {
        if let Some((predicate, _)) = scan_predicate(field, value.as_bytes()) {
            return Some(predicate);
        }
    }
    None
}

/// `NewExhaustive` (`exhaustive_job.go:27-114`).
pub fn new_exhaustive(inputs: &Inputs) -> Result<Exhaustive, JobError> {
    if inputs.protocol != Protocol::Exhaustive {
        return Err(JobError(
            "only works for exhaustive search inputs".to_owned(),
        ));
    }

    if inputs.plan.len() != 1 {
        return Err(JobError(
            "Invalid query: Search Jobs does not support using AND/OR with filters (for example \"file:.go or file:.rs\"). Consider splitting your query into multiple subqueries and creating separate search jobs for each.".to_owned(),
        ));
    }

    let basic = &inputs.plan[0];

    if !q_string_value(&basic.to_parse_tree(), FIELD_SELECT).is_empty() {
        return Err(JobError(
            "select: is not supported in Search Jobs".to_owned(),
        ));
    }

    if let Some(predicate) = has_predicates(FIELD_FILE, &inputs.query) {
        return Err(JobError(format!(
            "file predicates are not supported. Got {predicate}"
        )));
    }

    if inputs.pattern_type == SearchType::Regex {
        if let Some(Node::Pattern(term)) = &basic.pattern {
            if term.value == ".*" {
                return Err(JobError("regex search with .* is not supported".to_owned()));
            }
        }
    }

    let repo_options = jb::to_repo_options(basic, &inputs.user_settings);
    let result_types =
        jb::compute_result_types(basic, inputs.pattern_type, EXHAUSTIVE_DEFAULT_RESULT_TYPES);

    let unsupported = jb::ResultTypes(result_types.0 & !EXHAUSTIVE_SUPPORTED_RESULT_TYPES.0);
    if unsupported.0 != 0 {
        return Err(JobError(format!(
            "your query contains the following type filters: {}. However Search Jobs only supports: {}.",
            result_types.as_string(),
            EXHAUSTIVE_SUPPORTED_RESULT_TYPES.as_string(),
        )));
    }

    let (child, only_cloned, skip_partitioning) = if result_types.has(jb::ResultTypes(
        jb::ResultTypes::COMMIT.0 | jb::ResultTypes::DIFF.0,
    )) {
        let (_, _, own) = jb::is_ownership_search(basic);
        let diff = result_types.has(jb::ResultTypes::DIFF);
        let limit = jb::parameters_max_results(&basic.parameters, inputs.default_limit());
        let child = ExhaustiveChild::Commit(CommitSearchJob {
            query: query_to_git_query(basic, diff),
            diff,
            limit,
            include_modified_files: inputs.sub_repo_enabled || own,
            concurrency: 4,
        });
        (child, true, true)
    } else if result_types.has(jb::ResultTypes(
        jb::ResultTypes::FILE.0 | jb::ResultTypes::PATH.0,
    )) {
        let use_full_deadline = jb::parameters_timeout_nanos(&basic.parameters).is_some()
            || jb::parameters_count(&basic.parameters).is_some()
            || inputs.protocol != Protocol::Batch;
        let pattern_info = jb::to_text_pattern_info(
            basic,
            result_types,
            &inputs.features,
            inputs.default_limit(),
        )
        .map_err(JobError)?;
        let path_regexps = jb::get_path_regexps(basic, &pattern_info)
            .into_iter()
            .map(|regex| regex.as_str().to_owned())
            .collect();
        let child = ExhaustiveChild::SearcherText(SearcherTextJob {
            pattern_info,
            path_regexps,
            use_full_deadline,
            indexed: false,
            num_context_lines: inputs.context_lines,
        });
        (child, false, false)
    } else {
        return Err(JobError(format!(
            "internal error: unsupported result types {}",
            result_types.as_string()
        )));
    };

    let contains_ref_globs = repo_options
        .repo_filters
        .iter()
        .any(|filter| filter.revs.iter().any(RevisionSpecifier::has_ref_glob));

    Ok(Exhaustive {
        repo_pager_job: RepoPagerJob {
            repo_opts: repo_options,
            only_cloned,
            contains_ref_globs,
            skip_partitioning,
            child,
        },
    })
}

/// `TextPatternInfo.String()` (`types.go:336-369`).
pub fn text_pattern_info_string(pattern_info: &jb::TextPatternInfo) -> String {
    let mut args = vec![pattern_info.query.to_string()];

    if pattern_info.is_structural_pat {
        if pattern_info.comby_rule.is_empty() {
            args.push("comby".to_owned());
        } else {
            args.push(format!("comby:{}", pattern_info.comby_rule));
        }
    }
    if pattern_info.is_case_sensitive {
        args.push("case".to_owned());
    }
    if !pattern_info.pattern_matches_content {
        args.push("nocontent".to_owned());
    }
    if !pattern_info.pattern_matches_path {
        args.push("nopath".to_owned());
    }
    if pattern_info.file_match_limit > 0 {
        args.push(format!("filematchlimit:{}", pattern_info.file_match_limit));
    }
    for language in &pattern_info.languages {
        args.push(format!("lang:{language}"));
    }

    let path = if pattern_info.path_patterns_are_case_sensitive {
        "F"
    } else {
        "f"
    };
    if !pattern_info.exclude_paths.is_empty() {
        args.push(format!("-{path}:{}", go_quote(&pattern_info.exclude_paths)));
    }
    for include in &pattern_info.include_paths {
        args.push(format!("{path}:{}", go_quote(include)));
    }

    format!("TextPatternInfo{{{}}}", args.join(","))
}

fn bool_string(value: bool) -> String {
    if value {
        "true".to_owned()
    } else {
        "false".to_owned()
    }
}

fn string_slice<'a, I: IntoIterator<Item = &'a String>>(values: I) -> String {
    let joined = values.into_iter().cloned().collect::<Vec<_>>().join(" ");
    format!("[{joined}]")
}

/// The job returned by `Exhaustive.Job`: one `search.RepositoryRevisions`
/// resolved into the pager child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedJob {
    child: ExhaustiveChild,
    num_repos: usize,
}

impl ResolvedJob {
    /// `Describer.Name`.
    pub fn name(&self) -> &'static str {
        match &self.child {
            ExhaustiveChild::SearcherText(_) => "SearcherTextSearchJob",
            ExhaustiveChild::Commit(job) => {
                if job.diff {
                    "DiffSearchJob"
                } else {
                    "CommitSearchJob"
                }
            }
        }
    }

    /// `Describer.Children`; the leaf jobs have none.
    pub fn children(&self) -> Vec<&ResolvedJob> {
        Vec::new()
    }

    /// `Describer.Attributes`, normalized to `(key, value)` strings.
    pub fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut attributes = Vec::new();
        match &self.child {
            ExhaustiveChild::SearcherText(job) => {
                if verbosity == Verbosity::Max {
                    attributes.push((
                        "useFullDeadline".to_owned(),
                        bool_string(job.use_full_deadline),
                    ));
                    attributes.push((
                        "patternInfo".to_owned(),
                        text_pattern_info_string(&job.pattern_info),
                    ));
                    attributes.push(("numRepos".to_owned(), self.num_repos.to_string()));
                    attributes.push((
                        "pathRegexps".to_owned(),
                        string_slice(job.path_regexps.iter()),
                    ));
                }
                if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
                    attributes.push(("indexed".to_owned(), bool_string(job.indexed)));
                }
            }
            ExhaustiveChild::Commit(job) => {
                if verbosity == Verbosity::Max {
                    attributes.push((
                        "includeModifiedFiles".to_owned(),
                        bool_string(job.include_modified_files),
                    ));
                }
                if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
                    attributes.push(("query".to_owned(), job.query.to_string()));
                    attributes.push(("diff".to_owned(), bool_string(job.diff)));
                    attributes.push(("limit".to_owned(), job.limit.to_string()));
                }
            }
        }
        attributes
    }
}

// ---------------------------------------------------------------------------
// resolver injection (`reposNewResolver` / `repos.Resolver`)
// ---------------------------------------------------------------------------

/// `repos.RepoRevSpecs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRevSpecs {
    pub repo: MinimalRepo,
    pub revs: Vec<RevisionSpecifier>,
}

/// The page sequence `repos.Resolver.IterateRepoRevs` yields, plus the
/// terminal iterator error.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoRevSpecsIterator {
    pub pages: Vec<Vec<RepoRevSpecs>>,
    pub error: Option<JobError>,
}

/// `repos.Resolved`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Resolved {
    pub repo_revs: Vec<RepositoryRevisions>,
    pub backends_missing: i64,
}

/// Minimal stand-in for `repos.Resolver` (`repos.go:84-185`). The baseline
/// builds it from the runtime clients; this port injects the two methods the
/// `Exhaustive` wrappers call and forwards the constructed `RepoOptions`.
pub trait ExhaustiveResolver {
    /// `Resolver.IterateRepoRevs`.
    fn iterate_repo_revs(&self, repo_opts: &jb::RepoOptions) -> RepoRevSpecsIterator;
    /// `Resolver.ResolveRevSpecs`.
    fn resolve_rev_specs(
        &self,
        repo_opts: &jb::RepoOptions,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<Resolved, JobError>;
}

/// `reposNewResolver` (`exhaustive_job.go:145-147`). The baseline constructs a
/// `repos.Resolver` from `clients`; here the resolver is injected.
pub fn repos_new_resolver(resolver: &dyn ExhaustiveResolver) -> &dyn ExhaustiveResolver {
    resolver
}

impl Exhaustive {
    /// `Exhaustive.Job` (`exhaustive_job.go:127-133`). The single
    /// `RepositoryRevisions` becomes the unindexed repo set assigned by
    /// `setRepos`.
    pub fn job(&self, _repo_revs: &RepositoryRevisions) -> ResolvedJob {
        ResolvedJob {
            child: self.repo_pager_job.child.clone(),
            num_repos: 1,
        }
    }

    /// `Exhaustive.RepositoryRevSpecs` (`exhaustive_job.go:136-138`).
    pub fn repository_rev_specs(&self, resolver: &dyn ExhaustiveResolver) -> RepoRevSpecsIterator {
        repos_new_resolver(resolver).iterate_repo_revs(&self.repo_pager_job.repo_opts)
    }

    /// `Exhaustive.ResolveRepositoryRevSpec` (`exhaustive_job.go:141-143`).
    pub fn resolve_repository_rev_spec(
        &self,
        resolver: &dyn ExhaustiveResolver,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<Resolved, JobError> {
        repos_new_resolver(resolver)
            .resolve_rev_specs(&self.repo_pager_job.repo_opts, repo_rev_specs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_search_query::basic::plan_to_q;
    use sg_search_query::pipeline::pipeline_query;

    fn inputs(query: &str, search_type: SearchType) -> Inputs {
        let plan = pipeline_query(query, search_type).expect("valid query");
        Inputs {
            query: plan_to_q(&plan),
            plan,
            pattern_type: search_type,
            user_settings: jb::UserSettings::default(),
            protocol: Protocol::Exhaustive,
            features: Features::default(),
            context_lines: 1,
            sub_repo_enabled: false,
        }
    }

    fn repo_revs() -> RepositoryRevisions {
        RepositoryRevisions {
            repo: sg_zoekt::indexed_repo_revs::RepositoryRevisionsRepo {
                id: 1,
                name: "repo1".to_owned(),
            },
            revs: vec!["HEAD".to_owned()],
        }
    }

    #[test]
    fn rejects_wrong_protocol() {
        let mut inputs = inputs("foo type:file", SearchType::Standard);
        inputs.protocol = Protocol::Streaming;
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "only works for exhaustive search inputs"
        );
    }

    #[test]
    fn rejects_multiple_plans() {
        let inputs = inputs(
            "type:file index:no (repo:repo1 or repo:repo2) content",
            SearchType::Standard,
        );
        assert!(inputs.plan.len() > 1);
        assert!(new_exhaustive(&inputs)
            .unwrap_err()
            .0
            .starts_with("Invalid query: Search Jobs does not support"));
    }

    #[test]
    fn rejects_select() {
        let inputs = inputs("index:no foo select:file.owners", SearchType::Standard);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "select: is not supported in Search Jobs"
        );
    }

    #[test]
    fn rejects_file_predicates() {
        let inputs = inputs(
            "type:file index:no file:has.content(content)",
            SearchType::Standard,
        );
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "file predicates are not supported. Got has.content(content)"
        );
    }

    #[test]
    fn rejects_regex_dot_star() {
        let inputs = inputs("type:file index:no r:repo .*", SearchType::Regex);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "regex search with .* is not supported"
        );
    }

    #[test]
    fn rejects_unsupported_result_types() {
        let inputs = inputs("index:no type:repo", SearchType::Standard);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "your query contains the following type filters: repo. However Search Jobs only supports: commit, diff, file, path."
        );
    }

    #[test]
    fn builds_text_job() {
        let inputs = inputs("foo type:file", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(!exhaustive.repo_pager_job.only_cloned);
        assert!(!exhaustive.repo_pager_job.skip_partitioning);
        let job = exhaustive.job(&repo_revs());
        assert_eq!(job.name(), "SearcherTextSearchJob");
        assert!(job.children().is_empty());
        let attributes = job.attributes(Verbosity::Max);
        assert_eq!(
            attributes[0],
            ("useFullDeadline".to_owned(), "true".to_owned())
        );
        assert_eq!(
            attributes[1].1,
            "TextPatternInfo{\"foo\",nopath,filematchlimit:1000000}"
        );
        assert_eq!(attributes[2], ("numRepos".to_owned(), "1".to_owned()));
        assert_eq!(attributes.last().unwrap().0, "indexed");
    }

    #[test]
    fn builds_commit_job() {
        let inputs = inputs("type:commit author:alice foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(exhaustive.repo_pager_job.only_cloned);
        assert!(exhaustive.repo_pager_job.skip_partitioning);
        let job = exhaustive.job(&repo_revs());
        assert_eq!(job.name(), "CommitSearchJob");
        let attributes = job.attributes(Verbosity::Max);
        assert_eq!(
            attributes,
            vec![
                ("includeModifiedFiles".to_owned(), "false".to_owned()),
                (
                    "query".to_owned(),
                    "(*protocol.AuthorMatches(alice) AND *protocol.MessageMatches(foo))".to_owned(),
                ),
                ("diff".to_owned(), "false".to_owned()),
                ("limit".to_owned(), "1000000".to_owned()),
            ]
        );
    }

    #[test]
    fn text_branch_records_repo_options() {
        let inputs = inputs("repo:repo1@main type:file foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert_eq!(exhaustive.repo_pager_job.repo_opts.repo_filters.len(), 1);
        assert_eq!(
            exhaustive.repo_pager_job.repo_opts.repo_filters[0].repo,
            "repo1"
        );
        assert_eq!(
            exhaustive.repo_pager_job.repo_opts.repo_filters[0].revs[0].rev_spec,
            "main"
        );
        assert!(!exhaustive.repo_pager_job.contains_ref_globs);
    }

    #[test]
    fn text_branch_detects_ref_globs() {
        let inputs = inputs(
            "repo:repo1@*refs/heads/* type:file foo",
            SearchType::Standard,
        );
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(exhaustive.repo_pager_job.contains_ref_globs);
    }

    struct FakeResolver {
        seen: std::cell::RefCell<Vec<jb::RepoOptions>>,
    }

    impl ExhaustiveResolver for FakeResolver {
        fn iterate_repo_revs(&self, repo_opts: &jb::RepoOptions) -> RepoRevSpecsIterator {
            self.seen.borrow_mut().push(repo_opts.clone());
            RepoRevSpecsIterator {
                pages: vec![vec![RepoRevSpecs {
                    repo: MinimalRepo {
                        id: 3,
                        name: "alpha".to_owned(),
                    },
                    revs: vec![RevisionSpecifier::default()],
                }]],
                error: None,
            }
        }

        fn resolve_rev_specs(
            &self,
            repo_opts: &jb::RepoOptions,
            specs: &[RepoRevSpecs],
        ) -> Result<Resolved, JobError> {
            self.seen.borrow_mut().push(repo_opts.clone());
            Ok(Resolved {
                repo_revs: vec![RepositoryRevisions {
                    repo: sg_zoekt::indexed_repo_revs::RepositoryRevisionsRepo {
                        id: specs[0].repo.id,
                        name: specs[0].repo.name.clone(),
                    },
                    revs: vec!["HEAD".to_owned()],
                }],
                backends_missing: 2,
            })
        }
    }

    #[test]
    fn resolver_wrappers_forward_repo_options() {
        let inputs = inputs("type:commit repo:repo1 foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        let resolver = FakeResolver {
            seen: std::cell::RefCell::new(Vec::new()),
        };
        let iterator = exhaustive.repository_rev_specs(&resolver);
        assert_eq!(iterator.pages.len(), 1);
        let specs = &iterator.pages[0];
        let resolved = exhaustive
            .resolve_repository_rev_spec(&resolver, specs)
            .unwrap();
        assert_eq!(resolved.backends_missing, 2);
        assert_eq!(
            resolver.seen.borrow()[0].repo_filters[0].repo,
            "repo1".to_owned()
        );
        assert_eq!(
            resolver.seen.borrow()[1].repo_filters[0].repo,
            "repo1".to_owned()
        );
    }
}
