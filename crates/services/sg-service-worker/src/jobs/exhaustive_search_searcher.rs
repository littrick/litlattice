#![forbid(unsafe_code)]

//! `internal/search/exhaustive/service/searcher.go`: `FromSearchClient`, the
//! real exhaustive-search searcher factory.
//!
//! Ported:
//!
//! * [`FromSearchClient::new_search`] does the per-call user check, the
//!   `index:no ` prefix, the structural-search rejection, `client.Plan` and
//!   `jobutil.NewExhaustive` (`searcher.go:25-63`);
//! * [`SearcherQuery::repository_rev_specs`] maps the iterator to the
//!   queue's `RepositoryRevSpecs` (empty revision -> `HEAD`, `:` join) and
//!   swallows `isReposMissingError` (`searcher.go:71-106`);
//! * [`SearcherQuery::resolve_repository_rev_spec`] handles revision-specifier
//!   parsing, `Resolved.BackendsMissing`, the same-repo check and per-revision
//!   fan-out (`searcher.go:108-164`);
//! * [`SearcherQuery::search`] handles match writing, write-error precedence,
//!   and the `HEAD` + `RevisionNotFound` success case (`searcher.go:166-218`).
//!
//! A minimal local port of `jobutil.NewExhaustive` validation lives here
//! because `sg-service-worker` neither depends on `sg-service-frontend` (where
//! M9.8hp ported the full job) nor on the query parser. The [`Inputs`] model is
//! the *already planned* query: [`SearchClient::plan`] is the `client.Plan`
//! seam, so the injected client supplies the parsed [`Basic`]s (pattern values,
//! parameter values, predicate tokens and labels). Only the validation sequence
//! and the pager fields the searcher observes are reproduced; the job leaf is
//! modelled as [`ExhaustiveChild`].
//!
//! DB/gitserver/zoekt/searcher access is behind the injected [`RepoResolver`]
//! and [`JobRunner`] seams, both part of [`JobClients`] (the baseline's
//! `job.RuntimeClients`). The default implementations return explicit
//! not-ported errors. `isSameUser` reads the baseline's context actor, which
//! Rust does not have; [`UserContext`] is the injected stand-in.
//!
//! See `docs/overviews/m9_8hv_exhaustive_searcher_overview.md`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::exhaustive_search::{self as ex};
use sg_insights_query::sg_search_query::{
    self as sgq,
    basic::map_plan,
    fields::{FIELD_PATTERN_TYPE, FIELD_TYPE as QUERY_FIELD_TYPE},
    labels::Labels as QueryLabels,
    node::{
        go_quote, visit_field, Annotation as QueryAnnotation, Node as QueryNode,
        Operator as QueryOperator, OperatorKind as QueryOperatorKind, Pattern as QueryPattern,
    },
    parser::SearchType as QuerySearchType,
    transformer::lowercase_field_names,
};
use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions, RepositoryRevisionsRepo};
use sg_zoekt::languages::{get_language_by_name_or_alias, lang_to_file_regexp};

use crate::jobs::exhaustive_search_job::{MatchWriter, NewSearcher, SearchQuery};

const FIELD_COUNT: &str = "count";
const FIELD_FILE: &str = "file";
const FIELD_LANG: &str = "lang";
const FIELD_REPO: &str = "repo";
const FIELD_SELECT: &str = "select";
const FIELD_TYPE: &str = "type";

// ---------------------------------------------------------------------------
// planned query model (`search.Inputs` / `query.Basic`)
// ---------------------------------------------------------------------------

/// `query.SearchType` (`parser.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchType {
    Regex,
    Literal,
    Structural,
    Standard,
    CodyContext,
    Keyword,
}

/// `search.Protocol` (`internal/search/types.go:72-95`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Streaming,
    Batch,
    Exhaustive,
}

/// `search.Mode` (`internal/search/types.go:65-70`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Precise,
    SmartSearch,
}

/// The subset of `search.Features` `NewExhaustive` reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Features {
    pub content_based_lang_filters: bool,
    pub debug: bool,
}

/// The subset of `schema.Settings` `toRepoOptions` reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UserSettings {
    pub search_include_forks: Option<bool>,
    pub search_include_archived: Option<bool>,
}

/// A planned `query.Parameter` leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    pub field: String,
    pub value: String,
    pub negated: bool,
    /// `query.ScanPredicate`'s first return value when the value is a
    /// predicate (for example `has.content(content)`).
    pub predicate: Option<String>,
    /// Whether the parser labelled the value `IsPredicate`
    /// (`query.Labels.IsPredicate`).
    pub is_predicate: bool,
}

/// The planned pattern leaf. `Other` stands for an operator or any other
/// complex node: non-empty and not content-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternNode {
    Pattern { value: String, content: bool },
    Other,
}

/// A planned `query.Basic`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Basic {
    pub parameters: Vec<Parameter>,
    pub pattern: Option<PatternNode>,
}

/// A planned `query.Plan`: disjoint basic queries whose results are unioned.
pub type Plan = Vec<Basic>;

impl Basic {
    /// `Basic.IsEmptyPattern`.
    pub fn is_empty_pattern(&self) -> bool {
        match &self.pattern {
            None => true,
            Some(PatternNode::Pattern { value, .. }) => value.is_empty(),
            Some(PatternNode::Other) => false,
        }
    }

    /// `Parameters.IncludeExcludeValues`: predicates are skipped.
    pub fn include_exclude_values(&self, field: &str) -> (Vec<String>, Vec<String>) {
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        for parameter in &self.parameters {
            if parameter.field != field || parameter.is_predicate {
                continue;
            }
            if parameter.negated {
                exclude.push(parameter.value.clone());
            } else {
                include.push(parameter.value.clone());
            }
        }
        (include, exclude)
    }

    /// `Parameters.FindValue`: the first value of `field`.
    pub fn find_value(&self, field: &str) -> String {
        self.parameters
            .iter()
            .find(|parameter| parameter.field == field)
            .map(|parameter| parameter.value.clone())
            .unwrap_or_default()
    }

    /// `Q.StringValue(ParseTree, field)`: the last non-negated value.
    pub fn last_non_negated_value(&self, field: &str) -> String {
        let mut value = String::new();
        for parameter in &self.parameters {
            if parameter.field == field && !parameter.negated {
                value = parameter.value.clone();
            }
        }
        value
    }

    fn pattern_value(&self) -> Option<&str> {
        match &self.pattern {
            Some(PatternNode::Pattern { value, .. }) => Some(value),
            _ => None,
        }
    }
}

/// The `search.Inputs` fields `NewExhaustive` reads (`types.go:31-43`), with
/// `Plan.ToQ()` replaced by the planned basics themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct Inputs {
    pub plan: Plan,
    pub pattern_type: SearchType,
    pub user_settings: UserSettings,
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
// result types (`internal/search/result/result_type.go`)
// ---------------------------------------------------------------------------

/// `result.Types`: a bit set of result kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResultTypes(pub u8);

impl ResultTypes {
    pub const EMPTY: ResultTypes = ResultTypes(0);
    pub const REPO: ResultTypes = ResultTypes(1 << 0);
    pub const SYMBOL: ResultTypes = ResultTypes(1 << 1);
    pub const FILE: ResultTypes = ResultTypes(1 << 2);
    pub const PATH: ResultTypes = ResultTypes(1 << 3);
    pub const DIFF: ResultTypes = ResultTypes(1 << 4);
    pub const COMMIT: ResultTypes = ResultTypes(1 << 5);
    pub const STRUCTURAL: ResultTypes = ResultTypes(1 << 6);

    pub fn has(self, other: ResultTypes) -> bool {
        self.0 & other.0 != 0
    }

    pub fn with(self, other: ResultTypes) -> ResultTypes {
        ResultTypes(self.0 | other.0)
    }

    /// `result.TypeFromString`; unknown values are ignored.
    pub fn from_string(value: &str) -> Option<ResultTypes> {
        Some(match value {
            "repo" => Self::REPO,
            "symbol" => Self::SYMBOL,
            "file" => Self::FILE,
            "path" => Self::PATH,
            "diff" => Self::DIFF,
            "commit" => Self::COMMIT,
            "structural" => Self::STRUCTURAL,
            _ => return None,
        })
    }

    /// `result.Types.String`: names sorted and joined with `, `.
    pub fn as_string(self) -> String {
        let mut names: Vec<&str> = Vec::new();
        for (name, value) in [
            ("repo", Self::REPO),
            ("symbol", Self::SYMBOL),
            ("file", Self::FILE),
            ("path", Self::PATH),
            ("diff", Self::DIFF),
            ("commit", Self::COMMIT),
            ("structural", Self::STRUCTURAL),
        ] {
            if self.has(value) {
                names.push(name);
            }
        }
        names.sort_unstable();
        names.join(", ")
    }
}

/// `exhaustive_job.go:29-32`.
pub const EXHAUSTIVE_SUPPORTED_RESULT_TYPES: ResultTypes = ResultTypes(
    ResultTypes::COMMIT.0 | ResultTypes::DIFF.0 | ResultTypes::FILE.0 | ResultTypes::PATH.0,
);
/// `exhaustive_job.go:31-32`.
pub const EXHAUSTIVE_DEFAULT_RESULT_TYPES: ResultTypes =
    ResultTypes(ResultTypes::FILE.0 | ResultTypes::PATH.0);

/// `computeResultTypes` (`job.go:757`).
fn compute_result_types(
    basic: &Basic,
    search_type: SearchType,
    default_types: ResultTypes,
) -> ResultTypes {
    if search_type == SearchType::Structural && !basic.is_empty_pattern() {
        return ResultTypes::STRUCTURAL;
    }

    let (types, _) = basic.include_exclude_values(FIELD_TYPE);
    if types.is_empty() {
        // `visit_pattern` marks every pattern leaf; `IsContent` is only set on
        // content patterns (not `file:`/`path:` patterns).
        if let Some(PatternNode::Pattern { content: true, .. }) = &basic.pattern {
            return ResultTypes::FILE;
        }
    }

    if types.is_empty() {
        return default_types;
    }

    let mut result = ResultTypes::EMPTY;
    for t in types {
        if let Some(types) = ResultTypes::from_string(&t) {
            result = result.with(types);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// repo filters / revision specifiers
// ---------------------------------------------------------------------------

/// `query.RevisionSpecifier` (`repo_revs.go:13-44`). `RevAtTime` keeps the
/// encoded `rat=` payload because `String()` only re-emits it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionSpecifier {
    RevSpec(String),
    RefGlob(String),
    ExcludeRefGlob(String),
    RevAtTime(String),
}

impl Default for RevisionSpecifier {
    fn default() -> RevisionSpecifier {
        RevisionSpecifier::RevSpec(String::new())
    }
}

impl RevisionSpecifier {
    /// `RevisionSpecifier.String` (`repo_revs.go:33-44`).
    pub fn to_string_value(&self) -> String {
        match self {
            RevisionSpecifier::RevSpec(spec) => spec.clone(),
            RevisionSpecifier::RefGlob(glob) => format!("*{glob}"),
            RevisionSpecifier::ExcludeRefGlob(glob) => format!("*!{glob}"),
            RevisionSpecifier::RevAtTime(payload) => format!("rat={payload}"),
        }
    }

    /// `RevisionSpecifier.HasRefGlob`.
    pub fn has_ref_glob(&self) -> bool {
        matches!(
            self,
            RevisionSpecifier::RefGlob(_) | RevisionSpecifier::ExcludeRefGlob(_)
        )
    }
}

impl std::fmt::Display for RevisionSpecifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_value())
    }
}

/// `query.ParseRevisionSpecifier` (`repo_revs.go:150-163`) for repo filters.
/// The `rat=` payload is not validated here (the pipeline already did).
pub fn parse_revision_specifier(spec: &str) -> RevisionSpecifier {
    if let Some(glob) = spec.strip_prefix("*!") {
        RevisionSpecifier::ExcludeRefGlob(glob.to_owned())
    } else if let Some(glob) = spec.strip_prefix('*') {
        RevisionSpecifier::RefGlob(glob.to_owned())
    } else if let Some(payload) = spec.strip_prefix("rat=") {
        RevisionSpecifier::RevAtTime(payload.to_owned())
    } else {
        RevisionSpecifier::RevSpec(spec.to_owned())
    }
}

/// `query.ParseRevisionSpecifier` as called by `toRepoRevSpecs`, where a
/// malformed `rat=` payload is a real error (`ParseRevAtTime`).
fn parse_revision_specifier_checked(spec: &str) -> Result<RevisionSpecifier, String> {
    let Some(payload) = spec.strip_prefix("rat=") else {
        return Ok(parse_revision_specifier(spec));
    };
    // `ParseRevAtTime` decodes base64(URL) JSON; the decoded value is not used
    // by `String()`, but malformed payloads must error.
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(payload)
        .map_err(|err| err.to_string())?;
    serde_json::from_slice::<serde_json::Value>(&decoded).map_err(|err| err.to_string())?;
    Ok(RevisionSpecifier::RevAtTime(payload.to_owned()))
}

/// `query.ParsedRepoFilter` reduced to what this slice forwards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedRepoFilter {
    pub repo: String,
    pub revs: Vec<RevisionSpecifier>,
}

/// `query.ParseRepositoryRevisions` (`repo_revs.go:85-148`). The baseline
/// validates the repo pattern by compiling a case-insensitive regex; the
/// pipeline already validated it, so this port only splits the specifiers.
pub fn parse_repository_revisions(repo_and_optional_rev: &str) -> ParsedRepoFilter {
    let Some(index) = repo_and_optional_rev.find('@') else {
        return ParsedRepoFilter {
            repo: repo_and_optional_rev.to_owned(),
            revs: Vec::new(),
        };
    };
    let repo = repo_and_optional_rev[..index].to_owned();
    let mut revs = Vec::new();
    for part in repo_and_optional_rev[index + 1..].split(':') {
        if part.is_empty() {
            continue;
        }
        revs.push(parse_revision_specifier(part));
    }
    if revs.is_empty() {
        // Default branch.
        revs.push(RevisionSpecifier::default());
    }
    ParsedRepoFilter { repo, revs }
}

/// Minimal `search.RepoOptions` (`types.go:427-459`) for the searcher path.
///
/// Only the repo filters are modelled: they are forwarded to the injected
/// resolver and drive `containsRefGlobs`. `fork`/`archived`/`visibility`/...
/// are not observable in this slice.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoOptions {
    pub repo_filters: Vec<ParsedRepoFilter>,
    pub minus_repo_filters: Vec<String>,
}

impl RepoOptions {
    fn contains_ref_globs(&self) -> bool {
        self.repo_filters
            .iter()
            .any(|filter| filter.revs.iter().any(RevisionSpecifier::has_ref_glob))
    }
}

/// `repositories` (`job.go:706-724`).
fn repositories(basic: &Basic) -> (Vec<ParsedRepoFilter>, Vec<String>) {
    let mut repos = Vec::new();
    let mut minus_repos = Vec::new();
    for parameter in &basic.parameters {
        if parameter.field != FIELD_REPO || parameter.is_predicate {
            continue;
        }
        if parameter.negated {
            minus_repos.push(parameter.value.clone());
        } else {
            repos.push(parse_repository_revisions(&parameter.value));
        }
    }
    (repos, minus_repos)
}

/// `toRepoOptions` (`job.go:794-832`), reduced to the repo filters.
fn to_repo_options(basic: &Basic) -> RepoOptions {
    let (repo_filters, minus_repo_filters) = repositories(basic);
    RepoOptions {
        repo_filters,
        minus_repo_filters,
    }
}

// ---------------------------------------------------------------------------
// NewExhaustive
// ---------------------------------------------------------------------------

/// `hasPredicates` (`exhaustive_job.go:116-125`): positive values first, then
/// negated ones; the first registered predicate wins.
pub fn has_predicates(basic: &Basic, field: &str) -> Option<String> {
    let mut positive = Vec::new();
    let mut negated = Vec::new();
    for parameter in &basic.parameters {
        if parameter.field != field {
            continue;
        }
        if parameter.negated {
            negated.push(parameter);
        } else {
            positive.push(parameter);
        }
    }
    for parameter in positive.into_iter().chain(negated) {
        if let Some(predicate) = &parameter.predicate {
            return Some(predicate.clone());
        }
    }
    None
}

/// The job leaf `NewExhaustive` selects for an exhaustive search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExhaustiveChild {
    /// `searcher.TextSearchJob`.
    SearcherText,
    /// `commit.SearchJob`; `diff` selects `DiffSearchJob` vs
    /// `CommitSearchJob`.
    Commit { diff: bool },
}

/// `jobutil.Exhaustive` reduced to the pager fields the searcher forwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exhaustive {
    pub repo_options: RepoOptions,
    pub child: ExhaustiveChild,
    pub only_cloned: bool,
    pub contains_ref_globs: bool,
    pub skip_partitioning: bool,
    /// `commit.SearchJob.Limit` (`parameters.maxResults`).
    pub limit: i64,
}

/// `toTextPatternInfo`'s empty-pattern guard (`job.go:713-724`).
fn text_pattern_guard(basic: &Basic, features: &Features) -> Option<String> {
    let (mut files_include, mut files_exclude) = basic.include_exclude_values(FIELD_FILE);
    let (lang_alias_include, lang_alias_exclude) = basic.include_exclude_values(FIELD_LANG);
    let mut lang_exclude = Vec::new();
    if features.content_based_lang_filters {
        lang_exclude = to_lang_filters(&lang_alias_exclude);
    } else {
        files_include.extend(
            lang_alias_include
                .iter()
                .map(|lang| lang_to_file_regexp(lang)),
        );
        files_exclude.extend(
            lang_alias_exclude
                .iter()
                .map(|lang| lang_to_file_regexp(lang)),
        );
    }
    // The baseline applies this guard only when the pattern lowered to a
    // `Pattern` node (`job.go:713-724`); other node kinds are not checked.
    let pattern_is_empty = matches!(
        &basic.pattern,
        Some(PatternNode::Pattern { value, .. }) if value.is_empty()
    );
    if pattern_is_empty
        && files_exclude.is_empty()
        && files_include.is_empty()
        && lang_exclude.is_empty()
    {
        return Some(
            "At least one of pattern and include/exclude patterns must be non-empty".to_owned(),
        );
    }
    None
}

/// `toLangFilters` (`job.go:744-756`).
fn to_lang_filters(aliases: &[String]) -> Vec<String> {
    let mut filters: Vec<String> = Vec::new();
    for alias in aliases {
        let language = get_language_by_name_or_alias(alias).to_owned();
        if !filters.contains(&language) {
            filters.push(language);
        }
    }
    filters
}

/// `Parameters.MaxResults`: `count:`, else the protocol default, else
/// `limits.DefaultMaxSearchResults` (30).
fn parameters_max_results(basic: &Basic, default_limit: i64) -> i64 {
    let count = basic.find_value(FIELD_COUNT);
    if let (false, Ok(value)) = (count.is_empty(), count.parse::<i64>()) {
        return value;
    }
    if default_limit != 0 {
        return default_limit;
    }
    30
}

/// `NewExhaustive` (`exhaustive_job.go:27-114`), validation and pager fields
/// only.
pub fn new_exhaustive(inputs: &Inputs) -> Result<Exhaustive, String> {
    if inputs.protocol != Protocol::Exhaustive {
        return Err("only works for exhaustive search inputs".to_owned());
    }

    if inputs.plan.len() != 1 {
        return Err(
            "Invalid query: Search Jobs does not support using AND/OR with filters (for example \"file:.go or file:.rs\"). Consider splitting your query into multiple subqueries and creating separate search jobs for each.".to_owned(),
        );
    }

    let basic = &inputs.plan[0];

    if !basic.last_non_negated_value(FIELD_SELECT).is_empty() {
        return Err("select: is not supported in Search Jobs".to_owned());
    }

    if let Some(predicate) = has_predicates(basic, FIELD_FILE) {
        return Err(format!(
            "file predicates are not supported. Got {predicate}"
        ));
    }

    if inputs.pattern_type == SearchType::Regex && basic.pattern_value() == Some(".*") {
        return Err("regex search with .* is not supported".to_owned());
    }

    let repo_options = to_repo_options(basic);
    let result_types =
        compute_result_types(basic, inputs.pattern_type, EXHAUSTIVE_DEFAULT_RESULT_TYPES);

    let unsupported = ResultTypes(result_types.0 & !EXHAUSTIVE_SUPPORTED_RESULT_TYPES.0);
    if unsupported.0 != 0 {
        return Err(format!(
            "your query contains the following type filters: {}. However Search Jobs only supports: {}.",
            result_types.as_string(),
            EXHAUSTIVE_SUPPORTED_RESULT_TYPES.as_string(),
        ));
    }

    let (child, only_cloned, skip_partitioning, limit) =
        if result_types.has(ResultTypes(ResultTypes::COMMIT.0 | ResultTypes::DIFF.0)) {
            let diff = result_types.has(ResultTypes::DIFF);
            let limit = parameters_max_results(basic, inputs.default_limit());
            (ExhaustiveChild::Commit { diff }, true, true, limit)
        } else if result_types.has(ResultTypes(ResultTypes::FILE.0 | ResultTypes::PATH.0)) {
            if let Some(err) = text_pattern_guard(basic, &inputs.features) {
                return Err(err);
            }
            let limit = parameters_max_results(basic, inputs.default_limit());
            (ExhaustiveChild::SearcherText, false, false, limit)
        } else {
            return Err(format!(
                "internal error: unsupported result types {}",
                result_types.as_string()
            ));
        };

    let contains_ref_globs = repo_options.contains_ref_globs();

    Ok(Exhaustive {
        repo_options,
        child,
        only_cloned,
        contains_ref_globs,
        skip_partitioning,
        limit,
    })
}

// ---------------------------------------------------------------------------
// Runtime seams (`job.RuntimeClients` / `repos.Resolver` / `job.Run`)
// ---------------------------------------------------------------------------

/// Classification needed by the searcher's error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeErrorKind {
    /// `repos.ErrNoResolvedRepos` / `*repos.MissingRepoRevsError`.
    ReposMissing,
    /// `*gitdomain.RevisionNotFoundError`.
    RevisionNotFound,
    Other,
}

/// A backend error with the classification the baseline does with
/// `errors.Is`/`errors.HasType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    pub kind: RuntimeErrorKind,
    pub message: String,
}

impl RuntimeError {
    pub fn other(message: impl Into<String>) -> RuntimeError {
        RuntimeError {
            kind: RuntimeErrorKind::Other,
            message: message.into(),
        }
    }

    pub fn repos_missing(message: impl Into<String>) -> RuntimeError {
        RuntimeError {
            kind: RuntimeErrorKind::ReposMissing,
            message: message.into(),
        }
    }

    pub fn revision_not_found(message: impl Into<String>) -> RuntimeError {
        RuntimeError {
            kind: RuntimeErrorKind::RevisionNotFound,
            message: message.into(),
        }
    }

    /// `isReposMissingError`.
    pub fn is_repos_missing(&self) -> bool {
        self.kind == RuntimeErrorKind::ReposMissing
    }
}

/// `repos.RepoRevSpecs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRevSpecs {
    pub repo: MinimalRepo,
    pub revs: Vec<RevisionSpecifier>,
}

/// The page sequence `Resolver.IterateRepoRevs` yields, plus the terminal
/// iterator error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoRevSpecsIterator {
    pub pages: Vec<Vec<RepoRevSpecs>>,
    pub error: Option<RuntimeError>,
}

/// `repos.Resolved` (`repos.go:48-67`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    pub repo_revs: Vec<RepositoryRevisions>,
    pub backends_missing: i64,
}

/// `Resolver.ResolveRevSpecs`' non-idiomatic result
/// (`repos.go:170-185`): a useful `Resolved` *and* a terminal error (for
/// example `*MissingRepoRevsError`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveOutcome {
    pub resolved: Resolved,
    pub error: Option<RuntimeError>,
}

/// One `streaming.SearchEvent`; the searcher writes `results` and ignores
/// `Stats` (matching `searcher.go:188-202`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchEvent {
    pub results: Vec<serde_json::Value>,
}

/// The materialised result of `job.Run`: the emitted events plus the terminal
/// error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchOutcome {
    pub events: Vec<SearchEvent>,
    pub error: Option<RuntimeError>,
}

/// `repos.Resolver`: `IterateRepoRevs`/`ResolveRevSpecs`, plus the DB lookup
/// `minimalRepo` performs.
pub trait RepoResolver: Send + Sync {
    fn iterate_repo_revs(&self, repo_options: &RepoOptions) -> RepoRevSpecsIterator;

    fn resolve_revision_specs(
        &self,
        repo_options: &RepoOptions,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> ResolveOutcome;

    /// `DB.Repos().ListMinimalRepos(IDs: [repo_id])`.
    fn list_minimal_repos(&self, repo_id: i32) -> Result<Vec<MinimalRepo>, String>;
}

/// `job.Run` for the constructed exhaustive job.
pub trait JobRunner: Send + Sync {
    fn run(&self, exhaustive: &Exhaustive, repo_revs: &RepositoryRevisions) -> SearchOutcome;
}

/// `job.RuntimeClients`, reduced to the two seams this slice calls.
#[derive(Clone)]
pub struct JobClients {
    pub resolver: Arc<dyn RepoResolver>,
    pub runner: Arc<dyn JobRunner>,
}

impl JobClients {
    pub fn new(resolver: Arc<dyn RepoResolver>, runner: Arc<dyn JobRunner>) -> JobClients {
        JobClients { resolver, runner }
    }

    /// Clients that fail every operation with an explicit not-ported error.
    pub fn not_ported() -> JobClients {
        JobClients {
            resolver: Arc::new(NotPortedResolver),
            runner: Arc::new(NotPortedRunner),
        }
    }
}

struct NotPortedResolver;

impl RepoResolver for NotPortedResolver {
    fn iterate_repo_revs(&self, _repo_options: &RepoOptions) -> RepoRevSpecsIterator {
        RepoRevSpecsIterator {
            pages: Vec::new(),
            error: Some(RuntimeError::other(NOT_PORTED_REPOS)),
        }
    }

    fn resolve_revision_specs(
        &self,
        _repo_options: &RepoOptions,
        _repo_rev_specs: &[RepoRevSpecs],
    ) -> ResolveOutcome {
        ResolveOutcome {
            resolved: Resolved::default(),
            error: Some(RuntimeError::other(NOT_PORTED_REPOS)),
        }
    }

    fn list_minimal_repos(&self, _repo_id: i32) -> Result<Vec<MinimalRepo>, String> {
        Err(NOT_PORTED_REPOS.to_owned())
    }
}

struct NotPortedRunner;

impl JobRunner for NotPortedRunner {
    fn run(&self, _exhaustive: &Exhaustive, _repo_revs: &RepositoryRevisions) -> SearchOutcome {
        SearchOutcome {
            events: Vec::new(),
            error: Some(RuntimeError::other(NOT_PORTED_SEARCH)),
        }
    }
}

const NOT_PORTED_REPOS: &str = "not ported: exhaustive search repository resolution";
const NOT_PORTED_SEARCH: &str = "not ported: exhaustive search job execution";

// ---------------------------------------------------------------------------
// SearchClient
// ---------------------------------------------------------------------------

/// `client.SearchClient`, reduced to `Plan` and `JobClients`.
pub trait SearchClient: Send + Sync {
    /// `client.Plan(ctx, version, patternType, query, mode, protocol,
    /// contextLines)`. Implementations own query parsing and return the
    /// planned [`Inputs`].
    #[allow(clippy::too_many_arguments)]
    fn plan(
        &self,
        version: &str,
        pattern_type: Option<&str>,
        query: &str,
        search_mode: SearchMode,
        protocol: Protocol,
        context_lines: Option<i32>,
    ) -> Result<Inputs, String>;

    /// `client.JobClients()`.
    fn job_clients(&self) -> JobClients;
}

/// A `SearchClient` whose real plan/runtime wiring is not ported.
pub struct NotPortedSearchClient;

impl SearchClient for NotPortedSearchClient {
    fn plan(
        &self,
        _version: &str,
        _pattern_type: Option<&str>,
        _query: &str,
        _search_mode: SearchMode,
        _protocol: Protocol,
        _context_lines: Option<i32>,
    ) -> Result<Inputs, String> {
        Err("not ported: search client Plan".to_owned())
    }

    fn job_clients(&self) -> JobClients {
        JobClients::not_ported()
    }
}

/// Production [`SearchClient`]: `client.Plan`'s query-parsing stage over the
/// injected job clients.
///
/// The baseline reaches user settings and feature flags through services.
/// This port keeps the exhaustive-search defaults (`context:` substitution
/// disabled, features false, no search-context-line override) and exposes the
/// remaining fields for the parent that assembles the worker.
pub struct PlanSearchClient {
    clients: JobClients,
    user_settings: UserSettings,
    features: Features,
    default_context_lines: i32,
}

impl PlanSearchClient {
    pub fn new(clients: JobClients) -> PlanSearchClient {
        PlanSearchClient {
            clients,
            user_settings: UserSettings::default(),
            features: Features::default(),
            default_context_lines: 1,
        }
    }

    pub fn with_settings(
        mut self,
        user_settings: UserSettings,
        features: Features,
        default_context_lines: i32,
    ) -> PlanSearchClient {
        self.user_settings = user_settings;
        self.features = features;
        self.default_context_lines = default_context_lines;
        self
    }
}

impl SearchClient for PlanSearchClient {
    fn plan(
        &self,
        version: &str,
        pattern_type: Option<&str>,
        query: &str,
        _search_mode: SearchMode,
        protocol: Protocol,
        context_lines: Option<i32>,
    ) -> Result<Inputs, String> {
        // `searchClient.Plan` (`client.go:83-161`) without the services this
        // crate cannot reach.
        let search_type = detect_search_type(version, pattern_type)?;
        let search_type = override_search_type(query, search_type);

        let plan = sgq::pipeline_query(query, search_type)
            .map_err(|err| format!("invalid query {}: {err}", go_quote(query)))?;

        let plan = if search_type == QuerySearchType::Keyword {
            map_plan(plan, &mut |basic| experimental_phrase_boost(query, basic))
        } else {
            plan
        };

        Ok(Inputs {
            plan: convert_plan(&plan),
            pattern_type: convert_search_type(search_type),
            user_settings: self.user_settings,
            protocol,
            features: self.features,
            context_lines: context_lines.unwrap_or(self.default_context_lines),
            sub_repo_enabled: false,
        })
    }

    fn job_clients(&self) -> JobClients {
        self.clients.clone()
    }
}

/// `SearchTypeFromString` (`client.go:236-256`).
fn query_search_type_from_string(pattern_type: &str) -> Result<QuerySearchType, String> {
    match pattern_type {
        "standard" => Ok(QuerySearchType::Standard),
        "literal" => Ok(QuerySearchType::Literal),
        "regexp" => Ok(QuerySearchType::Regex),
        "structural" => Ok(QuerySearchType::Structural),
        "codycontext" => Ok(QuerySearchType::CodyContext),
        "keyword" => Ok(QuerySearchType::Keyword),
        "lucky" => Ok(QuerySearchType::Standard),
        other => Err(format!("unrecognized patternType {}", go_quote(other))),
    }
}

/// `detectSearchType` (`client.go:262-279`).
fn detect_search_type(
    version: &str,
    pattern_type: Option<&str>,
) -> Result<QuerySearchType, String> {
    if let Some(pattern_type) = pattern_type {
        return query_search_type_from_string(pattern_type);
    }
    match version {
        "V1" => Ok(QuerySearchType::Regex),
        "V2" => Ok(QuerySearchType::Literal),
        "V3" => Ok(QuerySearchType::Standard),
        other => Err(format!(
            "unrecognized version: want \"V1\", \"V2\", or \"V3\", got {}",
            go_quote(other)
        )),
    }
}

/// `overrideSearchType` (`client.go:281-309`): the last non-empty
/// `patterntype:` filter wins; unknown values are ignored.
fn override_search_type(input: &str, search_type: QuerySearchType) -> QuerySearchType {
    let Ok(query) = sgq::parse(input, QuerySearchType::Literal) else {
        return search_type;
    };
    let query = lowercase_field_names(&query);
    let mut search_type = search_type;
    visit_field(
        &query,
        FIELD_PATTERN_TYPE,
        &mut |value, _negated, _annotation| {
            search_type = match value {
                "standard" => QuerySearchType::Standard,
                "regex" | "regexp" => QuerySearchType::Regex,
                "literal" => QuerySearchType::Literal,
                "structural" => QuerySearchType::Structural,
                "codycontext" => QuerySearchType::CodyContext,
                "keyword" => QuerySearchType::Keyword,
                "lucky" => QuerySearchType::Standard,
                _ => search_type,
            };
        },
    );
    search_type
}

/// `ExperimentalPhraseBoost` (`transformer.go:685-744`). Duplicated from
/// `sg-service-frontend::search_client` because the worker cannot depend on
/// the frontend crate.
fn experimental_phrase_boost(original_query: &str, basic: sgq::Basic) -> sgq::Basic {
    let Some(pattern) = basic.pattern.as_ref() else {
        return basic;
    };

    for parameter in &basic.parameters.0 {
        if parameter.field == QUERY_FIELD_TYPE
            && !(parameter.value == "file" || parameter.value == "path")
        {
            return basic;
        }
    }

    match pattern {
        QueryNode::Pattern(pattern) => {
            if !pattern.annotation.labels.is_set(QueryLabels::QUOTED)
                || pattern.negated
                || pattern.annotation.labels.is_set(QueryLabels::REGEXP)
            {
                return basic;
            }
        }
        QueryNode::Operator(operator) => {
            if operator.kind != QueryOperatorKind::And {
                return basic;
            }
            for child in &operator.operands {
                match child {
                    QueryNode::Pattern(child) => {
                        if child.negated || child.annotation.labels.is_set(QueryLabels::REGEXP) {
                            return basic;
                        }
                    }
                    _ => return basic,
                }
            }
        }
        QueryNode::Parameter(_) => return basic,
    }

    let terms: Vec<&str> = original_query
        .split_whitespace()
        .filter(|term| !term.contains(':'))
        .collect();
    let phrase = terms.join(" ");

    let boosted = QueryNode::Operator(QueryOperator {
        kind: QueryOperatorKind::Or,
        operands: vec![
            QueryNode::Pattern(QueryPattern {
                value: phrase,
                negated: false,
                annotation: QueryAnnotation {
                    labels: QueryLabels::BOOST | QueryLabels::LITERAL | QueryLabels::STANDARD,
                    range: Default::default(),
                },
            }),
            pattern.clone(),
        ],
        annotation: QueryAnnotation::default(),
    });

    sgq::Basic::new(basic.parameters, Some(boosted))
}

fn convert_search_type(search_type: QuerySearchType) -> SearchType {
    match search_type {
        QuerySearchType::Regex => SearchType::Regex,
        QuerySearchType::Literal => SearchType::Literal,
        QuerySearchType::Structural => SearchType::Structural,
        QuerySearchType::Standard => SearchType::Standard,
        QuerySearchType::CodyContext => SearchType::CodyContext,
        QuerySearchType::Keyword => SearchType::Keyword,
    }
}

/// `Plan` -> this module's reduced model. The reduced model was already
/// diff-verified against the baseline by M9.8hv; this keeps the same shape.
fn convert_plan(plan: &sgq::Plan) -> Plan {
    plan.iter()
        .map(|basic| Basic {
            parameters: basic
                .parameters
                .0
                .iter()
                .map(|parameter| Parameter {
                    field: parameter.field.clone(),
                    value: parameter.value.clone(),
                    negated: parameter.negated,
                    predicate: sgq::parser::scan_predicate(
                        &parameter.field,
                        parameter.value.as_bytes(),
                    )
                    .map(|(predicate, _)| predicate),
                    is_predicate: parameter
                        .annotation
                        .labels
                        .is_set(QueryLabels::IS_PREDICATE),
                })
                .collect(),
            pattern: basic.pattern.as_ref().map(|node| match node {
                QueryNode::Pattern(pattern) => PatternNode::Pattern {
                    value: pattern.value.clone(),
                    content: pattern.annotation.labels.is_set(QueryLabels::IS_CONTENT),
                },
                _ => PatternNode::Other,
            }),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// User context (`actor.FromContext`)
// ---------------------------------------------------------------------------

/// The baseline compares `actor.FromContext(ctx).UID` with the requested user.
/// Rust has no context actor, so the actor identity is injected.
pub trait UserContext: Send + Sync {
    fn actor_uid(&self) -> Option<i32>;
}

/// No actor in context (the default; mirrors an unauthenticated request).
pub struct NoActor;

impl UserContext for NoActor {
    fn actor_uid(&self) -> Option<i32> {
        None
    }
}

/// `isSameUser` (`search.go:193-201`).
pub fn is_same_user(user_context: &dyn UserContext, user_id: i32) -> Result<(), String> {
    if user_id == 0 {
        return Err("exhaustive search must be done on behalf of an authenticated user".to_owned());
    }
    match user_context.actor_uid() {
        Some(uid) if uid == user_id => Ok(()),
        _ => Err(format!("exhaustive search must be run as user {user_id}")),
    }
}

// ---------------------------------------------------------------------------
// FromSearchClient / searchQuery
// ---------------------------------------------------------------------------

/// `UserContext` for worker handlers. The baseline handlers set
/// `actor.WithActor(ctx, actor.FromUser(userID))` with the record's initiator
/// before calling `NewSearch(ctx, userID, ...)`, so the request user is always
/// the actor.
pub struct RequestUserContext {
    uid: i32,
}

impl UserContext for RequestUserContext {
    fn actor_uid(&self) -> Option<i32> {
        Some(self.uid)
    }
}

enum UserContextSource {
    Fixed(Arc<dyn UserContext>),
    RequestUser,
}

/// `service.FromSearchClient` (`searcher.go:25-63`).
pub struct FromSearchClient {
    client: Arc<dyn SearchClient>,
    user_context: UserContextSource,
}

impl FromSearchClient {
    pub fn new(
        client: Arc<dyn SearchClient>,
        user_context: Arc<dyn UserContext>,
    ) -> FromSearchClient {
        FromSearchClient {
            client,
            user_context: UserContextSource::Fixed(user_context),
        }
    }

    /// Production wiring: real `client.Plan` over `clients`, with the actor
    /// taken from the request (see [`RequestUserContext`]).
    pub fn production(clients: JobClients) -> FromSearchClient {
        FromSearchClient {
            client: Arc::new(PlanSearchClient::new(clients)),
            user_context: UserContextSource::RequestUser,
        }
    }

    /// An explicit not-ported client (tests/seams).
    pub fn not_ported() -> FromSearchClient {
        FromSearchClient {
            client: Arc::new(NotPortedSearchClient),
            user_context: UserContextSource::Fixed(Arc::new(NoActor)),
        }
    }

    fn user_context(&self, user_id: i32) -> Arc<dyn UserContext> {
        match &self.user_context {
            UserContextSource::Fixed(user_context) => Arc::clone(user_context),
            UserContextSource::RequestUser => Arc::new(RequestUserContext { uid: user_id }),
        }
    }
}

#[async_trait]
impl NewSearcher for FromSearchClient {
    async fn new_search(&self, user_id: i32, query: &str) -> Result<Box<dyn SearchQuery>, String> {
        let user_context = self.user_context(user_id);
        is_same_user(user_context.as_ref(), user_id)?;

        // We run queries on searcher only which makes it easier to control
        // limits. Low latency is not a priority of Search Jobs.
        let query = format!("index:no {query}");

        if query.to_lowercase().contains("patterntype:structural") {
            return Err("Structural search is not supported in Search Jobs".to_owned());
        }

        let inputs = self.client.plan(
            "V3",
            None,
            &query,
            SearchMode::Precise,
            Protocol::Exhaustive,
            Some(0),
        )?;

        let exhaustive = new_exhaustive(&inputs)?;

        Ok(Box::new(SearcherQuery {
            user_id,
            exhaustive,
            clients: self.client.job_clients(),
            user_context,
        }))
    }
}

/// `service.searchQuery` (`searcher.go:65-231`).
struct SearcherQuery {
    user_id: i32,
    exhaustive: Exhaustive,
    clients: JobClients,
    user_context: Arc<dyn UserContext>,
}

impl SearcherQuery {
    fn minimal_repo(&self, repo_id: i32) -> Result<MinimalRepo, String> {
        let repos = self.clients.resolver.list_minimal_repos(repo_id)?;
        if repos.len() != 1 {
            return Err(format!(
                "looking up repo {repo_id} found {} entries",
                repos.len()
            ));
        }
        Ok(repos[0].clone())
    }

    /// `toRepoRevSpecs` (`searcher.go:145-164`).
    fn to_repo_rev_specs(
        &self,
        repo_rev_spec: &ex::RepositoryRevSpecs,
    ) -> Result<RepoRevSpecs, String> {
        let repo = self.minimal_repo(repo_rev_spec.repository)?;
        let mut revs = Vec::new();
        for spec in ex::revision_specifiers_get(&repo_rev_spec.revision_specifiers) {
            revs.push(parse_revision_specifier_checked(&spec)?);
        }
        Ok(RepoRevSpecs { repo, revs })
    }
}

/// `types.RepositoryRevSpecs.String()` (`exhaustive_search.go:50-52`).
fn repository_rev_specs_display(spec: &ex::RepositoryRevSpecs) -> String {
    format!(
        "RepositoryRevSpec{{{}@{}}}",
        spec.repository, spec.revision_specifiers
    )
}

#[async_trait]
impl SearchQuery for SearcherQuery {
    async fn repository_rev_specs(&self) -> Result<Vec<ex::RepositoryRevSpecs>, String> {
        // Baseline quirk: the error iterator built on a user mismatch is not
        // returned (`searcher.go:72-76`), so iteration proceeds regardless.
        let _ = is_same_user(self.user_context.as_ref(), self.user_id);

        let iterator = self
            .exhaustive
            .repository_rev_specs(self.clients.resolver.as_ref());

        let mut specs = Vec::new();
        for page in iterator.pages {
            for repo_rev in page {
                let mut revspecs = Vec::new();
                for rev in &repo_rev.revs {
                    let rev_str = rev.to_string_value();
                    // Avoid storing empty strings: our DB expects non-empty
                    // strings and HEAD is easier to read.
                    revspecs.push(if rev_str.is_empty() {
                        "HEAD".to_owned()
                    } else {
                        rev_str
                    });
                }
                specs.push(ex::RepositoryRevSpecs {
                    repository: repo_rev.repo.id as i32,
                    revision_specifiers: ex::revision_specifiers_join(&revspecs),
                });
            }
        }

        if let Some(err) = iterator.error {
            if err.is_repos_missing() {
                // Not an error for search jobs: no alerts exist yet.
                return Ok(specs);
            }
            return Err(err.message);
        }
        Ok(specs)
    }

    async fn resolve_repository_rev_spec(
        &self,
        repo_rev_spec: &ex::RepositoryRevSpecs,
    ) -> Result<Vec<ex::RepositoryRevision>, String> {
        is_same_user(self.user_context.as_ref(), self.user_id)?;

        let repo_pager_repo_rev_spec = self.to_repo_rev_specs(repo_rev_spec)?;

        let outcome = self.exhaustive.resolve_repository_rev_spec(
            self.clients.resolver.as_ref(),
            &[repo_pager_repo_rev_spec],
        );
        // Repos-missing is not an error: we just search nothing.
        if let Some(err) = outcome.error {
            if !err.is_repos_missing() {
                return Err(err.message);
            }
        }
        let page = outcome.resolved;

        if page.backends_missing > 0 {
            return Err("job needs to be retried, some backends are down".to_owned());
        }

        let mut repo_revs = Vec::new();
        for repo_rev in &page.repo_revs {
            if repo_rev.repo.id as i32 != repo_rev_spec.repository {
                return Err(format!(
                    "ResolveRepositoryRevSpec returned a different repo ({}) to the input {}",
                    repo_rev.repo.id,
                    repository_rev_specs_display(repo_rev_spec),
                ));
            }
            for rev in &repo_rev.revs {
                repo_revs.push(ex::RepositoryRevision {
                    repository_rev_specs: repo_rev_spec.clone(),
                    revision: rev.clone(),
                });
            }
        }
        Ok(repo_revs)
    }

    async fn search(
        &self,
        repo_rev: &ex::RepositoryRevision,
        match_writer: &dyn MatchWriter,
    ) -> Result<(), String> {
        is_same_user(self.user_context.as_ref(), self.user_id)?;

        let repo = self.minimal_repo(repo_rev.repository_rev_specs.repository)?;

        let repo_revs = RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id: repo.id,
                name: repo.name,
            },
            revs: vec![repo_rev.revision.clone()],
        };

        let outcome = self.clients.runner.run(&self.exhaustive, &repo_revs);

        let mut write_error = None;
        'events: for event in outcome.events {
            for result in event.results {
                if let Err(err) = match_writer.write(result) {
                    write_error = Some(err);
                    break 'events;
                }
            }
        }
        if let Some(err) = write_error {
            return Err(err);
        }

        // An empty repository we treat as success: searching HEAD has not yet
        // validated the commit exists.
        if let Some(err) = outcome.error {
            if repo_rev.revision == "HEAD" && err.kind == RuntimeErrorKind::RevisionNotFound {
                return Ok(());
            }
            return Err(err.message);
        }
        Ok(())
    }
}

impl Exhaustive {
    /// `Exhaustive.RepositoryRevSpecs` (`exhaustive_job.go:136-138`).
    pub fn repository_rev_specs(&self, resolver: &dyn RepoResolver) -> RepoRevSpecsIterator {
        resolver.iterate_repo_revs(&self.repo_options)
    }

    /// `Exhaustive.ResolveRepositoryRevSpec` (`exhaustive_job.go:141-143`).
    pub fn resolve_repository_rev_spec(
        &self,
        resolver: &dyn RepoResolver,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> ResolveOutcome {
        resolver.resolve_revision_specs(&self.repo_options, repo_rev_specs)
    }

    /// `Exhaustive.Job` (`exhaustive_job.go:127-133`): the single
    /// `RepositoryRevisions` becomes the unindexed repo set.
    pub fn job(&self, repo_revs: &RepositoryRevisions) -> (ExhaustiveChild, RepositoryRevisions) {
        (self.child, repo_revs.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parameter(field: &str, value: &str) -> Parameter {
        Parameter {
            field: field.to_owned(),
            value: value.to_owned(),
            negated: false,
            predicate: None,
            is_predicate: false,
        }
    }

    fn negated(field: &str, value: &str) -> Parameter {
        Parameter {
            negated: true,
            ..parameter(field, value)
        }
    }

    fn pattern(value: &str, content: bool) -> PatternNode {
        PatternNode::Pattern {
            value: value.to_owned(),
            content,
        }
    }

    fn inputs(plan: Vec<Basic>, pattern_type: SearchType) -> Inputs {
        Inputs {
            plan,
            pattern_type,
            user_settings: UserSettings::default(),
            protocol: Protocol::Exhaustive,
            features: Features::default(),
            context_lines: 1,
            sub_repo_enabled: false,
        }
    }

    #[test]
    fn rejects_wrong_protocol() {
        let mut inputs = inputs(vec![Basic::default()], SearchType::Standard);
        inputs.protocol = Protocol::Streaming;
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err(),
            "only works for exhaustive search inputs"
        );
    }

    #[test]
    fn rejects_select() {
        let basic = Basic {
            parameters: vec![parameter(FIELD_SELECT, "file.owners")],
            pattern: Some(pattern("foo", true)),
        };
        assert_eq!(
            new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap_err(),
            "select: is not supported in Search Jobs"
        );
    }

    #[test]
    fn rejects_file_predicates() {
        let basic = Basic {
            parameters: vec![Parameter {
                predicate: Some("has.content(content)".to_owned()),
                is_predicate: true,
                ..parameter(FIELD_FILE, "has.content(content)")
            }],
            pattern: Some(pattern("foo", true)),
        };
        assert_eq!(
            new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap_err(),
            "file predicates are not supported. Got has.content(content)"
        );
    }

    #[test]
    fn rejects_regex_dot_star() {
        let basic = Basic {
            parameters: vec![parameter(FIELD_TYPE, "file")],
            pattern: Some(pattern(".*", true)),
        };
        assert_eq!(
            new_exhaustive(&inputs(vec![basic], SearchType::Regex)).unwrap_err(),
            "regex search with .* is not supported"
        );
    }

    #[test]
    fn rejects_unsupported_result_types() {
        let basic = Basic {
            parameters: vec![parameter(FIELD_TYPE, "repo")],
            pattern: Some(pattern("foo", true)),
        };
        assert_eq!(
            new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap_err(),
            "your query contains the following type filters: repo. However Search Jobs only supports: commit, diff, file, path."
        );
    }

    #[test]
    fn builds_text_job() {
        let basic = Basic {
            parameters: vec![parameter(FIELD_TYPE, "file")],
            pattern: Some(pattern("foo", true)),
        };
        let exhaustive = new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap();
        assert_eq!(exhaustive.child, ExhaustiveChild::SearcherText);
        assert!(!exhaustive.only_cloned);
        assert!(!exhaustive.skip_partitioning);
    }

    #[test]
    fn builds_commit_job() {
        let basic = Basic {
            parameters: vec![
                parameter(FIELD_TYPE, "commit"),
                parameter("author", "alice"),
            ],
            pattern: Some(pattern("foo", true)),
        };
        let exhaustive = new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap();
        assert_eq!(exhaustive.child, ExhaustiveChild::Commit { diff: false });
        assert!(exhaustive.only_cloned);
        assert!(exhaustive.skip_partitioning);
        assert_eq!(exhaustive.limit, 1_000_000);
    }

    #[test]
    fn records_repo_filters_and_ref_globs() {
        let basic = Basic {
            parameters: vec![
                parameter(FIELD_REPO, "repo1@*refs/heads/*"),
                parameter(FIELD_TYPE, "file"),
            ],
            pattern: Some(pattern("foo", true)),
        };
        let exhaustive = new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap();
        assert_eq!(exhaustive.repo_options.repo_filters.len(), 1);
        assert_eq!(exhaustive.repo_options.repo_filters[0].repo, "repo1");
        assert!(exhaustive.contains_ref_globs);
    }

    #[test]
    fn negated_repo_moves_to_minus_filters() {
        let basic = Basic {
            parameters: vec![negated(FIELD_REPO, "repo1"), parameter(FIELD_TYPE, "file")],
            pattern: Some(pattern("foo", true)),
        };
        let exhaustive = new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap();
        assert!(exhaustive.repo_options.repo_filters.is_empty());
        assert_eq!(
            exhaustive.repo_options.minus_repo_filters,
            vec!["repo1".to_owned()]
        );
    }

    #[test]
    fn revision_specifier_round_trips() {
        assert_eq!(parse_revision_specifier("main").to_string_value(), "main");
        assert_eq!(
            parse_revision_specifier("*refs/heads/*").to_string_value(),
            "*refs/heads/*"
        );
        assert_eq!(
            parse_revision_specifier("*!refs/heads/dev1").to_string_value(),
            "*!refs/heads/dev1"
        );
        assert!(parse_revision_specifier("*x").has_ref_glob());
    }

    #[test]
    fn empty_text_query_rejected_by_pattern_guard() {
        let basic = Basic {
            parameters: vec![parameter(FIELD_TYPE, "file")],
            pattern: Some(pattern("", true)),
        };
        assert_eq!(
            new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap_err(),
            "At least one of pattern and include/exclude patterns must be non-empty"
        );
    }

    #[test]
    fn empty_text_query_allowed_when_file_filter_present() {
        let basic = Basic {
            parameters: vec![
                parameter(FIELD_TYPE, "file"),
                parameter(FIELD_FILE, "pkg/.*"),
            ],
            pattern: Some(pattern("", true)),
        };
        let exhaustive = new_exhaustive(&inputs(vec![basic], SearchType::Standard)).unwrap();
        assert_eq!(exhaustive.child, ExhaustiveChild::SearcherText);
    }
}
