#![forbid(unsafe_code)]

//! `internal/search/job/jobutil/job.go` job tree construction.
//!
//! This ports the construction entry points of the baseline:
//!
//! * [`new_plan_job`] (`NewPlanJob`), [`new_basic_job`] (`NewBasicJob`),
//!   [`new_text_search_job`] (`NewTextSearchJob`), [`new_flat_job`]
//!   (`NewFlatJob`);
//! * the `jobBuilder` zoekt constructors (`newZoektGlobalSearch` /
//!   `newZoektSearch`);
//! * [`to_and_job`], [`to_or_job`], [`to_pattern_expression_job`],
//!   [`to_flat_jobs`];
//! * the structural `orderRacingJobs` rewriting.
//!
//! The result is an enumerable [`JobNode`] tree implementing [`SearchJob`]
//! (Name/Children/Attributes). [`SearchJob::run`] executes the tree: the
//! wrappers reuse the ported `search_jobutil_combinators` /
//! `search_jobutil_expression` semantics, and the zoekt text/symbol leaves
//! delegate to the ported jobs in [`crate::search_job`]. Searcher, commit,
//! structural and repo-pager leaves still return an explicit per-kind
//! "not ported" error. No GraphQL/HTTP/gRPC/DB is touched.
//!
//! See `docs/overviews/m9_8ho_jobutil_tree_overview.md` and
//! `docs/overviews/m9_8hw_jobutil_tree_run_overview.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use regex::Regex;

use sg_proto::gitserver as pb;
use sg_search_query::basic::{plan_to_q, q_string, q_string_value, Basic, Plan};
use sg_search_query::fields::{
    FIELD_ARCHIVED, FIELD_CONTEXT, FIELD_FILE, FIELD_FORK, FIELD_REPO, FIELD_SELECT,
};
use sg_search_query::labels::Labels;
use sg_search_query::node::{visit_field, visit_parameter, Node, Operator, OperatorKind, Pattern};
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::{init, run};
use sg_search_query::predicate::parse_as_predicate;
use sg_search_query::validate::{parse_yes_no_only, YesNoOnly};
use sg_search_query::{mapper, string_human};
use sg_zoekt::global_query::{default_global_query_scope, GlobalZoektQuery};
use sg_zoekt::indexed_repo_revs::{partition_repos, IndexedRepoRevs, RepositoryRevisions};
use sg_zoekt::languages::lang_to_file_regexp;
use sg_zoekt::query::{self as zoekt_query, IndexedRequestType};

use crate::codycontext::{self, CodyContextJobParts, CodyContextOptions, CodyFileMatcher};
use crate::search_client::{Features, Protocol, SearchMode};
use crate::search_commit_job::CodeMonitorHookHandle;
use crate::search_contexts::{
    parse_repository_revisions, ParsedRepoFilter, RepoVisibility, RequestContext,
};
use crate::search_job::{
    GlobalTextSearchJob, JobContext, JobError, RepoSubsetTextSearchJob, RuntimeClients, SearchJob,
    SymbolSearchJob, Verbosity,
};
use crate::search_jobutil_combinators as comb;
use crate::search_jobutil_combinators::format_go_duration;
use crate::search_jobutil_expression as expr;
use crate::search_jobutil_filters::{
    patterns_in_tree, ContainsPatternError, ContainsPatternStage, PatternNode as FilterPatternNode,
};
use crate::search_jobutil_job_builders as jb;
use crate::search_jobutil_repos::ResolvedRepos;
use crate::search_stream::convert::Match;
use crate::search_stream::progress::{RepoStatusMap, SearchEvent, Stats};
use crate::search_stream::zoekt_search::{SelectPath, ZoektParameters};

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Injection point for `codycontext.NewSearchJob`.
pub type CodyContextHook = dyn Fn(&[Basic], &TreeInputs) -> Result<JobNode, JobError>;

/// The `search.Inputs` subset the construction entry points read.
pub struct TreeInputs {
    pub protocol: Protocol,
    pub pattern_type: SearchType,
    pub user_settings: jb::UserSettings,
    pub features: Features,
    pub context_lines: i32,
    pub sanitize_search_patterns: Vec<String>,
    pub on_sourcegraph_dot_com: bool,
    /// `Inputs.Query` rendered by `AlertJob` attributes.
    pub original_query: String,
    /// `actor.FromContext(ctx)`: carried into `search.RepoOptions` for
    /// `ResolveSearchContextSpec` and into `subRepoPermsFilterJob`.
    pub request_context: RequestContext,
    /// `authz.SubRepoEnabled(authz.DefaultSubRepoPermsChecker)`.
    pub sub_repo_enabled: bool,
    /// `codycontext.NewSearchJob`; `None` means "not ported".
    pub cody_context_hook: Option<Box<CodyContextHook>>,
    /// `Inputs.Features` fields `codycontext` reads
    /// (`CodyContextCodeCount`, `CodyContextTextCount`, `CodyFileMatcher`).
    /// `None` models `Inputs.Features == nil`.
    pub cody_context: Option<CodyContextOptions>,
}

impl TreeInputs {
    fn default_limit(&self) -> i64 {
        default_limit(self.protocol)
    }
}

/// `Inputs.DefaultLimit` (`internal/search/types.go:50-63`).
pub fn default_limit(protocol: Protocol) -> i64 {
    match protocol {
        Protocol::Streaming => 10_000,
        Protocol::Batch => jb::DEFAULT_MAX_SEARCH_RESULTS,
        Protocol::Exhaustive => 1_000_000,
    }
}

// ---------------------------------------------------------------------------
// Full `search.RepoOptions` model
// ---------------------------------------------------------------------------

/// `search.RepoOptions` as read by the construction code and its attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeRepoOptions {
    pub repo_filters: Vec<ParsedRepoFilter>,
    pub minus_repo_filters: Vec<String>,
    pub description_patterns: Vec<String>,
    pub case_sensitive_repo_filters: bool,
    pub search_context_spec: String,
    pub commit_after: Option<jb::RepoHasCommitAfterArgs>,
    pub visibility: RepoVisibility,
    pub limit: i64,
    pub cursors: usize,
    pub use_index: YesNoOnly,
    pub has_file_content: Vec<sg_search_query::basic::RepoHasFileContentArgs>,
    pub has_kvps: Vec<jb::RepoKvpFilter>,
    pub has_topics: Vec<jb::RepoHasTopic>,
    pub fork_set: bool,
    pub no_forks: bool,
    pub only_forks: bool,
    pub only_cloned: bool,
    pub archived_set: bool,
    pub no_archived: bool,
    pub only_archived: bool,
    /// `actor.FromContext(ctx)` for `ResolveSearchContextSpec`.
    pub request_context: RequestContext,
}

impl Default for TreeRepoOptions {
    fn default() -> Self {
        // Mirrors `search.RepoOptions{}` for the fields `addPatternAsRepoFilter`
        // returns on rejection; the rejected options are never turned into a
        // job, so this value is not observable in attributes.
        TreeRepoOptions {
            repo_filters: Vec::new(),
            minus_repo_filters: Vec::new(),
            description_patterns: Vec::new(),
            case_sensitive_repo_filters: false,
            search_context_spec: String::new(),
            commit_after: None,
            visibility: RepoVisibility::Any,
            limit: 0,
            cursors: 0,
            use_index: YesNoOnly::Yes,
            has_file_content: Vec::new(),
            has_kvps: Vec::new(),
            has_topics: Vec::new(),
            fork_set: false,
            no_forks: false,
            only_forks: false,
            only_cloned: false,
            archived_set: false,
            no_archived: false,
            only_archived: false,
            request_context: RequestContext::anonymous(),
        }
    }
}

impl TreeRepoOptions {
    /// Fill from the `toRepoOptions` result. `case_sensitive_repo_filters`,
    /// `limit` and `only_cloned` stay at their zero values, as in the baseline.
    pub fn from_job_builders(options: &jb::RepoOptions) -> Self {
        TreeRepoOptions {
            repo_filters: options.repo_filters.clone(),
            minus_repo_filters: options.minus_repo_filters.clone(),
            description_patterns: options.description_patterns.clone(),
            case_sensitive_repo_filters: false,
            search_context_spec: options.search_context_spec.clone(),
            commit_after: options.commit_after.clone(),
            visibility: options.visibility,
            limit: 0,
            cursors: 0,
            use_index: options.use_index,
            has_file_content: options.has_file_content.clone(),
            has_kvps: options.has_kvps.clone(),
            has_topics: options.has_topics.clone(),
            fork_set: options.fork_set,
            no_forks: options.no_forks,
            only_forks: options.only_forks,
            only_cloned: false,
            archived_set: options.archived_set,
            no_archived: options.no_archived,
            only_archived: options.only_archived,
            request_context: RequestContext::anonymous(),
        }
    }

    /// `RepoOptions.Attributes` (`internal/search/types.go:461-560`).
    pub fn attributes(&self) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if !self.repo_filters.is_empty() {
            res.push((
                "repoFilters".to_owned(),
                parsed_repo_filters_string(&self.repo_filters),
            ));
        }
        if !self.minus_repo_filters.is_empty() {
            res.push((
                "minusRepoFilters".to_owned(),
                string_slice(self.minus_repo_filters.iter().map(String::as_str)),
            ));
        }
        if !self.description_patterns.is_empty() {
            res.push((
                "descriptionPatterns".to_owned(),
                string_slice(self.description_patterns.iter().map(String::as_str)),
            ));
        }
        if self.case_sensitive_repo_filters {
            res.push(("caseSensitiveRepoFilters".to_owned(), "true".to_owned()));
        }
        if !self.search_context_spec.is_empty() {
            res.push((
                "searchContextSpec".to_owned(),
                self.search_context_spec.clone(),
            ));
        }
        if let Some(commit_after) = &self.commit_after {
            res.push(("commitAfter.time".to_owned(), commit_after.time_ref.clone()));
            res.push((
                "commitAfter.negated".to_owned(),
                commit_after.negated.to_string(),
            ));
        }
        if self.visibility != RepoVisibility::Any {
            res.push((
                "visibility".to_owned(),
                visibility_str(self.visibility).to_owned(),
            ));
        }
        if self.limit > 0 {
            res.push(("limit".to_owned(), self.limit.to_string()));
        }
        // `Cursors` is never set by `toRepoOptions`; the zero value is omitted.
        if self.use_index != YesNoOnly::Yes {
            res.push((
                "useIndex".to_owned(),
                yes_no_only_str(self.use_index).to_owned(),
            ));
        }
        for (index, arg) in self.has_file_content.iter().enumerate() {
            let mut nondefault = Vec::new();
            if !arg.path.is_empty() {
                nondefault.push(("path".to_owned(), arg.path.clone()));
            }
            if !arg.content.is_empty() {
                nondefault.push(("content".to_owned(), arg.content.clone()));
            }
            if arg.negated {
                nondefault.push(("negated".to_owned(), "true".to_owned()));
            }
            push_scoped(&mut res, &format!("hasFileContent[{index}]"), nondefault);
        }
        for (index, arg) in self.has_kvps.iter().enumerate() {
            let mut nondefault = Vec::new();
            if !arg.key.is_empty() {
                nondefault.push(("key".to_owned(), arg.key.clone()));
            }
            if let Some(value) = &arg.value {
                nondefault.push(("value".to_owned(), value.clone()));
            }
            if arg.negated {
                nondefault.push(("negated".to_owned(), "true".to_owned()));
            }
            push_scoped(&mut res, &format!("hasKVPs[{index}]"), nondefault);
        }
        for (index, arg) in self.has_topics.iter().enumerate() {
            let mut nondefault = Vec::new();
            if !arg.topic.is_empty() {
                nondefault.push(("topic".to_owned(), arg.topic.clone()));
            }
            if arg.negated {
                nondefault.push(("negated".to_owned(), "true".to_owned()));
            }
            push_scoped(&mut res, &format!("hasTopics[{index}]"), nondefault);
        }
        if self.fork_set {
            res.push(("forkSet".to_owned(), "true".to_owned()));
        }
        if !self.no_forks {
            res.push(("noForks".to_owned(), self.no_forks.to_string()));
        }
        if self.only_forks {
            res.push(("onlyForks".to_owned(), "true".to_owned()));
        }
        if self.only_cloned {
            res.push(("onlyCloned".to_owned(), "true".to_owned()));
        }
        if self.archived_set {
            res.push(("archivedSet".to_owned(), "true".to_owned()));
        }
        if !self.no_archived {
            res.push(("noArchived".to_owned(), self.no_archived.to_string()));
        }
        if self.only_archived {
            res.push(("onlyArchived".to_owned(), "true".to_owned()));
        }
        res
    }

    fn scoped_attributes(&self) -> Vec<(String, String)> {
        let mut res = Vec::new();
        push_scoped(&mut res, "repoOpts", self.attributes());
        res
    }
}

fn push_scoped(res: &mut Vec<(String, String)>, scope: &str, kvs: Vec<(String, String)>) {
    for (key, value) in kvs {
        res.push((format!("{scope}.{key}"), value));
    }
}

fn visibility_str(value: RepoVisibility) -> &'static str {
    // `query.RepoVisibility` is a string type whose values are capitalized.
    match value {
        RepoVisibility::Any => "Any",
        RepoVisibility::Private => "Private",
        RepoVisibility::Public => "Public",
    }
}

fn yes_no_only_str(value: YesNoOnly) -> &'static str {
    match value {
        YesNoOnly::Yes => "yes",
        YesNoOnly::No => "no",
        YesNoOnly::Only => "only",
        YesNoOnly::Invalid => "invalid",
    }
}

fn string_slice<'a>(values: impl Iterator<Item = &'a str>) -> String {
    // OTel's `attribute.Value.Emit` renders STRINGSLICE as `json.Marshal` of
    // the slice (`["a","b"]`), which is what the baseline job attributes use.
    let values: Vec<&str> = values.collect();
    serde_json::to_string(&values).expect("string slice is serializable")
}

fn parsed_repo_filters_string(filters: &[ParsedRepoFilter]) -> String {
    let mut out = String::from("[");
    for (index, filter) in filters.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&filter.repo);
        if !filter.revs.is_empty() {
            out.push('@');
            for (rev_index, rev) in filter.revs.iter().enumerate() {
                if rev_index > 0 {
                    out.push(':');
                }
                out.push_str(&revision_specifier_string(rev));
            }
        }
    }
    out.push(']');
    out
}

fn revision_specifier_string(rev: &crate::search_contexts::RevisionSpecifier) -> String {
    // `query.RevisionSpecifier.String()` (`repo_revs.go:42-53`), including the
    // `rat=` URL-base64 JSON payload.
    rev.spec_string()
}

// ---------------------------------------------------------------------------
// Enumerable job tree
// ---------------------------------------------------------------------------

/// One constructed job. `children` mirrors `Job.Children`; `kind` carries the
/// attributes and name.
#[derive(Debug, Clone, PartialEq)]
pub struct JobNode {
    kind: NodeKind,
    children: Vec<JobNode>,
}

#[derive(Debug, Clone, PartialEq)]
enum NodeKind {
    Noop,
    /// `codycontext.noopJob` (`codycontext/job.go:269-279`): a *different*
    /// Go type from `jobutil.NoopJob`, so `NewAlertJob` still wraps it in an
    /// `AlertJob`.
    CodyNoop,
    Or,
    And,
    Parallel,
    Sequential {
        ensure_unique: bool,
    },
    Timeout {
        nanos: i64,
    },
    Limit {
        limit: i64,
    },
    Log,
    Alert {
        query: String,
        original_query: String,
        pattern_type: &'static str,
        /// `search.Inputs.SearchMode`; `Precise` for trees produced by
        /// `new_plan_job` (construction does not model the runtime mode).
        search_mode: SearchMode,
        /// `dotcom.SourcegraphDotComMode()` (`alert/observer.go:95`).
        on_sourcegraph_dot_com: bool,
    },
    /// `searcher.TextSearchJob` (`internal/search/searcher/search.go:33-55`).
    SearcherText {
        pattern_info: crate::search_searcher_jobs::TextPatternInfo,
        /// `PathRegexps`, stored as sources to keep `JobNode` `PartialEq`.
        path_regexps: Vec<String>,
        indexed: bool,
        num_context_lines: i32,
        use_full_deadline: bool,
        /// `Repos`; filled by [`set_tree_repos`].
        repos: Vec<RepositoryRevisions>,
    },
    SearcherSymbol {
        request: jb::SymbolSearchRequest,
        limit: i64,
        /// `Repos`; filled by [`set_tree_repos`].
        repos: Vec<RepositoryRevisions>,
    },
    Commit {
        /// `gitprotocol.Node.String()`, rendered in `Attributes`.
        query: String,
        /// Structured gitserver query executed by `search_commit_job`.
        git_query: Box<pb::QueryNode>,
        /// `SearchJob.Repos`; empty until the repo pager is ported.
        repos: Vec<RepositoryRevisions>,
        diff: bool,
        limit: i64,
        /// `commit.SearchJob.CodeMonitorSearchWrapper`.
        code_monitor_search_wrapper: Option<CodeMonitorHookHandle>,
    },
    Structural {
        pattern_info: jb::TextPatternInfo,
        /// `SearcherArgs.UseFullDeadline`.
        use_full_deadline: bool,
        /// `SearchJob.UseIndex` (`f.Index()`).
        use_index: YesNoOnly,
        /// `BatchRetry = Protocol == Batch`.
        batch_retry: bool,
        /// `SearchJob.Unindexed`; filled by [`set_tree_repos`].
        unindexed: Vec<RepositoryRevisions>,
        /// `SearchJob.Indexed`; filled by [`set_tree_repos`].
        indexed: Option<IndexedRepoRevs>,
    },
    ZoektRepoSubsetText {
        /// `Repos`; `None` models a nil pointer.
        repos: Option<IndexedRepoRevs>,
        query: zoekt_query::Query,
        /// `ZoektQueryRegexps`: also passed as path regexps at run time.
        zoekt_query_regexps: Vec<String>,
        zoekt_params: ZoektParameters,
    },
    ZoektSymbol {
        /// `Repos`; `None` models a nil pointer.
        repos: Option<IndexedRepoRevs>,
        query: zoekt_query::Query,
        zoekt_params: ZoektParameters,
    },
    ZoektGlobalText {
        global: GlobalZoektQuery,
        zoekt_params: ZoektParameters,
        /// `search.RepoOptions` passed to the global job.
        repo_opts: sg_zoekt::global_query::RepoOptions,
        global_zoekt_query_regexps: Vec<String>,
        /// Construction-time repo options, rendered in `Attributes`.
        tree_repo_opts: TreeRepoOptions,
    },
    ZoektGlobalSymbol {
        global: GlobalZoektQuery,
        zoekt_params: ZoektParameters,
        /// `search.RepoOptions` passed to the global job.
        repo_opts: sg_zoekt::global_query::RepoOptions,
        /// Construction-time repo options, rendered in `Attributes`.
        tree_repo_opts: TreeRepoOptions,
    },
    ComputeExcluded {
        repo_opts: TreeRepoOptions,
    },
    FileContains {
        original_patterns: Vec<String>,
        filter_patterns: Vec<String>,
        /// Raw `file:contains.content()` patterns for the diff searcher.
        raw_filter_patterns: Vec<String>,
        /// `search.TextPatternInfo.IsCaseSensitive` for the diff searcher.
        case_sensitive: bool,
    },
    FileHasOwners {
        include: Vec<String>,
        exclude: Vec<String>,
    },
    FileHasContributors {
        include: Vec<String>,
        exclude: Vec<String>,
    },
    Select {
        select: Vec<String>,
    },
    SelectOwners,
    Sanitize {
        patterns: Vec<String>,
    },
    /// `jobutil.subRepoPermsFilterJob`; carries the request actor the
    /// baseline reads from `actor.FromContext(ctx)`.
    SubRepoPerms {
        actor: crate::search_jobutil_misc_jobs::Actor,
    },
    /// `codycontext.searchJob` (`codycontext/job.go:234-265`). The baseline's
    /// `Children()` is nil, so the three sub-jobs are stored out-of-line.
    CodyContext {
        symbol_job: Option<Box<JobNode>>,
        code_job: Box<JobNode>,
        text_job: Box<JobNode>,
        patterns: Vec<String>,
        code_count: i32,
        text_count: i32,
        file_matcher: CodyFileMatcher,
    },
    RepoPager {
        repo_opts: TreeRepoOptions,
        contains_ref_globs: bool,
        /// `repoPagerJob.skipPartitioning` (`repo_pager_job.go:25-27`);
        /// commit/diff pagers set it, text/symbol pagers leave it false.
        skip_partitioning: bool,
    },
    ReposPartial,
    RepoSearch {
        repo_opts: TreeRepoOptions,
        repo_name_patterns: Vec<String>,
    },
    /// Fixed-event leaf used only by the execution differential harness.
    StaticEvents {
        events: Vec<StaticEvent>,
        error: Option<comb::JobFailure>,
    },
}

/// One fixed event emitted by a [`NodeKind::StaticEvents`] leaf.
#[derive(Debug, Clone, PartialEq)]
pub struct StaticEvent {
    pub results: Vec<Match>,
    pub is_limit_hit: bool,
    /// `(repo id, RepoStatus bits)` pairs.
    pub status: Vec<(i32, u8)>,
}

impl StaticEvent {
    fn to_search_event(&self) -> SearchEvent {
        let mut status = RepoStatusMap::default();
        for (id, bits) in &self.status {
            status.update(*id, *bits);
        }
        SearchEvent {
            results: self.results.clone(),
            stats: Stats {
                is_limit_hit: self.is_limit_hit,
                status,
                ..Stats::default()
            },
        }
    }
}

impl JobNode {
    fn leaf(kind: NodeKind) -> JobNode {
        JobNode {
            kind,
            children: Vec::new(),
        }
    }

    fn new(kind: NodeKind, children: Vec<JobNode>) -> JobNode {
        JobNode { kind, children }
    }

    fn is_noop(&self) -> bool {
        matches!(self.kind, NodeKind::Noop)
    }

    /// `Job.Children` for the code-monitor tree rewrite.
    pub fn children(&self) -> &[JobNode] {
        &self.children
    }

    /// `*commit.SearchJob`.
    pub fn is_commit_search_job(&self) -> bool {
        matches!(self.kind, NodeKind::Commit { .. })
    }

    /// `*repos.ComputeExcludedJob` or `*jobutil.NoopJob` (not `codycontext`'s
    /// distinct no-op type).
    pub fn is_noop_or_compute_excluded_job(&self) -> bool {
        matches!(self.kind, NodeKind::Noop | NodeKind::ComputeExcluded { .. })
    }

    /// Copy this node with new children (equivalent to `job.Map` rebuilding a
    /// container after mapping its children).
    pub fn with_children(&self, children: Vec<JobNode>) -> JobNode {
        JobNode {
            kind: self.kind.clone(),
            children,
        }
    }

    /// `commit.SearchJob` copy with `CodeMonitorSearchWrapper` set. The runner
    /// always executes commit jobs with concurrency 1 (`SearchJob.Concurrency`
    /// is not modelled separately; `CommitLeafJob` uses 1).
    pub fn with_code_monitor_hook(&self, hook: CodeMonitorHookHandle) -> JobNode {
        match &self.kind {
            NodeKind::Commit {
                query,
                git_query,
                repos,
                diff,
                limit,
                ..
            } => JobNode {
                kind: NodeKind::Commit {
                    query: query.clone(),
                    git_query: git_query.clone(),
                    repos: repos.clone(),
                    diff: *diff,
                    limit: *limit,
                    code_monitor_search_wrapper: Some(hook),
                },
                children: self.children.clone(),
            },
            _ => self.clone(),
        }
    }

    /// Replacement for `*repos.ComputeExcludedJob` / `*jobutil.NoopJob`.
    pub fn as_noop(&self) -> JobNode {
        new_noop_job()
    }

    /// Number of commit leaves carrying a `CodeMonitorSearchWrapper`.
    pub fn code_monitor_hook_count(&self) -> usize {
        let own = matches!(
            &self.kind,
            NodeKind::Commit {
                code_monitor_search_wrapper: Some(_),
                ..
            }
        ) as usize;
        own + self
            .children
            .iter()
            .map(JobNode::code_monitor_hook_count)
            .sum::<usize>()
    }

    fn name_str(&self) -> &'static str {
        match &self.kind {
            NodeKind::Noop | NodeKind::CodyNoop => "NoopJob",
            NodeKind::Or => "OrJob",
            NodeKind::And => "AndJob",
            NodeKind::Parallel => "ParallelJob",
            NodeKind::Sequential { .. } => "SequentialJob",
            NodeKind::Timeout { .. } => "TimeoutJob",
            NodeKind::Limit { .. } => "LimitJob",
            NodeKind::Log => "LogJob",
            NodeKind::Alert { .. } => "AlertJob",
            NodeKind::SearcherText { .. } => "SearcherTextSearchJob",
            NodeKind::SearcherSymbol { .. } => "SearcherSymbolSearchJob",
            NodeKind::Commit { diff, .. } => {
                if *diff {
                    "DiffSearchJob"
                } else {
                    "CommitSearchJob"
                }
            }
            NodeKind::Structural { .. } => "StructuralSearchJob",
            NodeKind::ZoektRepoSubsetText { .. } => "ZoektRepoSubsetTextSearchJob",
            NodeKind::ZoektSymbol { .. } => "ZoektSymbolSearchJob",
            NodeKind::ZoektGlobalText { .. } => "ZoektGlobalTextSearchJob",
            NodeKind::ZoektGlobalSymbol { .. } => "ZoektGlobalSymbolSearchJob",
            NodeKind::ComputeExcluded { .. } => "ReposComputeExcludedJob",
            NodeKind::FileContains { .. } => "FileContainsFilterJob",
            NodeKind::FileHasOwners { .. } => "FileHasOwnersFilterJob",
            NodeKind::FileHasContributors { .. } => "FileHasContributorsFilterJob",
            NodeKind::Select { .. } => "SelectJob",
            NodeKind::SelectOwners => "SelectOwnersSearchJob",
            NodeKind::Sanitize { .. } => "SanitizeJob",
            NodeKind::SubRepoPerms { .. } => "SubRepoPermsFilterJob",
            NodeKind::CodyContext { .. } => "CodyContextSearchJob",
            NodeKind::RepoPager { .. } => "RepoPagerJob",
            NodeKind::ReposPartial => "PartialReposJob",
            NodeKind::RepoSearch { .. } => "RepoSearchJob",
            NodeKind::StaticEvents { .. } => "StaticEventsJob",
        }
    }

    fn basic_attributes(&self) -> Vec<(String, String)> {
        match &self.kind {
            NodeKind::Noop
            | NodeKind::CodyNoop
            | NodeKind::Or
            | NodeKind::And
            | NodeKind::Parallel
            | NodeKind::Log
            | NodeKind::SelectOwners
            | NodeKind::Sanitize { .. }
            | NodeKind::SubRepoPerms { .. }
            | NodeKind::ReposPartial
            | NodeKind::StaticEvents { .. } => Vec::new(),
            NodeKind::Sequential { ensure_unique } => {
                vec![("ensureUnique".to_owned(), ensure_unique.to_string())]
            }
            NodeKind::Timeout { nanos } => vec![("timeout".to_owned(), go_duration(*nanos))],
            NodeKind::Limit { limit } => vec![("limit".to_owned(), limit.to_string())],
            NodeKind::Alert {
                query,
                original_query,
                pattern_type,
                ..
            } => vec![
                ("query".to_owned(), query.clone()),
                ("originalQuery".to_owned(), original_query.clone()),
                ("patternType".to_owned(), (*pattern_type).to_owned()),
            ],
            NodeKind::SearcherText { indexed, .. } => {
                vec![("indexed".to_owned(), indexed.to_string())]
            }
            NodeKind::SearcherSymbol {
                request,
                limit,
                repos,
            } => {
                let mut res = Vec::new();
                push_scoped(&mut res, "request", symbol_request_attributes(request));
                res.push(("numRepos".to_owned(), repos.len().to_string()));
                res.push(("limit".to_owned(), limit.to_string()));
                res
            }
            NodeKind::Commit {
                query, diff, limit, ..
            } => vec![
                ("query".to_owned(), query.clone()),
                ("diff".to_owned(), diff.to_string()),
                ("limit".to_owned(), limit.to_string()),
            ],
            NodeKind::Structural { pattern_info, .. } => {
                let mut res = Vec::new();
                push_scoped(
                    &mut res,
                    "patternInfo",
                    text_pattern_info_attributes(pattern_info),
                );
                res
            }
            NodeKind::ZoektRepoSubsetText {
                query,
                zoekt_params,
                ..
            } => vec![
                ("query".to_owned(), query.to_string()),
                (
                    "type".to_owned(),
                    indexed_request_type_str(zoekt_params.typ).to_owned(),
                ),
            ],
            NodeKind::ZoektSymbol { query, .. } => vec![("query".to_owned(), query.to_string())],
            NodeKind::ZoektGlobalText {
                global,
                zoekt_params,
                tree_repo_opts,
                ..
            }
            | NodeKind::ZoektGlobalSymbol {
                global,
                zoekt_params,
                tree_repo_opts,
                ..
            } => {
                let mut res = vec![
                    ("query".to_owned(), global.query.to_string()),
                    (
                        "type".to_owned(),
                        indexed_request_type_str(zoekt_params.typ).to_owned(),
                    ),
                ];
                res.extend(tree_repo_opts.scoped_attributes());
                res
            }
            NodeKind::ComputeExcluded { repo_opts } => repo_opts.scoped_attributes(),
            NodeKind::FileContains {
                original_patterns,
                filter_patterns,
                ..
            } => vec![
                (
                    "originalPatterns".to_owned(),
                    string_slice(original_patterns.iter().map(String::as_str)),
                ),
                (
                    "filterPatterns".to_owned(),
                    string_slice(filter_patterns.iter().map(String::as_str)),
                ),
            ],
            NodeKind::FileHasOwners { include, exclude } => vec![
                (
                    "includeOwners".to_owned(),
                    string_slice(include.iter().map(String::as_str)),
                ),
                (
                    "excludeOwners".to_owned(),
                    string_slice(exclude.iter().map(String::as_str)),
                ),
            ],
            NodeKind::FileHasContributors { include, exclude } => vec![
                (
                    "includeContributors".to_owned(),
                    string_slice(include.iter().map(String::as_str)),
                ),
                (
                    "excludeContributors".to_owned(),
                    string_slice(exclude.iter().map(String::as_str)),
                ),
            ],
            NodeKind::Select { select } => vec![(
                "select".to_owned(),
                string_slice(select.iter().map(String::as_str)),
            )],
            NodeKind::CodyContext {
                patterns,
                code_count,
                text_count,
                ..
            } => vec![
                (
                    "patterns".to_owned(),
                    string_slice(patterns.iter().map(String::as_str)),
                ),
                ("codeCount".to_owned(), code_count.to_string()),
                ("textCount".to_owned(), text_count.to_string()),
            ],
            NodeKind::RepoPager { repo_opts, .. } => repo_opts.scoped_attributes(),
            NodeKind::RepoSearch {
                repo_opts,
                repo_name_patterns,
            } => {
                let mut res = repo_opts.scoped_attributes();
                res.push((
                    "repoNamePatterns".to_owned(),
                    string_slice(repo_name_patterns.iter().map(String::as_str)),
                ));
                res
            }
        }
    }
}

/// Read access to a [`NodeKind::CodyContext`] node's out-of-line sub-jobs
/// (the baseline hides them behind `Children() == nil`).
pub struct CodyContextView<'a> {
    pub symbol_job: Option<&'a JobNode>,
    pub code_job: &'a JobNode,
    pub text_job: &'a JobNode,
    pub patterns: &'a [String],
    pub code_count: i32,
    pub text_count: i32,
    pub file_matcher: &'a CodyFileMatcher,
}

impl JobNode {
    /// The codycontext sub-jobs, when this is a `CodyContextSearchJob`.
    pub fn cody_context_view(&self) -> Option<CodyContextView<'_>> {
        let NodeKind::CodyContext {
            symbol_job,
            code_job,
            text_job,
            patterns,
            code_count,
            text_count,
            file_matcher,
        } = &self.kind
        else {
            return None;
        };
        Some(CodyContextView {
            symbol_job: symbol_job.as_deref(),
            code_job,
            text_job,
            patterns,
            code_count: *code_count,
            text_count: *text_count,
            file_matcher,
        })
    }

    /// `fileMatcher(repoID, path)`; `None` for non-codycontext nodes.
    pub fn file_matcher_accepts(&self, repo_id: i32, path: &str) -> Option<bool> {
        self.cody_context_view()
            .map(|view| view.file_matcher.accepts(repo_id, path))
    }

    /// Runs the tree and exposes the baseline `(alert, error)` pair.
    ///
    /// [`SearchJob::run`] cannot carry an alert (the ported interface returns
    /// only `Result<(), JobError>`), so it delegates here and drops the alert;
    /// the execution differential harness uses this method to compare alerts.
    pub fn run_with_alert(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let run_ctx = comb::RunContext::new_with_request_context(
            ctx.deadline,
            ctx.request_context,
        );
        match self.to_combinator() {
            Ok(mut job) => job.run(&run_ctx, clients, stream),
            Err(err) => (None, Some(comb::JobFailure::Message(err.0))),
        }
    }
}

impl SearchJob for JobNode {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        match self.run_with_alert(ctx, clients, stream).1 {
            None => Ok(()),
            Some(failure) => Err(JobError(failure.to_string())),
        }
    }

    fn name(&self) -> &'static str {
        self.name_str()
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
            Verbosity::Basic | Verbosity::Max => self.basic_attributes(),
        }
    }
}

/// The single child a unary wrapper runs; `NoopJob` when absent (defensive:
/// construction never produces a childless unary node).
fn only_child(mut children: Vec<Box<dyn comb::CombinatorJob>>) -> Box<dyn comb::CombinatorJob> {
    match children.len() {
        0 => comb::new_noop_job(),
        1 => children.pop().expect("one child"),
        _ => comb::new_sequential_job(false, children),
    }
}

/// `jobutil.subRepoPermsFilterJob` as a combinator: filters each streamed
/// event's results through the configured sub-repository checker, appending
/// errors exactly like the Go job (`errors.Append` + the generic
/// `subRepoFilterFunc` error from `applySubRepoFiltering`).
struct SubRepoPermsFilterCombinator {
    runtime: std::sync::Arc<crate::sub_repo_perms_checker::SubRepoPermsRuntime>,
    child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for SubRepoPermsFilterCombinator {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Self { runtime, child } = self;
        let mut errs: Vec<String> = Vec::new();
        let (alert, failure) = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let results = std::mem::take(&mut event.results);
                let (results, err) = crate::search_jobutil_misc_jobs::apply_sub_repo_filtering(
                    runtime.checker.as_ref(),
                    &runtime.actor,
                    &runtime.modified_files,
                    &mut crate::search_jobutil_misc_jobs::NoopPermsLog,
                    results,
                );
                event.results = results;
                if let Some(err) = err {
                    errs.push(err.to_string());
                }
                stream(event);
            };
            child.run(ctx, clients, &mut filtered)
        };
        if let Some(failure) = &failure {
            errs.push(failure.to_string());
        }
        if errs.is_empty() {
            (alert, failure)
        } else {
            (
                alert,
                Some(comb::JobFailure::Message(
                    crate::search_jobutil_misc_jobs::format_multi_error(&errs),
                )),
            )
        }
    }

    fn name(&self) -> &'static str {
        "SubRepoPermsFilterJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }
}

fn indexed_request_type_str(typ: IndexedRequestType) -> &'static str {
    match typ {
        IndexedRequestType::Text => "text",
        IndexedRequestType::Symbol => "symbol",
    }
}

/// Stores compiled path regexps as their source, keeping [`JobNode`] `Clone +
/// PartialEq` (`regex::Regex` is not `PartialEq`).
fn regexp_patterns(regexps: Vec<Regex>) -> Vec<String> {
    regexps
        .into_iter()
        .map(|regexp| regexp.as_str().to_owned())
        .collect()
}

/// Recompiles the path regexps at run time; they were compiled once during
/// construction, so a failure here is not expected.
fn compile_regexps(patterns: &[String]) -> Result<Vec<Regex>, JobError> {
    patterns
        .iter()
        .map(|pattern| Regex::new(pattern).map_err(|error| JobError(error.to_string())))
        .collect()
}

fn alert_pattern_type(value: &str) -> SearchType {
    match value {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        _ => SearchType::Standard,
    }
}

/// Adapts a childless [`crate::search_job`] leaf to [`comb::CombinatorJob`].
struct TreeLeafJob {
    name: &'static str,
    job: Box<dyn SearchJob>,
    /// `SymbolSearchJob` / `GlobalSymbolSearchJob` drop errors once the
    /// context is canceled (baseline `if ctx.Err() == nil`).
    swallow_error_when_canceled: bool,
}

impl comb::CombinatorJob for TreeLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let job_ctx = ctx.as_job_context();
        match self.job.run(&job_ctx, clients, stream) {
            Ok(()) => (None, None),
            Err(err) => {
                if self.swallow_error_when_canceled && ctx.is_canceled() {
                    (None, None)
                } else {
                    (None, Some(comb::JobFailure::Message(err.to_string())))
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// `repoPagerJob` (`repo_pager_job.go:19-136`) wired to a [`JobNode`] child.
///
/// Per page: `MaybeSendStats` → `PartitionRepos(..., TextRequest, ...)` (all
/// unindexed when `skipPartitioning`) → `setRepos` on the partial child →
/// run it, aggregating alerts and stopping at the first child error; finally
/// returns the page iterator's error.
struct TreeRepoPagerJob {
    repo_opts: TreeRepoOptions,
    contains_ref_globs: bool,
    skip_partitioning: bool,
    /// `reposPartialJob.inner`; unresolved until a page arrives.
    inner: JobNode,
}

impl comb::CombinatorJob for TreeRepoPagerJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Some(repo_pager) = clients.repo_pager else {
            return (
                None,
                Some(comb::JobFailure::Message(
                    "RepoPagerJob execution is not ported (M9.8hw)".to_owned(),
                )),
            );
        };

        let max_alerter = comb::MaxAlerter::new();
        // `repoPagerJob.Run` resolves with the node's `search.RepoOptions`
        // (`repo_pager_job.go:98-121`). The production pager reads them; the
        // option-unaware default implementation ignores them.
        let resolve_options = crate::search_repos::ResolveOptions::from_tree(&self.repo_opts);
        let resolved_pages = repo_pager.pages.resolve_pages(&resolve_options);
        for page in &resolved_pages.pages {
            page.maybe_send_stats(stream);

            let resolved = if self.skip_partitioning {
                ResolvedRepos::new(None, page.repo_revs.clone())
            } else {
                // The baseline always passes `search.TextRequest`, including
                // for symbol pagers (`repo_pager_job.go:105-113`).
                match partition_repos(
                    &page.repo_revs,
                    IndexedRequestType::Text,
                    self.repo_opts.use_index,
                    self.contains_ref_globs,
                    || repo_pager.zoekt.list(),
                ) {
                    Ok((indexed, unindexed)) => ResolvedRepos::new(Some(indexed), unindexed),
                    Err(err) => {
                        return (
                            max_alerter.alert(),
                            Some(comb::JobFailure::Message(err.to_string())),
                        )
                    }
                }
            };

            let resolved_tree = set_tree_repos(&self.inner, &resolved);
            let mut job = match resolved_tree.to_combinator() {
                Ok(job) => job,
                Err(err) => return (max_alerter.alert(), Some(comb::JobFailure::Message(err.0))),
            };
            let (alert, err) = job.run(ctx, clients, stream);
            max_alerter.add(alert);
            if let Some(err) = err {
                return (max_alerter.alert(), Some(err));
            }
        }

        match resolved_pages.error {
            // `maybeMissingRepoRevsError` is the only producer of this text
            // and Go carries the typed `*MissingRepoRevsError` through the
            // iterator, so keep the rev details for the alert description.
            Some(err)
                if err.0 == crate::search_repos::ERR_MISSING_REPO_REVS =>
            {
                (
                    max_alerter.alert(),
                    Some(comb::JobFailure::MissingRepoRevs(resolved_pages.missing)),
                )
            }
            Some(err) => (max_alerter.alert(), Some(comb::JobFailure::Message(err.0))),
            None => (max_alerter.alert(), None),
        }
    }

    fn name(&self) -> &'static str {
        "RepoPagerJob"
    }
}

/// `setRepos` (`repo_pager_job.go:59-93`) over the modeled [`JobNode`]
/// leaves: zoekt jobs receive the indexed set; searcher and commit jobs the
/// unindexed list. Containers are rewritten bottom-up, as `job.Map` does.
fn set_tree_repos(node: &JobNode, resolved: &ResolvedRepos) -> JobNode {
    let kind = match &node.kind {
        NodeKind::ZoektRepoSubsetText {
            query,
            zoekt_query_regexps,
            zoekt_params,
            ..
        } => NodeKind::ZoektRepoSubsetText {
            repos: resolved.indexed.clone(),
            query: query.clone(),
            zoekt_query_regexps: zoekt_query_regexps.clone(),
            zoekt_params: zoekt_params.clone(),
        },
        NodeKind::ZoektSymbol {
            query,
            zoekt_params,
            ..
        } => NodeKind::ZoektSymbol {
            repos: resolved.indexed.clone(),
            query: query.clone(),
            zoekt_params: zoekt_params.clone(),
        },
        NodeKind::SearcherText {
            pattern_info,
            path_regexps,
            indexed,
            num_context_lines,
            use_full_deadline,
            ..
        } => NodeKind::SearcherText {
            pattern_info: pattern_info.clone(),
            path_regexps: path_regexps.clone(),
            indexed: *indexed,
            num_context_lines: *num_context_lines,
            use_full_deadline: *use_full_deadline,
            repos: resolved.unindexed.clone(),
        },
        NodeKind::Structural {
            pattern_info,
            use_full_deadline,
            use_index,
            batch_retry,
            ..
        } => NodeKind::Structural {
            pattern_info: pattern_info.clone(),
            use_full_deadline: *use_full_deadline,
            use_index: *use_index,
            batch_retry: *batch_retry,
            unindexed: resolved.unindexed.clone(),
            indexed: resolved.indexed.clone(),
        },
        NodeKind::SearcherSymbol { request, limit, .. } => NodeKind::SearcherSymbol {
            request: request.clone(),
            limit: *limit,
            repos: resolved.unindexed.clone(),
        },
        NodeKind::Commit {
            query,
            git_query,
            diff,
            limit,
            code_monitor_search_wrapper,
            ..
        } => NodeKind::Commit {
            query: query.clone(),
            git_query: git_query.clone(),
            repos: resolved.unindexed.clone(),
            diff: *diff,
            limit: *limit,
            code_monitor_search_wrapper: code_monitor_search_wrapper.clone(),
        },
        other => other.clone(),
    };
    let children = node
        .children
        .iter()
        .map(|child| set_tree_repos(child, resolved))
        .collect();
    JobNode { kind, children }
}

/// `searchalert.Observer` ported from `internal/search/alert/observer.go`
/// (M9.39a): context errors are ignored, errors that `errorToAlert` cannot
/// convert are accumulated, `ErrNoResolvedRepos` becomes the
/// `No repositories found` alert (`observer.go:247`), and `Done` adds the
/// structural-search hint and drops residual errors once results were seen.
struct TreeAlertObserver {
    inputs: comb::AlertInputs,
    has_results: bool,
    alert: Option<comb::Alert>,
    error: Option<comb::JobFailure>,
}

impl TreeAlertObserver {
    fn new(inputs: comb::AlertInputs) -> Self {
        Self {
            inputs,
            has_results: false,
            alert: None,
            error: None,
        }
    }

    fn append_error(&mut self, error: comb::JobFailure) {
        self.error = Some(match self.error.take() {
            None => error,
            Some(comb::JobFailure::Multi(mut errors)) => {
                errors.push(error);
                comb::JobFailure::Multi(errors)
            }
            Some(existing) => comb::JobFailure::Multi(vec![existing, error]),
        });
    }

    /// `Observer.update` (`observer.go:200-204`): replace only on a strictly
    /// higher priority (ties keep the existing alert).
    fn update(&mut self, alert: comb::Alert) {
        if self
            .alert
            .as_ref()
            .is_none_or(|current| alert.priority > current.priority)
        {
            self.alert = Some(alert);
        }
    }

    /// `Observer.Error(ctx, err)` (`observer.go:177-197`). When `errorToAlert`
    /// produces an alert, residual errors are dropped exactly like the
    /// baseline's `alert, _ := o.errorToAlert(...)`.
    fn observe(
        &mut self,
        ctx: &comb::RunContext,
        clients: Option<&RuntimeClients<'_>>,
        err: &comb::JobFailure,
    ) {
        // `isContextError`: a canceled/expired context wins over the error's
        // identity; such errors are reported through progress stats instead.
        if ctx.err().is_some() || err.is_canceled() || err.is_deadline_exceeded() {
            return;
        }
        match self.error_to_alert(ctx, clients, err) {
            Some(alert) => self.update(alert),
            None => self.append_error(err.clone()),
        }
    }

    /// `Observer.errorToAlert` (`observer.go:221-285`) / `multierrorToAlert`
    /// (`observer.go:167-175`). Branch order mirrors the baseline: `Multi`
    /// first, then typed over-limit / missing-revs (checked before the
    /// string-based structural-search branches), then `ErrNoResolvedRepos`
    /// (only without results) and the structural memory alerts. Everything
    /// else keeps bubbling up.
    ///
    /// `authz.ErrStalePermissions` and `*gitdomain.BadCommitError` are
    /// translated right after `Multi`, before the no-resolved-repos branch
    /// (`observer.go:237-245`); everything else keeps bubbling.
    fn error_to_alert(
        &self,
        ctx: &comb::RunContext,
        clients: Option<&RuntimeClients<'_>>,
        err: &comb::JobFailure,
    ) -> Option<comb::Alert> {
        match err {
            comb::JobFailure::Multi(errors) => {
                let alerts: Vec<Option<comb::Alert>> = errors
                    .iter()
                    .map(|part| self.error_to_alert(ctx, clients, part))
                    .collect();
                comb::max_priority_alert(&alerts)
            }
            // `observer.go:237-241`: `errors.HasType[authz.ErrStalePermissions]`.
            comb::JobFailure::StalePermissions => Some(comb::Alert {
                prometheus_type: "no_resolved_repos__stale_permissions".to_owned(),
                title: "Permissions syncing in progress".to_owned(),
                description: "Permissions are being synced from your code host, please wait for a minute and try again.".to_owned(),
                ..comb::Alert::default()
            }),
            // `observer.go:243-245`: `*gitdomain.BadCommitError`.
            comb::JobFailure::BadCommit { spec } => Some(alert_for_invalid_revision(spec)),
            // `observer.go:251-259`: `*errOverRepoLimit`.
            comb::JobFailure::OverRepoLimit {
                proposed_queries,
                description,
            } => Some(comb::Alert {
                prometheus_type: "over_repo_limit".to_owned(),
                title: "Too many matching repositories".to_owned(),
                proposed_queries: proposed_queries.clone(),
                description: description.clone(),
                ..comb::Alert::default()
            }),
            // `observer.go:260-265`: `*MissingRepoRevsError`, priority 6.
            comb::JobFailure::MissingRepoRevs(missing) => {
                let mut alert = alert_for_missing_repo_revs(missing);
                alert.priority = 6;
                Some(alert)
            }
            comb::JobFailure::Message(message)
                if !self.has_results && message == crate::search_jobutil_repos::ERR_NO_RESOLVED_REPOS =>
            {
                Some(self.alert_for_no_resolved_repos(clients))
            }
            // `observer.go:267-275`: comby OOM / abnormal worker exit.
            comb::JobFailure::Message(message)
                if message.contains("Worker_oomed")
                    || message.contains("Worker_exited_abnormally") =>
            {
                Some(comb::Alert {
                    prometheus_type: "structural_search_needs_more_memory".to_owned(),
                    title: "Structural search needs more memory".to_owned(),
                    description: "Running your structural search may require more memory. If you are running the query on many repositories, try reducing the number of repositories with the `repo:` filter.".to_owned(),
                    priority: 5,
                    ..comb::Alert::default()
                })
            }
            // `observer.go:276-284`: searcher OOM.
            comb::JobFailure::Message(message) if message.contains("Out of memory") => {
                Some(comb::Alert {
                    prometheus_type:
                        "structural_search_needs_more_memory__give_searcher_more_memory".to_owned(),
                    title: "Structural search needs more memory".to_owned(),
                    description: r#"Running your structural search requires more memory. You could try reducing the number of repositories with the "repo:" filter. If you are an administrator, try double the memory allocated for the "searcher" service. If you're unsure, reach out to us at support@sourcegraph.com."#.to_owned(),
                    priority: 4,
                    ..comb::Alert::default()
                })
            }
            _ => None,
        }
    }

    /// `Observer.alertForNoResolvedRepos` (`observer.go:65-162`), text
    /// verbatim. `clients == None` models the parts that need no database
    /// (the DB branches then fall through, like Go's `err != nil` guards).
    fn alert_for_no_resolved_repos(
        &self,
        clients: Option<&RuntimeClients<'_>>,
    ) -> comb::Alert {
        let nodes = run(init(
            self.inputs.query.clone(),
            self.inputs.pattern_type,
        ))
        .unwrap_or_default();
        let (repo_values, minus_repo_values) = query_include_exclude(&nodes, FIELD_REPO, true);
        let repo_filters: Vec<ParsedRepoFilter> = repo_values
            .iter()
            .filter_map(|value| parse_repository_revisions(value).ok())
            .collect();
        let (context_values, _) = query_include_exclude(&nodes, FIELD_CONTEXT, false);
        let fork = query_yes_no_only(&nodes, FIELD_FORK);
        let archived = query_yes_no_only(&nodes, FIELD_ARCHIVED);

        if context_values.len() == 1
            && !crate::search_contexts::is_global_search_context_spec(&context_values[0])
            && !repo_filters.is_empty()
        {
            let without_context_filter = {
                let kept = mapper::map_field(&nodes, FIELD_CONTEXT, &mut |_, _, _| None);
                string_human(&kept)
            };
            return comb::Alert {
                prometheus_type: "no_resolved_repos__context_none_in_common".to_owned(),
                title: format!(
                    "No repositories found for your query within the context {}",
                    context_values[0]
                ),
                proposed_queries: vec![comb::QueryDescription {
                    description: "search in the global context".to_owned(),
                    query: format!(
                        "context:{} {}",
                        crate::search_contexts::GLOBAL_SEARCH_CONTEXT_NAME,
                        without_context_filter
                    ),
                    pattern_type: self.inputs.pattern_type,
                    annotations: BTreeMap::new(),
                }],
                ..comb::Alert::default()
            };
        }

        if !self.inputs.on_sourcegraph_dot_com {
            if let Some(clients) = clients {
                // `needsRepositoryConfiguration`: only branch when the count
                // read succeeds (`err == nil`).
                if let Ok(count) = clients.db.count_code_host_external_services() {
                    if count == 0 {
                        let description = if clients.db.current_user_is_site_admin() {
                            "To start searching code, first go to site admin to configure repositories and code hosts."
                        } else {
                            "To start searching code, ask the site admin to configure and enable repositories."
                        };
                        return comb::Alert {
                            title: "No repositories or code hosts configured".to_owned(),
                            description: description.to_owned(),
                            ..comb::Alert::default()
                        };
                    }
                }
            }
        }

        let (only_forks, no_forks, forks_not_set) = match fork {
            Some(YesNoOnly::Only) => (true, false, false),
            Some(YesNoOnly::No) => (false, true, false),
            Some(YesNoOnly::Yes) => (false, false, false),
            // Invalid cannot reach here (parsing/validation rejects it); Go
            // panics in `yesNoOnlyValue`, we treat it as unset.
            Some(YesNoOnly::Invalid) | None => (false, false, true),
        };
        let archived_not_set = archived.is_none();

        let mut proposed_queries = Vec::new();
        if forks_not_set {
            let options = alert_repo_options(&repo_filters, &minus_repo_values);
            if repos_exist(clients, &options) {
                proposed_queries.push(comb::QueryDescription {
                    description: "include forked repositories in your query.".to_owned(),
                    query: format!("{} fork:yes", self.inputs.original_query),
                    pattern_type: self.inputs.pattern_type,
                    annotations: BTreeMap::new(),
                });
            }
        }

        if archived_not_set {
            let mut options = alert_repo_options(&repo_filters, &minus_repo_values);
            options.only_forks = only_forks;
            options.no_forks = no_forks;
            options.only_archived = true;
            if repos_exist(clients, &options) {
                proposed_queries.push(comb::QueryDescription {
                    description: "include archived repositories in your query.".to_owned(),
                    query: format!("{} archived:yes", self.inputs.original_query),
                    pattern_type: self.inputs.pattern_type,
                    annotations: BTreeMap::new(),
                });
            }
        }

        if !proposed_queries.is_empty() {
            return comb::Alert {
                prometheus_type: "no_resolved_repos__repos_exist_when_altered".to_owned(),
                title: "No repositories found".to_owned(),
                description:
                    "Try altering the query or use a different `repo:<regexp>` filter to see results"
                        .to_owned(),
                proposed_queries,
                ..comb::Alert::default()
            };
        }

        comb::Alert {
            prometheus_type: "no_resolved_repos__generic".to_owned(),
            title: "No repositories found".to_owned(),
            description: "Try using a different `repo:<regexp>` filter to see results".to_owned(),
            ..comb::Alert::default()
        }
    }
}

/// `Q.StringValues` / `Q.Repositories` value partition. `skip_predicates`
/// matches `Repositories` (`IsPredicate` values are not repo filters).
fn query_include_exclude(
    nodes: &[Node],
    field: &str,
    skip_predicates: bool,
) -> (Vec<String>, Vec<String>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    visit_field(nodes, field, &mut |value, negated, annotation| {
        if skip_predicates && annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        if negated {
            exclude.push(value.to_owned());
        } else {
            include.push(value.to_owned());
        }
    });
    (include, exclude)
}

/// `Q.Fork` / `Q.Archived` (`types.go:123-145`): the last value wins.
fn query_yes_no_only(nodes: &[Node], field: &str) -> Option<YesNoOnly> {
    let mut result = None;
    visit_field(nodes, field, &mut |value, _, _| {
        result = Some(parse_yes_no_only(value));
    });
    result
}

/// `search.RepoOptions` from the alert branch: filters only (no context).
fn alert_repo_options(
    repo_filters: &[ParsedRepoFilter],
    minus_repo_filters: &[String],
) -> crate::search_repos::ResolveOptions {
    crate::search_repos::ResolveOptions {
        repo_filters: repo_filters.to_vec(),
        minus_repo_filters: minus_repo_filters.to_vec(),
        ..crate::search_repos::ResolveOptions::default()
    }
}

/// `Observer.reposExist` (`observer.go:51-63`): resolve and report whether any
/// page carries a repo revision. Missing repo pager yields `false`, matching a
/// failing iterator.
fn repos_exist(
    clients: Option<&RuntimeClients<'_>>,
    options: &crate::search_repos::ResolveOptions,
) -> bool {
    let Some(pager) = clients.and_then(|clients| clients.repo_pager) else {
        return false;
    };
    let resolved = pager.pages.resolve_pages(options);
    resolved
        .pages
        .iter()
        .any(|page| !page.repo_revs.is_empty())
}

/// `searchalert.AlertForMissingRepoRevs` (`observer.go:330-370`), text
/// verbatim: single repo / single rev, single repo / multiple revs, and the
/// multi-repo list with a 10-entry sample. Callers force `priority = 6`.
/// `search.AlertForInvalidRevision` (`internal/search/alert.go:167-175`).
/// `strings.TrimSuffix(revision, "^0")` then the fixed description.
fn alert_for_invalid_revision(revision: &str) -> comb::Alert {
    let revision = revision.strip_suffix("^0").unwrap_or(revision);
    comb::Alert {
        title: "Invalid revision syntax".to_owned(),
        description: format!(
            "We don't know how to interpret the revision ({revision}) you specified. Learn more about the revision syntax in our documentation: https://sourcegraph.com/docs/code-search/queries#repository-revisions."
        ),
        ..comb::Alert::default()
    }
}

fn alert_for_missing_repo_revs(
    missing_repo_revs: &[crate::search_repos::RepoRevSpecs],
) -> comb::Alert {
    let description = if missing_repo_revs.len() == 1 {
        let revs = missing_repo_revs[0].rev_specs();
        if revs.len() == 1 {
            format!(
                "The repository {} matched by your repo: filter could not be searched because it does not contain the revision {:?}.",
                missing_repo_revs[0].repo.name, revs[0]
            )
        } else {
            format!(
                "The repository {} matched by your repo: filter could not be searched because it has multiple specified revisions: @{}.",
                missing_repo_revs[0].repo.name,
                revs.join(",")
            )
        }
    } else {
        let sample_size = missing_repo_revs.len().min(10);
        let mut description = format!(
            "{} repositories matched by your repo: filter could not be searched because the following revisions do not exist, or differ but were specified for the same repository:",
            missing_repo_revs.len()
        );
        for repo_revs in &missing_repo_revs[..sample_size] {
            description.push_str(&format!(
                "\n* {}@{}",
                repo_revs.repo.name,
                repo_revs.rev_specs().join(",")
            ));
        }
        if sample_size < missing_repo_revs.len() {
            description.push_str("\n* ...");
        }
        description
    };
    comb::Alert {
        prometheus_type: "missing_repo_revs".to_owned(),
        title: "Some repositories could not be searched".to_owned(),
        description,
        ..comb::Alert::default()
    }
}

impl comb::AlertObserver for TreeAlertObserver {
    fn set_has_results(&mut self, has_results: bool) {
        self.has_results = has_results;
    }

    fn observe_error(&mut self, ctx: &comb::RunContext, err: &comb::JobFailure) {
        self.observe(ctx, None, err);
    }

    fn observe_error_with_clients(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        err: &comb::JobFailure,
    ) {
        self.observe(ctx, Some(clients), err);
    }

    fn done(&mut self) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        if !self.has_results
            && self.inputs.pattern_type != SearchType::Structural
            && comb::has_match_hole(&self.inputs.original_query)
        {
            let structural = comb::alert_for_structural_search_not_set(&self.inputs.original_query);
            if self
                .alert
                .as_ref()
                .is_none_or(|alert| structural.priority > alert.priority)
            {
                self.alert = Some(structural);
            }
        }
        if self.has_results && self.error.is_some() {
            return (self.alert.take(), None);
        }
        (self.alert.take(), self.error.take())
    }
}

/// `NodeKind::Commit` execution: delegates to [`crate::search_commit_job`]
/// when the injected runtime clients include commit seams.
struct CommitLeafJob {
    name: &'static str,
    query: pb::QueryNode,
    repos: Vec<RepositoryRevisions>,
    diff: bool,
    limit: i64,
    code_monitor_search_wrapper: Option<CodeMonitorHookHandle>,
}

impl comb::CombinatorJob for CommitLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Some(commit_clients) = clients.commit else {
            return (
                None,
                Some(comb::JobFailure::Message(format!(
                    "{} execution is not ported (M9.8if)",
                    self.name
                ))),
            );
        };
        let mut job = crate::search_commit_job::SearchJob {
            query: self.query.clone(),
            repos: self.repos.clone(),
            diff: self.diff,
            limit: self.limit,
            include_modified_files: false,
            concurrency: 1,
            code_monitor_search_wrapper: self.code_monitor_search_wrapper.clone(),
        };
        match job.run(&ctx.as_job_context(), commit_clients, stream) {
            Ok(()) => (None, None),
            Err(error) => (None, Some(comb::JobFailure::Message(error.to_string()))),
        }
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// `searchrepos.ComputeExcludedJob` (`internal/search/repos/excluded_job.go`):
/// count archived/forks excluded by the repo options and emit one stats event.
struct ComputeExcludedJob {
    repo_opts: TreeRepoOptions,
}

impl comb::CombinatorJob for ComputeExcludedJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        match compute_excluded_repos(ctx, clients, &self.repo_opts) {
            Ok((forks, archived)) => {
                stream(SearchEvent {
                    results: Vec::new(),
                    stats: Stats {
                        excluded_forks: forks,
                        excluded_archived: archived,
                        ..Stats::default()
                    },
                });
                (None, None)
            }
            Err(err) => (None, Some(comb::JobFailure::Message(err.0))),
        }
    }

    fn name(&self) -> &'static str {
        "ReposComputeExcludedJob"
    }
}

/// `computeExcludedRepos` (`repos.go:1004-1089`).
///
/// The fork/archived probes are only run when the corresponding
/// `fork:`/`archived:` source was absent and the repo filter is not exactly
/// one anchored literal. Both counts feed one `Stats` event.
fn compute_excluded_repos(
    ctx: &comb::RunContext,
    clients: &RuntimeClients<'_>,
    opts: &TreeRepoOptions,
) -> Result<(i64, i64), JobError> {
    if ctx.is_canceled() {
        return Err(JobError("context canceled".to_owned()));
    }

    // `ResolveSearchContextSpec` (`repos.go:1021-1024`). Resolution failures
    // (including the unknown-context text) abort before any count query.
    let search_context = clients
        .db
        .resolve_search_context(&opts.search_context_spec, &opts.request_context)?;
    let search_context_id = search_context.id;

    let limit = if opts.limit != 0 {
        opts.limit
    } else {
        clients.max_repos.unwrap_or(i64::from(i32::MAX >> 1))
    };

    let include_patterns: Vec<String> = opts
        .repo_filters
        .iter()
        .map(|filter| optimize_repo_pattern_with_heuristics(&filter.repo))
        .collect();

    let options = crate::search_job::ReposListOptions {
        include_patterns,
        exclude_pattern: sg_zoekt::languages::union_regexps(&opts.minus_repo_filters),
        // List N+1 repos so the caller can see omitted repos.
        limit: Some(limit.saturating_add(1)),
        no_forks: opts.no_forks,
        only_forks: opts.only_forks,
        no_archived: opts.no_archived,
        only_archived: opts.only_archived,
        only_private: opts.visibility == RepoVisibility::Private,
        no_private: opts.visibility == RepoVisibility::Public,
        search_context_id,
        // `db.Repos().Count(ctx, ...)` uses the same request actor predicate
        // (`repos.go:1066-1081`).
        authz_actor_uid: ctx.request_context().actor_uid,
        authz_internal: ctx.request_context().internal,
        ..crate::search_job::ReposListOptions::default()
    };

    let exactly_one = exactly_one_repo(&opts.repo_filters);

    let mut forks = 0;
    if !opts.fork_set && !exactly_one {
        let mut select_forks = options.clone();
        select_forks.only_forks = true;
        select_forks.no_forks = false;
        forks = clients.db.count_repos(&select_forks)?;
    }

    let mut archived = 0;
    if !opts.archived_set && !exactly_one {
        let mut select_archived = options.clone();
        select_archived.only_archived = true;
        select_archived.no_archived = false;
        archived = clients.db.count_repos(&select_archived)?;
    }

    Ok((forks, archived))
}

/// `optimizeRepoPatternWithHeuristics` (`repos.go:1198-1209`). The
/// Sourcegraph.com `^` prefix is not modeled (the port runs with dotcom mode
/// off, matching the differential environment).
fn optimize_repo_pattern_with_heuristics(pattern: &str) -> String {
    pattern.replace("github.com", r"github\.com")
}

/// `ExactlyOneRepo` (`repos.go:1094-1109`): one repo filter anchored by `^`
/// and `$` whose body parses to a single literal (`regexpsyntax.OpLiteral`).
fn exactly_one_repo(filters: &[ParsedRepoFilter]) -> bool {
    let [filter] = filters else {
        return false;
    };
    let repo = &filter.repo;
    if repo.len() < 2 || !repo.starts_with('^') || !repo.ends_with('$') {
        return false;
    }
    regexp_body_is_literal(&repo[1..repo.len() - 1])
}

/// Conservative `OpLiteral` check: every metacharacter makes the pattern
/// non-literal; a backslash escapes the following character (which is then
/// literal).
fn regexp_body_is_literal(body: &str) -> bool {
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if chars.next().is_none() {
                    return false;
                }
            }
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                return false
            }
            _ => {}
        }
    }
    true
}

/// `sanitizeJob` (`jobutil/sanitize_job.go`): drop match ranges/commits whose
/// matched text matches any sanitize pattern.
struct SanitizeCombinatorJob {
    patterns: Vec<Regex>,
    child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for SanitizeCombinatorJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let patterns = &self.patterns;
        let mut filtered = |event: SearchEvent| {
            stream(sanitize_event(event, patterns));
        };
        self.child.run(ctx, clients, &mut filtered)
    }

    fn name(&self) -> &'static str {
        "SanitizeJob"
    }
}

/// `sanitizeJob.sanitizeEvent`.
fn sanitize_event(mut event: SearchEvent, patterns: &[Regex]) -> SearchEvent {
    let mut sanitized = Vec::with_capacity(event.results.len());
    for result in std::mem::take(&mut event.results) {
        match result {
            Match::File(file_match) => {
                if let Some(file_match) = sanitize_file_match(file_match, patterns) {
                    sanitized.push(Match::File(file_match));
                }
            }
            Match::Commit(commit_match) => {
                if sanitize_commit_match(&commit_match, patterns) {
                    sanitized.push(Match::Commit(commit_match));
                }
            }
            Match::Repo(repo_match) => sanitized.push(Match::Repo(repo_match)),
            // The baseline's `default` arm drops unknown result kinds; owner
            // matches are not produced at this stage.
            Match::Owner(_) => {}
        }
    }
    event.results = sanitized;
    event
}

fn matches_any_sanitize_pattern(value: &str, patterns: &[Regex]) -> bool {
    patterns.iter().any(|pattern| pattern.is_match(value))
}

fn sanitize_file_match(
    mut file_match: crate::search_stream::convert::FileMatch,
    patterns: &[Regex],
) -> Option<crate::search_stream::convert::FileMatch> {
    if !file_match.symbols.is_empty() {
        return Some(file_match);
    }

    let mut sanitized_chunks = Vec::with_capacity(file_match.chunk_matches.len());
    for chunk in std::mem::take(&mut file_match.chunk_matches) {
        let mut sanitized_ranges = Vec::with_capacity(chunk.ranges.len());
        for (index, range) in chunk.ranges.iter().enumerate() {
            let start = (range.start.offset - chunk.content_start.offset).max(0) as usize;
            let end = (range.end.offset - chunk.content_start.offset).max(0) as usize;
            let matched = chunk.content.get(start..end).unwrap_or("");
            if matches_any_sanitize_pattern(matched, patterns) {
                continue;
            }
            sanitized_ranges.push(chunk.ranges[index]);
        }
        if sanitized_ranges.is_empty() {
            continue;
        }
        sanitized_chunks.push(crate::search_stream::convert::ChunkMatch {
            content: chunk.content,
            content_start: chunk.content_start,
            ranges: sanitized_ranges,
        });
    }

    if sanitized_chunks.is_empty() {
        return None;
    }
    file_match.chunk_matches = sanitized_chunks;
    Some(file_match)
}

fn sanitize_commit_match(
    commit_match: &crate::search_stream::convert::CommitMatch,
    patterns: &[Regex],
) -> bool {
    match &commit_match.diff_preview {
        Some(diff_preview) => !matches_any_sanitize_pattern(&diff_preview.content, patterns),
        None => true,
    }
}

/// `RepoSearchJob` (`jobutil/repos.go:21-165`) execution over the injected
/// page source. The description store is only consulted for `description:`
/// patterns (and only when a page is non-empty by construction).
struct RepoSearchLeafJob {
    repo_opts: TreeRepoOptions,
    repo_name_patterns: Vec<Regex>,
}

impl comb::CombinatorJob for RepoSearchLeafJob {
    fn run(
        &mut self,
        _ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Some(repo_pager) = clients.repo_pager else {
            return (
                None,
                Some(comb::JobFailure::Message(
                    "RepoSearchJob execution is not ported (M9.37a)".to_owned(),
                )),
            );
        };

        let description_patterns: Vec<Regex> = self
            .repo_opts
            .description_patterns
            .iter()
            .filter_map(|pattern| Regex::new(&format!("(?is){pattern}")).ok())
            .collect();

        let resolved = repo_pager
            .pages
            .resolve_pages(&crate::search_repos::ResolveOptions::from_tree(
                &self.repo_opts,
            ));
        for page in &resolved.pages {
            page.maybe_send_stats(stream);

            let description_matches = if description_patterns.is_empty() {
                BTreeMap::new()
            } else {
                let Some(descriptions) = repo_pager.descriptions else {
                    return (
                        None,
                        Some(comb::JobFailure::Message(
                            "RepoSearchJob descriptions are not ported (M9.37a)".to_owned(),
                        )),
                    );
                };
                let ids: Vec<u32> = page.repo_revs.iter().map(|repo| repo.repo.id).collect();
                match descriptions.get_repo_descriptions_by_ids(&ids) {
                    Ok(repo_descriptions) => crate::search_jobutil_repos::description_match_ranges(
                        &repo_descriptions,
                        &description_patterns,
                    ),
                    Err(err) => return (None, Some(comb::JobFailure::Message(err.0))),
                }
            };

            stream(SearchEvent {
                results: crate::search_jobutil_repos::repo_revs_to_repo_matches(
                    &page.repo_revs,
                    &self.repo_name_patterns,
                    &description_matches,
                ),
                stats: Stats::default(),
            });
        }

        match resolved.error {
            // Do not error with no results for repo search.
            Some(err) if err.0 == crate::search_jobutil_repos::ERR_NO_RESOLVED_REPOS => (None, None),
            Some(err) => (None, Some(comb::JobFailure::Message(err.0))),
            None => (None, None),
        }
    }

    fn name(&self) -> &'static str {
        "RepoSearchJob"
    }
}

impl JobNode {
    /// Converts this tree into the executable combinator representation.
    fn to_combinator(&self) -> Result<Box<dyn comb::CombinatorJob>, JobError> {
        // A pager resolves its partial child per page, so it cannot be
        // converted bottom-up like the other nodes (`repo_pager_job.go:95-136`).
        if let NodeKind::RepoPager {
            repo_opts,
            contains_ref_globs,
            skip_partitioning,
        } = &self.kind
        {
            let inner = self
                .children
                .first()
                .and_then(|partial| partial.children.first())
                .cloned()
                .unwrap_or_else(new_noop_job);
            return Ok(Box::new(TreeRepoPagerJob {
                repo_opts: repo_opts.clone(),
                contains_ref_globs: *contains_ref_globs,
                skip_partitioning: *skip_partitioning,
                inner,
            }));
        }

        let mut children = Vec::with_capacity(self.children.len());
        for child in &self.children {
            children.push(child.to_combinator()?);
        }

        Ok(match &self.kind {
            NodeKind::Noop | NodeKind::CodyNoop => comb::new_noop_job(),
            // `LogJob.Run` only writes event logs when authenticated; events
            // and errors pass through unchanged.
            NodeKind::Log => only_child(children),
            NodeKind::Sequential { ensure_unique } => {
                comb::new_sequential_job(*ensure_unique, children)
            }
            NodeKind::Parallel => comb::new_parallel_job(children),
            NodeKind::And => expr::new_and_job(children),
            NodeKind::Or => expr::new_or_job(children),
            NodeKind::Timeout { nanos } => {
                let timeout = Duration::from_nanos((*nanos).max(0) as u64);
                comb::new_timeout_job(timeout, only_child(children))
            }
            NodeKind::Limit { limit } => comb::new_limit_job(*limit, only_child(children)),
            NodeKind::Alert {
                query,
                original_query,
                pattern_type,
                search_mode,
                on_sourcegraph_dot_com,
            } => {
                let inputs = comb::AlertInputs {
                    query: query.clone(),
                    original_query: original_query.clone(),
                    pattern_type: alert_pattern_type(pattern_type),
                    search_mode: *search_mode,
                    on_sourcegraph_dot_com: *on_sourcegraph_dot_com,
                    ..comb::AlertInputs::default()
                };
                let observer_inputs = inputs.clone();
                comb::new_alert_job(
                    inputs,
                    only_child(children),
                    Box::new(move || Box::new(TreeAlertObserver::new(observer_inputs.clone()))),
                )
            }
            // Select over the tree child; keeps the child's alert/error pair
            // (the `SearchJob`-based `SelectJob` has no alert slot).
            NodeKind::Select { select } => {
                Box::new(crate::search_jobutil_filters::SelectCombinatorJob::new(
                    select.clone(),
                    only_child(children),
                ))
            }
            NodeKind::Commit {
                git_query,
                repos,
                diff,
                limit,
                code_monitor_search_wrapper,
                ..
            } => Box::new(CommitLeafJob {
                name: self.name_str(),
                query: (**git_query).clone(),
                repos: repos.clone(),
                diff: *diff,
                limit: *limit,
                code_monitor_search_wrapper: code_monitor_search_wrapper.clone(),
            }),
            NodeKind::StaticEvents { events, error } => Box::new(comb::StaticJob::new(
                events.iter().map(StaticEvent::to_search_event).collect(),
                error.clone(),
            )),
            NodeKind::ZoektRepoSubsetText {
                repos,
                query,
                zoekt_query_regexps,
                zoekt_params,
            } => {
                let job = RepoSubsetTextSearchJob {
                    repos: repos.clone(),
                    query: query.clone(),
                    zoekt_query_regexps: compile_regexps(zoekt_query_regexps)?,
                    typ: zoekt_params.typ,
                    zoekt_params: zoekt_params.clone(),
                    since: None,
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: false,
                })
            }
            NodeKind::ZoektSymbol {
                repos,
                query,
                zoekt_params,
            } => {
                let job = SymbolSearchJob {
                    repos: repos.clone(),
                    query: query.clone(),
                    zoekt_params: zoekt_params.clone(),
                    since: None,
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: true,
                })
            }
            NodeKind::ZoektGlobalText {
                global,
                zoekt_params,
                repo_opts,
                global_zoekt_query_regexps,
                ..
            } => {
                let job = GlobalTextSearchJob {
                    global_zoekt_query: global.clone(),
                    zoekt_params: zoekt_params.clone(),
                    repo_opts: repo_opts.clone(),
                    global_zoekt_query_regexps: compile_regexps(global_zoekt_query_regexps)?,
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: false,
                })
            }
            NodeKind::ZoektGlobalSymbol {
                global,
                zoekt_params,
                repo_opts,
                ..
            } => {
                // `GlobalSymbolSearchJob`: same as the global text job but
                // with nil path regexps and `ctx.Err() == nil` swallowing.
                let job = GlobalTextSearchJob {
                    global_zoekt_query: global.clone(),
                    zoekt_params: zoekt_params.clone(),
                    repo_opts: repo_opts.clone(),
                    global_zoekt_query_regexps: Vec::new(),
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: true,
                })
            }
            // `codycontext.searchJob.Run`: the blocking collector is ported in
            // `codycontext::execution` (M9.8jy); convert the out-of-line
            // symbol/code/text sub-jobs to combinators.
            NodeKind::CodyContext {
                symbol_job,
                code_job,
                text_job,
                patterns,
                code_count,
                text_count,
                file_matcher,
            } => {
                let symbol_job = match symbol_job {
                    Some(job) => Some(job.to_combinator()?),
                    None => None,
                };
                Box::new(codycontext::CodyContextExecutionJob::new(
                    symbol_job,
                    code_job.to_combinator()?,
                    text_job.to_combinator()?,
                    patterns.clone(),
                    *code_count,
                    *text_count,
                    file_matcher.clone(),
                ))
            }
            NodeKind::SearcherText {
                pattern_info,
                path_regexps,
                indexed,
                num_context_lines,
                use_full_deadline,
                repos,
            } => {
                let job = crate::search_searcher_jobs::TextSearchJob {
                    pattern_info: pattern_info.clone(),
                    repos: to_searcher_repos(repos),
                    path_regexps: compile_regexps(path_regexps)?,
                    indexed: *indexed,
                    num_context_lines: *num_context_lines,
                    use_full_deadline: *use_full_deadline,
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: false,
                })
            }
            NodeKind::SearcherSymbol {
                request,
                limit,
                repos,
            } => {
                let job = crate::search_searcher_jobs::SymbolSearchJob {
                    request: to_searcher_symbol_request(request),
                    repos: to_searcher_repos(repos),
                    limit: i32::try_from(*limit).unwrap_or(i32::MAX),
                };
                // `SymbolSearchJob.Run` stops once the context is done.
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: true,
                })
            }
            NodeKind::ComputeExcluded { repo_opts } => {
                Box::new(ComputeExcludedJob {
                    repo_opts: repo_opts.clone(),
                })
            }
            NodeKind::Sanitize { patterns } => Box::new(SanitizeCombinatorJob {
                patterns: compile_regexps(patterns)?,
                child: only_child(children),
            }),
            // `authz.DefaultSubRepoPermsChecker` defaults to the noop checker:
            // no runtime means the baseline does not even wrap the child
            // (`job.go:229`). When configured and enabled, filter file/commit
            // results the way `subRepoPermsFilterJob` does. The checker stays
            // the process-wide one; the actor is the request's
            // (`actor.FromContext(ctx)`).
            NodeKind::SubRepoPerms { actor } => {
                match crate::sub_repo_perms_checker::sub_repo_runtime() {
                    Some(runtime)
                        if crate::search_jobutil_misc_jobs::SubRepoPermissionChecker::enabled(
                            runtime.checker.as_ref(),
                        ) =>
                    {
                        let request_runtime =
                            std::sync::Arc::new(crate::sub_repo_perms_checker::SubRepoPermsRuntime {
                                checker: std::sync::Arc::clone(&runtime.checker),
                                actor: *actor,
                                modified_files: runtime.modified_files.clone(),
                            });
                        Box::new(SubRepoPermsFilterCombinator {
                            runtime: request_runtime,
                            child: only_child(children),
                        })
                    }
                    _ => only_child(children),
                }
            }
            // `reposPartialJob` is resolved by the enclosing pager; when run
            // directly it just runs its child.
            NodeKind::ReposPartial => only_child(children),
            NodeKind::RepoSearch {
                repo_opts,
                repo_name_patterns,
            } => Box::new(RepoSearchLeafJob {
                repo_opts: repo_opts.clone(),
                repo_name_patterns: compile_regexps(repo_name_patterns)?,
            }),
            NodeKind::Structural {
                pattern_info,
                use_full_deadline,
                batch_retry,
                unindexed,
                indexed,
                ..
            } => {
                let job = crate::search_leaf_jobs::StructuralSearchJob {
                    pattern_info: to_searcher_pattern_info(pattern_info),
                    use_full_deadline: *use_full_deadline,
                    batch_retry: *batch_retry,
                    unindexed: to_leaf_repos(unindexed),
                    indexed: indexed.clone(),
                };
                Box::new(TreeLeafJob {
                    name: self.name_str(),
                    job: Box::new(job),
                    swallow_error_when_canceled: false,
                })
            }
            NodeKind::FileContains {
                original_patterns,
                filter_patterns,
                raw_filter_patterns,
                case_sensitive,
            } => Box::new(crate::search_leaf_jobs::FileContainsLeafJob {
                include_patterns: filter_patterns.clone(),
                original_patterns: original_patterns.clone(),
                raw_include_patterns: raw_filter_patterns.clone(),
                case_sensitive: *case_sensitive,
                searcher: None,
                child: only_child(children),
            }),
            NodeKind::FileHasOwners { include, exclude } => {
                Box::new(crate::search_leaf_jobs::FileHasOwnersLeafJob {
                    include: include.clone(),
                    exclude: exclude.clone(),
                    child: only_child(children),
                })
            }
            NodeKind::FileHasContributors { include, exclude } => {
                Box::new(crate::search_leaf_jobs::FileHasContributorsLeafJob {
                    include: include.clone(),
                    exclude: exclude.clone(),
                    child: only_child(children),
                })
            }
            NodeKind::SelectOwners => {
                Box::new(crate::search_leaf_jobs::SelectOwnersLeafJob {
                    child: only_child(children),
                })
            }
            // Handled by the early return above; kept for exhaustiveness.
            NodeKind::RepoPager { .. } => {
                return Err(JobError(
                    "RepoPagerJob execution is not ported (M9.8hw)".to_owned(),
                ));
            }
        })
    }

    /// Public constructors used by the execution differential harness.
    pub fn noop() -> JobNode {
        new_noop_job()
    }

    pub fn log(child: JobNode) -> JobNode {
        new_log_job(child)
    }

    pub fn limit(limit: i64, child: JobNode) -> JobNode {
        new_limit_job(limit, child)
    }

    pub fn timeout(nanos: i64, child: JobNode) -> JobNode {
        new_timeout_job(nanos, child)
    }

    pub fn sequential(ensure_unique: bool, children: Vec<JobNode>) -> JobNode {
        new_sequential_job(ensure_unique, children)
    }

    pub fn parallel(children: Vec<JobNode>) -> JobNode {
        new_parallel_job(children)
    }

    pub fn and(children: Vec<JobNode>) -> JobNode {
        new_and_job(children)
    }

    pub fn or(children: Vec<JobNode>) -> JobNode {
        new_or_job(children)
    }

    /// `searchrepos.ComputeExcludedJob`.
    pub fn compute_excluded(repo_opts: TreeRepoOptions) -> JobNode {
        JobNode::leaf(NodeKind::ComputeExcluded { repo_opts })
    }

    /// `jobutil.sanitizeJob`.
    pub fn sanitize(patterns: Vec<String>, child: JobNode) -> JobNode {
        JobNode::new(NodeKind::Sanitize { patterns }, vec![child])
    }

    /// `jobutil.subRepoPermsFilterJob` for the anonymous actor.
    pub fn sub_repo_perms(child: JobNode) -> JobNode {
        Self::sub_repo_perms_for_actor(
            child,
            crate::search_jobutil_misc_jobs::Actor::default(),
        )
    }

    /// `jobutil.subRepoPermsFilterJob` with the request actor.
    pub fn sub_repo_perms_for_actor(
        child: JobNode,
        actor: crate::search_jobutil_misc_jobs::Actor,
    ) -> JobNode {
        JobNode::new(NodeKind::SubRepoPerms { actor }, vec![child])
    }

    /// `jobutil.reposPartialJob`, runnable without an enclosing pager.
    pub fn repos_partial(child: JobNode) -> JobNode {
        JobNode::new(NodeKind::ReposPartial, vec![child])
    }

    /// `jobutil.RepoSearchJob`.
    pub fn repo_search(repo_opts: TreeRepoOptions, repo_name_patterns: Vec<String>) -> JobNode {
        JobNode::leaf(NodeKind::RepoSearch {
            repo_opts,
            repo_name_patterns,
        })
    }

    /// `searchrepos.repoPagerJob` over the tree's partial child.
    pub fn repo_pager(
        repo_opts: TreeRepoOptions,
        contains_ref_globs: bool,
        skip_partitioning: bool,
        inner: JobNode,
    ) -> JobNode {
        JobNode::new(
            NodeKind::RepoPager {
                repo_opts,
                contains_ref_globs,
                skip_partitioning,
            },
            vec![JobNode::new(NodeKind::ReposPartial, vec![inner])],
        )
    }

    pub fn zoekt_repo_subset_text(
        repos: Option<IndexedRepoRevs>,
        query: zoekt_query::Query,
        zoekt_query_regexps: Vec<String>,
        zoekt_params: ZoektParameters,
    ) -> JobNode {
        JobNode::leaf(NodeKind::ZoektRepoSubsetText {
            repos,
            query,
            zoekt_query_regexps,
            zoekt_params,
        })
    }

    pub fn zoekt_symbol(
        repos: Option<IndexedRepoRevs>,
        query: zoekt_query::Query,
        zoekt_params: ZoektParameters,
    ) -> JobNode {
        JobNode::leaf(NodeKind::ZoektSymbol {
            repos,
            query,
            zoekt_params,
        })
    }

    pub fn zoekt_global_text(
        global: GlobalZoektQuery,
        zoekt_params: ZoektParameters,
        repo_opts: sg_zoekt::global_query::RepoOptions,
        global_zoekt_query_regexps: Vec<String>,
        tree_repo_opts: TreeRepoOptions,
    ) -> JobNode {
        JobNode::leaf(NodeKind::ZoektGlobalText {
            global,
            zoekt_params,
            repo_opts,
            global_zoekt_query_regexps,
            tree_repo_opts,
        })
    }

    pub fn zoekt_global_symbol(
        global: GlobalZoektQuery,
        zoekt_params: ZoektParameters,
        repo_opts: sg_zoekt::global_query::RepoOptions,
        tree_repo_opts: TreeRepoOptions,
    ) -> JobNode {
        JobNode::leaf(NodeKind::ZoektGlobalSymbol {
            global,
            zoekt_params,
            repo_opts,
            tree_repo_opts,
        })
    }

    /// `NewAlertJob` harness constructor: wraps `child` with the same fields
    /// `new_alert_job` reads, including the runtime `search_mode`.
    pub fn alert(inputs: &comb::AlertInputs, child: JobNode) -> JobNode {
        if child.is_noop() {
            return child;
        }
        JobNode::new(
            NodeKind::Alert {
                query: inputs.query.clone(),
                original_query: inputs.original_query.clone(),
                pattern_type: comb::pattern_type_str(inputs.pattern_type),
                search_mode: inputs.search_mode,
                on_sourcegraph_dot_com: inputs.on_sourcegraph_dot_com,
            },
            vec![child],
        )
    }

    /// `NewSelectJob` harness constructor.
    pub fn select(path: Vec<String>, child: JobNode) -> JobNode {
        JobNode::new(NodeKind::Select { select: path }, vec![child])
    }

    /// `commit.SearchJob` leaf harness constructor: the query tree and repos
    /// are resolved at construction time (the repo pager is still unported in
    /// production construction).
    pub fn commit(
        git_query: pb::QueryNode,
        repos: Vec<RepositoryRevisions>,
        diff: bool,
        limit: i64,
    ) -> JobNode {
        let query = crate::search_commit_job::query_node_string(&git_query);
        JobNode::leaf(NodeKind::Commit {
            query,
            git_query: Box::new(git_query),
            repos,
            diff,
            limit,
            code_monitor_search_wrapper: None,
        })
    }

    /// Fixed-event leaf harness constructor.
    pub fn static_events(events: Vec<StaticEvent>, error: Option<comb::JobFailure>) -> JobNode {
        JobNode::leaf(NodeKind::StaticEvents { events, error })
    }
}

fn symbol_request_attributes(request: &jb::SymbolSearchRequest) -> Vec<(String, String)> {
    let mut res = vec![("pattern".to_owned(), request.regexp_pattern.clone())];
    if request.is_case_sensitive {
        res.push(("isCaseSensitive".to_owned(), "true".to_owned()));
    }
    if !request.include_patterns.is_empty() {
        res.push((
            "includePatterns".to_owned(),
            string_slice(request.include_patterns.iter().map(String::as_str)),
        ));
    }
    if !request.exclude_pattern.is_empty() {
        res.push(("excludePattern".to_owned(), request.exclude_pattern.clone()));
    }
    if !request.include_langs.is_empty() {
        res.push((
            "includeLangs".to_owned(),
            string_slice(request.include_langs.iter().map(String::as_str)),
        ));
    }
    if !request.exclude_langs.is_empty() {
        res.push((
            "excludeLangs".to_owned(),
            string_slice(request.exclude_langs.iter().map(String::as_str)),
        ));
    }
    res
}

fn text_pattern_info_attributes(info: &jb::TextPatternInfo) -> Vec<(String, String)> {
    let mut res = vec![("query".to_owned(), info.query.to_string())];
    if info.is_structural_pat {
        res.push(("isStructural".to_owned(), "true".to_owned()));
    }
    if !info.comby_rule.is_empty() {
        res.push(("combyRule".to_owned(), info.comby_rule.clone()));
    }
    if info.is_case_sensitive {
        res.push(("isCaseSensitive".to_owned(), "true".to_owned()));
    }
    res.push((
        "fileMatchLimit".to_owned(),
        info.file_match_limit.to_string(),
    ));
    if info.index != YesNoOnly::Yes {
        res.push(("index".to_owned(), yes_no_only_str(info.index).to_owned()));
    }
    if !info.select.is_empty() {
        res.push((
            "select".to_owned(),
            string_slice(info.select.iter().map(String::as_str)),
        ));
    }
    if !info.include_paths.is_empty() {
        res.push((
            "includePatterns".to_owned(),
            string_slice(info.include_paths.iter().map(String::as_str)),
        ));
    }
    if !info.exclude_paths.is_empty() {
        res.push(("excludePattern".to_owned(), info.exclude_paths.clone()));
    }
    if info.path_patterns_are_case_sensitive {
        res.push(("pathPatternsAreCaseSensitive".to_owned(), "true".to_owned()));
    }
    if info.pattern_matches_path {
        res.push(("patternMatchesPath".to_owned(), "true".to_owned()));
    }
    if !info.languages.is_empty() {
        res.push((
            "languages".to_owned(),
            string_slice(info.languages.iter().map(String::as_str)),
        ));
    }
    res
}

fn go_duration(nanos: i64) -> String {
    if nanos < 0 {
        format!(
            "-{}",
            format_go_duration(Duration::from_nanos(nanos.unsigned_abs()))
        )
    } else {
        format_go_duration(Duration::from_nanos(nanos as u64))
    }
}

// ---------------------------------------------------------------------------
// Combinator constructors (baseline collapse rules)
// ---------------------------------------------------------------------------

fn new_noop_job() -> JobNode {
    JobNode::leaf(NodeKind::Noop)
}

fn new_or_job(mut children: Vec<JobNode>) -> JobNode {
    match children.len() {
        0 => new_noop_job(),
        1 => children.remove(0),
        _ => JobNode::new(NodeKind::Or, children),
    }
}

fn new_and_job(mut children: Vec<JobNode>) -> JobNode {
    match children.len() {
        0 => new_noop_job(),
        1 => children.remove(0),
        _ => JobNode::new(NodeKind::And, children),
    }
}

fn new_parallel_job(mut children: Vec<JobNode>) -> JobNode {
    match children.len() {
        0 => new_noop_job(),
        1 => children.remove(0),
        _ => JobNode::new(NodeKind::Parallel, children),
    }
}

fn new_sequential_job(ensure_unique: bool, mut children: Vec<JobNode>) -> JobNode {
    match children.len() {
        0 => new_noop_job(),
        1 => children.remove(0),
        _ => JobNode::new(NodeKind::Sequential { ensure_unique }, children),
    }
}

fn new_timeout_job(nanos: i64, child: JobNode) -> JobNode {
    if child.is_noop() {
        return child;
    }
    JobNode::new(NodeKind::Timeout { nanos }, vec![child])
}

fn new_limit_job(limit: i64, child: JobNode) -> JobNode {
    if child.is_noop() {
        return child;
    }
    JobNode::new(NodeKind::Limit { limit }, vec![child])
}

fn new_alert_job(inputs: &TreeInputs, plan: &Plan, child: JobNode) -> JobNode {
    if child.is_noop() {
        return child;
    }
    let query = q_string(&plan_to_q(plan));
    JobNode::new(
        NodeKind::Alert {
            query,
            original_query: inputs.original_query.clone(),
            pattern_type: crate::search_jobutil_combinators::pattern_type_str(inputs.pattern_type),
            // `TreeInputs` does not model the runtime `search.SearchMode`.
            search_mode: SearchMode::Precise,
            on_sourcegraph_dot_com: inputs.on_sourcegraph_dot_com,
        },
        vec![child],
    )
}

fn new_log_job(child: JobNode) -> JobNode {
    JobNode::new(NodeKind::Log, vec![child])
}

fn repo_pager(child: JobNode, repo_opts: TreeRepoOptions, contains_ref_globs: bool) -> JobNode {
    repo_pager_with_partitioning(child, repo_opts, contains_ref_globs, false)
}

fn repo_pager_with_partitioning(
    child: JobNode,
    repo_opts: TreeRepoOptions,
    contains_ref_globs: bool,
    skip_partitioning: bool,
) -> JobNode {
    JobNode::new(
        NodeKind::RepoPager {
            repo_opts,
            contains_ref_globs,
            skip_partitioning,
        },
        vec![JobNode::new(NodeKind::ReposPartial, vec![child])],
    )
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// `NewPlanJob` (`job.go:35-63`).
pub fn new_plan_job(inputs: &TreeInputs, plan: &Plan) -> Result<JobNode, JobError> {
    let mut children = Vec::with_capacity(plan.len());
    for basic in plan {
        children.push(new_basic_job(inputs, basic)?);
    }

    let mut job_tree = new_or_job(children);

    if inputs.pattern_type == SearchType::CodyContext {
        match &inputs.cody_context_hook {
            Some(hook) => job_tree = hook(plan, inputs)?,
            None => job_tree = new_cody_context_job(inputs, plan)?,
        }
    }

    let alert_job = new_alert_job(inputs, plan, job_tree);
    Ok(new_log_job(alert_job))
}

/// `codycontext.NewSearchJob` (`job.go:34-89`) with
/// `newJob = NewBasicJob(inputs, ...)`.
///
/// The baseline constructs the symbol sub-job first (only when `symbols` is
/// non-empty), then code, then text, propagating the first constructor error.
fn new_cody_context_job(inputs: &TreeInputs, plan: &[Basic]) -> Result<JobNode, JobError> {
    let parts = codycontext::new_search_job(plan, inputs.cody_context.as_ref())?;
    let CodyContextJobParts::Jobs {
        symbol_query,
        code_query,
        text_query,
        patterns,
        code_count,
        text_count,
        file_matcher,
    } = parts
    else {
        // `codycontext.newNoopJob()`: a distinct Go type from
        // `jobutil.NoopJob`, so `NewAlertJob` does not unwrap it.
        return Ok(JobNode::leaf(NodeKind::CodyNoop));
    };

    let symbol_job = match &symbol_query {
        Some(symbol_query) => Some(Box::new(new_basic_job(inputs, symbol_query)?)),
        // `symbolJob` defaults to `newNoopJob()`; invisible because
        // `searchJob.Children()` is nil.
        None => None,
    };
    let code_job = Box::new(new_basic_job(inputs, &code_query)?);
    let text_job = Box::new(new_basic_job(inputs, &text_query)?);

    Ok(JobNode::new(
        NodeKind::CodyContext {
            symbol_job,
            code_job,
            text_job,
            patterns,
            code_count,
            text_count,
            file_matcher,
        },
        Vec::new(),
    ))
}

/// `NewBasicJob` (`job.go:65-281`).
pub fn new_basic_job(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    let mut children = Vec::new();

    // `file:contains.content()` rewrite. The original query feeds the
    // post-filter and the git query.
    let file_contains_patterns = file_contains_content(basic);
    let original_query = basic.clone();
    let mut b = basic.clone();
    if !file_contains_patterns.is_empty() {
        let mut new_nodes = Vec::with_capacity(file_contains_patterns.len() + 1);
        for pattern in &file_contains_patterns {
            let mut node = Pattern {
                value: pattern.clone(),
                negated: false,
                annotation: Default::default(),
            };
            node.annotation.labels.set(Labels::REGEXP);
            new_nodes.push(Node::Pattern(node));
        }
        if let Some(pattern) = &b.pattern {
            new_nodes.push(pattern.clone());
        }
        b.pattern = Some(Node::Operator(Operator {
            kind: OperatorKind::And,
            operands: new_nodes,
            annotation: Default::default(),
        }));
    }

    let default_limit = inputs.default_limit();
    let result_types = jb::compute_result_types(&b, inputs.pattern_type, jb::DEFAULT_RESULT_TYPES);
    let file_match_limit = jb::compute_file_match_limit(&b, default_limit) as i32;
    let job_repo_options = jb::to_repo_options(&b, &inputs.user_settings);
    let (repo_universe_search, skip_repo_subset_search, run_zoekt_over_repos) = jb::job_mode(
        &b,
        &job_repo_options,
        result_types,
        &jb::SearchInputs {
            protocol: inputs.protocol,
            pattern_type: inputs.pattern_type,
            on_sourcegraph_dot_com: inputs.on_sourcegraph_dot_com,
        },
    );
    let mut repo_options = TreeRepoOptions::from_job_builders(&job_repo_options);
    // `actor.FromContext(ctx)` travels with the repo options into
    // `doQueryDB`/`computeExcludedRepos` (`ResolveSearchContextSpec`).
    repo_options.request_context = inputs.request_context;
    let contains_ref_globs = contains_ref_globs(&b);

    // `filter.SelectPathFromString(b.FindValue(query.FieldSelect))`.
    let selector = jb::select_path_from_string(&q_string_value(&b.to_parse_tree(), FIELD_SELECT))
        .unwrap_or_default();
    let builder = JobBuilder {
        result_types,
        features: inputs.features.clone(),
        pattern_type: inputs.pattern_type,
        file_match_limit,
        select: selector,
        num_context_lines: inputs.context_lines,
    };

    if result_types.has(jb::ResultTypes::FILE | jb::ResultTypes::PATH) {
        if repo_universe_search {
            children.push(new_zoekt_global_search(
                &builder,
                &b,
                &repo_options,
                IndexedRequestType::Text,
            )?);
        }

        if !skip_repo_subset_search {
            if run_zoekt_over_repos {
                let search_job = new_zoekt_search(&builder, &b, IndexedRequestType::Text)?;
                children.push(repo_pager(
                    search_job,
                    repo_options.clone(),
                    contains_ref_globs,
                ));
            }

            children.push(new_text_search_job(
                inputs,
                &b,
                result_types,
                repo_options.clone(),
            )?);
        }
    }

    if result_types.has(jb::ResultTypes::SYMBOL) {
        if repo_universe_search {
            children.push(new_zoekt_global_search(
                &builder,
                &b,
                &repo_options,
                IndexedRequestType::Symbol,
            )?);
        }

        if !skip_repo_subset_search && run_zoekt_over_repos {
            let search_job = new_zoekt_search(&builder, &b, IndexedRequestType::Symbol)?;
            children.push(repo_pager(
                search_job,
                repo_options.clone(),
                contains_ref_globs,
            ));
        }
    }

    if result_types.has(jb::ResultTypes::COMMIT) || result_types.has(jb::ResultTypes::DIFF) {
        let own = jb::is_ownership_search(&b).2;
        let diff = result_types.has(jb::ResultTypes::DIFF);
        let commit_repo_options = TreeRepoOptions {
            only_cloned: true,
            ..repo_options.clone()
        };
        // `IncludeModifiedFiles` is Max-only and therefore not part of this
        // construction tree. `own` only feeds it in the baseline.
        let _ = own;
        let commit_job = JobNode::leaf(NodeKind::Commit {
            query: query_to_git_query(&original_query, diff).to_string(),
            git_query: Box::new(crate::search_commit_job::query_to_git_query(
                &original_query,
                diff,
            )),
            // Repo pager is not ported; the leaf runs with no repos.
            repos: Vec::new(),
            diff,
            limit: i64::from(file_match_limit),
            code_monitor_search_wrapper: None,
        });
        children.push(repo_pager_with_partitioning(
            commit_job,
            commit_repo_options,
            contains_ref_globs,
            true,
        ));
    }

    children.push(JobNode::leaf(NodeKind::ComputeExcluded {
        repo_opts: repo_options.clone(),
    }));

    children.push(to_flat_jobs(inputs, &b)?);

    let mut basic_job = new_parallel_job(children);

    if !file_contains_patterns.is_empty() {
        basic_job = new_file_contains_filter(
            file_contains_patterns,
            original_query.pattern.as_ref(),
            b.is_case_sensitive(),
            basic_job,
        )?;
    }

    let (include_owners, exclude_owners, ownership) = jb::is_ownership_search(&b);
    if ownership {
        basic_job = JobNode::new(
            NodeKind::FileHasOwners {
                include: include_owners,
                exclude: exclude_owners,
            },
            vec![basic_job],
        );
    }

    let (include_contributors, exclude_contributors, contributors) = jb::is_contributor_search(&b);
    if contributors {
        let include = jb::contributors_as_regexp(&include_contributors, b.is_case_sensitive())
            .iter()
            .map(|re| re.as_str().to_owned())
            .collect();
        let exclude = jb::contributors_as_regexp(&exclude_contributors, b.is_case_sensitive())
            .iter()
            .map(|re| re.as_str().to_owned())
            .collect();
        basic_job = JobNode::new(
            NodeKind::FileHasContributors { include, exclude },
            vec![basic_job],
        );
    }

    if inputs.sub_repo_enabled {
        basic_job = JobNode::sub_repo_perms_for_actor(
            basic_job,
            inputs.request_context.sub_repo_actor(),
        );
    }

    let select_value = q_string_value(&b.to_parse_tree(), FIELD_SELECT);
    if !select_value.is_empty() {
        let select_path = jb::select_path_from_string(&select_value).unwrap_or_default();
        if jb::is_select_owners_search(&select_path) {
            basic_job = JobNode::new(NodeKind::SelectOwners, vec![basic_job]);
        } else {
            basic_job = JobNode::new(
                NodeKind::Select {
                    select: select_path,
                },
                vec![basic_job],
            );
        }
    }

    if !inputs.sanitize_search_patterns.is_empty() {
        basic_job = JobNode::new(
            NodeKind::Sanitize {
                patterns: inputs.sanitize_search_patterns.clone(),
            },
            vec![basic_job],
        );
    }

    let max_results = jb::parameters_max_results(&b.parameters, default_limit);
    basic_job = new_limit_job(max_results, basic_job);

    let timeout = jb::timeout_duration(inputs.protocol, &b, jb::DEFAULT_MAX_TIMEOUT_SECONDS);
    basic_job = new_timeout_job(timeout, basic_job);

    if inputs.on_sourcegraph_dot_com && matches!(b.pattern, Some(Node::Pattern(_))) {
        basic_job = order_racing_jobs(basic_job);
    }

    Ok(basic_job)
}

/// `NewTextSearchJob` (`job.go:282-304`).
pub fn new_text_search_job(
    inputs: &TreeInputs,
    basic: &Basic,
    result_types: jb::ResultTypes,
    repo_options: TreeRepoOptions,
) -> Result<JobNode, JobError> {
    let info = jb::to_text_pattern_info(
        basic,
        result_types,
        &inputs.features,
        inputs.default_limit(),
    )
    .map_err(JobError)?;
    // `useFullDeadline = b.GetTimeout() != nil || b.Count() != nil ||
    // inputs.Protocol != search.Batch` (`job.go:283-284`).
    let use_full_deadline = jb::parameters_timeout_nanos(&basic.parameters).is_some()
        || jb::parameters_count(&basic.parameters).is_some()
        || inputs.protocol != Protocol::Batch;
    let path_regexps = regexp_patterns(jb::get_path_regexps(basic, &info));

    let searcher_job = JobNode::leaf(NodeKind::SearcherText {
        pattern_info: to_searcher_pattern_info(&info),
        path_regexps,
        indexed: false,
        num_context_lines: inputs.context_lines,
        use_full_deadline,
        repos: Vec::new(),
    });
    Ok(repo_pager(
        searcher_job,
        repo_options,
        contains_ref_globs(basic),
    ))
}

/// `Exhaustive.Job` (`exhaustive_job.go:127-133`): resolve the exhaustive
/// `repoPagerJob` with a single pre-resolved `RepositoryRevisions`, which
/// `setRepos` assigns to the unindexed leaf.
///
/// The exhaustive searcher already resolved the repo/rev, so this bypasses
/// the pager and returns a directly executable leaf; it is the Rust
/// equivalent of `repoPagerJob.child.Resolve(resolvedRepos{unindexed:
/// []search.RepositoryRevisions{repoRevs}})`.
pub fn exhaustive_job_node(
    exhaustive: &crate::search_jobutil_exhaustive::Exhaustive,
    repo_revs: RepositoryRevisions,
) -> Result<JobNode, JobError> {
    use crate::search_jobutil_exhaustive::ExhaustiveChild;

    match &exhaustive.repo_pager_job.child {
        ExhaustiveChild::SearcherText(job) => Ok(JobNode::leaf(NodeKind::SearcherText {
            pattern_info: to_searcher_pattern_info(&job.pattern_info),
            path_regexps: job.path_regexps.clone(),
            indexed: job.indexed,
            num_context_lines: job.num_context_lines,
            use_full_deadline: job.use_full_deadline,
            repos: vec![repo_revs],
        })),
        ExhaustiveChild::Commit(job) => Ok(JobNode::commit(
            crate::search_jobutil_exhaustive::git_query_node_to_proto(&job.query),
            vec![repo_revs],
            job.diff,
            job.limit,
        )),
    }
}

/// Convert the job-builder `search.TextPatternInfo` into the searcher job's
/// view (`search_searcher_jobs::TextPatternInfo`).
fn to_searcher_pattern_info(
    info: &jb::TextPatternInfo,
) -> crate::search_searcher_jobs::TextPatternInfo {
    crate::search_searcher_jobs::TextPatternInfo {
        // `protocol.FromJobNode(nil)` yields an empty regex pattern node.
        query: Some(info.query.clone()),
        is_structural_pat: info.is_structural_pat,
        comby_rule: info.comby_rule.clone(),
        is_case_sensitive: info.is_case_sensitive,
        file_match_limit: info.file_match_limit,
        select: info.select.clone(),
        include_paths: info.include_paths.clone(),
        exclude_paths: info.exclude_paths.clone(),
        include_langs: info.include_langs.clone(),
        exclude_langs: info.exclude_langs.clone(),
        path_patterns_are_case_sensitive: info.path_patterns_are_case_sensitive,
        pattern_matches_content: info.pattern_matches_content,
        pattern_matches_path: info.pattern_matches_path,
        languages: info.languages.clone(),
    }
}

/// Convert the pager's `search.RepositoryRevisions` into the searcher job's
/// `MinimalRepo`-based view (both models carry only id/name).
fn to_searcher_repos(
    repos: &[RepositoryRevisions],
) -> Vec<crate::search_searcher_jobs::RepositoryRevisions> {
    repos
        .iter()
        .map(|repo| crate::search_searcher_jobs::RepositoryRevisions {
            repo: crate::search_stream::convert::MinimalRepo {
                id: repo.repo.id as i32,
                name: repo.repo.name.clone(),
            },
            revs: repo.revs.clone(),
        })
        .collect()
}

/// Convert the pager's zoekt `RepositoryRevisions` into the structural leaf's
/// model (`search_leaf_jobs::RepositoryRevisions`).
fn to_leaf_repos(repos: &[RepositoryRevisions]) -> Vec<crate::search_leaf_jobs::RepositoryRevisions> {
    repos
        .iter()
        .map(|repo| crate::search_leaf_jobs::RepositoryRevisions {
            repo: crate::search_stream::convert::MinimalRepo {
                id: repo.repo.id as i32,
                name: repo.repo.name.clone(),
            },
            revs: repo.revs.clone(),
        })
        .collect()
}

fn to_searcher_symbol_request(
    request: &jb::SymbolSearchRequest,
) -> crate::search_searcher_jobs::SymbolSearchRequest {
    crate::search_searcher_jobs::SymbolSearchRequest {
        regexp_pattern: request.regexp_pattern.clone(),
        is_case_sensitive: request.is_case_sensitive,
        include_patterns: request.include_patterns.clone(),
        exclude_pattern: request.exclude_pattern.clone(),
        include_langs: request.include_langs.clone(),
        exclude_langs: request.exclude_langs.clone(),
    }
}

struct JobBuilder {
    result_types: jb::ResultTypes,
    features: Features,
    pattern_type: SearchType,
    file_match_limit: i32,
    select: jb::SelectPath,
    num_context_lines: i32,
}

impl JobBuilder {
    fn zoekt_result_types(&self) -> zoekt_query::ResultTypes {
        let mut types = zoekt_query::ResultTypes::default();
        for (from, to) in [
            (jb::ResultTypes::REPO, zoekt_query::ResultTypes::REPO),
            (jb::ResultTypes::SYMBOL, zoekt_query::ResultTypes::SYMBOL),
            (jb::ResultTypes::FILE, zoekt_query::ResultTypes::FILE),
            (jb::ResultTypes::PATH, zoekt_query::ResultTypes::PATH),
            (jb::ResultTypes::DIFF, zoekt_query::ResultTypes::DIFF),
            (jb::ResultTypes::COMMIT, zoekt_query::ResultTypes::COMMIT),
            (
                jb::ResultTypes::STRUCTURAL,
                zoekt_query::ResultTypes::STRUCTURAL,
            ),
        ] {
            if self.result_types.has(from) {
                types = types.with(to);
            }
        }
        types
    }

    fn zoekt_features(&self) -> zoekt_query::Features {
        zoekt_query::Features {
            content_based_lang_filters: self.features.content_based_lang_filters,
        }
    }

    /// `search.ZoektParameters` constructed by the baseline job builder.
    fn zoekt_parameters(&self, typ: IndexedRequestType) -> ZoektParameters {
        ZoektParameters {
            typ,
            file_match_limit: self.file_match_limit,
            select: SelectPath(self.select.clone()),
            num_context_lines: self.num_context_lines,
            features: crate::search_stream::zoekt_search::Features {
                debug: self.features.debug,
                zoekt_search_options_override: self
                    .features
                    .zoekt_search_options_override
                    .clone(),
            },
            pattern_type: self.pattern_type,
            ..ZoektParameters::default()
        }
    }
}

fn zoekt_repo_options(options: &TreeRepoOptions) -> sg_zoekt::global_query::RepoOptions {
    sg_zoekt::global_query::RepoOptions {
        visibility: match options.visibility {
            RepoVisibility::Any => sg_zoekt::global_query::RepoVisibility::Any,
            RepoVisibility::Private => sg_zoekt::global_query::RepoVisibility::Private,
            RepoVisibility::Public => sg_zoekt::global_query::RepoVisibility::Public,
        },
        minus_repo_filters: options.minus_repo_filters.clone(),
        only_archived: options.only_archived,
        no_archived: options.no_archived,
        only_forks: options.only_forks,
        no_forks: options.no_forks,
    }
}

/// `jobBuilder.newZoektGlobalSearch` (`job.go:858-909`).
fn new_zoekt_global_search(
    builder: &JobBuilder,
    basic: &Basic,
    repo_options: &TreeRepoOptions,
    typ: IndexedRequestType,
) -> Result<JobNode, JobError> {
    let zoekt_query = zoekt_query::from_basic(
        basic,
        builder.zoekt_result_types(),
        &builder.zoekt_features(),
        typ,
    )
    .map_err(|error| JobError(error.to_string()))?;

    let default_scope = default_global_query_scope(&zoekt_repo_options(repo_options))
        .map_err(|error| JobError(error.to_string()))?;

    let include_private = matches!(
        repo_options.visibility,
        RepoVisibility::Private | RepoVisibility::Any
    );
    let global = GlobalZoektQuery::new(zoekt_query, default_scope, include_private);
    let zoekt_params = builder.zoekt_parameters(typ);
    let global_repo_opts = zoekt_repo_options(repo_options);

    Ok(match typ {
        IndexedRequestType::Symbol => JobNode::leaf(NodeKind::ZoektGlobalSymbol {
            global,
            zoekt_params,
            repo_opts: global_repo_opts,
            tree_repo_opts: repo_options.clone(),
        }),
        IndexedRequestType::Text => {
            // `zoektQueryPatternsAsRegexps(globalZoektQuery.Query)`.
            let regexps = regexp_patterns(jb::zoekt_query_patterns_as_regexps(&global.query));
            JobNode::leaf(NodeKind::ZoektGlobalText {
                global,
                zoekt_params,
                repo_opts: global_repo_opts,
                global_zoekt_query_regexps: regexps,
                tree_repo_opts: repo_options.clone(),
            })
        }
    })
}

/// `jobBuilder.newZoektSearch` (`job.go:911-936`).
fn new_zoekt_search(
    builder: &JobBuilder,
    basic: &Basic,
    typ: IndexedRequestType,
) -> Result<JobNode, JobError> {
    let zoekt_query = zoekt_query::from_basic(
        basic,
        builder.zoekt_result_types(),
        &builder.zoekt_features(),
        typ,
    )
    .map_err(|error| JobError(error.to_string()))?;

    let zoekt_params = builder.zoekt_parameters(typ);

    Ok(match typ {
        IndexedRequestType::Symbol => JobNode::leaf(NodeKind::ZoektSymbol {
            repos: None,
            query: zoekt_query,
            zoekt_params,
        }),
        IndexedRequestType::Text => {
            // `ZoektQueryRegexps: zoektQueryPatternsAsRegexps(zoektQuery)`.
            let regexps = regexp_patterns(jb::zoekt_query_patterns_as_regexps(&zoekt_query));
            JobNode::leaf(NodeKind::ZoektRepoSubsetText {
                repos: None,
                query: zoekt_query,
                zoekt_query_regexps: regexps,
                zoekt_params,
            })
        }
    })
}

/// `NewFlatJob` (`job.go:361-520`).
pub fn new_flat_job(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    let default_limit = inputs.default_limit();
    let max_results = jb::parameters_max_results(&basic.parameters, default_limit);
    let result_types =
        jb::compute_result_types(basic, inputs.pattern_type, jb::DEFAULT_RESULT_TYPES);
    let job_repo_options = jb::to_repo_options(basic, &inputs.user_settings);
    let (_, skip_repo_subset_search, _) = jb::job_mode(
        basic,
        &job_repo_options,
        result_types,
        &jb::SearchInputs {
            protocol: inputs.protocol,
            pattern_type: inputs.pattern_type,
            on_sourcegraph_dot_com: inputs.on_sourcegraph_dot_com,
        },
    );
    let mut repo_options = TreeRepoOptions::from_job_builders(&job_repo_options);
    repo_options.request_context = inputs.request_context;
    let contains_ref_globs = contains_ref_globs(basic);

    let mut all_jobs = Vec::new();

    if result_types.has(jb::ResultTypes::SYMBOL) && !skip_repo_subset_search {
        let request = jb::to_symbol_search_request(basic, &inputs.features).map_err(JobError)?;
        let symbol_job = JobNode::leaf(NodeKind::SearcherSymbol {
            request,
            limit: max_results,
            repos: Vec::new(),
        });
        all_jobs.push(repo_pager(
            symbol_job,
            repo_options.clone(),
            contains_ref_globs,
        ));
    }

    if result_types.has(jb::ResultTypes::STRUCTURAL) {
        let pattern_info =
            jb::to_text_pattern_info(basic, result_types, &inputs.features, default_limit)
                .map_err(JobError)?;
        // `useFullDeadline = f.GetTimeout() != nil || f.Count() != nil ||
        // searchInputs.Protocol != search.Batch` (`job.go:366-367`).
        let use_full_deadline = jb::parameters_timeout_nanos(&basic.parameters).is_some()
            || jb::parameters_count(&basic.parameters).is_some()
            || inputs.protocol != Protocol::Batch;
        let structural_job = JobNode::leaf(NodeKind::Structural {
            pattern_info,
            use_full_deadline,
            use_index: basic.parameters.index(),
            batch_retry: inputs.protocol == Protocol::Batch,
            unindexed: Vec::new(),
            indexed: None,
        });
        all_jobs.push(repo_pager(
            structural_job,
            repo_options.clone(),
            contains_ref_globs,
        ));
    }

    if result_types.has(jb::ResultTypes::REPO) && repo_search_valid(basic) {
        let (updated, ok) = add_pattern_as_repo_filter(
            &basic.pattern_string(),
            repo_options.clone(),
            basic.is_case_sensitive(),
        );
        if ok {
            repo_options = updated;
            let repo_name_patterns = repo_options
                .repo_filters
                .iter()
                .map(|filter| format!("(?i){}", filter.repo))
                .collect();
            all_jobs.push(JobNode::leaf(NodeKind::RepoSearch {
                repo_opts: repo_options.clone(),
                repo_name_patterns,
            }));
        }
    }

    Ok(new_parallel_job(all_jobs))
}

/// `NewFlatJob`'s repo field allowlist.
fn repo_search_valid(basic: &Basic) -> bool {
    const ALLOWLIST: &[&str] = &[
        "repo",
        "context",
        "type",
        "",
        "index",
        "count",
        "timeout",
        "fork",
        "archived",
        "visibility",
        "case",
        "repohasfile",
        "repohascommitafter",
        "patterntype",
        "select",
    ];
    let mut valid = true;
    visit_parameter(&basic.to_parse_tree(), &mut |field, _, _, _| {
        if !ALLOWLIST.contains(&field) {
            valid = false;
        }
    });
    valid
}

/// `NewFlatJob`'s `addPatternAsRepoFilter`.
fn add_pattern_as_repo_filter(
    pattern: &str,
    mut options: TreeRepoOptions,
    case_sensitive: bool,
) -> (TreeRepoOptions, bool) {
    if pattern.is_empty() {
        return (options, true);
    }

    options.repo_filters = options.repo_filters.clone();
    options.case_sensitive_repo_filters = case_sensitive;

    let mut split = pattern.splitn(2, '@');
    let prefix = split.next().unwrap_or_default();
    let has_suffix = split.next().is_some();
    if !has_suffix || !prefix.is_empty() {
        match parse_repository_revisions(prefix) {
            Ok(filter) => {
                options.repo_filters.push(filter);
                (options, true)
            }
            Err(_) => (TreeRepoOptions::default(), false),
        }
    } else {
        // Pattern starts with `@`: cannot be used as a repo filter.
        (TreeRepoOptions::default(), false)
    }
}

/// `toAndJob` (`job.go:1006-1029`).
fn to_and_job(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    let Node::Operator(operator) = basic.pattern.as_ref().expect("And requires operator") else {
        unreachable!("toAndJob requires an Operator pattern");
    };
    let operands = operator.operands.clone();
    let mut jobs = Vec::with_capacity(operands.len());
    for operand in operands {
        let mapped = basic.map_pattern(operand);
        let child = to_pattern_expression_job(inputs, &mapped)?;
        jobs.push(new_limit_job(40000, child));
    }
    Ok(new_and_job(jobs))
}

/// `toOrJob` (`job.go:1031-1043`).
fn to_or_job(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    let Node::Operator(operator) = basic.pattern.as_ref().expect("Or requires operator") else {
        unreachable!("toOrJob requires an Operator pattern");
    };
    let operands = operator.operands.clone();
    let mut jobs = Vec::with_capacity(operands.len());
    for operand in operands {
        jobs.push(to_pattern_expression_job(
            inputs,
            &basic.map_pattern(operand),
        )?);
    }
    Ok(new_or_job(jobs))
}

/// `toPatternExpressionJob` (`job.go:1045-1070`).
fn to_pattern_expression_job(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    match &basic.pattern {
        Some(Node::Operator(operator)) => {
            if operator.operands.is_empty() {
                return Ok(new_noop_job());
            }
            match operator.kind {
                OperatorKind::And => to_and_job(inputs, basic),
                OperatorKind::Or => to_or_job(inputs, basic),
                OperatorKind::Concat => Err(JobError(
                    "unrecognized type query.Operator in evaluatePatternExpression".to_owned(),
                )),
            }
        }
        Some(Node::Pattern(_)) => new_flat_job(inputs, basic),
        Some(Node::Parameter(_)) => Ok(new_noop_job()),
        None => Err(JobError(
            "unrecognized type <nil> in evaluatePatternExpression".to_owned(),
        )),
    }
}

/// `toFlatJobs` (`job.go:1074-1081`).
fn to_flat_jobs(inputs: &TreeInputs, basic: &Basic) -> Result<JobNode, JobError> {
    if basic.pattern.is_none() {
        new_flat_job(inputs, basic)
    } else {
        to_pattern_expression_job(inputs, basic)
    }
}

// ---------------------------------------------------------------------------
// file:contains.content()
// ---------------------------------------------------------------------------

/// `Parameters.FileContainsContent` for the `file:contains.content()` predicate.
fn file_contains_content(basic: &Basic) -> Vec<String> {
    let mut include = Vec::new();
    let nodes = basic.parameters.to_nodes();
    visit_field(&nodes, FIELD_FILE, &mut |value, _negated, annotation| {
        if !annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        let Some((name, args)) = parse_as_predicate(value) else {
            return;
        };
        let canonical = match name.as_str() {
            "contains.content" | "has.content" => "contains.content",
            _ => return,
        };
        if canonical == "contains.content" {
            include.push(args);
        }
    });
    include
}

/// `NewFileContainsFilterJob` (`filter_file_contains.go:39-72`).
fn new_file_contains_filter(
    include_patterns: Vec<String>,
    original_pattern: Option<&Node>,
    case_sensitive: bool,
    child: JobNode,
) -> Result<JobNode, JobError> {
    let include_matchers = compile_contains_patterns(
        &include_patterns,
        case_sensitive,
        ContainsPatternStage::Include,
    )?;
    let pattern_node = original_pattern.map(node_to_filter_pattern);
    let original_patterns = pattern_node
        .as_ref()
        .map(patterns_in_tree)
        .unwrap_or_default();
    let original_matchers = compile_contains_patterns(
        &original_patterns,
        case_sensitive,
        ContainsPatternStage::Original,
    )?;

    Ok(JobNode::new(
        NodeKind::FileContains {
            original_patterns: original_matchers,
            filter_patterns: include_matchers,
            raw_filter_patterns: include_patterns,
            case_sensitive,
        },
        vec![child],
    ))
}

fn node_to_filter_pattern(node: &Node) -> FilterPatternNode {
    match node {
        Node::Pattern(pattern) => FilterPatternNode::Pattern(pattern.value.clone()),
        Node::Operator(operator) => FilterPatternNode::Operator(
            operator
                .operands
                .iter()
                .map(node_to_filter_pattern)
                .collect(),
        ),
        Node::Parameter(_) => {
            panic!("unknown pattern node type query.Parameter")
        }
    }
}

fn compile_contains_patterns(
    patterns: &[String],
    case_sensitive: bool,
    stage: ContainsPatternStage,
) -> Result<Vec<String>, JobError> {
    let mut matchers = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        let pattern = if case_sensitive {
            pattern.clone()
        } else {
            format!("(?i:{pattern})")
        };
        match Regex::new(&pattern) {
            Ok(regex) => matchers.push(regex.as_str().to_owned()),
            Err(error) => {
                return Err(JobError(
                    ContainsPatternError {
                        stage,
                        pattern,
                        message: error.to_string(),
                    }
                    .to_string(),
                ))
            }
        }
    }
    Ok(matchers)
}

// ---------------------------------------------------------------------------
// query.ContainsRefGlobs
// ---------------------------------------------------------------------------

fn contains_ref_globs(basic: &Basic) -> bool {
    let (filters, _) = repositories(basic);
    filters
        .iter()
        .any(|filter| filter.revs.iter().any(|rev| rev.has_ref_glob()))
}

fn repositories(basic: &Basic) -> (Vec<ParsedRepoFilter>, Vec<String>) {
    let mut repos = Vec::new();
    let mut negated = Vec::new();
    let nodes = basic.parameters.to_nodes();
    visit_field(&nodes, FIELD_REPO, &mut |value, is_negated, annotation| {
        if annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        if is_negated {
            negated.push(value.to_owned());
        } else {
            match parse_repository_revisions(value) {
                Ok(filter) => repos.push(filter),
                Err(error) => panic!("repo field {value:?} is an invalid regex: {error}"),
            }
        }
    });
    (repos, negated)
}

// ---------------------------------------------------------------------------
// orderRacingJobs
// ---------------------------------------------------------------------------

fn has_descendant(node: &JobNode, predicate: &dyn Fn(&NodeKind) -> bool) -> bool {
    if predicate(&node.kind) {
        return true;
    }
    node.children
        .iter()
        .any(|child| has_descendant(child, predicate))
}

fn map_tree(node: JobNode, map: &mut dyn FnMut(JobNode) -> JobNode) -> JobNode {
    let mapped_children = node
        .children
        .into_iter()
        .map(|child| map_tree(child, map))
        .collect();
    let mapped = JobNode {
        kind: node.kind,
        children: mapped_children,
    };
    map(mapped)
}

/// `orderRacingJobs` (`job.go:306-358`).
fn order_racing_jobs(root: JobNode) -> JobNode {
    let mut collection = Vec::new();

    let mut first = map_tree(root.clone(), &mut |node| {
        if matches!(node.kind, NodeKind::RepoPager { .. })
            && has_descendant(&node, &|kind| matches!(kind, NodeKind::SearcherText { .. }))
        {
            collection.push(node);
            new_noop_job()
        } else {
            node
        }
    });

    first = map_tree(first, &mut |node| {
        if matches!(node.kind, NodeKind::RepoSearch { .. }) {
            collection.push(node);
            new_noop_job()
        } else {
            node
        }
    });

    if collection.is_empty() {
        return root;
    }

    let mut seen_zoekt_repo_search = false;
    let mut second = map_tree(first, &mut |node| {
        if matches!(node.kind, NodeKind::RepoPager { .. })
            && has_descendant(&node, &|kind| {
                matches!(kind, NodeKind::ZoektRepoSubsetText { .. })
            })
        {
            seen_zoekt_repo_search = true;
            let mut children = vec![node];
            children.extend(collection.clone());
            new_sequential_job(false, children)
        } else {
            node
        }
    });

    let mut seen_zoekt_global_search = false;
    second = map_tree(second, &mut |node| {
        if matches!(node.kind, NodeKind::ZoektGlobalText { .. }) && !seen_zoekt_global_search {
            seen_zoekt_global_search = true;
            let mut children = vec![node];
            children.extend(collection.clone());
            new_sequential_job(false, children)
        } else {
            node
        }
    });

    if !seen_zoekt_repo_search && !seen_zoekt_global_search {
        return root;
    }

    second
}

// ---------------------------------------------------------------------------
// commit.QueryToGitQuery + gitprotocol reduction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GitNode {
    Boolean(bool),
    AuthorMatches {
        expr: String,
        ignore_case: bool,
    },
    CommitterMatches {
        expr: String,
        ignore_case: bool,
    },
    CommitBefore {
        time: String,
    },
    CommitAfter {
        time: String,
    },
    MessageMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffModifiesFile {
        expr: String,
        ignore_case: bool,
    },
    Operator {
        kind: GitOperatorKind,
        operands: Vec<GitNode>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GitOperatorKind {
    And,
    Or,
    Not,
}

impl GitNode {
    fn cost(&self) -> f64 {
        match self {
            GitNode::Boolean(_) => 0.0,
            GitNode::CommitBefore { .. } | GitNode::CommitAfter { .. } => 1.0,
            GitNode::AuthorMatches { .. } | GitNode::CommitterMatches { .. } => 5.0,
            GitNode::MessageMatches { .. } => 10.0,
            GitNode::DiffModifiesFile { .. } => 1000.0,
            GitNode::DiffMatches { .. } => 10000.0,
            GitNode::Operator { operands, .. } => operands.iter().map(GitNode::cost).sum(),
        }
    }
}

impl std::fmt::Display for GitNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitNode::Boolean(value) => write!(f, "*protocol.Boolean({value})"),
            GitNode::AuthorMatches { expr, .. } => {
                write!(f, "*protocol.AuthorMatches({expr})")
            }
            GitNode::CommitterMatches { expr, .. } => {
                write!(f, "*protocol.CommitterMatches({expr})")
            }
            GitNode::CommitBefore { time } => write!(f, "*protocol.CommitBefore({time})"),
            GitNode::CommitAfter { time } => write!(f, "*protocol.CommitAfter({time})"),
            GitNode::MessageMatches { expr, .. } => {
                write!(f, "*protocol.MessageMatches({expr})")
            }
            GitNode::DiffMatches { expr, .. } => write!(f, "*protocol.DiffMatches({expr})"),
            GitNode::DiffModifiesFile { expr, .. } => {
                write!(f, "*protocol.DiffModifiesFile({expr})")
            }
            GitNode::Operator { kind, operands } => {
                let (separator, prefix) = match kind {
                    GitOperatorKind::And => (" AND ", ""),
                    GitOperatorKind::Or => (" OR ", ""),
                    GitOperatorKind::Not => (" AND NOT ", "NOT "),
                };
                let rendered: Vec<String> = operands
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect();
                write!(f, "({prefix}{})", rendered.join(separator))
            }
        }
    }
}

fn git_new_operator(kind: GitOperatorKind, operands: Vec<GitNode>) -> GitNode {
    GitNode::Operator { kind, operands }
}

fn git_new_and(operands: Vec<GitNode>) -> GitNode {
    match operands.len() {
        0 => GitNode::Boolean(true),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitNode::Operator {
                        kind: GitOperatorKind::And,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            git_new_operator(GitOperatorKind::And, flattened)
        }
    }
}

fn git_new_or(operands: Vec<GitNode>) -> GitNode {
    match operands.len() {
        0 => GitNode::Boolean(false),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitNode::Operator {
                        kind: GitOperatorKind::Or,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            git_new_operator(GitOperatorKind::Or, flattened)
        }
    }
}

fn git_new_not(operand: GitNode) -> GitNode {
    match operand {
        GitNode::Operator {
            kind: GitOperatorKind::Not,
            operands,
        } => operands.into_iter().next().expect("one operand"),
        other => git_new_operator(GitOperatorKind::Not, vec![other]),
    }
}

/// `QueryToGitQuery` (`internal/search/commit/commit.go:213-236`).
///
/// Shared with [`crate::search_commit_job`], which translates the resulting
/// tree to the gitserver `QueryNode` proto.
pub(crate) fn query_to_git_query(basic: &Basic, diff: bool) -> GitNode {
    let case_sensitive = basic.is_case_sensitive();
    let mut res = Vec::with_capacity(basic.parameters.0.len() + 2);

    for parameter in &basic.parameters.0 {
        if parameter.annotation.labels.is_set(Labels::IS_PREDICATE) {
            continue;
        }
        if let Some(predicate) = query_parameter_to_predicate(parameter, case_sensitive, diff) {
            res.push(predicate);
        }
    }

    if let Some(predicate) = basic
        .pattern
        .as_ref()
        .and_then(|pattern| query_pattern_to_predicate(pattern, case_sensitive, diff))
    {
        res.push(predicate);
    }

    reduce(git_new_and(res))
}

fn query_pattern_to_predicate(node: &Node, case_sensitive: bool, diff: bool) -> Option<GitNode> {
    match node {
        Node::Operator(operator) => {
            let operands: Vec<GitNode> = operator
                .operands
                .iter()
                .filter_map(|operand| query_pattern_to_predicate(operand, case_sensitive, diff))
                .collect();
            Some(match operator.kind {
                OperatorKind::And => git_new_and(operands),
                OperatorKind::Or => git_new_or(operands),
                OperatorKind::Concat => return None,
            })
        }
        Node::Pattern(pattern) => {
            let expr = pattern.regexp_pattern();
            let predicate = if diff {
                GitNode::DiffMatches {
                    expr,
                    ignore_case: !case_sensitive,
                }
            } else {
                GitNode::MessageMatches {
                    expr,
                    ignore_case: !case_sensitive,
                }
            };
            Some(if pattern.negated {
                git_new_not(predicate)
            } else {
                predicate
            })
        }
        Node::Parameter(_) => None,
    }
}

fn query_parameter_to_predicate(
    parameter: &sg_search_query::node::Parameter,
    case_sensitive: bool,
    diff: bool,
) -> Option<GitNode> {
    let predicate = match parameter.field.as_str() {
        "author" => GitNode::AuthorMatches {
            expr: parameter.value.clone(),
            ignore_case: !case_sensitive,
        },
        "committer" => GitNode::CommitterMatches {
            expr: parameter.value.clone(),
            ignore_case: !case_sensitive,
        },
        "before" => GitNode::CommitBefore {
            time: parameter.value.clone(),
        },
        "after" => GitNode::CommitAfter {
            time: parameter.value.clone(),
        },
        "message" => GitNode::MessageMatches {
            expr: parameter.value.clone(),
            ignore_case: !case_sensitive,
        },
        "content" => {
            if diff {
                GitNode::DiffMatches {
                    expr: parameter.value.clone(),
                    ignore_case: !case_sensitive,
                }
            } else {
                GitNode::MessageMatches {
                    expr: parameter.value.clone(),
                    ignore_case: !case_sensitive,
                }
            }
        }
        "file" => GitNode::DiffModifiesFile {
            expr: parameter.value.clone(),
            ignore_case: !case_sensitive,
        },
        "lang" => GitNode::DiffModifiesFile {
            expr: lang_to_file_regexp(&parameter.value),
            ignore_case: true,
        },
        _ => return None,
    };

    Some(if parameter.negated {
        git_new_not(predicate)
    } else {
        predicate
    })
}

fn reduce(node: GitNode) -> GitNode {
    let node = match node {
        GitNode::Operator { kind, operands } => GitNode::Operator {
            kind,
            operands: operands.into_iter().map(reduce).collect(),
        },
        other => other,
    };

    let node = propagate_boolean(node);
    let node = rewrite_conjunctive(node);
    let node = flatten(node);
    let node = merge_or_regexp(node);
    sort_and_by_cost(node)
}

fn propagate_boolean(node: GitNode) -> GitNode {
    let GitNode::Operator { kind, operands } = node else {
        return node;
    };
    match kind {
        GitOperatorKind::Not => {
            if let Some(GitNode::Boolean(value)) = operands.first() {
                GitNode::Boolean(!value)
            } else {
                GitNode::Operator { kind, operands }
            }
        }
        GitOperatorKind::And => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitNode::Boolean(false) => return GitNode::Boolean(false),
                    GitNode::Boolean(true) => {}
                    other => filtered.push(other),
                }
            }
            git_new_operator(GitOperatorKind::And, filtered)
        }
        GitOperatorKind::Or => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitNode::Boolean(true) => return GitNode::Boolean(true),
                    GitNode::Boolean(false) => {}
                    other => filtered.push(other),
                }
            }
            git_new_operator(GitOperatorKind::Or, filtered)
        }
    }
}

fn rewrite_conjunctive(node: GitNode) -> GitNode {
    let GitNode::Operator {
        kind: GitOperatorKind::Or,
        operands,
    } = node
    else {
        return node;
    };

    let mut and_operands: Vec<Vec<GitNode>> = Vec::new();
    let mut siblings = Vec::new();
    for operand in operands {
        match operand {
            GitNode::Operator {
                kind: GitOperatorKind::And,
                operands,
            } => and_operands.push(operands),
            other => siblings.push(other),
        }
    }

    if and_operands.is_empty() {
        return GitNode::Operator {
            kind: GitOperatorKind::Or,
            operands: siblings,
        };
    }

    let distributed = distribute(&and_operands, siblings);
    let new_and_operands = distributed
        .into_iter()
        .map(|row| git_new_operator(GitOperatorKind::Or, row))
        .collect();
    git_new_operator(GitOperatorKind::And, new_and_operands)
}

fn distribute(prefixes: &[Vec<GitNode>], nodes: Vec<GitNode>) -> Vec<Vec<GitNode>> {
    if prefixes.is_empty() {
        return vec![nodes];
    }

    let distributed = distribute(&prefixes[1..], nodes);
    let mut res = Vec::with_capacity(distributed.len() * prefixes[0].len());
    for prefix in &prefixes[0] {
        for row in &distributed {
            let mut new_row = row.clone();
            new_row.push(prefix.clone());
            res.push(new_row);
        }
    }
    res
}

fn flatten(node: GitNode) -> GitNode {
    let GitNode::Operator { kind, operands } = node else {
        return node;
    };
    if kind == GitOperatorKind::Not {
        return GitNode::Operator { kind, operands };
    }

    let mut flattened = Vec::with_capacity(operands.len());
    for operand in operands {
        match operand {
            GitNode::Operator {
                kind: nested_kind,
                operands,
            } if nested_kind == kind => flattened.extend(operands),
            other => flattened.push(other),
        }
    }
    git_new_operator(kind, flattened)
}

fn merge_or_regexp(node: GitNode) -> GitNode {
    let GitNode::Operator {
        kind: GitOperatorKind::Or,
        operands,
    } = node
    else {
        return node;
    };

    fn union(left: &str, right: &str) -> String {
        format!("(?:{left})|(?:{right})")
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum Key {
        Author(bool),
        Committer(bool),
        Message(bool),
        Diff(bool),
        DiffModifiesFile(bool),
    }

    let mut unmergeable = Vec::new();
    let mut mergeable: BTreeMap<(u8, bool), GitNode> = BTreeMap::new();
    for operand in operands {
        let (kind_rank, ignore_case, key) = match &operand {
            GitNode::AuthorMatches { ignore_case, .. } => {
                (0, *ignore_case, Key::Author(*ignore_case))
            }
            GitNode::CommitterMatches { ignore_case, .. } => {
                (1, *ignore_case, Key::Committer(*ignore_case))
            }
            GitNode::MessageMatches { ignore_case, .. } => {
                (2, *ignore_case, Key::Message(*ignore_case))
            }
            GitNode::DiffMatches { ignore_case, .. } => (3, *ignore_case, Key::Diff(*ignore_case)),
            GitNode::DiffModifiesFile { ignore_case, .. } => {
                (4, *ignore_case, Key::DiffModifiesFile(*ignore_case))
            }
            _ => {
                unmergeable.push(operand);
                continue;
            }
        };
        let _ = key;
        let map_key = (kind_rank, ignore_case);
        match (mergeable.remove(&map_key), operand) {
            (
                Some(GitNode::AuthorMatches { expr: prev, .. }),
                GitNode::AuthorMatches { expr, .. },
            ) => {
                mergeable.insert(
                    map_key,
                    GitNode::AuthorMatches {
                        expr: union(&prev, &expr),
                        ignore_case,
                    },
                );
            }
            (
                Some(GitNode::CommitterMatches { expr: prev, .. }),
                GitNode::CommitterMatches { expr, .. },
            ) => {
                mergeable.insert(
                    map_key,
                    GitNode::CommitterMatches {
                        expr: union(&prev, &expr),
                        ignore_case,
                    },
                );
            }
            (
                Some(GitNode::MessageMatches { expr: prev, .. }),
                GitNode::MessageMatches { expr, .. },
            ) => {
                mergeable.insert(
                    map_key,
                    GitNode::MessageMatches {
                        expr: union(&prev, &expr),
                        ignore_case,
                    },
                );
            }
            (Some(GitNode::DiffMatches { expr: prev, .. }), GitNode::DiffMatches { expr, .. }) => {
                mergeable.insert(
                    map_key,
                    GitNode::DiffMatches {
                        expr: union(&prev, &expr),
                        ignore_case,
                    },
                );
            }
            (
                Some(GitNode::DiffModifiesFile { expr: prev, .. }),
                GitNode::DiffModifiesFile { expr, .. },
            ) => {
                mergeable.insert(
                    map_key,
                    GitNode::DiffModifiesFile {
                        expr: union(&prev, &expr),
                        ignore_case,
                    },
                );
            }
            (None, operand) => {
                mergeable.insert(map_key, operand);
            }
            (Some(previous), _) => {
                mergeable.insert(map_key, previous);
            }
        }
    }

    let mut res = unmergeable;
    res.extend(mergeable.into_values());
    git_new_operator(GitOperatorKind::Or, res)
}

fn sort_and_by_cost(mut node: GitNode) -> GitNode {
    if let GitNode::Operator {
        kind: GitOperatorKind::And,
        operands,
    } = &mut node
    {
        operands.sort_by(|left, right| {
            left.cost()
                .partial_cmp(&right.cost())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    node
}

// ---------------------------------------------------------------------------
// Debug helper for the harness/overview
// ---------------------------------------------------------------------------

impl JobNode {
    /// Recursive `(name, attributes, children)` view used by the differential
    /// harness.
    pub fn describe(&self) -> (&'static str, Vec<(String, String)>, Vec<&JobNode>) {
        (
            self.name_str(),
            self.basic_attributes(),
            self.children.iter().collect(),
        )
    }
}

#[cfg(test)]
mod pager_tests {
    use std::cell::Cell;

    use super::*;
    use crate::search_job::{JobStreamer, PrivateRepoLister, RepoPagerClients, ReposListOptions};
    use crate::search_jobutil_repos::{RepoPage, RepoPageSource, ZoektRepoLister};
    use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions, RepositoryRevisionsRepo};
    use sg_zoekt::types::{ListResponse, SearchOptions, SearchResult};

    struct NoDb;
    impl PrivateRepoLister for NoDb {
        fn list_minimal_repos(
            &self,
            _options: &ReposListOptions,
        ) -> Result<Vec<MinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    struct NoStreamer;
    impl JobStreamer for NoStreamer {
        fn stream_search(
            &self,
            _query: &zoekt_query::Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            Ok(())
        }
    }

    struct Pages {
        pages: Vec<RepoPage>,
        error: Option<JobError>,
    }
    impl RepoPageSource for Pages {
        fn pages(&self) -> Vec<RepoPage> {
            self.pages.clone()
        }
        fn error(&self) -> Option<JobError> {
            self.error.clone()
        }
    }

    struct Lister {
        calls: Cell<usize>,
    }
    impl ZoektRepoLister for Lister {
        fn list(&self) -> Result<ListResponse, JobError> {
            self.calls.set(self.calls.get() + 1);
            Ok(ListResponse {
                repos: Vec::new(),
                repos_map: std::collections::BTreeMap::new(),
                crashes: 0,
                stats: Default::default(),
            })
        }
    }

    fn revision(id: u32, name: &str) -> RepositoryRevisions {
        RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id,
                name: name.to_owned(),
            },
            revs: vec!["HEAD".to_owned()],
        }
    }

    fn page(repo_id: u32, backends_missing: i64) -> RepoPage {
        RepoPage {
            repo_revs: vec![revision(repo_id, "repo")],
            backends_missing,
        }
    }

    fn static_inner() -> JobNode {
        JobNode::static_events(
            vec![StaticEvent {
                results: Vec::new(),
                is_limit_hit: false,
                status: vec![(7, 1)],
            }],
            None,
        )
    }

    fn run_pager(
        pages: &Pages,
        lister: &Lister,
        skip_partitioning: bool,
        contains_ref_globs: bool,
        with_clients: bool,
    ) -> (Vec<SearchEvent>, Option<comb::JobFailure>) {
        let mut job = TreeRepoPagerJob {
            repo_opts: TreeRepoOptions::default(),
            contains_ref_globs,
            skip_partitioning,
            inner: static_inner(),
        };
        let pager = RepoPagerClients {
            pages,
            zoekt: lister,
            descriptions: None,
        };
        let db = NoDb;
        let streamer = NoStreamer;
        let clients = RuntimeClients {
            db: &db,
            zoekt: &streamer,
            max_repos: None,
            searcher: None,
            commit: None,
            repo_pager: if with_clients { Some(&pager) } else { None },
        };
        let ctx = comb::RunContext::default();
        let mut events = Vec::new();
        let mut sink = |event: SearchEvent| events.push(event);
        let (_alert, failure) = comb::CombinatorJob::run(&mut job, &ctx, &clients, &mut sink);
        (events, failure)
    }

    #[test]
    fn runs_inner_once_per_page_and_forwards_page_stats() {
        let pages = Pages {
            pages: vec![page(1, 2), page(2, 0)],
            error: None,
        };
        let lister = Lister {
            calls: Cell::new(0),
        };
        let (events, failure) = run_pager(&pages, &lister, true, false, true);
        assert!(failure.is_none());
        // `skipPartitioning` never consults the zoekt list.
        assert_eq!(lister.calls.get(), 0);
        // `MaybeSendStats` for page 1, then one inner event per page.
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].stats.backends_missing, 2);
        assert!(events[0].stats.status.entries().is_empty());
        assert_eq!(events[1].stats.status.entries(), vec![(7, 1)]);
        assert_eq!(events[2].stats.status.entries(), vec![(7, 1)]);
    }

    #[test]
    fn partitions_each_page_against_the_zoekt_list() {
        let pages = Pages {
            pages: vec![page(1, 0), page(2, 0)],
            error: None,
        };
        let lister = Lister {
            calls: Cell::new(0),
        };
        let (events, failure) = run_pager(&pages, &lister, false, false, true);
        assert!(failure.is_none());
        assert_eq!(lister.calls.get(), 2);
        assert_eq!(events.len(), 2);

        // Ref-globs force the unindexed fallback without a list call.
        let lister = Lister {
            calls: Cell::new(0),
        };
        let (events, failure) = run_pager(&pages, &lister, false, true, true);
        assert!(failure.is_none());
        assert_eq!(lister.calls.get(), 0);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn propagates_terminal_page_error() {
        let pages = Pages {
            pages: Vec::new(),
            error: Some(JobError("page failed".to_owned())),
        };
        let lister = Lister {
            calls: Cell::new(0),
        };
        let (_events, failure) = run_pager(&pages, &lister, true, false, true);
        assert_eq!(
            failure,
            Some(comb::JobFailure::Message("page failed".to_owned()))
        );
    }

    #[test]
    fn without_clients_keeps_the_explicit_error() {
        let pages = Pages {
            pages: vec![page(1, 0)],
            error: None,
        };
        let lister = Lister {
            calls: Cell::new(0),
        };
        let (events, failure) = run_pager(&pages, &lister, true, false, false);
        assert!(events.is_empty());
        assert_eq!(
            failure,
            Some(comb::JobFailure::Message(
                "RepoPagerJob execution is not ported (M9.8hw)".to_owned()
            ))
        );
    }

    #[test]
    fn set_tree_repos_assigns_indexed_and_unindexed_sets() {
        let tree = JobNode::new(
            NodeKind::Parallel,
            vec![
                JobNode::zoekt_repo_subset_text(
                    Some(IndexedRepoRevs::default()),
                    zoekt_query::Query::Const(true),
                    Vec::new(),
                    ZoektParameters::default(),
                ),
                JobNode::zoekt_symbol(
                    Some(IndexedRepoRevs::default()),
                    zoekt_query::Query::Const(true),
                    ZoektParameters::default(),
                ),
                JobNode::commit(pb::QueryNode::default(), vec![revision(9, "old")], false, 1),
            ],
        );
        let mut indexed = IndexedRepoRevs::default();
        indexed.repo_revs.insert(1, revision(1, "indexed"));
        let resolved = ResolvedRepos::new(Some(indexed), vec![revision(2, "unindexed")]);
        let rewritten = set_tree_repos(&tree, &resolved);

        match &rewritten.children[0].kind {
            NodeKind::ZoektRepoSubsetText { repos, .. } => {
                let repos = repos.as_ref().expect("indexed set");
                assert_eq!(repos.repo_revs.len(), 1);
                assert!(repos.repo_revs.contains_key(&1));
            }
            other => panic!("unexpected text node: {other:?}"),
        }
        match &rewritten.children[1].kind {
            NodeKind::ZoektSymbol { repos, .. } => {
                let repos = repos.as_ref().expect("indexed set");
                assert_eq!(repos.repo_revs.len(), 1);
            }
            other => panic!("unexpected symbol node: {other:?}"),
        }
        match &rewritten.children[2].kind {
            NodeKind::Commit { repos, .. } => assert_eq!(repos, &vec![revision(2, "unindexed")]),
            other => panic!("unexpected commit node: {other:?}"),
        }
    }
}
