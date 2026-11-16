//! Zoekt search job structs (`internal/search/zoekt/indexed_search.go:589-724`,
//! `internal/search/zoekt/symbol_search.go:16-83`) and the minimal
//! `internal/search/job/job.go:27-77` interface subset.
//!
//! The three jobs here are thin wrappers around the already-ported execution
//! functions [`zoekt_search::zoekt_search`] and
//! [`zoekt_search::do_zoekt_search_global`] (M9.8gz). This module only adds:
//!
//! * [`SearchJob`] / [`Verbosity`] / [`JobContext`] / [`RuntimeClients`];
//! * the exact `Run` early-exit and error semantics of the baseline jobs;
//! * the `Name`/`Children`/`MapChildren`/`Attributes` descriptions, with
//!   OpenTelemetry `attribute.KeyValue`s normalized to `(key, value)` strings.
//!
//! Not ported: the job composition tree (`jobutil`), job registration,
//! GraphQL/HTTP wiring, real database/gRPC clients and tracing spans. The
//! baseline's `privateReposForActor` database read is injected through
//! [`PrivateRepoLister`] with `ListMinimalRepos` semantics.
//!
//! See `docs/overviews/m9_8hf_zoekt_search_jobs_overview.md`.

use std::fmt;
use std::time::{Duration, Instant};

use regex::Regex;
use sg_zoekt::global_query::{GlobalZoektQuery, RepoOptions};
use sg_zoekt::indexed_repo_revs::{IndexedRepoRevs, MinimalRepo};
use sg_zoekt::query::{IndexedRequestType, Query};
use sg_zoekt::types::{SearchOptions, SearchResult};

use crate::search_contexts::{get_global_search_context, SearchContext};
use crate::search_stream::progress::SearchEvent;
use crate::search_stream::zoekt_search::{self, ZoektParameters};

/// `job.Verbosity` (`internal/search/job/job.go:68-72`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Verbosity {
    /// No fields.
    #[default]
    None,
    /// Essential fields.
    Basic,
    /// All possible fields.
    Max,
}

/// The parts of `context.Context` these jobs consume: the deadline that
/// `zoektSearch` / `DoZoektSearchGlobal` turn into `SearchOptions.MaxWallTime`,
/// and the request actor (`actor.FromContext`) `privateReposForActor`
/// (`indexed_search.go:697-724`) uses for its `ListMinimalRepos` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JobContext {
    pub deadline: Option<Instant>,
    /// `actor.FromContext(ctx)`. Defaults to the zero actor for
    /// `context.Background()`, preserving the pre-M9.51b anonymous read.
    pub request_context: crate::search_contexts::RequestContext,
}

impl JobContext {
    /// `context.Background()`: anonymous actor and no deadline.
    pub fn anonymous() -> Self {
        Self::default()
    }
}

/// Error returned by a [`SearchJob`]. The baseline returns the underlying Go
/// error; this port keeps its `Display` text so differential output matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobError(pub String);

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JobError {}

/// `job.MapFunc`: applied to children by `MapChildren`. These 3 jobs have no
/// children, so [`SearchJob::map_children`] returns `self` without calling it.
pub type MapFunc = dyn Fn(&dyn SearchJob) -> Box<dyn SearchJob>;

/// Minimal `job.Job` + `job.Describer` subset (`internal/search/job/job.go:27-64`).
///
/// `Run` returns `()` where the baseline also returns a `*search.Alert`; none
/// of these jobs ever produces an alert.
pub trait SearchJob {
    /// `Job.Run`.
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError>;

    /// `Describer.Name`.
    fn name(&self) -> &'static str;

    /// `Describer.Children`; all jobs in this slice return an empty list.
    fn children(&self) -> Vec<&dyn SearchJob> {
        Vec::new()
    }

    /// `Job.MapChildren`; identity for a childless job.
    fn map_children(&self, _map: &MapFunc) -> &Self
    where
        Self: Sized,
    {
        self
    }

    /// `Describer.Attributes`, with OpenTelemetry attributes normalized to
    /// `(key, value)` strings (see the overview, §2).
    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)>;
}

/// The full-result streamer these jobs need. It mirrors
/// [`zoekt_search::Streamer`] but is object safe (associated error fixed to
/// [`JobError`]) so it can live behind `&dyn` in [`RuntimeClients`].
pub trait JobStreamer {
    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError>;
}

/// Adapts an object-safe [`JobStreamer`] to the generic
/// [`zoekt_search::Streamer`] used by the existing execution module.
struct StreamerAdapter<'a>(&'a dyn JobStreamer);

impl zoekt_search::Streamer for StreamerAdapter<'_> {
    type Error = JobError;

    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        self.0.stream_search(query, options, sender)
    }
}

/// Subset of `database.ReposListOptions` constructed by `privateReposForActor`.
/// `limit` is `SearchLimits(conf).MaxRepos + 1`; the caller injects the
/// configured maximum through [`RuntimeClients::max_repos`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReposListOptions {
    pub only_private: bool,
    pub no_private: bool,
    pub limit: Option<i64>,
    pub only_forks: bool,
    pub no_forks: bool,
    pub only_archived: bool,
    pub no_archived: bool,
    pub exclude_pattern: String,
    /// `IncludePatterns` (`database.ReposListOptions`). Used by
    /// `computeExcludedRepos`, not by `privateReposForActor`.
    pub include_patterns: Vec<String>,
    /// `SearchContextID` resolved by `searchcontexts.ResolveSearchContextSpec`.
    pub search_context_id: i64,
    /// `AuthzQueryConds`: authenticated user ID (`0` = anonymous). Filled from
    /// [`JobContext::request_context`] so the private-repo read applies the
    /// same visibility predicate as the baseline (`repos_perm.go:32-121`).
    pub authz_actor_uid: i32,
    /// `AuthzQueryConds`: internal actors bypass the repository predicate.
    pub authz_internal: bool,
}

/// Read-only `db.Repos().ListMinimalRepos`/`Repos().Count` semantics used by
/// `privateReposForActor` (`indexed_search.go:697-724`) and
/// `computeExcludedRepos` (`repos.go:1004-1089`).
pub trait PrivateRepoLister {
    fn list_minimal_repos(&self, options: &ReposListOptions) -> Result<Vec<MinimalRepo>, JobError>;

    /// `db.Repos().Count`. The default keeps list-only fakes (`M9.8*` seams,
    /// tests) working: `ComputeExcluded` then reports the explicit seam error
    /// instead of fabricating a count.
    fn count_repos(&self, _options: &ReposListOptions) -> Result<i64, JobError> {
        Err(JobError(
            "db.Repos().Count is not ported (M9.37a)".to_owned(),
        ))
    }

    /// `db.ExternalServices().Count(kinds=CodeHost kinds)`
    /// (`searchalert.needsRepositoryConfiguration`,
    /// `internal/search/alert/observer.go:302-317`). The default keeps
    /// list-only fakes working: `alert_for_no_resolved_repos` then skips the
    /// "no repositories or code hosts configured" branch, exactly like Go
    /// does when the count read returns an error.
    fn count_code_host_external_services(&self) -> Result<i64, JobError> {
        Err(JobError(
            "db.ExternalServices().Count is not ported (M9.39a)".to_owned(),
        ))
    }

    /// `auth.CheckCurrentUserIsSiteAdmin(ctx, db) == nil`
    /// (`internal/search/alert/observer.go:94`). `RuntimeClients` carries no
    /// actor, so the default models the anonymous actor (`false`).
    fn current_user_is_site_admin(&self) -> bool {
        false
    }

    /// `searchcontexts.ResolveSearchContextSpec(ctx, ...)`. The default
    /// resolves only the global/empty spec (which needs no database) and
    /// keeps the explicit seam error for everything else; production and
    /// differential fakes override it with a real `search_contexts` lookup.
    /// `ctx` carries the request actor's permission predicate.
    fn resolve_search_context(
        &self,
        spec: &str,
        _ctx: &crate::search_contexts::RequestContext,
    ) -> Result<SearchContext, JobError> {
        if crate::search_contexts::is_global_search_context_spec(spec) {
            return Ok(get_global_search_context());
        }
        Err(JobError(
            "search context resolution is not ported (M9.37a)".to_owned(),
        ))
    }
}

/// `repos.Resolver.Iterator` + `zoekt.Streamer.List` collaborators the
/// `RepoPagerJob` tree node needs (`repo_pager_job.go:19-27,95-136`).
pub struct RepoPagerClients<'a> {
    pub pages: &'a dyn crate::search_jobutil_repos::RepoPageSource,
    pub zoekt: &'a dyn crate::search_jobutil_repos::ZoektRepoLister,
    /// `db.Repos().GetRepoDescriptionsByIDs`; only read by the `RepoSearch`
    /// tree node when `description:` patterns are present. `None` makes that
    /// node report the explicit descriptions seam error.
    pub descriptions: Option<&'a dyn crate::search_jobutil_repos::RepoDescriptionLister>,
}

impl std::fmt::Debug for RepoPagerClients<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepoPagerClients").finish_non_exhaustive()
    }
}

/// `job.RuntimeClients`, reduced to what these jobs read.
pub struct RuntimeClients<'a> {
    /// `clients.DB` (only `Repos().ListMinimalRepos` is used).
    pub db: &'a dyn PrivateRepoLister,
    /// `clients.Zoekt`.
    pub zoekt: &'a dyn JobStreamer,
    /// `limits.SearchLimits(conf.Get()).MaxRepos`; `None` uses the baseline
    /// default `math.MaxInt32 >> 1`.
    pub max_repos: Option<i64>,
    /// Injected searcher (`index:no`) collaborators: the searcher service
    /// client/limiter, gitserver revision resolution and the symbols service.
    /// `None` means the searcher backend is not wired; the jobs in
    /// [`crate::search_searcher_jobs`] then fail with the explicit
    /// `searcher backend is not ported yet` error.
    ///
    /// Additive: the zoekt jobs in this module never read it.
    pub searcher: Option<&'a crate::search_searcher_client::SearcherClients<'a>>,
    /// Injected commit-search collaborators (`gitserver.Search` and the
    /// username-expansion DB reads). `None` means the commit/diff leaf is not
    /// configured; `JobNode::run` then returns the explicit per-kind
    /// not-ported error instead of panicking.
    ///
    /// Additive: the jobs in this module never read it.
    pub commit: Option<&'a crate::search_commit_job::CommitRuntimeClients<'a>>,
    /// Injected repository-pager collaborators (`repos.Resolver.Iterator` and
    /// `zoekt.Streamer.List`). `None` keeps the explicit
    /// `RepoPagerJob execution is not ported (M9.8hw)` error for tree nodes.
    ///
    /// Additive: no job in this module reads it.
    pub repo_pager: Option<&'a RepoPagerClients<'a>>,
}

impl RuntimeClients<'_> {
    fn max_repos_limit(&self) -> i64 {
        self.max_repos
            .unwrap_or(i64::from(i32::MAX >> 1))
            .saturating_add(1)
    }
}

/// `since func(time.Time) time.Duration` (`json:"-"`, test hook).
pub type SinceFn = Box<dyn Fn(Instant) -> Duration + Send + Sync>;

/// `RepoSubsetTextSearchJob` (`indexed_search.go:589-648`).
pub struct RepoSubsetTextSearchJob {
    /// `Repos`; `None` models a nil pointer, `Some(empty)` an empty set.
    pub repos: Option<IndexedRepoRevs>,
    pub query: Query,
    /// `ZoektQueryRegexps`: also passed to `zoektSearch` as path regexps.
    pub zoekt_query_regexps: Vec<Regex>,
    pub typ: IndexedRequestType,
    pub zoekt_params: ZoektParameters,
    pub since: Option<SinceFn>,
}

impl SearchJob for RepoSubsetTextSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        mut stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        // Baseline: nil Repos or an empty RepoRevs map returns (nil, nil)
        // without calling zoekt or sending any event.
        let Some(repos) = self.repos.as_ref() else {
            return Ok(());
        };
        if repos.repo_revs.is_empty() {
            return Ok(());
        }

        let streamer = StreamerAdapter(clients.zoekt);
        let since = |t0: Instant| match &self.since {
            Some(since) => since(t0),
            None => t0.elapsed(),
        };
        zoekt_search::zoekt_search(
            repos,
            self.query.clone(),
            &self.zoekt_query_regexps,
            self.typ,
            &streamer,
            &self.zoekt_params,
            ctx.deadline,
            since,
            &mut stream,
        )
        .map_err(|err| JobError(err.to_string()))
    }

    fn name(&self) -> &'static str {
        "ZoektRepoSubsetTextSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if verbosity == Verbosity::Max {
            res.push((
                "fileMatchLimit".to_owned(),
                self.zoekt_params.file_match_limit.to_string(),
            ));
            res.push(("select".to_owned(), self.zoekt_params.select.0.join(".")));
            res.push((
                "zoektQueryRegexps".to_owned(),
                string_slice(self.zoekt_query_regexps.iter().map(Regex::as_str)),
            ));
            // Repos is nil for un-indexed search.
            if let Some(repos) = &self.repos {
                res.push(("numRepoRevs".to_owned(), repos.repo_revs.len().to_string()));
                res.push((
                    "numBranchRepos".to_owned(),
                    repos.branch_repos.len().to_string(),
                ));
            }
        }
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            res.push(("query".to_owned(), self.query.to_string()));
            res.push(("type".to_owned(), request_type_str(self.typ).to_owned()));
        }
        res
    }
}

/// `GlobalTextSearchJob` (`indexed_search.go:650-694`).
pub struct GlobalTextSearchJob {
    pub global_zoekt_query: GlobalZoektQuery,
    pub zoekt_params: ZoektParameters,
    pub repo_opts: RepoOptions,
    pub global_zoekt_query_regexps: Vec<Regex>,
}

impl SearchJob for GlobalTextSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        mut stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let user_private_repos = private_repos_for_actor(ctx, clients, &self.repo_opts);
        self.global_zoekt_query
            .apply_private_filter(&user_private_repos);
        self.zoekt_params.query = self.global_zoekt_query.generate();

        let streamer = StreamerAdapter(clients.zoekt);
        zoekt_search::do_zoekt_search_global(
            &streamer,
            &self.zoekt_params,
            &self.global_zoekt_query_regexps,
            ctx.deadline,
            &mut stream,
        )
        .map_err(|err| JobError(err.to_string()))
    }

    fn name(&self) -> &'static str {
        "ZoektGlobalTextSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if verbosity == Verbosity::Max {
            res.push((
                "fileMatchLimit".to_owned(),
                self.zoekt_params.file_match_limit.to_string(),
            ));
            res.push(("select".to_owned(), self.zoekt_params.select.0.join(".")));
            res.push((
                "repoScope".to_owned(),
                string_slice(
                    self.global_zoekt_query
                        .repo_scope
                        .iter()
                        .map(Query::to_string),
                ),
            ));
            res.push((
                "includePrivate".to_owned(),
                self.global_zoekt_query.include_private.to_string(),
            ));
            res.push((
                "globalZoektQueryRegexps".to_owned(),
                string_slice(self.global_zoekt_query_regexps.iter().map(Regex::as_str)),
            ));
        }
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            res.push((
                "query".to_owned(),
                self.global_zoekt_query.query.to_string(),
            ));
            res.push((
                "type".to_owned(),
                request_type_str(self.zoekt_params.typ).to_owned(),
            ));
            res.extend(
                repo_options_attributes(&self.repo_opts)
                    .into_iter()
                    .map(|(key, value)| (format!("repoOpts.{key}"), value)),
            );
        }
        res
    }
}

/// `SymbolSearchJob` (`symbol_search.go:16-83`). The task description calls it
/// `RepoSubsetSymbolSearchJob`; the snapshot's actual type name is
/// `SymbolSearchJob`.
pub struct SymbolSearchJob {
    /// `Repos`; `None` models a nil pointer, `Some(empty)` an empty set.
    pub repos: Option<IndexedRepoRevs>,
    pub query: Query,
    pub zoekt_params: ZoektParameters,
    pub since: Option<SinceFn>,
}

impl SearchJob for SymbolSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        mut stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Some(repos) = self.repos.as_ref() else {
            return Ok(());
        };
        if repos.repo_revs.is_empty() {
            return Ok(());
        }

        let streamer = StreamerAdapter(clients.zoekt);
        let since = |t0: Instant| match &self.since {
            Some(since) => since(t0),
            None => t0.elapsed(),
        };
        // The baseline passes nil path regexps and `search.SymbolRequest`.
        let result = zoekt_search::zoekt_search(
            repos,
            self.query.clone(),
            &[],
            IndexedRequestType::Symbol,
            &streamer,
            &self.zoekt_params,
            ctx.deadline,
            since,
            &mut stream,
        );
        match result {
            Ok(()) => Ok(()),
            // "Only record error if we haven't timed out": when the context's
            // deadline has already passed, swallow the error.
            Err(err) => {
                if ctx
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    Ok(())
                } else {
                    Err(JobError(err.to_string()))
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "ZoektSymbolSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if verbosity == Verbosity::Max {
            res.push((
                "fileMatchLimit".to_owned(),
                self.zoekt_params.file_match_limit.to_string(),
            ));
            res.push(("select".to_owned(), self.zoekt_params.select.0.join(".")));
            // Repos is nil for un-indexed search.
            if let Some(repos) = &self.repos {
                res.push(("numRepoRevs".to_owned(), repos.repo_revs.len().to_string()));
                res.push((
                    "numBranchRepos".to_owned(),
                    repos.branch_repos.len().to_string(),
                ));
            }
        }
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            // The symbol job's Basic attributes contain no `type` field.
            res.push(("query".to_owned(), self.query.to_string()));
        }
        res
    }
}

/// `privateReposForActor` (`indexed_search.go:697-724`): build the
/// `ListMinimalRepos` options, call the injected read-only lister, and, on
/// error, log and fall back to the (empty) result. Errors never propagate.
fn private_repos_for_actor(
    ctx: &JobContext,
    clients: &RuntimeClients<'_>,
    repo_opts: &RepoOptions,
) -> Vec<(u32, String)> {
    let options = ReposListOptions {
        only_private: true,
        limit: Some(clients.max_repos_limit()),
        only_forks: repo_opts.only_forks,
        no_forks: repo_opts.no_forks,
        only_archived: repo_opts.only_archived,
        no_archived: repo_opts.no_archived,
        exclude_pattern: sg_zoekt::languages::union_regexps(&repo_opts.minus_repo_filters),
        // `actor.FromContext(ctx)` (`indexed_search.go:697-724`): the store
        // applies `AuthzQueryConds` for this actor. Anonymous (uid 0,
        // `internal=false`) keeps the historical predicate.
        authz_actor_uid: ctx.request_context.actor_uid,
        authz_internal: ctx.request_context.internal,
        ..ReposListOptions::default()
    };
    match clients.db.list_minimal_repos(&options) {
        Ok(repos) => repos.into_iter().map(|repo| (repo.id, repo.name)).collect(),
        Err(err) => {
            // The baseline logs `"doResults: failed to list user private
            // repos"` and continues with whatever the store returned.
            tracing::error!(
                error = %err,
                "doResults: failed to list user private repos"
            );
            Vec::new()
        }
    }
}

/// `RepoOptions.Attributes` (`internal/search/types.go:428-562`), restricted to
/// the fields this slice models. Unmodeled fields keep the Go zero value:
/// `visibility` `""` is emitted (it is not `query.Any`), `useIndex` `""` is
/// emitted, and `noForks`/`noArchived` are emitted with `false` by default.
fn repo_options_attributes(options: &RepoOptions) -> Vec<(String, String)> {
    let mut res = Vec::new();
    if !options.minus_repo_filters.is_empty() {
        res.push((
            "minusRepoFilters".to_owned(),
            string_slice(options.minus_repo_filters.iter().map(String::as_str)),
        ));
    }
    if options.visibility.as_str() != "Any" {
        res.push((
            "visibility".to_owned(),
            options.visibility.as_str().to_owned(),
        ));
    }
    // `UseIndex` is not modeled; the Go zero value `""` differs from `"yes"`.
    res.push(("useIndex".to_owned(), String::new()));
    if !options.no_forks {
        res.push(("noForks".to_owned(), options.no_forks.to_string()));
    }
    if options.only_forks {
        res.push(("onlyForks".to_owned(), options.only_forks.to_string()));
    }
    if !options.no_archived {
        res.push(("noArchived".to_owned(), options.no_archived.to_string()));
    }
    if options.only_archived {
        res.push(("onlyArchived".to_owned(), options.only_archived.to_string()));
    }
    res
}

fn request_type_str(typ: IndexedRequestType) -> &'static str {
    match typ {
        IndexedRequestType::Text => "text",
        IndexedRequestType::Symbol => "symbol",
    }
}

/// Normalize a Go `attribute.StringSlice` the way `fmt.Sprintf("%v", []string)`
/// does: `"[a b]"` (and `"[]"` when empty).
fn string_slice<I, S>(values: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let joined = values
        .into_iter()
        .map(|value| value.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopStreamer;

    impl JobStreamer for NoopStreamer {
        fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            panic!("stream_search must not be called for an empty repo set");
        }
    }

    struct NoopLister;

    impl PrivateRepoLister for NoopLister {
        fn list_minimal_repos(
            &self,
            _options: &ReposListOptions,
        ) -> Result<Vec<MinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    fn params() -> ZoektParameters {
        ZoektParameters::default()
    }

    fn clients<'a>(db: &'a NoopLister, zoekt: &'a NoopStreamer) -> RuntimeClients<'a> {
        RuntimeClients {
            db,
            zoekt,
            max_repos: None,
            searcher: None,
            commit: None,
            repo_pager: None,
        }
    }

    #[test]
    fn names_match_baseline() {
        let streamer = NoopStreamer;
        let lister = NoopLister;
        let clients = clients(&lister, &streamer);
        let text = RepoSubsetTextSearchJob {
            repos: None,
            query: Query::Const(true),
            zoekt_query_regexps: Vec::new(),
            typ: IndexedRequestType::Text,
            zoekt_params: params(),
            since: None,
        };
        let symbol = SymbolSearchJob {
            repos: None,
            query: Query::Const(true),
            zoekt_params: params(),
            since: None,
        };
        let global = GlobalTextSearchJob {
            global_zoekt_query: GlobalZoektQuery::new(Query::Const(true), None, false),
            zoekt_params: params(),
            repo_opts: RepoOptions::default(),
            global_zoekt_query_regexps: Vec::new(),
        };
        assert_eq!(text.name(), "ZoektRepoSubsetTextSearchJob");
        assert_eq!(symbol.name(), "ZoektSymbolSearchJob");
        assert_eq!(global.name(), "ZoektGlobalTextSearchJob");
        assert!(text.children().is_empty());
        assert!(symbol.children().is_empty());
        assert!(global.children().is_empty());
        assert!(std::ptr::eq(
            text.map_children(&|_| panic!("map closure must not run")),
            &text
        ));
        // Keep `clients` used so this test also pins the constructor shape.
        assert!(clients
            .db
            .list_minimal_repos(&ReposListOptions::default())
            .is_ok());
    }

    #[test]
    fn nil_and_empty_repos_do_not_stream() {
        let streamer = NoopStreamer;
        let lister = NoopLister;
        let clients = clients(&lister, &streamer);
        let ctx = JobContext::default();
        let mut events = Vec::new();
        let mut sink = |event| events.push(event);

        let mut text = RepoSubsetTextSearchJob {
            repos: None,
            query: Query::Const(true),
            zoekt_query_regexps: Vec::new(),
            typ: IndexedRequestType::Text,
            zoekt_params: params(),
            since: None,
        };
        text.run(&ctx, &clients, &mut sink).unwrap();
        text.repos = Some(IndexedRepoRevs::default());
        text.run(&ctx, &clients, &mut sink).unwrap();

        let mut symbol = SymbolSearchJob {
            repos: Some(IndexedRepoRevs::default()),
            query: Query::Const(true),
            zoekt_params: params(),
            since: None,
        };
        symbol.run(&ctx, &clients, &mut sink).unwrap();
        symbol.repos = None;
        symbol.run(&ctx, &clients, &mut sink).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn repo_options_attributes_follow_go_zero_values() {
        let attrs = repo_options_attributes(&RepoOptions::default());
        assert_eq!(
            attrs,
            vec![
                ("visibility".to_owned(), String::new()),
                ("useIndex".to_owned(), String::new()),
                ("noForks".to_owned(), "false".to_owned()),
                ("noArchived".to_owned(), "false".to_owned()),
            ]
        );
    }
}
