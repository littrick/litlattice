#![forbid(unsafe_code)]

//! Repository resolution (`internal/search/repos/repos.go`) — M9.10a.
//!
//! Ported:
//!
//! * [`Resolver::iterate`] (`Iterator`): page over `doQueryDB` +
//!   `doFilterDBResolved`, collecting `MissingRepoRevsError` per page and only
//!   returning it at the end; other errors stop iteration;
//! * [`Resolver::iterate_repo_revs`] (`IterateRepoRevs`): the DB-only page
//!   loop for exhaustive search;
//! * [`Resolver::resolve_rev_specs`] (`ResolveRevSpecs`): gitserver/searcher
//!   filtering of already-associated rev specs;
//! * [`Resolver::do_query_db`]: `findPatternRevs` → search-context
//!   resolution → `ReposListOptions` construction → N+1 cursor paging →
//!   `associateReposWithRevs` → Perforce changelist remapping;
//! * [`Resolver::do_filter_db_resolved`]: `filterGitserver`
//!   (`normalizeRefs` + `filterHasCommitAfter`) and
//!   `filterRepoHasFileContent` (indexed zoekt `List` + unindexed searcher).
//!
//! Everything external is injected: [`RepoResolverDb`], [`ResolverGitserver`],
//! [`ResolverZoektLister`] and [`ResolverSearcher`]. The production adapters
//! live in [`crate::search_runtime`]; tests use fakes. Ordering is
//! deterministic: the baseline's `conc/pool` results are indexed by input
//! position, and this port executes sequentially in the same order.
//!
//! `RevAtTime` is wired through [`ResolverGitserver::rev_at_time`] (M9.18b);
//! the production adapter in [`crate::search_runtime`] speaks the gitserver
//! `RevAtTime` RPC. Not ported (see
//! `docs/overviews/m9_10a_search_repos_resolver_overview.md` §4):
//! `computeExcludedRepos`, and the baseline's exact/LIKE SQL optimization (the
//! regexp path is semantically equivalent except for `%`/`_` literals inside
//! exact patterns).

use std::collections::{BTreeMap, BTreeSet};

use regex::{Regex, RegexBuilder};
use sg_db::repos::{
    RepoChangelistIDs, RepoKvpFilter as DbRepoKvpFilter, RepoListCursor, RepoListItem,
    RepoListOptions, RepoListSort, RepoTopicFilter,
};
use sg_search_query::basic::RepoHasFileContentArgs;
use sg_search_query::validate::YesNoOnly;
use sg_zoekt::indexed_repo_revs::{
    partition_repos, RepositoryRevisions, RepositoryRevisionsRepo,
};
use sg_zoekt::query::{query_for_file_content_args, Query};
use sg_zoekt::types::{ListOptions, ListResponse, RepoListField};
use time::OffsetDateTime;

use crate::search_contexts::{
    is_auto_defined_search_context, ParsedRepoFilter, RepoVisibility,
    RequestContext, RevisionSpecifier, SearchContext,
};
use crate::search_job::JobError;
use crate::search_jobutil_job_builders::{RepoHasCommitAfterArgs, RepoHasTopic, RepoKvpFilter};
use crate::search_jobutil_tree::TreeRepoOptions;
use crate::search_searcher_client::protocol;

/// `searchrepos.ErrNoResolvedRepos` (`repos.go:1213`).
pub const ERR_NO_RESOLVED_REPOS: &str = "no resolved repositories";
/// `MissingRepoRevsError.Error` (`repos.go:1228`).
pub const ERR_MISSING_REPO_REVS: &str = "missing repo revs";

/// `Resolved` (`repos.go:48-56`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    pub repo_revs: Vec<RepositoryRevisions>,
    pub backends_missing: i64,
}

/// `RepoRevSpecs` (`repos.go:1230-1247`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoRevSpecs {
    pub repo: RepositoryRevisionsRepo,
    pub revs: Vec<RevisionSpecifier>,
    /// `Repo.ExternalRepo.ServiceType`; read by the Perforce changelist
    /// remapping and otherwise unused.
    pub external_service_type: Option<String>,
}

impl RepoRevSpecs {
    /// `(*RepoRevSpecs).RevSpecs`: ref-glob revisions are omitted.
    pub fn rev_specs(&self) -> Vec<String> {
        self.revs
            .iter()
            .filter(|rev| rev.ref_glob.is_empty() && rev.exclude_ref_glob.is_empty())
            .map(|rev| rev.rev_spec.clone())
            .collect()
    }
}

/// `dbResolved` (`repos.go:216-221`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbResolved {
    pub associated: Vec<RepoRevSpecs>,
    pub missing: Vec<RepoRevSpecs>,
}

/// `search.RepoOptions` fields the resolver reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveOptions {
    pub repo_filters: Vec<ParsedRepoFilter>,
    pub minus_repo_filters: Vec<String>,
    pub description_patterns: Vec<String>,
    pub case_sensitive_repo_filters: bool,
    pub search_context_spec: String,
    pub commit_after: Option<RepoHasCommitAfterArgs>,
    pub visibility: RepoVisibility,
    pub limit: i64,
    pub cursors: Vec<RepoListCursor>,
    pub use_index: YesNoOnly,
    pub has_file_content: Vec<RepoHasFileContentArgs>,
    pub has_kvps: Vec<RepoKvpFilter>,
    pub has_topics: Vec<RepoHasTopic>,
    pub no_forks: bool,
    pub only_forks: bool,
    pub only_cloned: bool,
    pub no_archived: bool,
    pub only_archived: bool,
    /// `ExperimentalFeatures.PerforceChangelistMapping == "enabled"`.
    pub perforce_changelist_mapping: bool,
    /// `actor.FromContext(ctx)` + `dotcom.SourcegraphDotComMode()` used by
    /// `ResolveSearchContextSpec`. The zero value is the anonymous actor.
    pub request_context: RequestContext,
}

impl Default for ResolveOptions {
    fn default() -> Self {
        Self {
            repo_filters: Vec::new(),
            minus_repo_filters: Vec::new(),
            description_patterns: Vec::new(),
            case_sensitive_repo_filters: false,
            search_context_spec: String::new(),
            commit_after: None,
            visibility: RepoVisibility::Any,
            limit: 0,
            cursors: Vec::new(),
            use_index: YesNoOnly::Yes,
            has_file_content: Vec::new(),
            has_kvps: Vec::new(),
            has_topics: Vec::new(),
            no_forks: false,
            only_forks: false,
            only_cloned: false,
            no_archived: false,
            only_archived: false,
            perforce_changelist_mapping: false,
            request_context: RequestContext::anonymous(),
        }
    }
}

/// `database.ReposListOptions` built by `doQueryDB`.
pub fn repo_list_options(
    options: &ResolveOptions,
    include_patterns: &[String],
    limit: i64,
) -> RepoListOptions {
    RepoListOptions {
        include_patterns: include_patterns.to_vec(),
        exclude_pattern: crate::search_contexts::union_regexps(&options.minus_repo_filters),
        description_patterns: options.description_patterns.clone(),
        case_sensitive_patterns: options.case_sensitive_repo_filters,
        kvp_filters: options
            .has_kvps
            .iter()
            .map(|filter| DbRepoKvpFilter {
                key: filter.key.clone(),
                value: filter.value.clone(),
                negated: filter.negated,
                key_only: filter.key_only,
            })
            .collect(),
        topic_filters: options
            .has_topics
            .iter()
            .map(|filter| RepoTopicFilter {
                topic: filter.topic.clone(),
                negated: filter.negated,
            })
            .collect(),
        cursors: options.cursors.clone(),
        limit,
        no_forks: options.no_forks,
        only_forks: options.only_forks,
        no_archived: options.no_archived,
        only_archived: options.only_archived,
        no_private: options.visibility == RepoVisibility::Public,
        only_private: options.visibility == RepoVisibility::Private,
        only_cloned: options.only_cloned,
        search_context_id: 0,
        order_by: vec![
            RepoListSort::stars_desc_nulls_last(),
            RepoListSort::id_desc(),
        ],
        // `AuthzQueryConds` reads the request actor (`ResolveOptions`
        // carries `actor.FromContext(ctx)`). The production DB adapter
        // overrides `authz_enforce_for_site_admins` from the live site
        // configuration; `false` is Go's default.
        authz_actor_uid: options.request_context.actor_uid,
        authz_internal: options.request_context.internal,
        authz_enforce_for_site_admins: false,
    }
}

/// `patternRevspec` (`repos.go:1122-1126`).
#[derive(Debug, Clone)]
pub struct PatternRevspec {
    include_pattern: Regex,
    revs: Vec<RevisionSpecifier>,
}

/// A `gitdomain.RefGlob` (`common.go:497-505`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefGlob {
    pub include: String,
    pub exclude: String,
}

/// `gitdomain.RefGlobs` (`common.go:507-583`).
#[derive(Debug, Clone, Default)]
pub struct RefGlobs {
    patterns: Vec<(Regex, bool)>,
}

impl RefGlobs {
    pub fn is_match(&self, reference: &str) -> bool {
        let mut matched = false;
        for (pattern, include) in &self.patterns {
            if *include == matched {
                continue;
            }
            if pattern.is_match(reference) {
                matched = *include;
            }
        }
        matched
    }
}

/// `gitdomain.CompileRefGlobs` (`common.go:518-567`).
pub fn compile_ref_globs(globs: &[RefGlob]) -> Result<RefGlobs, JobError> {
    let mut patterns = Vec::with_capacity(globs.len());
    for glob in globs {
        if !glob.exclude.is_empty() && !glob.exclude.starts_with("refs/") {
            return Err(JobError(format!(
                "git ref exclude glob must begin with \"refs/\" (got {:?})",
                glob.exclude
            )));
        }
        let include = if !glob.include.is_empty() {
            let mut include = glob.include.clone();
            if !include.starts_with("refs/") {
                include = format!("refs/{include}");
            }
            if !include.contains(['?', '*', '[']) {
                let suffix = if include.ends_with('/') { "*" } else { "/*" };
                include.push_str(&format!("{{,{suffix}}}"));
            }
            include
        } else {
            String::new()
        };
        let (pattern, is_include) = if !include.is_empty() {
            (include, true)
        } else {
            (glob.exclude.clone(), false)
        };
        for expanded in expand_braces(&pattern)? {
            patterns.push((compile_glob(&expanded)?, is_include));
        }
    }
    Ok(RefGlobs { patterns })
}

/// Expand `{a,b}` alternatives (the only brace syntax `CompileRefGlobs`
/// produces is `{,/*}`).
fn expand_braces(pattern: &str) -> Result<Vec<String>, JobError> {
    let Some(open) = pattern.find('{') else {
        return Ok(vec![pattern.to_owned()]);
    };
    let mut depth = 0usize;
    let mut close = None;
    for (index, ch) in pattern[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + index);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close) = close else {
        return Err(JobError(format!("invalid ref glob: {pattern:?}")));
    };
    let head = &pattern[..open];
    let body = &pattern[open + 1..close];
    let tail = &pattern[close + 1..];
    let mut results = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, ch) in body.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                results.extend(expand_braces(&format!("{head}{}{tail}", &body[start..index]))?);
                start = index + 1;
            }
            _ => {}
        }
    }
    results.extend(expand_braces(&format!("{head}{}{tail}", &body[start..]))?);
    Ok(results)
}

/// Translate one git glob (without braces) into an anchored regexp.
fn compile_glob(glob: &str) -> Result<Regex, JobError> {
    let mut regex = String::from("^");
    let chars: Vec<char> = glob.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        match chars[i] {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '[' => {
                let mut end = i + 1;
                while end < chars.len() && chars[end] != ']' {
                    end += 1;
                }
                if end >= chars.len() {
                    return Err(JobError(format!("invalid ref glob: {glob:?}")));
                }
                let body: String = chars[i + 1..end].iter().collect();
                let body = if let Some(rest) = body.strip_prefix('!') {
                    format!("^{rest}")
                } else {
                    body
                };
                regex.push('[');
                regex.push_str(&body);
                regex.push(']');
                i = end;
            }
            '\\' if i + 1 < chars.len() => {
                i += 1;
                regex.push_str(&regex::escape(&chars[i].to_string()));
            }
            ch => regex.push_str(&regex::escape(&ch.to_string())),
        }
        i += 1;
    }
    regex.push('$');
    Regex::new(&regex).map_err(|error| JobError(format!("invalid ref glob: {error}")))
}

// ---------------------------------------------------------------------------
// Injected collaborators
// ---------------------------------------------------------------------------

/// `database.DB` reads the resolver performs.
pub trait RepoResolverDb: Send + Sync {
    fn resolve_search_context(
        &self,
        spec: &str,
        ctx: &RequestContext,
    ) -> Result<SearchContext, JobError>;
    fn list_minimal_repos(&self, options: &RepoListOptions) -> Result<Vec<RepoListItem>, JobError>;
    fn get_search_context_repository_revisions(
        &self,
        id: i64,
    ) -> Result<Vec<crate::search_contexts::SearchContextRepositoryRevisions>, JobError>;
    fn batch_get_repo_commit_changelist(
        &self,
        repos: &[RepoChangelistIDs],
    ) -> Result<BTreeMap<u32, BTreeMap<i64, String>>, JobError>;

    /// `Repos().ListMinimalRepos(IDs: ids)` for the exhaustive searcher's
    /// `minimalRepo` lookup (`searcher.go:196-207`). The default keeps the
    /// explicit not-ported behavior for fakes that only implement the pager
    /// reads.
    fn list_minimal_repos_by_ids(&self, _ids: &[i32]) -> Result<Vec<RepoListItem>, JobError> {
        Err(JobError(
            "ListMinimalRepos by IDs is not ported for this resolver".to_owned(),
        ))
    }
}

/// A missing revision/repository (`*gitdomain.RevisionNotFoundError` /
/// `gitdomain.IsRepoNotExist`) versus a fatal error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitserverError {
    Missing,
    Fatal(String),
}

/// `gitserver.Client` methods the resolver calls.
pub trait ResolverGitserver: Send + Sync {
    /// `ResolveRevision(..., EnsureRevision:false)`.
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, GitserverError>;
    /// `ListRefs(repo, ListRefsOpts{})`.
    fn list_refs(&self, repo: &str) -> Result<Vec<String>, JobError>;
    /// `Commits{N:2, After, Ranges:[rev]}` length.
    fn commits_after(
        &self,
        repo: &str,
        rev: &str,
        after: OffsetDateTime,
    ) -> Result<usize, GitserverError>;
    /// `RevAtTime(repo, spec, date)` (`internal/gitserver/commands.go:631`):
    /// the OID of the nearest ancestor of `rev` with a committer time before
    /// `timestamp`, or `None` when the backend found no such commit.
    ///
    /// The default keeps older fakes compiling; the production adapter in
    /// [`crate::search_runtime`] overrides it with the real RPC.
    fn rev_at_time(
        &self,
        _repo: &str,
        _rev: &str,
        _timestamp: OffsetDateTime,
    ) -> Result<Option<String>, GitserverError> {
        Err(GitserverError::Fatal(
            "gitserver RevAtTime is not ported yet".to_owned(),
        ))
    }
}

/// `zoekt.Streamer.List` for `filterRepoHasFileContent`.
pub trait ResolverZoektLister: Send + Sync {
    fn list(&self, query: &Query, opts: Option<ListOptions>) -> Result<ListResponse, JobError>;
}

/// `searcher.Search` reduced to "did any file match?".
pub trait ResolverSearcher: Send + Sync {
    fn search(&self, request: &protocol::Request) -> Result<bool, JobError>;
}

/// `repos.Resolver` (`repos.go:84-96`).
pub struct Resolver {
    pub db: std::sync::Arc<dyn RepoResolverDb>,
    pub gitserver: std::sync::Arc<dyn ResolverGitserver>,
    pub zoekt: std::sync::Arc<dyn ResolverZoektLister>,
    pub searcher: Option<std::sync::Arc<dyn ResolverSearcher>>,
    /// `limits.SearchLimits(conf).MaxRepos` when `RepoOptions.Limit == 0`.
    pub max_repos: Option<i64>,
}

/// One page of [`Resolver::resolve_page`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageResult {
    pub resolved: Resolved,
    pub next: Option<Vec<RepoListCursor>>,
    pub missing: Vec<RepoRevSpecs>,
    /// A fatal error (DB, gitserver, zoekt or searcher); missing revs are in
    /// [`Self::missing`], matching the useful-error-with-result shape of the
    /// baseline.
    pub error: Option<JobError>,
}

/// Result of [`Resolver::iterate`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveOutcome {
    pub pages: Vec<Resolved>,
    pub missing: Vec<RepoRevSpecs>,
    pub error: Option<JobError>,
}

impl Resolver {
    fn limit(&self, options: &ResolveOptions) -> i64 {
        if options.limit != 0 {
            options.limit
        } else {
            self.max_repos.unwrap_or(i64::from(i32::MAX >> 1))
        }
    }

    /// `Resolver.Iterator` (`repos.go:98-121`), materialized.
    pub fn iterate(&self, options: &ResolveOptions) -> ResolveOutcome {
        let mut options = options.clone();
        if options.limit == 0 {
            options.limit = 4096;
        }
        let mut pages = Vec::new();
        let mut missing = Vec::new();
        let mut cursors = options.cursors.clone();
        loop {
            options.cursors = cursors;
            let page = self.resolve_page(&options);
            if let Some(error) = page.error {
                // The baseline's iterator yields no item for a non-missing
                // error (`return nil, errs`); pages already yielded stay.
                return ResolveOutcome {
                    pages,
                    missing,
                    error: Some(error),
                };
            }
            missing.extend(page.missing.iter().cloned());
            pages.push(page.resolved);
            match page.next {
                Some(next) => cursors = next,
                None => break,
            }
        }
        ResolveOutcome {
            pages,
            error: maybe_missing_repo_revs_error(&missing),
            missing,
        }
    }

    /// `Resolver.IterateRepoRevs` (`repos.go:130-165`).
    pub fn iterate_repo_revs(&self, options: &ResolveOptions) -> (Vec<RepoRevSpecs>, Option<JobError>) {
        let mut options = options.clone();
        if options.limit == 0 {
            options.limit = 4096;
        }
        let mut missing = Vec::new();
        let mut associated = Vec::new();
        let mut cursors = options.cursors.clone();
        loop {
            options.cursors = cursors;
            let (result, next) = match self.do_query_db(&options) {
                Ok(value) => value,
                Err(error) => return (associated, Some(error)),
            };
            missing.extend(result.missing);
            associated.extend(result.associated);
            match next {
                Some(next) => cursors = next,
                None => break,
            }
        }
        (
            associated,
            maybe_missing_repo_revs_error(&missing),
        )
    }

    /// `Resolver.ResolveRevSpecs` (`repos.go:173-186`).
    pub fn resolve_rev_specs(
        &self,
        options: &ResolveOptions,
        repo_rev_specs: Vec<RepoRevSpecs>,
    ) -> (Resolved, Option<JobError>) {
        let result = DbResolved {
            associated: repo_rev_specs,
            missing: Vec::new(),
        };
        let (resolved, missing, error) = self.do_filter_db_resolved(options, result);
        if let Some(error) = error {
            return (Resolved::default(), Some(error));
        }
        (resolved, maybe_missing_repo_revs_error(&missing))
    }

    /// `DB.Repos().ListMinimalRepos(ctx, ReposListOptions{IDs: ids})`, used by
    /// the exhaustive searcher's `minimalRepo` (`searcher.go:196-207`).
    pub fn minimal_repos_by_ids(&self, ids: &[i32]) -> Result<Vec<RepoListItem>, JobError> {
        self.db.list_minimal_repos_by_ids(ids)
    }

    /// `Resolver.resolve` (`repos.go:198-214`).
    pub fn resolve_page(&self, options: &ResolveOptions) -> PageResult {
        let (result, next) = match self.do_query_db(options) {
            Ok(value) => value,
            Err(error) => {
                return PageResult {
                    error: Some(error),
                    ..PageResult::default()
                }
            }
        };
        let (resolved, missing, error) = self.do_filter_db_resolved(options, result);
        PageResult {
            resolved,
            next,
            missing,
            error,
        }
    }

    /// `Resolver.doQueryDB` (`repos.go:223-366`).
    pub fn do_query_db(
        &self,
        options: &ResolveOptions,
    ) -> Result<(DbResolved, Option<Vec<RepoListCursor>>), JobError> {
        let (include_patterns, include_pattern_revs) =
            find_pattern_revs(&options.repo_filters, options.case_sensitive_repo_filters)?;
        let limit = self.limit(options);

        let search_context = self
            .db
            .resolve_search_context(&options.search_context_spec, &options.request_context)?;

        let mut list_options = repo_list_options(options, &include_patterns, limit + 1);
        // A search context without a query replaces `context:` at parse time.
        if search_context.query.is_empty() {
            list_options.search_context_id = search_context.id;
        }

        let mut repos = self.db.list_minimal_repos(&list_options)?;

        if repos.is_empty() && options.cursors.is_empty() {
            return Err(JobError(ERR_NO_RESOLVED_REPOS.to_owned()));
        }

        let mut next = None;
        if repos.len() as i64 == limit + 1 {
            let last = repos.last().expect("len == limit+1 > 0");
            next = Some(next_cursor(last, &list_options.order_by));
            repos.pop();
        }

        let mut search_context_repo_revs: BTreeMap<u32, RepoRevSpecs> = BTreeMap::new();
        if !is_auto_defined_search_context(&search_context) && search_context.query.is_empty() {
            let sc_repo_revs = self
                .db
                .get_search_context_repository_revisions(search_context.id)?;
            for repo_rev in sc_repo_revs {
                search_context_repo_revs.insert(
                    repo_rev.repo.id as u32,
                    RepoRevSpecs {
                        repo: RepositoryRevisionsRepo {
                            id: repo_rev.repo.id as u32,
                            name: repo_rev.repo.name,
                        },
                        revs: repo_rev
                            .revisions
                            .into_iter()
                            .map(|rev| RevisionSpecifier {
                                rev_spec: rev,
                                ..RevisionSpecifier::default()
                            })
                            .collect(),
                        external_service_type: None,
                    },
                );
            }
        }

        let (mut associated, missing) = associate_repos_with_revs(
            &repos,
            &search_context_repo_revs,
            &include_pattern_revs,
        );

        if options.perforce_changelist_mapping {
            associated = self.resolve_perforce_changelist_ids(associated);
        }

        Ok((
            DbResolved {
                associated,
                missing,
            },
            next,
        ))
    }

    /// `resolvePerforceChangeListIdsToCommitSHAs` (`repos.go:493-555`).
    fn resolve_perforce_changelist_ids(
        &self,
        mut repo_revs: Vec<RepoRevSpecs>,
    ) -> Vec<RepoRevSpecs> {
        let mut repos_to_map = Vec::new();
        for repo_rev in &repo_revs {
            if repo_rev.external_service_type.as_deref() != Some("perforce")
                || repo_rev.revs.is_empty()
            {
                continue;
            }
            let changelist_ids: Vec<i64> = repo_rev
                .revs
                .iter()
                .filter_map(|rev| extract_changelist_number(&rev.rev_spec).ok())
                .collect();
            if !changelist_ids.is_empty() {
                repos_to_map.push(RepoChangelistIDs {
                    repo_id: repo_rev.repo.id as i32,
                    changelist_ids,
                });
            }
        }
        if repos_to_map.is_empty() {
            return repo_revs;
        }
        let Ok(changelists) = self.db.batch_get_repo_commit_changelist(&repos_to_map) else {
            // The baseline logs and keeps the original revs.
            return repo_revs;
        };
        for repo_rev in &mut repo_revs {
            let Some(sub_map) = changelists.get(&repo_rev.repo.id) else {
                continue;
            };
            if repo_rev.external_service_type.as_deref() != Some("perforce") {
                continue;
            }
            for rev in &mut repo_rev.revs {
                if let Ok(changelist) = extract_changelist_number(&rev.rev_spec) {
                    if let Some(commit) = sub_map.get(&changelist) {
                        rev.rev_spec = commit.clone();
                    }
                }
            }
        }
        repo_revs
    }

    /// `Resolver.doFilterDBResolved` (`repos.go:368-393`).
    pub fn do_filter_db_resolved(
        &self,
        options: &ResolveOptions,
        result: DbResolved,
    ) -> (Resolved, Vec<RepoRevSpecs>, Option<JobError>) {
        let mut missing = result.missing;

        let (repo_revs, filtered_missing) =
            match self.filter_gitserver(options, &result.associated) {
                Ok(value) => value,
                Err(error) => return (Resolved::default(), Vec::new(), Some(error)),
            };
        missing.extend(filtered_missing);

        let (repo_revs, missing_has_file_content, backends_missing) =
            match self.filter_repo_has_file_content(&repo_revs, options) {
                Ok(value) => value,
                Err(error) => {
                    return (
                        Resolved::default(),
                        Vec::new(),
                        Some(JobError(format!("filter has file content: {}", error.0))),
                    )
                }
            };
        missing.extend(missing_has_file_content);

        (
            Resolved {
                repo_revs,
                backends_missing,
            },
            missing,
            None,
        )
    }

    /// `Resolver.filterGitserver` (`repos.go:395-412`).
    fn filter_gitserver(
        &self,
        options: &ResolveOptions,
        associated: &[RepoRevSpecs],
    ) -> Result<(Vec<RepositoryRevisions>, Vec<RepoRevSpecs>), JobError> {
        let (normalized, normalized_missing) = self
            .normalize_refs(associated)
            .map_err(|error| JobError(format!("normalize refs: {}", error.0)))?;
        let filtered = self
            .filter_has_commit_after(&normalized, options)
            .map_err(|error| JobError(format!("filter has commit after: {}", error.0)))?;
        Ok((filtered, normalized_missing))
    }

    /// `Resolver.normalizeRefs` (`repos.go:557-672`).
    fn normalize_refs(
        &self,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<(Vec<RepositoryRevisions>, Vec<RepoRevSpecs>), JobError> {
        let mut results = Vec::with_capacity(repo_rev_specs.len());
        let mut missing = Vec::new();
        for repo_rev in repo_rev_specs {
            let expanded = self.normalize_repo_refs(repo_rev, &mut missing)?;
            results.push(RepositoryRevisions {
                repo: repo_rev.repo.clone(),
                revs: expanded,
            });
        }
        results.retain(|result| !result.revs.is_empty());
        Ok((results, missing))
    }

    /// `Resolver.normalizeRepoRefs` (`repos.go:601-672`).
    fn normalize_repo_refs(
        &self,
        repo_rev: &RepoRevSpecs,
        missing: &mut Vec<RepoRevSpecs>,
    ) -> Result<Vec<String>, JobError> {
        let mut revs = Vec::with_capacity(repo_rev.revs.len());
        let mut globs = Vec::new();
        for rev in &repo_rev.revs {
            // Parsed `rat=` filters carry the full payload; cross-crate
            // constructors (worker) only carry the encoded text in
            // `rev_spec`, for which the baseline passes the parsed timestamp
            // along. The epoch keeps the call shape for that legacy path.
            let rev_at_time = rev
                .rev_at_time_value()
                .map(|value| (value.rev_spec.clone(), value.timestamp))
                .or_else(|| {
                    rev.rev_at_time
                        .then(|| (rev.rev_spec.clone(), OffsetDateTime::UNIX_EPOCH))
                });
            if !rev.ref_glob.is_empty() {
                globs.push(RefGlob {
                    include: rev.ref_glob.clone(),
                    exclude: String::new(),
                });
            } else if !rev.exclude_ref_glob.is_empty() {
                globs.push(RefGlob {
                    include: String::new(),
                    exclude: rev.exclude_ref_glob.clone(),
                });
            } else if let Some((rev_spec, timestamp)) = rev_at_time {
                match self
                    .gitserver
                    .rev_at_time(&repo_rev.repo.name, &rev_spec, timestamp)
                {
                    Ok(Some(commit)) => revs.push(commit),
                    Ok(None) => {}
                    Err(GitserverError::Missing) => missing.push(RepoRevSpecs {
                        repo: repo_rev.repo.clone(),
                        revs: vec![rev.clone()],
                        external_service_type: repo_rev.external_service_type.clone(),
                    }),
                    Err(GitserverError::Fatal(error)) => return Err(JobError(error)),
                }
            } else if rev.rev_spec.is_empty() || rev.rev_spec == "HEAD" {
                revs.push(rev.rev_spec.clone());
            } else {
                let trimmed = rev.rev_spec.trim_start_matches('^');
                match self.gitserver.resolve_revision(&repo_rev.repo.name, trimmed) {
                    Ok(_) => revs.push(rev.rev_spec.clone()),
                    Err(GitserverError::Missing) => missing.push(RepoRevSpecs {
                        repo: repo_rev.repo.clone(),
                        revs: vec![rev.clone()],
                        external_service_type: repo_rev.external_service_type.clone(),
                    }),
                    Err(GitserverError::Fatal(error)) => return Err(JobError(error)),
                }
            }
        }

        if globs.is_empty() {
            return Ok(revs);
        }

        let compiled = compile_ref_globs(&globs)?;
        let all_refs = self.gitserver.list_refs(&repo_rev.repo.name)?;
        for reference in all_refs {
            if compiled.is_match(&reference) {
                revs.push(
                    reference
                        .strip_prefix("refs/heads/")
                        .unwrap_or(&reference)
                        .to_owned(),
                );
            }
        }
        Ok(revs)
    }

    /// `Resolver.filterHasCommitAfter` (`repos.go:674-742`).
    fn filter_has_commit_after(
        &self,
        repo_revs: &[RepositoryRevisions],
        options: &ResolveOptions,
    ) -> Result<Vec<RepositoryRevisions>, JobError> {
        let Some(commit_after) = &options.commit_after else {
            return Ok(repo_revs.to_vec());
        };
        let Some(time_ref) = crate::search_commit_job::parse_git_date(&commit_after.time_ref)
        else {
            return Err(JobError("invalid time ref: invalid date format".to_owned()));
        };
        let mut filtered = Vec::with_capacity(repo_revs.len());
        for repo_rev in repo_revs {
            let mut kept = Vec::with_capacity(repo_rev.revs.len());
            for rev in &repo_rev.revs {
                let result = self
                    .gitserver
                    .commits_after(&repo_rev.repo.name, rev, time_ref);
                let has_commit_after = match result {
                    Ok(count) => count > 0,
                    Err(GitserverError::Missing) => false,
                    Err(GitserverError::Fatal(error)) => return Err(JobError(error)),
                };
                let keep = if commit_after.negated {
                    !has_commit_after
                } else {
                    has_commit_after
                };
                if keep {
                    kept.push(rev.clone());
                }
            }
            if !kept.is_empty() {
                filtered.push(RepositoryRevisions {
                    repo: repo_rev.repo.clone(),
                    revs: kept,
                });
            }
        }
        Ok(filtered)
    }

    /// `Resolver.filterRepoHasFileContent` (`repos.go:768-960`).
    fn filter_repo_has_file_content(
        &self,
        repo_revs: &[RepositoryRevisions],
        options: &ResolveOptions,
    ) -> Result<(Vec<RepositoryRevisions>, Vec<RepoRevSpecs>, i64), JobError> {
        if options.has_file_content.is_empty() {
            return Ok((repo_revs.to_vec(), Vec::new(), 0));
        }

        let (indexed, unindexed) = partition_repos(
            repo_revs,
            sg_zoekt::query::IndexedRequestType::Text,
            options.use_index,
            false,
            || {
                self.zoekt.list(
                    &Query::Const(true),
                    Some(ListOptions {
                        field: RepoListField::ReposMap,
                    }),
                )
            },
        )
        .map_err(|error| JobError(error.to_string()))?;

        let minimal_repo_map: BTreeMap<u32, RepositoryRevisionsRepo> = repo_revs
            .iter()
            .map(|repo_rev| (repo_rev.repo.id, repo_rev.repo.clone()))
            .collect();

        let mut filtered: BTreeMap<u32, RepositoryRevisions> = BTreeMap::new();
        let mut missing = Vec::new();
        let mut backends_missing = 0i64;

        // Indexed revs: one zoekt List per predicate, intersected.
        let mut matching_all: Option<BTreeSet<(u32, String)>> = None;
        for arg in &options.has_file_content {
            let content_query = query_for_file_content_args(arg, options.case_sensitive_repo_filters);
            let query = Query::And(vec![
                Query::BranchesRepos(indexed.branch_repos_query()),
                content_query,
            ]);
            let response = self.zoekt.list(
                &query,
                Some(ListOptions {
                    field: RepoListField::ReposMap,
                }),
            )?;
            backends_missing += response.crashes;

            let mut found = BTreeSet::new();
            for (repo_id, repo) in &response.repos_map {
                let Some(input_revs) = indexed.repo_revs.get(repo_id) else {
                    continue;
                };
                for branch in &repo.branches {
                    for input_rev in &input_revs.revs {
                        if branch.name == *input_rev
                            || (branch.name == "HEAD" && input_rev.is_empty())
                        {
                            found.insert((*repo_id, input_rev.clone()));
                        }
                    }
                }
            }
            matching_all = Some(match matching_all {
                None => found,
                Some(previous) => previous.intersection(&found).cloned().collect(),
            });
        }
        for (repo_id, rev) in matching_all.unwrap_or_default() {
            let Some(repo) = minimal_repo_map.get(&repo_id) else {
                continue;
            };
            filtered
                .entry(repo_id)
                .or_insert_with(|| RepositoryRevisions {
                    repo: repo.clone(),
                    revs: Vec::new(),
                })
                .revs
                .push(rev);
        }

        // Unindexed revs: searcher per rev/predicate.
        let Some(searcher) = &self.searcher else {
            if !unindexed.is_empty() {
                return Err(JobError(
                    "searcher backend is not ported yet".to_owned(),
                ));
            }
            let matched = matched_repo_revs(repo_revs, &filtered);
            return Ok((matched, missing, backends_missing));
        };
        for repo_rev in &unindexed {
            for rev in &repo_rev.revs {
                let mut matches_all = true;
                for arg in &options.has_file_content {
                    let has_matches = match self.check_has_matches(searcher, arg, repo_rev, rev, &mut missing) {
                        Ok(value) => value,
                        Err(error) => return Err(error),
                    };
                    if has_matches != !arg.negated {
                        matches_all = false;
                        break;
                    }
                }
                if matches_all {
                    filtered
                        .entry(repo_rev.repo.id)
                        .or_insert_with(|| RepositoryRevisions {
                            repo: repo_rev.repo.clone(),
                            revs: Vec::new(),
                        })
                        .revs
                        .push(rev.clone());
                }
            }
        }

        Ok((matched_repo_revs(repo_revs, &filtered), missing, backends_missing))
    }

    fn check_has_matches(
        &self,
        searcher: &std::sync::Arc<dyn ResolverSearcher>,
        arg: &RepoHasFileContentArgs,
        repo_rev: &RepositoryRevisions,
        rev: &str,
        missing: &mut Vec<RepoRevSpecs>,
    ) -> Result<bool, JobError> {
        let commit_id = match self.gitserver.resolve_revision(&repo_rev.repo.name, rev) {
            Ok(commit) => commit,
            Err(GitserverError::Missing) => {
                if rev == "HEAD" || rev.is_empty() {
                    return Ok(false);
                }
                missing.push(RepoRevSpecs {
                    repo: repo_rev.repo.clone(),
                    revs: vec![RevisionSpecifier {
                        rev_spec: rev.to_owned(),
                        ..RevisionSpecifier::default()
                    }],
                    external_service_type: None,
                });
                return Ok(false);
            }
            Err(GitserverError::Fatal(error)) => return Err(JobError(error)),
        };
        self.repo_has_file_content_at_commit(searcher, &repo_rev.repo, &commit_id, arg)
    }

    /// `Resolver.repoHasFileContentAtCommit` (`repos.go:962-1002`).
    fn repo_has_file_content_at_commit(
        &self,
        searcher: &std::sync::Arc<dyn ResolverSearcher>,
        repo: &RepositoryRevisionsRepo,
        commit_id: &str,
        args: &RepoHasFileContentArgs,
    ) -> Result<bool, JobError> {
        let mut pattern_info = protocol::PatternInfo {
            query: Some(protocol::QueryNode::Pattern(protocol::PatternNode {
                value: args.content.clone(),
                is_negated: args.negated,
                is_regexp: true,
                boost: false,
            })),
            is_case_sensitive: false,
            limit: 1,
            pattern_matches_content: true,
            ..protocol::PatternInfo::default()
        };
        if !args.path.is_empty() {
            pattern_info.include_paths = vec![args.path.clone()];
            pattern_info.pattern_matches_path = true;
        }
        let request = protocol::Request {
            repo: repo.name.clone(),
            repo_id: repo.id,
            commit: commit_id.to_owned(),
            branch: String::new(),
            pattern_info,
            fetch_timeout: std::time::Duration::from_secs(3600),
            indexed: false,
            num_context_lines: 0,
        };
        searcher.search(&request)
    }
}

fn matched_repo_revs(
    repo_revs: &[RepositoryRevisions],
    filtered: &BTreeMap<u32, RepositoryRevisions>,
) -> Vec<RepositoryRevisions> {
    repo_revs
        .iter()
        .filter_map(|repo_rev| filtered.get(&repo_rev.repo.id).cloned())
        .collect()
}

/// `maybeMissingRepoRevsError` (`repos.go:1215-1222`).
pub fn maybe_missing_repo_revs_error(missing: &[RepoRevSpecs]) -> Option<JobError> {
    if missing.is_empty() {
        None
    } else {
        Some(JobError(ERR_MISSING_REPO_REVS.to_owned()))
    }
}

/// `next` cursor construction (`doQueryDB`).
fn next_cursor(last: &RepoListItem, order_by: &[RepoListSort]) -> Vec<RepoListCursor> {
    let mut next = Vec::with_capacity(order_by.len());
    for sort in order_by {
        let value = match sort.field.as_str() {
            "stars" => last.stars.to_string(),
            "id" => last.id.to_string(),
            _ => String::new(),
        };
        next.push(RepoListCursor {
            column: sort.field.clone(),
            value,
            direction: if sort.descending { "prev" } else { "next" }.to_owned(),
        });
    }
    next
}

/// `extractChangelistNumber` (`repos.go:474-491`).
pub fn extract_changelist_number(rev_spec: &str) -> Result<i64, JobError> {
    let Some(rest) = rev_spec.strip_prefix("changelist/") else {
        return Err(JobError(format!("invalid changelist format: {rev_spec}")));
    };
    rest.parse::<i64>()
        .map_err(|error| JobError(format!("failed to parse changelist number: {error}")))
}

/// `findPatternRevs` (`repos.go:1186-1201`).
pub fn find_pattern_revs(
    include_patterns: &[ParsedRepoFilter],
    case_sensitive: bool,
) -> Result<(Vec<String>, Vec<PatternRevspec>), JobError> {
    let mut output_patterns = Vec::with_capacity(include_patterns.len());
    let mut include_pattern_revs = Vec::new();
    for pattern in include_patterns {
        let repo = optimize_repo_pattern_with_heuristics(&pattern.repo);
        output_patterns.push(repo);
        if !pattern.revs.is_empty() {
            let regex = RegexBuilder::new(&pattern.repo)
                .case_insensitive(!case_sensitive)
                .build()
                .map_err(|error| JobError(error.to_string()))?;
            include_pattern_revs.push(PatternRevspec {
                include_pattern: regex,
                revs: pattern.revs.clone(),
            });
        }
    }
    Ok((output_patterns, include_pattern_revs))
}

/// `optimizeRepoPatternWithHeuristics` (`repos.go:1203-1211`). Dotcom mode is
/// not modeled: the resolver is not called in dotcom-only tests.
pub fn optimize_repo_pattern_with_heuristics(repo_pattern: &str) -> String {
    repo_pattern.replace("github.com", "github\\.com")
}

/// `getRevsForMatchedRepo` (`repos.go:1129-1184`).
pub fn get_revs_for_matched_repo(
    repo: &str,
    patterns: &[PatternRevspec],
) -> (Vec<RevisionSpecifier>, Vec<RevisionSpecifier>) {
    let rev_lists: Vec<&Vec<RevisionSpecifier>> = patterns
        .iter()
        .filter(|pattern| pattern.include_pattern.is_match(repo))
        .map(|pattern| &pattern.revs)
        .collect();

    if rev_lists.len() == 1 {
        return (rev_lists[0].clone(), Vec::new());
    }
    if rev_lists.is_empty() {
        return (
            vec![RevisionSpecifier {
                rev_spec: String::new(),
                ..RevisionSpecifier::default()
            }],
            Vec::new(),
        );
    }

    // Go's map key is the struct itself, so `*RevAtTime` participates by
    // pointer identity (`repos.go:1150`). The `Arc` address reproduces that.
    let key = |rev: &RevisionSpecifier| {
        (
            rev.rev_spec.clone(),
            rev.ref_glob.clone(),
            rev.exclude_ref_glob.clone(),
            rev.rev_at_time_payload
                .as_ref()
                .map(|rev_at_time| std::sync::Arc::as_ptr(rev_at_time) as usize),
        )
    };
    let mut rev_counts: BTreeMap<
        (String, String, String, Option<usize>),
        (RevisionSpecifier, usize),
    > = BTreeMap::new();
    let mut alive_count = 0usize;
    for (i, rev_list) in rev_lists.iter().enumerate() {
        alive_count = 0;
        for rev in rev_list.iter() {
            let entry = rev_counts
                .entry(key(rev))
                .or_insert_with(|| (rev.clone(), 0));
            if entry.1 == i {
                alive_count += 1;
            }
            entry.1 += 1;
        }
    }

    if alive_count > 0 {
        let mut matched: Vec<RevisionSpecifier> = rev_counts
            .into_values()
            .filter(|(_, seen)| *seen == rev_lists.len())
            .map(|(rev, _)| rev)
            .collect();
        matched.sort_by(compare_revision_specifiers);
        return (matched, Vec::new());
    }

    let mut clashing: Vec<RevisionSpecifier> =
        rev_counts.into_values().map(|(rev, _)| rev).collect();
    clashing.sort_by(compare_revision_specifiers);
    (Vec::new(), clashing)
}

fn compare_revision_specifiers(a: &RevisionSpecifier, b: &RevisionSpecifier) -> std::cmp::Ordering {
    // `query.RevisionSpecifier.Compare` (`repo_revs.go:55-70`) compares
    // `RevAtTime.String()` (the nil pointer renders as `rat=bnVsbA==`).
    use crate::search_contexts::revision_specifier_rev_at_time_string;
    a.rev_spec
        .cmp(&b.rev_spec)
        .then_with(|| a.ref_glob.cmp(&b.ref_glob))
        .then_with(|| {
            revision_specifier_rev_at_time_string(a)
                .cmp(&revision_specifier_rev_at_time_string(b))
        })
        .then_with(|| a.exclude_ref_glob.cmp(&b.exclude_ref_glob))
}

/// `associateReposWithRevs` (`repos.go:414-472`).
pub fn associate_repos_with_revs(
    repos: &[RepoListItem],
    search_context_repo_revs: &BTreeMap<u32, RepoRevSpecs>,
    include_pattern_revs: &[PatternRevspec],
) -> (Vec<RepoRevSpecs>, Vec<RepoRevSpecs>) {
    let mut associated = Vec::with_capacity(repos.len());
    let mut missing_flags = Vec::with_capacity(repos.len());
    for repo in repos {
        let mut revs: Vec<RevisionSpecifier> = Vec::new();
        let mut is_missing = false;

        if !search_context_repo_revs.is_empty() && revs.is_empty() {
            if let Some(sc_repo_rev) = search_context_repo_revs.get(&(repo.id as u32)) {
                revs = sc_repo_rev.revs.clone();
            }
        }

        if revs.is_empty() {
            let (matched, clashing) = get_revs_for_matched_repo(&repo.name, include_pattern_revs);
            revs = matched;
            if revs.is_empty() && !clashing.is_empty() {
                revs = clashing;
                is_missing = true;
            }
        }

        associated.push(RepoRevSpecs {
            repo: RepositoryRevisionsRepo {
                id: repo.id as u32,
                name: repo.name.clone(),
            },
            revs,
            external_service_type: if repo.external_service_type.is_empty() {
                None
            } else {
                Some(repo.external_service_type.clone())
            },
        });
        missing_flags.push(is_missing);
    }

    let mut present = Vec::with_capacity(associated.len());
    let mut missing = Vec::new();
    for (repo_rev, is_missing) in associated.into_iter().zip(missing_flags) {
        if is_missing {
            missing.push(repo_rev);
        } else {
            present.push(repo_rev);
        }
    }
    (present, missing)
}

/// Adapter that implements [`crate::search_jobutil_repos::RepoPageSource`] on
/// top of a [`Resolver`], feeding the production `RepoPagerJob` tree node.
pub struct ResolverRepoPageSource {
    resolver: std::sync::Arc<Resolver>,
}

impl ResolverRepoPageSource {
    pub fn new(resolver: std::sync::Arc<Resolver>) -> Self {
        Self { resolver }
    }
}

impl crate::search_jobutil_repos::RepoPageSource for ResolverRepoPageSource {
    fn pages(&self) -> Vec<crate::search_jobutil_repos::RepoPage> {
        // The production tree always calls `resolve_pages`; this keeps the
        // option-unaware contract well-defined for other callers.
        self.resolve_pages(&ResolveOptions::default()).pages
    }

    fn error(&self) -> Option<JobError> {
        None
    }

    fn resolve_pages(
        &self,
        options: &ResolveOptions,
    ) -> crate::search_jobutil_repos::ResolvedPages {
        let outcome = self.resolver.iterate(options);
        crate::search_jobutil_repos::ResolvedPages {
            pages: outcome
                .pages
                .into_iter()
                .map(|resolved| crate::search_jobutil_repos::RepoPage {
                    repo_revs: resolved.repo_revs,
                    backends_missing: resolved.backends_missing,
                })
                .collect(),
            error: outcome.error,
            missing: outcome.missing,
        }
    }
}

/// Conversion from the job builder's full repo options.
impl ResolveOptions {
    pub fn from_tree(options: &TreeRepoOptions) -> Self {
        ResolveOptions {
            repo_filters: options.repo_filters.clone(),
            minus_repo_filters: options.minus_repo_filters.clone(),
            description_patterns: options.description_patterns.clone(),
            case_sensitive_repo_filters: options.case_sensitive_repo_filters,
            search_context_spec: options.search_context_spec.clone(),
            commit_after: options.commit_after.clone(),
            visibility: options.visibility,
            limit: options.limit,
            cursors: Vec::new(),
            use_index: options.use_index,
            has_file_content: options.has_file_content.clone(),
            has_kvps: options.has_kvps.clone(),
            has_topics: options.has_topics.clone(),
            no_forks: options.no_forks,
            only_forks: options.only_forks,
            only_cloned: options.only_cloned,
            no_archived: options.no_archived,
            only_archived: options.only_archived,
            perforce_changelist_mapping: false,
            request_context: options.request_context,
        }
    }
}

// `RepoRevSpecs` needs the external service type for Perforce remapping, but
// `RepositoryRevisionsRepo` deliberately keeps only id/name. Store it in a
// side map on the resolver call instead: the baseline reads
// `Repo.ExternalRepo.ServiceType`. To avoid widening `RepositoryRevisions`,
// `associate_repos_with_revs` records it in `RevSpecsExternal`.
