//! Leaf job execution for the `jobutil` tree (M9.37b).
//!
//! Baseline:
//!
//! * `internal/search/structural/structural.go` — `SearchJob` over the
//!   searcher (`index:no`) backend;
//! * `internal/search/job/jobutil/filter_file_contains.go` — `FileContains`
//!   post-filter (file/diff matches);
//! * `internal/search/job/jobutil/filter_file_contributor.go` —
//!   `file:has.contributor()` filter;
//! * `internal/own/search/filter_job.go` — `file:has.owner()` filter;
//! * `internal/own/search/select_job.go` — `select:file.owners`.
//!
//! The filter jobs' core logic is ported in
//! [`crate::search_jobutil_filters`]; this module adapts it to the
//! combinator tree (`CombinatorJob`) and adds the two ownership jobs.
//!
//! Ownership/CODEOWNERS parsing lives in the enterprise `internal/own`
//! package. Rather than reimplementing the parser, the tree reads owner
//! references through [`OwnershipFetcher`] (the differential harness feeds it
//! the same fixture rules the Go oracle builds). When the seam is absent the
//! jobs follow the Go error path: ownership errors raise
//! `ownership-search-error` and drop the affected result; a missing
//! contributor fetcher returns the explicit unsupported error.
//!
//! See `docs/overviews/m9_37b_search_leaf_jobs_overview.md`.

use std::collections::HashSet;

use regex::Regex;

use sg_zoekt::indexed_repo_revs::IndexedRepoRevs;

use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_jobutil_combinators as comb;
use crate::search_owners;
use crate::search_jobutil_filters::{
    self, CommitContentSearcher, ContributorFetcher, ContributorOptions, ContainsPatternError,
};
use crate::search_searcher_jobs::{RepositoryRevisions as SearcherRepoRevs, TextSearchJob};
use crate::search_stream::convert::{Match, MinimalRepo, OwnerMatch, ResolvedOwner};
use crate::search_stream::progress::{SearchEvent, Stats};

/// Explicit error when the searcher backend is not wired (same text as the
/// searcher jobs).
const SEARCHER_NOT_PORTED: &str = "searcher backend is not ported yet";

/// One CODEOWNERS/assigned-owner reference (`own.Reference` reduced to the
/// fields the ownership jobs consume).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnershipRef {
    pub handle: String,
    pub email: String,
    /// Set for assigned owners (`own.Reference.UserID`).
    pub user_id: Option<i32>,
    /// Set for assigned teams (`own.Reference.TeamID`).
    pub team_id: Option<i32>,
    /// `own.RepoContext.CodeHostKind` for `ResolutionGuess` (github teams).
    pub code_host_kind: Option<String>,
    pub repo_name: Option<String>,
}

/// `own.Search` rules lookup: owner references matching `path` at `commit`.
pub trait OwnershipFetcher {
    fn owners(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
        path: &str,
    ) -> Result<Vec<OwnershipRef>, JobError>;

    /// `own.ByTextReference(...).Resolve`: references that resolve to the same
    /// user/team as `text`. Errors are swallowed by the baseline.
    fn resolve_text(&self, _text: &str) -> Vec<OwnershipRef> {
        Vec::new()
    }
}

/// Optional leaf collaborators injected through
/// [`crate::search_commit_job::CommitRuntimeClients::leaf`].
#[derive(Clone, Default)]
pub struct LeafJobClients {
    /// `gitserver.Client.ContributorCount`.
    pub contributors: Option<std::sync::Arc<dyn ContributorFetcher + Send + Sync>>,
    /// `searcher.Search` for the `file:contains.content()` diff path.
    pub content_searcher: Option<std::sync::Arc<dyn CommitContentSearcher + Send + Sync>>,
    /// `own` rules lookup.
    pub ownership: Option<std::sync::Arc<dyn OwnershipFetcher + Send + Sync>>,
}

impl LeafJobClients {
    pub fn contributor_fetcher(&self) -> Option<&dyn ContributorFetcher> {
        self.contributors
            .as_ref()
            .map(|fetcher| fetcher.as_ref() as &dyn ContributorFetcher)
    }

    pub fn content_searcher(&self) -> Option<&dyn CommitContentSearcher> {
        self.content_searcher
            .as_ref()
            .map(|searcher| searcher.as_ref() as &dyn CommitContentSearcher)
    }

    pub fn ownership_fetcher(&self) -> Option<&dyn OwnershipFetcher> {
        self.ownership
            .as_ref()
            .map(|fetcher| fetcher.as_ref() as &dyn OwnershipFetcher)
    }
}

fn leaf_clients<'a>(clients: &'a RuntimeClients<'_>) -> Option<&'a LeafJobClients> {
    clients.commit.and_then(|commit| commit.leaf)
}

fn searcher_not_ported() -> JobError {
    JobError(SEARCHER_NOT_PORTED.to_owned())
}

// ---------------------------------------------------------------------------
// Structural
// ---------------------------------------------------------------------------

/// `structural.SearchJob` (`structural.go:183-220`).
pub struct StructuralSearchJob {
    pub pattern_info: crate::search_searcher_jobs::TextPatternInfo,
    pub use_full_deadline: bool,
    pub batch_retry: bool,
    pub unindexed: Vec<RepositoryRevisions>,
    pub indexed: Option<IndexedRepoRevs>,
}

/// Minimal `search.RepositoryRevisions` shared by the two repo sets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepositoryRevisions {
    pub repo: MinimalRepo,
    pub revs: Vec<String>,
}

impl StructuralSearchJob {
    fn run_with_pattern(
        &self,
        pattern_info: crate::search_searcher_jobs::TextPatternInfo,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        // `SearchJob.Run`: `repoSet = [UnindexedList(...)]`, plus the indexed
        // map when non-nil (`structural.go:204-213`).
        let mut sets: Vec<(bool, Vec<SearcherRepoRevs>)> =
            vec![(false, to_searcher_revs(&self.unindexed))];
        if let Some(indexed) = &self.indexed {
            let repos: Vec<SearcherRepoRevs> = indexed
                .repo_revs
                .values()
                .map(|repo| SearcherRepoRevs {
                    repo: MinimalRepo {
                        id: repo.repo.id as i32,
                        name: repo.repo.name.clone(),
                    },
                    revs: repo.revs.clone(),
                })
                .collect();
            sets.push((true, repos));
        }

        // `streamStructuralSearch`: one TextSearchJob per repo set. Go runs
        // them concurrently through errgroup; this port runs them in order.
        for (indexed, repos) in sets {
            // `searcher.TextSearchJob.Run` returns before touching the
            // backend when the repo list is empty.
            if repos.is_empty() {
                continue;
            }
            let mut job = TextSearchJob {
                pattern_info: pattern_info.clone(),
                repos,
                path_regexps: Vec::new(),
                indexed,
                num_context_lines: 0,
                use_full_deadline: self.use_full_deadline,
            };
            job.run(ctx, clients, stream)?;
        }
        Ok(())
    }
}

impl SearchJob for StructuralSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let has_repos = !self.unindexed.is_empty()
            || self
                .indexed
                .as_ref()
                .is_some_and(|indexed| !indexed.repo_revs.is_empty());
        if has_repos && clients.searcher.is_none() {
            return Err(searcher_not_ported());
        }

        // `streamStructuralSearch` streams directly unless batching.
        if !self.batch_retry {
            return self.run_with_pattern(self.pattern_info.clone(), ctx, clients, stream);
        }

        // `runStructuralSearch` (`structural.go:115-165`): aggregate, retry
        // once with a higher FileMatchLimit when nothing matched.
        let mut first = Vec::new();
        let first_err =
            self.run_with_pattern(self.pattern_info.clone(), ctx, clients, &mut |event| {
                first.push(event)
            });
        let mut event = merge_events(first);

        if event.results.is_empty() && first_err.is_ok() {
            let mut retry_pattern = self.pattern_info.clone();
            retry_pattern.file_match_limit = 1000;
            let mut retry = Vec::new();
            let retry_err =
                self.run_with_pattern(retry_pattern, ctx, clients, &mut |event| retry.push(event));
            if let Err(err) = retry_err {
                // Send the retry stats if any, else the first try's.
                let retry_stats = merge_events(retry).stats;
                let stats = if retry_stats.zero() {
                    event.stats.clone()
                } else {
                    retry_stats
                };
                stream(SearchEvent {
                    results: Vec::new(),
                    stats,
                });
                return Err(err);
            }
            // Mirrors the baseline, which keeps `event = agg.SearchEvent`
            // (the first attempt) after a successful retry.
            if event.results.is_empty() {
                event.stats.is_limit_hit = false;
            }
        }

        // `StructuralSearchJob failed to convert results` for non-file
        // matches (`structural.go:147-153`).
        if event
            .results
            .iter()
            .any(|result| !matches!(result, Match::File(_)))
        {
            return Err(JobError(
                "StructuralSearchJob failed to convert results".to_owned(),
            ));
        }

        stream(event);
        first_err
    }

    fn name(&self) -> &'static str {
        "StructuralSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            vec![("patternInfo".to_owned(), self.pattern_info.to_string())]
        } else {
            Vec::new()
        }
    }
}

fn to_searcher_revs(repos: &[RepositoryRevisions]) -> Vec<SearcherRepoRevs> {
    repos
        .iter()
        .map(|repo| SearcherRepoRevs {
            repo: repo.repo.clone(),
            revs: repo.revs.clone(),
        })
        .collect()
}

fn merge_events(events: Vec<SearchEvent>) -> SearchEvent {
    let mut merged = SearchEvent {
        results: Vec::new(),
        stats: Stats::default(),
    };
    for event in events {
        merged.results.extend(event.results);
        merged.stats.update(&event.stats);
    }
    merged
}

// ---------------------------------------------------------------------------
// file:contains.content()
// ---------------------------------------------------------------------------

/// `fileContainsFilterJob` over a combinator child.
pub struct FileContainsLeafJob {
    /// Case-folded compiled sources (`NodeKind::FileContains.filter_patterns`).
    pub include_patterns: Vec<String>,
    /// Case-folded compiled sources (`NodeKind::FileContains.original_patterns`).
    pub original_patterns: Vec<String>,
    /// Raw `file:contains.content()` patterns, passed verbatim to the searcher.
    pub raw_include_patterns: Vec<String>,
    /// Original case sensitivity (`search.TextPatternInfo.IsCaseSensitive`).
    pub case_sensitive: bool,
    pub searcher: Option<Box<dyn CommitContentSearcher>>,
    pub child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for FileContainsLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        // Prefer the job-local searcher, else the runtime seam.
        let Self {
            include_patterns,
            original_patterns,
            raw_include_patterns,
            case_sensitive,
            searcher,
            child,
        } = self;
        let runtime_searcher = leaf_clients(clients).and_then(LeafJobClients::content_searcher);
        let searcher = searcher.as_deref().or(runtime_searcher);
        let mut filtered = |event: SearchEvent| {
            match filter_event_from_sources(
                event,
                include_patterns,
                original_patterns,
                raw_include_patterns,
                *case_sensitive,
                searcher,
            ) {
                Ok(event) => stream(event),
                Err(error) => {
                    // Construction compiled the same sources successfully, so
                    // this is unreachable; surface it as the event-filter error.
                    stream(SearchEvent {
                        results: Vec::new(),
                        stats: Stats::default(),
                    });
                    let _ = error;
                }
            }
        };
        child.run(ctx, clients, &mut filtered)
    }

    fn name(&self) -> &'static str {
        "FileContainsFilterJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }
}

fn filter_event_from_sources(
    event: SearchEvent,
    include_patterns: &[String],
    original_patterns: &[String],
    raw_include_patterns: &[String],
    case_sensitive: bool,
    searcher: Option<&dyn CommitContentSearcher>,
) -> Result<SearchEvent, ContainsPatternError> {
    search_jobutil_filters::filter_event_from_sources(
        event,
        include_patterns,
        original_patterns,
        raw_include_patterns,
        case_sensitive,
        searcher,
    )
}

// ---------------------------------------------------------------------------
// file:has.contributor()
// ---------------------------------------------------------------------------

/// `fileHasContributorsJob` over a combinator child.
pub struct FileHasContributorsLeafJob {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for FileHasContributorsLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Some(fetcher) = leaf_clients(clients).and_then(LeafJobClients::contributor_fetcher) else {
            return (
                None,
                Some(comb::JobFailure::Message(
                    "contributors backend is not ported yet".to_owned(),
                )),
            );
        };
        let Self {
            include,
            exclude,
            child,
        } = self;
        let include_matchers = compile_valid(include);
        let exclude_matchers = compile_valid(exclude);
        let mut errs: Vec<JobError> = Vec::new();

        let (alert, failure) = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let results = std::mem::take(&mut event.results);
                let mut kept = Vec::with_capacity(results.len());
                for result in results {
                    let Match::File(fm) = result else {
                        continue;
                    };
                    if ctx.deadline_exceeded() {
                        errs.push(JobError("context deadline exceeded".to_owned()));
                        break;
                    }
                    let options = ContributorOptions {
                        range: fm.file.commit_id.clone(),
                        path: fm.file.path.clone(),
                    };
                    let contributors =
                        match fetcher.contributor_count(&fm.file.repo.name, &options) {
                            Ok(contributors) => contributors,
                            Err(err) => {
                                errs.push(err);
                                continue;
                            }
                        };
                    if search_jobutil_filters::any_filter_matches(
                        &contributors,
                        &exclude_matchers,
                    ) {
                        continue;
                    }
                    if !search_jobutil_filters::all_filters_match(
                        &contributors,
                        &include_matchers,
                    ) {
                        continue;
                    }
                    kept.push(Match::File(fm));
                }
                event.results = kept;
                stream(event);
            };
            child.run(ctx, clients, &mut filtered)
        };

        if let Some(comb::JobFailure::Message(message)) = &failure {
            errs.push(JobError(message.clone()));
        }
        if errs.is_empty() {
            (alert, failure)
        } else {
            let message = errs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            (alert, Some(comb::JobFailure::Message(message)))
        }
    }

    fn name(&self) -> &'static str {
        "FileHasContributorsFilterJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }
}

fn compile_valid(sources: &[String]) -> Vec<Regex> {
    sources
        .iter()
        .filter_map(|source| Regex::new(source).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// file:has.owner() / select:file.owners
// ---------------------------------------------------------------------------

/// `search.AlertForOwnershipSearchError` (`alert.go:187-194`).
fn alert_for_ownership_error() -> comb::Alert {
    comb::Alert {
        kind: "ownership-search-error".to_owned(),
        title: "Error during ownership search".to_owned(),
        description: "Ownership search returned an error.".to_owned(),
        priority: 1,
        ..comb::Alert::default()
    }
}

/// `search.AlertForUnownedResult` (`alert.go:175-183`).
fn alert_for_unowned_result() -> comb::Alert {
    comb::Alert {
        kind: "unowned-results".to_owned(),
        title: "Some results have no owners".to_owned(),
        description: "For some results, no ownership data was found, or no rule applied to the result. [Learn more about configuring code ownership](https://sourcegraph.com/docs/own).".to_owned(),
        priority: 0,
        ..comb::Alert::default()
    }
}

fn max_alert(current: Option<comb::Alert>, candidate: Option<comb::Alert>) -> Option<comb::Alert> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => {
            if candidate.priority > current.priority {
                Some(candidate)
            } else {
                Some(current)
            }
        }
        (None, candidate) => candidate,
        (current, None) => current,
    }
}

/// `own.ByTextReference(ctx, db, text)` plus DB resolution: the text key plus
/// every reference the resolved user/team is known by (`bag.linkBack`).
fn build_bag(ownership: Option<&dyn OwnershipFetcher>, term: &str) -> search_owners::Bag {
    let mut bag = search_owners::Bag::by_text_reference(term);
    if let Some(fetcher) = ownership {
        for reference in fetcher.resolve_text(term) {
            bag.add_reference(&reference);
        }
    }
    bag
}

/// `ownersFilters` (`filter_job.go:154-172`).
pub fn ownership_terms_match(
    owners: &[OwnershipRef],
    search_terms: &[String],
    all_bags: &[search_owners::Bag],
    exclude: bool,
) -> bool {
    // Empty search terms means "any owner matches".
    if search_terms.len() == 1 && search_terms[0].is_empty() {
        return !owners.is_empty() == !exclude;
    }
    for bag in all_bags {
        let within = owners.iter().any(|owner| bag.contains(owner));
        if within == exclude {
            return false;
        }
    }
    true
}

/// `fileHasOwnersJob` (`filter_job.go:17-80`).
pub struct FileHasOwnersLeafJob {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for FileHasOwnersLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let Self {
            include,
            exclude,
            child,
        } = self;
        let ownership = leaf_clients(clients).and_then(LeafJobClients::ownership_fetcher);
        let include_bags: Vec<search_owners::Bag> =
            include.iter().map(|term| build_bag(ownership, term)).collect();
        let exclude_bags: Vec<search_owners::Bag> =
            exclude.iter().map(|term| build_bag(ownership, term)).collect();
        let mut had_error = false;

        let (mut alert, failure) = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let results = std::mem::take(&mut event.results);
                let (kept, error) = apply_ownership_filtering(
                    ownership,
                    &include_bags,
                    include,
                    &exclude_bags,
                    exclude,
                    results,
                );
                had_error = error;
                event.results = kept;
                stream(event);
            };
            child.run(ctx, clients, &mut filtered)
        };

        if had_error {
            alert = max_alert(alert, Some(alert_for_ownership_error()));
        }
        (alert, failure)
    }

    fn name(&self) -> &'static str {
        "FileHasOwnersFilterJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }
}

fn apply_ownership_filtering(
    ownership: Option<&dyn OwnershipFetcher>,
    include_bags: &[search_owners::Bag],
    include_terms: &[String],
    exclude_bags: &[search_owners::Bag],
    exclude_terms: &[String],
    matches: Vec<Match>,
) -> (Vec<Match>, bool) {
    let mut had_error = false;
    let mut filtered = Vec::with_capacity(matches.len());

    'matches: for result in matches {
        let (paths, commit, repo) = match &result {
            Match::File(fm) => (
                vec![fm.file.path.clone()],
                fm.file.commit_id.clone(),
                fm.file.repo.clone(),
            ),
            Match::Commit(cm) => (
                cm.modified_files.clone(),
                cm.commit.id.clone(),
                cm.repo.clone(),
            ),
            Match::Repo(_) | Match::Owner(_) => continue 'matches,
        };
        if paths.is_empty() {
            continue 'matches;
        }
        let Some(ownership) = ownership else {
            // Baseline: the rules fetch errors, the result is dropped and an
            // ownership-search-error alert is raised.
            had_error = true;
            continue 'matches;
        };
        let mut file_owner_sets = Vec::with_capacity(paths.len());
        for path in &paths {
            match ownership.owners(repo.id, &repo.name, &commit, path) {
                Ok(owners) => file_owner_sets.push(owners),
                Err(_) => {
                    had_error = true;
                    continue 'matches;
                }
            }
        }

        let mut file_matches_include_terms = false;
        for owners in &file_owner_sets {
            if !include_terms.is_empty()
                && ownership_terms_match(owners, include_terms, include_bags, false)
            {
                file_matches_include_terms = true;
            }
            if !exclude_terms.is_empty() && !ownership_terms_match(owners, exclude_terms, exclude_bags, true)
            {
                continue 'matches;
            }
        }
        if !include_terms.is_empty() && !file_matches_include_terms {
            continue 'matches;
        }
        filtered.push(result);
    }

    (filtered, had_error)
}

/// `own.Reference.ResolutionGuess` (`ownref.go:46`). Assigned owners
/// (`user_id`/`team_id`) are already resolved by the production fetcher; the
/// explicit `team_id` branch keeps those resolved teams intact.
pub fn resolution_guess(reference: &OwnershipRef) -> Option<ResolvedOwner> {
    if reference.handle.is_empty() && reference.email.is_empty() {
        return None;
    }
    let is_github_team = reference
        .code_host_kind
        .as_deref()
        .is_some_and(|kind| kind.eq_ignore_ascii_case("github"))
        && reference.handle.contains('/');
    if reference.team_id.is_some() || is_github_team {
        let name = reference.handle.clone();
        Some(ResolvedOwner::Team {
            handle: reference.handle.clone(),
            email: reference.email.clone(),
            name: name.clone(),
            display_name: name,
        })
    } else {
        Some(ResolvedOwner::Person {
            handle: reference.handle.clone(),
            email: reference.email.clone(),
            user: None,
        })
    }
}

/// `selectOwnersJob` (`select_job.go:17-95`).
pub struct SelectOwnersLeafJob {
    pub child: Box<dyn comb::CombinatorJob>,
}

impl comb::CombinatorJob for SelectOwnersLeafJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let ownership = leaf_clients(clients).and_then(LeafJobClients::ownership_fetcher);
        let mut had_error = false;
        let mut has_result_with_no_owners = false;
        let mut seen: HashSet<(String, String, String, String, String)> = HashSet::new();

        let (mut alert, failure) = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let mut owners = Vec::new();
                for result in std::mem::take(&mut event.results) {
                    let Match::File(fm) = result else {
                        continue;
                    };
                    let Some(ownership) = ownership else {
                        had_error = true;
                        continue;
                    };
                    let refs = match ownership.owners(
                        fm.file.repo.id,
                        &fm.file.repo.name,
                        &fm.file.commit_id,
                        &fm.file.path,
                    ) {
                        Ok(refs) => refs,
                        Err(_) => {
                            had_error = true;
                            continue;
                        }
                    };
                    if refs.is_empty() {
                        has_result_with_no_owners = true;
                        continue;
                    }
                    for reference in refs {
                        // `Reference.ResolutionGuess` returns nil when there is
                        // no handle or email to guess from; the baseline raises
                        // `ownership-search-error` and skips the reference.
                        let Some(owner) = resolution_guess(&reference) else {
                            had_error = true;
                            continue;
                        };
                        let (owner_type, owner_identifier) = match &owner {
                            ResolvedOwner::Person { handle, email, .. } => {
                                ("person", format!("Person:{handle}{email}"))
                            }
                            ResolvedOwner::Team { handle, .. } => {
                                ("team", format!("Team:{handle}"))
                            }
                        };
                        // `OwnerMatch.Key`: repo name, commit, input rev and
                        // `Type()+Identifier()`.
                        let key = (
                            owner_type.to_owned(),
                            owner_identifier,
                            fm.file.repo.name.clone(),
                            fm.file.commit_id.clone(),
                            fm.file.input_rev.clone().unwrap_or_default(),
                        );
                        if seen.insert(key) {
                            owners.push(Match::Owner(OwnerMatch {
                                owner,
                                repo: fm.file.repo.clone(),
                            }));
                        }
                    }
                }
                event.results = owners;
                stream(event);
            };
            self.child.run(ctx, clients, &mut filtered)
        };

        if had_error {
            alert = max_alert(alert, Some(alert_for_ownership_error()));
        }
        if has_result_with_no_owners {
            alert = max_alert(alert, Some(alert_for_unowned_result()));
        }
        (alert, failure)
    }

    fn name(&self) -> &'static str {
        "SelectOwnersSearchJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }
}
