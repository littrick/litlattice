//! jobutil repository pager and repository search
//! (`internal/search/job/jobutil/{repo_pager_job,repos}.go`).
//!
//! Ported:
//!
//! * [`RepoPagerJob`]: the per-page `MaybeSendStats` → partition →
//!   `child.Resolve` → child run loop, `skipPartitioning`, and error
//!   propagation of the page iterator (`repo_pager_job.go:95-136`);
//! * [`ReposPartialJob`] and [`set_repos`]: the `Partial`/`Resolve` contract
//!   and the `job.Map`-style rewrite that assigns the indexed set to
//!   zoekt jobs and the unindexed list to searcher/commit/structural jobs
//!   (`repo_pager_job.go:29-93`);
//! * [`RepoSearchJob`]: per-page result events, repo-description lookup and
//!   the name/description match-range computation
//!   (`repos.go:21-165`).
//!
//! Reuses [`crate::search_job::{SearchJob, Verbosity, JobContext,
//! RuntimeClients, JobError}`] without modifying `search_job.rs`, and
//! [`sg_zoekt::indexed_repo_revs::partition_repos`] for the
//! indexed/unindexed split.
//!
//! Database/repository-list/index queries are injected through
//! [`RepoPageSource`], [`RepoDescriptionLister`] and [`ZoektRepoLister`].
//! The concrete search backends (zoekt/searcher/commit/structural `Run`) and
//! the job-tree builders/registration are not ported; [`RepoJob::run`] only
//! executes the synthetic [`RepoJob::Container`] and [`RepoJob::Passthrough`]
//! nodes, while the six real leaf kinds are structural placeholders.
//!
//! See `docs/overviews/m9_8hl_jobutil_repos_overview.md`.

use std::collections::BTreeMap;

use regex::Regex;
use sg_search_query::validate::parse_yes_no_only;
use sg_zoekt::indexed_repo_revs::{partition_repos, IndexedRepoRevs, RepositoryRevisions};
use sg_zoekt::query::IndexedRequestType;
use sg_zoekt::types::ListResponse;

use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_stream::convert::{Match, RepoMatch};
use crate::search_stream::events::{Location, Range};
use crate::search_stream::progress::{SearchEvent, Stats};

/// `searchrepos.ErrNoResolvedRepos` text
/// (`internal/search/repos/repos.go:1213`). `RepoSearchJob.Run` ignores an
/// iterator error with this message.
pub const ERR_NO_RESOLVED_REPOS: &str = "no resolved repositories";

// ---------------------------------------------------------------------------
// `search.RepoOptions` attributes subset
// ---------------------------------------------------------------------------

/// Minimal `search.RepoOptions` (`internal/search/types.go:427-459`).
///
/// `visibility` and `use_index` are kept as raw strings so the Go zero value
/// (`""`) is reproduced in [`RepoOptions::attributes`]; unmodeled fields
/// (`RepoFilters`, `CommitAfter`, `Cursors`, `HasFileContent`, `HasKVPs`,
/// `HasTopics`) are never emitted by the differential corpus.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoOptions {
    pub minus_repo_filters: Vec<String>,
    pub description_patterns: Vec<String>,
    pub case_sensitive_repo_filters: bool,
    pub search_context_spec: String,
    pub visibility: String,
    pub limit: i64,
    pub use_index: String,
    pub fork_set: bool,
    pub no_forks: bool,
    pub only_forks: bool,
    pub only_cloned: bool,
    pub archived_set: bool,
    pub no_archived: bool,
    pub only_archived: bool,
}

impl RepoOptions {
    /// `RepoOptions.Attributes` (`internal/search/types.go:461-560`),
    /// restricted to the modeled fields and preserving the baseline order.
    pub fn attributes(&self) -> Vec<(String, String)> {
        let mut res = Vec::new();
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
            res.push((
                "caseSensitiveRepoFilters".to_owned(),
                self.case_sensitive_repo_filters.to_string(),
            ));
        }
        if !self.search_context_spec.is_empty() {
            res.push((
                "searchContextSpec".to_owned(),
                self.search_context_spec.clone(),
            ));
        }
        // Go zero value `""` is not `query.Any`, so it is emitted as-is.
        if self.visibility != "Any" {
            res.push(("visibility".to_owned(), self.visibility.clone()));
        }
        if self.limit > 0 {
            res.push(("limit".to_owned(), self.limit.to_string()));
        }
        // Go zero value `""` is not `query.Yes` ("yes").
        if self.use_index != "yes" {
            res.push(("useIndex".to_owned(), self.use_index.clone()));
        }
        if self.fork_set {
            res.push(("forkSet".to_owned(), self.fork_set.to_string()));
        }
        if !self.no_forks {
            res.push(("noForks".to_owned(), self.no_forks.to_string()));
        }
        if self.only_forks {
            res.push(("onlyForks".to_owned(), self.only_forks.to_string()));
        }
        if self.only_cloned {
            res.push(("onlyCloned".to_owned(), self.only_cloned.to_string()));
        }
        if self.archived_set {
            res.push(("archivedSet".to_owned(), self.archived_set.to_string()));
        }
        if !self.no_archived {
            res.push(("noArchived".to_owned(), self.no_archived.to_string()));
        }
        if self.only_archived {
            res.push(("onlyArchived".to_owned(), self.only_archived.to_string()));
        }
        res
    }

    /// `trace.Scoped("repoOpts", ...)`.
    fn scoped_attributes(&self) -> Vec<(String, String)> {
        self.attributes()
            .into_iter()
            .map(|(key, value)| (format!("repoOpts.{key}"), value))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Injected clients
// ---------------------------------------------------------------------------

/// One `repos.Resolved` page (`internal/search/repos/repos.go:48-55`).
#[derive(Debug, Clone, Default)]
pub struct RepoPage {
    pub repo_revs: Vec<RepositoryRevisions>,
    pub backends_missing: i64,
}

impl RepoPage {
    /// `Resolved.MaybeSendStats`.
    pub fn maybe_send_stats(&self, stream: &mut dyn FnMut(SearchEvent)) {
        if self.backends_missing > 0 {
            stream(SearchEvent {
                results: Vec::new(),
                stats: Stats {
                    backends_missing: self.backends_missing,
                    ..Stats::default()
                },
            });
        }
    }
}

/// `repos.Resolver.Iterator` reduced to its observable behavior: a sequence
/// of pages followed by an optional terminal error.
pub trait RepoPageSource {
    fn pages(&self) -> Vec<RepoPage>;
    fn error(&self) -> Option<JobError>;
}

/// `db.Repos().GetRepoDescriptionsByIDs`.
pub trait RepoDescriptionLister {
    fn get_repo_descriptions_by_ids(&self, ids: &[u32]) -> Result<BTreeMap<u32, String>, JobError>;
}

/// `zoekt.Streamer.List` result used by `zoekt.PartitionRepos`.
pub trait ZoektRepoLister {
    fn list(&self) -> Result<ListResponse, JobError>;
}

// ---------------------------------------------------------------------------
// resolvedRepos / reposPartialJob / setRepos
// ---------------------------------------------------------------------------

/// `resolvedRepos` (`repo_pager_job.go:29-34`). `indexed` is `None` for a
/// nil `*zoekt.IndexedRepoRevs` (`skipPartitioning`).
#[derive(Debug, Clone, Default)]
pub struct ResolvedRepos {
    pub indexed: Option<IndexedRepoRevs>,
    pub unindexed: Vec<RepositoryRevisions>,
}

impl ResolvedRepos {
    pub fn new(indexed: Option<IndexedRepoRevs>, unindexed: Vec<RepositoryRevisions>) -> Self {
        Self { indexed, unindexed }
    }
}

/// Minimal job tree the pager can populate.
///
/// The six real kinds mirror `setRepos`' type switch. `Container` and
/// `Passthrough` are synthetic nodes used by the differential harness to
/// exercise the bottom-up traversal and event passthrough; the baseline's
/// actual container jobs (`SequentialJob`, ...) and leaf backends are not
/// ported.
#[derive(Debug, Clone)]
pub enum RepoJob {
    ZoektRepoSubsetText {
        repos: Option<IndexedRepoRevs>,
    },
    SearcherText {
        repos: Vec<RepositoryRevisions>,
    },
    ZoektSymbol {
        repos: Option<IndexedRepoRevs>,
    },
    SearcherSymbol {
        repos: Vec<RepositoryRevisions>,
    },
    CommitSearch {
        diff: bool,
        repos: Vec<RepositoryRevisions>,
    },
    StructuralSearch {
        indexed: Option<IndexedRepoRevs>,
        unindexed: Vec<RepositoryRevisions>,
    },
    Container {
        label: String,
        children: Vec<RepoJob>,
    },
    Passthrough {
        label: String,
        events: Vec<SearchEvent>,
    },
}

impl RepoJob {
    /// The baseline job's `Name()`.
    pub fn name(&self) -> &str {
        match self {
            Self::ZoektRepoSubsetText { .. } => "ZoektRepoSubsetTextSearchJob",
            Self::SearcherText { .. } => "SearcherTextSearchJob",
            Self::ZoektSymbol { .. } => "ZoektSymbolSearchJob",
            Self::SearcherSymbol { .. } => "SearcherSymbolSearchJob",
            Self::CommitSearch { diff, .. } => {
                if *diff {
                    "DiffSearchJob"
                } else {
                    "CommitSearchJob"
                }
            }
            Self::StructuralSearch { .. } => "StructuralSearchJob",
            Self::Container { label, .. } | Self::Passthrough { label, .. } => label,
        }
    }

    /// `Describer.Children`, restricted to the modeled tree.
    pub fn children(&self) -> &[RepoJob] {
        match self {
            Self::Container { children, .. } => children,
            _ => &[],
        }
    }

    /// Run the resolved tree. Only the synthetic nodes do work: a
    /// [`RepoJob::Container`] runs its children in order (stopping on the
    /// first error), a [`RepoJob::Passthrough`] emits its events. The six
    /// real kinds are backend placeholders and return `Ok(())`.
    pub fn run(
        &mut self,
        _ctx: &JobContext,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        match self {
            Self::Container { children, .. } => {
                for child in children {
                    child.run(_ctx, stream)?;
                }
                Ok(())
            }
            Self::Passthrough { events, .. } => {
                for event in events.iter() {
                    stream(event.clone());
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// `setRepos` (`repo_pager_job.go:59-93`). Mirrors `job.Map`'s bottom-up
/// order: containers rewrite their children first and are otherwise
/// unchanged; the six leaf kinds receive the indexed/unindexed sets.
pub fn set_repos(
    job: &RepoJob,
    indexed: Option<&IndexedRepoRevs>,
    unindexed: &[RepositoryRevisions],
) -> RepoJob {
    match job {
        RepoJob::Container { label, children } => RepoJob::Container {
            label: label.clone(),
            children: children
                .iter()
                .map(|child| set_repos(child, indexed, unindexed))
                .collect(),
        },
        RepoJob::ZoektRepoSubsetText { .. } => RepoJob::ZoektRepoSubsetText {
            repos: indexed.cloned(),
        },
        RepoJob::SearcherText { .. } => RepoJob::SearcherText {
            repos: unindexed.to_vec(),
        },
        RepoJob::ZoektSymbol { .. } => RepoJob::ZoektSymbol {
            repos: indexed.cloned(),
        },
        RepoJob::SearcherSymbol { .. } => RepoJob::SearcherSymbol {
            repos: unindexed.to_vec(),
        },
        RepoJob::CommitSearch { diff, .. } => RepoJob::CommitSearch {
            diff: *diff,
            repos: unindexed.to_vec(),
        },
        RepoJob::StructuralSearch { .. } => RepoJob::StructuralSearch {
            indexed: indexed.cloned(),
            unindexed: unindexed.to_vec(),
        },
        RepoJob::Passthrough { .. } => job.clone(),
    }
}

/// `reposPartialJob` (`repo_pager_job.go:36-57`). This is a partial job, not
/// a runnable `Job`, so it does not implement [`SearchJob`].
#[derive(Debug, Clone)]
pub struct ReposPartialJob {
    pub inner: RepoJob,
}

impl ReposPartialJob {
    pub fn new(inner: RepoJob) -> Self {
        Self { inner }
    }

    /// `reposPartialJob.Partial`.
    pub fn partial(&self) -> &RepoJob {
        &self.inner
    }

    /// `reposPartialJob.Resolve`.
    pub fn resolve(&self, repos: &ResolvedRepos) -> RepoJob {
        set_repos(&self.inner, repos.indexed.as_ref(), &repos.unindexed)
    }

    /// `reposPartialJob.Name`.
    pub fn name(&self) -> &'static str {
        "PartialReposJob"
    }

    /// `reposPartialJob.Attributes` (always nil).
    pub fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }

    /// `reposPartialJob.Children` names.
    pub fn children_names(&self) -> Vec<String> {
        vec![self.inner.name().to_owned()]
    }
}

// ---------------------------------------------------------------------------
// repoPagerJob
// ---------------------------------------------------------------------------

/// `repoPagerJob` (`repo_pager_job.go:19-27`).
pub struct RepoPagerJob<'a> {
    pub repo_opts: RepoOptions,
    pub contains_ref_globs: bool,
    pub child: ReposPartialJob,
    pub skip_partitioning: bool,
    pub pages: &'a dyn RepoPageSource,
    pub zoekt: &'a dyn ZoektRepoLister,
}

impl RepoPagerJob<'_> {
    /// `repoPagerJob.Run`, minus alerts and tracing spans. The baseline
    /// aggregates child alerts through `search.MaxAlerter`; the modeled
    /// children never produce alerts, so only the error path remains.
    pub fn run_events(
        &mut self,
        ctx: &JobContext,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let pages = self.pages.pages();
        for page in &pages {
            page.maybe_send_stats(stream);

            let resolved = if self.skip_partitioning {
                ResolvedRepos {
                    indexed: None,
                    unindexed: page.repo_revs.clone(),
                }
            } else {
                let use_index = parse_yes_no_only(&self.repo_opts.use_index);
                match partition_repos(
                    &page.repo_revs,
                    IndexedRequestType::Text,
                    use_index,
                    self.contains_ref_globs,
                    || self.zoekt.list(),
                ) {
                    Ok((indexed, unindexed)) => ResolvedRepos {
                        indexed: Some(indexed),
                        unindexed,
                    },
                    Err(err) => return Err(JobError(err.to_string())),
                }
            };

            let mut job = self.child.resolve(&resolved);
            job.run(ctx, stream)?;
        }

        match self.pages.error() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl SearchJob for RepoPagerJob<'_> {
    fn run(
        &mut self,
        ctx: &JobContext,
        _clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        self.run_events(ctx, stream)
    }

    fn name(&self) -> &'static str {
        "RepoPagerJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        // `VerbosityMax` falls through to `VerbosityBasic`.
        if matches!(verbosity, Verbosity::Max) {
            res.push((
                "containsRefGlobs".to_owned(),
                self.contains_ref_globs.to_string(),
            ));
        }
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.extend(self.repo_opts.scoped_attributes());
        }
        res
    }
}

// ---------------------------------------------------------------------------
// RepoSearchJob
// ---------------------------------------------------------------------------

/// `RepoSearchJob` (`repos.go:21-25`).
pub struct RepoSearchJob<'a> {
    pub repo_opts: RepoOptions,
    pub description_patterns: Vec<Regex>,
    pub repo_name_patterns: Vec<Regex>,
    pub pages: &'a dyn RepoPageSource,
    pub descriptions: &'a dyn RepoDescriptionLister,
}

impl RepoSearchJob<'_> {
    /// `RepoSearchJob.repoDescriptions`.
    fn repo_descriptions(
        &self,
        repo_revs: &[RepositoryRevisions],
    ) -> Result<BTreeMap<u32, String>, JobError> {
        let ids: Vec<u32> = repo_revs.iter().map(|repo_rev| repo_rev.repo.id).collect();
        self.descriptions.get_repo_descriptions_by_ids(&ids)
    }

    /// `RepoSearchJob.Run`, minus the tracing span and with the injected
    /// page source/description store.
    pub fn run_events(&mut self, stream: &mut dyn FnMut(SearchEvent)) -> Result<(), JobError> {
        let pages = self.pages.pages();
        for page in &pages {
            page.maybe_send_stats(stream);

            let description_matches = if self.description_patterns.is_empty() {
                BTreeMap::new()
            } else {
                let repo_descriptions = self.repo_descriptions(&page.repo_revs)?;
                description_match_ranges(&repo_descriptions, &self.description_patterns)
            };

            stream(SearchEvent {
                results: repo_revs_to_repo_matches(
                    &page.repo_revs,
                    &self.repo_name_patterns,
                    &description_matches,
                ),
                stats: Stats::default(),
            });
        }

        // Do not error with no results for repo search.
        match self.pages.error() {
            Some(err) if err.0 == ERR_NO_RESOLVED_REPOS => Ok(()),
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl SearchJob for RepoSearchJob<'_> {
    fn run(
        &mut self,
        _ctx: &JobContext,
        _clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        self.run_events(stream)
    }

    fn name(&self) -> &'static str {
        "RepoSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.extend(self.repo_opts.scoped_attributes());
            res.push((
                "repoNamePatterns".to_owned(),
                string_slice(self.repo_name_patterns.iter().map(Regex::as_str)),
            ));
        }
        res
    }
}

/// `repoMatchRanges` (`repos.go:145-165`).
///
/// Go's `FindAllStringSubmatchIndex` and Rust's [`Regex::find_iter`] both
/// return leftmost, non-overlapping matches in order; only the full-match
/// offsets are read. Zero-width match iteration is not exercised by the
/// corpus.
pub fn repo_match_ranges(repo_name: &str, repo_name_regexps: &[Regex]) -> Vec<Range> {
    let mut res = Vec::new();
    for repo_name_re in repo_name_regexps {
        for matched in repo_name_re.find_iter(repo_name) {
            res.push(range_from_offsets(
                repo_name,
                matched.start(),
                matched.end(),
            ));
        }
    }
    res
}

/// `descriptionMatchRanges` (`repos.go:78-102`).
pub fn description_match_ranges(
    repo_descriptions: &BTreeMap<u32, String>,
    description_patterns: &[Regex],
) -> BTreeMap<u32, Vec<Range>> {
    let mut res: BTreeMap<u32, Vec<Range>> = BTreeMap::new();
    for (repo_id, repo_description) in repo_descriptions {
        for re in description_patterns {
            for matched in re.find_iter(repo_description) {
                res.entry(*repo_id).or_default().push(range_from_offsets(
                    repo_description,
                    matched.start(),
                    matched.end(),
                ));
            }
        }
    }
    res
}

/// `repoRevsToRepoMatches` (`repos.go:122-143`).
pub fn repo_revs_to_repo_matches(
    repos: &[RepositoryRevisions],
    repo_name_regexps: &[Regex],
    description_matches: &BTreeMap<u32, Vec<Range>>,
) -> Vec<Match> {
    let mut matches = Vec::with_capacity(repos.len());

    for repo in repos {
        // Get repo name matches once per repo.
        let repo_name_matches = repo_match_ranges(&repo.repo.name, repo_name_regexps);

        for rev in &repo.revs {
            let mut repo_match = RepoMatch {
                name: repo.repo.name.clone(),
                id: repo.repo.id as i32,
                rev: rev.clone(),
                description_matches: Vec::new(),
                repo_name_matches: repo_name_matches.clone(),
            };
            if let Some(ranges) = description_matches.get(&repo.repo.id) {
                repo_match.description_matches = ranges.clone();
            }
            matches.push(Match::Repo(repo_match));
        }
    }

    matches
}

/// Build a [`Range`] from a byte-offset match. `Column` counts UTF-8 runes
/// after the last newline, matching Go's `utf8.RuneCountInString` for the
/// single-line repo name/description inputs.
fn range_from_offsets(text: &str, start: usize, end: usize) -> Range {
    Range {
        start: Location {
            offset: start as i64,
            line: 0,
            column: text[..start].chars().count() as i64,
        },
        end: Location {
            offset: end as i64,
            line: 0,
            column: text[..end].chars().count() as i64,
        },
    }
}

/// Normalize a Go `attribute.StringSlice` the way the differential oracles
/// do: `"[a b]"` (and `"[]"` when empty).
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
    use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisionsRepo};
    use sg_zoekt::query::Query;
    use sg_zoekt::types::{SearchOptions, SearchResult};

    struct NoopLister;
    impl RepoDescriptionLister for NoopLister {
        fn get_repo_descriptions_by_ids(
            &self,
            _ids: &[u32],
        ) -> Result<BTreeMap<u32, String>, JobError> {
            Ok(BTreeMap::new())
        }
    }

    struct NoopPages {
        pages: Vec<RepoPage>,
        error: Option<JobError>,
    }
    impl RepoPageSource for NoopPages {
        fn pages(&self) -> Vec<RepoPage> {
            self.pages.clone()
        }
        fn error(&self) -> Option<JobError> {
            self.error.clone()
        }
    }

    struct EmptyZoekt;
    impl ZoektRepoLister for EmptyZoekt {
        fn list(&self) -> Result<ListResponse, JobError> {
            Ok(ListResponse::default())
        }
    }

    struct NoopRepoLister;
    impl crate::search_job::PrivateRepoLister for NoopRepoLister {
        fn list_minimal_repos(
            &self,
            _options: &crate::search_job::ReposListOptions,
        ) -> Result<Vec<MinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    struct NoopStreamer;
    impl crate::search_job::JobStreamer for NoopStreamer {
        fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn clients<'a>(lister: &'a NoopRepoLister, streamer: &'a NoopStreamer) -> RuntimeClients<'a> {
        RuntimeClients {
            db: lister,
            zoekt: streamer,
            max_repos: None,
            searcher: None,
        }
    }

    fn owner(id: u32, name: &str, revs: &[&str]) -> RepositoryRevisions {
        RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id,
                name: name.to_owned(),
            },
            revs: revs.iter().map(|rev| (*rev).to_owned()).collect(),
        }
    }

    #[test]
    fn set_repos_assigns_indexed_and_unindexed_per_kind() {
        let indexed = IndexedRepoRevs::default();
        let unindexed = vec![owner(2, "two", &["main"])];
        let tree = RepoJob::Container {
            label: "seq".to_owned(),
            children: vec![
                RepoJob::ZoektRepoSubsetText { repos: None },
                RepoJob::SearcherText { repos: Vec::new() },
                RepoJob::ZoektSymbol { repos: None },
                RepoJob::SearcherSymbol { repos: Vec::new() },
                RepoJob::CommitSearch {
                    diff: false,
                    repos: Vec::new(),
                },
                RepoJob::StructuralSearch {
                    indexed: None,
                    unindexed: Vec::new(),
                },
                RepoJob::Passthrough {
                    label: "emit".to_owned(),
                    events: Vec::new(),
                },
            ],
        };

        let resolved = set_repos(&tree, Some(&indexed), &unindexed);
        let RepoJob::Container { children, .. } = resolved else {
            panic!("expected container");
        };
        assert!(matches!(
            &children[0],
            RepoJob::ZoektRepoSubsetText { repos: Some(_) }
        ));
        assert!(matches!(
            &children[1],
            RepoJob::SearcherText { repos } if repos == &unindexed
        ));
        assert!(matches!(
            &children[2],
            RepoJob::ZoektSymbol { repos: Some(_) }
        ));
        assert!(matches!(
            &children[3],
            RepoJob::SearcherSymbol { repos } if repos == &unindexed
        ));
        assert!(matches!(
            &children[4],
            RepoJob::CommitSearch { repos, .. } if repos == &unindexed
        ));
        assert!(matches!(
            &children[5],
            RepoJob::StructuralSearch { indexed: Some(_), unindexed: u } if u == &unindexed
        ));
        assert!(matches!(&children[6], RepoJob::Passthrough { .. }));
    }

    #[test]
    fn set_repos_nil_indexed_stays_nil() {
        let tree = RepoJob::ZoektRepoSubsetText {
            repos: Some(IndexedRepoRevs::default()),
        };
        let resolved = set_repos(&tree, None, &[]);
        assert!(matches!(
            resolved,
            RepoJob::ZoektRepoSubsetText { repos: None }
        ));
    }

    #[test]
    fn repo_match_ranges_counts_utf8_runes() {
        let patterns = vec![Regex::new("é+").unwrap(), Regex::new("c").unwrap()];
        let ranges = repo_match_ranges("aébc", &patterns);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start.offset, 1);
        assert_eq!(ranges[0].end.offset, 3);
        assert_eq!(ranges[0].start.column, 1);
        assert_eq!(ranges[0].end.column, 2);
        assert_eq!(ranges[1].start.offset, 4);
        assert_eq!(ranges[1].start.column, 3);
        assert_eq!(ranges[0].start.line, 0);
    }

    #[test]
    fn description_match_ranges_only_records_hits() {
        let mut descriptions = BTreeMap::new();
        descriptions.insert(1, "hello world".to_owned());
        descriptions.insert(2, "absent".to_owned());
        let patterns = vec![Regex::new("o").unwrap()];
        let ranges = description_match_ranges(&descriptions, &patterns);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[&1].len(), 2);
        assert_eq!(ranges[&1][0].start.offset, 4);
        assert_eq!(ranges[&1][1].start.offset, 7);
    }

    #[test]
    fn repo_revs_to_repo_matches_one_match_per_rev() {
        let repos = vec![owner(1, "repo", &["", "dev"]), owner(2, "other", &["main"])];
        let patterns = vec![Regex::new("rep").unwrap()];
        let mut descriptions = BTreeMap::new();
        descriptions.insert(1, vec![Range::default()]);
        let matches = repo_revs_to_repo_matches(&repos, &patterns, &descriptions);
        assert_eq!(matches.len(), 3);
        let Match::Repo(first) = &matches[0] else {
            panic!("expected repo match");
        };
        assert_eq!(first.rev, "");
        assert_eq!(first.id, 1);
        assert_eq!(first.description_matches.len(), 1);
        assert_eq!(first.repo_name_matches.len(), 1);
        let Match::Repo(third) = &matches[2] else {
            panic!("expected repo match");
        };
        assert!(third.description_matches.is_empty());
    }

    #[test]
    fn repo_search_empty_page_still_sends_event_and_ignores_no_repos_error() {
        let pages = NoopPages {
            pages: vec![RepoPage::default()],
            error: Some(JobError(ERR_NO_RESOLVED_REPOS.to_owned())),
        };
        let descriptions = NoopLister;
        let mut job = RepoSearchJob {
            repo_opts: RepoOptions::default(),
            description_patterns: Vec::new(),
            repo_name_patterns: Vec::new(),
            pages: &pages,
            descriptions: &descriptions,
        };
        let mut events = Vec::new();
        job.run_events(&mut |event| events.push(event)).unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].results.is_empty());
    }

    #[test]
    fn repo_search_propagates_other_iterator_errors() {
        let pages = NoopPages {
            pages: Vec::new(),
            error: Some(JobError("boom".to_owned())),
        };
        let descriptions = NoopLister;
        let mut job = RepoSearchJob {
            repo_opts: RepoOptions::default(),
            description_patterns: Vec::new(),
            repo_name_patterns: Vec::new(),
            pages: &pages,
            descriptions: &descriptions,
        };
        let err = job.run_events(&mut |_| {}).unwrap_err();
        assert_eq!(err.0, "boom");
    }

    #[test]
    fn pager_skip_partitioning_passes_events_through() {
        let event = SearchEvent::default();
        let pages = NoopPages {
            pages: vec![RepoPage {
                repo_revs: vec![owner(1, "repo", &["main"])],
                backends_missing: 0,
            }],
            error: None,
        };
        let zoekt = EmptyZoekt;
        let mut job = RepoPagerJob {
            repo_opts: RepoOptions::default(),
            contains_ref_globs: false,
            child: ReposPartialJob::new(RepoJob::Container {
                label: "seq".to_owned(),
                children: vec![RepoJob::Passthrough {
                    label: "emit".to_owned(),
                    events: vec![event],
                }],
            }),
            skip_partitioning: true,
            pages: &pages,
            zoekt: &zoekt,
        };
        let ctx = JobContext::default();
        let mut events = Vec::new();
        job.run_events(&ctx, &mut |event| events.push(event))
            .unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn pager_attributes_scope_repo_opts() {
        let pages = NoopPages {
            pages: Vec::new(),
            error: None,
        };
        let zoekt = EmptyZoekt;
        let job = RepoPagerJob {
            repo_opts: RepoOptions {
                use_index: "only".to_owned(),
                limit: 5,
                ..RepoOptions::default()
            },
            contains_ref_globs: true,
            child: ReposPartialJob::new(RepoJob::Passthrough {
                label: "emit".to_owned(),
                events: Vec::new(),
            }),
            skip_partitioning: false,
            pages: &pages,
            zoekt: &zoekt,
        };
        assert_eq!(
            job.attributes(Verbosity::Max),
            vec![
                ("containsRefGlobs".to_owned(), "true".to_owned()),
                ("repoOpts.visibility".to_owned(), String::new()),
                ("repoOpts.limit".to_owned(), "5".to_owned()),
                ("repoOpts.useIndex".to_owned(), "only".to_owned()),
                ("repoOpts.noForks".to_owned(), "false".to_owned()),
                ("repoOpts.noArchived".to_owned(), "false".to_owned()),
            ]
        );
        assert_eq!(job.name(), "RepoPagerJob");
        assert_eq!(job.child.name(), "PartialReposJob");
        assert_eq!(job.child.children_names(), vec!["emit".to_owned()]);
        assert!(job.children().is_empty());
        // Keep the fake clients constructor exercised.
        let (lister, streamer) = (NoopRepoLister, NoopStreamer);
        let clients = clients(&lister, &streamer);
        assert!(clients.db.list_minimal_repos(&Default::default()).is_ok());
    }
}
