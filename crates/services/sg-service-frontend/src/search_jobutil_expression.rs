//! jobutil expression jobs (`internal/search/job/jobutil/expression_job.go:1-195`).
//!
//! Ported:
//!
//! * [`new_and_job`] / [`AndJob`]: run every child, keep only matches seen by
//!   *all* children (an n-way intersection), forward an event when it has
//!   streamable matches or non-zero stats, and aggregate child errors and
//!   alerts.
//! * [`new_or_job`] / [`OrJob`]: stream the intersection live, then, after all
//!   children finish, stream the union minus the intersection
//!   (`Merger::UnsentTracked`). Child errors are joined and then filtered with
//!   `errors.Ignore(err, IsContextCanceled)`.
//! * The 0/1-child constructor simplifications (`NoopJob` / the child itself)
//!   and the `Name`/`Children`/`Attributes` descriptions.
//! * A private model of `result.Merger` (`internal/search/result/merger.go`),
//!   including `Match.Key`-equivalent deduplication, `AppendMatches` for
//!   file/repo/commit matches, the "sent once" flag and the popcount-ordered
//!   `UnsentTracked` union.
//!
//! The jobs implement [`CombinatorJob`] from
//! [`crate::search_jobutil_combinators`], so they can return an alert and a
//! classified [`JobFailure`]; existing `search_job::SearchJob` children plug in
//! through `SearchJobAdapter`. `search_job.rs`,
//! `search_jobutil_combinators.rs` and `search_jobutil_filters.rs` are not
//! modified.
//!
//! Not ported: the full job tree/registration, GraphQL/HTTP wiring, real
//! gRPC/DB clients, OpenTelemetry spans, and `MapChildren` tree rewriting.
//! Children run sequentially rather than through the baseline's 16-way pool
//! (the runtime clients/sender cannot be shared across threads without
//! changing `search_job.rs`); the observable sets match, differing only in the
//! baseline's scheduling-dependent ordering.
//!
//! See `docs/overviews/m9_8hk_jobutil_expression_overview.md`.

use std::collections::HashMap;

use crate::search_job::{RuntimeClients, Verbosity};
use crate::search_jobutil_combinators::{
    new_noop_job, Alert, CombinatorJob, JobFailure, MaxAlerter, RunContext, Sender,
};
use crate::search_stream::convert::{
    dedup_symbols, CommitMatch, Match, RepoMatch, ResolvedOwner, SymbolMatch,
};
use crate::search_stream::progress::{SearchEvent, Stats};

/// `NewAndJob` (`expression_job.go:17-26`).
pub fn new_and_job(mut children: Vec<Box<dyn CombinatorJob>>) -> Box<dyn CombinatorJob> {
    match children.len() {
        0 => new_noop_job(),
        1 => children.pop().expect("one child"),
        _ => Box::new(AndJob { children }),
    }
}

/// `NewOrJob` (`expression_job.go:91-102`).
pub fn new_or_job(mut children: Vec<Box<dyn CombinatorJob>>) -> Box<dyn CombinatorJob> {
    match children.len() {
        0 => new_noop_job(),
        1 => children.pop().expect("one child"),
        _ => Box::new(OrJob { children }),
    }
}

/// `AndJob` (`expression_job.go:28-89`).
pub struct AndJob {
    children: Vec<Box<dyn CombinatorJob>>,
}

impl CombinatorJob for AndJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let max_alerter = MaxAlerter::new();
        let mut merger = Merger::new(self.children.len());
        let mut errors: Vec<Option<JobFailure>> = Vec::with_capacity(self.children.len());

        for (child_num, child) in self.children.iter_mut().enumerate() {
            let mut intersecting = |event: SearchEvent| {
                let mut event = event;
                event.results = merger.add_matches(event.results, child_num);
                if !event.results.is_empty() || !event.stats.zero() {
                    stream(event);
                }
            };
            let (alert, err) = child.run(ctx, clients, &mut intersecting);
            max_alerter.add(alert);
            errors.push(err);
        }

        (max_alerter.alert(), join_errors(errors))
    }

    fn name(&self) -> &'static str {
        "AndJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        self.children.iter().map(AsRef::as_ref).collect()
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// `OrJob` (`expression_job.go:104-195`).
pub struct OrJob {
    children: Vec<Box<dyn CombinatorJob>>,
}

impl CombinatorJob for OrJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let max_alerter = MaxAlerter::new();
        let mut merger = Merger::new(self.children.len());
        let mut errors: Vec<Option<JobFailure>> = Vec::with_capacity(self.children.len());

        for (child_num, child) in self.children.iter_mut().enumerate() {
            let mut unioning = |event: SearchEvent| {
                let mut event = event;
                event.results = merger.add_matches(event.results, child_num);
                if !event.results.is_empty() || !event.stats.zero() {
                    stream(event);
                }
            };
            let (alert, err) = child.run(ctx, clients, &mut unioning);
            max_alerter.add(alert);
            errors.push(err);
        }

        let err = join_errors(errors);

        // Send results that were only seen by some of the sources, regardless
        // of whether a child errored.
        let unsent_tracked = merger.unsent_tracked();
        if !unsent_tracked.is_empty() {
            stream(SearchEvent {
                results: unsent_tracked,
                stats: Stats::default(),
            });
        }

        (max_alerter.alert(), ignore_canceled(err))
    }

    fn name(&self) -> &'static str {
        "OrJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        self.children.iter().map(AsRef::as_ref).collect()
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// `conc`'s `ErrorPool.Wait` collects every child error and returns
/// `errors.Join(errs...)`; a single error is kept as a single value here
/// (equivalent after flattening to parts). Nested [`JobFailure::Multi`] values
/// are flattened, which matches the differential normalizer's leaf view.
fn join_errors(errs: Vec<Option<JobFailure>>) -> Option<JobFailure> {
    let mut parts = Vec::new();
    for err in errs.into_iter().flatten() {
        match err {
            JobFailure::Multi(inner) => parts.extend(inner),
            other => parts.push(other),
        }
    }
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(JobFailure::Multi(parts)),
    }
}

/// `errors.Ignore(err, errors.IsContextCanceled)` for the error returned by
/// `p.Wait()`, i.e. a stdlib `errors.Join`.
///
/// `Ignore` first tries `As(err, *multiError)`. The pool's `*joinError` is not
/// a sourcegraph `*multiError`, so that branch does not match; the predicate
/// branch then calls `errors.Is(joinError, context.Canceled)`, which recurses
/// through `Unwrap() []error`. Therefore *any* canceled leaf makes the whole
/// joined error be dropped, even when it also contains non-canceled errors.
fn ignore_canceled(err: Option<JobFailure>) -> Option<JobFailure> {
    match err {
        Some(err) if err.is_canceled() => None,
        other => other,
    }
}

/// `result.Key` (`internal/search/result/match.go:57-100`). The fields match
/// the Go struct; commit author dates are encoded as Unix nanoseconds (the
/// baseline compares `time.Time` values by struct equality).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MatchKey {
    repo: String,
    rev: String,
    author_nanos: i128,
    commit: String,
    path: String,
    owner_metadata: String,
    type_rank: i32,
}

/// Baseline match ranks (`internal/search/result/match.go:44-50`).
mod rank {
    pub const FILE: i32 = 0;
    pub const COMMIT: i32 = 1;
    pub const DIFF: i32 = 2;
    pub const REPO: i32 = 3;
    pub const OWNER: i32 = 4;
}

fn match_key(match_: &Match) -> MatchKey {
    match match_ {
        Match::File(fm) => MatchKey {
            repo: fm.file.repo.name.clone(),
            rev: fm.file.input_rev.clone().unwrap_or_default(),
            author_nanos: 0,
            commit: fm.file.commit_id.clone(),
            path: fm.file.path.clone(),
            owner_metadata: String::new(),
            type_rank: rank::FILE,
        },
        Match::Repo(repo) => MatchKey {
            repo: repo.name.clone(),
            rev: repo.rev.clone(),
            author_nanos: 0,
            commit: String::new(),
            path: String::new(),
            owner_metadata: String::new(),
            type_rank: rank::REPO,
        },
        Match::Commit(cm) => MatchKey {
            repo: cm.repo.name.clone(),
            rev: String::new(),
            author_nanos: cm.commit.author.date.unix_timestamp_nanos(),
            commit: cm.commit.id.clone(),
            path: String::new(),
            owner_metadata: String::new(),
            type_rank: if cm.diff_preview.is_some() {
                rank::DIFF
            } else {
                rank::COMMIT
            },
        },
        Match::Owner(om) => MatchKey {
            repo: om.repo.name.clone(),
            // `convert::OwnerMatch` does not model `InputRev`/`CommitID`, so
            // those Go key fields are approximated as empty here.
            rev: String::new(),
            author_nanos: 0,
            commit: String::new(),
            path: String::new(),
            owner_metadata: owner_metadata(&om.owner),
            type_rank: rank::OWNER,
        },
    }
}

/// `OwnerMatch.Key`'s `OwnerMetadata = Type() + Identifier()`
/// (`internal/search/result/owner.go:20-40,81-93`).
fn owner_metadata(owner: &ResolvedOwner) -> String {
    match owner {
        ResolvedOwner::Person { handle, email, .. } => {
            format!("personPerson:{handle}{email}")
        }
        ResolvedOwner::Team { name, .. } => format!("teamTeam:{name}"),
    }
}

/// One tracked `mergeVal` (`internal/search/result/merger.go:33-37`).
struct MergeVal {
    match_: Match,
    seen: Vec<bool>,
    sent: bool,
}

impl MergeVal {
    fn pop_count(&self) -> usize {
        self.seen.iter().filter(|seen| **seen).count()
    }
}

/// Private port of `result.Merger` (`internal/search/result/merger.go:20-135`).
struct Merger {
    num_sources: usize,
    matches: HashMap<MatchKey, MergeVal>,
}

impl Merger {
    fn new(num_sources: usize) -> Self {
        Self {
            num_sources,
            matches: HashMap::new(),
        }
    }

    /// `Merger.AddMatches`: add a source's matches and return the matches that
    /// are now streamable (seen by every source).
    fn add_matches(&mut self, matches: Vec<Match>, source: usize) -> Vec<Match> {
        let mut streamable = Vec::with_capacity(matches.len());
        for match_ in matches {
            if let Some(streamable_match) = self.add_match(match_, source) {
                streamable.push(streamable_match);
            }
        }
        streamable
    }

    fn add_match(&mut self, match_: Match, source: usize) -> Option<Match> {
        let key = match_key(&match_);
        match self.matches.get_mut(&key) {
            Some(prev) if prev.sent => {
                // Already returned as streamable; ignore duplicate keys.
                None
            }
            Some(prev) => {
                append_matches(&mut prev.match_, &match_);
                if let Some(slot) = prev.seen.get_mut(source) {
                    *slot = true;
                }
                if prev.seen.iter().all(|seen| *seen) {
                    prev.sent = true;
                    Some(prev.match_.clone())
                } else {
                    None
                }
            }
            None => {
                let mut seen = vec![false; self.num_sources];
                if let Some(slot) = seen.get_mut(source) {
                    *slot = true;
                }
                self.matches.insert(
                    key,
                    MergeVal {
                        match_,
                        seen,
                        sent: false,
                    },
                );
                None
            }
        }
    }

    /// `Merger.UnsentTracked`: the tracked, not-yet-sent matches ordered by
    /// descending number of sources that saw them. The baseline's map iteration
    /// makes ties nondeterministic; this stable sort keeps insertion order.
    fn unsent_tracked(self) -> Vec<Match> {
        let mut values: Vec<MergeVal> = self
            .matches
            .into_values()
            .filter(|value| !value.sent)
            .collect();
        values.sort_by_key(|value| std::cmp::Reverse(value.pop_count()));
        values.into_iter().map(|value| value.match_).collect()
    }
}

/// `AppendMatches` (`file.go:169`, `commit.go:145`, `repo.go`, `owner.go`).
/// The baseline's type assertions cannot fail because the keys match.
fn append_matches(dst: &mut Match, src: &Match) {
    match (dst, src) {
        (Match::File(dst), Match::File(src)) => {
            dst.chunk_matches.extend(src.chunk_matches.iter().cloned());
            dst.symbols.extend(src.symbols.iter().cloned());
            dst.symbols.sort_by(compare_symbol_matches);
            let symbols = std::mem::take(&mut dst.symbols);
            dst.symbols = dedup_symbols(symbols);
            dst.limit_hit = dst.limit_hit || src.limit_hit;
        }
        (Match::Commit(dst), Match::Commit(src)) => append_commit_matches(dst, src),
        (Match::Repo(dst), Match::Repo(src)) => append_repo_matches(dst, src),
        // `OwnerMatch` has no `AppendMatches`; mismatched types cannot occur.
        _ => {}
    }
}

fn append_commit_matches(dst: &mut CommitMatch, src: &CommitMatch) {
    if let (Some(dst), Some(src)) = (&mut dst.message_preview, &src.message_preview) {
        dst.matched_ranges
            .extend(src.matched_ranges.iter().copied());
    }
}

fn append_repo_matches(dst: &mut RepoMatch, src: &RepoMatch) {
    dst.repo_name_matches
        .extend(src.repo_name_matches.iter().copied());
    dst.description_matches
        .extend(src.description_matches.iter().copied());
}

/// `compareSymbolMatches` (`internal/search/result/symbol.go:115-133`).
fn compare_symbol_matches(left: &SymbolMatch, right: &SymbolMatch) -> std::cmp::Ordering {
    left.symbol
        .line
        .cmp(&right.symbol.line)
        .then_with(|| left.symbol.name.cmp(&right.symbol.name))
        .then_with(|| left.symbol.kind.cmp(&right.symbol.kind))
        .then_with(|| left.symbol.parent.cmp(&right.symbol.parent))
        .then_with(|| left.symbol.parent_kind.cmp(&right.symbol.parent_kind))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_job::{JobError, JobStreamer, PrivateRepoLister, ReposListOptions};
    use crate::search_stream::convert::{ChunkMatch, FileMatch, FileRef, MinimalRepo};
    use crate::search_stream::events::Range;
    use sg_zoekt::indexed_repo_revs::MinimalRepo as ZoektMinimalRepo;
    use sg_zoekt::query::Query;
    use sg_zoekt::types::{SearchOptions, SearchResult};

    struct NoopStreamer;

    impl JobStreamer for NoopStreamer {
        fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            panic!("stream_search must not be called");
        }
    }

    struct NoopLister;

    impl PrivateRepoLister for NoopLister {
        fn list_minimal_repos(
            &self,
            _options: &ReposListOptions,
        ) -> Result<Vec<ZoektMinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    struct StaticJob {
        events: Vec<SearchEvent>,
        alert: Option<Alert>,
        error: Option<JobFailure>,
    }

    impl StaticJob {
        fn events(events: Vec<SearchEvent>) -> Self {
            Self {
                events,
                alert: None,
                error: None,
            }
        }
    }

    impl CombinatorJob for StaticJob {
        fn run(
            &mut self,
            _ctx: &RunContext,
            _clients: &RuntimeClients<'_>,
            stream: Sender<'_>,
        ) -> (Option<Alert>, Option<JobFailure>) {
            for event in &self.events {
                stream(event.clone());
            }
            (self.alert.clone(), self.error.clone())
        }

        fn name(&self) -> &'static str {
            "StaticJob"
        }
    }

    fn clients<'a>(db: &'a NoopLister, zoekt: &'a NoopStreamer) -> RuntimeClients<'a> {
        RuntimeClients {
            db,
            zoekt,
            max_repos: None,
            searcher: None,
            commit: None,
        }
    }

    fn file(path: &str, chunks: usize) -> Match {
        Match::File(FileMatch {
            file: FileRef {
                repo: MinimalRepo {
                    id: 1,
                    name: "repo".to_owned(),
                },
                commit_id: "c".to_owned(),
                path: path.to_owned(),
                input_rev: None,
                precise_language: String::new(),
            },
            chunk_matches: vec![
                ChunkMatch {
                    content: String::new(),
                    content_start: Default::default(),
                    ranges: vec![Range::default()],
                };
                chunks
            ],
            symbols: vec![],
            path_matches: vec![],
            limit_hit: false,
            debug: None,
        })
    }

    fn event(results: Vec<Match>) -> SearchEvent {
        SearchEvent {
            results,
            stats: Stats::default(),
        }
    }

    fn run(job: &mut dyn CombinatorJob) -> (Vec<SearchEvent>, Option<Alert>, Option<JobFailure>) {
        let streamer = NoopStreamer;
        let lister = NoopLister;
        let clients = clients(&lister, &streamer);
        let ctx = RunContext::default();
        let mut events = Vec::new();
        let mut sink = |event| events.push(event);
        let (alert, err) = job.run(&ctx, &clients, &mut sink);
        (events, alert, err)
    }

    #[test]
    fn constructor_simplifications_match_baseline() {
        assert_eq!(new_and_job(Vec::new()).name(), "NoopJob");
        assert_eq!(new_or_job(Vec::new()).name(), "NoopJob");

        let one = new_and_job(vec![Box::new(StaticJob::events(Vec::new()))]);
        assert_eq!(one.name(), "StaticJob");
        let one = new_or_job(vec![Box::new(StaticJob::events(Vec::new()))]);
        assert_eq!(one.name(), "StaticJob");

        let two = new_and_job(vec![
            Box::new(StaticJob::events(Vec::new())),
            Box::new(StaticJob::events(Vec::new())),
        ]);
        assert_eq!(two.name(), "AndJob");
        assert_eq!(two.children().len(), 2);
        assert!(two.attributes(Verbosity::Max).is_empty());
        let two = new_or_job(vec![
            Box::new(StaticJob::events(Vec::new())),
            Box::new(StaticJob::events(Vec::new())),
        ]);
        assert_eq!(two.name(), "OrJob");
        assert_eq!(two.children().len(), 2);
    }

    #[test]
    fn single_child_passes_events_through_unchanged() {
        for mut job in [
            new_and_job(vec![Box::new(StaticJob::events(vec![event(vec![file(
                "a", 1,
            )])]))]),
            new_or_job(vec![Box::new(StaticJob::events(vec![event(vec![file(
                "a", 1,
            )])]))]),
        ] {
            let (events, alert, err) = run(job.as_mut());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].results.len(), 1);
            assert!(alert.is_none());
            assert!(err.is_none());
        }
    }

    #[test]
    fn and_streams_intersection_once_with_merged_chunks() {
        let mut job = new_and_job(vec![
            Box::new(StaticJob::events(vec![event(vec![file("a", 1)])])),
            Box::new(StaticJob::events(vec![event(vec![file("a", 1)])])),
            Box::new(StaticJob::events(vec![event(vec![file("a", 1)])])),
        ]);
        let (events, _, err) = run(job.as_mut());
        assert!(err.is_none());
        // The intersection becomes streamable when the third source sees it.
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].results.len(), 1);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert_eq!(fm.chunk_matches.len(), 3);
    }

    #[test]
    fn and_drops_zero_result_zero_stats_events() {
        let mut job = new_and_job(vec![
            Box::new(StaticJob::events(vec![event(Vec::new())])),
            Box::new(StaticJob::events(vec![event(Vec::new())])),
        ]);
        let (events, _, err) = run(job.as_mut());
        assert!(events.is_empty());
        assert!(err.is_none());
    }

    #[test]
    fn or_streams_intersection_then_unsent_union() {
        let mut job = new_or_job(vec![
            Box::new(StaticJob::events(vec![event(vec![
                file("a", 1),
                file("b", 1),
            ])])),
            Box::new(StaticJob::events(vec![event(vec![
                file("a", 1),
                file("c", 1),
            ])])),
        ]);
        let (events, _, err) = run(job.as_mut());
        assert!(err.is_none());
        assert_eq!(events.len(), 2);
        // Live intersection: only "a".
        assert_eq!(events[0].results.len(), 1);
        // Trailing union minus intersection: "b" and "c" (order is not
        // guaranteed by the baseline).
        assert_eq!(events[1].results.len(), 2);
    }

    #[test]
    fn or_ignores_any_error_tree_containing_canceled() {
        let canceled_then_message = JobFailure::Multi(vec![
            JobFailure::Canceled,
            JobFailure::Message("boom".to_owned()),
        ]);
        // `errors.Is(joinError, context.Canceled)` is recursive, so the whole
        // joined error is dropped even though it also has a message.
        assert!(ignore_canceled(Some(canceled_then_message)).is_none());
        assert!(ignore_canceled(Some(JobFailure::Canceled)).is_none());
        assert_eq!(
            ignore_canceled(Some(JobFailure::DeadlineExceeded)),
            Some(JobFailure::DeadlineExceeded)
        );
        assert_eq!(
            ignore_canceled(Some(JobFailure::Message("boom".to_owned()))),
            Some(JobFailure::Message("boom".to_owned()))
        );
    }

    #[test]
    fn or_returns_unsent_results_even_when_a_child_errors() {
        let mut job = new_or_job(vec![
            Box::new(StaticJob::events(vec![event(vec![file("a", 1)])])),
            Box::new(StaticJob {
                events: vec![event(vec![file("b", 1)])],
                alert: None,
                error: Some(JobFailure::Message("boom".to_owned())),
            }),
        ]);
        let (events, _, err) = run(job.as_mut());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].results.len(), 2);
        assert_eq!(err, Some(JobFailure::Message("boom".to_owned())));
    }

    #[test]
    fn merger_never_sends_the_same_match_twice() {
        let mut merger = Merger::new(2);
        let first = merger.add_matches(vec![file("a", 1)], 0);
        assert!(first.is_empty());
        let second = merger.add_matches(vec![file("a", 1)], 1);
        assert_eq!(second.len(), 1);
        let third = merger.add_matches(vec![file("a", 1)], 1);
        assert!(third.is_empty());
        assert!(merger.unsent_tracked().is_empty());
    }

    #[test]
    fn error_join_keeps_child_order_and_flattens_parts() {
        let joined = join_errors(vec![
            Some(JobFailure::Message("first".to_owned())),
            None,
            Some(JobFailure::Multi(vec![
                JobFailure::Message("second".to_owned()),
                JobFailure::Message("third".to_owned()),
            ])),
        ])
        .expect("joined error");
        let parts = joined
            .parts()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(parts, vec!["first", "second", "third"]);
        assert!(join_errors(vec![None, None]).is_none());
    }
}
