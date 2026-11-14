//! `internal/search/zoekt/indexed_search.go:273-437`: zoekt search execution
//! (`zoektSearch`, `DoZoektSearchGlobal`) and result sending (`sendMatches`).
//!
//! This module drives a [`Streamer`] and forwards normalized events to the
//! frontend event stream ([`super::progress::SearchEvent`]). The pure
//! `FileMatch` conversions live in [`super::zoekt_results`]; repository/ref
//! attribution lives in [`sg_zoekt::indexed_repo_revs::IndexedRepoRevs`].
//!
//! The baseline receives a `context.Context` and a `zoekt.Streamer` whose
//! sender yields full `*zoekt.SearchResult` values. There is no context in this
//! port: callers inject a [`std::time::Instant`] deadline and a `since`
//! function (see §4 of the M9.8gz overview). `sg_zoekt::streamer::Streamer`
//! yields wire [`sg_zoekt::types::SearchChunk`]s, which now carry the full
//! `Stats`/`Progress` the result-level [`Streamer`] needs; use
//! [`wire_chunk_to_search_result`] to adapt them without changing event
//! encoding (M9.8ib).

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sg_search_query::SearchType;
use sg_zoekt::indexed_repo_revs::IndexedRepoRevs;
use sg_zoekt::query::{BranchRepos, IndexedRequestType, Query};
use sg_zoekt::types::{
    FileMatch, SearchChunk, SearchOptions, SearchResult, DEFAULT_DOCUMENT_RANKS_WEIGHT,
    DEFAULT_FLUSH_WALL_TIME, DEFAULT_MAX_WALL_TIME, DEFAULT_SHARD_MAX_MATCH_COUNT,
    DEFAULT_TOTAL_MAX_MATCH_COUNT,
};

use super::convert;
use super::progress::{self, repo_status, SearchEvent, Stats};
use super::zoekt_results;

/// `filter.Repository`: the `select` root that returns repository matches.
pub const SELECT_REPOSITORY: &str = "repo";

/// `filter.SelectPath`: a parsed `select:` value (`internal/search/filter/select.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectPath(pub Vec<String>);

impl SelectPath {
    /// `SelectPath.Root`: the first element, or the empty string.
    pub fn root(&self) -> &str {
        self.0.first().map(String::as_str).unwrap_or("")
    }

    /// `SelectPath.Root() == filter.Repository`.
    pub fn is_repository(&self) -> bool {
        self.root() == SELECT_REPOSITORY
    }
}

/// `search.Features` fields read by `ToSearchOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Features {
    /// `Features.Debug`: set `SearchOptions.DebugScore`.
    pub debug: bool,
    /// `Features.ZoektSearchOptionsOverride`: JSON applied over the
    /// constructed [`SearchOptions`] (see [`apply_search_options_override`]).
    ///
    /// The HTTP handler only copies `?zoekt-search-opts=` into this field for
    /// authenticated requests (`cmd/frontend/internal/search/search.go:144`).
    pub zoekt_search_options_override: String,
}

/// `search.ZoektParameters` (`internal/search/types.go:161-168`).
///
/// `ToSearchOptions` in the baseline reads the context (`policy.ShouldTrace`)
/// and `conf`; those values are injected as fields here instead of a config
/// dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct ZoektParameters {
    pub query: Query,
    pub typ: IndexedRequestType,
    pub file_match_limit: i32,
    pub select: SelectPath,
    pub num_context_lines: i32,
    pub features: Features,
    pub pattern_type: SearchType,
    /// `policy.ShouldTrace(ctx)`.
    pub trace: bool,
    /// `conf.SearchFlushWallTime(use_bm25)` when an experimental override is
    /// set; `None` resolves to the 2s/500ms default.
    pub flush_wall_time_override: Option<Duration>,
    /// `conf.CodeIntelRankingDocumentReferenceCountsEnabled()`.
    pub use_document_ranks: bool,
    /// `conf.SearchDocumentRanksWeight()`.
    pub document_ranks_weight: f64,
}

impl Default for ZoektParameters {
    fn default() -> Self {
        Self {
            query: Query::Const(true),
            typ: IndexedRequestType::Text,
            file_match_limit: 0,
            select: SelectPath::default(),
            num_context_lines: 0,
            features: Features::default(),
            pattern_type: SearchType::Standard,
            trace: false,
            flush_wall_time_override: None,
            use_document_ranks: false,
            document_ranks_weight: DEFAULT_DOCUMENT_RANKS_WEIGHT,
        }
    }
}

impl ZoektParameters {
    /// `search.ZoektParameters.ToSearchOptions` (`internal/search/types.go:170-235`).
    ///
    /// The baseline builds the full options and then applies
    /// `Features.ZoektSearchOptionsOverride` from a deferred closure, so the
    /// override also applies to the `select:repo` early return and rolls back
    /// entirely on a JSON/type error.
    pub fn to_search_options(&self) -> SearchOptions {
        let mut options = self.base_search_options();
        if !self.features.zoekt_search_options_override.is_empty() {
            apply_search_options_override(
                &mut options,
                &self.features.zoekt_search_options_override,
            );
        }
        options
    }

    /// The body of `ToSearchOptions`, before the override `defer`.
    fn base_search_options(&self) -> SearchOptions {
        let use_bm25 =
            self.pattern_type == SearchType::CodyContext && self.typ == IndexedRequestType::Text;

        let mut options = SearchOptions {
            estimate_doc_count: false,
            whole: false,
            shard_max_match_count: DEFAULT_SHARD_MAX_MATCH_COUNT,
            total_max_match_count: DEFAULT_TOTAL_MAX_MATCH_COUNT,
            shard_repo_max_match_count: 0,
            max_wall_time: DEFAULT_MAX_WALL_TIME,
            flush_wall_time: Duration::ZERO,
            max_doc_display_count: 0,
            max_match_display_count: 0,
            num_context_lines: i64::from(self.num_context_lines),
            chunk_matches: true,
            use_document_ranks: false,
            // The baseline leaves this at the Go zero value until after the
            // `select:repo` early return; only then is the configured weight
            // applied.
            document_ranks_weight: 0.0,
            trace: self.trace,
            debug_score: false,
            use_bm25_scoring: use_bm25,
        };

        // Keyword and BM25 searches match more broadly, so consider more
        // candidates per shard/replica.
        if use_bm25 || self.pattern_type == SearchType::Keyword {
            options.shard_max_match_count *= 10;
            options.total_max_match_count *= 10;
        }

        let limit = i64::from(self.file_match_limit);
        options.max_doc_display_count = limit;
        if limit > options.shard_max_match_count {
            options.shard_max_match_count = limit;
        }
        if limit > options.total_max_match_count {
            options.total_max_match_count = limit;
        }

        // Repo searches only check one file per repository.
        if self.select.is_repository() {
            options.shard_repo_max_match_count = 1;
            return options;
        }

        if self.features.debug {
            options.debug_score = true;
        }

        options.flush_wall_time = self.flush_wall_time_override.unwrap_or_else(|| {
            if use_bm25 {
                Duration::from_secs(2)
            } else {
                DEFAULT_FLUSH_WALL_TIME
            }
        });
        options.use_document_ranks = self.use_document_ranks;
        options.document_ranks_weight = self.document_ranks_weight;

        options
    }
}

/// `ToSearchOptions`' deferred `json.Unmarshal` override.
///
/// Go unmarshals into the already-built `*zoekt.SearchOptions`, so only keys
/// present in the JSON change; unknown keys are ignored, `null` is a no-op,
/// and **any** error (syntax, top-level non-object, type mismatch, integer
/// overflow) restores the pre-override value wholesale. The candidate is kept
/// separate so Rust reproduces that all-or-nothing rollback.
fn apply_search_options_override(options: &mut SearchOptions, raw: &str) {
    let Ok(serde_json::Value::Object(fields)) = serde_json::from_str::<serde_json::Value>(raw)
    else {
        // Mirrors `json.Unmarshal` erroring for malformed JSON or a non-object
        // top level; `null`/unknown values below are no-ops.
        return;
    };

    let mut candidate = options.clone();
    for (key, value) in fields {
        if !apply_search_option_field(&mut candidate, &key, &value) {
            return;
        }
    }
    *options = candidate;
}

/// Applies one `zoekt.SearchOptions` field. Returns `false` when Go's
/// `encoding/json` would report a type error (which rolls back the override).
///
/// `Go` matches field names case-insensitively when there is no exact match;
/// `SpanContext` is the only Go field this port does not model, and unknown
/// keys are ignored.
fn apply_search_option_field(
    options: &mut SearchOptions,
    key: &str,
    value: &serde_json::Value,
) -> bool {
    // `null` has no effect on Go's non-pointer struct fields.
    if value.is_null() {
        return true;
    }
    match key.to_ascii_lowercase().as_str() {
        "estimatedoccount" => set_bool(&mut options.estimate_doc_count, value),
        "whole" => set_bool(&mut options.whole, value),
        "shardmaxmatchcount" => set_i64(&mut options.shard_max_match_count, value),
        "totalmaxmatchcount" => set_i64(&mut options.total_max_match_count, value),
        "shardrepomaxmatchcount" => set_i64(&mut options.shard_repo_max_match_count, value),
        "maxwalltime" => set_duration(&mut options.max_wall_time, value),
        "flushwalltime" => set_duration(&mut options.flush_wall_time, value),
        "maxdocdisplaycount" => set_i64(&mut options.max_doc_display_count, value),
        "maxmatchdisplaycount" => set_i64(&mut options.max_match_display_count, value),
        "numcontextlines" => set_i64(&mut options.num_context_lines, value),
        "chunkmatches" => set_bool(&mut options.chunk_matches, value),
        "usedocumentranks" => set_bool(&mut options.use_document_ranks, value),
        "documentranksweight" => set_f64(&mut options.document_ranks_weight, value),
        "usebm25scoring" => set_bool(&mut options.use_bm25_scoring, value),
        "trace" => set_bool(&mut options.trace, value),
        "debugscore" => set_bool(&mut options.debug_score, value),
        _ => true,
    }
}

fn set_bool(slot: &mut bool, value: &serde_json::Value) -> bool {
    match value.as_bool() {
        Some(value) => {
            *slot = value;
            true
        }
        None => false,
    }
}

fn set_i64(slot: &mut i64, value: &serde_json::Value) -> bool {
    match value.as_i64() {
        Some(value) => {
            *slot = value;
            true
        }
        // A fractional or exponent number is not accepted for Go integer
        // fields (`strconv.ParseInt` on the literal).
        None => false,
    }
}

/// `time.Duration` is `int64` nanoseconds on the wire. Rust's [`Duration`] is
/// unsigned; a negative override value is not representable and is treated as
/// a rollback (documented errata).
fn set_duration(slot: &mut Duration, value: &serde_json::Value) -> bool {
    match value.as_i64() {
        Some(nanos) if nanos >= 0 => {
            *slot = Duration::from_nanos(nanos as u64);
            true
        }
        _ => false,
    }
}

fn set_f64(slot: &mut f64, value: &serde_json::Value) -> bool {
    match value.as_f64() {
        Some(value) => {
            *slot = value;
            true
        }
        None => false,
    }
}

/// The baseline `zoekt.Streamer`: a blocking search transport whose sender
/// receives full [`SearchResult`] values.
///
/// This is deliberately distinct from [`sg_zoekt::streamer::Streamer`], whose
/// sender only sees [`sg_zoekt::types::SearchChunk`] values.
pub trait Streamer {
    type Error: std::error::Error + Clone + Send + Sync + 'static;

    /// `zoekt.Streamer.StreamSearch`.
    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), Self::Error>;
}

/// The blocking adapter from
/// [`sg_zoekt::streamer::BlockingStreamer`] drives an async
/// `sg_zoekt::streamer::Streamer` to completion and can therefore be consumed
/// through this module's synchronous [`Streamer`] directly (M9.8ig). Event
/// encoding and job decisions are unchanged.
impl<S> Streamer for sg_zoekt::streamer::BlockingStreamer<S>
where
    S: sg_zoekt::streamer::Streamer,
{
    type Error = S::Error;

    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), Self::Error> {
        self.stream_search_blocking(query, options, sender)
    }
}

/// Lossless wire→result adapter (`zoekt.SearchResultFromProto`, see
/// `internal/search/backend/grpc.go:44-53`): converts a wire
/// [`SearchChunk`] into the full [`SearchResult`] this module's [`Streamer`]
/// yields, preserving `files`, `stats` and `progress`. The `repo_urls` /
/// `line_fragments` maps are empty because Sourcegraph passes `nil` for both.
pub fn wire_chunk_to_search_result(chunk: SearchChunk) -> SearchResult {
    SearchResult::from(chunk)
}

/// Errors returned by [`zoekt_search`] / [`do_zoekt_search_global`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoektSearchError<E> {
    /// The injected deadline had already elapsed; the baseline returns
    /// `ctx.Err()` (`context.DeadlineExceeded`).
    DeadlineExceeded,
    /// The transport error returned by the streamer.
    Streamer(E),
}

impl<E: fmt::Display> fmt::Display for ZoektSearchError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => f.write_str("context deadline exceeded"),
            Self::Streamer(err) => write!(f, "{err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ZoektSearchError<E> {}

/// `sendMatches` (`indexed_search.go:358-437`).
///
/// Sends exactly one event for a non-empty `event.files` (all matches at
/// once), or one stats-only event when there are no files.
pub fn send_matches(
    event: &SearchResult,
    path_regexps: &[regex::Regex],
    get_repo_input_rev: impl Fn(&FileMatch) -> (convert::MinimalRepo, Vec<String>),
    typ: IndexedRequestType,
    selector: &SelectPath,
    sender: &mut impl FnMut(SearchEvent),
) {
    // In the case of zoekt, non-zero `Crashes` in practice means a backend
    // was missing.
    let mut stats = Stats {
        backends_missing: event.stats.crashes,
        is_limit_hit: event.stats.files_skipped + event.stats.shards_skipped > 0,
        ..Stats::default()
    };

    let select_repos = selector.is_repository();
    if select_repos {
        // Zoekt cannot report ReposSkipped, so repo-result limits are decided
        // elsewhere (cancellation / the `count:` cap).
        stats.is_limit_hit = false;
    }

    if event.files.is_empty() {
        sender(SearchEvent {
            results: Vec::new(),
            stats,
        });
        return;
    }

    let mut matches = Vec::with_capacity(event.files.len());
    for file in &event.files {
        let (repo, input_revs) = get_repo_input_rev(file);

        if select_repos {
            matches.push(convert::Match::Repo(convert::RepoMatch {
                name: repo.name,
                id: repo.id,
                rev: String::new(),
                description_matches: Vec::new(),
                repo_name_matches: Vec::new(),
            }));
            continue;
        }

        let chunk_matches = if typ != IndexedRequestType::Symbol {
            zoekt_results::zoekt_file_match_to_multiline_matches(file)
        } else {
            Vec::new()
        };
        let path_matches = zoekt_results::zoekt_file_match_to_path_match_ranges(file, path_regexps);

        // One `FileMatch` per input revision (fan-out).
        for input_rev in input_revs {
            let symbols = if typ == IndexedRequestType::Symbol {
                zoekt_results::zoekt_file_match_to_symbol_results(&repo, &input_rev, file)
            } else {
                Vec::new()
            };
            let file_ref = convert::FileRef {
                repo: repo.clone(),
                commit_id: file.version.clone(),
                path: String::from_utf8_lossy(&file.file_name).into_owned(),
                input_rev: Some(input_rev),
                precise_language: file.language.clone(),
            };
            let debug = if file.debug.is_empty() {
                None
            } else {
                Some(file.debug.clone())
            };
            matches.push(convert::Match::File(convert::FileMatch {
                file: file_ref,
                chunk_matches: chunk_matches.clone(),
                symbols,
                path_matches: path_matches.clone(),
                limit_hit: false,
                debug,
            }));
        }
    }

    sender(SearchEvent {
        results: matches,
        stats,
    });
}

/// Apply the injected context deadline to `options`, matching the baseline:
/// `MaxWallTime = time.Until(deadline)`, and an error when it is already past.
fn apply_deadline<E>(
    options: &mut SearchOptions,
    deadline: Option<Instant>,
) -> Result<(), ZoektSearchError<E>> {
    let Some(deadline) = deadline else {
        return Ok(());
    };
    match deadline.checked_duration_since(Instant::now()) {
        Some(remaining) => {
            options.max_wall_time = remaining;
            Ok(())
        }
        // `time.Until(deadline) < 0` -> `ctx.Err()`.
        None => Err(ZoektSearchError::DeadlineExceeded),
    }
}

/// `zoektSearch` (`indexed_search.go:314-356`).
///
/// Sends the converted zoekt events and, when the stream finishes without
/// results after `since(t0) >= MaxWallTime`, a `RepoStatusTimedOut` stats-only
/// event for every searched repository.
#[allow(clippy::too_many_arguments)]
pub fn zoekt_search<E>(
    repos: &IndexedRepoRevs,
    q: Query,
    path_regexps: &[regex::Regex],
    typ: IndexedRequestType,
    client: &impl Streamer<Error = E>,
    params: &ZoektParameters,
    deadline: Option<Instant>,
    since: impl Fn(Instant) -> Duration,
    sender: &mut impl FnMut(SearchEvent),
) -> Result<(), ZoektSearchError<E>>
where
    E: std::error::Error + Clone + Send + Sync + 'static,
{
    if repos.repo_revs.is_empty() {
        return Ok(());
    }

    let branches: Vec<BranchRepos> = repos
        .branch_repos()
        .into_iter()
        .map(|branch_repos| {
            let mut bytes = Vec::new();
            branch_repos
                .repos
                .serialize_into(&mut bytes)
                .expect("serializing a roaring bitmap into a Vec cannot fail");
            BranchRepos {
                branch: branch_repos.branch,
                repos: bytes,
            }
        })
        .collect();
    let final_query = Query::and(vec![Query::BranchesRepos(branches), q]);

    let mut search_opts = params.to_search_options();
    let t0 = Instant::now();
    apply_deadline(&mut search_opts, deadline)?;

    let found_results = AtomicBool::new(false);
    {
        let mut callback = |event: SearchResult| {
            // `foundResults.CompareAndSwap(false, FileCount != 0 || MatchCount != 0)`.
            let has_results = event.stats.file_count != 0 || event.stats.match_count != 0;
            found_results.fetch_or(has_results, Ordering::Relaxed);

            let get_repo_input_rev = |file: &FileMatch| {
                let (repo, revs) = repos.get_repo_input_rev(file);
                (
                    convert::MinimalRepo {
                        id: repo.id as i32,
                        name: repo.name,
                    },
                    revs,
                )
            };
            send_matches(
                &event,
                path_regexps,
                get_repo_input_rev,
                typ,
                &params.select,
                &mut *sender,
            );
        };
        client
            .stream_search(&final_query, &search_opts, &mut callback)
            .map_err(ZoektSearchError::Streamer)?;
    }

    if !found_results.load(Ordering::Relaxed) && since(t0) >= search_opts.max_wall_time {
        let mut status = progress::RepoStatusMap::default();
        for repo_rev in repos.repo_revs.values() {
            status.update(repo_rev.repo.id as i32, repo_status::TIMED_OUT);
        }
        sender(SearchEvent {
            results: Vec::new(),
            stats: Stats {
                status,
                ..Stats::default()
            },
        });
    }

    Ok(())
}

/// `DoZoektSearchGlobal` (`indexed_search.go:273-312`).
///
/// Repository and input revision are fixed to
/// `(file.repository_id/file.repository, [""])`, as in the baseline.
pub fn do_zoekt_search_global<E>(
    client: &impl Streamer<Error = E>,
    params: &ZoektParameters,
    path_regexps: &[regex::Regex],
    deadline: Option<Instant>,
    sender: &mut impl FnMut(SearchEvent),
) -> Result<(), ZoektSearchError<E>>
where
    E: std::error::Error + Clone + Send + Sync + 'static,
{
    let mut search_opts = params.to_search_options();
    apply_deadline(&mut search_opts, deadline)?;

    let mut callback = |event: SearchResult| {
        let get_repo_input_rev = |file: &FileMatch| {
            (
                convert::MinimalRepo {
                    id: file.repository_id as i32,
                    name: file.repository.clone(),
                },
                vec![String::new()],
            )
        };
        send_matches(
            &event,
            path_regexps,
            get_repo_input_rev,
            params.typ,
            &params.select,
            &mut *sender,
        );
    };

    client
        .stream_search(&params.query, &search_opts, &mut callback)
        .map_err(ZoektSearchError::Streamer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_zoekt::indexed_repo_revs::{RepositoryRevisions, RepositoryRevisionsRepo};
    use sg_zoekt::types::{
        ChunkMatch, Location, MinimalRepoListEntry, Range as ZoektRange, RepositoryBranch,
    };

    fn file(id: u32, name: &str, version: &str, branches: &[&str]) -> FileMatch {
        FileMatch {
            repository_id: id,
            repository: name.to_owned(),
            version: version.to_owned(),
            branches: branches.iter().map(|b| (*b).to_owned()).collect(),
            ..FileMatch::default()
        }
    }

    fn empty_event(files: Vec<FileMatch>, crashes: i64, skipped: i64) -> SearchResult {
        SearchResult {
            files,
            stats: sg_zoekt::types::Stats {
                crashes,
                files_skipped: skipped,
                ..Default::default()
            },
            ..SearchResult::default()
        }
    }

    #[test]
    fn empty_files_still_sends_stats_only_event() {
        let mut events = Vec::new();
        send_matches(
            &empty_event(Vec::new(), 2, 1),
            &[],
            |_| (convert::MinimalRepo::default(), vec![String::new()]),
            IndexedRequestType::Text,
            &SelectPath::default(),
            &mut |event| events.push(event),
        );
        assert_eq!(events.len(), 1);
        assert!(events[0].results.is_empty());
        assert_eq!(events[0].stats.backends_missing, 2);
        assert!(events[0].stats.is_limit_hit);
    }

    #[test]
    fn repo_select_clears_limit_hit_and_emits_repo_match() {
        let mut events = Vec::new();
        send_matches(
            &empty_event(vec![file(7, "r", "v", &[])], 0, 5),
            &[],
            |_| {
                (
                    convert::MinimalRepo {
                        id: 7,
                        name: "r".to_owned(),
                    },
                    vec![String::new()],
                )
            },
            IndexedRequestType::Text,
            &SelectPath(vec!["repo".to_owned()]),
            &mut |event| events.push(event),
        );
        assert_eq!(events.len(), 1);
        assert!(!events[0].stats.is_limit_hit);
        match &events[0].results[0] {
            convert::Match::Repo(repo) => {
                assert_eq!(repo.id, 7);
                assert_eq!(repo.name, "r");
            }
            other => panic!("expected repo match, got {other:?}"),
        }
    }

    #[test]
    fn text_match_fans_out_per_input_rev_and_sets_debug() {
        let mut zoekt_file = file(1, "r", "commit", &["main"]);
        zoekt_file.file_name = b"src/a.go".to_vec();
        zoekt_file.language = "Go".to_owned();
        zoekt_file.debug = "rank=1".to_owned();
        zoekt_file.chunk_matches = vec![ChunkMatch {
            content: b"needle\n".to_vec(),
            content_start: Location {
                byte_offset: 0,
                line_number: 1,
                column: 1,
            },
            ranges: vec![ZoektRange {
                start: Location {
                    byte_offset: 0,
                    line_number: 1,
                    column: 1,
                },
                end: Location {
                    byte_offset: 6,
                    line_number: 1,
                    column: 7,
                },
            }],
            ..ChunkMatch::default()
        }];

        let mut events = Vec::new();
        send_matches(
            &empty_event(vec![zoekt_file], 0, 0),
            &[],
            |_| {
                (
                    convert::MinimalRepo {
                        id: 1,
                        name: "r".to_owned(),
                    },
                    vec!["main".to_owned(), "dev".to_owned()],
                )
            },
            IndexedRequestType::Text,
            &SelectPath(vec!["content".to_owned()]),
            &mut |event| events.push(event),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].results.len(), 2);
        for (index, input_rev) in ["main", "dev"].iter().enumerate() {
            match &events[0].results[index] {
                convert::Match::File(file_match) => {
                    assert_eq!(file_match.file.input_rev.as_deref(), Some(*input_rev));
                    assert_eq!(file_match.chunk_matches.len(), 1);
                    assert_eq!(file_match.debug.as_deref(), Some("rank=1"));
                    assert!(file_match.symbols.is_empty());
                }
                other => panic!("expected file match, got {other:?}"),
            }
        }
    }

    #[test]
    fn wire_chunk_adapter_preserves_stats_and_progress() {
        let chunk = SearchChunk {
            files: vec![file(3, "repo", "v", &["HEAD"])],
            crashes: 2,
            stats: sg_zoekt::types::Stats {
                crashes: 2,
                file_count: 1,
                match_count: 4,
                ..Default::default()
            },
            progress: sg_zoekt::types::Progress {
                priority: 1.5,
                max_pending_priority: 8.0,
            },
        };
        let result = wire_chunk_to_search_result(chunk.clone());
        assert_eq!(result.files, chunk.files);
        assert_eq!(result.stats, chunk.stats);
        assert_eq!(result.progress, chunk.progress);
        assert!(result.repo_urls.is_empty());
        assert!(result.line_fragments.is_empty());
    }

    #[test]
    fn to_search_options_matches_baseline_defaults() {
        let params = ZoektParameters::default();
        let options = params.to_search_options();
        assert_eq!(options.max_wall_time, Duration::from_secs(20));
        assert_eq!(options.shard_max_match_count, 10_000);
        assert_eq!(options.total_max_match_count, 100_000);
        assert_eq!(options.flush_wall_time, Duration::from_millis(500));
        assert!(options.chunk_matches);

        let repo_params = ZoektParameters {
            select: SelectPath(vec!["repo".to_owned()]),
            ..ZoektParameters::default()
        };
        let repo_options = repo_params.to_search_options();
        assert_eq!(repo_options.shard_repo_max_match_count, 1);
        assert_eq!(repo_options.flush_wall_time, Duration::ZERO);
    }

    fn with_override(raw: &str) -> ZoektParameters {
        ZoektParameters {
            features: Features {
                zoekt_search_options_override: raw.to_owned(),
                ..Features::default()
            },
            ..ZoektParameters::default()
        }
    }

    #[test]
    fn to_search_options_applies_override_fields() {
        let options = with_override(
            r#"{"Trace":true,"DebugScore":true,"MaxWallTime":1500000000,
                "FlushWallTime":250000000,"ShardMaxMatchCount":7,
                "DocumentRanksWeight":0.25,"UseDocumentRanks":true}"#,
        )
        .to_search_options();
        assert!(options.trace);
        assert!(options.debug_score);
        assert_eq!(options.max_wall_time, Duration::from_millis(1500));
        assert_eq!(options.flush_wall_time, Duration::from_millis(250));
        assert_eq!(options.shard_max_match_count, 7);
        assert_eq!(options.document_ranks_weight, 0.25);
        assert!(options.use_document_ranks);
        // Untouched fields keep the constructed value.
        assert_eq!(options.total_max_match_count, 100_000);
    }

    #[test]
    fn to_search_options_override_matches_field_names_case_insensitively() {
        let options = with_override(r#"{"debugscore":true,"CHUNKMATCHES":false}"#).to_search_options();
        assert!(options.debug_score);
        assert!(!options.chunk_matches);
    }

    #[test]
    fn to_search_options_override_unknown_keys_and_null_are_no_ops() {
        let options = with_override(r#"{"Bogus":1,"Trace":null}"#).to_search_options();
        assert!(!options.trace);
        assert_eq!(options.shard_max_match_count, 10_000);
    }

    #[test]
    fn to_search_options_override_rolls_back_on_type_error() {
        // `ShardMaxMatchCount` is valid but `Trace` is not: Go rolls back both.
        let options = with_override(r#"{"ShardMaxMatchCount":7,"Trace":"yes"}"#)
            .to_search_options();
        assert_eq!(options.shard_max_match_count, 10_000);
        assert!(!options.trace);
    }

    #[test]
    fn to_search_options_override_rolls_back_on_bad_json() {
        let options = with_override("{not json").to_search_options();
        assert_eq!(options.shard_max_match_count, 10_000);
        assert!(!options.trace);
    }

    #[test]
    fn to_search_options_override_applies_after_select_repo_early_return() {
        let params = ZoektParameters {
            select: SelectPath(vec!["repo".to_owned()]),
            features: Features {
                zoekt_search_options_override: r#"{"ShardRepoMaxMatchCount":9,"DebugScore":true}"#
                    .to_owned(),
                ..Features::default()
            },
            ..ZoektParameters::default()
        };
        let options = params.to_search_options();
        assert_eq!(options.shard_repo_max_match_count, 9);
        assert!(options.debug_score);
    }

    #[test]
    fn to_search_options_override_rejects_fractional_integers() {
        let options = with_override(r#"{"ShardMaxMatchCount":1.5}"#).to_search_options();
        assert_eq!(options.shard_max_match_count, 10_000);
    }

    #[derive(Debug, Clone)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test error")
        }
    }

    impl std::error::Error for TestError {}

    #[derive(Clone)]
    struct FakeAsyncStreamer {
        chunks: Vec<SearchChunk>,
    }

    impl sg_zoekt::streamer::Streamer for FakeAsyncStreamer {
        type Error = TestError;

        async fn list(
            &self,
            _query: &Query,
            _opts: Option<sg_zoekt::types::ListOptions>,
        ) -> Result<sg_zoekt::types::ListResponse, TestError> {
            Ok(sg_zoekt::types::ListResponse::default())
        }

        async fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            sender: &mut (dyn FnMut(SearchChunk) + Send),
        ) -> Result<(), TestError> {
            for chunk in &self.chunks {
                sender(chunk.clone());
            }
            Ok(())
        }
    }

    #[test]
    fn blocking_streamer_satisfies_the_synchronous_streamer_trait() {
        let streamer = sg_zoekt::streamer::BlockingStreamer::new(FakeAsyncStreamer {
            chunks: vec![
                SearchChunk {
                    stats: sg_zoekt::types::Stats {
                        file_count: 1,
                        ..Default::default()
                    },
                    ..SearchChunk::default()
                },
                SearchChunk {
                    stats: sg_zoekt::types::Stats {
                        match_count: 2,
                        ..Default::default()
                    },
                    ..SearchChunk::default()
                },
            ],
        });
        let mut results = Vec::new();
        Streamer::stream_search(
            &streamer,
            &Query::Const(true),
            &SearchOptions::default(),
            &mut |result| results.push(result),
        )
        .expect("fake stream succeeds");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].stats.file_count, 1);
        assert_eq!(results[1].stats.match_count, 2);
    }

    #[test]
    fn zoekt_search_skips_empty_repo_set_and_emits_timeout() {
        struct Fake {
            calls: std::cell::Cell<usize>,
        }
        impl Streamer for Fake {
            type Error = TestError;
            fn stream_search(
                &self,
                _query: &Query,
                _options: &SearchOptions,
                _sender: &mut dyn FnMut(SearchResult),
            ) -> Result<(), Self::Error> {
                self.calls.set(self.calls.get() + 1);
                Ok(())
            }
        }

        let fake = Fake {
            calls: std::cell::Cell::new(0),
        };
        let mut events = Vec::new();
        zoekt_search(
            &IndexedRepoRevs::default(),
            Query::Const(true),
            &[],
            IndexedRequestType::Text,
            &fake,
            &ZoektParameters::default(),
            None,
            |_| Duration::ZERO,
            &mut |event| events.push(event),
        )
        .expect("empty repo set is a no-op");
        assert_eq!(fake.calls.get(), 0);
        assert!(events.is_empty());

        let mut indexed = IndexedRepoRevs::default();
        indexed.add(
            &RepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: 1,
                    name: "r".to_owned(),
                },
                revs: vec!["main".to_owned()],
            },
            MinimalRepoListEntry {
                branches: vec![RepositoryBranch {
                    name: "main".to_owned(),
                    version: "s".to_owned(),
                }],
                ..MinimalRepoListEntry::default()
            },
        );
        events.clear();
        zoekt_search(
            &indexed,
            Query::Const(true),
            &[],
            IndexedRequestType::Text,
            &fake,
            &ZoektParameters::default(),
            None,
            |_| Duration::from_secs(20),
            &mut |event| events.push(event),
        )
        .expect("search succeeds");
        assert_eq!(fake.calls.get(), 1);
        assert_eq!(events.len(), 1);
        assert!(events[0].results.is_empty());
        assert_eq!(
            events[0].stats.status.filter(repo_status::TIMED_OUT),
            vec![1]
        );
    }
}
