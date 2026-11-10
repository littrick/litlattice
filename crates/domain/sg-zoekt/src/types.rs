#![forbid(unsafe_code)]

//! Request/response types for the zoekt-webserver API and their proto
//! conversions.
//!
//! These mirror `zoekt.SearchOptions`, `zoekt.ListOptions`,
//! `zoekt.SearchResult`, `zoekt.FileMatch`, `zoekt.ChunkMatch`,
//! `zoekt.ListResponse` and friends. The defaults in [`SearchOptions::hybrid`]
//! reproduce `search.ZoektParameters.ToSearchOptions` for the searcher's
//! hybrid path (`cmd/searcher/internal/search/hybrid.go`).

use std::collections::BTreeMap;
use std::time::Duration;

use prost_types::Duration as ProtoDuration;

use crate::proto;

/// `search.ZoektParameters.ToSearchOptions` default wall-clock budget.
pub const DEFAULT_MAX_WALL_TIME: Duration = Duration::from_secs(20);
/// `conf.SearchFlushWallTime(false)` default.
pub const DEFAULT_FLUSH_WALL_TIME: Duration = Duration::from_millis(500);
/// Per-shard match budget.
pub const DEFAULT_SHARD_MAX_MATCH_COUNT: i64 = 10_000;
/// Per-replica match budget.
pub const DEFAULT_TOTAL_MAX_MATCH_COUNT: i64 = 100_000;
/// `conf.SearchDocumentRanksWeight` default.
pub const DEFAULT_DOCUMENT_RANKS_WEIGHT: f64 = 4500.0;

/// `zoekt.SearchOptions`.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchOptions {
    pub estimate_doc_count: bool,
    pub whole: bool,
    pub shard_max_match_count: i64,
    pub total_max_match_count: i64,
    pub shard_repo_max_match_count: i64,
    pub max_wall_time: Duration,
    pub flush_wall_time: Duration,
    pub max_doc_display_count: i64,
    pub max_match_display_count: i64,
    pub num_context_lines: i64,
    pub chunk_matches: bool,
    pub use_document_ranks: bool,
    pub document_ranks_weight: f64,
    pub trace: bool,
    pub debug_score: bool,
    pub use_bm25_scoring: bool,
}

impl SearchOptions {
    /// The options hybrid search uses: `ToSearchOptions(ctx)` for a text
    /// request with no feature overrides, followed by `ChunkMatches = true`.
    ///
    /// `FileMatchLimit` is `limit`; the baseline raises the per-shard and
    /// per-replica budgets when the limit is larger than their defaults.
    pub fn hybrid(limit: i32, num_context_lines: i32) -> Self {
        let limit = i64::from(limit);
        let mut shard_max = DEFAULT_SHARD_MAX_MATCH_COUNT;
        let mut total_max = DEFAULT_TOTAL_MAX_MATCH_COUNT;
        if limit > shard_max {
            shard_max = limit;
        }
        if limit > total_max {
            total_max = limit;
        }
        Self {
            estimate_doc_count: false,
            whole: false,
            shard_max_match_count: shard_max,
            total_max_match_count: total_max,
            shard_repo_max_match_count: 0,
            max_wall_time: DEFAULT_MAX_WALL_TIME,
            flush_wall_time: DEFAULT_FLUSH_WALL_TIME,
            max_doc_display_count: limit,
            max_match_display_count: 0,
            num_context_lines: i64::from(num_context_lines),
            chunk_matches: true,
            use_document_ranks: false,
            document_ranks_weight: DEFAULT_DOCUMENT_RANKS_WEIGHT,
            trace: false,
            debug_score: false,
            use_bm25_scoring: false,
        }
    }

    /// `SearchOptions.ToProto` (`query.SearchOptionsToProto`).
    pub fn to_proto(&self) -> proto::SearchOptions {
        proto::SearchOptions {
            estimate_doc_count: self.estimate_doc_count,
            whole: self.whole,
            shard_max_match_count: self.shard_max_match_count,
            total_max_match_count: self.total_max_match_count,
            shard_repo_max_match_count: self.shard_repo_max_match_count,
            max_wall_time: Some(duration_to_proto(self.max_wall_time)),
            flush_wall_time: Some(duration_to_proto(self.flush_wall_time)),
            max_doc_display_count: self.max_doc_display_count,
            max_match_display_count: self.max_match_display_count,
            num_context_lines: self.num_context_lines,
            chunk_matches: self.chunk_matches,
            use_document_ranks: self.use_document_ranks,
            document_ranks_weight: self.document_ranks_weight,
            trace: self.trace,
            debug_score: self.debug_score,
            use_bm25_scoring: self.use_bm25_scoring,
        }
    }
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self::hybrid(0, 0)
    }
}

/// `zoekt.RepoListField`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RepoListField {
    #[default]
    Unknown,
    Repos,
    ReposMap,
}

impl RepoListField {
    fn to_proto(self) -> i32 {
        match self {
            Self::Unknown => proto::list_options::RepoListField::UnknownUnspecified as i32,
            Self::Repos => proto::list_options::RepoListField::Repos as i32,
            Self::ReposMap => proto::list_options::RepoListField::ReposMap as i32,
        }
    }
}

/// `zoekt.ListOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListOptions {
    pub field: RepoListField,
}

impl ListOptions {
    pub fn to_proto(self) -> proto::ListOptions {
        proto::ListOptions {
            field: self.field.to_proto(),
        }
    }
}

/// `zoekt.RepositoryBranch`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepositoryBranch {
    pub name: String,
    pub version: String,
}

/// `zoekt.MinimalRepoListEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MinimalRepoListEntry {
    pub has_symbols: bool,
    pub branches: Vec<RepositoryBranch>,
    pub index_time_unix: i64,
}

/// `zoekt.RepoListEntry` (only the fields the searcher path uses).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoListEntry {
    pub repository: Repository,
    pub index_time_unix: i64,
}

/// `zoekt.Repository`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Repository {
    pub id: u32,
    pub name: String,
    pub branches: Vec<RepositoryBranch>,
    pub has_symbols: bool,
    pub tombstone: bool,
}

/// `zoekt.ListResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListResponse {
    pub repos: Vec<RepoListEntry>,
    pub repos_map: BTreeMap<u32, MinimalRepoListEntry>,
    pub crashes: i64,
}

impl ListResponse {
    /// `ListResponse.FromProto`.
    pub fn from_proto(response: proto::ListResponse) -> Self {
        Self {
            repos: response
                .repos
                .into_iter()
                .map(|entry| {
                    let repository = entry.repository.unwrap_or_default();
                    RepoListEntry {
                        index_time_unix: entry
                            .index_metadata
                            .as_ref()
                            .and_then(|metadata| metadata.index_time.as_ref())
                            .map(|timestamp| timestamp.seconds)
                            .unwrap_or_default(),
                        repository: Repository {
                            id: repository.id,
                            name: repository.name,
                            branches: repository
                                .branches
                                .into_iter()
                                .map(|branch| RepositoryBranch {
                                    name: branch.name,
                                    version: branch.version,
                                })
                                .collect(),
                            has_symbols: repository.has_symbols,
                            tombstone: repository.tombstone,
                        },
                    }
                })
                .collect(),
            repos_map: response
                .repos_map
                .into_iter()
                .map(|(repo_id, entry)| {
                    (
                        repo_id,
                        MinimalRepoListEntry {
                            has_symbols: entry.has_symbols,
                            branches: entry
                                .branches
                                .into_iter()
                                .map(|branch| RepositoryBranch {
                                    name: branch.name,
                                    version: branch.version,
                                })
                                .collect(),
                            index_time_unix: entry.index_time_unix,
                        },
                    )
                })
                .collect(),
            crashes: response.crashes,
        }
    }
}

/// `zoekt.Location`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Location {
    pub byte_offset: u32,
    /// 1-based line number, as zoekt reports it.
    pub line_number: u32,
    /// 1-based column in runes, as zoekt reports it.
    pub column: u32,
}

impl Location {
    fn from_proto(location: Option<proto::Location>) -> Self {
        let location = location.unwrap_or_default();
        Self {
            byte_offset: location.byte_offset,
            line_number: location.line_number,
            column: location.column,
        }
    }
}

/// `zoekt.Range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    pub start: Location,
    pub end: Location,
}

/// `zoekt.ChunkMatch`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChunkMatch {
    pub content: Vec<u8>,
    pub content_start: Location,
    pub file_name: bool,
    pub ranges: Vec<Range>,
    pub score: f64,
}

impl ChunkMatch {
    fn from_proto(chunk: proto::ChunkMatch) -> Self {
        Self {
            content: chunk.content,
            content_start: Location::from_proto(chunk.content_start),
            file_name: chunk.file_name,
            ranges: chunk
                .ranges
                .into_iter()
                .map(|range| Range {
                    start: Location::from_proto(range.start),
                    end: Location::from_proto(range.end),
                })
                .collect(),
            score: chunk.score,
        }
    }

    /// `(*zoekt.ChunkMatch).sizeBytes`.
    ///
    /// `SymbolInfo` (empty) and `DebugScore` (empty) are not carried by the
    /// port; the baseline accounts for their slice header (`24`) and string
    /// header (`16`) regardless, so they are added as empty values.
    pub fn size_bytes(&self) -> u64 {
        // Content.
        let mut sz = 24 + self.content.len() as u64;
        // ContentStart.
        sz += 12;
        // FileName.
        sz += 1;
        // Ranges: slice header + 24 per range.
        sz += 24 + self.ranges.len() as u64 * 24;
        // SymbolInfo (empty).
        sz += 24;
        // Score.
        sz += 8;
        // DebugScore (empty).
        sz += 16;
        sz
    }
}

/// `zoekt.FileMatch`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FileMatch {
    pub score: f64,
    /// `zoekt.FileMatch.Debug` (proto `debug`): the `DebugScore` string. Kept
    /// so callers can verify that ranking and aggregation never rewrite it.
    pub debug: String,
    pub file_name: Vec<u8>,
    pub repository: String,
    pub repository_id: u32,
    pub branches: Vec<String>,
    pub chunk_matches: Vec<ChunkMatch>,
    pub content: Vec<u8>,
    pub language: String,
    pub version: String,
}

impl FileMatch {
    fn from_proto(file: proto::FileMatch) -> Self {
        Self {
            score: file.score,
            debug: file.debug,
            file_name: file.file_name,
            repository: file.repository,
            repository_id: file.repository_id,
            branches: file.branches,
            chunk_matches: file
                .chunk_matches
                .into_iter()
                .map(ChunkMatch::from_proto)
                .collect(),
            content: file.content,
            language: file.language,
            version: file.version,
        }
    }

    /// `(*zoekt.FileMatch).sizeBytes`.
    ///
    /// The port does not carry `SubRepositoryName`, `SubRepositoryPath`,
    /// `LineMatches` or `Checksum`; the baseline adds a fixed `16 + 0` for each
    /// of the first two and a fixed slice header (`24`) for the last two, so
    /// they are accounted for as empty values here.
    pub fn size_bytes(&self) -> u64 {
        // Score.
        let mut sz = 8u64;
        sz += 16 + self.debug.len() as u64;
        sz += 16 + self.file_name.len() as u64;
        sz += 16 + self.repository.len() as u64;
        sz += 16 + self.language.len() as u64;
        sz += 16; // SubRepositoryName (empty)
        sz += 16; // SubRepositoryPath (empty)
        sz += 16 + self.version.len() as u64;
        // Branches.
        sz += 24;
        for branch in &self.branches {
            sz += 16 + branch.len() as u64;
        }
        // LineMatches (always empty in this port).
        sz += 24;
        // ChunkMatches.
        sz += 24;
        for chunk in &self.chunk_matches {
            sz += chunk.size_bytes();
        }
        // RepositoryID.
        sz += 4;
        // RepositoryPriority.
        sz += 8;
        // Content.
        sz += 24 + self.content.len() as u64;
        // Checksum (always empty in this port).
        sz += 24;
        sz
    }
}

/// One `SearchResponse` chunk from the streaming API.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchChunk {
    pub files: Vec<FileMatch>,
    /// Number of shard crashes reported for this chunk.
    pub crashes: i64,
}

impl SearchChunk {
    /// `SearchResponse.FromProto`.
    pub fn from_proto(response: proto::SearchResponse) -> Self {
        Self {
            files: response
                .files
                .into_iter()
                .map(FileMatch::from_proto)
                .collect(),
            crashes: response.stats.map(|stats| stats.crashes).unwrap_or(0),
        }
    }
}

/// `zoekt.FlushReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum FlushReason {
    /// `FlushReason(0)`: no reason (the baseline stringifies it as `none`).
    #[default]
    Unknown,
    /// `FlushReasonTimerExpired`.
    TimerExpired,
    /// `FlushReasonFinalFlush`.
    FinalFlush,
    /// `FlushReasonMaxSize`.
    MaxSize,
}

impl FlushReason {
    /// `(zoekt.FlushReason).String`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TimerExpired => "timer_expired",
            Self::FinalFlush => "final_flush",
            Self::MaxSize => "max_size_reached",
            Self::Unknown => "none",
        }
    }

    /// `zoekt.FlushReason` from its proto value.
    pub fn from_proto(value: i32) -> Self {
        match value {
            1 => Self::TimerExpired,
            2 => Self::FinalFlush,
            3 => Self::MaxSize,
            _ => Self::Unknown,
        }
    }
}

/// `zoekt.Progress`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Progress {
    pub priority: f64,
    pub max_pending_priority: f64,
}

impl Progress {
    /// `(*zoekt.Progress).sizeBytes`.
    pub fn size_bytes(&self) -> u64 {
        2 * 8
    }
}

/// `zoekt.Stats`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Stats {
    pub content_bytes_loaded: i64,
    pub index_bytes_loaded: i64,
    pub crashes: i64,
    pub duration: Duration,
    pub file_count: i64,
    pub shard_files_considered: i64,
    pub files_considered: i64,
    pub files_loaded: i64,
    pub files_skipped: i64,
    pub shards_scanned: i64,
    pub shards_skipped: i64,
    pub shards_skipped_filter: i64,
    pub match_count: i64,
    pub ngram_matches: i64,
    pub ngram_lookups: i64,
    pub wait: Duration,
    pub match_tree_construction: Duration,
    pub match_tree_search: Duration,
    pub regexps_considered: i64,
    pub flush_reason: FlushReason,
}

impl Stats {
    /// `(*zoekt.Stats).sizeBytes`: `16*8` plus one byte for `FlushReason`.
    pub fn size_bytes(&self) -> u64 {
        16 * 8 + 1
    }

    /// `(*zoekt.Stats).Add`: field-wise addition. The first non-zero
    /// `FlushReason` is sticky when aggregating several zoekts.
    pub fn add(&mut self, other: &Stats) {
        self.content_bytes_loaded += other.content_bytes_loaded;
        self.index_bytes_loaded += other.index_bytes_loaded;
        self.crashes += other.crashes;
        // `Duration` is deliberately not aggregated (the baseline's `Add`
        // leaves it untouched).
        self.file_count += other.file_count;
        self.shard_files_considered += other.shard_files_considered;
        self.files_considered += other.files_considered;
        self.files_loaded += other.files_loaded;
        self.files_skipped += other.files_skipped;
        self.shards_scanned += other.shards_scanned;
        self.shards_skipped += other.shards_skipped;
        self.shards_skipped_filter += other.shards_skipped_filter;
        self.match_count += other.match_count;
        self.ngram_matches += other.ngram_matches;
        self.ngram_lookups += other.ngram_lookups;
        self.wait += other.wait;
        self.match_tree_construction += other.match_tree_construction;
        self.match_tree_search += other.match_tree_search;
        self.regexps_considered += other.regexps_considered;
        if self.flush_reason == FlushReason::Unknown {
            self.flush_reason = other.flush_reason;
        }
    }
}

/// `zoekt.SearchResult`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchResult {
    pub stats: Stats,
    pub progress: Progress,
    pub files: Vec<FileMatch>,
    pub repo_urls: BTreeMap<String, String>,
    pub line_fragments: BTreeMap<String, String>,
}

impl SearchResult {
    /// `(*zoekt.SearchResult).SizeBytes`: a best-effort lower bound on the
    /// in-memory size, used by the aggregator's `maxSizeBytes` guard.
    pub fn size_bytes(&self) -> u64 {
        let mut sz = self.stats.size_bytes() + self.progress.size_bytes();
        // Files slice header + contents.
        sz += 24;
        for file in &self.files {
            sz += file.size_bytes();
        }
        // RepoURLs map header + entries.
        sz += 48;
        for (key, value) in &self.repo_urls {
            sz += 16 + key.len() as u64;
            sz += 16 + value.len() as u64;
        }
        // LineFragments map header + entries.
        sz += 48;
        for (key, value) in &self.line_fragments {
            sz += 16 + key.len() as u64;
            sz += 16 + value.len() as u64;
        }
        sz
    }
}

fn duration_to_proto(duration: Duration) -> ProtoDuration {
    ProtoDuration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_options_match_the_baseline_defaults() {
        let options = SearchOptions::hybrid(10, 3);
        assert_eq!(options.max_wall_time, Duration::from_secs(20));
        assert_eq!(options.flush_wall_time, Duration::from_millis(500));
        assert_eq!(options.shard_max_match_count, 10_000);
        assert_eq!(options.total_max_match_count, 100_000);
        assert_eq!(options.max_doc_display_count, 10);
        assert_eq!(options.num_context_lines, 3);
        assert!(options.chunk_matches);
        assert!(!options.use_bm25_scoring);
        assert!(!options.use_document_ranks);
        assert_eq!(options.document_ranks_weight, 4500.0);
    }

    #[test]
    fn large_limits_raise_the_match_budgets() {
        let options = SearchOptions::hybrid(200_000, 0);
        assert_eq!(options.shard_max_match_count, 200_000);
        assert_eq!(options.total_max_match_count, 200_000);

        let options = SearchOptions::hybrid(20_000, 0);
        assert_eq!(options.shard_max_match_count, 20_000);
        assert_eq!(options.total_max_match_count, 100_000);
    }

    #[test]
    fn converts_durations_to_proto() {
        let options = SearchOptions::hybrid(1, 0);
        let proto = options.to_proto();
        let max_wall_time = proto.max_wall_time.expect("max wall time");
        assert_eq!(max_wall_time.seconds, 20);
        assert_eq!(max_wall_time.nanos, 0);
        let flush = proto.flush_wall_time.expect("flush wall time");
        assert_eq!(flush.seconds, 0);
        assert_eq!(flush.nanos, 500_000_000);
    }

    #[test]
    fn converts_list_responses_with_repo_maps() {
        let response = proto::ListResponse {
            repos: Vec::new(),
            repos_map: [(
                42,
                proto::MinimalRepoListEntry {
                    has_symbols: true,
                    branches: vec![proto::RepositoryBranch {
                        name: "HEAD".to_owned(),
                        version: "abc123".to_owned(),
                    }],
                    index_time_unix: 1_700_000_000,
                },
            )]
            .into_iter()
            .collect(),
            crashes: 2,
            stats: None,
        };
        let response = ListResponse::from_proto(response);
        assert_eq!(response.crashes, 2);
        let entry = &response.repos_map[&42];
        assert!(entry.has_symbols);
        assert_eq!(entry.branches.len(), 1);
        assert_eq!(entry.branches[0].name, "HEAD");
        assert_eq!(entry.branches[0].version, "abc123");
        assert_eq!(entry.index_time_unix, 1_700_000_000);
    }

    #[test]
    fn converts_stream_chunks() {
        let response = proto::SearchResponse {
            stats: Some(proto::Stats {
                crashes: 3,
                ..proto::Stats::default()
            }),
            progress: None,
            files: vec![proto::FileMatch {
                score: 1.5,
                file_name: b"main.go".to_vec(),
                repository: "test/repo".to_owned(),
                repository_id: 7,
                branches: vec!["HEAD".to_owned()],
                chunk_matches: vec![proto::ChunkMatch {
                    content: b"needle\n".to_vec(),
                    content_start: Some(proto::Location {
                        byte_offset: 10,
                        line_number: 1,
                        column: 1,
                    }),
                    file_name: false,
                    ranges: vec![proto::Range {
                        start: Some(proto::Location {
                            byte_offset: 10,
                            line_number: 1,
                            column: 1,
                        }),
                        end: Some(proto::Location {
                            byte_offset: 16,
                            line_number: 1,
                            column: 7,
                        }),
                    }],
                    symbol_info: Vec::new(),
                    score: 0.0,
                    debug_score: String::new(),
                }],
                content: Vec::new(),
                language: "Go".to_owned(),
                version: "abc123".to_owned(),
                ..proto::FileMatch::default()
            }],
        };
        let chunk = SearchChunk::from_proto(response);
        assert_eq!(chunk.crashes, 3);
        assert_eq!(chunk.files.len(), 1);
        let file = &chunk.files[0];
        assert_eq!(file.file_name, b"main.go");
        assert_eq!(file.repository_id, 7);
        assert_eq!(file.chunk_matches.len(), 1);
        let chunk_match = &file.chunk_matches[0];
        assert_eq!(chunk_match.content, b"needle\n");
        assert_eq!(chunk_match.content_start.byte_offset, 10);
        assert_eq!(chunk_match.ranges[0].end.column, 7);
        assert_eq!(file.language, "Go");
        assert_eq!(file.version, "abc123");
    }
}
