//! M9.8ig differential harness: `internal/search/backend/grpc.go:27-52`
//! (`zoektGRPCClient.StreamSearch`: per-chunk `SearchResultFromProto` +
//! `sender.Send`) against [`sg_zoekt::streamer::BlockingStreamer`] driving an
//! async [`sg_zoekt::streamer::Streamer`] fake.
//!
//! The corpus describes, per case, a sequence of wire `SearchResponse` chunks.
//! This harness parses each with `SearchChunk::from_proto`, yields them from the
//! fake async stream, and drives the blocking bridge. Both sides emit
//! byte-identical normalized JSON (`Files`/`Stats`/`Progress` per result).
//!
//! Run with `SG_M9IG_DIFF=1 M9IG_CORPUS=<json> M9IG_OUT=<json>`; the wrapper
//! script in `test_ws/p9-go` does this.

use std::fmt;
use std::time::Duration;

use prost_types::Duration as ProtoDuration;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::proto;
use sg_zoekt::streamer::{BlockingStreamer, Streamer};
use sg_zoekt::types::{
    ChunkMatch, FileMatch, ListOptions, ListResponse, Location, Range, SearchChunk, SearchOptions,
    SearchResult, Stats, SymbolInfo,
};

#[derive(Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    chunks: Vec<ChunkSpec>,
}

#[derive(Deserialize, Default)]
struct ChunkSpec {
    #[serde(default)]
    stats: Option<StatsSpec>,
    #[serde(default)]
    progress: Option<ProgressSpec>,
    #[serde(default)]
    files: Vec<FileSpec>,
}

#[derive(Deserialize, Default)]
struct DurationSpec {
    #[serde(default)]
    seconds: i64,
    #[serde(default)]
    nanos: i32,
}

#[derive(Deserialize, Default)]
struct StatsSpec {
    #[serde(default)]
    content_bytes_loaded: i64,
    #[serde(default)]
    index_bytes_loaded: i64,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    duration: Option<DurationSpec>,
    #[serde(default)]
    file_count: i64,
    #[serde(default)]
    shard_files_considered: i64,
    #[serde(default)]
    files_considered: i64,
    #[serde(default)]
    files_loaded: i64,
    #[serde(default)]
    files_skipped: i64,
    #[serde(default)]
    shards_scanned: i64,
    #[serde(default)]
    shards_skipped: i64,
    #[serde(default)]
    shards_skipped_filter: i64,
    #[serde(default)]
    match_count: i64,
    #[serde(default)]
    ngram_matches: i64,
    #[serde(default)]
    ngram_lookups: i64,
    #[serde(default)]
    wait: Option<DurationSpec>,
    #[serde(default)]
    match_tree_construction: Option<DurationSpec>,
    #[serde(default)]
    match_tree_search: Option<DurationSpec>,
    #[serde(default)]
    regexps_considered: i64,
    #[serde(default)]
    flush_reason: i32,
}

#[derive(Deserialize, Default)]
struct ProgressSpec {
    #[serde(default)]
    priority: f64,
    #[serde(default)]
    max_pending_priority: f64,
}

#[derive(Deserialize, Default)]
struct FileSpec {
    #[serde(default)]
    score: f64,
    #[serde(default)]
    debug: String,
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    repository_id: u32,
    #[serde(default)]
    branches: Vec<String>,
    #[serde(default)]
    content: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    chunk_matches: Vec<ChunkMatchSpec>,
}

#[derive(Deserialize, Default)]
struct ChunkMatchSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
    #[serde(default)]
    symbol_info: Vec<SymbolSpec>,
    #[serde(default)]
    score: f64,
}

#[derive(Deserialize, Default, Clone, Copy)]
struct LocationSpec {
    #[serde(default)]
    byte_offset: u32,
    #[serde(default)]
    line_number: u32,
    #[serde(default)]
    column: u32,
}

#[derive(Deserialize, Default)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Deserialize, Default)]
struct SymbolSpec {
    #[serde(default)]
    sym: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

fn to_proto_duration(spec: &DurationSpec) -> ProtoDuration {
    ProtoDuration {
        seconds: spec.seconds,
        nanos: spec.nanos,
    }
}

fn to_proto_stats(spec: &StatsSpec) -> proto::Stats {
    proto::Stats {
        content_bytes_loaded: spec.content_bytes_loaded,
        index_bytes_loaded: spec.index_bytes_loaded,
        crashes: spec.crashes,
        duration: spec.duration.as_ref().map(to_proto_duration),
        file_count: spec.file_count,
        shard_files_considered: spec.shard_files_considered,
        files_considered: spec.files_considered,
        files_loaded: spec.files_loaded,
        files_skipped: spec.files_skipped,
        shards_scanned: spec.shards_scanned,
        shards_skipped: spec.shards_skipped,
        shards_skipped_filter: spec.shards_skipped_filter,
        match_count: spec.match_count,
        ngram_matches: spec.ngram_matches,
        ngram_lookups: spec.ngram_lookups,
        wait: spec.wait.as_ref().map(to_proto_duration),
        match_tree_construction: spec.match_tree_construction.as_ref().map(to_proto_duration),
        match_tree_search: spec.match_tree_search.as_ref().map(to_proto_duration),
        regexps_considered: spec.regexps_considered,
        flush_reason: spec.flush_reason,
    }
}

fn to_proto_location(spec: LocationSpec) -> proto::Location {
    proto::Location {
        byte_offset: spec.byte_offset,
        line_number: spec.line_number,
        column: spec.column,
    }
}

fn to_proto_chunk_match(spec: &ChunkMatchSpec) -> proto::ChunkMatch {
    proto::ChunkMatch {
        content: spec.content.as_bytes().to_vec(),
        content_start: Some(to_proto_location(spec.content_start)),
        file_name: spec.file_name,
        ranges: spec
            .ranges
            .iter()
            .map(|range| proto::Range {
                start: Some(to_proto_location(range.start)),
                end: Some(to_proto_location(range.end)),
            })
            .collect(),
        symbol_info: spec
            .symbol_info
            .iter()
            .map(|symbol| proto::SymbolInfo {
                sym: symbol.sym.clone(),
                kind: symbol.kind.clone(),
                parent: symbol.parent.clone(),
                parent_kind: symbol.parent_kind.clone(),
            })
            .collect(),
        score: spec.score,
        debug_score: String::new(),
    }
}

fn to_proto_file(spec: &FileSpec) -> proto::FileMatch {
    proto::FileMatch {
        score: spec.score,
        debug: spec.debug.clone(),
        file_name: spec.file_name.as_bytes().to_vec(),
        repository: spec.repository.clone(),
        repository_id: spec.repository_id,
        branches: spec.branches.clone(),
        chunk_matches: spec
            .chunk_matches
            .iter()
            .map(to_proto_chunk_match)
            .collect(),
        content: spec.content.as_bytes().to_vec(),
        language: spec.language.clone(),
        version: spec.version.clone(),
        ..proto::FileMatch::default()
    }
}

fn to_proto_response(chunk: &ChunkSpec) -> proto::SearchResponse {
    proto::SearchResponse {
        stats: chunk.stats.as_ref().map(to_proto_stats),
        progress: chunk.progress.as_ref().map(|progress| proto::Progress {
            priority: progress.priority,
            max_pending_priority: progress.max_pending_priority,
        }),
        files: chunk.files.iter().map(to_proto_file).collect(),
    }
}

fn duration_nanos(duration: Duration) -> i64 {
    // Corpora stay well within the i64 nanosecond range.
    duration.as_nanos() as i64
}

fn stats_json(stats: &Stats) -> Value {
    json!({
        "content_bytes_loaded": stats.content_bytes_loaded,
        "index_bytes_loaded": stats.index_bytes_loaded,
        "crashes": stats.crashes,
        "duration_nanos": duration_nanos(stats.duration),
        "file_count": stats.file_count,
        "shard_files_considered": stats.shard_files_considered,
        "files_considered": stats.files_considered,
        "files_loaded": stats.files_loaded,
        "files_skipped": stats.files_skipped,
        "shards_scanned": stats.shards_scanned,
        "shards_skipped": stats.shards_skipped,
        "shards_skipped_filter": stats.shards_skipped_filter,
        "match_count": stats.match_count,
        "ngram_matches": stats.ngram_matches,
        "ngram_lookups": stats.ngram_lookups,
        "wait_nanos": duration_nanos(stats.wait),
        "match_tree_construction_nanos": duration_nanos(stats.match_tree_construction),
        "match_tree_search_nanos": duration_nanos(stats.match_tree_search),
        "regexps_considered": stats.regexps_considered,
        "flush_reason": stats.flush_reason.as_str(),
    })
}

fn location_json(location: &Location) -> Value {
    json!({
        "byte_offset": location.byte_offset,
        "line_number": location.line_number,
        "column": location.column,
    })
}

fn symbol_json(symbol: &Option<SymbolInfo>) -> Value {
    match symbol {
        Some(symbol) => json!({
            "sym": symbol.sym,
            "kind": symbol.kind,
            "parent": symbol.parent,
            "parent_kind": symbol.parent_kind,
        }),
        None => Value::Null,
    }
}

fn chunk_match_json(chunk: &ChunkMatch) -> Value {
    json!({
        "content": String::from_utf8_lossy(&chunk.content),
        "content_start": location_json(&chunk.content_start),
        "file_name": chunk.file_name,
        "ranges": chunk
            .ranges
            .iter()
            .map(|range: &Range| json!({
                "start": location_json(&range.start),
                "end": location_json(&range.end),
            }))
            .collect::<Vec<_>>(),
        "symbol_info": chunk.symbol_info.iter().map(symbol_json).collect::<Vec<_>>(),
        "score_bits": chunk.score.to_bits(),
    })
}

fn file_json(file: &FileMatch) -> Value {
    json!({
        "score_bits": file.score.to_bits(),
        "debug": file.debug,
        "file_name": String::from_utf8_lossy(&file.file_name),
        "repository": file.repository,
        "repository_id": file.repository_id,
        "branches": file.branches,
        "content": String::from_utf8_lossy(&file.content),
        "language": file.language,
        "version": file.version,
        "chunk_matches": file.chunk_matches.iter().map(chunk_match_json).collect::<Vec<_>>(),
    })
}

fn result_json(result: &SearchResult) -> Value {
    json!({
        "stats": stats_json(&result.stats),
        "progress": {
            "priority_bits": result.progress.priority.to_bits(),
            "max_pending_priority_bits": result.progress.max_pending_priority.to_bits(),
        },
        "files": result.files.iter().map(file_json).collect::<Vec<_>>(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError;

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test error")
    }
}

impl std::error::Error for TestError {}

/// A self-contained async stream: yields the corpus chunks in order, yielding
/// once per chunk to prove the future is polled on a Tokio runtime.
struct FakeAsyncStreamer {
    chunks: Vec<SearchChunk>,
}

impl Streamer for FakeAsyncStreamer {
    type Error = TestError;

    async fn list(
        &self,
        _query: &sg_zoekt::query::Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, TestError> {
        Ok(ListResponse::default())
    }

    async fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), TestError> {
        for chunk in &self.chunks {
            sender(chunk.clone());
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

fn run_case(case: &Case) -> Value {
    let chunks: Vec<SearchChunk> = case
        .chunks
        .iter()
        .map(|chunk| SearchChunk::from_proto(to_proto_response(chunk)))
        .collect();
    let streamer = BlockingStreamer::new(FakeAsyncStreamer { chunks });

    let mut results = Vec::new();
    streamer
        .stream_search_blocking(
            &sg_zoekt::query::Query::Const(true),
            &SearchOptions::default(),
            &mut |result| results.push(result),
        )
        .expect("fake stream succeeds");

    json!({
        "name": case.name,
        "results": results.iter().map(result_json).collect::<Vec<_>>(),
    })
}

#[test]
fn zoekt_blocking_streamer_diff() {
    if std::env::var("SG_M9IG_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9IG_CORPUS").expect("M9IG_CORPUS must be set when SG_M9IG_DIFF=1");
    let out_path = std::env::var("M9IG_OUT").expect("M9IG_OUT must be set when SG_M9IG_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}
