//! M9.8gz differential harness:
//! `internal/search/zoekt/indexed_search.go:273-437` (`sendMatches`,
//! `zoektSearch`, `DoZoektSearchGlobal`) and
//! `internal/search/types.go:161-235` (`ZoektParameters.ToSearchOptions`)
//! against [`sg_service_frontend::search_stream::zoekt_search`].
//!
//! Run with `SG_M9GZ_DIFF=1 M9GZ_CORPUS=<json> M9GZ_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::cell::RefCell;
use std::fmt;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::SearchType;
use sg_service_frontend::search_stream::convert::{Match, MinimalRepo};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_service_frontend::search_stream::zoekt_search::{
    do_zoekt_search_global, send_matches, zoekt_search, Features, SelectPath, Streamer,
    ZoektParameters,
};
use sg_zoekt::indexed_repo_revs::{IndexedRepoRevs, RepositoryRevisions, RepositoryRevisionsRepo};
use sg_zoekt::query::{IndexedRequestType, Query};
use sg_zoekt::types::{
    ChunkMatch, FileMatch, Location as ZoektLocation, MinimalRepoListEntry, Range as ZoektRange,
    RepositoryBranch, SearchOptions, SearchResult as ZoektSearchResult, Stats as ZoektStats,
    SymbolInfo, DEFAULT_DOCUMENT_RANKS_WEIGHT,
};

#[derive(Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    typ: String,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    file_match_limit: i32,
    #[serde(default)]
    num_context_lines: i32,
    #[serde(default)]
    features_debug: bool,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    since_ms: i64,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    path_regexps: Vec<String>,
    #[serde(default)]
    event: Option<ResultSpec>,
    #[serde(default)]
    repo: RepoRef,
    #[serde(default)]
    input_revs: Vec<String>,
    #[serde(default)]
    batches: Vec<ResultSpec>,
    #[serde(default)]
    stream_error: bool,
    #[serde(default)]
    repos: Vec<RepoSpec>,
}

#[derive(Deserialize, Default, Clone)]
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize, Default)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
    #[serde(default)]
    branches: Vec<BranchSpec>,
}

#[derive(Deserialize, Default)]
struct BranchSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

#[derive(Deserialize, Default)]
struct ResultSpec {
    #[serde(default)]
    files: Vec<FileSpec>,
    #[serde(default)]
    stats: StatsSpec,
}

#[derive(Deserialize, Default)]
struct StatsSpec {
    #[serde(default)]
    file_count: i64,
    #[serde(default)]
    match_count: i64,
    #[serde(default)]
    files_skipped: i64,
    #[serde(default)]
    shards_skipped: i64,
    #[serde(default)]
    crashes: i64,
}

#[derive(Deserialize, Default)]
struct FileSpec {
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    repository_id: u32,
    #[serde(default)]
    version: String,
    #[serde(default)]
    branches: Vec<String>,
    #[serde(default)]
    language: String,
    #[serde(default)]
    debug: String,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
}

#[derive(Deserialize, Default)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
    #[serde(default)]
    symbol_info: Vec<Option<SymbolSpec>>,
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

impl LocationSpec {
    fn to_zoekt(self) -> ZoektLocation {
        ZoektLocation {
            byte_offset: self.byte_offset,
            line_number: self.line_number,
            column: self.column,
        }
    }
}

fn to_file(spec: &FileSpec) -> FileMatch {
    FileMatch {
        file_name: spec.file_name.as_bytes().to_vec(),
        repository: spec.repository.clone(),
        repository_id: spec.repository_id,
        version: spec.version.clone(),
        branches: spec.branches.clone(),
        language: spec.language.clone(),
        debug: spec.debug.clone(),
        chunk_matches: spec.chunk_matches.iter().map(to_chunk).collect(),
        ..FileMatch::default()
    }
}

fn to_chunk(spec: &ChunkSpec) -> ChunkMatch {
    ChunkMatch {
        content: spec.content.as_bytes().to_vec(),
        content_start: spec.content_start.to_zoekt(),
        file_name: spec.file_name,
        ranges: spec
            .ranges
            .iter()
            .map(|range| ZoektRange {
                start: range.start.to_zoekt(),
                end: range.end.to_zoekt(),
            })
            .collect(),
        symbol_info: spec
            .symbol_info
            .iter()
            .map(|symbol| {
                symbol.as_ref().map(|symbol| SymbolInfo {
                    sym: symbol.sym.clone(),
                    kind: symbol.kind.clone(),
                    parent: symbol.parent.clone(),
                    parent_kind: symbol.parent_kind.clone(),
                })
            })
            .collect(),
        score: 0.0,
    }
}

fn to_result(spec: &ResultSpec) -> ZoektSearchResult {
    ZoektSearchResult {
        stats: ZoektStats {
            file_count: spec.stats.file_count,
            match_count: spec.stats.match_count,
            files_skipped: spec.stats.files_skipped,
            shards_skipped: spec.stats.shards_skipped,
            crashes: spec.stats.crashes,
            ..ZoektStats::default()
        },
        files: spec.files.iter().map(to_file).collect(),
        ..ZoektSearchResult::default()
    }
}

fn to_indexed_repos(specs: &[RepoSpec]) -> IndexedRepoRevs {
    let mut indexed = IndexedRepoRevs::default();
    for spec in specs {
        indexed.add(
            &RepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: spec.id as u32,
                    name: spec.name.clone(),
                },
                revs: spec.revs.clone(),
            },
            MinimalRepoListEntry {
                branches: spec
                    .branches
                    .iter()
                    .map(|branch| RepositoryBranch {
                        name: branch.name.clone(),
                        version: branch.version.clone(),
                    })
                    .collect(),
                ..MinimalRepoListEntry::default()
            },
        );
    }
    indexed
}

fn inner_query() -> Query {
    Query::Substring {
        pattern: "needle".to_owned(),
        case_sensitive: false,
        file_name: false,
        content: true,
    }
}

fn request_type(typ: &str) -> IndexedRequestType {
    if typ == "symbol" {
        IndexedRequestType::Symbol
    } else {
        IndexedRequestType::Text
    }
}

fn pattern_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        _ => SearchType::Standard,
    }
}

fn params(case: &Case) -> ZoektParameters {
    ZoektParameters {
        query: inner_query(),
        typ: request_type(&case.typ),
        file_match_limit: case.file_match_limit,
        select: SelectPath(case.select.clone()),
        num_context_lines: case.num_context_lines,
        features: Features {
            debug: case.features_debug,
            ..Features::default()
        },
        pattern_type: pattern_type(&case.pattern_type),
        trace: false,
        flush_wall_time_override: None,
        use_document_ranks: false,
        document_ranks_weight: DEFAULT_DOCUMENT_RANKS_WEIGHT,
    }
}

fn deadline(kind: &str) -> Option<Instant> {
    match kind {
        "expired" => Some(Instant::now() - Duration::from_secs(1)),
        "future" => Some(Instant::now() + Duration::from_secs(30)),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct FakeError(String);

impl fmt::Display for FakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FakeError {}

struct FakeStreamer {
    batches: Vec<ZoektSearchResult>,
    error: bool,
    record: RefCell<Option<(Query, SearchOptions)>>,
}

impl FakeStreamer {
    fn new(case: &Case) -> Self {
        Self {
            batches: case.batches.iter().map(to_result).collect(),
            error: case.stream_error,
            record: RefCell::new(None),
        }
    }

    fn called(&self) -> bool {
        self.record.borrow().is_some()
    }

    fn recorded(&self) -> Option<(Query, SearchOptions)> {
        self.record.borrow().clone()
    }
}

impl Streamer for FakeStreamer {
    type Error = FakeError;

    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(ZoektSearchResult),
    ) -> Result<(), Self::Error> {
        *self.record.borrow_mut() = Some((query.clone(), options.clone()));
        for batch in &self.batches {
            sender(batch.clone());
        }
        if self.error {
            return Err(FakeError("stream failed".to_owned()));
        }
        Ok(())
    }
}

#[test]
fn zoekt_search_execution_diff() {
    if std::env::var("SG_M9GZ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GZ_CORPUS").expect("M9GZ_CORPUS must be set when SG_M9GZ_DIFF=1");
    let out_path = std::env::var("M9GZ_OUT").expect("M9GZ_OUT must be set when SG_M9GZ_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_case(case: &Case) -> Value {
    match case.kind.as_str() {
        "send_matches" => run_send_matches(case),
        "zoekt_search" => run_zoekt_search(case),
        "global" => run_global(case),
        other => panic!("unknown case kind {other}"),
    }
}

fn regexps(case: &Case) -> Vec<regex::Regex> {
    case.path_regexps
        .iter()
        .map(|pattern| regex::Regex::new(pattern).expect("compile path regexp"))
        .collect()
}

fn empty_output(case: &Case) -> Value {
    json!({
        "name": case.name,
        "kind": case.kind,
        "stream_called": false,
        "error": "",
        "options": Value::Null,
        "query": Value::Null,
        "events": Vec::<Value>::new(),
    })
}

fn run_send_matches(case: &Case) -> Value {
    let result = to_result(case.event.as_ref().unwrap_or(&ResultSpec::default()));
    let regexps = regexps(case);
    let repo = MinimalRepo {
        id: case.repo.id,
        name: case.repo.name.clone(),
    };
    let mut events: Vec<SearchEvent> = Vec::new();
    send_matches(
        &result,
        &regexps,
        |_file| (repo.clone(), case.input_revs.clone()),
        request_type(&case.typ),
        &SelectPath(case.select.clone()),
        &mut |event| events.push(event),
    );

    let mut output = empty_output(case);
    output["events"] = events.iter().map(event_json).collect();
    output
}

fn run_zoekt_search(case: &Case) -> Value {
    let repos = to_indexed_repos(&case.repos);
    let client = FakeStreamer::new(case);
    let params = params(case);
    let regexps = regexps(case);
    let since_ms = case.since_ms.max(0) as u64;

    let mut events: Vec<SearchEvent> = Vec::new();
    let outcome = zoekt_search(
        &repos,
        inner_query(),
        &regexps,
        request_type(&case.typ),
        &client,
        &params,
        deadline(&case.deadline),
        |_| Duration::from_millis(since_ms),
        &mut |event| events.push(event),
    );
    structured_output(case, &client, outcome, &events)
}

fn run_global(case: &Case) -> Value {
    let client = FakeStreamer::new(case);
    let params = params(case);
    let regexps = regexps(case);

    let mut events: Vec<SearchEvent> = Vec::new();
    let outcome = do_zoekt_search_global(
        &client,
        &params,
        &regexps,
        deadline(&case.deadline),
        &mut |event| events.push(event),
    );
    structured_output(case, &client, outcome, &events)
}

fn structured_output<E: fmt::Display>(
    case: &Case,
    client: &FakeStreamer,
    outcome: Result<(), E>,
    events: &[SearchEvent],
) -> Value {
    let record = client.recorded();
    json!({
        "name": case.name,
        "kind": case.kind,
        "stream_called": client.called(),
        "error": match &outcome {
            Ok(()) => String::new(),
            Err(err) => err.to_string(),
        },
        "options": record
            .as_ref()
            .map(|(_, options)| options_json(options, &case.deadline))
            .unwrap_or(Value::Null),
        "query": record.as_ref().map(|(query, _)| query_json(query)),
        "events": events.iter().map(event_json).collect::<Vec<Value>>(),
    })
}

fn query_json(query: &Query) -> Value {
    if let Query::And(children) = query {
        if children.len() == 2 {
            if let Query::BranchesRepos(list) = &children[0] {
                let mut branches: Vec<(String, Vec<u32>)> = list
                    .iter()
                    .map(|branch| {
                        let bitmap = roaring::RoaringBitmap::deserialize_from(&branch.repos[..])
                            .expect("decode branch repos roaring bitmap");
                        let mut ids: Vec<u32> = bitmap.iter().collect();
                        ids.sort_unstable();
                        (branch.branch.clone(), ids)
                    })
                    .collect();
                branches.sort_by(|a, b| a.0.cmp(&b.0));
                let branches: Vec<Value> = branches
                    .into_iter()
                    .map(|(branch, repo_ids)| json!({"branch": branch, "repo_ids": repo_ids}))
                    .collect();
                return json!({"branches": branches, "inner": children[1].to_string()});
            }
        }
    }
    json!({"branches": Vec::<Value>::new(), "inner": query.to_string()})
}

fn options_json(options: &SearchOptions, deadline: &str) -> Value {
    let max_wall_time = if deadline == "none" {
        json!(options.max_wall_time.as_millis() as i64)
    } else {
        Value::Null
    };
    json!({
        "estimate_doc_count": options.estimate_doc_count,
        "whole": options.whole,
        "shard_max_match_count": options.shard_max_match_count,
        "total_max_match_count": options.total_max_match_count,
        "shard_repo_max_match_count": options.shard_repo_max_match_count,
        "max_wall_time_ms": max_wall_time,
        "max_wall_time_overridden": options.max_wall_time != Duration::from_secs(20),
        "flush_wall_time_ms": options.flush_wall_time.as_millis() as i64,
        "max_doc_display_count": options.max_doc_display_count,
        "max_match_display_count": options.max_match_display_count,
        "num_context_lines": options.num_context_lines,
        "chunk_matches": options.chunk_matches,
        "use_document_ranks": options.use_document_ranks,
        "document_ranks_weight": format!("{}", options.document_ranks_weight),
        "trace": options.trace,
        "debug_score": options.debug_score,
        "use_bm25_scoring": options.use_bm25_scoring,
    })
}

fn event_json(event: &SearchEvent) -> Value {
    let results: Vec<Value> = event.results.iter().map(match_json).collect();
    let status: Vec<[i32; 2]> = event
        .stats
        .status
        .entries()
        .into_iter()
        .map(|(id, mask)| [id, i32::from(mask)])
        .collect();
    json!({
        "results": results,
        "stats": {
            "is_limit_hit": event.stats.is_limit_hit,
            "backends_missing": event.stats.backends_missing,
            "status": status,
        },
    })
}

fn match_json(match_: &Match) -> Value {
    match match_ {
        Match::File(file_match) => json!({
            "kind": "file",
            "file": file_json(&file_match.file),
            "chunk_matches": file_match.chunk_matches.iter().map(chunk_json).collect::<Vec<Value>>(),
            "symbols": file_match.symbols.iter().map(symbol_json).collect::<Vec<Value>>(),
            "path_matches": ranges_json(&file_match.path_matches),
            "debug": file_match.debug,
        }),
        Match::Repo(repo) => json!({
            "kind": "repo",
            "name": repo.name,
            "id": repo.id,
        }),
        _ => json!({"kind": "unknown"}),
    }
}

fn file_json(file: &sg_service_frontend::search_stream::convert::FileRef) -> Value {
    json!({
        "repo": {"id": file.repo.id, "name": file.repo.name},
        "commit_id": file.commit_id,
        "path": file.path,
        "input_rev": file.input_rev.clone().unwrap_or_default(),
        "precise_language": file.precise_language,
    })
}

fn chunk_json(chunk: &sg_service_frontend::search_stream::convert::ChunkMatch) -> Value {
    json!({
        "content": chunk.content,
        "content_start": location_json(&chunk.content_start),
        "ranges": ranges_json(&chunk.ranges),
    })
}

fn symbol_json(symbol_match: &sg_service_frontend::search_stream::convert::SymbolMatch) -> Value {
    let symbol = &symbol_match.symbol;
    json!({
        "file": file_json(&symbol_match.file),
        "symbol": {
            "name": symbol.name,
            "path": symbol.path,
            "line": symbol.line,
            "character": symbol.character,
            "kind": symbol.kind,
            "language": symbol.language,
            "parent": symbol.parent,
            "parent_kind": symbol.parent_kind,
        },
    })
}

fn location_json(location: &Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn ranges_json(ranges: &[Range]) -> Vec<Value> {
    ranges
        .iter()
        .map(|range| {
            json!({
                "start": location_json(&range.start),
                "end": location_json(&range.end),
            })
        })
        .collect()
}
