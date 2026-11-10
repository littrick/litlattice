//! M9.8gw differential harness:
//! `internal/search/zoekt/indexed_search.go:443-578`
//! (`zoektFileMatchToMultilineMatches`, `zoektFileMatchToPathMatchRanges`,
//! `zoektFileMatchToSymbolResults` incl. `result.DedupSymbols`) against
//! [`sg_service_frontend::search_stream::zoekt_results`].
//!
//! Run with `SG_M9GW_DIFF=1 M9GW_CORPUS=<json> M9GW_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_stream::convert::MinimalRepo;
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::zoekt_results::{
    zoekt_file_match_to_multiline_matches, zoekt_file_match_to_path_match_ranges,
    zoekt_file_match_to_symbol_results,
};
use sg_zoekt::types::{
    ChunkMatch, FileMatch, Location as ZoektLocation, Range as ZoektRange, SymbolInfo,
};

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    repo: CorpusRepo,
    input_rev: String,
    file: CorpusFile,
    path_regexps: Vec<String>,
}

#[derive(Deserialize)]
struct CorpusRepo {
    id: i32,
    name: String,
}

#[derive(Deserialize)]
struct CorpusFile {
    file_name: String,
    version: String,
    language: String,
    chunk_matches: Vec<CorpusChunk>,
}

#[derive(Deserialize)]
struct CorpusChunk {
    content: String,
    content_start: CorpusLocation,
    file_name: bool,
    ranges: Vec<CorpusRange>,
    symbol_info: Vec<Option<CorpusSymbolInfo>>,
}

#[derive(Deserialize, Clone, Copy)]
struct CorpusLocation {
    byte_offset: u32,
    line_number: u32,
    column: u32,
}

#[derive(Deserialize)]
struct CorpusRange {
    start: CorpusLocation,
    end: CorpusLocation,
}

#[derive(Deserialize)]
struct CorpusSymbolInfo {
    sym: String,
    kind: String,
    parent: String,
    parent_kind: String,
}

impl CorpusLocation {
    fn to_zoekt(self) -> ZoektLocation {
        ZoektLocation {
            byte_offset: self.byte_offset,
            line_number: self.line_number,
            column: self.column,
        }
    }
}

fn to_file_match(file: &CorpusFile) -> FileMatch {
    FileMatch {
        file_name: file.file_name.as_bytes().to_vec(),
        version: file.version.clone(),
        language: file.language.clone(),
        chunk_matches: file
            .chunk_matches
            .iter()
            .map(|chunk| ChunkMatch {
                content: chunk.content.as_bytes().to_vec(),
                content_start: chunk.content_start.to_zoekt(),
                file_name: chunk.file_name,
                ranges: chunk
                    .ranges
                    .iter()
                    .map(|range| ZoektRange {
                        start: range.start.to_zoekt(),
                        end: range.end.to_zoekt(),
                    })
                    .collect(),
                symbol_info: chunk
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
            })
            .collect(),
        ..FileMatch::default()
    }
}

#[test]
fn zoekt_result_conversion_diff() {
    if std::env::var("SG_M9GW_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GW_CORPUS").expect("M9GW_CORPUS must be set when SG_M9GW_DIFF=1");
    let out_path = std::env::var("M9GW_OUT").expect("M9GW_OUT must be set when SG_M9GW_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_case(case: &CorpusCase) -> Value {
    let file = to_file_match(&case.file);
    let regexps: Vec<regex::Regex> = case
        .path_regexps
        .iter()
        .map(|pattern| regex::Regex::new(pattern).expect("compile path regexp"))
        .collect();
    let repo = MinimalRepo {
        id: case.repo.id,
        name: case.repo.name.clone(),
    };

    let chunks: Vec<Value> = zoekt_file_match_to_multiline_matches(&file)
        .iter()
        .map(|chunk| {
            json!({
                "content": chunk.content,
                "content_start": location_json(&chunk.content_start),
                "ranges": ranges_json(&chunk.ranges),
            })
        })
        .collect();

    let path_ranges = zoekt_file_match_to_path_match_ranges(&file, &regexps);

    let symbols: Vec<Value> = zoekt_file_match_to_symbol_results(&repo, &case.input_rev, &file)
        .iter()
        .map(|symbol_match| {
            let file_ref = &symbol_match.file;
            let symbol = &symbol_match.symbol;
            json!({
                "file": {
                    "repo": {"id": file_ref.repo.id, "name": file_ref.repo.name},
                    "commit_id": file_ref.commit_id,
                    "path": file_ref.path,
                    "input_rev": file_ref.input_rev.clone().unwrap_or_default(),
                    "precise_language": file_ref.precise_language,
                },
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
        })
        .collect();

    json!({
        "name": case.name,
        "chunk_matches": chunks,
        "path_match_ranges": ranges_json(&path_ranges),
        "symbol_matches": symbols,
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
