#![forbid(unsafe_code)]

//! `xtask`: repository automation for the Rust rewrite.
//!
//! Subcommands:
//! - `gen-proto`: verify (or update) the pinned baseline proto snapshot.
//! - `contract`: T0 static checks (proto snapshot + gRPC surface).
//! - `diff`: replay `symbols`/`searcher` fixtures against an endpoint and compare.
//! - `record`: call an endpoint and store its normalized response in a fixture.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use sg_proto::searcher as spb;
use sg_proto::symbols as pb;

/// Baseline commit the snapshot was taken from.
const BASELINE_COMMIT: &str = "c864f15af264f0f456a6d8a83290b5c940715349";

const PROTO_SERVICES: &[&str] = &["symbols", "searcher", "gitserver"];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        return Err(usage());
    };
    let rest: Vec<String> = args.collect();
    match command.as_str() {
        "gen-proto" => gen_proto(&rest),
        "contract" => contract(&rest),
        "diff" => diff(&rest),
        "record" => record(&rest),
        "-h" | "--help" | "help" => {
            println!("{}", usage());
            Ok(())
        }
        other => Err(format!("unknown command {other:?}\n\n{}", usage())),
    }
}

fn usage() -> String {
    "\
usage: xtask <command> [options]

commands:
  gen-proto [--update]            verify pinned protos against the baseline snapshot
  contract                        T0 static checks (protos + gRPC surface)
  diff --service symbols|searcher --fixture DIR [--addr URL]
                                  replay fixtures and compare normalized responses
  record --service symbols|searcher --fixture DIR --addr URL
                                  store normalized responses into fixtures
"
    .to_owned()
}

fn workspace_root() -> Result<PathBuf, String> {
    // xtask runs from anywhere; the crate dir is `<root>/xtask`.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "cannot locate workspace root".to_owned())
}

// ---------------------------------------------------------------------------
// gen-proto / contract
// ---------------------------------------------------------------------------

fn snapshot_dir() -> Result<PathBuf, String> {
    Ok(workspace_root()?.join("testdata/proto"))
}

fn crate_proto_dir() -> Result<PathBuf, String> {
    Ok(workspace_root()?.join("crates/net/sg-proto/proto"))
}

fn gen_proto(args: &[String]) -> Result<(), String> {
    let update = args.iter().any(|arg| arg == "--update");
    let snapshot = snapshot_dir()?;
    let crate_dir = crate_proto_dir()?;

    let mut checked = 0usize;
    for service in PROTO_SERVICES {
        let relative = PathBuf::from(service)
            .join("v1")
            .join(format!("{service}.proto"));
        let source = snapshot.join(&relative);
        let target = crate_dir.join(&relative);
        let source_bytes = fs::read(&source)
            .map_err(|err| format!("read snapshot {}: {err}", source.display()))?;

        if update {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| format!("create {}: {err}", parent.display()))?;
            }
            fs::write(&target, &source_bytes)
                .map_err(|err| format!("write {}: {err}", target.display()))?;
        } else {
            let target_bytes = fs::read(&target)
                .map_err(|err| format!("read generated proto {}: {err}", target.display()))?;
            if target_bytes != source_bytes {
                return Err(format!(
                    "proto drift: {} differs from the pinned snapshot; run `xtask gen-proto --update` after reviewing the baseline diff",
                    target.display()
                ));
            }
        }
        checked += 1;
    }

    verify_manifest(&snapshot)?;
    println!("gen-proto: {checked} proto files match the pinned snapshot ({BASELINE_COMMIT})");
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn verify_manifest(snapshot: &Path) -> Result<(), String> {
    let manifest_path = snapshot.join("MANIFEST.json");
    let manifest: Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .map_err(|err| format!("read {}: {err}", manifest_path.display()))?,
    )
    .map_err(|err| format!("parse {}: {err}", manifest_path.display()))?;

    let commit = manifest
        .get("baseline_commit")
        .and_then(Value::as_str)
        .ok_or_else(|| "MANIFEST.json: missing baseline_commit".to_owned())?;
    if commit != BASELINE_COMMIT {
        return Err(format!(
            "MANIFEST.json: baseline_commit is {commit}, expected {BASELINE_COMMIT}"
        ));
    }

    let files = manifest
        .get("files")
        .and_then(Value::as_object)
        .ok_or_else(|| "MANIFEST.json: missing files".to_owned())?;
    for (relative, expected) in files {
        let expected = expected
            .as_str()
            .ok_or_else(|| format!("MANIFEST.json: hash for {relative} is not a string"))?;
        let bytes = fs::read(snapshot.join(relative))
            .map_err(|err| format!("read snapshot {relative}: {err}"))?;
        let actual = sha256_hex(&bytes);
        if actual != expected {
            return Err(format!(
                "snapshot hash mismatch for {relative}: got {actual}, expected {expected}"
            ));
        }
    }
    Ok(())
}

fn contract(_args: &[String]) -> Result<(), String> {
    gen_proto(&[])?;

    let snapshot = snapshot_dir()?;
    let symbols = read_proto(&snapshot.join("symbols/v1/symbols.proto"))?;
    for rpc in ["Search", "LocalCodeIntel", "SymbolInfo", "Healthz"] {
        if !symbols.contains(&format!("rpc {rpc}(")) {
            return Err(format!("symbols proto is missing rpc {rpc}"));
        }
    }
    let searcher = read_proto(&snapshot.join("searcher/v1/searcher.proto"))?;
    if !searcher.contains("rpc Search(") {
        return Err("searcher proto is missing rpc Search".to_owned());
    }
    let gitserver = read_proto(&snapshot.join("gitserver/v1/gitserver.proto"))?;
    for rpc in ["ReadFile", "Archive", "ChangedFiles"] {
        if !gitserver.contains(&format!("rpc {rpc}(")) {
            return Err(format!("gitserver proto is missing rpc {rpc}"));
        }
    }

    println!("contract: proto snapshot and gRPC surface OK");
    Ok(())
}

fn read_proto(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| format!("read {}: {err}", path.display()))
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rpc {
    Search,
    LocalCodeIntel,
    SymbolInfo,
}

impl Rpc {
    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "search" => Ok(Self::Search),
            "local_code_intel" => Ok(Self::LocalCodeIntel),
            "symbol_info" => Ok(Self::SymbolInfo),
            other => Err(format!("unknown symbols rpc {other:?}")),
        }
    }
}

/// Which service a fixture targets. `searcher` has a single streaming RPC, so
/// its `rpc` is fixed rather than an enum.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FixtureService {
    Symbols(Rpc),
    Searcher,
}

impl FixtureService {
    fn name(self) -> &'static str {
        match self {
            Self::Symbols(_) => "symbols",
            Self::Searcher => "searcher",
        }
    }
}

struct FixtureCase {
    path: PathBuf,
    service: FixtureService,
    request: Value,
    expected: Option<Value>,
}

fn load_fixture(path: &Path) -> Result<FixtureCase, String> {
    let value: Value = serde_json::from_slice(
        &fs::read(path).map_err(|err| format!("read {}: {err}", path.display()))?,
    )
    .map_err(|err| format!("parse {}: {err}", path.display()))?;

    let service = value
        .get("service")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{}: missing service", path.display()))?;
    let rpc = value
        .get("rpc")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{}: missing rpc", path.display()))?;
    let service = match service {
        "symbols" => FixtureService::Symbols(Rpc::parse(rpc)?),
        "searcher" => {
            if rpc != "search" {
                return Err(format!(
                    "{}: unknown searcher rpc {rpc:?} (only search)",
                    path.display()
                ));
            }
            FixtureService::Searcher
        }
        other => {
            return Err(format!(
                "{}: unsupported service {other:?} (symbols or searcher)",
                path.display()
            ))
        }
    };
    let request = value
        .get("request")
        .cloned()
        .ok_or_else(|| format!("{}: missing request", path.display()))?;
    Ok(FixtureCase {
        path: path.to_path_buf(),
        service,
        request,
        expected: value.get("expected").cloned(),
    })
}

fn collect_fixtures(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut fixtures = Vec::new();
    for entry in fs::read_dir(dir).map_err(|err| format!("read dir {}: {err}", dir.display()))? {
        let path = entry
            .map_err(|err| format!("read dir {}: {err}", dir.display()))?
            .path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            fixtures.push(path);
        }
    }
    fixtures.sort();
    if fixtures.is_empty() {
        return Err(format!("no *.json fixtures in {}", dir.display()));
    }
    Ok(fixtures)
}

fn parse_args(args: &[String]) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        let Some(name) = flag.strip_prefix("--") else {
            return Err(format!("unexpected argument {flag:?}"));
        };
        let value = iter
            .next()
            .ok_or_else(|| format!("missing value for --{name}"))?;
        parsed.insert(name.to_owned(), value.clone());
    }
    Ok(parsed)
}

fn required<'a>(args: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, String> {
    args.get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("missing required --{name}"))
}

// ---------------------------------------------------------------------------
// symbols request/response JSON
// ---------------------------------------------------------------------------

fn string(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("request: {field} must be a string"))
}

fn optional_string(value: &Value, field: &str) -> Result<String, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(_) => Err(format!("request: {field} must be a string")),
    }
}

fn strings(value: &Value, field: &str) -> Result<Vec<String>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("request: {field} must contain strings"))
            })
            .collect(),
        Some(_) => Err(format!("request: {field} must be an array")),
    }
}

fn bool_field(value: &Value, field: &str) -> Result<bool, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(format!("request: {field} must be a bool")),
    }
}

fn i32_field(value: &Value, field: &str) -> Result<i32, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(0),
        Some(number) => number
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .ok_or_else(|| format!("request: {field} must be an int32")),
    }
}

fn repo_commit_path(value: &Value) -> Result<pb::RepoCommitPath, String> {
    let path = value
        .get("repo_commit_path")
        .ok_or_else(|| "request: missing repo_commit_path".to_owned())?;
    Ok(pb::RepoCommitPath {
        repo: string(path, "repo")?,
        commit: string(path, "commit")?,
        path: string(path, "path")?,
    })
}

fn point(value: &Value) -> Result<pb::Point, String> {
    let point = value
        .get("point")
        .ok_or_else(|| "request: missing point".to_owned())?;
    Ok(pb::Point {
        row: i32_field(point, "row")?,
        column: i32_field(point, "column")?,
    })
}

fn duration(value: &Value) -> Result<Option<prost_types::Duration>, String> {
    match value.get("timeout") {
        None | Some(Value::Null) => Ok(None),
        Some(timeout) => Ok(Some(prost_types::Duration {
            seconds: timeout.get("seconds").and_then(Value::as_i64).unwrap_or(0),
            nanos: timeout.get("nanos").and_then(Value::as_i64).unwrap_or(0) as i32,
        })),
    }
}

fn search_request(value: &Value) -> Result<pb::SearchRequest, String> {
    Ok(pb::SearchRequest {
        repo: string(value, "repo")?,
        commit_id: string(value, "commit_id")?,
        query: string(value, "query")?,
        is_reg_exp: bool_field(value, "is_reg_exp")?,
        is_case_sensitive: bool_field(value, "is_case_sensitive")?,
        include_patterns: strings(value, "include_patterns")?,
        exclude_pattern: optional_string(value, "exclude_pattern")?,
        first: i32_field(value, "first")?,
        timeout: duration(value)?,
        include_langs: strings(value, "include_langs")?,
        exclude_langs: strings(value, "exclude_langs")?,
    })
}

fn range_json(range: &pb::Range) -> Value {
    json!({"row": range.row, "column": range.column, "length": range.length})
}

fn range_key(range: &pb::Range) -> (i32, i32, i32) {
    (range.row, range.column, range.length)
}

fn search_symbol_json(symbol: &pb::search_response::Symbol) -> Value {
    json!({
        "name": symbol.name,
        "path": symbol.path,
        "line": symbol.line,
        "character": symbol.character,
        "kind": symbol.kind,
        "language": symbol.language,
        "parent": symbol.parent,
        "parent_kind": symbol.parent_kind,
        "signature": symbol.signature,
        "file_limited": symbol.file_limited,
    })
}

fn normalize_search_response(response: &pb::SearchResponse) -> Value {
    let mut symbols: Vec<Value> = response.symbols.iter().map(search_symbol_json).collect();
    symbols.sort_by(|left, right| {
        let key = |value: &Value| {
            (
                value["path"].as_str().unwrap_or_default().to_owned(),
                value["line"].as_i64().unwrap_or_default(),
                value["character"].as_i64().unwrap_or_default(),
                value["name"].as_str().unwrap_or_default().to_owned(),
                value["kind"].as_str().unwrap_or_default().to_owned(),
            )
        };
        key(left).cmp(&key(right))
    });
    json!({
        "symbols": symbols,
        "error": response.error,
        "limit_hit": response.limit_hit,
    })
}

fn local_symbol_json(symbol: &pb::local_code_intel_response::Symbol) -> Value {
    let def = symbol
        .def
        .as_ref()
        .map(range_json)
        .unwrap_or_else(|| json!(null));
    let mut refs: Vec<(i32, i32, i32)> = symbol.refs.iter().map(range_key).collect();
    refs.sort_unstable();
    let refs: Vec<Value> = refs
        .into_iter()
        .map(|(row, column, length)| json!({"row": row, "column": column, "length": length}))
        .collect();
    json!({"name": symbol.name, "hover": symbol.hover, "def": def, "refs": refs})
}

/// Flatten streamed chunks and sort, because both implementations derive the
/// emission order from hash maps and chunk boundaries are not stable.
fn normalize_local_code_intel(messages: &[pb::LocalCodeIntelResponse]) -> Value {
    let mut symbols: Vec<Value> = messages
        .iter()
        .flat_map(|message| message.symbols.iter())
        .map(local_symbol_json)
        .collect();
    symbols.sort_by(|left, right| {
        let key = |value: &Value| {
            (
                value["name"].as_str().unwrap_or_default().to_owned(),
                value["def"]["row"].as_i64().unwrap_or_default(),
                value["def"]["column"].as_i64().unwrap_or_default(),
                value["def"]["length"].as_i64().unwrap_or_default(),
                value["hover"].as_str().unwrap_or_default().to_owned(),
            )
        };
        key(left).cmp(&key(right))
    });
    json!({ "symbols": symbols })
}

fn normalize_symbol_info(response: &pb::SymbolInfoResponse) -> Value {
    let Some(result) = &response.result else {
        return json!({"result": null});
    };
    let definition = result
        .definition
        .as_ref()
        .map(|definition| {
            json!({
                "repo_commit_path": definition.repo_commit_path.as_ref().map(|path| json!({
                    "repo": path.repo,
                    "commit": path.commit,
                    "path": path.path,
                })),
                "range": definition.range.as_ref().map(range_json),
            })
        })
        .unwrap_or_else(|| json!(null));
    json!({
        "result": {
            "definition": definition,
            "hover": result.hover,
        }
    })
}

// ---------------------------------------------------------------------------
// searcher request/response JSON
// ---------------------------------------------------------------------------

fn u32_field(value: &Value, field: &str) -> Result<u32, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(0),
        Some(number) => number
            .as_u64()
            .and_then(|number| u32::try_from(number).ok())
            .ok_or_else(|| format!("request: {field} must be a uint32")),
    }
}

fn searcher_duration(value: &Value) -> Result<Option<prost_types::Duration>, String> {
    match value.get("fetch_timeout") {
        None | Some(Value::Null) => Ok(None),
        Some(timeout) => Ok(Some(prost_types::Duration {
            seconds: timeout.get("seconds").and_then(Value::as_i64).unwrap_or(0),
            nanos: timeout.get("nanos").and_then(Value::as_i64).unwrap_or(0) as i32,
        })),
    }
}

fn searcher_query_nodes(value: &Value) -> Result<Vec<spb::QueryNode>, String> {
    let items = value
        .as_array()
        .ok_or_else(|| "request: query children must be an array".to_owned())?;
    items.iter().map(searcher_query_node).collect()
}

fn searcher_query_node(value: &Value) -> Result<spb::QueryNode, String> {
    let node = if let Some(pattern) = value.get("pattern") {
        spb::query_node::Value::Pattern(spb::PatternNode {
            value: string(pattern, "value")?,
            is_negated: bool_field(pattern, "is_negated")?,
            is_regexp: bool_field(pattern, "is_regexp")?,
            boost: bool_field(pattern, "boost")?,
        })
    } else if let Some(children) = value.get("and") {
        spb::query_node::Value::And(spb::AndNode {
            children: searcher_query_nodes(children)?,
        })
    } else if let Some(children) = value.get("or") {
        spb::query_node::Value::Or(spb::OrNode {
            children: searcher_query_nodes(children)?,
        })
    } else {
        return Err("request: query node must be one of pattern, and, or".to_owned());
    };
    Ok(spb::QueryNode { value: Some(node) })
}

fn searcher_pattern_info(value: &Value) -> Result<spb::PatternInfo, String> {
    let query = value
        .get("query")
        .ok_or_else(|| "request: pattern_info.query is required".to_owned())?;
    Ok(spb::PatternInfo {
        is_structural: bool_field(value, "is_structural")?,
        is_case_sensitive: bool_field(value, "is_case_sensitive")?,
        exclude_pattern: optional_string(value, "exclude_pattern")?,
        include_patterns: strings(value, "include_patterns")?,
        path_patterns_are_case_sensitive: bool_field(value, "path_patterns_are_case_sensitive")?,
        limit: i64_field(value, "limit")?,
        pattern_matches_content: bool_field(value, "pattern_matches_content")?,
        pattern_matches_path: bool_field(value, "pattern_matches_path")?,
        comby_rule: optional_string(value, "comby_rule")?,
        languages: strings(value, "languages")?,
        select: optional_string(value, "select")?,
        query: Some(searcher_query_node(query)?),
        include_langs: strings(value, "include_langs")?,
        exclude_langs: strings(value, "exclude_langs")?,
    })
}

fn searcher_request(value: &Value) -> Result<spb::SearchRequest, String> {
    let pattern_info = value
        .get("pattern_info")
        .ok_or_else(|| "request: missing pattern_info".to_owned())?;
    Ok(spb::SearchRequest {
        repo: string(value, "repo")?,
        repo_id: u32_field(value, "repo_id")?,
        commit_oid: string(value, "commit_oid")?,
        indexed: bool_field(value, "indexed")?,
        pattern_info: Some(searcher_pattern_info(pattern_info)?),
        branch: optional_string(value, "branch")?,
        fetch_timeout: searcher_duration(value)?,
        num_context_lines: i32_field(value, "num_context_lines")?,
    })
}

fn i64_field(value: &Value, field: &str) -> Result<i64, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(0),
        Some(number) => number
            .as_i64()
            .ok_or_else(|| format!("request: {field} must be an int64")),
    }
}

fn location_json(location: &spb::Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn chunk_json(chunk: &spb::ChunkMatch) -> Value {
    let mut ranges: Vec<Value> = chunk
        .ranges
        .iter()
        .map(|range| {
            json!({
                "start": range.start.as_ref().map(location_json),
                "end": range.end.as_ref().map(location_json),
            })
        })
        .collect();
    // Ranges are emitted in match order, which follows the worker's scan but
    // can interleave for AND/OR queries; sort for a stable comparison.
    ranges.sort_by_key(|range| range["start"]["offset"].as_i64().unwrap_or_default());
    json!({
        "content": String::from_utf8_lossy(&chunk.content),
        "content_start": chunk.content_start.as_ref().map(location_json),
        "ranges": ranges,
    })
}

fn file_match_json(file_match: &spb::FileMatch) -> Value {
    let mut chunks: Vec<Value> = file_match.chunk_matches.iter().map(chunk_json).collect();
    chunks.sort_by_key(|chunk| chunk["content_start"]["offset"].as_i64().unwrap_or_default());
    json!({
        "path": String::from_utf8_lossy(&file_match.path),
        "language": String::from_utf8_lossy(&file_match.language),
        "limit_hit": file_match.limit_hit,
        "chunks": chunks,
    })
}

/// Collect the streaming events into one document. File matches are sorted
/// because parallel workers and path order make emission order unstable.
fn normalize_searcher_response(messages: &[spb::SearchResponse]) -> Value {
    let mut files = Vec::new();
    let mut done = Value::Null;
    for message in messages {
        match &message.message {
            Some(spb::search_response::Message::FileMatch(file_match)) => {
                files.push(file_match_json(file_match));
            }
            Some(spb::search_response::Message::DoneMessage(done_message)) => {
                done = json!({ "limit_hit": done_message.limit_hit });
            }
            None => {}
        }
    }
    files.sort_by(|left, right| {
        let key = |value: &Value| {
            (
                value["path"].as_str().unwrap_or_default().to_owned(),
                value["chunks"][0]["content_start"]["offset"]
                    .as_i64()
                    .unwrap_or_default(),
                value["chunks"]
                    .as_array()
                    .map(Vec::len)
                    .unwrap_or_default(),
            )
        };
        key(left).cmp(&key(right))
    });
    json!({ "file_matches": files, "done": done })
}

// ---------------------------------------------------------------------------
// RPC calls
// ---------------------------------------------------------------------------

async fn call_search(
    client: &mut pb::symbols_service_client::SymbolsServiceClient<tonic::transport::Channel>,
    request: &Value,
) -> Result<Value, String> {
    let response = client
        .search(search_request(request)?)
        .await
        .map_err(|err| format!("Search RPC failed: {err}"))?
        .into_inner();
    Ok(normalize_search_response(&response))
}

async fn call_local_code_intel(
    client: &mut pb::symbols_service_client::SymbolsServiceClient<tonic::transport::Channel>,
    request: &Value,
) -> Result<Value, String> {
    let mut stream = client
        .local_code_intel(pb::LocalCodeIntelRequest {
            repo_commit_path: Some(repo_commit_path(request)?),
        })
        .await
        .map_err(|err| format!("LocalCodeIntel RPC failed: {err}"))?
        .into_inner();
    let mut messages = Vec::new();
    while let Some(message) = stream
        .message()
        .await
        .map_err(|err| format!("LocalCodeIntel stream failed: {err}"))?
    {
        messages.push(message);
    }
    Ok(normalize_local_code_intel(&messages))
}

async fn call_symbol_info(
    client: &mut pb::symbols_service_client::SymbolsServiceClient<tonic::transport::Channel>,
    request: &Value,
) -> Result<Value, String> {
    let response = client
        .symbol_info(pb::SymbolInfoRequest {
            repo_commit_path: Some(repo_commit_path(request)?),
            point: Some(point(request)?),
        })
        .await
        .map_err(|err| format!("SymbolInfo RPC failed: {err}"))?
        .into_inner();
    Ok(normalize_symbol_info(&response))
}

async fn call_symbols(
    client: &mut pb::symbols_service_client::SymbolsServiceClient<tonic::transport::Channel>,
    rpc: Rpc,
    request: &Value,
) -> Result<Value, String> {
    match rpc {
        Rpc::Search => call_search(client, request).await,
        Rpc::LocalCodeIntel => call_local_code_intel(client, request).await,
        Rpc::SymbolInfo => call_symbol_info(client, request).await,
    }
}

async fn call_searcher(
    client: &mut spb::searcher_service_client::SearcherServiceClient<tonic::transport::Channel>,
    request: &Value,
) -> Result<Value, String> {
    let mut stream = client
        .search(searcher_request(request)?)
        .await
        .map_err(|err| format!("Search RPC failed: {err}"))?
        .into_inner();
    let mut messages = Vec::new();
    while let Some(message) = stream
        .message()
        .await
        .map_err(|err| format!("Search stream failed: {err}"))?
    {
        messages.push(message);
    }
    Ok(normalize_searcher_response(&messages))
}

/// Replay every fixture against one endpoint. All cases must belong to
/// `service`; callers pass that same name from `--service`.
async fn evaluate(service: &str, addr: &str, cases: &[FixtureCase]) -> Result<Vec<Value>, String> {
    let mut results = Vec::with_capacity(cases.len());
    match service {
        "symbols" => {
            let mut client = pb::symbols_service_client::SymbolsServiceClient::connect(
                addr.to_owned(),
            )
            .await
            .map_err(|err| format!("connect {addr}: {err}"))?;
            for case in cases {
                let FixtureService::Symbols(rpc) = case.service else {
                    return Err(format!(
                        "{}: fixture targets searcher, not symbols",
                        case.path.display()
                    ));
                };
                results.push(call_symbols(&mut client, rpc, &case.request).await?);
            }
        }
        "searcher" => {
            let mut client = spb::searcher_service_client::SearcherServiceClient::connect(
                addr.to_owned(),
            )
            .await
            .map_err(|err| format!("connect {addr}: {err}"))?;
            for case in cases {
                if case.service != FixtureService::Searcher {
                    return Err(format!(
                        "{}: fixture targets symbols, not searcher",
                        case.path.display()
                    ));
                }
                results.push(call_searcher(&mut client, &case.request).await?);
            }
        }
        other => {
            return Err(format!(
                "unsupported service {other:?} (symbols or searcher)"
            ))
        }
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// diff / record
// ---------------------------------------------------------------------------

fn default_addr(service: &str) -> &'static str {
    match service {
        "searcher" => "http://127.0.0.1:3181",
        _ => "http://127.0.0.1:3184",
    }
}

fn load_cases(fixture_dir: &Path, service: &str) -> Result<Vec<FixtureCase>, String> {
    let cases = collect_fixtures(fixture_dir)?
        .iter()
        .map(|path| load_fixture(path))
        .collect::<Result<Vec<_>, _>>()?;
    for case in &cases {
        if case.service.name() != service {
            return Err(format!(
                "{}: fixture targets {}, but --service is {service}",
                case.path.display(),
                case.service.name()
            ));
        }
    }
    Ok(cases)
}

#[tokio::main]
async fn diff(args: &[String]) -> Result<(), String> {
    let args = parse_args(args)?;
    let service = required(&args, "service")?;
    if !matches!(service, "symbols" | "searcher") {
        return Err(format!(
            "diff: unsupported service {service:?} (symbols or searcher)"
        ));
    }
    let fixture_dir = PathBuf::from(required(&args, "fixture")?);
    let addr = args
        .get("addr")
        .cloned()
        .unwrap_or_else(|| default_addr(service).to_owned());

    let cases = load_cases(&fixture_dir, service)?;
    let actuals = evaluate(service, &addr, &cases).await?;

    let mut failures = 0usize;
    for (case, actual) in cases.iter().zip(&actuals) {
        let expected = case
            .expected
            .as_ref()
            .ok_or_else(|| format!("{}: missing expected response", case.path.display()))?;
        if actual == expected {
            println!("PASS {}", relative(&case.path));
        } else {
            failures += 1;
            eprintln!("FAIL {}", relative(&case.path));
            eprintln!("  expected: {}", compact(expected));
            eprintln!("  actual:   {}", compact(actual));
        }
    }

    if failures > 0 {
        return Err(format!("{failures} fixture(s) differ"));
    }
    println!("diff: all fixtures match");
    Ok(())
}

#[tokio::main]
async fn record(args: &[String]) -> Result<(), String> {
    let args = parse_args(args)?;
    let service = required(&args, "service")?;
    if !matches!(service, "symbols" | "searcher") {
        return Err(format!(
            "record: unsupported service {service:?} (symbols or searcher)"
        ));
    }
    let fixture_dir = PathBuf::from(required(&args, "fixture")?);
    let addr = required(&args, "addr")?.to_owned();

    let cases = load_cases(&fixture_dir, service)?;
    let actuals = evaluate(service, &addr, &cases).await?;

    for (case, actual) in cases.iter().zip(actuals) {
        let mut document: Value = serde_json::from_slice(
            &fs::read(&case.path)
                .map_err(|err| format!("read {}: {err}", case.path.display()))?,
        )
        .map_err(|err| format!("parse {}: {err}", case.path.display()))?;
        document["expected"] = actual;
        let serialized = serde_json::to_string_pretty(&document)
            .map_err(|err| format!("serialize {}: {err}", case.path.display()))?;
        fs::write(&case.path, format!("{serialized}\n"))
            .map_err(|err| format!("write {}: {err}", case.path.display()))?;
        println!("RECORDED {}", relative(&case.path));
    }
    Ok(())
}

fn relative(path: &Path) -> String {
    path.strip_prefix(workspace_root().unwrap_or_default())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(offset: i32) -> spb::Location {
        spb::Location {
            offset,
            line: 0,
            column: offset,
        }
    }

    fn file(path: &str, chunk_offsets: &[i32]) -> spb::FileMatch {
        spb::FileMatch {
            path: path.as_bytes().to_vec(),
            chunk_matches: chunk_offsets
                .iter()
                .map(|&offset| spb::ChunkMatch {
                    content: b"x".to_vec(),
                    content_start: Some(location(offset)),
                    ranges: vec![spb::Range {
                        start: Some(location(offset)),
                        end: Some(location(offset + 1)),
                    }],
                })
                .collect(),
            limit_hit: false,
            language: b"Go".to_vec(),
        }
    }

    #[test]
    fn normalizes_and_sorts_stream_events() {
        let messages = vec![
            spb::SearchResponse {
                message: Some(spb::search_response::Message::FileMatch(file("b", &[30]))),
            },
            spb::SearchResponse {
                message: Some(spb::search_response::Message::DoneMessage(
                    spb::search_response::Done { limit_hit: true },
                )),
            },
            spb::SearchResponse {
                message: Some(spb::search_response::Message::FileMatch(file(
                    "a",
                    &[20, 5],
                ))),
            },
        ];
        let normalized = normalize_searcher_response(&messages);
        assert_eq!(normalized["done"]["limit_hit"], json!(true));
        assert_eq!(normalized["file_matches"][0]["path"], json!("a"));
        assert_eq!(
            normalized["file_matches"][0]["chunks"][0]["content_start"]["offset"],
            json!(5)
        );
        assert_eq!(normalized["file_matches"][1]["path"], json!("b"));
    }

    #[test]
    fn parses_boolean_query_nodes() {
        let value = json!({
            "and": [
                {"pattern": {"value": "a", "is_regexp": true}},
                {"or": [{"pattern": {"value": "b"}}]},
            ]
        });
        let node = searcher_query_node(&value).expect("query");
        let Some(spb::query_node::Value::And(and)) = node.value else {
            panic!("expected an and node");
        };
        assert_eq!(and.children.len(), 2);
        assert!(matches!(
            and.children[1].value,
            Some(spb::query_node::Value::Or(_))
        ));
    }
}
