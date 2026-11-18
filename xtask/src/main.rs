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

use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use sg_proto::gitserver as gpb;
use sg_proto::repoupdater as rpb;
use sg_proto::searcher as spb;
use sg_proto::symbols as pb;

/// Baseline commit the snapshot was taken from.
const BASELINE_COMMIT: &str = "c864f15af264f0f456a6d8a83290b5c940715349";

const PROTO_SERVICES: &[&str] = &["symbols", "searcher", "gitserver", "repoupdater"];

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
        "replay-write" => replay_write(&rest),
        "canary" => canary(&rest),
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
  replay-write --addr URL --repo NAME --phase clone|fetch|delete
                                  run one gitserver write scenario and print it
                                  as normalized JSON (for dev/rs/scripts/gitserver)
  canary --addr URL --repo NAME   health check: clone progress, HEAD, repo list
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
    let repoupdater = read_proto(&snapshot.join("repoupdater/v1/repoupdater.proto"))?;
    for rpc in [
        "RepoUpdateSchedulerInfo",
        "EnqueueRepoUpdate",
        "RecloneRepository",
        "EnqueueChangesetSync",
    ] {
        if !repoupdater.contains(&format!("rpc {rpc}(")) {
            return Err(format!("repoupdater proto is missing rpc {rpc}"));
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

/// Read-path gitserver RPCs covered by the first M9 fixtures.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GitserverRpc {
    Archive,
    ReadFile,
    ChangedFiles,
    ResolveRevision,
    GetObject,
    DefaultBranch,
    MergeBase,
    Stat,
    ReadDir,
    ListRefs,
    GetCommit,
    RevAtTime,
    RawDiff,
    CommitLog,
    MergeBaseOctopus,
    ContributorCounts,
    FirstEverCommit,
    BehindAhead,
    Blame,
    DiskInfo,
    RepoCloneProgress,
    Search,
}

impl GitserverRpc {
    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "archive" => Ok(Self::Archive),
            "read_file" => Ok(Self::ReadFile),
            "changed_files" => Ok(Self::ChangedFiles),
            "resolve_revision" => Ok(Self::ResolveRevision),
            "get_object" => Ok(Self::GetObject),
            "default_branch" => Ok(Self::DefaultBranch),
            "merge_base" => Ok(Self::MergeBase),
            "stat" => Ok(Self::Stat),
            "read_dir" => Ok(Self::ReadDir),
            "list_refs" => Ok(Self::ListRefs),
            "get_commit" => Ok(Self::GetCommit),
            "rev_at_time" => Ok(Self::RevAtTime),
            "raw_diff" => Ok(Self::RawDiff),
            "commit_log" => Ok(Self::CommitLog),
            "merge_base_octopus" => Ok(Self::MergeBaseOctopus),
            "contributor_counts" => Ok(Self::ContributorCounts),
            "first_ever_commit" => Ok(Self::FirstEverCommit),
            "behind_ahead" => Ok(Self::BehindAhead),
            "blame" => Ok(Self::Blame),
            "disk_info" => Ok(Self::DiskInfo),
            "repo_clone_progress" => Ok(Self::RepoCloneProgress),
            "search" => Ok(Self::Search),
            other => Err(format!(
                "unknown gitserver rpc {other:?} (archive, read_file, changed_files, \
                 resolve_revision, get_object, default_branch, merge_base, stat, read_dir, \
                 list_refs, get_commit, rev_at_time, raw_diff, commit_log, \
                 merge_base_octopus, contributor_counts, first_ever_commit, behind_ahead, \
                 blame, disk_info, repo_clone_progress, search)"
            )),
        }
    }
}

/// Which service a fixture targets. `searcher` has a single streaming RPC, so
/// its `rpc` is fixed rather than an enum.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FixtureService {
    Symbols(Rpc),
    Searcher,
    Gitserver(GitserverRpc),
    Repoupdater(RepoUpdaterRpc),
}

impl FixtureService {
    fn name(self) -> &'static str {
        match self {
            Self::Symbols(_) => "symbols",
            Self::Searcher => "searcher",
            Self::Gitserver(_) => "gitserver",
            Self::Repoupdater(_) => "repoupdater",
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
        "gitserver" => FixtureService::Gitserver(GitserverRpc::parse(rpc)?),
        "repoupdater" => FixtureService::Repoupdater(RepoUpdaterRpc::parse(rpc)?),
        other => {
            return Err(format!(
                "{}: unsupported service {other:?} (symbols, searcher, gitserver or repoupdater)",
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
    chunks.sort_by_key(|chunk| {
        chunk["content_start"]["offset"]
            .as_i64()
            .unwrap_or_default()
    });
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
                value["chunks"].as_array().map(Vec::len).unwrap_or_default(),
            )
        };
        key(left).cmp(&key(right))
    });
    json!({ "file_matches": files, "done": done })
}

// ---------------------------------------------------------------------------
// gitserver request/response JSON
// ---------------------------------------------------------------------------

fn gitserver_archive_request(value: &Value) -> Result<gpb::ArchiveRequest, String> {
    let format = match value.get("format").and_then(Value::as_str) {
        None | Some("zip") => gpb::ArchiveFormat::Zip as i32,
        Some("tar") => gpb::ArchiveFormat::Tar as i32,
        Some(other) => return Err(format!("request: unknown archive format {other:?}")),
    };
    Ok(gpb::ArchiveRequest {
        repo: string(value, "repo")?,
        treeish: string(value, "treeish")?,
        format,
        paths: strings(value, "paths")?
            .into_iter()
            .map(String::into_bytes)
            .collect(),
    })
}

fn gitserver_read_file_request(value: &Value) -> Result<gpb::ReadFileRequest, String> {
    Ok(gpb::ReadFileRequest {
        repo_name: string(value, "repo_name")?,
        commit: string(value, "commit")?,
        path: string(value, "path")?.into_bytes(),
    })
}

fn gitserver_changed_files_request(value: &Value) -> Result<gpb::ChangedFilesRequest, String> {
    let base = match value.get("base") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone().into_bytes()),
        Some(_) => return Err("request: base must be a string".to_owned()),
    };
    Ok(gpb::ChangedFilesRequest {
        repo_name: string(value, "repo_name")?,
        base,
        head: string(value, "head")?.into_bytes(),
    })
}

/// Archive bytes are compared by digest: stream chunk boundaries depend on
/// buffer sizes and are not part of the contract.
fn normalize_archive(chunks: &[gpb::ArchiveResponse]) -> Value {
    let mut hasher = Sha256::new();
    let mut size = 0usize;
    for chunk in chunks {
        hasher.update(&chunk.data);
        size += chunk.data.len();
    }
    json!({
        "sha256": format!("{:x}", hasher.finalize()),
        "size_bytes": size,
    })
}

fn normalize_read_file(chunks: &[gpb::ReadFileResponse]) -> Value {
    let mut data = Vec::new();
    for chunk in chunks {
        data.extend_from_slice(&chunk.data);
    }
    json!({
        "data": String::from_utf8_lossy(&data),
        "size_bytes": data.len(),
    })
}

fn normalize_changed_files(chunks: &[gpb::ChangedFilesResponse]) -> Value {
    let mut files: Vec<(String, &'static str)> = chunks
        .iter()
        .flat_map(|chunk| chunk.files.iter())
        .map(|file| {
            let status = match gpb::changed_file::Status::try_from(file.status) {
                Ok(gpb::changed_file::Status::Added) => "added",
                Ok(gpb::changed_file::Status::Modified) => "modified",
                Ok(gpb::changed_file::Status::Deleted) => "deleted",
                Ok(gpb::changed_file::Status::TypeChanged) => "type_changed",
                Ok(gpb::changed_file::Status::Unspecified) | Err(_) => "unspecified",
            };
            (String::from_utf8_lossy(&file.path).into_owned(), status)
        })
        .collect();
    files.sort();
    json!({
        "files": files
            .into_iter()
            .map(|(path, status)| json!({"path": path, "status": status}))
            .collect::<Vec<_>>(),
    })
}

fn gitserver_resolve_revision_request(
    value: &Value,
) -> Result<gpb::ResolveRevisionRequest, String> {
    Ok(gpb::ResolveRevisionRequest {
        repo_name: string(value, "repo_name")?,
        rev_spec: optional_string(value, "rev_spec")?.into_bytes(),
        ensure_revision: value.get("ensure_revision").and_then(Value::as_bool),
    })
}

fn gitserver_get_object_request(value: &Value) -> Result<gpb::GetObjectRequest, String> {
    Ok(gpb::GetObjectRequest {
        repo: string(value, "repo")?,
        object_name: string(value, "object_name")?,
    })
}

fn gitserver_default_branch_request(value: &Value) -> Result<gpb::DefaultBranchRequest, String> {
    Ok(gpb::DefaultBranchRequest {
        repo_name: string(value, "repo_name")?,
        short_ref: value
            .get("short_ref")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn gitserver_merge_base_request(value: &Value) -> Result<gpb::MergeBaseRequest, String> {
    Ok(gpb::MergeBaseRequest {
        repo_name: string(value, "repo_name")?,
        base: string(value, "base")?.into_bytes(),
        head: string(value, "head")?.into_bytes(),
    })
}

fn gitserver_stat_request(value: &Value) -> Result<gpb::StatRequest, String> {
    Ok(gpb::StatRequest {
        repo_name: string(value, "repo_name")?,
        commit_sha: string(value, "commit_sha")?,
        path: optional_string(value, "path")?.into_bytes(),
    })
}

fn gitserver_read_dir_request(value: &Value) -> Result<gpb::ReadDirRequest, String> {
    Ok(gpb::ReadDirRequest {
        repo_name: string(value, "repo_name")?,
        commit_sha: string(value, "commit_sha")?,
        path: match value.get("path") {
            None | Some(Value::Null) => None,
            Some(Value::String(text)) => Some(text.clone().into_bytes()),
            Some(_) => return Err("request: path must be a string".to_owned()),
        },
        recursive: value
            .get("recursive")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn gitserver_list_refs_request(value: &Value) -> Result<gpb::ListRefsRequest, String> {
    Ok(gpb::ListRefsRequest {
        repo_name: string(value, "repo_name")?,
        heads_only: value
            .get("heads_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        tags_only: value
            .get("tags_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        points_at_commit: strings(value, "points_at_commit")?,
        contains_sha: match value.get("contains_sha") {
            None | Some(Value::Null) => None,
            Some(Value::String(text)) => Some(text.clone()),
            Some(_) => return Err("request: contains_sha must be a string".to_owned()),
        },
    })
}

fn gitserver_get_commit_request(value: &Value) -> Result<gpb::GetCommitRequest, String> {
    Ok(gpb::GetCommitRequest {
        repo_name: string(value, "repo_name")?,
        commit: string(value, "commit")?,
        include_modified_files: value
            .get("include_modified_files")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// A protobuf timestamp given as `{"seconds": n, "nanos": n}`; `None` when the
/// field is absent or null.
fn optional_timestamp(
    value: &Value,
    field: &str,
) -> Result<Option<prost_types::Timestamp>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(object) => Ok(Some(prost_types::Timestamp {
            seconds: object
                .get("seconds")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("request: {field}.seconds must be an integer"))?,
            nanos: object
                .get("nanos")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                .try_into()
                .map_err(|_| format!("request: {field}.nanos out of range"))?,
        })),
    }
}

fn gitserver_rev_at_time_request(value: &Value) -> Result<gpb::RevAtTimeRequest, String> {
    Ok(gpb::RevAtTimeRequest {
        repo_name: string(value, "repo_name")?,
        rev_spec: optional_string(value, "rev_spec")?.into_bytes(),
        time: optional_timestamp(value, "time")?,
    })
}

fn gitserver_raw_diff_request(value: &Value) -> Result<gpb::RawDiffRequest, String> {
    let comparison_type = match string(value, "comparison_type")?.as_str() {
        "unspecified" => gpb::raw_diff_request::ComparisonType::Unspecified,
        "only_in_head" => gpb::raw_diff_request::ComparisonType::OnlyInHead,
        "intersection" => gpb::raw_diff_request::ComparisonType::Intersection,
        other => return Err(format!("request: unknown comparison_type {other:?}")),
    };
    Ok(gpb::RawDiffRequest {
        repo_name: string(value, "repo_name")?,
        base_rev_spec: string(value, "base_rev_spec")?.into_bytes(),
        head_rev_spec: string(value, "head_rev_spec")?.into_bytes(),
        comparison_type: comparison_type as i32,
        paths: strings(value, "paths")?
            .into_iter()
            .map(String::into_bytes)
            .collect(),
        inter_hunk_context: optional_u32(value, "inter_hunk_context")?,
        context_lines: optional_u32(value, "context_lines")?,
    })
}

fn optional_u32(value: &Value, field: &str) -> Result<Option<u32>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(number) => number
            .as_u64()
            .and_then(|number| u32::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| format!("request: {field} must be a u32")),
    }
}

fn gitserver_commit_log_request(value: &Value) -> Result<gpb::CommitLogRequest, String> {
    let order = match value
        .get("order")
        .and_then(Value::as_str)
        .unwrap_or("unspecified")
    {
        "unspecified" | "default" => gpb::commit_log_request::CommitLogOrder::Unspecified,
        "commit_date" => gpb::commit_log_request::CommitLogOrder::CommitDate,
        "topo_date" => gpb::commit_log_request::CommitLogOrder::TopoDate,
        other => return Err(format!("request: unknown order {other:?}")),
    };
    Ok(gpb::CommitLogRequest {
        repo_name: string(value, "repo_name")?,
        ranges: strings(value, "ranges")?
            .into_iter()
            .map(String::into_bytes)
            .collect(),
        all_refs: value
            .get("all_refs")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        after: optional_timestamp(value, "after")?,
        before: optional_timestamp(value, "before")?,
        max_commits: value
            .get("max_commits")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .try_into()
            .map_err(|_| "request: max_commits out of range".to_owned())?,
        skip: value
            .get("skip")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .try_into()
            .map_err(|_| "request: skip out of range".to_owned())?,
        follow_only_first_parent: value
            .get("follow_only_first_parent")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        include_modified_files: value
            .get("include_modified_files")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        order: order as i32,
        message_query: optional_string(value, "message_query")?.into_bytes(),
        author_query: optional_string(value, "author_query")?.into_bytes(),
        path: optional_string(value, "path")?.into_bytes(),
        follow_path_renames: value
            .get("follow_path_renames")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn gitserver_merge_base_octopus_request(
    value: &Value,
) -> Result<gpb::MergeBaseOctopusRequest, String> {
    Ok(gpb::MergeBaseOctopusRequest {
        repo_name: string(value, "repo_name")?,
        revspecs: strings(value, "revspecs")?
            .into_iter()
            .map(String::into_bytes)
            .collect(),
    })
}

fn gitserver_contributor_counts_request(
    value: &Value,
) -> Result<gpb::ContributorCountsRequest, String> {
    Ok(gpb::ContributorCountsRequest {
        repo_name: string(value, "repo_name")?,
        range: optional_string(value, "range")?.into_bytes(),
        after: optional_timestamp(value, "after")?,
        path: optional_string(value, "path")?.into_bytes(),
    })
}

fn gitserver_first_ever_commit_request(
    value: &Value,
) -> Result<gpb::FirstEverCommitRequest, String> {
    Ok(gpb::FirstEverCommitRequest {
        repo_name: string(value, "repo_name")?,
    })
}

fn gitserver_behind_ahead_request(value: &Value) -> Result<gpb::BehindAheadRequest, String> {
    Ok(gpb::BehindAheadRequest {
        repo_name: string(value, "repo_name")?,
        left: optional_string(value, "left")?.into_bytes(),
        right: optional_string(value, "right")?.into_bytes(),
    })
}

/// `DiskInfoRequest` has no fields.
fn gitserver_disk_info_request(_value: &Value) -> Result<gpb::DiskInfoRequest, String> {
    Ok(gpb::DiskInfoRequest {})
}

fn gitserver_repo_clone_progress_request(
    value: &Value,
) -> Result<gpb::RepoCloneProgressRequest, String> {
    // The empty name is a valid request that the server rejects.
    Ok(gpb::RepoCloneProgressRequest {
        repo_name: optional_string(value, "repo_name")?,
    })
}

fn gitserver_search_request(value: &Value) -> Result<gpb::SearchRequest, String> {
    Ok(gpb::SearchRequest {
        repo: optional_string(value, "repo")?,
        revisions: strings(value, "revisions")?
            .into_iter()
            .map(|rev_spec| gpb::RevisionSpecifier { rev_spec })
            .collect(),
        limit: value.get("limit").and_then(Value::as_i64).unwrap_or(0),
        include_diff: value
            .get("include_diff")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        include_modified_files: value
            .get("include_modified_files")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        query: match value.get("query") {
            None | Some(Value::Null) => None,
            Some(node) => Some(gitserver_query_node(node)?),
        },
    })
}

/// `{"author_matches": {"expr": …, "ignore_case": …}}` and friends.
fn gitserver_query_node(value: &Value) -> Result<gpb::QueryNode, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "request: query node must be an object".to_owned())?;
    let node = if let Some(node) = object.get("author_matches") {
        let (expr, ignore_case) = gitserver_regexp_fields(node)?;
        gpb::query_node::Value::AuthorMatches(gpb::AuthorMatchesNode { expr, ignore_case })
    } else if let Some(node) = object.get("committer_matches") {
        let (expr, ignore_case) = gitserver_regexp_fields(node)?;
        gpb::query_node::Value::CommitterMatches(gpb::CommitterMatchesNode { expr, ignore_case })
    } else if let Some(node) = object.get("commit_before") {
        gpb::query_node::Value::CommitBefore(gpb::CommitBeforeNode {
            timestamp: optional_timestamp(node, "timestamp")?,
        })
    } else if let Some(node) = object.get("commit_after") {
        gpb::query_node::Value::CommitAfter(gpb::CommitAfterNode {
            timestamp: optional_timestamp(node, "timestamp")?,
        })
    } else if let Some(node) = object.get("message_matches") {
        let (expr, ignore_case) = gitserver_regexp_fields(node)?;
        gpb::query_node::Value::MessageMatches(gpb::MessageMatchesNode { expr, ignore_case })
    } else if let Some(node) = object.get("diff_matches") {
        let (expr, ignore_case) = gitserver_regexp_fields(node)?;
        gpb::query_node::Value::DiffMatches(gpb::DiffMatchesNode { expr, ignore_case })
    } else if let Some(node) = object.get("diff_modifies_file") {
        let (expr, ignore_case) = gitserver_regexp_fields(node)?;
        gpb::query_node::Value::DiffModifiesFile(gpb::DiffModifiesFileNode { expr, ignore_case })
    } else if let Some(node) = object.get("boolean") {
        gpb::query_node::Value::Boolean(gpb::BooleanNode {
            value: node.get("value").and_then(Value::as_bool).unwrap_or(false),
        })
    } else if let Some(node) = object.get("operator") {
        gpb::query_node::Value::Operator(gitserver_operator_node(node)?)
    } else {
        return Err("request: empty query node".to_owned());
    };
    Ok(gpb::QueryNode { value: Some(node) })
}

fn gitserver_regexp_fields(value: &Value) -> Result<(String, bool), String> {
    Ok((
        string(value, "expr")?,
        value
            .get("ignore_case")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}

fn gitserver_operator_node(value: &Value) -> Result<gpb::OperatorNode, String> {
    let kind = match string(value, "kind")?.as_str() {
        "and" => gpb::OperatorKind::And,
        "or" => gpb::OperatorKind::Or,
        "not" => gpb::OperatorKind::Not,
        "unspecified" => gpb::OperatorKind::Unspecified,
        other => return Err(format!("request: unknown operator kind {other:?}")),
    };
    let operands = match value.get("operands") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(gitserver_query_node)
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("request: operands must be an array".to_owned()),
    };
    Ok(gpb::OperatorNode {
        kind: kind as i32,
        operands,
    })
}

fn gitserver_blame_request(value: &Value) -> Result<gpb::BlameRequest, String> {
    let range = match value.get("range") {
        None | Some(Value::Null) => None,
        Some(object) => Some(gpb::BlameRange {
            start_line: object
                .get("start_line")
                .and_then(Value::as_u64)
                .and_then(|line| u32::try_from(line).ok())
                .ok_or_else(|| "request: range.start_line must be a u32".to_owned())?,
            end_line: object
                .get("end_line")
                .and_then(Value::as_u64)
                .and_then(|line| u32::try_from(line).ok())
                .ok_or_else(|| "request: range.end_line must be a u32".to_owned())?,
        }),
    };
    Ok(gpb::BlameRequest {
        repo_name: string(value, "repo_name")?,
        commit: string(value, "commit")?,
        path: optional_string(value, "path")?.into_bytes(),
        ignore_whitespace: value
            .get("ignore_whitespace")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        range,
    })
}

/// `proto.FileInfo` as JSON. Modes are the raw Go `fs.FileMode` bits; the
/// names are compared byte-for-byte.
fn normalize_file_info(file: &gpb::FileInfo) -> Value {
    let submodule = file.submodule.as_ref().map(|submodule| {
        json!({
            "url": submodule.url,
            "path": String::from_utf8_lossy(&submodule.path),
            "commit_sha": submodule.commit_sha,
        })
    });
    json!({
        "name": String::from_utf8_lossy(&file.name),
        "size": file.size,
        "mode": file.mode,
        "blob_oid": file.blob_oid,
        "submodule": submodule,
    })
}

fn normalize_timestamp(timestamp: &prost_types::Timestamp) -> Value {
    json!({"seconds": timestamp.seconds, "nanos": timestamp.nanos})
}

fn normalize_git_ref(git_ref: &gpb::GitRef) -> Value {
    let ref_type = match gpb::git_ref::RefType::try_from(git_ref.ref_type) {
        Ok(gpb::git_ref::RefType::Branch) => "branch",
        Ok(gpb::git_ref::RefType::Tag) => "tag",
        Ok(gpb::git_ref::RefType::Unspecified) | Err(_) => "unspecified",
    };
    json!({
        "ref_name": String::from_utf8_lossy(&git_ref.ref_name),
        "short_ref_name": String::from_utf8_lossy(&git_ref.short_ref_name),
        "target_commit": git_ref.target_commit,
        "ref_oid": git_ref.ref_oid,
        "created_at": git_ref.created_at.as_ref().map(normalize_timestamp),
        "ref_type": ref_type,
        "is_head": git_ref.is_head,
    })
}

fn normalize_git_signature(signature: &gpb::GitSignature) -> Value {
    json!({
        "name": String::from_utf8_lossy(&signature.name),
        "email": String::from_utf8_lossy(&signature.email),
        "date": signature.date.as_ref().map(normalize_timestamp),
    })
}

fn normalize_contributor_count(count: &gpb::ContributorCount) -> Value {
    json!({
        // `ContributorCount.ToProto` never sets a date.
        "author": count.author.as_ref().map(normalize_git_signature),
        "count": count.count,
    })
}

fn normalize_git_commit(commit: &gpb::GitCommit) -> Value {
    json!({
        "oid": commit.oid,
        "author": commit.author.as_ref().map(normalize_git_signature),
        "committer": commit.committer.as_ref().map(normalize_git_signature),
        "message": String::from_utf8_lossy(&commit.message),
        "parents": commit.parents,
    })
}

fn normalize_commit_log(chunks: &[gpb::CommitLogResponse]) -> Value {
    let mut commits = Vec::new();
    for chunk in chunks {
        for response in &chunk.commits {
            commits.push(json!({
                "commit": response.commit.as_ref().map(normalize_git_commit),
                "modified_files": response
                    .modified_files
                    .iter()
                    .map(|file| String::from_utf8_lossy(file).into_owned())
                    .collect::<Vec<_>>(),
            }));
        }
    }
    json!({"commits": commits})
}

fn normalize_blame_hunk(hunk: &gpb::BlameHunk) -> Value {
    json!({
        "start_line": hunk.start_line,
        "end_line": hunk.end_line,
        "start_byte": hunk.start_byte,
        "end_byte": hunk.end_byte,
        "commit": hunk.commit,
        "author": hunk.author.as_ref().map(|author| json!({
            "name": String::from_utf8_lossy(&author.name),
            "email": author.email,
            "date": author.date.as_ref().map(normalize_timestamp),
        })),
        "message": hunk.message,
        "filename": hunk.filename,
        "previous_commit": hunk.previous_commit.as_ref().map(|previous| json!({
            "commit": previous.commit,
            "filename": previous.filename,
        })),
    })
}

fn normalize_blame(chunks: &[gpb::BlameResponse]) -> Value {
    json!({
        "hunks": chunks
            .iter()
            .filter_map(|chunk| chunk.hunk.as_ref())
            .map(normalize_blame_hunk)
            .collect::<Vec<_>>(),
    })
}

fn normalize_search(chunks: &[gpb::SearchResponse]) -> Value {
    let mut matches = Vec::new();
    let mut limit_hit = Value::Null;
    for chunk in chunks {
        match &chunk.message {
            Some(gpb::search_response::Message::Match(message)) => {
                matches.push(normalize_commit_match(message));
            }
            Some(gpb::search_response::Message::LimitHit(hit)) => {
                limit_hit = Value::Bool(*hit);
            }
            None => {}
        }
    }
    json!({"matches": matches, "limit_hit": limit_hit})
}

fn normalize_commit_match(commit_match: &gpb::CommitMatch) -> Value {
    json!({
        "oid": commit_match.oid,
        "author": commit_match.author.as_ref().map(normalize_search_signature),
        "committer": commit_match.committer.as_ref().map(normalize_search_signature),
        "parents": commit_match.parents,
        "refs": commit_match.refs,
        "source_refs": commit_match.source_refs,
        "message": commit_match.message.as_ref().map(normalize_matched_string),
        "diff": commit_match.diff.as_ref().map(normalize_matched_string),
        "modified_files": commit_match.modified_files,
    })
}

fn normalize_search_signature(signature: &gpb::commit_match::Signature) -> Value {
    json!({
        "name": signature.name,
        "email": signature.email,
        "date": signature.date.as_ref().map(normalize_timestamp),
    })
}

fn normalize_matched_string(matched: &gpb::commit_match::MatchedString) -> Value {
    json!({
        "content": matched.content,
        "ranges": matched
            .ranges
            .iter()
            .map(|range| json!({
                "start": range.start.as_ref().map(normalize_location),
                "end": range.end.as_ref().map(normalize_location),
            }))
            .collect::<Vec<_>>(),
    })
}

fn normalize_location(location: &gpb::commit_match::Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

/// `RawDiff` chunks are compared by concatenated text; chunk boundaries are
/// implementation details.
fn normalize_raw_diff(chunks: &[gpb::RawDiffResponse]) -> Value {
    let mut data = Vec::new();
    for chunk in chunks {
        data.extend_from_slice(&chunk.chunk);
    }
    json!({
        "diff": String::from_utf8_lossy(&data),
        "size_bytes": data.len(),
    })
}

/// `DiskInfo` reads the live filesystem, so only the volume size is compared
/// exactly; the free/used split moves with unrelated writes, so it is frozen
/// as invariants (Go and Rust both compute the percentage from the same
/// `statfs` fields in `float32`). The rounded percentage itself is not frozen:
/// unrelated writes to the filesystem move it between record and replay.
fn normalize_disk_info(response: &gpb::DiskInfoResponse) -> Value {
    let total = response.total_space;
    let free = response.free_space;
    let expected = if total > 0 {
        (total - free) as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    json!({
        "total_space": total,
        "free_space_positive": free > 0,
        "free_space_le_total": free <= total,
        "percent_used_matches_formula": (f64::from(response.percent_used) - expected).abs() < 0.05,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize_git_object(object: &gpb::GitObject) -> Value {
    let r#type = match gpb::git_object::ObjectType::try_from(object.r#type) {
        Ok(gpb::git_object::ObjectType::Commit) => "commit",
        Ok(gpb::git_object::ObjectType::Tag) => "tag",
        Ok(gpb::git_object::ObjectType::Tree) => "tree",
        Ok(gpb::git_object::ObjectType::Blob) => "blob",
        Ok(gpb::git_object::ObjectType::Unspecified) | Err(_) => "unspecified",
    };
    json!({"id": hex(&object.id), "type": r#type})
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

/// `repoupdater.v1.RepoUpdaterService` RPCs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RepoUpdaterRpc {
    SchedulerInfo,
    EnqueueRepoUpdate,
    RecloneRepository,
    EnqueueChangesetSync,
}

impl RepoUpdaterRpc {
    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "repo_update_scheduler_info" => Ok(Self::SchedulerInfo),
            "enqueue_repo_update" => Ok(Self::EnqueueRepoUpdate),
            "reclone_repository" => Ok(Self::RecloneRepository),
            "enqueue_changeset_sync" => Ok(Self::EnqueueChangesetSync),
            other => Err(format!("unknown repoupdater rpc {other:?}")),
        }
    }
}

fn normalize_scheduler_info(response: &rpb::RepoUpdateSchedulerInfoResponse) -> Value {
    let schedule = response.schedule.as_ref().map(|schedule| {
        json!({
            "index": schedule.index,
            "total": schedule.total,
            "interval_seconds": schedule.interval_seconds,
            "due": schedule.due.as_ref().map(|due| json!({
                "seconds": due.seconds,
                "nanos": due.nanos,
            })),
        })
    });
    let queue = response.queue.as_ref().map(|queue| {
        json!({
            "index": queue.index,
            "total": queue.total,
            "updating": queue.updating,
            "priority": queue.priority,
        })
    });
    json!({"schedule": schedule, "queue": queue})
}

async fn call_repoupdater(
    client: &mut rpb::repo_updater_service_client::RepoUpdaterServiceClient<
        tonic::transport::Channel,
    >,
    rpc: RepoUpdaterRpc,
    request: &Value,
) -> Result<Value, tonic::Status> {
    match rpc {
        RepoUpdaterRpc::SchedulerInfo => {
            let id = request
                .get("id")
                .and_then(Value::as_i64)
                .unwrap_or_default() as i32;
            let response = client
                .repo_update_scheduler_info(rpb::RepoUpdateSchedulerInfoRequest { id })
                .await?
                .into_inner();
            Ok(normalize_scheduler_info(&response))
        }
        RepoUpdaterRpc::EnqueueRepoUpdate => {
            let repo = request
                .get("repo")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let response = client
                .enqueue_repo_update(rpb::EnqueueRepoUpdateRequest { repo })
                .await?
                .into_inner();
            Ok(json!({"id": response.id, "name": response.name}))
        }
        RepoUpdaterRpc::RecloneRepository => {
            let repo_name = request
                .get("repo_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            client
                .reclone_repository(rpb::RecloneRepositoryRequest { repo_name })
                .await?;
            Ok(json!({}))
        }
        RepoUpdaterRpc::EnqueueChangesetSync => {
            let ids = request
                .get("ids")
                .and_then(Value::as_array)
                .map(|ids| ids.iter().filter_map(Value::as_i64).collect())
                .unwrap_or_default();
            client
                .enqueue_changeset_sync(rpb::EnqueueChangesetSyncRequest { ids })
                .await?;
            Ok(json!({}))
        }
    }
}

/// Why a gitserver replay produced no response: an RPC status (frozen in the
/// fixture as `{"error": {...}}`) or a fixture bug (surfaced as a hard error).
enum GitserverFailure {
    Status(tonic::Status),
    Fixture(String),
}

impl From<tonic::Status> for GitserverFailure {
    fn from(status: tonic::Status) -> Self {
        Self::Status(status)
    }
}

/// Error responses are part of the contract: freeze the gRPC code, message
/// and the decoded `details` (`google.rpc.Status` with `gitserver.v1.*`
/// `Any` payloads) the baseline's client consumes.
fn normalize_status(status: &tonic::Status) -> Value {
    let mut error = json!({
        "code": code_name(status.code()),
        "message": status.message(),
    });
    if !status.details().is_empty() {
        match decode_status_details(status.details()) {
            Some(details) => error["details"] = details,
            None => error["details_hex"] = json!(hex(status.details())),
        }
    }
    json!({"error": error})
}

/// `google.rpc.Status`, the payload of the `grpc-status-details-bin` header.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RepoNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(bool, tag = "2")]
    clone_in_progress: bool,
    #[prost(string, tag = "3")]
    clone_progress: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RevisionNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(string, tag = "2")]
    spec: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FileNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(string, tag = "2")]
    commit: String,
    #[prost(bytes = "vec", tag = "3")]
    path: Vec<u8>,
}

fn decode_status_details(bytes: &[u8]) -> Option<Value> {
    let status = RpcStatus::decode(bytes).ok()?;
    Some(json!({
        "code": status.code,
        "message": status.message,
        "details": status.details.iter().map(decode_any).collect::<Vec<_>>(),
    }))
}

fn decode_any(any: &prost_types::Any) -> Value {
    match any.type_url.as_str() {
        "type.googleapis.com/gitserver.v1.RepoNotFoundPayload" => {
            RepoNotFoundPayload::decode(any.value.as_slice())
                .map(|payload| {
                    json!({
                        "type": "repo_not_found",
                        "repo": payload.repo,
                        "clone_in_progress": payload.clone_in_progress,
                        "clone_progress": payload.clone_progress,
                    })
                })
                .unwrap_or_else(|_| decode_any_fallback(any))
        }
        "type.googleapis.com/gitserver.v1.RevisionNotFoundPayload" => {
            RevisionNotFoundPayload::decode(any.value.as_slice())
                .map(|payload| json!({"type": "revision_not_found", "repo": payload.repo, "spec": payload.spec}))
                .unwrap_or_else(|_| decode_any_fallback(any))
        }
        "type.googleapis.com/gitserver.v1.FileNotFoundPayload" => {
            FileNotFoundPayload::decode(any.value.as_slice())
                .map(|payload| {
                    json!({
                        "type": "file_not_found",
                        "repo": payload.repo,
                        "commit": payload.commit,
                        // Paths are arbitrary bytes; UTF-8 keeps the fixture
                        // readable, anything else stays lossless as hex.
                        "path": match std::str::from_utf8(&payload.path) {
                            Ok(path) => json!(path),
                            Err(_) => json!({"hex": hex(&payload.path)}),
                        },
                    })
                })
                .unwrap_or_else(|_| decode_any_fallback(any))
        }
        _ => decode_any_fallback(any),
    }
}

fn decode_any_fallback(any: &prost_types::Any) -> Value {
    json!({"type_url": any.type_url, "value_hex": hex(&any.value)})
}

fn code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

#[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
async fn call_gitserver(
    client: &mut gpb::gitserver_service_client::GitserverServiceClient<tonic::transport::Channel>,
    rpc: GitserverRpc,
    request: &Value,
) -> Result<Value, GitserverFailure> {
    match rpc {
        GitserverRpc::Archive => {
            let mut stream = client
                .archive(gitserver_archive_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_archive(&messages))
        }
        GitserverRpc::ReadFile => {
            let mut stream = client
                .read_file(gitserver_read_file_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_read_file(&messages))
        }
        GitserverRpc::ResolveRevision => {
            let response = client
                .resolve_revision(
                    gitserver_resolve_revision_request(request)
                        .map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"commit_sha": response.commit_sha}))
        }
        GitserverRpc::GetObject => {
            let response = client
                .get_object(
                    gitserver_get_object_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            let object = response.object.map(|object| normalize_git_object(&object));
            Ok(json!({"object": object}))
        }
        GitserverRpc::DefaultBranch => {
            let response = client
                .default_branch(
                    gitserver_default_branch_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"ref_name": response.ref_name, "commit": response.commit}))
        }
        GitserverRpc::MergeBase => {
            let response = client
                .merge_base(
                    gitserver_merge_base_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"merge_base_commit_sha": response.merge_base_commit_sha}))
        }
        GitserverRpc::Stat => {
            let response = client
                .stat(gitserver_stat_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            Ok(json!({
                "file_info": response.file_info.as_ref().map(normalize_file_info),
            }))
        }
        GitserverRpc::ReadDir => {
            let mut stream = client
                .read_dir(gitserver_read_dir_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut files = Vec::new();
            while let Some(message) = stream.message().await? {
                files.extend(message.file_info.iter().map(normalize_file_info));
            }
            // Chunk boundaries are an implementation detail; order is git's
            // tree order, but sort so fixtures do not depend on it.
            files.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
            Ok(json!({"file_info": files}))
        }
        GitserverRpc::ListRefs => {
            let mut stream = client
                .list_refs(gitserver_list_refs_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut refs = Vec::new();
            while let Some(message) = stream.message().await? {
                refs.extend(message.refs.iter().map(normalize_git_ref));
            }
            // Chunking is an implementation detail; git's ref order is stable
            // and meaningful, so preserve it.
            Ok(json!({"refs": refs}))
        }
        GitserverRpc::GetCommit => {
            let response = client
                .get_commit(
                    gitserver_get_commit_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({
                "commit": response.commit.as_ref().map(normalize_git_commit),
                "modified_files": response
                    .modified_files
                    .iter()
                    .map(|file| String::from_utf8_lossy(file).into_owned())
                    .collect::<Vec<_>>(),
            }))
        }
        GitserverRpc::RevAtTime => {
            let response = client
                .rev_at_time(
                    gitserver_rev_at_time_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"commit_sha": response.commit_sha}))
        }
        GitserverRpc::RawDiff => {
            let mut stream = client
                .raw_diff(gitserver_raw_diff_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_raw_diff(&messages))
        }
        GitserverRpc::CommitLog => {
            let mut stream = client
                .commit_log(
                    gitserver_commit_log_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_commit_log(&messages))
        }
        GitserverRpc::MergeBaseOctopus => {
            let response = client
                .merge_base_octopus(
                    gitserver_merge_base_octopus_request(request)
                        .map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"merge_base_commit_sha": response.merge_base_commit_sha}))
        }
        GitserverRpc::ContributorCounts => {
            let response = client
                .contributor_counts(
                    gitserver_contributor_counts_request(request)
                        .map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({
                "counts": response
                    .counts
                    .iter()
                    .map(normalize_contributor_count)
                    .collect::<Vec<_>>(),
            }))
        }
        GitserverRpc::FirstEverCommit => {
            let response = client
                .first_ever_commit(
                    gitserver_first_ever_commit_request(request)
                        .map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({
                "commit": response.commit.as_ref().map(normalize_git_commit),
            }))
        }
        GitserverRpc::BehindAhead => {
            let response = client
                .behind_ahead(
                    gitserver_behind_ahead_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({"behind": response.behind, "ahead": response.ahead}))
        }
        GitserverRpc::Blame => {
            let mut stream = client
                .blame(gitserver_blame_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_blame(&messages))
        }
        GitserverRpc::DiskInfo => {
            let response = client
                .disk_info(gitserver_disk_info_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            Ok(normalize_disk_info(&response))
        }
        GitserverRpc::RepoCloneProgress => {
            let response = client
                .repo_clone_progress(
                    gitserver_repo_clone_progress_request(request)
                        .map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            Ok(json!({
                "cloned": response.cloned,
                "clone_in_progress": response.clone_in_progress,
                "clone_progress": response.clone_progress,
            }))
        }
        GitserverRpc::Search => {
            let mut stream = client
                .search(gitserver_search_request(request).map_err(GitserverFailure::Fixture)?)
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_search(&messages))
        }
        GitserverRpc::ChangedFiles => {
            let mut stream = client
                .changed_files(
                    gitserver_changed_files_request(request).map_err(GitserverFailure::Fixture)?,
                )
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(normalize_changed_files(&messages))
        }
    }
}

/// Replay every fixture against one endpoint. All cases must belong to
/// `service`; callers pass that same name from `--service`.
async fn evaluate(service: &str, addr: &str, cases: &[FixtureCase]) -> Result<Vec<Value>, String> {
    let mut results = Vec::with_capacity(cases.len());
    match service {
        "symbols" => {
            let mut client =
                pb::symbols_service_client::SymbolsServiceClient::connect(addr.to_owned())
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
            let mut client =
                spb::searcher_service_client::SearcherServiceClient::connect(addr.to_owned())
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
        "gitserver" => {
            let mut client =
                gpb::gitserver_service_client::GitserverServiceClient::connect(addr.to_owned())
                    .await
                    .map_err(|err| format!("connect {addr}: {err}"))?;
            for case in cases {
                let FixtureService::Gitserver(rpc) = case.service else {
                    return Err(format!(
                        "{}: fixture targets symbols or searcher, not gitserver",
                        case.path.display()
                    ));
                };
                match call_gitserver(&mut client, rpc, &case.request).await {
                    Ok(value) => results.push(value),
                    Err(GitserverFailure::Status(status)) => {
                        results.push(normalize_status(&status))
                    }
                    Err(GitserverFailure::Fixture(err)) => {
                        return Err(format!("{}: {err}", case.path.display()))
                    }
                }
            }
        }
        "repoupdater" => {
            let mut client = rpb::repo_updater_service_client::RepoUpdaterServiceClient::connect(
                addr.to_owned(),
            )
            .await
            .map_err(|err| format!("connect {addr}: {err}"))?;
            for case in cases {
                let FixtureService::Repoupdater(rpc) = case.service else {
                    return Err(format!(
                        "{}: fixture targets another service, not repoupdater",
                        case.path.display()
                    ));
                };
                match call_repoupdater(&mut client, rpc, &case.request).await {
                    Ok(value) => results.push(value),
                    Err(status) => results.push(normalize_status(&status)),
                }
            }
        }
        other => {
            return Err(format!(
                "unsupported service {other:?} (symbols, searcher, gitserver or repoupdater)"
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
        "gitserver" => "http://127.0.0.1:3178",
        "repoupdater" => "http://127.0.0.1:3182",
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
    if !matches!(
        service,
        "symbols" | "searcher" | "gitserver" | "repoupdater"
    ) {
        return Err(format!(
            "diff: unsupported service {service:?} (symbols, searcher, gitserver or repoupdater)"
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
    if !matches!(
        service,
        "symbols" | "searcher" | "gitserver" | "repoupdater"
    ) {
        return Err(format!(
            "record: unsupported service {service:?} (symbols, searcher, gitserver or repoupdater)"
        ));
    }
    let fixture_dir = PathBuf::from(required(&args, "fixture")?);
    let addr = required(&args, "addr")?.to_owned();

    let cases = load_cases(&fixture_dir, service)?;
    let actuals = evaluate(service, &addr, &cases).await?;

    for (case, actual) in cases.iter().zip(actuals) {
        let mut document: Value = serde_json::from_slice(
            &fs::read(&case.path).map_err(|err| format!("read {}: {err}", case.path.display()))?,
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

// ---------------------------------------------------------------------------
// replay-write / canary
// ---------------------------------------------------------------------------

type GitClient = gpb::gitserver_service_client::GitserverServiceClient<tonic::transport::Channel>;
type RepositoryClient = gpb::gitserver_repository_service_client::GitserverRepositoryServiceClient<
    tonic::transport::Channel,
>;

/// Normalized `ListRepositoriesResponse` restricted to the scenario repository:
/// the instances use different `SRC_REPOS_DIR`s, so unrelated repositories on
/// either shard would otherwise make the reports differ. Paths are relative to
/// the data dir, so they do not leak those directories.
fn normalize_repository_list(response: &gpb::ListRepositoriesResponse, repo: &str) -> Value {
    let mut repositories = response
        .repositories
        .iter()
        .filter(|repository| repository.name == repo)
        .map(|repository| {
            json!({
                "path": String::from_utf8_lossy(&repository.path),
                "name": repository.name,
            })
        })
        .collect::<Vec<_>>();
    repositories.sort_by(|left, right| {
        left["path"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["path"].as_str().unwrap_or_default())
    });
    json!({
        "repositories": repositories,
        "next_page_token": response.next_page_token,
    })
}

/// `last_fetched` is the wall-clock fetch time and differs between runs; only
/// its presence is part of the replay. `last_changed` comes from the
/// `sg_refhash_v2` mtime, which is deterministic (the latest commit time) while
/// the file is created by the first clone, so it is compared by value there.
fn normalize_fetch_response(
    response: &gpb::FetchRepositoryResponse,
    exact_last_changed: bool,
) -> Value {
    let last_changed = response.last_changed.as_ref().map(|time| time.seconds);
    json!({
        "last_fetched_present": response.last_fetched.is_some(),
        "last_changed": if exact_last_changed {
            json!(last_changed)
        } else {
            json!(last_changed.is_some())
        },
    })
}

fn normalize_clone_progress(response: &gpb::RepoCloneProgressResponse) -> Value {
    json!({
        "cloned": response.cloned,
        "clone_in_progress": response.clone_in_progress,
        // A finished clone has an empty progress message in both
        // implementations; a running one is inherently timing-dependent.
        "clone_progress_present": !response.clone_progress.is_empty(),
    })
}

async fn fetch_repository(client: &mut RepositoryClient, repo: &str, exact: bool) -> Value {
    match client
        .fetch_repository(gpb::FetchRepositoryRequest {
            repo_name: repo.to_owned(),
        })
        .await
    {
        Ok(response) => normalize_fetch_response(&response.into_inner(), exact),
        Err(status) => normalize_status(&status),
    }
}

async fn clone_progress(client: &mut GitClient, repo: &str) -> Value {
    match client
        .repo_clone_progress(gpb::RepoCloneProgressRequest {
            repo_name: repo.to_owned(),
        })
        .await
    {
        Ok(response) => normalize_clone_progress(&response.into_inner()),
        Err(status) => normalize_status(&status),
    }
}

async fn resolve_head(client: &mut GitClient, repo: &str) -> Value {
    match client
        .resolve_revision(gpb::ResolveRevisionRequest {
            repo_name: repo.to_owned(),
            rev_spec: b"HEAD".to_vec(),
            ensure_revision: None,
        })
        .await
    {
        Ok(response) => json!({"commit_sha": response.into_inner().commit_sha}),
        Err(status) => normalize_status(&status),
    }
}

/// `RevAtTime` goes through the Go process's in-process LRU cache
/// (`revAtTimeCache`); replaying it here checks that both implementations
/// resolve the same commit while the disk repository is shared with a cache.
async fn rev_at_time_now(client: &mut GitClient, repo: &str) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    match client
        .rev_at_time(gpb::RevAtTimeRequest {
            repo_name: repo.to_owned(),
            rev_spec: b"HEAD".to_vec(),
            time: Some(prost_types::Timestamp {
                seconds: now,
                nanos: 0,
            }),
        })
        .await
    {
        Ok(response) => json!({"commit_sha": response.into_inner().commit_sha}),
        Err(status) => normalize_status(&status),
    }
}

async fn list_repositories(client: &mut RepositoryClient, repo: &str) -> Value {
    match client
        .list_repositories(gpb::ListRepositoriesRequest {
            page_size: 1000,
            page_token: String::new(),
        })
        .await
    {
        Ok(response) => normalize_repository_list(&response.into_inner(), repo),
        Err(status) => normalize_status(&status),
    }
}

/// One end of the write-path differential (M9.4). The script drives both
/// gitservers through the same phases against one shared database and remote,
/// then compares the normalized reports:
///
/// - `clone`: list, clone via `FetchRepository`, clone progress, resolved HEAD,
///   `RevAtTime`, an idempotent second fetch, and the list again.
/// - `fetch`: fetch after the remote gained a commit, then resolve.
/// - `delete`: `DeleteRepository`, then progress/list/resolve afterwards.
#[tokio::main]
async fn replay_write(args: &[String]) -> Result<(), String> {
    let args = parse_args(args)?;
    let addr = required(&args, "addr")?.to_owned();
    let repo = required(&args, "repo")?.to_owned();
    let phase = args.get("phase").map(String::as_str).unwrap_or("clone");
    if !matches!(phase, "clone" | "fetch" | "delete") {
        return Err(format!(
            "replay-write: unknown --phase {phase:?} (clone, fetch or delete)"
        ));
    }

    let mut git = GitClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;
    let mut repositories = RepositoryClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;

    let mut steps: Vec<Value> = Vec::new();
    let mut push = |name: &str, value: Value| steps.push(json!({"step": name, "result": value}));

    match phase {
        "clone" => {
            push(
                "list-before",
                list_repositories(&mut repositories, &repo).await,
            );
            push(
                "fetch-clone",
                fetch_repository(&mut repositories, &repo, true).await,
            );
            push("clone-progress", clone_progress(&mut git, &repo).await);
            push("resolve-head", resolve_head(&mut git, &repo).await);
            push("rev-at-time-now", rev_at_time_now(&mut git, &repo).await);
            // The second fetch is a no-op for the ref set, so `sg_refhash_v2`
            // keeps its deterministic mtime.
            push(
                "fetch-idempotent",
                fetch_repository(&mut repositories, &repo, true).await,
            );
            push(
                "list-after",
                list_repositories(&mut repositories, &repo).await,
            );
        }
        "fetch" => {
            // `last_changed` is the wall-clock mtime now that the ref set grew.
            push(
                "fetch-updated",
                fetch_repository(&mut repositories, &repo, false).await,
            );
            push("resolve-head", resolve_head(&mut git, &repo).await);
            push("rev-at-time-now", rev_at_time_now(&mut git, &repo).await);
            push(
                "list-after",
                list_repositories(&mut repositories, &repo).await,
            );
        }
        "delete" => {
            push(
                "delete",
                match repositories
                    .delete_repository(gpb::DeleteRepositoryRequest {
                        repo_name: repo.clone(),
                    })
                    .await
                {
                    Ok(_) => json!({}),
                    Err(status) => normalize_status(&status),
                },
            );
            push("clone-progress", clone_progress(&mut git, &repo).await);
            push(
                "list-after",
                list_repositories(&mut repositories, &repo).await,
            );
            push("resolve-head", resolve_head(&mut git, &repo).await);
        }
        _ => unreachable!("phase validated above"),
    }

    let report = json!({"phase": phase, "repo": repo, "steps": steps});
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|err| format!("serialize replay report: {err}"))?
    );
    Ok(())
}

/// Read-only readiness probe for a gitserver instance, used by the rollout
/// script before switching traffic to it (and before rolling back).
#[tokio::main]
async fn canary(args: &[String]) -> Result<(), String> {
    let args = parse_args(args)?;
    let addr = required(&args, "addr")?.to_owned();
    let repo = required(&args, "repo")?.to_owned();

    let mut git = GitClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;
    let mut repositories = RepositoryClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;

    let clone_progress = clone_progress(&mut git, &repo).await;
    let resolve_head = resolve_head(&mut git, &repo).await;
    let list = list_repositories(&mut repositories, &repo).await;

    // A canary passes when the repository is cloned and HEAD resolves; the
    // list is reported for diagnostics but an empty shard is not an error.
    let cloned = clone_progress["cloned"].as_bool().unwrap_or(false);
    let resolved = resolve_head.get("commit_sha").is_some();
    let report = json!({
        "addr": addr,
        "repo": repo,
        "ok": cloned && resolved,
        "clone_progress": clone_progress,
        "resolve_head": resolve_head,
        "list": list,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|err| format!("serialize canary: {err}"))?
    );
    if cloned && resolved {
        Ok(())
    } else {
        Err(format!(
            "canary failed for {addr}: repo {repo:?} is not cloned or HEAD does not resolve"
        ))
    }
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

    #[test]
    fn hashes_archive_chunks_independent_of_boundaries() {
        let chunk = |data: &[u8]| gpb::ArchiveResponse {
            data: data.to_vec(),
        };
        let split = [chunk(b"hello "), chunk(b"world")];
        let whole = [chunk(b"hello world")];
        assert_eq!(normalize_archive(&split), normalize_archive(&whole));
        assert_eq!(normalize_archive(&whole)["size_bytes"], json!(11));
    }

    #[test]
    fn sorts_changed_files_and_names_statuses() {
        let messages = vec![gpb::ChangedFilesResponse {
            files: vec![
                gpb::ChangedFile {
                    path: b"b.txt".to_vec(),
                    status: gpb::changed_file::Status::Deleted as i32,
                },
                gpb::ChangedFile {
                    path: b"a.txt".to_vec(),
                    status: gpb::changed_file::Status::Added as i32,
                },
            ],
        }];
        let normalized = normalize_changed_files(&messages);
        assert_eq!(normalized["files"][0]["path"], json!("a.txt"));
        assert_eq!(normalized["files"][0]["status"], json!("added"));
        assert_eq!(normalized["files"][1]["status"], json!("deleted"));
    }

    #[test]
    fn normalizes_gitserver_error_statuses() {
        let status = tonic::Status::not_found("file not found");
        assert_eq!(
            normalize_status(&status),
            json!({"error": {"code": "not_found", "message": "file not found"}})
        );
        assert_eq!(code_name(tonic::Code::Unimplemented), "unimplemented");
        assert_eq!(code_name(tonic::Code::InvalidArgument), "invalid_argument");
    }

    /// Build the status the Go server sends for `newRepoNotFoundError`.
    fn repo_not_found_status(repo: &str) -> tonic::Status {
        let payload = RepoNotFoundPayload {
            repo: repo.to_owned(),
            clone_in_progress: false,
            clone_progress: String::new(),
        };
        let details = RpcStatus {
            code: tonic::Code::NotFound as i32,
            message: "repo not found".to_owned(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/gitserver.v1.RepoNotFoundPayload".to_owned(),
                value: payload.encode_to_vec(),
            }],
        };
        tonic::Status::with_details(
            tonic::Code::NotFound,
            "repo not found",
            tonic::codegen::Bytes::from(details.encode_to_vec()),
        )
    }

    #[test]
    fn normalizes_error_details_payloads() {
        assert_eq!(
            normalize_status(&repo_not_found_status("test/repo")),
            json!({
                "error": {
                    "code": "not_found",
                    "message": "repo not found",
                    "details": {
                        "code": tonic::Code::NotFound as i32,
                        "message": "repo not found",
                        "details": [{
                            "type": "repo_not_found",
                            "repo": "test/repo",
                            "clone_in_progress": false,
                            "clone_progress": "",
                        }],
                    },
                },
            })
        );
    }

    #[test]
    fn keeps_non_utf8_detail_paths_lossless() {
        let payload = FileNotFoundPayload {
            repo: "test/repo".to_owned(),
            commit: "abc".to_owned(),
            path: b"dir/\xff.txt".to_vec(),
        };
        let details = RpcStatus {
            code: tonic::Code::NotFound as i32,
            message: "file not found".to_owned(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/gitserver.v1.FileNotFoundPayload".to_owned(),
                value: payload.encode_to_vec(),
            }],
        };
        let status = tonic::Status::with_details(
            tonic::Code::NotFound,
            "file not found",
            tonic::codegen::Bytes::from(details.encode_to_vec()),
        );
        assert_eq!(
            normalize_status(&status)["error"]["details"]["details"][0]["path"],
            json!({"hex": "6469722fff2e747874"})
        );
    }

    #[test]
    fn parses_gitserver_requests() {
        let request = gitserver_archive_request(&json!({
            "repo": "r", "treeish": "c", "format": "tar", "paths": ["a/"],
        }))
        .expect("archive request");
        assert_eq!(request.format, gpb::ArchiveFormat::Tar as i32);
        assert_eq!(request.paths, vec![b"a/".to_vec()]);

        let request = gitserver_changed_files_request(&json!({
            "repo_name": "r", "base": "b", "head": "h",
        }))
        .expect("changed files request");
        assert_eq!(request.base, Some(b"b".to_vec()));

        let request = gitserver_changed_files_request(&json!({
            "repo_name": "r", "head": "h",
        }))
        .expect("changed files request");
        assert_eq!(request.base, None);

        let request = gitserver_stat_request(&json!({
            "repo_name": "r", "commit_sha": "c", "path": "dir/file.txt",
        }))
        .expect("stat request");
        assert_eq!(request.path, b"dir/file.txt".to_vec());

        // An omitted path means the root.
        let request = gitserver_stat_request(&json!({"repo_name": "r", "commit_sha": "c"}))
            .expect("stat request");
        assert!(request.path.is_empty());

        let request = gitserver_read_dir_request(&json!({"repo_name": "r", "commit_sha": "c"}))
            .expect("read_dir request");
        assert_eq!(request.path, None);
        assert!(!request.recursive);

        let request = gitserver_read_dir_request(&json!({
            "repo_name": "r", "commit_sha": "c", "path": "", "recursive": true,
        }))
        .expect("read_dir request");
        assert_eq!(request.path, Some(Vec::new()));
        assert!(request.recursive);

        let request =
            gitserver_list_refs_request(&json!({"repo_name": "r"})).expect("list_refs request");
        assert!(!request.heads_only);
        assert!(!request.tags_only);
        assert!(request.points_at_commit.is_empty());
        assert_eq!(request.contains_sha, None);

        let request = gitserver_list_refs_request(&json!({
            "repo_name": "r",
            "heads_only": true,
            "tags_only": true,
            "points_at_commit": ["a", "b"],
            "contains_sha": "c",
        }))
        .expect("list_refs request");
        assert!(request.heads_only);
        assert!(request.tags_only);
        assert_eq!(
            request.points_at_commit,
            vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(request.contains_sha.as_deref(), Some("c"));

        let request = gitserver_get_commit_request(&json!({"repo_name": "r", "commit": "c"}))
            .expect("get_commit request");
        assert_eq!(request.commit, "c");
        assert!(!request.include_modified_files);

        let request = gitserver_get_commit_request(&json!({
            "repo_name": "r", "commit": "c", "include_modified_files": true,
        }))
        .expect("get_commit request");
        assert!(request.include_modified_files);

        let request = gitserver_rev_at_time_request(&json!({
            "repo_name": "r", "rev_spec": "main", "time": {"seconds": 1705276800},
        }))
        .expect("rev_at_time request");
        assert_eq!(request.rev_spec, b"main");
        assert_eq!(request.time.map(|time| time.seconds), Some(1_705_276_800));

        let request =
            gitserver_rev_at_time_request(&json!({"repo_name": "r"})).expect("rev_at_time request");
        assert!(request.rev_spec.is_empty());
        assert_eq!(request.time, None);

        let request = gitserver_raw_diff_request(&json!({
            "repo_name": "r",
            "base_rev_spec": "a",
            "head_rev_spec": "b",
            "comparison_type": "intersection",
            "paths": ["p"],
            "inter_hunk_context": 5,
        }))
        .expect("raw_diff request");
        assert_eq!(
            request.comparison_type,
            gpb::raw_diff_request::ComparisonType::Intersection as i32
        );
        assert_eq!(request.paths, vec![b"p".to_vec()]);
        assert_eq!(request.inter_hunk_context, Some(5));
        assert_eq!(request.context_lines, None);

        let request = gitserver_commit_log_request(&json!({
            "repo_name": "r",
            "ranges": ["main", "dev"],
            "before": {"seconds": 1735689600},
            "max_commits": 2,
            "skip": 1,
            "include_modified_files": true,
            "order": "commit_date",
            "message_query": "fix",
        }))
        .expect("commit_log request");
        assert_eq!(request.ranges, vec![b"main".to_vec(), b"dev".to_vec()]);
        assert!(!request.all_refs);
        assert_eq!(request.before.map(|time| time.seconds), Some(1_735_689_600));
        assert_eq!(request.after, None);
        assert_eq!(request.max_commits, 2);
        assert_eq!(request.skip, 1);
        assert!(request.include_modified_files);
        assert_eq!(
            request.order,
            gpb::commit_log_request::CommitLogOrder::CommitDate as i32
        );
        assert_eq!(request.message_query, b"fix");

        let request = gitserver_merge_base_octopus_request(&json!({
            "repo_name": "r", "revspecs": ["main", "dev"],
        }))
        .expect("merge_base_octopus request");
        assert_eq!(request.revspecs, vec![b"main".to_vec(), b"dev".to_vec()]);

        let request = gitserver_contributor_counts_request(&json!({
            "repo_name": "r",
            "range": "main",
            "after": {"seconds": 1700000000},
            "path": "a.txt",
        }))
        .expect("contributor_counts request");
        assert_eq!(request.range, b"main");
        assert_eq!(request.after.map(|time| time.seconds), Some(1_700_000_000));
        assert_eq!(request.path, b"a.txt");

        let request = gitserver_contributor_counts_request(&json!({"repo_name": "r"}))
            .expect("contributor_counts request");
        assert!(request.range.is_empty());
        assert_eq!(request.after, None);
        assert!(request.path.is_empty());

        let request = gitserver_first_ever_commit_request(&json!({"repo_name": "r"}))
            .expect("first_ever_commit request");
        assert_eq!(request.repo_name, "r");

        let request = gitserver_behind_ahead_request(&json!({
            "repo_name": "r", "left": "main", "right": "dev",
        }))
        .expect("behind_ahead request");
        assert_eq!(request.left, b"main");
        assert_eq!(request.right, b"dev");

        let request = gitserver_behind_ahead_request(&json!({"repo_name": "r"}))
            .expect("behind_ahead request");
        assert!(request.left.is_empty());
        assert!(request.right.is_empty());

        let request = gitserver_blame_request(&json!({
            "repo_name": "r",
            "commit": "HEAD",
            "path": "a.txt",
            "ignore_whitespace": true,
            "range": {"start_line": 2, "end_line": 5},
        }))
        .expect("blame request");
        assert_eq!(request.commit, "HEAD");
        assert_eq!(request.path, b"a.txt");
        assert!(request.ignore_whitespace);
        let range = request.range.expect("range");
        assert_eq!((range.start_line, range.end_line), (2, 5));

        let request = gitserver_blame_request(&json!({"repo_name": "r", "commit": "HEAD"}))
            .expect("blame request");
        assert!(request.path.is_empty());
        assert!(!request.ignore_whitespace);
        assert!(request.range.is_none());

        let request = gitserver_disk_info_request(&json!({})).expect("disk_info request");
        assert_eq!(request, gpb::DiskInfoRequest {});

        let request = gitserver_repo_clone_progress_request(&json!({
            "repo_name": "r",
        }))
        .expect("repo_clone_progress request");
        assert_eq!(request.repo_name, "r");

        let request = gitserver_repo_clone_progress_request(&json!({"repo_name": ""}))
            .expect("repo_clone_progress request");
        assert!(request.repo_name.is_empty());
    }
}
