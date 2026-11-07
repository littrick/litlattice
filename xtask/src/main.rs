#![forbid(unsafe_code)]

//! `xtask`: repository automation for the Rust rewrite.
//!
//! Subcommands:
//! - `gen-proto`: verify (or update) the pinned baseline proto snapshot.
//! - `contract`: T0 static checks (proto snapshot + gRPC surface).
//! - `diff`: replay `symbols` fixtures against an endpoint and compare.
//! - `record`: call an endpoint and store its normalized response in a fixture.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
  diff --service symbols --fixture DIR [--addr URL]
                                  replay fixtures and compare normalized responses
  record --service symbols --fixture DIR --addr URL
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

struct FixtureCase {
    path: PathBuf,
    rpc: Rpc,
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
    if service != "symbols" {
        return Err(format!(
            "{}: unsupported service {service:?} (only symbols for now)",
            path.display()
        ));
    }
    let rpc = value
        .get("rpc")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{}: missing rpc", path.display()))?;
    let request = value
        .get("request")
        .cloned()
        .ok_or_else(|| format!("{}: missing request", path.display()))?;
    Ok(FixtureCase {
        path: path.to_path_buf(),
        rpc: Rpc::parse(rpc)?,
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
        exclude_pattern: string(value, "exclude_pattern")?,
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

async fn call(
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

// ---------------------------------------------------------------------------
// diff / record
// ---------------------------------------------------------------------------

#[tokio::main]
async fn diff(args: &[String]) -> Result<(), String> {
    let args = parse_args(args)?;
    let service = required(&args, "service")?;
    if service != "symbols" {
        return Err(format!(
            "diff: unsupported service {service:?} (only symbols)"
        ));
    }
    let fixture_dir = PathBuf::from(required(&args, "fixture")?);
    let addr = args
        .get("addr")
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:3184".to_owned());

    let fixtures = collect_fixtures(&fixture_dir)?;
    let mut client = pb::symbols_service_client::SymbolsServiceClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;

    let mut failures = 0usize;
    for path in fixtures {
        let case = load_fixture(&path)?;
        let expected = case
            .expected
            .as_ref()
            .ok_or_else(|| format!("{}: missing expected response", case.path.display()))?;
        let actual = call(&mut client, case.rpc, &case.request).await?;
        if &actual == expected {
            println!("PASS {}", relative(&path));
        } else {
            failures += 1;
            eprintln!("FAIL {}", relative(&path));
            eprintln!("  expected: {}", compact(expected));
            eprintln!("  actual:   {}", compact(&actual));
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
    if service != "symbols" {
        return Err(format!(
            "record: unsupported service {service:?} (only symbols)"
        ));
    }
    let fixture_dir = PathBuf::from(required(&args, "fixture")?);
    let addr = required(&args, "addr")?.to_owned();

    let fixtures = collect_fixtures(&fixture_dir)?;
    let mut client = pb::symbols_service_client::SymbolsServiceClient::connect(addr.clone())
        .await
        .map_err(|err| format!("connect {addr}: {err}"))?;

    for path in fixtures {
        let case = load_fixture(&path)?;
        let actual = call(&mut client, case.rpc, &case.request).await?;
        let mut document: Value = serde_json::from_slice(
            &fs::read(&path).map_err(|err| format!("read {}: {err}", path.display()))?,
        )
        .map_err(|err| format!("parse {}: {err}", path.display()))?;
        document["expected"] = actual;
        let serialized = serde_json::to_string_pretty(&document)
            .map_err(|err| format!("serialize {}: {err}", path.display()))?;
        fs::write(&path, format!("{serialized}\n"))
            .map_err(|err| format!("write {}: {err}", path.display()))?;
        println!("RECORDED {}", relative(&path));
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
