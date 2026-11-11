//! M9.8iq differential harness: the Rust `symbols_client` + `symbol_search`
//! modules against the Go `internal/symbols` client and
//! `internal/search/symbol.ZoektSymbolsClient`.
//!
//! Run with `SG_M9IQ_DIFF=1 M9IQ_CORPUS=<json> M9IQ_OUT=<json>
//! M9IQ_BASE_PORT=<port>` (the wrapper script in `test_ws/p9-go` does this).
//! Both sides read the same corpus and emit byte-identical normalized JSON.
//!
//! Each corpus case gets its own in-process fake `symbols.v1.SymbolsService`
//! server on a fixed loopback port (`M9IQ_BASE_PORT + case_index`); the fake
//! zoekt searcher replays the corpus files and records the outgoing query and
//! search options.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use tonic::{Request, Response, Status};

use sg_proto::symbols as pb;
use sg_service_frontend::search_searcher_client::{SymbolsClient, SymbolsParameters};
use sg_service_frontend::search_stream::convert;
use sg_service_frontend::symbol_search::{
    SymbolSearchParams, ZoektSymbolSearcher, ZoektSymbolsClient,
};
use sg_service_frontend::symbols_client::{GrpcSymbolsClient, NoSubRepoPerms, SubRepoPermsChecker};
use sg_zoekt::query::{BranchRepos, Query};
use sg_zoekt::types::{
    ChunkMatch, FileMatch, ListResponse, Location, MinimalRepoListEntry, Range, RepositoryBranch,
    SearchOptions, SearchResult, SymbolInfo,
};

#[derive(Debug, Deserialize, Default, Clone)]
struct BranchSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoEntrySpec {
    #[serde(default)]
    has_symbols: bool,
    #[serde(default)]
    branches: Vec<BranchSpec>,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct LocationSpec {
    #[serde(default)]
    line: i32,
    #[serde(default)]
    column: i32,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct SymbolInfoSpec {
    #[serde(default)]
    sym: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
    #[serde(default)]
    symbol_info: Vec<Option<SymbolInfoSpec>>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct FileSpec {
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ZoektSpec {
    #[serde(default)]
    repos_map: BTreeMap<String, RepoEntrySpec>,
    #[serde(default)]
    files: Vec<FileSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct SymbolSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    character: i32,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct GRPCErrorSpec {
    #[serde(default)]
    message: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ServiceSpec {
    #[serde(default)]
    symbols: Vec<SymbolSpec>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    grpc_error: Option<GRPCErrorSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct SubRepoSpec {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    deny_paths: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    repo_id: u32,
    #[serde(default)]
    commit_id: String,
    #[serde(default)]
    input_rev: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    first: Option<i32>,
    #[serde(default)]
    include_patterns: Option<Vec<String>>,
    #[serde(default)]
    parent_canceled: bool,
    #[serde(default)]
    sub_repo: SubRepoSpec,
    #[serde(default)]
    zoekt: ZoektSpec,
    #[serde(default)]
    service: ServiceSpec,
}

fn to_location(spec: LocationSpec) -> Location {
    Location {
        byte_offset: 0,
        line_number: spec.line as u32,
        column: spec.column as u32,
    }
}

fn build_list(spec: &ZoektSpec) -> ListResponse {
    let mut repos_map = BTreeMap::new();
    for (raw_id, entry) in &spec.repos_map {
        let id: u32 = raw_id.parse().expect("repo id");
        repos_map.insert(
            id,
            MinimalRepoListEntry {
                has_symbols: entry.has_symbols,
                branches: entry
                    .branches
                    .iter()
                    .map(|branch| RepositoryBranch {
                        name: branch.name.clone(),
                        version: branch.version.clone(),
                    })
                    .collect(),
                index_time_unix: 0,
            },
        );
    }
    ListResponse {
        repos_map,
        ..ListResponse::default()
    }
}

fn build_search_result(spec: &ZoektSpec) -> SearchResult {
    let mut files = Vec::with_capacity(spec.files.len());
    for file in &spec.files {
        let mut chunk_matches = Vec::with_capacity(file.chunk_matches.len());
        for chunk in &file.chunk_matches {
            let ranges: Vec<Range> = chunk
                .ranges
                .iter()
                .map(|range| Range {
                    start: to_location(range.start),
                    end: to_location(range.start),
                })
                .collect();
            let symbol_info: Vec<Option<SymbolInfo>> = chunk
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
                .collect();
            chunk_matches.push(ChunkMatch {
                content: chunk.content.as_bytes().to_vec(),
                content_start: Location {
                    byte_offset: 0,
                    line_number: 1,
                    column: 1,
                },
                file_name: chunk.file_name,
                ranges,
                symbol_info,
                score: 0.0,
            });
        }
        files.push(FileMatch {
            file_name: file.file_name.as_bytes().to_vec(),
            language: file.language.clone(),
            chunk_matches,
            ..FileMatch::default()
        });
    }
    SearchResult {
        files,
        ..SearchResult::default()
    }
}

struct FakeZoekt {
    list: ListResponse,
    search_result: SearchResult,
    seen: Mutex<Option<(Query, SearchOptions)>>,
}

impl ZoektSymbolSearcher for FakeZoekt {
    fn list_all_indexed(&self) -> Result<ListResponse, String> {
        Ok(self.list.clone())
    }

    fn search(&self, query: &Query, options: &SearchOptions) -> Result<SearchResult, String> {
        *self.seen.lock().expect("zoekt seen lock") = Some((query.clone(), options.clone()));
        Ok(self.search_result.clone())
    }
}

struct FakeChecker {
    deny: HashSet<String>,
}

impl SubRepoPermsChecker for FakeChecker {
    fn enabled(&self) -> bool {
        true
    }

    fn include(&self, _repo: &str, path: &str) -> Result<bool, String> {
        Ok(!self.deny.contains(path))
    }
}

struct ServiceState {
    spec: ServiceSpec,
    request: Mutex<Option<pb::SearchRequest>>,
}

#[derive(Clone)]
struct FakeSymbols {
    state: Arc<ServiceState>,
}

#[tonic::async_trait]
impl pb::symbols_service_server::SymbolsService for FakeSymbols {
    type LocalCodeIntelStream =
        tokio_stream::wrappers::ReceiverStream<Result<pb::LocalCodeIntelResponse, Status>>;

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        let request = request.into_inner();
        *self.state.request.lock().expect("service request lock") = Some(request);
        if let Some(error) = &self.state.spec.grpc_error {
            return Err(Status::invalid_argument(error.message.clone()));
        }
        let symbols = self
            .state
            .spec
            .symbols
            .iter()
            .map(|symbol| pb::search_response::Symbol {
                name: symbol.name.clone(),
                path: symbol.path.clone(),
                line: symbol.line,
                character: symbol.character,
                kind: symbol.kind.clone(),
                language: symbol.language.clone(),
                parent: symbol.parent.clone(),
                parent_kind: symbol.parent_kind.clone(),
                signature: String::new(),
                file_limited: false,
            })
            .collect();
        Ok(Response::new(pb::SearchResponse {
            symbols,
            error: self.state.spec.error.clone(),
            limit_hit: self.state.spec.limit_hit,
        }))
    }

    async fn local_code_intel(
        &self,
        _request: Request<pb::LocalCodeIntelRequest>,
    ) -> Result<Response<Self::LocalCodeIntelStream>, Status> {
        Err(Status::unimplemented("local_code_intel"))
    }

    async fn symbol_info(
        &self,
        _request: Request<pb::SymbolInfoRequest>,
    ) -> Result<Response<pb::SymbolInfoResponse>, Status> {
        Err(Status::unimplemented("symbol_info"))
    }

    async fn healthz(
        &self,
        _request: Request<pb::HealthzRequest>,
    ) -> Result<Response<pb::HealthzResponse>, Status> {
        Ok(Response::new(pb::HealthzResponse {}))
    }
}

async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..5_000 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("fake symbols server at {addr} did not start");
}

fn repo_ids(branch: &BranchRepos) -> Vec<i64> {
    let bitmap = roaring::RoaringBitmap::deserialize_from(&branch.repos[..])
        .expect("deserialize repo id bitmap");
    bitmap.iter().map(i64::from).collect()
}

fn query_summary(query: &Query) -> Value {
    let mut branch: Option<String> = None;
    let mut repo_ids_out: Vec<i64> = Vec::new();
    let mut symbol_kind: Option<&str> = None;
    let mut symbol_pattern: Option<String> = None;
    let mut file_patterns: Vec<Value> = Vec::new();

    fn visit(
        node: &Query,
        branch: &mut Option<String>,
        repo_ids_out: &mut Vec<i64>,
        symbol_kind: &mut Option<&str>,
        symbol_pattern: &mut Option<String>,
        file_patterns: &mut Vec<Value>,
    ) {
        match node {
            Query::And(children) => {
                for child in children {
                    visit(
                        child,
                        branch,
                        repo_ids_out,
                        symbol_kind,
                        symbol_pattern,
                        file_patterns,
                    );
                }
            }
            Query::BranchesRepos(list) => {
                if let Some(first) = list.first() {
                    *branch = Some(first.branch.clone());
                    *repo_ids_out = repo_ids(first);
                }
            }
            Query::Symbol(inner) => match inner.as_ref() {
                Query::Substring { pattern, .. } => {
                    *symbol_kind = Some("substring");
                    *symbol_pattern = Some(pattern.clone());
                }
                Query::Regexp { regexp, .. } => {
                    *symbol_kind = Some("regexp");
                    *symbol_pattern = Some(regexp.clone());
                }
                _ => {}
            },
            Query::Substring {
                pattern,
                file_name: true,
                ..
            } => file_patterns.push(json!({"kind": "substring", "pattern": pattern})),
            Query::Regexp {
                regexp,
                file_name: true,
                ..
            } => file_patterns.push(json!({"kind": "regexp", "pattern": regexp})),
            _ => {}
        }
    }

    visit(
        query,
        &mut branch,
        &mut repo_ids_out,
        &mut symbol_kind,
        &mut symbol_pattern,
        &mut file_patterns,
    );

    json!({
        "branch": branch,
        "repo_ids": repo_ids_out,
        "symbol_kind": symbol_kind,
        "symbol_pattern": symbol_pattern,
        "file_patterns": file_patterns,
    })
}

fn match_out(symbol: &convert::Symbol, file: &convert::FileRef) -> Value {
    json!({
        "repo": file.repo.name,
        "repo_id": i64::from(file.repo.id),
        "commit_id": file.commit_id,
        "input_rev": file.input_rev,
        "path": file.path,
        "file_language": file.precise_language,
        "name": symbol.name,
        "line": i64::from(symbol.line),
        "character": i64::from(symbol.character),
        "kind": symbol.kind,
        "language": symbol.language,
        "parent": symbol.parent,
        "parent_kind": symbol.parent_kind,
    })
}

fn request_out(request: &Option<pb::SearchRequest>) -> Value {
    match request {
        None => Value::Null,
        Some(request) => {
            let (seconds, nanos) = request
                .timeout
                .as_ref()
                .map(|timeout| (timeout.seconds, i64::from(timeout.nanos)))
                .unwrap_or((0, 0));
            json!({
                "repo": request.repo,
                "commit_id": request.commit_id,
                "query": request.query,
                "is_reg_exp": request.is_reg_exp,
                "is_case_sensitive": request.is_case_sensitive,
                "include_patterns": request.include_patterns,
                "exclude_pattern": request.exclude_pattern,
                "first": i64::from(request.first),
                "timeout_seconds": seconds,
                "timeout_nanos": nanos,
                "include_langs": request.include_langs,
                "exclude_langs": request.exclude_langs,
            })
        }
    }
}

fn run_case(
    runtime: &tokio::runtime::Runtime,
    case_index: usize,
    case: &Case,
    base_port: u16,
) -> Value {
    let port = base_port + case_index as u16;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let state = Arc::new(ServiceState {
        spec: case.service.clone(),
        request: Mutex::new(None),
    });
    let service = pb::symbols_service_server::SymbolsServiceServer::new(FakeSymbols {
        state: Arc::clone(&state),
    });
    let handle = runtime.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve(addr)
            .await;
    });
    runtime.block_on(wait_for_server(addr));

    let checker: Arc<dyn SubRepoPermsChecker> = if case.sub_repo.enabled {
        Arc::new(FakeChecker {
            deny: case.sub_repo.deny_paths.iter().cloned().collect(),
        })
    } else {
        Arc::new(NoSubRepoPerms)
    };

    let symbols_client = GrpcSymbolsClient::new(vec![format!("http://127.0.0.1:{port}")])
        .with_sub_repo_perms(Arc::clone(&checker));
    let symbols_client: Arc<dyn SymbolsClient + Send + Sync> = Arc::new(symbols_client);

    let zoekt = Arc::new(FakeZoekt {
        list: build_list(&case.zoekt),
        search_result: build_search_result(&case.zoekt),
        seen: Mutex::new(None),
    });

    let client = ZoektSymbolsClient {
        zoekt: Some(zoekt.clone()),
        symbols: symbols_client,
        sub_repo_perms: Arc::clone(&checker),
    };

    let deadline = if case.parent_canceled {
        Some(Instant::now() - Duration::from_secs(1))
    } else {
        None
    };
    let params = SymbolSearchParams {
        repo: convert::MinimalRepo {
            id: case.repo_id as i32,
            name: case.repo.clone(),
        },
        commit_id: case.commit_id.clone(),
        input_rev: case.input_rev.clone(),
        query: case.query.clone(),
        first: case.first,
        include_patterns: case.include_patterns.clone(),
        deadline,
    };

    let outcome = client.compute(&params);
    let (matches, error) = match outcome {
        Ok(matches) => (matches, Value::Null),
        Err(error) => (Vec::new(), Value::String(error)),
    };
    let matches: Vec<Value> = matches
        .iter()
        .map(|matched| match_out(&matched.symbol, &matched.file))
        .collect();

    let seen = zoekt.seen.lock().expect("zoekt seen lock").clone();
    let zoekt_out = match seen {
        None => json!({"called": false, "options": Value::Null, "query": Value::Null}),
        Some((query, options)) => json!({
            "called": true,
            "options": {
                "max_wall_time_seconds": i64::try_from(options.max_wall_time.as_secs()).unwrap_or(i64::MAX),
                "shard_max_match_count": options.shard_max_match_count,
                "total_max_match_count": options.total_max_match_count,
                "max_doc_display_count": options.max_doc_display_count,
                "chunk_matches": options.chunk_matches,
                "num_context_lines": options.num_context_lines,
                "trace": options.trace,
            },
            "query": query_summary(&query),
        }),
    };

    let request = state.request.lock().expect("service request lock").clone();

    let result = json!({
        "name": case.name,
        "error": error,
        "matches": matches,
        "zoekt": zoekt_out,
        "service_request": request_out(&request),
    });

    handle.abort();
    result
}

#[test]
fn symbols_client_diff() {
    if std::env::var("SG_M9IQ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9IQ_CORPUS").expect("M9IQ_CORPUS must be set when SG_M9IQ_DIFF=1");
    let out_path = std::env::var("M9IQ_OUT").expect("M9IQ_OUT must be set when SG_M9IQ_DIFF=1");
    let base_port: u16 = std::env::var("M9IQ_BASE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(39500);

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build test runtime");

    let entries: Vec<Value> = cases
        .iter()
        .enumerate()
        .map(|(index, case)| run_case(&runtime, index, case, base_port))
        .collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
    runtime.shutdown_background();
}

#[test]
fn missing_endpoints_keep_the_not_ported_error() {
    assert!(sg_service_frontend::symbols_client::default_symbols_client(Vec::new()).is_none());
    let symbols = sg_service_frontend::search_runtime::NotPortedSymbols;
    assert_eq!(
        symbols.search(&SymbolsParameters::default()),
        Err(sg_service_frontend::search_searcher_client::SearcherError::not_ported())
    );
}
