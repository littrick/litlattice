//! M9.8il differential harness: [`GrpcSearcherClient`] against real
//! `searcher.Search` (`internal/search/searcher/client.go`).
//!
//! Run with `SG_M9IL_DIFF=1 M9IL_CORPUS=<json> M9IL_OUT=<json>
//! M9IL_BASE_PORT=<port>` (the wrapper script in `test_ws/p9-go` does this).
//! Both sides read the same corpus and emit byte-identical normalized JSON.
//!
//! Each corpus case gets its own in-process fake gRPC server(s) on fixed
//! loopback ports (`M9IL_BASE_PORT + case_index*4 + endpoint_index`) so the
//! rendezvous hash sees the same endpoint strings as the Go oracle.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use sg_proto::searcher as pb;
use sg_service_frontend::search_searcher_client::{
    protocol, GrpcSearcherClient, SearcherClient, SearcherErrorKind,
};

#[derive(Debug, Deserialize, Default, Clone)]
struct QuerySpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    is_negated: bool,
    #[serde(default)]
    is_regexp: bool,
    #[serde(default)]
    boost: bool,
    #[serde(default)]
    children: Vec<QuerySpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct PatternInfoSpec {
    #[serde(default)]
    query: Option<QuerySpec>,
    #[serde(default)]
    is_structural: bool,
    #[serde(default)]
    is_case_sensitive: bool,
    #[serde(default)]
    exclude_paths: String,
    #[serde(default)]
    include_paths: Vec<String>,
    #[serde(default)]
    path_patterns_are_case_sensitive: bool,
    #[serde(default)]
    limit: i32,
    #[serde(default)]
    pattern_matches_content: bool,
    #[serde(default)]
    pattern_matches_path: bool,
    #[serde(default)]
    comby_rule: String,
    #[serde(default)]
    select: String,
    #[serde(default)]
    include_langs: Vec<String>,
    #[serde(default)]
    exclude_langs: Vec<String>,
    #[serde(default)]
    languages: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct LocationSpec {
    #[serde(default)]
    offset: i32,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    column: i32,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct MatchSpec {
    #[serde(default)]
    path: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ErrorSpec {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct EndpointSpec {
    #[serde(default)]
    matches: Vec<MatchSpec>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    error: Option<ErrorSpec>,
    #[serde(default)]
    error_after_matches: usize,
    #[serde(default)]
    fail_calls: usize,
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
    commit: String,
    #[serde(default)]
    branch: String,
    #[serde(default)]
    indexed: bool,
    #[serde(default)]
    pattern_info: PatternInfoSpec,
    #[serde(default)]
    fetch_timeout_nanos: u64,
    #[serde(default)]
    num_context_lines: i32,
    #[serde(default)]
    endpoints: Vec<EndpointSpec>,
}

fn to_query_node(spec: &QuerySpec) -> protocol::QueryNode {
    match spec.kind.as_str() {
        "pattern" => protocol::QueryNode::Pattern(protocol::PatternNode {
            value: spec.value.clone(),
            is_negated: spec.is_negated,
            is_regexp: spec.is_regexp,
            boost: spec.boost,
        }),
        "and" => protocol::QueryNode::And(spec.children.iter().map(to_query_node).collect()),
        "or" => protocol::QueryNode::Or(spec.children.iter().map(to_query_node).collect()),
        other => panic!("unknown query kind {other}"),
    }
}

fn build_request(case: &Case) -> protocol::Request {
    let pattern_info = &case.pattern_info;
    protocol::Request {
        repo: case.repo.clone(),
        repo_id: case.repo_id,
        commit: case.commit.clone(),
        branch: case.branch.clone(),
        indexed: case.indexed,
        pattern_info: protocol::PatternInfo {
            query: pattern_info.query.as_ref().map(to_query_node),
            is_structural: pattern_info.is_structural,
            is_case_sensitive: pattern_info.is_case_sensitive,
            exclude_paths: pattern_info.exclude_paths.clone(),
            include_paths: pattern_info.include_paths.clone(),
            include_langs: pattern_info.include_langs.clone(),
            exclude_langs: pattern_info.exclude_langs.clone(),
            path_patterns_are_case_sensitive: pattern_info.path_patterns_are_case_sensitive,
            limit: pattern_info.limit,
            pattern_matches_content: pattern_info.pattern_matches_content,
            pattern_matches_path: pattern_info.pattern_matches_path,
            comby_rule: pattern_info.comby_rule.clone(),
            select: pattern_info.select.clone(),
            languages: pattern_info.languages.clone(),
        },
        fetch_timeout: Duration::from_nanos(case.fetch_timeout_nanos),
        num_context_lines: case.num_context_lines,
    }
}

fn status_code(name: &str) -> tonic::Code {
    match name {
        "not_found" => tonic::Code::NotFound,
        "deadline_exceeded" => tonic::Code::DeadlineExceeded,
        "unavailable" => tonic::Code::Unavailable,
        "resource_exhausted" => tonic::Code::ResourceExhausted,
        "internal" => tonic::Code::Internal,
        "canceled" => tonic::Code::Cancelled,
        other => panic!("unknown error code {other}"),
    }
}

fn to_pb_location(location: LocationSpec) -> pb::Location {
    pb::Location {
        offset: location.offset,
        line: location.line,
        column: location.column,
    }
}

fn to_pb_file_match(spec: &MatchSpec) -> pb::FileMatch {
    pb::FileMatch {
        path: spec.path.as_bytes().to_vec(),
        language: spec.language.as_bytes().to_vec(),
        limit_hit: spec.limit_hit,
        chunk_matches: spec
            .chunk_matches
            .iter()
            .map(|chunk| pb::ChunkMatch {
                content: chunk.content.as_bytes().to_vec(),
                content_start: Some(to_pb_location(chunk.content_start)),
                ranges: chunk
                    .ranges
                    .iter()
                    .map(|range| pb::Range {
                        start: Some(to_pb_location(range.start)),
                        end: Some(to_pb_location(range.end)),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn file_match_response(spec: &MatchSpec) -> pb::SearchResponse {
    pb::SearchResponse {
        message: Some(pb::search_response::Message::FileMatch(to_pb_file_match(
            spec,
        ))),
    }
}

fn done_response(limit_hit: bool) -> pb::SearchResponse {
    pb::SearchResponse {
        message: Some(pb::search_response::Message::DoneMessage(
            pb::search_response::Done { limit_hit },
        )),
    }
}

struct EndpointState {
    spec: EndpointSpec,
    calls: AtomicUsize,
    first_request: Mutex<Option<pb::SearchRequest>>,
}

#[derive(Clone)]
struct FakeEndpoint {
    state: Arc<EndpointState>,
}

#[tonic::async_trait]
impl pb::searcher_service_server::SearcherService for FakeEndpoint {
    type SearchStream =
        Pin<Box<dyn Stream<Item = Result<pb::SearchResponse, Status>> + Send + 'static>>;

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        let call = self.state.calls.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut first = self.state.first_request.lock().expect("first request lock");
            if first.is_none() {
                *first = Some(request.into_inner());
            }
        }
        let spec = &self.state.spec;
        let failing = spec
            .error
            .as_ref()
            .is_some_and(|_| spec.fail_calls == 0 || call <= spec.fail_calls);
        let mut messages: Vec<Result<pb::SearchResponse, Status>> = Vec::new();
        if failing {
            let error = spec.error.as_ref().expect("failing endpoint error");
            for match_spec in spec.matches.iter().take(spec.error_after_matches) {
                messages.push(Ok(file_match_response(match_spec)));
            }
            messages.push(Err(Status::new(
                status_code(&error.code),
                error.message.clone(),
            )));
        } else {
            for match_spec in &spec.matches {
                messages.push(Ok(file_match_response(match_spec)));
            }
            messages.push(Ok(done_response(spec.limit_hit)));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(messages))))
    }
}

async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..5_000 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("fake searcher server at {addr} did not start");
}

fn start_servers(
    runtime: &tokio::runtime::Runtime,
    case_index: usize,
    endpoints: &[EndpointSpec],
    base_port: u16,
) -> (
    Vec<String>,
    Vec<Arc<EndpointState>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let mut urls = Vec::with_capacity(endpoints.len());
    let mut states = Vec::with_capacity(endpoints.len());
    let mut handles = Vec::with_capacity(endpoints.len());
    for (endpoint_index, spec) in endpoints.iter().enumerate() {
        let port = base_port + (case_index as u16) * 4 + endpoint_index as u16;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let state = Arc::new(EndpointState {
            spec: spec.clone(),
            calls: AtomicUsize::new(0),
            first_request: Mutex::new(None),
        });
        let service = pb::searcher_service_server::SearcherServiceServer::new(FakeEndpoint {
            state: Arc::clone(&state),
        });
        let handle = runtime.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve(addr)
                .await;
        });
        runtime.block_on(wait_for_server(addr));
        urls.push(format!("http://{addr}"));
        states.push(state);
        handles.push(handle);
    }
    (urls, states, handles)
}

fn normalize_location(location: protocol::Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn normalize_match(file_match: &protocol::FileMatch) -> Value {
    json!({
        "path": file_match.path.as_str(),
        "language": file_match.language.as_str(),
        "limit_hit": file_match.limit_hit,
        "chunk_matches": file_match.chunk_matches.iter().map(|chunk| json!({
            "content": chunk.content.as_str(),
            "content_start": normalize_location(chunk.content_start),
            "ranges": chunk.ranges.iter().map(|range| json!({
                "start": normalize_location(range.start),
                "end": normalize_location(range.end),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

fn normalize_query(node: &pb::QueryNode) -> Value {
    match node.value.as_ref() {
        Some(pb::query_node::Value::Pattern(pattern)) => json!({
            "kind": "pattern",
            "value": pattern.value.as_str(),
            "is_negated": pattern.is_negated,
            "is_regexp": pattern.is_regexp,
            "boost": pattern.boost,
        }),
        Some(pb::query_node::Value::And(and)) => json!({
            "kind": "and",
            "children": and.children.iter().map(normalize_query).collect::<Vec<_>>(),
        }),
        Some(pb::query_node::Value::Or(or)) => json!({
            "kind": "or",
            "children": or.children.iter().map(normalize_query).collect::<Vec<_>>(),
        }),
        None => Value::Null,
    }
}

fn normalize_request(request: &pb::SearchRequest) -> Value {
    let pattern_info = request.pattern_info.clone().unwrap_or_default();
    let timeout = request.fetch_timeout.unwrap_or_default();
    json!({
        "repo": request.repo.as_str(),
        "repo_id": request.repo_id,
        "commit_oid": request.commit_oid.as_str(),
        "indexed": request.indexed,
        "branch": request.branch.as_str(),
        "num_context_lines": request.num_context_lines,
        "fetch_timeout_seconds": timeout.seconds,
        "fetch_timeout_nanos": timeout.nanos,
        "pattern_info": {
            "query": pattern_info.query.as_ref().map(normalize_query).unwrap_or(Value::Null),
            "is_structural": pattern_info.is_structural,
            "is_case_sensitive": pattern_info.is_case_sensitive,
            "exclude_pattern": pattern_info.exclude_pattern.as_str(),
            "include_patterns": pattern_info.include_patterns,
            "path_patterns_are_case_sensitive": pattern_info.path_patterns_are_case_sensitive,
            "limit": pattern_info.limit,
            "pattern_matches_content": pattern_info.pattern_matches_content,
            "pattern_matches_path": pattern_info.pattern_matches_path,
            "comby_rule": pattern_info.comby_rule.as_str(),
            "languages": pattern_info.languages,
            "select": pattern_info.select.as_str(),
            "include_langs": pattern_info.include_langs,
            "exclude_langs": pattern_info.exclude_langs,
        },
    })
}

fn kind_name(kind: SearcherErrorKind) -> &'static str {
    match kind {
        SearcherErrorKind::Other => "other",
        SearcherErrorKind::RepoNotFound { .. } => "repo_not_found",
        SearcherErrorKind::RevisionNotFound => "revision_not_found",
        SearcherErrorKind::NotFound => "not_found",
        SearcherErrorKind::Timeout => "timeout",
        SearcherErrorKind::Temporary => "temporary",
    }
}

fn run_case(
    runtime: &tokio::runtime::Runtime,
    case_index: usize,
    case: &Case,
    base_port: u16,
) -> Value {
    let (urls, states, handles) = start_servers(runtime, case_index, &case.endpoints, base_port);
    let request = build_request(case);
    let client = GrpcSearcherClient::new(urls.clone());

    let mut matches: Vec<protocol::FileMatch> = Vec::new();
    let outcome = client.search(&request, &mut |file_match| matches.push(file_match));

    let routing: Vec<usize> = client
        .route_order(&case.repo, &case.commit)
        .iter()
        .map(|url| {
            urls.iter()
                .position(|candidate| candidate == url)
                .expect("routed endpoint must be configured")
        })
        .collect();

    let served: Vec<Value> = states
        .iter()
        .map(|state| {
            let calls = state.calls.load(Ordering::SeqCst);
            let first = state
                .first_request
                .lock()
                .expect("first request lock")
                .as_ref()
                .map(normalize_request);
            json!({ "calls": calls, "request": first.unwrap_or(Value::Null) })
        })
        .collect();

    let matches: Vec<Value> = matches.iter().map(normalize_match).collect();
    let (limit_hit, error) = match outcome {
        Ok(limit_hit) => (limit_hit, Value::Null),
        Err(error) => (
            false,
            json!({ "kind": kind_name(error.kind), "message": error.message }),
        ),
    };

    let result = json!({
        "name": case.name.as_str(),
        "routing": routing,
        "served": served,
        "matches": matches,
        "limit_hit": limit_hit,
        "error": error,
    });

    runtime.block_on(async move {
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    });
    result
}

#[test]
fn searcher_grpc_client_diff() {
    if std::env::var("SG_M9IL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9IL_CORPUS").expect("M9IL_CORPUS must be set when SG_M9IL_DIFF=1");
    let out_path = std::env::var("M9IL_OUT").expect("M9IL_OUT must be set when SG_M9IL_DIFF=1");
    let base_port: u16 = std::env::var("M9IL_BASE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(39400);

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
