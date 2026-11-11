//! M9.8iw differential harness: the gRPC connection cache and its
//! searcher/symbols connectors (`internal/grpc/defaults/cache.go`,
//! `internal/symbols/client.go`) against the Rust
//! `search_env::GrpcConnectionCache` + `search_searcher_client` /
//! `symbols_client` connectors.
//!
//! Run with `SG_M9IW_DIFF=1 M9IW_CORPUS=<json> M9IW_OUT=<json>
//! M9IW_BASE_PORT=<port>`; the wrapper script in `test_ws/p9-go` does this.
//!
//! Each corpus case starts in-process fake `searcher.v1`/`symbols.v1` servers
//! on fixed loopback ports and replays the call sequence through the
//! production cached connectors. Both sides emit normalized JSON:
//! `calls` (cache lookups), `connections` (Go `net.Listener.Accept` vs Rust
//! successful channel constructions) and the per-endpoint distribution. A
//! parse-error endpoint contributes an error but no connection, exactly like
//! the baseline `connAndError` cache entry.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use sg_proto::searcher as searcher_pb;
use sg_proto::symbols as symbols_pb;
use sg_service_frontend::search_env::{GrpcChannelSource, GrpcConnectionCache};
use sg_service_frontend::search_searcher_client::{
    connection_key, SearcherConnector, SearcherError,
};
use sg_service_frontend::symbols_client::SymbolsConnector;

/// Each case may use up to this many endpoints; ports are laid out with this
/// stride so the Go and Rust harnesses agree without sharing state.
const MAX_ENDPOINTS: u16 = 8;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[serde(default = "default_service")]
    service: String,
    endpoints: Vec<String>,
    #[serde(default = "one")]
    caches: usize,
    #[serde(default = "one")]
    concurrency: usize,
    calls: Vec<serde_json::Value>,
}

fn default_service() -> String {
    "searcher".to_owned()
}

fn one() -> usize {
    1
}

#[derive(Debug, Serialize)]
struct EndpointResult {
    calls: usize,
    connections: usize,
    errors: usize,
}

#[derive(Debug, Serialize)]
struct CaseResult {
    name: String,
    calls: usize,
    connections: usize,
    errors: usize,
    by_endpoint: BTreeMap<String, EndpointResult>,
}

/// A cache wrapped with creation counters. The cached connector is the code
/// under test; this wrapper only observes successful `channel()` calls.
struct CountingSource {
    inner: GrpcConnectionCache,
    created: AtomicUsize,
    by_key: Mutex<BTreeMap<String, usize>>,
}

impl CountingSource {
    fn new() -> Self {
        Self {
            inner: GrpcConnectionCache::new(),
            created: AtomicUsize::new(0),
            by_key: Mutex::new(BTreeMap::new()),
        }
    }
}

impl GrpcChannelSource for CountingSource {
    fn channel(&self, endpoint: &str) -> Result<tonic::transport::Channel, SearcherError> {
        let (result, created) = self.inner.channel_with_creation(endpoint);
        if created {
            self.created.fetch_add(1, Ordering::SeqCst);
            *self
                .by_key
                .lock()
                .expect("counting source map")
                .entry(endpoint.to_owned())
                .or_insert(0) += 1;
        }
        result
    }
}

fn parse_calls(case: &Case) -> Vec<(usize, usize)> {
    case.calls
        .iter()
        .map(|call| match call {
            serde_json::Value::Number(number) => (
                0,
                usize::try_from(number.as_u64().expect("call index")).expect("usize"),
            ),
            serde_json::Value::Array(pair) => {
                assert_eq!(pair.len(), 2, "call pair must be [cache, endpoint]");
                (
                    usize::try_from(pair[0].as_u64().expect("cache index")).expect("usize"),
                    usize::try_from(pair[1].as_u64().expect("endpoint index")).expect("usize"),
                )
            }
            other => panic!("invalid call entry {other:?}"),
        })
        .collect()
}

fn endpoint_address(
    spec: &str,
    base_port: u16,
    case_index: usize,
    endpoint_index: usize,
) -> String {
    if spec.starts_with("invalid") {
        "invalid endpoint".to_owned()
    } else {
        format!(
            "http://127.0.0.1:{}",
            base_port + case_index as u16 * MAX_ENDPOINTS + endpoint_index as u16
        )
    }
}

struct FakeSearcher;

#[tonic::async_trait]
impl searcher_pb::searcher_service_server::SearcherService for FakeSearcher {
    type SearchStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<searcher_pb::SearchResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn search(
        &self,
        _request: Request<searcher_pb::SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        let response = searcher_pb::SearchResponse {
            message: Some(searcher_pb::search_response::Message::DoneMessage(
                searcher_pb::search_response::Done { limit_hit: false },
            )),
        };
        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
            response,
        )]))))
    }
}

struct FakeSymbols;

#[tonic::async_trait]
impl symbols_pb::symbols_service_server::SymbolsService for FakeSymbols {
    type LocalCodeIntelStream =
        tokio_stream::Empty<Result<symbols_pb::LocalCodeIntelResponse, Status>>;

    async fn search(
        &self,
        _request: Request<symbols_pb::SearchRequest>,
    ) -> Result<Response<symbols_pb::SearchResponse>, Status> {
        Ok(Response::new(symbols_pb::SearchResponse::default()))
    }

    async fn local_code_intel(
        &self,
        _request: Request<symbols_pb::LocalCodeIntelRequest>,
    ) -> Result<Response<Self::LocalCodeIntelStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn symbol_info(
        &self,
        _request: Request<symbols_pb::SymbolInfoRequest>,
    ) -> Result<Response<symbols_pb::SymbolInfoResponse>, Status> {
        Ok(Response::new(symbols_pb::SymbolInfoResponse::default()))
    }

    async fn healthz(
        &self,
        _request: Request<symbols_pb::HealthzRequest>,
    ) -> Result<Response<symbols_pb::HealthzResponse>, Status> {
        Ok(Response::new(symbols_pb::HealthzResponse::default()))
    }
}

async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..5_000 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("fake gRPC server at {addr} did not start");
}

async fn use_searcher(source: Arc<dyn GrpcChannelSource>, address: &str) -> bool {
    let connector =
        sg_service_frontend::search_searcher_client::CachedSearcherConnector::new(source);
    match connector.connect(address) {
        Ok(mut client) => {
            if let Ok(response) = client.search(searcher_pb::SearchRequest::default()).await {
                let mut stream = response.into_inner();
                while stream.message().await.ok().flatten().is_some() {}
            }
            true
        }
        Err(_) => false,
    }
}

async fn use_symbols(source: Arc<dyn GrpcChannelSource>, address: &str) -> bool {
    let connector = sg_service_frontend::symbols_client::CachedSymbolsConnector::new(source);
    match connector.connect(address) {
        Ok(mut client) => {
            let _ = client.search(symbols_pb::SearchRequest::default()).await;
            true
        }
        Err(_) => false,
    }
}

fn run_case(
    runtime: &tokio::runtime::Runtime,
    case_index: usize,
    case: &Case,
    base_port: u16,
) -> CaseResult {
    let calls = parse_calls(case);
    let addresses: Vec<String> = case
        .endpoints
        .iter()
        .enumerate()
        .map(|(index, spec)| endpoint_address(spec, base_port, case_index, index))
        .collect();

    let mut handles = Vec::new();
    for (index, spec) in case.endpoints.iter().enumerate() {
        if spec.starts_with("invalid") {
            continue;
        }
        let address = addresses[index]
            .strip_prefix("http://")
            .expect("server address")
            .to_owned();
        let addr: SocketAddr = address.parse().expect("socket address");
        handles.push(runtime.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    searcher_pb::searcher_service_server::SearcherServiceServer::new(FakeSearcher),
                )
                .add_service(
                    symbols_pb::symbols_service_server::SymbolsServiceServer::new(FakeSymbols),
                )
                .serve(addr)
                .await;
        }));
        runtime.block_on(wait_for_server(addr));
    }

    let sources: Vec<Arc<CountingSource>> = (0..case.caches.max(1))
        .map(|_| Arc::new(CountingSource::new()))
        .collect();
    let concurrency = case.concurrency.max(1);
    let service = case.service.clone();
    let sources_for_tasks = sources.clone();
    let addresses_for_tasks = addresses.clone();
    let calls_for_tasks = calls.clone();
    let endpoint_total = addresses.len();

    let errors = runtime.block_on(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..concurrency {
            let sources = sources_for_tasks.clone();
            let addresses = addresses_for_tasks.clone();
            let calls = calls_for_tasks.clone();
            let service = service.clone();
            let endpoint_count = addresses.len();
            tasks.spawn(async move {
                let mut errors = vec![0usize; endpoint_count];
                for position in (worker..calls.len()).step_by(concurrency) {
                    let (cache_index, endpoint_index) = calls[position];
                    let source: Arc<dyn GrpcChannelSource> = sources[cache_index].clone();
                    let used = if service == "symbols" {
                        use_symbols(source, &addresses[endpoint_index]).await
                    } else {
                        use_searcher(source, &addresses[endpoint_index]).await
                    };
                    if !used {
                        errors[endpoint_index] += 1;
                    }
                }
                errors
            });
        }
        let mut total = vec![0usize; endpoint_total];
        while let Some(result) = tasks.join_next().await {
            let partial = result.expect("worker task");
            for (index, count) in partial.into_iter().enumerate() {
                total[index] += count;
            }
        }
        total
    });

    // Attribute successful channel constructions back to endpoint indices.
    let mut key_to_endpoint = BTreeMap::new();
    for (index, address) in addresses.iter().enumerate() {
        key_to_endpoint.insert(address.clone(), index);
        key_to_endpoint.insert(connection_key(address), index);
    }
    let mut connections_by_endpoint = vec![0usize; addresses.len()];
    for source in &sources {
        for (key, count) in source.by_key.lock().expect("counting source map").iter() {
            if let Some(index) = key_to_endpoint.get(key) {
                connections_by_endpoint[*index] += *count;
            }
        }
    }

    let mut calls_by_endpoint = vec![0usize; addresses.len()];
    for (_, endpoint_index) in &calls {
        calls_by_endpoint[*endpoint_index] += 1;
    }

    let mut by_endpoint = BTreeMap::new();
    for index in 0..addresses.len() {
        by_endpoint.insert(
            index.to_string(),
            EndpointResult {
                calls: calls_by_endpoint[index],
                connections: connections_by_endpoint[index],
                errors: errors[index],
            },
        );
    }

    let result = CaseResult {
        name: case.name.clone(),
        calls: calls.len(),
        connections: connections_by_endpoint.iter().sum(),
        errors: errors.iter().sum(),
        by_endpoint,
    };

    for handle in handles {
        handle.abort();
    }
    result
}

#[test]
fn grpc_conn_cache_diff() {
    if std::env::var("SG_M9IW_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9IW_CORPUS").expect("M9IW_CORPUS must be set when SG_M9IW_DIFF=1");
    let out_path = std::env::var("M9IW_OUT").expect("M9IW_OUT must be set when SG_M9IW_DIFF=1");
    let base_port: u16 = std::env::var("M9IW_BASE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(39700);

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build test runtime");

    let entries: Vec<CaseResult> = cases
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
