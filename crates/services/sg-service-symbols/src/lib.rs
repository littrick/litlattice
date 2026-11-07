#![forbid(unsafe_code)]

//! `symbols` service: gRPC `symbols.v1.SymbolsService` and HTTP `/healthz`
//! multiplexed on one cleartext port (`:3184`), matching the Go baseline.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio::net::TcpListener;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_proto::symbols as pb;

pub mod cache;
pub mod config;
pub mod ctags;
pub mod gitserver;
pub mod index;
pub mod language;
pub mod search;
pub mod squirrel;
pub mod store;
pub mod tar;

use cache::Cache;
use config::SymbolsConfig;
use gitserver::{GitserverClient, GitserverReader};
use index::Indexer;
use search::SearchEngine;

/// Baseline `cmd/symbols/shared` binds a fixed address.
pub const DEFAULT_ADDR: &str = ":3184";

const HEALTHZ_BODY: &str = "OK";

pub fn service_name() -> &'static str {
    "symbols"
}

/// The symbols service. The fetch/parse building blocks are implemented; the
/// search RPCs still return `UNIMPLEMENTED` until the SQLite store lands.
pub struct SymbolsService {
    addr: String,
    config: Option<SymbolsConfig>,
}

impl SymbolsService {
    pub fn new() -> Self {
        Self {
            addr: DEFAULT_ADDR.to_owned(),
            config: None,
        }
    }

    /// Test hook: bind a different address.
    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            config: None,
        }
    }
}

impl Default for SymbolsService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for SymbolsService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        // Register the same variable the baseline package registers, so
        // `--help` lists it. The check itself runs in the binary entry point.
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        self.config = Some(SymbolsConfig::load(env)?);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.addr.is_empty() {
            return Err(ConfigError::new("symbols listen address must not be empty"));
        }
        self.config
            .as_ref()
            .ok_or_else(|| ConfigError::new("symbols configuration was not loaded"))?
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("symbols configuration was not loaded"))?;

        let listener = TcpListener::bind(self.addr.as_str()).await.map_err(|err| {
            ServiceError::with_source(format!("failed to listen on {}", self.addr), err)
        })?;

        let gitserver: Arc<dyn GitserverReader> =
            Arc::new(GitserverClient::new(config.gitserver_addrs.clone()));
        let indexer = Arc::new(Indexer::new(config.clone(), Arc::clone(&gitserver)));
        let cache = Arc::new(Cache::new(&config, indexer, &ctx.shutdown));
        cache.spawn_janitor();
        let engine = Arc::new(SearchEngine::new(Arc::clone(&cache)));

        let routes = tonic::service::Routes::new(
            pb::symbols_service_server::SymbolsServiceServer::new(SymbolsGrpc {
                engine: Arc::clone(&engine),
                gitserver: Arc::clone(&gitserver),
            }),
        );
        let http: sg_http::HttpHandler =
            Arc::new(|req: Request<Incoming>| Box::pin(async move { http_response(req) }));

        tracing::info!(
            addr = %self.addr,
            cache_dir = %config.cache_dir.display(),
            gitservers = ?config.gitserver_addrs,
            "symbols listening"
        );
        ctx.ready.mark_ready();

        let served = sg_http::serve(listener, routes, http, ctx.shutdown.clone()).await;
        cache.shutdown().await;
        served.map_err(|err| ServiceError::with_source("symbols http server failed", err))
    }
}

struct SymbolsGrpc {
    engine: Arc<SearchEngine>,
    gitserver: Arc<dyn GitserverReader>,
}

#[tonic::async_trait]
impl pb::symbols_service_server::SymbolsService for SymbolsGrpc {
    async fn search(
        &self,
        request: GrpcRequest<pb::SearchRequest>,
    ) -> Result<GrpcResponse<pb::SearchResponse>, Status> {
        let request = request.into_inner();
        let params = search::Params {
            repo: request.repo,
            commit: request.commit_id,
            query: request.query,
            is_case_sensitive: request.is_case_sensitive,
            include_patterns: request.include_patterns,
            exclude_pattern: request.exclude_pattern,
            first: i64::from(request.first),
            timeout: request.timeout.map(|timeout| {
                Duration::new(timeout.seconds.max(0) as u64, timeout.nanos.max(0) as u32)
            }),
            include_langs: request.include_langs,
            exclude_langs: request.exclude_langs,
        };

        // The baseline returns search failures in the response body and keeps
        // the RPC successful.
        let response = match self.engine.search(&params).await {
            Ok(outcome) => pb::SearchResponse {
                symbols: outcome.symbols.into_iter().map(convert_symbol).collect(),
                error: None,
                limit_hit: outcome.limit_hit,
            },
            Err(err) => {
                tracing::error!(
                    repo = %params.repo,
                    commit = %params.commit,
                    query = %params.query,
                    error = %err,
                    "symbol search failed"
                );
                pb::SearchResponse {
                    symbols: Vec::new(),
                    error: Some(err),
                    limit_hit: false,
                }
            }
        };
        Ok(GrpcResponse::new(response))
    }

    type LocalCodeIntelStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<pb::LocalCodeIntelResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    // The stream item type is fixed by tonic (`Result<_, Status>`).
    #[allow(clippy::result_large_err)]
    async fn local_code_intel(
        &self,
        request: GrpcRequest<pb::LocalCodeIntelRequest>,
    ) -> Result<GrpcResponse<Self::LocalCodeIntelStream>, Status> {
        let path = internal_repo_commit_path(request.into_inner().repo_commit_path);
        let files = BlockingFileSource {
            gitserver: Arc::clone(&self.gitserver),
        };

        // Tree-sitter parsing is CPU-bound and `read_file` is blocking, so the
        // whole squirrel request runs off the async reactor.
        let symbols = tokio::task::spawn_blocking(move || {
            let service = squirrel::Service::new(&files, None);
            service.local_code_intel(path)
        })
        .await
        .map_err(|err| Status::unknown(format!("local code intel task failed: {err}")))?
        .map_err(squirrel_status)?;

        let chunks = chunk_local_code_intel(symbols.into_iter().map(convert_local_symbol));
        let responses = chunks
            .into_iter()
            .map(|symbols| Ok(pb::LocalCodeIntelResponse { symbols }));
        Ok(GrpcResponse::new(Box::pin(tokio_stream::iter(responses))))
    }

    async fn symbol_info(
        &self,
        _request: GrpcRequest<pb::SymbolInfoRequest>,
    ) -> Result<GrpcResponse<pb::SymbolInfoResponse>, Status> {
        Err(Status::unimplemented("symbol info is not implemented yet"))
    }

    async fn healthz(
        &self,
        _request: GrpcRequest<pb::HealthzRequest>,
    ) -> Result<GrpcResponse<pb::HealthzResponse>, Status> {
        Ok(GrpcResponse::new(pb::HealthzResponse {}))
    }
}

fn convert_symbol(symbol: store::Symbol) -> pb::search_response::Symbol {
    pb::search_response::Symbol {
        name: symbol.name,
        path: symbol.path,
        line: symbol.line,
        character: symbol.character,
        kind: symbol.kind,
        language: symbol.language,
        parent: symbol.parent,
        parent_kind: symbol.parent_kind,
        signature: symbol.signature,
        file_limited: symbol.file_limited,
    }
}

/// `squirrel.readFileFunc` backed by gitserver. Only valid on a blocking
/// thread (it drives the async client with `Handle::block_on`).
struct BlockingFileSource {
    gitserver: Arc<dyn GitserverReader>,
}

impl squirrel::FileSource for BlockingFileSource {
    fn read_file(&self, path: &squirrel::RepoCommitPath) -> Result<Vec<u8>, String> {
        tokio::runtime::Handle::current().block_on(self.gitserver.read_file(
            &path.repo,
            &path.commit,
            &path.path,
        ))
    }
}

fn internal_repo_commit_path(path: Option<pb::RepoCommitPath>) -> squirrel::RepoCommitPath {
    path.map(|path| squirrel::RepoCommitPath {
        repo: path.repo,
        commit: path.commit,
        path: path.path,
    })
    .unwrap_or_default()
}

fn convert_range(range: squirrel::Range) -> pb::Range {
    pb::Range {
        row: range.row as i32,
        column: range.column as i32,
        length: range.length as i32,
    }
}

fn convert_local_symbol(symbol: squirrel::Symbol) -> pb::local_code_intel_response::Symbol {
    pb::local_code_intel_response::Symbol {
        name: symbol.name,
        hover: symbol.hover,
        def: Some(convert_range(symbol.def)),
        refs: symbol.refs.into_iter().map(convert_range).collect(),
    }
}

fn squirrel_status(err: squirrel::Error) -> Status {
    let message = err.to_string();
    match err {
        squirrel::Error::UnrecognizedFileExtension => Status::invalid_argument(message),
        squirrel::Error::UnsupportedLanguage => Status::unimplemented(message),
        squirrel::Error::Message(_) => Status::unknown(message),
    }
}

/// Baseline `internal/grpc/chunk` maximum message size.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Group symbols into protobuf messages under 1 MiB, mirroring
/// `chunk.Chunker.Send`/`Flush` (including the empty-chunk edge case).
fn chunk_local_code_intel(
    symbols: impl Iterator<Item = pb::local_code_intel_response::Symbol>,
) -> Vec<Vec<pb::local_code_intel_response::Symbol>> {
    use prost::Message;

    let mut chunks = Vec::new();
    let mut buffer = Vec::new();
    let mut size_bytes = 0usize;

    for symbol in symbols {
        if size_bytes == 0 {
            buffer.clear();
        }

        let item_size = symbol.encoded_len();
        if item_size + size_bytes >= MAX_MESSAGE_SIZE {
            chunks.push(std::mem::take(&mut buffer));
            size_bytes = 0;
        }

        buffer.push(symbol);
        size_bytes += item_size;
    }

    if size_bytes != 0 {
        chunks.push(buffer);
    }
    chunks
}

fn http_response(req: Request<Incoming>) -> Response<tonic::body::BoxBody> {
    match req.uri().path() {
        // Baseline `http.ServeMux` matches the path regardless of method.
        "/healthz" => text_response(StatusCode::OK, HEALTHZ_BODY),
        _ => text_response(StatusCode::NOT_FOUND, "404 page not found\n"),
    }
}

fn text_response(status: StatusCode, body: &str) -> Response<tonic::body::BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body.to_owned()))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sg_core::{EnvReader, LockedEnv, ReadyHandle, ServiceContext};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[tokio::test]
    async fn serves_http_healthz_and_grpc_healthz_on_one_port() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        let addr = probe.local_addr().expect("local addr");
        drop(probe);

        let shutdown = CancellationToken::new();
        let ready = ReadyHandle::new();
        let mut service = Box::new(SymbolsService::with_addr(addr.to_string()));
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        env.lock();
        let ctx = ServiceContext {
            service_name: service_name(),
            shutdown: shutdown.clone(),
            ready: ready.clone(),
            env: Arc::new(LockedEnv::new(env)),
        };
        let server = tokio::spawn(async move { service.start(ctx).await });
        ready.wait_ready().await;

        // gRPC over HTTP/2 prior knowledge.
        let mut client =
            pb::symbols_service_client::SymbolsServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("grpc connect");
        client
            .healthz(pb::HealthzRequest {})
            .await
            .expect("grpc healthz");

        // Plain HTTP/1.1 on the same port.
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("tcp");
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        assert!(response.ends_with("OK"), "got: {response}");

        shutdown.cancel();
        server.await.expect("server task").expect("clean stop");
    }
}
