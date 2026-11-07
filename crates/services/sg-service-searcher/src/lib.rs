#![forbid(unsafe_code)]

//! `searcher` service: gRPC `searcher.v1.SearcherService` and HTTP
//! `/healthz` multiplexed on one cleartext port (`:3181`), matching the Go
//! baseline.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_proto::searcher as pb;

pub mod chunk;
pub mod config;
pub mod debug;
pub mod filter;
pub mod langmatch;
pub mod matchtree;
pub mod metrics;
pub mod pathmatch;
pub mod protocol;
pub mod regex_search;
pub mod search;
pub mod sender;
pub mod store;
pub mod zipcache;

use config::SearcherConfig;
use protocol::Request as SearchRequest;
use search::{SearchService, ServiceError as SearchServiceError};
use sender::LimitedStream;
use store::{ArchiveFetcher, Store};

/// Baseline `cmd/searcher/shared` binds `:3181` by default.
pub const DEFAULT_ADDR: &str = ":3181";

/// Baseline `makeHTTPServer` returns lowercase `ok`.
const HEALTHZ_BODY: &str = "ok";

pub fn service_name() -> &'static str {
    "searcher"
}

/// The searcher service.
pub struct SearcherService {
    addr: String,
    /// `SRC_PROF_HTTP`; `None` disables the debug server (baseline noop server).
    prof_addr: Option<String>,
    config: Option<SearcherConfig>,
    /// Test hook: replaces the gitserver-backed [`ArchiveFetcher`].
    fetcher: Option<Arc<dyn ArchiveFetcher>>,
    /// Test hook: replaces `SEARCHER_CACHE_DIR`.
    cache_dir: Option<std::path::PathBuf>,
}

impl SearcherService {
    pub fn new() -> Self {
        Self {
            addr: DEFAULT_ADDR.to_owned(),
            prof_addr: None,
            config: None,
            fetcher: None,
            cache_dir: None,
        }
    }

    /// Test hook: bind a different address.
    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            prof_addr: None,
            config: None,
            fetcher: None,
            cache_dir: None,
        }
    }

    /// Test hook: serve archives from `fetcher` instead of gitserver.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn ArchiveFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Test hook: keep fetched archives under `dir`.
    pub fn with_cache_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }
}

impl Default for SearcherService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for SearcherService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        // Registered by the baseline `internal/debugserver` init in every
        // service binary; an empty value disables the server.
        let prof_addr = env.get(
            "SRC_PROF_HTTP",
            debug::DEFAULT_ADDR,
            "net/http/pprof http bind address.",
        );
        self.prof_addr = Some(prof_addr).filter(|addr| !addr.is_empty());
        let config = SearcherConfig::load(env)?;
        // `SEARCHER_ADDR` is only a fallback default; tests may override it.
        if self.addr == DEFAULT_ADDR {
            self.addr = config.listen_address.clone();
        }
        self.config = Some(config);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.addr.is_empty() {
            return Err(ConfigError::new(
                "searcher listen address must not be empty",
            ));
        }
        self.config
            .as_ref()
            .ok_or_else(|| ConfigError::new("searcher configuration was not loaded"))?
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("searcher configuration was not loaded"))?;

        let cache_dir = self
            .cache_dir
            .clone()
            .unwrap_or_else(|| config.cache_dir.clone());
        let fetcher: Arc<dyn ArchiveFetcher> = match self.fetcher.clone() {
            Some(fetcher) => fetcher,
            None => Arc::new(sg_git::GitserverClient::new(config.gitserver_addrs.clone())),
        };
        let store = Store::new(
            fetcher,
            &cache_dir,
            config.cache_size_bytes,
            config.background_timeout,
            config.gitserver_addrs.len(),
            ctx.shutdown.clone(),
        );
        store.start();
        let search_service = Arc::new(SearchService::new(Arc::clone(&store)));

        let listener = TcpListener::bind(self.addr.as_str()).await.map_err(|err| {
            ServiceError::with_source(format!("failed to listen on {}", self.addr), err)
        })?;

        let streams = TaskTracker::new();
        let routes = tonic::service::Routes::new(
            pb::searcher_service_server::SearcherServiceServer::new(SearcherGrpc {
                service: search_service,
                shutdown: ctx.shutdown.clone(),
                streams: streams.clone(),
            }),
        );
        let http: sg_http::HttpHandler = std::sync::Arc::new(|req: Request<Incoming>| {
            Box::pin(async move { http_response(req) })
        });

        tracing::info!(
            addr = %self.addr,
            cache_dir = %config.cache_dir.display(),
            gitservers = ?config.gitserver_addrs,
            "searcher listening"
        );
        ctx.ready.mark_ready();

        sg_metrics::register_service_metadata(sg_metrics::VERSION);
        let mut background = tokio::task::JoinSet::new();
        if let Some(prof_addr) = self.prof_addr.clone() {
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move { debug::serve(&prof_addr, shutdown).await });
        }

        let served = sg_http::serve(listener, routes, http, ctx.shutdown.clone()).await;
        // The debug server and in-flight searches may outlive an early
        // main-server failure.
        ctx.shutdown.cancel();
        streams.close();
        streams.wait().await;
        store.shutdown().await;
        while background.join_next().await.is_some() {}
        served.map_err(|err| ServiceError::with_source("searcher http server failed", err))
    }
}

/// Buffered matches between the blocking search workers and the gRPC stream.
const STREAM_BUFFER: usize = 64;

struct SearcherGrpc {
    service: Arc<SearchService>,
    shutdown: CancellationToken,
    streams: TaskTracker,
}

#[tonic::async_trait]
impl pb::searcher_service_server::SearcherService for SearcherGrpc {
    type SearchStream =
        Pin<Box<dyn Stream<Item = Result<pb::SearchResponse, Status>> + Send + 'static>>;

    async fn search(
        &self,
        request: GrpcRequest<pb::SearchRequest>,
    ) -> Result<GrpcResponse<Self::SearchStream>, Status> {
        let mut request = SearchRequest::from_proto(request.get_ref());

        // BACKCOMPAT: Old frontends send neither of these fields, but we still
        // want to search file content in that case.
        if !request.pattern_info.pattern_matches_content
            && !request.pattern_info.pattern_matches_path
        {
            request.pattern_info.pattern_matches_content = true;
        }
        request.validate().map_err(Status::invalid_argument)?;

        // No limit for streaming search since upstream limits will either be
        // sent in the request, or propagated by a cancelled context.
        if request.pattern_info.limit == 0 {
            request.pattern_info.limit = i32::MAX;
        }

        // Per-request token: cancelled when the limit is hit, when the client
        // drops the stream, or when the service shuts down.
        let cancel = self.shutdown.child_token();
        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<pb::SearchResponse, Status>>(STREAM_BUFFER);

        let sender = {
            let tx = tx.clone();
            let cancel_on_drop = cancel.clone();
            Arc::new(LimitedStream::new(
                request.pattern_info.limit,
                Box::new(move |file_match| {
                    let response = pb::SearchResponse {
                        message: Some(pb::search_response::Message::FileMatch(
                            file_match.to_proto(),
                        )),
                    };
                    // The stream is dropped when the client disconnects; stop
                    // the workers instead of blocking forever.
                    if tx.blocking_send(Ok(response)).is_err() {
                        cancel_on_drop.cancel();
                    }
                }),
                cancel.clone(),
            ))
        };

        let service = Arc::clone(&self.service);
        self.streams.spawn(async move {
            let result = service.search(&request, Arc::clone(&sender)).await;
            match result {
                Ok(()) => {
                    let done = pb::SearchResponse {
                        message: Some(pb::search_response::Message::DoneMessage(
                            pb::search_response::Done {
                                limit_hit: sender.limit_hit(),
                            },
                        )),
                    };
                    let _ = tx.send(Ok(done)).await;
                }
                Err(err) => {
                    let _ = tx.send(Err(convert_to_grpc_error(&cancel, err))).await;
                }
            }
        });

        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Baseline `convertToGRPCError`: cancellation wins over the underlying error;
/// an already-typed status passes through; everything else is `Unknown`
/// (`Aborted` for killed subprocesses).
fn convert_to_grpc_error(cancel: &CancellationToken, err: SearchServiceError) -> Status {
    if cancel.is_cancelled() {
        return Status::cancelled("context canceled");
    }
    let message = err.to_string();
    if message.contains("signal: killed") {
        return Status::aborted(message);
    }
    Status::unknown(message)
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

    use bytes::Bytes;
    use sg_core::{LockedEnv, ReadyHandle, ServiceContext};
    use sg_git::TarByteStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// In-memory gitserver serving one fixed tar archive.
    struct FakeFetcher {
        tar: Vec<u8>,
    }

    #[async_trait]
    impl ArchiveFetcher for FakeFetcher {
        async fn archive(
            &self,
            _repo: &str,
            _commit: &str,
            _paths: &[String],
        ) -> Result<TarByteStream, String> {
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            let tar = self.tar.clone();
            tokio::task::spawn(async move {
                for chunk in tar.chunks(7) {
                    let _ = sender.send(Ok(Bytes::copy_from_slice(chunk))).await;
                }
            });
            Ok(Box::pin(ReceiverStream::new(receiver)))
        }

        async fn read_file_optional(
            &self,
            _repo: &str,
            _commit: &str,
            _path: &str,
        ) -> Result<Option<Vec<u8>>, String> {
            Ok(None)
        }
    }

    fn make_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in files {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).expect("path");
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, *data).expect("append");
        }
        builder.into_inner().expect("finish")
    }

    fn fetcher(files: &[(&str, &[u8])]) -> Arc<dyn ArchiveFetcher> {
        Arc::new(FakeFetcher {
            tar: make_tar(files),
        })
    }

    fn context(shutdown: CancellationToken, ready: ReadyHandle) -> ServiceContext {
        let mut env = EnvReader::new();
        let mut service = Box::new(SearcherService::with_addr("127.0.0.1:0"));
        service.configure(&mut env).expect("configure");
        env.lock();
        let ctx = ServiceContext {
            service_name: service_name(),
            shutdown,
            ready,
            env: Arc::new(LockedEnv::new(env)),
        };
        // `configure` above is only used to populate the registry; the service
        // under test is rebuilt in `spawn`.
        ctx
    }

    fn probe_addr() -> std::net::SocketAddr {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        let addr = probe.local_addr().expect("local addr");
        drop(probe);
        addr
    }

    /// Start a server over `fetcher` and return its address plus a shutdown
    /// handle.
    async fn start(
        fetcher: Arc<dyn ArchiveFetcher>,
    ) -> (
        std::net::SocketAddr,
        tempfile::TempDir,
        CancellationToken,
        tokio::task::JoinHandle<Result<(), ServiceError>>,
    ) {
        let addr = probe_addr();
        let cache = tempfile::tempdir().expect("cache dir");
        let shutdown = CancellationToken::new();
        let ready = ReadyHandle::new();
        let ctx = context(shutdown.clone(), ready.clone());

        let mut service = Box::new(
            SearcherService::with_addr(addr.to_string())
                .with_fetcher(fetcher)
                .with_cache_dir(cache.path()),
        );
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        let server = tokio::spawn(async move { service.start(ctx).await });
        ready.wait_ready().await;
        (addr, cache, shutdown, server)
    }

    async fn connect(
        addr: std::net::SocketAddr,
    ) -> pb::searcher_service_client::SearcherServiceClient<tonic::transport::Channel> {
        pb::searcher_service_client::SearcherServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("grpc connect")
    }

    fn request(pattern: &str, limit: i64) -> pb::SearchRequest {
        pb::SearchRequest {
            repo: "github.com/gorilla/mux".to_owned(),
            commit_oid: "0123456789012345678901234567890123456789".to_owned(),
            pattern_info: Some(pb::PatternInfo {
                query: Some(pb::QueryNode {
                    value: Some(pb::query_node::Value::Pattern(pb::PatternNode {
                        value: pattern.to_owned(),
                        ..pb::PatternNode::default()
                    })),
                }),
                limit,
                ..pb::PatternInfo::default()
            }),
            ..pb::SearchRequest::default()
        }
    }

    #[tokio::test]
    async fn serves_http_healthz_and_grpc_search_on_one_port() {
        let fetcher = fetcher(&[
            ("mux.go", b"package mux\n// route matches\n"),
            ("router.go", b"package mux\n// no hit\n"),
            ("README.md", b"routes\n"),
        ]);
        let (addr, _cache, shutdown, server) = start(fetcher).await;

        let mut client = connect(addr).await;
        let mut stream = client
            .search(request("route", 0))
            .await
            .expect("search")
            .into_inner();

        let mut paths = Vec::new();
        let mut done = None;
        while let Some(message) = stream.next().await {
            let message = message.expect("ok");
            match message.message {
                Some(pb::search_response::Message::FileMatch(file_match)) => {
                    paths.push(String::from_utf8_lossy(&file_match.path).into_owned());
                }
                Some(pb::search_response::Message::DoneMessage(done_message)) => {
                    done = Some(done_message);
                }
                None => {}
            }
        }
        paths.sort();
        assert_eq!(paths, vec!["README.md".to_owned(), "mux.go".to_owned()]);
        assert!(!done.expect("done").limit_hit);

        // Plain HTTP/1.1 on the same port.
        let mut socket = tokio::net::TcpStream::connect(addr).await.expect("tcp");
        socket
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        assert!(response.ends_with("ok"), "got: {response}");

        shutdown.cancel();
        server.await.expect("server task").expect("clean stop");
    }

    #[tokio::test]
    async fn stops_at_the_limit_and_reports_limit_hit() {
        let fetcher = fetcher(&[
            ("a.go", b"route\n"),
            ("b.go", b"route\n"),
            ("c.go", b"route\n"),
        ]);
        let (addr, _cache, shutdown, server) = start(fetcher).await;

        let mut client = connect(addr).await;
        let mut stream = client
            .search(request("route", 1))
            .await
            .expect("search")
            .into_inner();

        let mut matches = 0;
        let mut done = None;
        while let Some(message) = stream.next().await {
            let message = message.expect("ok");
            match message.message {
                Some(pb::search_response::Message::FileMatch(_)) => matches += 1,
                Some(pb::search_response::Message::DoneMessage(done_message)) => {
                    done = Some(done_message)
                }
                None => {}
            }
        }
        assert_eq!(matches, 1);
        assert!(done.expect("done").limit_hit);

        shutdown.cancel();
        server.await.expect("server task").expect("clean stop");
    }

    #[tokio::test]
    async fn rejects_invalid_requests_before_streaming() {
        let (addr, _cache, shutdown, server) = start(fetcher(&[])).await;

        let mut client = connect(addr).await;
        let status = client
            .search(pb::SearchRequest::default())
            .await
            .expect_err("invalid request");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "Repo must be non-empty");

        shutdown.cancel();
        server.await.expect("server task").expect("clean stop");
    }
}
