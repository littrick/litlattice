#![forbid(unsafe_code)]

//! `searcher` service: gRPC `searcher.v1.SearcherService` and HTTP
//! `/healthz` multiplexed on one cleartext port (`:3181`), matching the Go
//! baseline.

use std::pin::Pin;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio::net::TcpListener;
use tokio_stream::Stream;
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
pub mod store;
pub mod zipcache;

use config::SearcherConfig;
use protocol::Request as SearchRequest;

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
}

impl SearcherService {
    pub fn new() -> Self {
        Self {
            addr: DEFAULT_ADDR.to_owned(),
            prof_addr: None,
            config: None,
        }
    }

    /// Test hook: bind a different address.
    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            prof_addr: None,
            config: None,
        }
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

        let listener = TcpListener::bind(self.addr.as_str()).await.map_err(|err| {
            ServiceError::with_source(format!("failed to listen on {}", self.addr), err)
        })?;

        let routes = tonic::service::Routes::new(
            pb::searcher_service_server::SearcherServiceServer::new(SearcherGrpc),
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
        // The debug server may outlive an early main-server failure.
        ctx.shutdown.cancel();
        while background.join_next().await.is_some() {}
        served.map_err(|err| ServiceError::with_source("searcher http server failed", err))
    }
}

struct SearcherGrpc;

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

        // M8.6 replaces the immediate `Done` with the unindexed search
        // pipeline; the stream shape and termination semantics are fixed here.
        let responses = vec![Ok(pb::SearchResponse {
            message: Some(pb::search_response::Message::DoneMessage(
                pb::search_response::Done { limit_hit: false },
            )),
        })];
        Ok(GrpcResponse::new(Box::pin(tokio_stream::iter(responses))))
    }
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

    use sg_core::{LockedEnv, ReadyHandle, ServiceContext};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    use super::*;

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

    #[tokio::test]
    async fn serves_http_healthz_and_grpc_search_on_one_port() {
        let addr = probe_addr();
        let shutdown = CancellationToken::new();
        let ready = ReadyHandle::new();
        let ctx = context(shutdown.clone(), ready.clone());

        let mut service = Box::new(SearcherService::with_addr(addr.to_string()));
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        let server = tokio::spawn(async move { service.start(ctx).await });
        ready.wait_ready().await;

        let mut client =
            pb::searcher_service_client::SearcherServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("grpc connect");
        let mut stream = client
            .search(pb::SearchRequest {
                repo: "github.com/gorilla/mux".to_owned(),
                commit_oid: "0123456789012345678901234567890123456789".to_owned(),
                pattern_info: Some(pb::PatternInfo {
                    query: Some(pb::QueryNode {
                        value: Some(pb::query_node::Value::Pattern(pb::PatternNode {
                            value: "route".to_owned(),
                            ..pb::PatternNode::default()
                        })),
                    }),
                    ..pb::PatternInfo::default()
                }),
                ..pb::SearchRequest::default()
            })
            .await
            .expect("search")
            .into_inner();

        let message = stream.next().await.expect("done").expect("ok");
        assert!(matches!(
            message.message,
            Some(pb::search_response::Message::DoneMessage(
                pb::search_response::Done { limit_hit: false }
            ))
        ));
        assert!(stream.next().await.is_none());

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
    async fn rejects_invalid_requests_before_streaming() {
        let addr = probe_addr();
        let shutdown = CancellationToken::new();
        let ready = ReadyHandle::new();
        let ctx = context(shutdown.clone(), ready.clone());

        let mut service = Box::new(SearcherService::with_addr(addr.to_string()));
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        let server = tokio::spawn(async move { service.start(ctx).await });
        ready.wait_ready().await;

        let mut client =
            pb::searcher_service_client::SearcherServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("grpc connect");
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
