#![forbid(unsafe_code)]

//! `symbols` service: gRPC `symbols.v1.SymbolsService` and HTTP `/healthz`
//! multiplexed on one cleartext port (`:3184`), matching the Go baseline.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio::net::TcpListener;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_proto::symbols as pb;

pub mod config;
pub mod ctags;
pub mod gitserver;
pub mod tar;

use config::SymbolsConfig;

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

        let routes = tonic::service::Routes::new(
            pb::symbols_service_server::SymbolsServiceServer::new(SymbolsGrpc),
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

        sg_http::serve(listener, routes, http, ctx.shutdown.clone())
            .await
            .map_err(|err| ServiceError::with_source("symbols http server failed", err))
    }
}

#[derive(Default)]
struct SymbolsGrpc;

#[tonic::async_trait]
impl pb::symbols_service_server::SymbolsService for SymbolsGrpc {
    async fn search(
        &self,
        _request: GrpcRequest<pb::SearchRequest>,
    ) -> Result<GrpcResponse<pb::SearchResponse>, Status> {
        Err(Status::unimplemented("Search is not implemented yet"))
    }

    type LocalCodeIntelStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<pb::LocalCodeIntelResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn local_code_intel(
        &self,
        _request: GrpcRequest<pb::LocalCodeIntelRequest>,
    ) -> Result<GrpcResponse<Self::LocalCodeIntelStream>, Status> {
        Err(Status::unimplemented(
            "LocalCodeIntel is not implemented yet",
        ))
    }

    async fn symbol_info(
        &self,
        _request: GrpcRequest<pb::SymbolInfoRequest>,
    ) -> Result<GrpcResponse<pb::SymbolInfoResponse>, Status> {
        Err(Status::unimplemented("SymbolInfo is not implemented yet"))
    }

    async fn healthz(
        &self,
        _request: GrpcRequest<pb::HealthzRequest>,
    ) -> Result<GrpcResponse<pb::HealthzResponse>, Status> {
        Ok(GrpcResponse::new(pb::HealthzResponse {}))
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
