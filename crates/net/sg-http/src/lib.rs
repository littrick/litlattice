#![forbid(unsafe_code)]

//! `sg-http`: HTTP/gRPC multiplexing on one cleartext port.
//!
//! Ports the baseline `internal/grpc.MultiplexHandlers` predicate:
//!
//! ```text
//! r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc")
//! ```
//!
//! Matching requests are dispatched to the configured `tonic::service::Routes`;
//! everything else is served by the plain HTTP handler. hyper's auto builder
//! supports h2c prior knowledge but not the HTTP/1.1 upgrade handshake; see the
//! P1 PoC for the accepted difference.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response, Version};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;
use tonic::service::Routes;
use tower::Service;

/// Maximum time to wait for in-flight connections after shutdown.
const DRAIN_GRACE: Duration = Duration::from_secs(10);

pub type HttpFuture = Pin<Box<dyn Future<Output = Response<BoxBody>> + Send>>;

/// Plain-HTTP branch of the multiplexer.
pub type HttpHandler = Arc<dyn Fn(Request<Incoming>) -> HttpFuture + Send + Sync>;

/// Exact port of the Go multiplex predicate. `Header.Get` returns the first
/// value and `strings.Contains` is case-sensitive.
pub fn is_grpc<B>(req: &Request<B>) -> bool {
    req.version() == Version::HTTP_2
        && req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("application/grpc"))
}

/// Bind a TCP listener, accepting Go-style addresses with an empty host.
///
/// The baseline defaults (`SEARCHER_ADDR=:3181`, `SYMBOLS_ADDR=:3184`,
/// `SRC_PROF_HTTP=:6060`) use `:port`, which Go treats as all interfaces.
/// glibc's `getaddrinfo` rejects the empty host that `std::net::ToSocketAddrs`
/// passes through, so expand it to the IPv4 wildcard explicitly.
pub async fn listen(addr: &str) -> io::Result<TcpListener> {
    match addr.strip_prefix(':') {
        Some(port) => TcpListener::bind(format!("0.0.0.0:{port}")).await,
        None => TcpListener::bind(addr).await,
    }
}

/// Go's `net` enables `TCP_NODELAY` on accepted connections; without it a
/// streamed gRPC response can sit in the kernel until the delayed-ACK timer
/// fires (~40ms), which dominates small Search responses.
fn enable_nodelay(stream: &tokio::net::TcpStream, peer: std::net::SocketAddr) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::debug!(%peer, error = %err, "failed to set TCP_NODELAY");
    }
}

/// Serve gRPC and plain HTTP on an already-bound listener until `shutdown` is
/// cancelled.
pub async fn serve(
    listener: TcpListener,
    routes: Routes,
    http: HttpHandler,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut connections: JoinSet<()> = JoinSet::new();
    // Sends GOAWAY to in-flight HTTP/2 connections and closes idle keep-alives.
    let graceful = GracefulShutdown::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                enable_nodelay(&stream, peer);
                let routes = routes.clone();
                let http = http.clone();
                let watcher = graceful.watcher();
                connections.spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let routes = routes.clone();
                        let http = http.clone();
                        async move { dispatch(req, routes, http).await }
                    });
                    let builder = auto::Builder::new(TokioExecutor::new());
                    let conn =
                        builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
                    if let Err(err) = watcher.watch(conn).await {
                        tracing::debug!(%peer, error = %err, "connection closed with error");
                    }
                });
            }
            Some(joined) = connections.join_next() => {
                if let Err(err) = joined {
                    // A panicking connection task must not take the server down.
                    tracing::error!(error = %err, "connection task failed");
                }
            }
        }
    }

    drain(graceful, &mut connections).await;

    Ok(())
}

/// Serve plain HTTP on an already-bound listener until `shutdown` is cancelled.
///
/// The Go baseline runs the per-service debug server (`SRC_PROF_HTTP`, which
/// exposes `/metrics`) outside the gRPC multiplexer; this is the same shape for
/// the Rust services.
pub async fn serve_http(
    listener: TcpListener,
    http: HttpHandler,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut connections: JoinSet<()> = JoinSet::new();
    let graceful = GracefulShutdown::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                enable_nodelay(&stream, peer);
                let http = http.clone();
                let watcher = graceful.watcher();
                connections.spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let http = http.clone();
                        async move { Ok::<_, std::convert::Infallible>(http(req).await) }
                    });
                    let builder = auto::Builder::new(TokioExecutor::new());
                    let conn = builder.serve_connection(TokioIo::new(stream), service);
                    if let Err(err) = watcher.watch(conn).await {
                        tracing::debug!(%peer, error = %err, "connection closed with error");
                    }
                });
            }
            Some(joined) = connections.join_next() => {
                if let Err(err) = joined {
                    tracing::error!(error = %err, "connection task failed");
                }
            }
        }
    }

    drain(graceful, &mut connections).await;

    Ok(())
}

async fn drain(graceful: GracefulShutdown, connections: &mut JoinSet<()>) {
    let drained = graceful.shutdown();
    tokio::pin!(drained);
    if tokio::time::timeout(DRAIN_GRACE, &mut drained)
        .await
        .is_err()
    {
        tracing::warn!(
            grace_seconds = DRAIN_GRACE.as_secs(),
            "connections did not drain in time, aborting"
        );
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        let _ = tokio::time::timeout(Duration::from_secs(1), drained).await;
    }
    while connections.join_next().await.is_some() {}
}

async fn dispatch(
    req: Request<Incoming>,
    mut routes: Routes,
    http: HttpHandler,
) -> Result<Response<BoxBody>, io::Error> {
    if is_grpc(&req) {
        let (parts, body) = req.into_parts();
        let req = Request::from_parts(parts, tonic::body::boxed(body));
        routes
            .call(req)
            .await
            .map_err(|err| io::Error::other(err.to_string()))
    } else {
        Ok(http(req).await)
    }
}

#[cfg(test)]
mod tests {
    use super::listen;

    #[tokio::test]
    async fn binds_go_style_wildcard_addresses() {
        let listener = listen(":0").await.expect("bind :0");
        assert!(listener.local_addr().expect("local addr").ip().is_unspecified());
    }
}
