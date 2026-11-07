//! P1: serve gRPC and plain HTTP on one cleartext port (h2c), reproducing the
//! Go baseline predicate from `internalgrpc.MultiplexHandlers`:
//!
//!   r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc")
//!
//! Everything else goes to the HTTP handler.
//!
//! Finding: hyper's auto builder supports h2c **prior knowledge** but not the
//! RFC 7540 §3.2 HTTP/1.1 upgrade handshake (Go's `h2c.NewHandler` supports
//! both). An upgrade request is therefore served as ordinary HTTP/1.1; the
//! client still gets a valid response, just not an upgraded connection.
//! See README.md for analysis and mitigation options.

use std::net::SocketAddr;

use http::{HeaderValue, Request, Response, Version};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tonic::body::BoxBody;
use tonic::service::Routes;
use tower::Service;

pub mod pb {
    tonic::include_proto!("poc.echo");
}

#[derive(Clone, Default)]
struct EchoService;

#[tonic::async_trait]
impl pb::echo_server::Echo for EchoService {
    async fn say(
        &self,
        request: tonic::Request<pb::EchoRequest>,
    ) -> Result<tonic::Response<pb::EchoReply>, tonic::Status> {
        // Prove custom metadata survives the multiplexer.
        let meta = request
            .metadata()
            .get("x-test")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("none")
            .to_owned();
        let message = format!("echo: {} (x-test={meta})", request.into_inner().message);
        Ok(tonic::Response::new(pb::EchoReply { message }))
    }
}

/// Exact port of the Go multiplex predicate. `Header.Get` returns the first
/// value and `strings.Contains` is case-sensitive.
fn is_grpc(req: &Request<Incoming>) -> bool {
    req.version() == Version::HTTP_2
        && req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("application/grpc"))
}

async fn http_handler(req: Request<Incoming>) -> Response<BoxBody> {
    let body = serde_json::json!({
        "handler": "http",
        "method": req.method().as_str(),
        "path": req.uri().path(),
        "version": format!("{:?}", req.version()),
        "content_type": req.headers().get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
    });
    let mut resp = Response::new(tonic::body::boxed(Full::new(Bytes::from(body.to_string()))));
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    resp
}

async fn serve(addr: SocketAddr) {
    let routes = Routes::new(pb::echo_server::EchoServer::new(EchoService));
    let listener = TcpListener::bind(addr).await.expect("bind");
    println!("listening on {addr}");
    loop {
        let (stream, _peer) = listener.accept().await.expect("accept");
        let routes = routes.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req: Request<Incoming>| {
                let mut routes = routes.clone();
                async move {
                    if is_grpc(&req) {
                        let (parts, body) = req.into_parts();
                        let req = Request::from_parts(parts, tonic::body::boxed(body));
                        routes.call(req).await.map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                        })
                    } else {
                        Ok::<_, std::io::Error>(http_handler(req).await)
                    }
                }
            });
            if let Err(err) = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await
            {
                eprintln!("connection error: {err}");
            }
        });
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("client") => {
            let url = args.get(2).cloned().unwrap_or_else(|| "http://127.0.0.1:18080".into());
            let mut client = pb::echo_client::EchoClient::connect(url)
                .await
                .expect("connect");
            let mut req = tonic::Request::new(pb::EchoRequest {
                message: "hello".into(),
            });
            req.metadata_mut()
                .insert("x-test", "via-multiplex".parse().expect("metadata"));
            let reply = client.say(req).await.expect("rpc").into_inner();
            println!("{}", reply.message);
        }
        _ => {
            let addr: SocketAddr = args
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| "127.0.0.1:18080".parse().unwrap());
            serve(addr).await;
        }
    }
}
