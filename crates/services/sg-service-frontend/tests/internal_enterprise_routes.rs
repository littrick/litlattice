//! M9.8eo differential harness: the internal routes whose OSS defaults are
//! `enterprise.DefaultServices` handlers (`/lsif/upload`, `/scip/upload`,
//! `/compute/stream`), mirroring
//! `cmd/frontend/internal/httpapi/zz_internal_enterprise_routes_harness_test.go`.
//!
//! Serves the real internal handler on an ephemeral port, issues raw HTTP/1.1
//! requests, and prints one `ce ` line per case with the status, `Location`,
//! `Content-Type`, `X-Content-Type-Options` and body.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_frontend::middleware;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn live() -> LiveConfig {
    let site = SiteConfiguration::parse("{}").expect("site");
    let service_connections = ServiceConnections {
        git_servers: vec!["gitserver-0:3178".to_owned()],
        ..ServiceConnections::default()
    };
    LiveConfig::new(Unified {
        site,
        service_connections,
    })
}

async fn serve() -> (SocketAddr, tokio_util::sync::CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handler = middleware::internal_handler(Arc::new(live()));
    let token = shutdown.clone();
    tokio::spawn(async move {
        let _ = sg_http::serve_http(listener, handler, token).await;
    });
    (addr, shutdown)
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn field(&self, name: &str) -> String {
        match self.headers.iter().find(|(key, _)| key == name) {
            None => "-".to_owned(),
            Some((_, value)) => format!("{value:?}"),
        }
    }
}

async fn fetch(addr: SocketAddr, method: &str, target: &str) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let request =
        format!("{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("header end");
    let head = String::from_utf8_lossy(&raw[..split]);
    let body = raw[split + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status");
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect();
    Response {
        status,
        headers,
        body,
    }
}

async fn run(addr: SocketAddr, name: &str, method: &str, target: &str) {
    let response = fetch(addr, method, target).await;
    let body = String::from_utf8_lossy(&response.body);
    println!(
        "ce {name} status={} location={} ctype={} xcto={} body={:?}",
        response.status,
        response.field("location"),
        response.field("content-type"),
        response.field("x-content-type-options"),
        body,
    );
}

#[tokio::test]
async fn internal_enterprise_routes_harness() {
    if std::env::var("SG_INTERNAL_ENTERPRISE_ROUTES_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let (addr, shutdown) = serve().await;
    // Warm-up: ensure the listener task accepted before the first case.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cases = [
        ("lsif-post", "POST", "/lsif/upload"),
        ("lsif-get", "GET", "/lsif/upload"),
        ("scip-post", "POST", "/scip/upload"),
        ("scip-head", "HEAD", "/scip/upload"),
        ("scip-get", "GET", "/scip/upload"),
        ("compute-get", "GET", "/compute/stream"),
        ("compute-post", "POST", "/compute/stream"),
        ("compute-head", "HEAD", "/compute/stream"),
        ("lsif-trailing", "POST", "/lsif/upload/"),
        ("scip-trailing", "POST", "/scip/upload/"),
        ("compute-trailing", "GET", "/compute/stream/"),
        ("unknown", "GET", "/nope"),
    ];
    for (name, method, target) in cases {
        run(addr, name, method, target).await;
    }
    shutdown.cancel();
}
