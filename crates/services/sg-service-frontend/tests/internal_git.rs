//! M9.8en differential harness: the internal HTTP surface
//! (`/git/*` redirects + `/ping`), mirroring
//! `cmd/frontend/internal/httpapi/zz_internal_git_harness_test.go`.
//!
//! Starts the real internal handler on an ephemeral port, issues raw HTTP/1.1
//! requests (Go's server and ours both answer wire-level), and prints one
//! `ci ` line per case with the status, `Location`, `Content-Type`,
//! `X-Content-Type-Options` and body.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_frontend::middleware;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const GITSERVERS: [&str; 3] = ["gitserver-0:3178", "gitserver-1:3178", "gitserver-2:3178"];
const PIN: (&str, &str) = ("pinned.example.com/r", "gitserver-2:3178");

fn live() -> LiveConfig {
    let site = SiteConfiguration::parse(
        r#"{"experimentalFeatures":{"gitServerPinnedRepos":{"pinned.example.com/r":"gitserver-2:3178"}}}"#,
    )
    .expect("site");
    let service_connections = ServiceConnections {
        git_servers: GITSERVERS.iter().map(|addr| (*addr).to_owned()).collect(),
        ..ServiceConnections::default()
    };
    let _ = PIN;
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
    let handler = middleware::internal_handler(
        Arc::new(live()),
        Arc::new(sg_service_frontend::graphql_api::UnportedGraphqlDeps),
        Arc::new(sg_service_frontend::search_stream::UnavailableDeps),
    );
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
        "ci {name} status={} location={} ctype={} xcto={} body={:?}",
        response.status,
        response.field("location"),
        response.field("content-type"),
        response.field("x-content-type-options"),
        body,
    );
}

#[tokio::test]
async fn internal_git_harness() {
    if std::env::var("SG_INTERNAL_GIT_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let (addr, shutdown) = serve().await;
    // Warm-up: ensure the listener task accepted before the first case.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cases = [
        ("ping-get", "GET", "/ping"),
        ("ping-post", "POST", "/ping"),
        ("ping-trailing", "GET", "/ping/"),
        (
            "git-info-get",
            "GET",
            "/git/github.com/foo/bar/info/refs?service=git-upload-pack",
        ),
        ("git-info-post", "POST", "/git/github.com/foo/bar/info/refs"),
        (
            "git-upload-get",
            "GET",
            "/git/gitlab.com/group/sub/repo/git-upload-pack",
        ),
        (
            "git-upload-post",
            "POST",
            "/git/gitlab.com/group/sub/repo/git-upload-pack?x=1",
        ),
        ("git-pinned", "GET", "/git/pinned.example.com/r/info/refs"),
        ("clean-double-slash", "GET", "/git//x/info/refs?a=b"),
        ("clean-dot", "GET", "/git/a/../b/info/refs"),
        ("unknown", "GET", "/nope"),
        (
            "git-trailing-slash",
            "GET",
            "/git/github.com/foo/bar/info/refs/",
        ),
        (
            "encoded-space",
            "GET",
            "/git/github.com/foo%20bar/info/refs",
        ),
        ("encoded-slash", "GET", "/git/a%2Fb/info/refs"),
        ("case-sensitive", "GET", "/Git/x/info/refs"),
    ];
    for (name, method, target) in cases {
        run(addr, name, method, target).await;
    }
    shutdown.cancel();
}
