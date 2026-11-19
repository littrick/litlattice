#![forbid(unsafe_code)]

//! Streaming reverse proxy to Prometheus/Alertmanager (`main.go`).
//!
//! The Go baseline uses `httputil.ReverseProxy` with a `Director` that rewrites
//! only the scheme and host, leaving the method, path, query, headers and both
//! bodies untouched. This module reproduces that behavior with a per-request
//! HTTP/1.1 client connection to `127.0.0.1:<port>`:
//!
//! * the request body is streamed straight through (the server-side
//!   [`Incoming`] body is handed to hyper's client),
//! * redirects are never followed (there is no redirect layer),
//! * the response body is streamed, so SSE and other long-lived responses work,
//! * hop-by-hop headers are removed from both directions, as
//!   `httputil.ReverseProxy` does.

use std::error::Error as StdError;

use http::header::{HeaderName, CONNECTION, HOST};
use http::{Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Incoming};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tonic::body::BoxBody;
use tonic::Status;

/// Headers [`httputil.ReverseProxy`] always strips.
///
/// `proxy-connection` is not in the standard hop-by-hop set but is removed by
/// Go's transport as well, so include it for parity.
const HOP_BY_HOP_HEADERS: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Reverse-proxy a server-side request to `target_authority`.
///
/// `target_authority` is either `host:port` (used verbatim) or a bare port,
/// which is expanded to `127.0.0.1:<port>`.
pub async fn proxy(request: Request<Incoming>, target_authority: &str) -> Response<BoxBody> {
    proxy_request(request, target_authority).await
}

/// Generic form of [`proxy`], used by tests and any caller with a custom body.
///
/// On connection or HTTP errors this returns `502 Bad Gateway` with the body
/// `prom-wrapper: proxy error: <cause>`.
pub async fn proxy_request<B>(request: Request<B>, target_authority: &str) -> Response<BoxBody>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: StdError + Send + Sync + 'static,
{
    match forward(request, target_authority).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, target = %target_authority, "proxy request failed");
            error_response(&format!("prom-wrapper: proxy error: {error}"))
        }
    }
}

async fn forward<B>(
    mut request: Request<B>,
    target_authority: &str,
) -> Result<Response<BoxBody>, String>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: StdError + Send + Sync + 'static,
{
    let authority = normalize_authority(target_authority);

    // Preserve the original Host (Go's ReverseProxy leaves `req.Host` alone).
    // HTTP/2 clients may not send a `host` header, so fall back to the request
    // authority before it is rewritten.
    let original_authority = request.uri().authority().cloned();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/".to_owned());

    strip_hop_by_hop(request.headers_mut());

    if request.headers().get(HOST).is_none() {
        if let Some(authority) = original_authority {
            if let Ok(value) = http::HeaderValue::from_str(authority.as_str()) {
                request.headers_mut().insert(HOST, value);
            }
        }
    }

    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.clone())
        .path_and_query(path_and_query)
        .build()
        .map_err(|error| format!("invalid upstream uri for {authority}: {error}"))?;
    *request.uri_mut() = uri;
    *request.version_mut() = Version::HTTP_11;

    let stream = TcpStream::connect(&authority)
        .await
        .map_err(|error| format!("connect to {authority}: {error}"))?;
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%authority, error = %error, "failed to set TCP_NODELAY");
    }

    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| format!("handshake with {authority}: {error}"))?;

    // The connection future drives the socket and feeds the response body;
    // detach it so the response body can stream while this task returns.
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(error = %error, "upstream connection closed with error");
        }
    });

    let response = sender
        .send_request(request)
        .await
        .map_err(|error| format!("request to {authority}: {error}"))?;

    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop(&mut parts.headers);

    // Map body failures to `Status::internal` so the body fits `BoxBody`
    // (and so the server can report a meaningful error).
    let body = body.map_err(move |error| {
        Status::internal(format!("upstream response body from {authority}: {error}"))
    });

    Ok(Response::from_parts(parts, BoxBody::new(body)))
}

fn normalize_authority(target: &str) -> String {
    let target = target.trim();
    if target.is_empty() {
        return "127.0.0.1:9090".to_owned();
    }
    if target.contains(':') || target.starts_with('[') {
        target.to_owned()
    } else {
        format!("127.0.0.1:{target}")
    }
}

/// Remove hop-by-hop headers, including any named by `Connection` tokens.
fn strip_hop_by_hop(headers: &mut http::HeaderMap) {
    let nominated: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|token| HeaderName::from_bytes(token.trim().as_bytes()).ok())
        .collect();

    for name in HOP_BY_HOP_HEADERS {
        headers.remove(name);
    }
    // `Connection` itself is covered by the list above, but removing nominated
    // headers is what actually makes `Connection: x-foo` safe to forward.
    for name in nominated {
        headers.remove(name);
    }
}

fn error_response(message: &str) -> Response<BoxBody> {
    let body = Full::new(Bytes::from(message.to_owned()))
        .map_err(|never: std::convert::Infallible| -> Status { match never {} });
    let mut response = Response::new(BoxBody::new(body));
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    response
}

#[cfg(test)]
mod tests {
    use super::{proxy_request, strip_hop_by_hop};
    use http::header::{HeaderMap, HeaderValue, CONNECTION};
    use http::{Method, Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full, StreamBody};
    use hyper::body::{Frame, Incoming};
    use hyper::server::conn::http1 as server_http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct Captured {
        method: String,
        path: String,
        query: Option<String>,
        header: Option<String>,
        dropped_header: bool,
        keep_alive_header: bool,
        body: Vec<u8>,
    }

    /// A one-shot echo server. Echoes the request metadata into the capture and
    /// replies with two separate data frames so the test can observe streaming.
    async fn spawn_echo_server() -> (std::net::SocketAddr, Arc<Mutex<Captured>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind echo server");
        let addr = listener.local_addr().expect("echo addr");
        let captured = Arc::new(Mutex::new(Captured::default()));
        let captured_service = Arc::clone(&captured);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let service = service_fn(move |request: Request<Incoming>| {
                let captured = Arc::clone(&captured_service);
                async move {
                    let method = request.method().to_string();
                    let path = request.uri().path().to_owned();
                    let query = request.uri().query().map(str::to_owned);
                    let header = request
                        .headers()
                        .get("x-test")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let dropped_header = request.headers().contains_key("x-drop-me");
                    let keep_alive_header = request.headers().contains_key("keep-alive");
                    let body = request
                        .into_body()
                        .collect()
                        .await
                        .expect("collect request body")
                        .to_bytes()
                        .to_vec();

                    *captured.lock().expect("capture lock") = Captured {
                        method,
                        path,
                        query,
                        header,
                        dropped_header,
                        keep_alive_header,
                        body,
                    };

                    let frames = vec![
                        Ok::<_, Infallible>(Frame::data(hyper::body::Bytes::from_static(
                            b"hello ",
                        ))),
                        Ok(Frame::data(hyper::body::Bytes::from_static(b"world"))),
                    ];
                    let body = StreamBody::new(tokio_stream::iter(frames));
                    Ok::<_, Infallible>(Response::new(body))
                }
            });
            let _ = server_http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        (addr, captured)
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // guard is explicitly dropped first
    async fn forwards_method_path_query_headers_and_body() {
        let (addr, captured) = spawn_echo_server().await;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/graph?target=up&x=1")
            .header("x-test", "value")
            .header("connection", "keep-alive, x-drop-me")
            .header("keep-alive", "timeout=5")
            .header("x-drop-me", "nope")
            .body(Full::new(hyper::body::Bytes::from_static(b"payload")))
            .expect("build request");

        let response = proxy_request(request, &addr.to_string()).await;
        assert_eq!(response.status(), StatusCode::OK);

        let captured = captured.lock().expect("capture lock");
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path, "/graph");
        assert_eq!(captured.query.as_deref(), Some("target=up&x=1"));
        assert_eq!(captured.header.as_deref(), Some("value"));
        assert_eq!(captured.body, b"payload");
        // Hop-by-hop headers (including Connection-nominated ones) are dropped.
        assert!(!captured.dropped_header);
        assert!(!captured.keep_alive_header);
        drop(captured);

        // The response body is streamed: two frames from the echo server arrive
        // as two frames on the client side (no buffering).
        let mut body = response.into_body();
        let first = body
            .frame()
            .await
            .expect("first frame")
            .expect("first frame ok")
            .into_data()
            .expect("first frame data");
        assert_eq!(first, hyper::body::Bytes::from_static(b"hello "));
        let second = body
            .frame()
            .await
            .expect("second frame")
            .expect("second frame ok")
            .into_data()
            .expect("second frame data");
        assert_eq!(second, hyper::body::Bytes::from_static(b"world"));
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn preserves_get_method_and_absent_body() {
        let (addr, captured) = spawn_echo_server().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/query?query=up")
            .body(Full::new(hyper::body::Bytes::new()))
            .expect("build request");

        let response = proxy_request(request, &addr.to_string()).await;
        assert_eq!(response.status(), StatusCode::OK);

        let captured = captured.lock().expect("capture lock");
        assert_eq!(captured.method, "GET");
        assert_eq!(captured.path, "/api/v1/query");
        assert_eq!(captured.query.as_deref(), Some("query=up"));
        assert!(captured.body.is_empty());
    }

    #[tokio::test]
    async fn connect_failure_returns_bad_gateway() {
        // Bind then drop to obtain a port that is almost certainly closed.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let request = Request::builder()
            .uri("/")
            .body(Full::new(hyper::body::Bytes::new()))
            .expect("build request");

        let response = proxy_request(request, &addr.to_string()).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect error body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.starts_with("prom-wrapper: proxy error:"),
            "unexpected body: {body}"
        );
    }

    #[test]
    fn strip_hop_by_hop_removes_named_and_nominated_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("x-drop-me", HeaderValue::from_static("nope"));
        headers.insert("x-keep-me", HeaderValue::from_static("yes"));
        headers.insert(
            CONNECTION,
            HeaderValue::from_static("keep-alive, x-drop-me"),
        );

        strip_hop_by_hop(&mut headers);

        assert!(!headers.contains_key("keep-alive"));
        assert!(!headers.contains_key("x-drop-me"));
        assert!(!headers.contains_key(CONNECTION));
        assert_eq!(
            headers.get("x-keep-me").and_then(|v| v.to_str().ok()),
            Some("yes")
        );
    }
}
