//! M9.8dx differential harness: the Rust side of
//! `test_ws/p9-go/run_frontend_health_diff.sh`.
//!
//! Prints the canonical `fehl ` lines produced by the real Go harness
//! (`TestFrontendHealthHarness`, `TestFrontendDebugHealthHarness`) for the
//! BlackHole + healthCheck chain and the debug health surface.

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use sg_service_frontend::{debug, middleware, version::DEV_VERSION};
use tonic::body::BoxBody;

fn next_response() -> Response<BoxBody> {
    // The Go harness's next handler writes "next" without setting a
    // Content-Type; net/http sniffs ASCII text.
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from_static(b"next"))));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

async fn canonical(response: Response<BoxBody>) -> (u16, String, String) {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body).into_owned();
    (status, content_type, body)
}

#[tokio::test]
async fn frontend_health_harness() {
    let version = Bytes::from_static(DEV_VERSION.as_bytes());

    let ext_cases = [
        ("healthz", "GET", "/healthz", ""),
        ("version", "GET", "/__version", ""),
        ("healthz-query", "GET", "/healthz?x=1", ""),
        ("healthz-trailing-slash", "GET", "/healthz/", ""),
        ("other", "GET", "/other", ""),
        ("blackhole-path", "POST", "/api/ext/github/webhook", ""),
        ("blackhole-grpc", "GET", "/healthz", "application/grpc"),
        (
            "grpc-params",
            "GET",
            "/healthz",
            "application/grpc; charset=utf-8",
        ),
    ];
    for (name, method, path, ctype) in ext_cases {
        let mut builder = Request::builder().method(method).uri(path);
        if !ctype.is_empty() {
            builder = builder.header(header::CONTENT_TYPE, ctype);
        }
        let request = builder.body(()).expect("request");
        let response = middleware::external_response_with_next(&request, &version, next_response);
        let (status, content_type, body) = canonical(response).await;
        println!("fehl ext {name} {status} {content_type} {body:?}");
    }

    let debug_cases = [
        ("healthz", "/healthz", true),
        ("ready-closed", "/ready", true),
        ("ready-open", "/ready", false),
    ];
    for (name, path, ready) in debug_cases {
        let request = Request::builder().uri(path).body(()).expect("request");
        let (status, content_type, body) = canonical(debug::response(&request, ready)).await;
        println!("fehl dbg {name} {status} {content_type} {body:?}");
    }
}
