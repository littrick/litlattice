#![forbid(unsafe_code)]

//! `internal/debugserver` — the per-service debug server on `SRC_PROF_HTTP`.
//!
//! Only the surface this slice can stand behind is ported:
//!
//! * `/healthz` — always `200 OK`, empty body;
//! * `/ready` — `200 OK` once the process marked itself ready, `503` before;
//! * `/metrics` — the Prometheus text payload;
//! * every other path — Go's `http.NotFound`.
//!
//! The baseline additionally serves pprof, `/vars`, `/debug/requests` and an
//! HTML index; those are not ported yet. A bind failure is logged and the main
//! service keeps running, exactly like the other Rust services.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes as HyperBytes, Incoming};
use tokio_util::sync::CancellationToken;
use tonic::body::BoxBody;

/// Serve the debug surface until `shutdown` is cancelled. A bind error is
/// logged and swallowed.
pub async fn serve(addr: &str, ready: Arc<AtomicBool>, shutdown: CancellationToken) {
    let listener = match sg_http::listen(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!(addr, error = %err, "debug server failed to listen");
            return;
        }
    };
    tracing::info!(addr, "debug server listening");
    let http = handler(ready);
    if let Err(err) = sg_http::serve_http(listener, http, shutdown).await {
        tracing::error!(addr, error = %err, "debug server failed");
    }
}

pub fn handler(ready: Arc<AtomicBool>) -> sg_http::HttpHandler {
    Arc::new(move |req: Request<Incoming>| {
        let ready = Arc::clone(&ready);
        Box::pin(async move { response(&req, ready.load(Ordering::SeqCst)) })
    })
}

/// `healthzHandler` / `readyHandler` / the metrics surface.
pub fn response<B>(req: &Request<B>, ready: bool) -> Response<BoxBody> {
    match req.uri().path() {
        "/healthz" => empty(StatusCode::OK),
        "/ready" => {
            if ready {
                empty(StatusCode::OK)
            } else {
                empty(StatusCode::SERVICE_UNAVAILABLE)
            }
        }
        "/metrics" => text(
            StatusCode::OK,
            sg_metrics::CONTENT_TYPE,
            sg_metrics::render_default(),
        ),
        _ => go_not_found(),
    }
}

fn empty(status: StatusCode) -> Response<BoxBody> {
    // `w.WriteHeader(status)` writes no body and no Content-Type.
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

fn go_not_found() -> Response<BoxBody> {
    text(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        Bytes::from_static(b"404 page not found\n"),
    )
}

fn text(status: StatusCode, content_type: &str, body: impl Into<HyperBytes>) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = header::HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str) -> Request<()> {
        Request::builder().uri(path).body(()).expect("request")
    }

    #[test]
    fn healthz_is_always_200_and_empty() {
        let response = response(&request("/healthz"), false);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn ready_reflects_the_ready_flag() {
        assert_eq!(
            response(&request("/ready"), false).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response(&request("/ready"), true).status(), StatusCode::OK);
    }

    #[test]
    fn unknown_paths_are_the_404_page() {
        let response = response(&request("/debug/pprof/"), true);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
