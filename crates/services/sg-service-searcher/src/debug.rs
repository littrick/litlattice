#![forbid(unsafe_code)]

//! Per-service debug endpoint (`SRC_PROF_HTTP`), equivalent to the baseline
//! `internal/debugserver`.
//!
//! The Go server also exposes pprof, `/vars`, `/debug/requests` and the gRPC
//! Web UI; the pilot serves the Prometheus surface (`/metrics`), which is what
//! monitoring scrapes. Bind failures are logged and the service keeps running,
//! so a missing debug port never takes searcher down during a gray rollout.

use std::sync::Arc;

use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio_util::sync::CancellationToken;

/// Baseline `debugserver` default (`SRC_PROF_HTTP`).
pub const DEFAULT_ADDR: &str = ":6060";

const INDEX_HTML: &str =
    "<!DOCTYPE html>\n<html><head><title>Sourcegraph searcher debug</title></head>\
<body><a href=\"metrics\">Metrics</a><br></body></html>\n";

/// Bind `addr` and serve the debug HTTP surface until `shutdown` is cancelled.
/// A bind error is logged and swallowed.
pub async fn serve(addr: &str, shutdown: CancellationToken) {
    let listener = match sg_http::listen(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!(addr, error = %err, "debug server failed to listen");
            return;
        }
    };
    tracing::info!(addr, "debug server listening");
    if let Err(err) = sg_http::serve_http(listener, handler(), shutdown).await {
        tracing::error!(addr, error = %err, "debug server failed");
    }
}

pub fn handler() -> sg_http::HttpHandler {
    Arc::new(|req: Request<Incoming>| Box::pin(async move { response(req) }))
}

fn response(req: Request<Incoming>) -> Response<tonic::body::BoxBody> {
    route(req.uri().path())
}

fn route(path: &str) -> Response<tonic::body::BoxBody> {
    match path {
        "/metrics" => text(
            StatusCode::OK,
            sg_metrics::CONTENT_TYPE,
            sg_metrics::render_default(),
        ),
        "/" => text(StatusCode::OK, "text/html; charset=utf-8", INDEX_HTML),
        // Go's `http.NotFound`.
        _ => text(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "404 page not found\n",
        ),
    }
}

fn text(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<tonic::body::BoxBody> {
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

    #[test]
    fn serves_metrics_and_404() {
        assert_eq!(route("/metrics").status(), StatusCode::OK);
        assert_eq!(route("/").status(), StatusCode::OK);
        assert_eq!(route("/pprof").status(), StatusCode::NOT_FOUND);
    }
}
