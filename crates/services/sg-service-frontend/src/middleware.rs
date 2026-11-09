#![forbid(unsafe_code)]

//! The outer half of `newExternalHTTPHandler`'s middleware chain:
//!
//! ```text
//! ipAllowlist → BlackHole → healthCheck → ServeMux
//! ```
//!
//! The baseline wraps the mux with `ipAllowlist → tenant → instrumentation →
//! tracepkg → ForbidAll → GoGet → BlackHole → healthCheck → gcontext → Trace`;
//! the remaining layers need live site configuration, metrics or the request
//! context and land in later slices (docs/overviews/m9_8dx §2.3).

use std::sync::Arc;

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use tonic::body::BoxBody;

use crate::health;
use crate::ipallowlist::{self, IpAllowlistConfig};

/// `middleware.isBlackhole`.
pub fn is_blackhole<B>(req: &Request<B>) -> bool {
    // We no longer support github webhooks.
    if req.uri().path() == "/api/ext/github/webhook" {
        return true;
    }

    // We no longer support gRPC on the external API.
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        == Some("application/grpc")
}

/// `middleware.BlackHole`: `410 Gone` with no body.
pub fn blackhole_response() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = StatusCode::GONE;
    response
}

/// The end of the external chain: `http.NotFound`.
pub fn go_not_found() -> Response<BoxBody> {
    let mut response = text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        Bytes::from_static(b"404 page not found\n"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

/// A response with an explicit Content-Type (used by the mux handlers).
pub fn text_response(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = header::HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

/// The chain with an injected next handler; the differential harness passes a
/// `"next"` handler, the service passes [`go_not_found`].
pub fn external_response_with_next<B>(
    req: &Request<B>,
    version: &Bytes,
    next: impl FnOnce() -> Response<BoxBody>,
) -> Response<BoxBody> {
    if is_blackhole(req) {
        return blackhole_response();
    }
    if let Some(response) = health::health_check(req.uri().path(), version) {
        return response;
    }
    next()
}

/// The chain with the baseline's outermost layer: `ipAllowlist` runs before
/// BlackHole and healthCheck, so a denied `/healthz` is a 403. `None` means
/// the allowlist is disabled (`configResolver()` returned nil).
pub fn external_response_with_ipallowlist<B>(
    req: &Request<B>,
    version: &Bytes,
    allowlist: Option<&IpAllowlistConfig>,
    next: impl FnOnce() -> Response<BoxBody>,
) -> Response<BoxBody> {
    if let Some(allowlist) = allowlist {
        if let Err(error) = allowlist.is_authorized(req) {
            return ipallowlist::forbidden_response(&error);
        }
    }
    external_response_with_next(req, version, next)
}

/// `newExternalHTTPHandler` without its config-dependent layers.
pub fn external_response<B>(req: &Request<B>, version: &Bytes) -> Response<BoxBody> {
    external_response_with_next(req, version, go_not_found)
}

/// `newInternalHTTPHandler`'s mux once every `/.internal/` route is registered;
/// none are ported yet, so every path is the mux 404.
pub fn internal_response<B>(_req: &Request<B>) -> Response<BoxBody> {
    go_not_found()
}

/// The hyper entry point for the external listener.
pub fn external_handler(
    version: Bytes,
    allowlist: Option<Arc<IpAllowlistConfig>>,
) -> sg_http::HttpHandler {
    Arc::new(move |req: Request<Incoming>| {
        let version = version.clone();
        let allowlist = allowlist.clone();
        Box::pin(async move {
            external_response_with_ipallowlist(&req, &version, allowlist.as_deref(), go_not_found)
        })
    })
}

/// The hyper entry point for the internal listener.
pub fn internal_handler() -> sg_http::HttpHandler {
    std::sync::Arc::new(|req: Request<Incoming>| Box::pin(async move { internal_response(&req) }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str) -> Request<()> {
        Request::builder().uri(path).body(()).expect("request")
    }

    #[test]
    fn blackhole_paths_are_gone() {
        assert_eq!(
            external_response(
                &request("/api/ext/github/webhook"),
                &Bytes::from_static(b"v")
            )
            .status(),
            StatusCode::GONE
        );

        let grpc = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&grpc, &Bytes::from_static(b"v")).status(),
            StatusCode::GONE
        );

        // The content-type check is exact: parameters do not match.
        let grpc_params = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc+proto")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&grpc_params, &Bytes::from_static(b"v")).status(),
            StatusCode::OK
        );
    }

    #[test]
    fn health_paths_are_served_by_the_middleware() {
        let response = external_response(&request("/healthz"), &Bytes::from_static(b"1.2.3"));
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn everything_else_is_the_mux_404() {
        let response = external_response(&request("/.api/graphql"), &Bytes::from_static(b"v"));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }
}
