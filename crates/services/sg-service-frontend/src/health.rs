#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cli/http.go:healthCheckMiddleware`.
//!
//! Requests for `/healthz` and `/__version` answer `200 OK` with the build
//! version string as the body. The baseline does not set a Content-Type
//! explicitly; Go's response writer sniffs the body, which for the version
//! string is `text/plain; charset=utf-8`. Everything else falls through to the
//! next handler.

use bytes::Bytes;
use http::{header, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

/// The paths answered directly by the middleware.
pub const HEALTH_PATHS: [&str; 2] = ["/healthz", "/__version"];

/// `healthCheckMiddleware`'s decision: `Some(response)` to short-circuit,
/// `None` to call the next handler.
pub fn health_check(path: &str, version: &Bytes) -> Option<Response<BoxBody>> {
    HEALTH_PATHS
        .contains(&path)
        .then(|| version_response(version))
}

/// The `/healthz` and `/__version` response: status 200, the version bytes,
/// and the sniffed Content-Type.
pub fn version_response(version: &Bytes) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(version.clone())));
    *response.status_mut() = StatusCode::OK;
    // Go's `DetectContentType` for ASCII text.
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_and_version_short_circuit() {
        let version = Bytes::from_static(b"0.0.0+dev");
        for path in ["/healthz", "/__version"] {
            let response = health_check(path, &version).expect("handled");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "text/plain; charset=utf-8"
            );
        }
    }

    #[test]
    fn other_paths_fall_through() {
        let version = Bytes::from_static(b"0.0.0+dev");
        for path in ["/", "/healthz/", "/Healthz", "/.api/graphql"] {
            assert!(health_check(path, &version).is_none(), "{path}");
        }
    }
}
