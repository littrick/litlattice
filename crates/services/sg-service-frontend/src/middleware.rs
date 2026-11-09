#![forbid(unsafe_code)]

//! The outer half of `newExternalHTTPHandler`'s middleware chain:
//!
//! ```text
//! ipAllowlist → tenant → ForbidAll → GoGet → BlackHole → healthCheck → ServeMux
//! ```
//!
//! The baseline wraps the mux with `ipAllowlist → tenant → instrumentation →
//! tracepkg → ForbidAll → GoGet → BlackHole → healthCheck → gcontext → Trace`;
//! the instrumentation/trace layers are observational, so they land in later
//! slices (docs/overviews/m9_8ea §1.4). `ForbidAll` reads the site
//! configuration snapshot and is ported here (m9_8ec).

use std::sync::Arc;

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use tonic::body::BoxBody;

use crate::auth;
use crate::goimportpath;
use crate::health;
use crate::ipallowlist::{self, IpAllowlistConfig};
use crate::mux::{self, SecurityConfig};
use crate::tenant;

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

/// The chain with the baseline's outer layers: `ipAllowlist` runs before the
/// tenant lookup, which runs before `ForbidAll`; all of it precedes GoGet,
/// BlackHole and healthCheck, so a denied `/healthz` is a 403 and an unknown
/// tenant beats a go-get response. `None` means the allowlist is disabled
/// (`configResolver()` returned nil); `forbid_all` is
/// [`auth::forbidden`]'s result for the current site configuration.
pub fn external_response_with_ipallowlist<B>(
    req: &mut Request<B>,
    version: &Bytes,
    allowlist: Option<&IpAllowlistConfig>,
    forbid_all: bool,
    external_url: &str,
    next: impl FnOnce(&mut Request<B>) -> Response<BoxBody>,
) -> Response<BoxBody> {
    if let Some(allowlist) = allowlist {
        if let Err(error) = allowlist.is_authorized(req) {
            return ipallowlist::forbidden_response(&error);
        }
    }
    if let Err(response) = tenant::external_tenant_from_hostname(req, tenant::fixed_tenant_one) {
        return response;
    }
    if forbid_all {
        return auth::forbidden_response();
    }
    if let Some(response) = goimportpath::response(req, external_url) {
        return response;
    }
    if is_blackhole(req) {
        return blackhole_response();
    }
    if let Some(response) = health::health_check(req.uri().path(), version) {
        return response;
    }
    next(req)
}

/// `newExternalHTTPHandler` without its config-dependent layers.
pub fn external_response<B>(
    req: &mut Request<B>,
    version: &Bytes,
    external_url: &str,
) -> Response<BoxBody> {
    external_response_with_ipallowlist(req, version, None, false, external_url, |_| go_not_found())
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
    forbid_all: bool,
    security: SecurityConfig,
) -> sg_http::HttpHandler {
    Arc::new(move |mut req: Request<Incoming>| {
        let version = version.clone();
        let allowlist = allowlist.clone();
        let security = security.clone();
        Box::pin(async move {
            external_response_with_ipallowlist(
                &mut req,
                &version,
                allowlist.as_deref(),
                forbid_all,
                &security.external_url,
                |req| mux::route(req, &security, mux::not_found_dispatch),
            )
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
                &mut request("/api/ext/github/webhook"),
                &Bytes::from_static(b"v"),
                ""
            )
            .status(),
            StatusCode::GONE
        );

        let mut grpc = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&mut grpc, &Bytes::from_static(b"v"), "").status(),
            StatusCode::GONE
        );

        // The content-type check is exact: parameters do not match.
        let mut grpc_params = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc+proto")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&mut grpc_params, &Bytes::from_static(b"v"), "").status(),
            StatusCode::OK
        );
    }

    #[test]
    fn health_paths_are_served_by_the_middleware() {
        let response =
            external_response(&mut request("/healthz"), &Bytes::from_static(b"1.2.3"), "");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn everything_else_is_the_mux_404() {
        let response =
            external_response(&mut request("/.api/graphql"), &Bytes::from_static(b"v"), "");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }

    #[test]
    fn go_get_is_served_before_blackhole_and_health() {
        let mut req = request("/sourcegraph/sourcegraph?go-get=1");
        let response = external_response(
            &mut req,
            &Bytes::from_static(b"v"),
            "https://sourcegraph.com",
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn forbid_all_precedes_go_get_health_and_the_mux() {
        for path in [
            "/sourcegraph/sourcegraph?go-get=1",
            "/healthz",
            "/__version",
            "/.api/graphql",
        ] {
            let mut req = request(path);
            let response = external_response_with_ipallowlist(
                &mut req,
                &Bytes::from_static(b"v"),
                None,
                true,
                "https://sourcegraph.com",
                |_| go_not_found(),
            );
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "text/plain; charset=utf-8",
                "{path}"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::X_CONTENT_TYPE_OPTIONS)
                    .unwrap(),
                "nosniff",
                "{path}"
            );
        }
    }

    #[test]
    fn forbid_all_runs_after_the_allowlist() {
        let allowlist = IpAllowlistConfig {
            authorized_client_ips: vec!["10.0.0.1".parse().unwrap()],
            ..IpAllowlistConfig::default()
        };
        let mut denied = request("/");
        denied
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut denied,
            &Bytes::from_static(b"v"),
            Some(&allowlist),
            true,
            "",
            |_| go_not_found(),
        );
        // The allowlist's HTML rejection wins over ForbidAll's plain text.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[test]
    fn allowlist_denial_precedes_the_tenant_middleware() {
        let allowlist = IpAllowlistConfig {
            authorized_client_ips: vec!["10.0.0.1".parse().unwrap()],
            ..IpAllowlistConfig::default()
        };
        let mut denied = request("/some/path");
        denied
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut denied,
            &Bytes::from_static(b"v"),
            Some(&allowlist),
            false,
            "",
            |_| go_not_found(),
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(denied
            .extensions()
            .get::<crate::tenant::TenantId>()
            .is_none());

        // An empty allowlist authorizes the request, which then reaches the
        // tenant mapper (success) and the next handler.
        let mut allowed = request("/some/path");
        allowed
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut allowed,
            &Bytes::from_static(b"v"),
            Some(&IpAllowlistConfig::default()),
            false,
            "",
            |_| go_not_found(),
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            allowed.extensions().get::<crate::tenant::TenantId>(),
            Some(&crate::tenant::TenantId(1))
        );
    }
}
