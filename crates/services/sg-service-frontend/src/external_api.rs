#![forbid(unsafe_code)]

//! The external `/.api/` subrouter (`NewHandler`,
//! `cmd/frontend/internal/httpapi/httpapi.go:100-155`), restricted to the
//! routes whose OSS behavior is deterministic:
//!
//! - `/.api/scim/v2*` (GET/POST/PUT/PATCH/DELETE) ->
//!   `enterprise.makeNotFoundHandler("SCIM handler")`;
//! - `/.api/lsif/upload` (POST) -> `lsifDeprecationHandler` (501, fixed text);
//! - `/.api/scip/upload` (POST) -> enterprise `code intel upload` 404;
//!   (HEAD) -> `noopHandler` 200;
//! - `/.api/compute/stream` (GET/POST) -> enterprise `compute streaming
//!   endpoint` 404;
//! - anything else -> gorilla's default `http.NotFoundHandler`
//!   (`404 page not found\n`, `nosniff`).
//!
//! gorilla enables `StrictSlash(true)` on the subrouter, but its PathPrefix
//! routes (`/scim/v2`) are unaffected; the exact routes redirect a trailing
//! slash away exactly like `http.Redirect`.

use http::{Method, Request, Response, StatusCode};
use tonic::body::BoxBody;

use sg_config::LiveConfig;

use crate::internal_api::{
    decode_path, empty_response, enterprise_not_found, escape_path, hex_escape_non_ascii, located,
    strict_slash_redirect, text_response,
};
use crate::mux;

const SCIM_PREFIX: &str = "/.api/scim/v2";
const LSIF_UPLOAD_PATH: &str = "/.api/lsif/upload";
const SCIP_UPLOAD_PATH: &str = "/.api/scip/upload";
const COMPUTE_STREAM_PATH: &str = "/.api/compute/stream";

/// `lsifDeprecationHandler` (`httpapi.go:459-462`).
const LSIF_DEPRECATION_MESSAGE: &str = "Sourcegraph v4.5+ no longer accepts LSIF uploads. The Sourcegraph CLI v4.4.2+ will translate LSIF to SCIP prior to uploading. Please check the version of the CLI utility used to upload this artifact.";

/// The route a request resolved to, before the method is checked.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Scim,
    LsifUpload,
    ScipUpload,
    ComputeStream,
}

impl Route {
    fn allows(&self, method: &Method) -> bool {
        match self {
            Route::Scim => matches!(
                *method,
                Method::GET | Method::POST | Method::PUT | Method::PATCH | Method::DELETE
            ),
            Route::LsifUpload => method == Method::POST,
            Route::ScipUpload => method == Method::POST || method == Method::HEAD,
            Route::ComputeStream => method == Method::GET || method == Method::POST,
        }
    }
}

/// The subrouter's `ServeHTTP` + the registered handlers.
pub fn api_response<B>(req: &Request<B>, _live: &LiveConfig) -> Response<BoxBody> {
    let raw_path = req.uri().path();
    let decoded = decode_path(if raw_path.is_empty() { "/" } else { raw_path });

    // gorilla `Router.ServeHTTP`'s `path.Clean` redirect: `Location` only.
    let cleaned = mux::clean_path(&decoded);
    if cleaned != decoded {
        let location = match req.uri().query() {
            Some(query) => format!("{}?{query}", escape_path(&cleaned)),
            None => escape_path(&cleaned),
        };
        return located(
            StatusCode::MOVED_PERMANENTLY,
            hex_escape_non_ascii(&location),
            None,
        );
    }

    let (route, matched_path) = match match_route(&decoded) {
        Some(route) => (route, decoded.as_str()),
        None => match decoded.strip_suffix('/').map(match_route) {
            Some(Some(route)) => (route, &decoded[..decoded.len() - 1]),
            _ => return page_not_found(),
        },
    };
    if !route.allows(req.method()) {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    // Prefix routes never take the strict-slash branch (their match consumes
    // the whole path); only the exact routes can be shorter than `decoded`.
    if matched_path.len() != decoded.len() {
        return strict_slash_redirect(req.method(), matched_path, req.uri().query());
    }

    match route {
        Route::Scim => enterprise_not_found("SCIM handler"),
        Route::LsifUpload => text_response(
            StatusCode::NOT_IMPLEMENTED,
            "text/plain; charset=utf-8",
            LSIF_DEPRECATION_MESSAGE,
            false,
        ),
        Route::ScipUpload if req.method() == Method::HEAD => empty_response(StatusCode::OK),
        Route::ScipUpload => enterprise_not_found("code intel upload"),
        Route::ComputeStream => enterprise_not_found("compute streaming endpoint"),
    }
}

/// Registered order: `PathPrefix("/scim/v2")` first, then the exact paths.
fn match_route(path: &str) -> Option<Route> {
    if path.strip_prefix(SCIM_PREFIX).is_some() {
        return Some(Route::Scim);
    }
    if path == LSIF_UPLOAD_PATH {
        return Some(Route::LsifUpload);
    }
    if path == SCIP_UPLOAD_PATH {
        return Some(Route::ScipUpload);
    }
    if path == COMPUTE_STREAM_PATH {
        return Some(Route::ComputeStream);
    }
    None
}

/// gorilla's default `NotFoundHandler` (`http.NotFoundHandler`).
fn page_not_found() -> Response<BoxBody> {
    text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "404 page not found\n",
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use sg_config::{ServiceConnections, SiteConfiguration, Unified};

    fn live() -> LiveConfig {
        LiveConfig::new(Unified {
            site: SiteConfiguration::parse("{}").expect("site"),
            service_connections: ServiceConnections::default(),
        })
    }

    fn request(method: Method, target: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(target)
            .body(())
            .expect("request")
    }

    #[tokio::test]
    async fn subrouter_prefix_and_default_not_found() {
        let live = live();
        // PathPrefix matches without a separating slash.
        assert_eq!(
            api_response(&request(Method::GET, "/.api/scim/v2anything"), &live).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            api_response(&request(Method::POST, "/.api/lsif/upload"), &live).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        let page = api_response(&request(Method::GET, "/.api/other"), &live);
        assert_eq!(page.status(), StatusCode::NOT_FOUND);
        let body = page.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"404 page not found\n");
    }
}
