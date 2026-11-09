#![forbid(unsafe_code)]

//! The innermost layer of `newExternalHTTPHandler`: the external `ServeMux`
//! and `secureHeadersMiddleware`.
//!
//! Baseline (`cmd/frontend/internal/cli/http.go`):
//!
//! ```text
//! /.api/       -> secureHeadersMiddleware(API)
//! /.executors/ -> secureHeadersMiddleware(never)
//! /.assets/    -> StripPrefix("/.assets") -> secureHeadersMiddleware(assets)
//! /            -> secureHeadersMiddleware(never)
//! ```
//!
//! The mux also reproduces Go 1.22 `ServeMux` path canonicalization: the
//! escaped path is cleaned (`.`/`..`/repeated `/`) and a subtree root without
//! its trailing slash is redirected with 301 before any handler runs.
//! Redirects are produced by the mux itself and therefore carry no security
//! headers. The API/UI/executor/asset handlers are not ported; `dispatch`
//! returns the mux 404 until they land.

use bytes::Bytes;
use http::header::{self, HeaderName, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;

/// `devExtension` (`cmd/frontend/internal/cli/serve_cmd.go:67`).
pub const DEV_EXTENSION: &str = "chrome-extension://bmfbcejdknlknpncfpeloejonjoledha";
/// `prodExtension` (`cmd/frontend/internal/cli/serve_cmd.go:69`).
pub const PROD_EXTENSION: &str = "chrome-extension://dgjhfomjieaadpoljlnidmbgkdffpack";

const TRUSTED_ALLOW_HEADERS: &str =
    "X-Requested-With, X-Sourcegraph-Client, Content-Type, Authorization, X-Sourcegraph-Should-Trace";
const UNTRUSTED_ALLOW_HEADERS: &str = "Content-Type, Authorization, X-Sourcegraph-Should-Trace";

/// The cross-origin policy attached to a route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrossOriginPolicy {
    /// `/.api/`: any origin, credentials, CORS headers.
    Api,
    /// `/.assets/`: CORS only for the `extension/` subtree.
    Assets,
    /// Everything else: no `Access-Control-Allow-*` headers at all.
    Never,
}

/// The site configuration fields `isTrustedOrigin` reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecurityConfig {
    /// `siteConfig.externalURL` (raw, trailing slash is kept here).
    pub external_url: String,
    /// `siteConfig.corsOrigin` (whitespace-separated origins, may contain `*`).
    pub cors_origin: String,
}

impl SecurityConfig {
    /// Reads the two untyped top-level keys; absent/non-string behaves like
    /// Go's zero string.
    pub fn from_site_config(config: &SiteConfiguration) -> Self {
        let raw = config.raw();
        let field = |key: &str| {
            raw.get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        Self {
            external_url: field("externalURL"),
            cors_origin: field("corsOrigin"),
        }
    }

    /// `isTrustedOrigin` (`http.go:350-367`).
    pub fn is_trusted_origin(&self, origin: &str) -> bool {
        if origin == DEV_EXTENSION || origin == PROD_EXTENSION {
            return true;
        }
        let mut allowed = false;
        if !self.cors_origin.is_empty() {
            allowed = self
                .cors_origin
                .split_whitespace()
                .any(|candidate| candidate == "*" || candidate == origin);
        }
        let external = self.external_url.trim_end_matches('/');
        if !external.is_empty() && external == origin {
            allowed = true;
        }
        allowed
    }
}

/// A route chosen by the mux. `Assets` carries the path after
/// `StripPrefix("/.assets")`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    Api,
    Executors,
    App,
    Assets { path: String },
}

impl Route {
    fn policy(&self) -> CrossOriginPolicy {
        match self {
            Route::Api => CrossOriginPolicy::Api,
            Route::Executors | Route::App => CrossOriginPolicy::Never,
            Route::Assets { .. } => CrossOriginPolicy::Assets,
        }
    }

    /// The path seen by `secureHeadersMiddleware` (assets already stripped).
    fn path<'a>(&'a self, request_path: &'a str) -> &'a str {
        match self {
            Route::Assets { path } => path,
            _ => request_path,
        }
    }
}

/// The production dispatch: every sub-handler is still a placeholder.
pub fn not_found_dispatch(_route: Route) -> Response<BoxBody> {
    crate::middleware::go_not_found()
}

/// `sm.ServeHTTP`: canonicalize the path, then dispatch through
/// `secureHeadersMiddleware`. `dispatch` is injected so the differential
/// harness can distinguish routed handlers.
pub fn route<B>(
    req: &Request<B>,
    config: &SecurityConfig,
    dispatch: impl FnOnce(Route) -> Response<BoxBody>,
) -> Response<BoxBody> {
    let escaped = req.uri().path();
    let escaped = if escaped.is_empty() { "/" } else { escaped };
    let cleaned = clean_path(escaped);

    // ServeMux.matchOrRedirect: a subtree root without the trailing slash.
    if let Some(target) = trailing_slash_target(&cleaned) {
        return moved_permanently(req, &redirect_location(req, target));
    }
    // Go redirects to the cleaned path once matching is done.
    if cleaned != escaped {
        return moved_permanently(req, &redirect_location(req, &cleaned));
    }

    let route = classify(&cleaned);
    let policy = route.policy();
    let path = route.path(&cleaned).to_owned();
    secure_headers(req, policy, &path, config, || dispatch(route))
}

/// `http.ServeMux` subtree matching for the four registered patterns.
fn classify(path: &str) -> Route {
    if path == "/.api" || path.starts_with("/.api/") {
        Route::Api
    } else if path == "/.executors" || path.starts_with("/.executors/") {
        Route::Executors
    } else if path == "/.assets" || path.starts_with("/.assets/") {
        Route::Assets {
            path: path.strip_prefix("/.assets").unwrap_or(path).to_owned(),
        }
    } else {
        Route::App
    }
}

fn trailing_slash_target(path: &str) -> Option<&'static str> {
    match path {
        "/.api" => Some("/.api/"),
        "/.executors" => Some("/.executors/"),
        "/.assets" => Some("/.assets/"),
        _ => None,
    }
}

fn redirect_location<B>(req: &Request<B>, path: &str) -> String {
    match req.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    }
}

/// `cleanPath` (`net/http/server.go:2450`): `path.Clean` plus the preserved
/// trailing slash.
pub fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_owned();
    }
    let owned;
    let path = if path.starts_with('/') {
        path
    } else {
        owned = format!("/{path}");
        &owned
    };
    let trailing = path.ends_with('/');

    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }

    let mut cleaned = String::from("/");
    cleaned.push_str(&segments.join("/"));
    if trailing && cleaned != "/" {
        cleaned.push('/');
    }
    cleaned
}

/// `secureHeadersMiddleware` (`http.go:236-253`).
pub fn secure_headers<B>(
    req: &Request<B>,
    policy: CrossOriginPolicy,
    path: &str,
    config: &SecurityConfig,
    next: impl FnOnce() -> Response<BoxBody>,
) -> Response<BoxBody> {
    let cors = cors_request(req, policy, path, config);

    let mut response = match &cors {
        Some(cors) if cors.preflight => empty_response(StatusCode::OK),
        _ => next(),
    };

    let headers = response.headers_mut();
    headers
        .entry(header::X_CONTENT_TYPE_OPTIONS)
        .or_insert(HeaderValue::from_static("nosniff"));
    headers
        .entry(header_name("x-xss-protection"))
        .or_insert(HeaderValue::from_static("1; mode=block"));
    headers
        .entry(header::X_FRAME_OPTIONS)
        .or_insert(HeaderValue::from_static("DENY"));
    headers
        .entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-cache, max-age=0"));

    if let Some(cors) = cors {
        for (name, value) in cors.headers {
            headers.insert(name, value);
        }
    }
    response
}

struct CorsRequest {
    preflight: bool,
    headers: Vec<(HeaderName, HeaderValue)>,
}

/// `handleCORSRequest` (`http.go:258-348`).
fn cors_request<B>(
    req: &Request<B>,
    policy: CrossOriginPolicy,
    path: &str,
    config: &SecurityConfig,
) -> Option<CorsRequest> {
    if policy == CrossOriginPolicy::Never {
        return None;
    }
    if policy == CrossOriginPolicy::Assets && !path.starts_with("/extension/") {
        return None;
    }

    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    let mut headers = vec![
        (
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        ),
        (
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_str(origin).unwrap_or_else(|_| HeaderValue::from_static("")),
        ),
    ];

    let preflight = req.method() == Method::OPTIONS;
    if preflight {
        headers.push((
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, OPTIONS"),
        ));
        let allow_headers = if config.is_trusted_origin(origin) {
            TRUSTED_ALLOW_HEADERS
        } else {
            UNTRUSTED_ALLOW_HEADERS
        };
        headers.push((
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static(allow_headers),
        ));
    }

    Some(CorsRequest { preflight, headers })
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

/// `http.Redirect(w, r, url, StatusMovedPermanently)` (`net/http/server.go:2226`).
/// The body is only written for GET requests without a pre-set Content-Type.
fn moved_permanently<B>(req: &Request<B>, location: &str) -> Response<BoxBody> {
    let method = req.method();
    let mut response = empty_response(StatusCode::MOVED_PERMANENTLY);
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if method == Method::GET || method == Method::HEAD {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    if method == Method::GET {
        let body = format!(
            "<a href=\"{}\">Moved Permanently</a>.\n\n",
            html_escape(location)
        );
        *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body)));
    }
    response
}

/// Go `htmlReplacer` (`net/http/server.go:2281`).
fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            '\'' => escaped.push_str("&#39;"),
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn header_name(name: &'static str) -> HeaderName {
    HeaderName::from_static(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    fn request(method: Method, uri: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(())
            .expect("request")
    }

    fn dispatch(route: Route) -> Response<BoxBody> {
        let body = match route {
            Route::Api => "api",
            Route::Executors => "executors",
            Route::App => "app",
            Route::Assets { path } => {
                return crate::middleware::text_response(
                    StatusCode::OK,
                    "text/plain; charset=utf-8",
                    format!("asset:{path}"),
                )
            }
        };
        crate::middleware::text_response(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            body.to_owned(),
        )
    }

    async fn body_of(response: Response<BoxBody>) -> String {
        use http_body_util::BodyExt;
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[test]
    fn clean_path_matches_go_path_clean() {
        assert_eq!(clean_path(""), "/");
        assert_eq!(clean_path("/"), "/");
        assert_eq!(clean_path("//"), "/");
        assert_eq!(clean_path("/a"), "/a");
        assert_eq!(clean_path("/a/"), "/a/");
        assert_eq!(clean_path("/a/./b"), "/a/b");
        assert_eq!(clean_path("/a//b"), "/a/b");
        assert_eq!(clean_path("/foo/../bar"), "/bar");
        assert_eq!(clean_path("/.."), "/");
        assert_eq!(clean_path("/a/../"), "/");
        assert_eq!(clean_path("relative"), "/relative");
    }

    #[test]
    fn routes_longest_prefix() {
        assert!(matches!(classify("/.api/graphql"), Route::Api));
        assert!(matches!(classify("/.api/"), Route::Api));
        assert!(matches!(classify("/.executors/run"), Route::Executors));
        assert!(matches!(
            classify("/.assets/extension/a.js"),
            Route::Assets { .. }
        ));
        assert!(matches!(classify("/"), Route::App));
        assert!(matches!(classify("/tree"), Route::App));
    }

    #[tokio::test]
    async fn api_requests_get_security_and_cors_headers() {
        let config = SecurityConfig::default();
        let request = request(Method::GET, "/.api/graphql");
        let response = route(&request, &config, dispatch);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(
            response.headers().get("x-xss-protection").unwrap(),
            "1; mode=block"
        );
        assert_eq!(response.headers().get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            response.headers().get("cache-control").unwrap(),
            "no-cache, max-age=0"
        );
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
        // No Origin header: Go writes an empty value.
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            ""
        );
        assert_eq!(body_of(response).await, "api");
    }

    #[tokio::test]
    async fn preflight_is_handled_without_the_next_handler() {
        let config = SecurityConfig::default();
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/.api/graphql")
            .header(header::ORIGIN, "https://evil.example")
            .body(())
            .expect("request");
        let response = secure_headers(
            &request,
            CrossOriginPolicy::Api,
            "/.api/graphql",
            &config,
            || panic!("next must not run for a CORS preflight"),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-headers")
                .unwrap(),
            UNTRUSTED_ALLOW_HEADERS
        );
        assert_eq!(body_of(response).await, "");
    }

    #[tokio::test]
    async fn never_policy_writes_no_cors_headers_even_for_options() {
        let config = SecurityConfig {
            external_url: "https://sourcegraph.example.com/".to_owned(),
            cors_origin: "*".to_owned(),
        };
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/whatever")
            .header(header::ORIGIN, "https://sourcegraph.example.com")
            .body(())
            .expect("request");
        let response = route(&request, &config, dispatch);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_none());
        assert_eq!(body_of(response).await, "app");
    }

    #[tokio::test]
    async fn assets_cors_only_under_extension() {
        let config = SecurityConfig::default();
        let extension = request(Method::GET, "/.assets/extension/x.js");
        let response = route(&extension, &config, dispatch);
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_some());
        assert_eq!(body_of(response).await, "asset:/extension/x.js");

        let other = request(Method::GET, "/.assets/other/x.js");
        let response = route(&other, &config, dispatch);
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_none());
        assert_eq!(body_of(response).await, "asset:/other/x.js");
    }

    #[tokio::test]
    async fn subtree_root_redirects_with_go_body() {
        let config = SecurityConfig::default();
        let response = route(&request(Method::GET, "/.api"), &config, dispatch);
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/.api/");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        assert!(response.headers().get("x-content-type-options").is_none());
        assert_eq!(
            body_of(response).await,
            "<a href=\"/.api/\">Moved Permanently</a>.\n\n"
        );
    }

    #[test]
    fn cleaned_path_redirects_and_keeps_the_query() {
        let config = SecurityConfig::default();
        let response = route(&request(Method::GET, "/foo/../bar?x=1"), &config, dispatch);
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/bar?x=1"
        );

        // The subtree-root redirect wins over the clean-path redirect and
        // keeps the query, like `matchOrRedirect`.
        let response = route(&request(Method::GET, "/foo/../.api?x=1"), &config, dispatch);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/.api/?x=1"
        );
    }

    #[tokio::test]
    async fn redirect_body_only_for_get() {
        let config = SecurityConfig::default();
        let response = route(&request(Method::POST, "/.api"), &config, dispatch);
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/.api/");
        assert_eq!(body_of(response).await, "");
    }

    #[test]
    fn trusted_origin_matrix() {
        let config = SecurityConfig {
            external_url: "https://sourcegraph.example.com/".to_owned(),
            cors_origin: "https://a.example".to_owned(),
        };
        assert!(config.is_trusted_origin(DEV_EXTENSION));
        assert!(config.is_trusted_origin(PROD_EXTENSION));
        assert!(config.is_trusted_origin("https://a.example"));
        assert!(config.is_trusted_origin("https://sourcegraph.example.com"));
        assert!(!config.is_trusted_origin("https://b.example"));
        assert!(!config.is_trusted_origin("https://sourcegraph.example.com/"));

        let star = SecurityConfig {
            cors_origin: "*".to_owned(),
            ..SecurityConfig::default()
        };
        assert!(star.is_trusted_origin("https://anything"));

        let empty = SecurityConfig::default();
        assert!(!empty.is_trusted_origin("https://sourcegraph.example.com"));
        assert!(!empty.is_trusted_origin(""));
    }
}
