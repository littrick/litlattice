#![forbid(unsafe_code)]

//! The front-end static asset surface:
//!
//! * `/.assets/*` — `cmd/frontend/internal/app/assetsutil/handler.go`'s
//!   `NewAssetHandler` around `httpgzip.FileServer(assets.Provider.Assets())`;
//! * the UI catch-all (`ui.Router()`, `cmd/frontend/internal/app/ui`) — the
//!   `app.html` SPA shell that boots the unchanged React front-end from the
//!   web build manifest's hashed bundle names, plus the static-page route table
//!   and legacy redirects.
//!
//! The filesystem/manifest side lives in [`sg_ui`]; this module only builds
//! HTTP responses. The outer `mux` already stripped `/.assets`, applied the
//! assets CORS policy and the default security headers, so the ordering here
//! mirrors the baseline `StripPrefix` → `secureHeadersMiddleware` →
//! `assetHandler` chain.

use std::sync::OnceLock;
use bytes::Bytes;
use http::header::{self, HeaderName, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use regex::Regex;
use tonic::body::BoxBody;

use sg_ui::{
    asset_url, AssetEntry, AssetError, AssetReader, BundleField, WebBuildManifest,
};

use crate::app_handlers::{self, AppConfig, AppDeps, AppEnv};

const DEV_VERSION: &str = "0.0.0+dev";
/// `error.html` hardcodes the product name instead of `conf.Branding()`.
const ERROR_PAGE_BRAND: &str = "Sourcegraph";

/// Build a `text/plain; charset=utf-8` response like `http.Error`.
fn http_error(status: StatusCode, message: &str, headers: http::HeaderMap) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(format!(
        "{message}\n"
    )))));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    // `http.Error` deletes a pre-set Content-Length.
    response.headers_mut().remove(header::CONTENT_LENGTH);
    response
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

fn empty_response_with_headers(status: StatusCode, headers: http::HeaderMap) -> Response<BoxBody> {
    let mut response = empty_response(status);
    *response.headers_mut() = headers;
    response
}

fn body_response(status: StatusCode, headers: http::HeaderMap, body: Vec<u8>) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// `localRedirect` (`httpgzip/fs.go:195-201`): a relative 301 with only a
/// `Location` header (no body, no content type).
fn local_redirect<B>(req: &Request<B>, path: &str, headers: http::HeaderMap) -> Response<BoxBody> {
    let location = match req.uri().query() {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_owned(),
    };
    let mut response = empty_response_with_headers(StatusCode::MOVED_PERMANENTLY, headers);
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

/// `assetsutil.NewAssetHandler` (`assetsutil/handler.go:15-65`) with the
/// baseline `httpgzip.FileServer` semantics.
///
/// `path` is the request path after `StripPrefix("/.assets")`, e.g.
/// `/main.abc.js`.
pub fn serve_asset<B>(
    req: &Request<B>,
    path: &str,
    cors_origin: &str,
    assets: &dyn AssetReader,
) -> Response<BoxBody> {
    let mut headers = http::HeaderMap::new();

    // Kludge to set a proper MIME type (`.svg` is sometimes detected as XML).
    let extension = file_path_ext(path);
    if extension == ".svg" {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("image/svg+xml"),
        );
    } else if extension == ".js" {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/javascript"),
        );
    }

    // Allow extensionHostFrame to be rendered in an iframe on trusted origins.
    if file_path_base(path) == "extensionHostFrame.html" && !cors_origin.is_empty() {
        if let Ok(value) = HeaderValue::from_str(&format!("frame-ancestors {cors_origin}")) {
            headers.insert(HeaderName::from_static("content-security-policy"), value);
        }
        if let Ok(value) = HeaderValue::from_str(&format!("allow-from {cors_origin}")) {
            headers.insert(header::X_FRAME_OPTIONS, value);
        }
    }

    // Only cache if the file is found: this avoids caching a 404 while a
    // deployment is propagating. `Assets().Open` is the outer probe.
    if assets.open(path).is_ok() {
        let cache = if path.contains("phabricator.bundle.js") {
            "max-age=300, public"
        } else {
            "immutable, max-age=31536000, public"
        };
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    }

    // `httpgzip.fileServer.ServeHTTP` (`fs.go:88-160`).
    if req.method() != Method::GET {
        headers.insert(header::ALLOW, HeaderValue::from_static("GET"));
        return http_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "405 Method Not Allowed\n\nmethod should be GET",
            headers,
        );
    }

    let cleaned = sg_ui::go_path_clean(&format!("/{path}"));
    let entry = match assets.open(&cleaned) {
        Ok(entry) => entry,
        Err(error) => {
            let (status, message) = match error {
                AssetError::NotFound => (StatusCode::NOT_FOUND, "404 Not Found"),
                AssetError::PermissionDenied => (StatusCode::FORBIDDEN, "403 Forbidden"),
                AssetError::Other(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "500 Internal Server Error")
                }
            };
            return http_error(status, message, headers);
        }
    };

    if entry.is_dir {
        if !path.ends_with('/') && !path.is_empty() {
            // `path.Base(url)+"/"`
            let location = format!("{}/", file_path_base(path));
            return local_redirect(req, &location, headers);
        }
        // `FileServerOptions{DisableDirListing: true}`.
        return http_error(StatusCode::FORBIDDEN, "403 Forbidden", headers);
    }
    if path.ends_with('/') && path != "/" {
        // `"../"+path.Base(url)`
        let location = format!("../{}", file_path_base(path));
        return local_redirect(req, &location, headers);
    }

    serve_content(req, &cleaned, entry, headers, assets)
}

/// `httpgzip.ServeContent` (`gzip.go:36-125`) for the header-relevant paths:
/// precompressed brotli/gzip variants are served verbatim; otherwise the raw
/// bytes are returned (no dynamic compression, see the slice overview §4).
fn serve_content<B>(
    req: &Request<B>,
    cleaned: &str,
    entry: AssetEntry,
    mut headers: http::HeaderMap,
    assets: &dyn AssetReader,
) -> Response<BoxBody> {
    // Eager Content-Type detection happens before the (skipped) dynamic gzip
    // path for the same reason as the baseline: after compression the original
    // bytes are no longer available for sniffing.
    if !headers.contains_key(header::CONTENT_TYPE) {
        let extension = file_path_ext(cleaned);
        let content_type =
            mime_by_extension(extension).unwrap_or_else(|| detect_content_type(&entry.data));
        if let Ok(value) = HeaderValue::from_str(content_type) {
            headers.insert(header::CONTENT_TYPE, value);
        }
    }
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    let accept_encoding = req
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let mut body = entry.data;
    let mut content_encoding: Option<&'static str> = None;

    if header_has_token(accept_encoding, "br") {
        if let Ok(compressed) = assets.open(&format!("{cleaned}.br")) {
            if !compressed.is_dir {
                body = compressed.data;
                content_encoding = Some("br");
            }
        }
    }
    if content_encoding.is_none() && header_has_token(accept_encoding, "gzip") {
        if let Ok(compressed) = assets.open(&format!("{cleaned}.gz")) {
            if !compressed.is_dir {
                body = compressed.data;
                content_encoding = Some("gzip");
            }
        }
    }

    if let Some(encoding) = content_encoding {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static(encoding));
        if let Ok(value) = HeaderValue::from_str(accept_encoding) {
            headers.append(header::VARY, value);
        }
        // `net/http.ServeContent` skips Content-Length when a Content-Encoding
        // is set (`fs.go:360-371`).
    } else {
        let length = body.len();
        if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
            headers.insert(header::CONTENT_LENGTH, value);
        }
    }

    body_response(StatusCode::OK, headers, body)
}

/// `httpguts.HeaderValuesContainsToken` (tokens are case-insensitive).
fn header_has_token(value: &str, token: &str) -> bool {
    value
        .split(',')
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(token))
}

/// `filepath.Ext`.
fn file_path_ext(path: &str) -> &str {
    let base_start = path.rfind('/').map(|index| index + 1).unwrap_or(0);
    match path[base_start..].rfind('.') {
        Some(dot) => &path[base_start + dot..],
        None => "",
    }
}

/// `path.Base`.
fn file_path_base(path: &str) -> &str {
    if path.is_empty() {
        return ".";
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    match trimmed.rfind('/') {
        Some(index) => &trimmed[index + 1..],
        None => trimmed,
    }
}

/// `mime.TypeByExtension` for the types the web bundle/UI actually serves.
fn mime_by_extension(extension: &str) -> Option<&'static str> {
    Some(match extension {
        ".css" => "text/css; charset=utf-8",
        ".html" | ".htm" => "text/html; charset=utf-8",
        ".json" => "application/json",
        ".png" => "image/png",
        ".txt" => "text/plain; charset=utf-8",
        ".woff2" => "font/woff2",
        ".woff" => "font/woff",
        ".ttf" => "font/ttf",
        ".ico" => "image/vnd.microsoft.icon",
        ".wasm" => "application/wasm",
        ".svg" => "image/svg+xml",
        ".gif" => "image/gif",
        ".jpg" | ".jpeg" => "image/jpeg",
        ".webmanifest" => "application/manifest+json",
        ".js" | ".mjs" => "text/javascript; charset=utf-8",
        ".gz" => "application/gzip",
        ".map" => "application/json",
        _ => return None,
    })
}

/// `http.DetectContentType` subset: HTML-looking text, other textual bytes and
/// the binary fallback. The corpus only exercises the first two.
fn detect_content_type(data: &[u8]) -> &'static str {
    let sample = &data[..data.len().min(512)];
    let trimmed = sample
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c))
        .map(|index| &sample[index..])
        .unwrap_or(sample);
    const HTML_TAGS: [&[u8]; 15] = [
        b"<!DOCTYPE HTML",
        b"<HTML",
        b"<HEAD",
        b"<SCRIPT",
        b"<IFRAME",
        b"<H1",
        b"<DIV",
        b"<FONT",
        b"<TABLE",
        b"<A",
        b"<STYLE",
        b"<TITLE",
        b"<B",
        b"<BODY",
        b"<BR",
    ];
    if trimmed.starts_with(b"<P") || trimmed.starts_with(b"<!--") {
        return "text/html; charset=utf-8";
    }
    for tag in HTML_TAGS {
        if trimmed.len() >= tag.len() && trimmed[..tag.len()].eq_ignore_ascii_case(tag) {
            return "text/html; charset=utf-8";
        }
    }
    if std::str::from_utf8(sample).is_ok()
        && sample
            .iter()
            .all(|byte| !byte.is_ascii_control() || matches!(byte, b'\t' | b'\n' | b'\r' | 0x0c))
    {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

/// `app.html` SPA shell input.
pub struct ShellInput<'a> {
    pub title: String,
    pub manifest: &'a WebBuildManifest,
    pub assets_root: &'a str,
    /// `(as, href)` preload pairs (`Common.PreloadedAssets`).
    pub preloads: Vec<(String, String)>,
    pub indexed: bool,
}

impl ShellInput<'_> {
    fn bundle(&self, field: BundleField) -> String {
        self.manifest.bundle_url(self.assets_root, field)
    }
}

/// Render the `app.html` SPA shell (`cmd/frontend/internal/app/ui/app.html`)
/// for the fields this slice owns: title, hashed bundles and preloads.
///
/// The full `JSContext`/injected HTML/SvelteKit payloads are follow-up slices;
/// `window.context` is left as an empty object.
pub fn render_shell(input: &ShellInput<'_>) -> String {
    let mut html = String::new();
    html.push_str("<!doctype html>\n<html lang=\"en\" class=\"base\">\n\n<head>\n");
    html.push_str("\t<meta charset=\"utf-8\">\n");
    html.push_str("\t<meta http-equiv=\"x-ua-compatible\" content=\"ie=edge\">\n");
    html.push_str("\t<meta name=\"google\" content=\"notranslate\">\n");
    html.push_str("\t<meta http-equiv=\"Content-Language\" content=\"en\">\n");
    html.push_str("\t<meta name=\"viewport\" content=\"width=device-width, viewport-fit=cover\" />\n");
    html.push_str("\t<meta name=\"referrer\" content=\"strict-origin-when-cross-origin\"/>\n");
    html.push_str("\t<meta name=\"color-scheme\" content=\"light dark\"/>\n");
    html.push_str(&format!(
        "\t<title>{}</title>\n",
        escape_html(input.title.as_str())
    ));
    if !input.manifest.main_css_bundle_path.is_empty() {
        html.push_str(&format!(
            "\t<link rel=\"stylesheet\" href=\"{}\">\n",
            input.bundle(BundleField::MainCss)
        ));
    }
    html.push_str("\t<link id='sourcegraph-chrome-webstore-item' rel=\"chrome-webstore-item\" href=\"https://chrome.google.com/webstore/detail/dgjhfomjieaadpoljlnidmbgkdffpack\">\n");
    html.push_str("\t<link rel=\"search\" href=\"/opensearch.xml\" type=\"application/opensearchdescription+xml\" title=\"Sourcegraph Search\">\n");
    for (as_, href) in &input.preloads {
        html.push_str(&format!(
            "\t<link rel=\"preload\" href=\"{}\" as=\"{}\" />\n",
            escape_html(href),
            escape_html(as_)
        ));
    }
    html.push_str("\t<script ignore-csp>\n");
    html.push_str("\t\twindow.context = {}\n");
    html.push_str("\t\twindow.pageError = null\n");
    html.push_str("\t</script>\n");
    html.push_str("</head>\n\n<body>\n\t<div id=\"root\"></div>\n");
    html.push_str(&format!(
        "\t<script src=\"{}\" type=\"module\"></script>\n",
        input.bundle(BundleField::MainJs)
    ));
    html.push_str("\t<noscript>\n\t\t<p>Sourcegraph is a web-based code search and navigation tool for dev teams. Search, navigate, and review code. Find answers.</p>\n\t\t<br><br><br>\n\t\tYou need to enable JavaScript to run this app.\n\t</noscript>\n");
    html.push_str("</body>\n\n</html>\n");
    html
}

/// The response wrapper for [`render_shell`].
pub fn shell_response(input: &ShellInput<'_>, status: StatusCode) -> Response<BoxBody> {
    let body = render_shell(input);
    let mut response = body_response(status, http::HeaderMap::new(), body.into_bytes());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    if !input.indexed {
        response.headers_mut().insert(
            HeaderName::from_static("x-robots-tag"),
            HeaderValue::from_static("noindex"),
        );
    }
    response
}

/// `error.html` (`cmd/frontend/internal/app/ui/error.html`). The random error
/// ID is left empty; the differential compares status/title only.
fn error_page_response(status: StatusCode, error: Option<&str>) -> Response<BoxBody> {
    let status_text = status_text(status.as_u16());
    let title = format!(
        "{} {} - {}",
        status.as_u16(),
        status_text,
        ERROR_PAGE_BRAND
    );
    let mut body = String::new();
    body.push_str("<!doctype html>\n<html lang=\"en\">\n<head>\n\t<meta charset=\"utf-8\">\n");
    body.push_str(&format!(
        "\t<title>{}</title>\n",
        escape_html(title.as_str())
    ));
    body.push_str("</head>\n<body>\n\t<div class=\"error\">\n\t\t<h1>\n");
    body.push_str(&format!(
        "\t\t\t<span class=\"status-code\">{}</span>\n",
        status.as_u16()
    ));
    body.push_str(&format!(
        "\t\t\t<span class=\"status-text\">{status_text}</span>\n"
    ));
    body.push_str("\t\t</h1>\n");
    if let Some(error) = error {
        body.push_str(&format!(
            "\t\t\t<pre class=\"error-text\">{}</pre>\n",
            escape_html(error)
        ));
    }
    body.push_str("\t\t<hr />\n\t\t<p>Sorry, there's been a problem. Please <a href=\"mailto:support@sourcegraph.com\">contact us</a> and include the error ID: <strong></strong></p>\n\t</div>\n</body>\n</html>\n");
    let mut headers = http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    body_response(status, headers, body.into_bytes())
}

/// `RouteName::Ui` dispatch: `ui.Router()`'s clean/StrictSlash redirects, its
/// static-page table, the SPA shell and the 404/500 error pages.
///
/// `path` is `URL.Path` (percent-decoded) exactly as the gorilla router sees
/// it. Without an injected asset provider the route keeps the earlier 404 (the
/// M9.8iu differential oracle relies on this).
pub fn serve_ui<B>(
    req: &Request<B>,
    path: &str,
    env: &AppEnv,
    config: &AppConfig,
    deps: &AppDeps,
) -> Response<BoxBody> {
    let Some(assets) = deps.ui_assets.as_deref() else {
        return app_handlers::not_found();
    };
    let path = if path.is_empty() { "/" } else { path };

    // `mux.Router.ServeHTTP`'s `cleanPath` redirect (no body, no content type).
    let cleaned = crate::mux::clean_path(path);
    if cleaned != path {
        return clean_path_redirect(req, &cleaned);
    }

    let candidates = ui_candidates(path, env);
    let mut method_mismatch = false;
    for candidate in candidates {
        if candidate.method != req.method() {
            method_mismatch = true;
            continue;
        }
        return candidate.serve(req, env, config, assets, deps);
    }
    if method_mismatch {
        // `gorilla/mux`'s `methodNotAllowedHandler`: an empty 405.
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    ui_not_found(env, config, assets)
}

/// One candidate route that matched the request path.
struct UiCandidate {
    method: Method,
    action: UiAction,
}

enum UiAction {
    /// `serveHome` (`handlers.go:351-378`).
    Home,
    /// `serveBasicPage`/`serveSignIn`; `title` is the page segment before
    /// `brandNameSubtitle` appends the brand.
    Shell { title: String, indexed: bool },
    /// `/search`'s title depends on `?q=`.
    Search,
    /// `serveEmbed`: the feature flag is off by default → empty 404.
    Embed,
    /// `servePingFromSelfHosted` (`handlers.go:566-635`): CORS headers from
    /// the `Referer` host, 204 for OPTIONS and an empty 200 for GET. The
    /// baseline's HubSpot `SyncUser` call is a no-op here (errata §4.4).
    PingSelfHosted,
    /// One `/githubapp/` subroute (`githubapp/httpapi.go:64-68`).
    GitHubApp(GitHubAppRoute),
    /// The repository routes (`router.go:244-330`). The DB/gitserver part is
    /// not available, so the handler keeps the `ui_not_found` fallback; the
    /// routing (GET-only, `StrictSlash`) still matches the baseline.
    RepoUnavailable,
    /// `http.Redirect` (`net/http/server.go`). `keep_query` mirrors the
    /// baseline handlers that re-serialize `r.URL` (StrictSlash,
    /// `staticRedirectHandler`, the Sourcegraph.com subdomain routes).
    Redirect {
        location: String,
        code: u16,
        keep_query: bool,
    },
}

impl UiCandidate {
    fn new(method: Method, action: UiAction) -> Self {
        Self { method, action }
    }

    fn get(action: UiAction) -> Self {
        Self::new(Method::GET, action)
    }

    fn serve<B>(
        &self,
        req: &Request<B>,
        env: &AppEnv,
        config: &AppConfig,
        assets: &dyn sg_ui::AssetsProvider,
        deps: &AppDeps,
    ) -> Response<BoxBody> {
        match &self.action {
            UiAction::Home => serve_home(req, env, config, assets),
            UiAction::Search => {
                let title = search_title(req, config);
                shell_with_manifest(env, config, assets, title, true, StatusCode::OK)
            }
            UiAction::Shell { title, indexed } => shell_with_manifest(
                env,
                config,
                assets,
                format!("{} - {}", title, config.brand_name),
                *indexed,
                StatusCode::OK,
            ),
            UiAction::Embed => empty_response(StatusCode::NOT_FOUND),
            UiAction::PingSelfHosted => serve_ping_from_self_hosted(req, env, config, assets),
            UiAction::GitHubApp(route) => serve_github_app(req, *route, deps),
            UiAction::RepoUnavailable => ui_not_found(env, config, assets),
            UiAction::Redirect {
                location,
                code,
                keep_query,
            } => {
                let location = match (keep_query, req.uri().query()) {
                    (true, Some(query)) if !query.is_empty() => format!("{location}?{query}"),
                    _ => location.clone(),
                };
                app_handlers::redirect(req, &location, *code)
            }
        }
    }
}

/// `serveHome` (`handlers.go:351-378`).
fn serve_home<B>(
    req: &Request<B>,
    env: &AppEnv,
    config: &AppConfig,
    assets: &dyn sg_ui::AssetsProvider,
) -> Response<BoxBody> {
    // `newCommon` runs first, so a missing manifest is a 500 even for HEAD.
    let _manifest = match assets.load_web_build_manifest() {
        Ok(manifest) => manifest,
        Err(error) => return manifest_error_response(env, config, &error),
    };
    if req.method() == Method::HEAD {
        return empty_response(StatusCode::OK);
    }
    // The baseline redirects unconditionally for non-HEAD requests; the
    // Cody-only `/cody` branch depends on `JSContext` (see overview §4).
    let location = match req.uri().query() {
        Some(query) if !query.is_empty() => format!("/search?{query}"),
        _ => "/search".to_owned(),
    };
    app_handlers::redirect(req, &location, 307)
}

fn shell_with_manifest(
    env: &AppEnv,
    config: &AppConfig,
    assets: &dyn sg_ui::AssetsProvider,
    title: String,
    indexed: bool,
    status: StatusCode,
) -> Response<BoxBody> {
    let manifest = match assets.load_web_build_manifest() {
        Ok(manifest) => manifest,
        Err(error) => return manifest_error_response(env, config, &error),
    };
    let input = shell_input(env, config, &manifest, title, indexed);
    shell_response(&input, status)
}

fn manifest_error_response(
    env: &AppEnv,
    _config: &AppConfig,
    error: &sg_ui::ManifestError,
) -> Response<BoxBody> {
    let message = format!("loading web build manifest: {error}");
    let shown = env.insecure_dev.then_some(message.as_str());
    error_page_response(StatusCode::INTERNAL_SERVER_ERROR, shown)
}

/// The unknown-route `NotFoundHandler` → `serveError(..., 404)` path: the
/// fancy `app.html` error page when the manifest loads, `error.html` otherwise.
fn ui_not_found(
    env: &AppEnv,
    config: &AppConfig,
    assets: &dyn sg_ui::AssetsProvider,
) -> Response<BoxBody> {
    match assets.load_web_build_manifest() {
        Ok(manifest) => {
            let title = format!("404 Not Found - {}", config.brand_name);
            let input = shell_input(env, config, &manifest, title, true);
            shell_response(&input, StatusCode::NOT_FOUND)
        }
        Err(_) => error_page_response(StatusCode::NOT_FOUND, None),
    }
}

fn shell_input<'a>(
    env: &'a AppEnv,
    config: &AppConfig,
    manifest: &'a WebBuildManifest,
    title: String,
    indexed: bool,
) -> ShellInput<'a> {
    // `newCommon`: the default mark is preloaded unless custom branding defines
    // a light or dark logo.
    let preloads = if config.branding_has_theme {
        Vec::new()
    } else {
        vec![(
            "image".to_owned(),
            format!(
                "{}?v2",
                asset_url(&env.assets_root, "/img/sourcegraph-mark.svg")
            ),
        )]
    };
    ShellInput {
        title,
        manifest,
        assets_root: &env.assets_root,
        preloads,
        indexed,
    }
}

/// `serveBasicPage` title for `/search` (`router.go:218-226`).
fn search_title<B>(req: &Request<B>, config: &AppConfig) -> String {
    let query = app_handlers::query_param(req.uri().query(), "q").unwrap_or_default();
    let short = limit_string(&query, 25, true);
    if short.is_empty() {
        config.brand_name.clone()
    } else {
        format!("{} - {}", short, config.brand_name)
    }
}

/// `limitString` (`router.go:364-372`).
fn limit_string(value: &str, n: usize, ellipsis: bool) -> String {
    if value.len() < n {
        return value.to_owned();
    }
    let cut = &value.as_bytes()[..n - 1];
    let cut = String::from_utf8_lossy(cut).into_owned();
    if ellipsis {
        format!("{cut}…")
    } else {
        cut
    }
}

/// The gorilla route table, in registration order (`ui/router.go:108-330`).
fn ui_candidates(path: &str, env: &AppEnv) -> Vec<UiCandidate> {
    let candidates = Vec::new();

    // `r.Path("/").Methods(Get, Head)`
    if path == "/" {
        return vec![
            UiCandidate::new(Method::GET, UiAction::Home),
            UiCandidate::new(Method::HEAD, UiAction::Home),
        ];
    }
    if path == "/sign-in" {
        return vec![
            UiCandidate::new(Method::GET, sign_in_action()),
            UiCandidate::new(Method::HEAD, sign_in_action()),
        ];
    }
    if let Some(location) = strict_slash_location(path, "/sign-in") {
        return vec![UiCandidate::get(UiAction::Redirect {
            location,
            code: 301,
            keep_query: true,
        })];
    }

    // `/ping-from-self-hosted` (`router.go:116`, `handlers.go:566`).
    if path == "/ping-from-self-hosted" {
        return vec![
            UiCandidate::new(Method::GET, UiAction::PingSelfHosted),
            UiCandidate::new(Method::OPTIONS, UiAction::PingSelfHosted),
        ];
    }
    if let Some(location) = strict_slash_location(path, "/ping-from-self-hosted") {
        return vec![UiCandidate::get(UiAction::Redirect {
            location,
            code: 301,
            keep_query: true,
        })];
    }

    // `/githubapp/` subrouter (`githubapp/httpapi.go:64-81`). The baseline's
    // `router.Use(siteAdminMiddleware)` runs *after* the routes are
    // registered, so gorilla never applies it; each handler's own output is
    // observable instead (errata §4.4). Routes need Redis/the DB only after
    // their offline-deterministic validation.
    if let Some(location) = strict_slash_location(path, "/githubapp") {
        return vec![UiCandidate::get(UiAction::Redirect {
            location,
            code: 301,
            keep_query: true,
        })];
    }
    if let Some(route) = github_app_route(path) {
        return vec![UiCandidate::get(UiAction::GitHubApp(route))];
    }
    if let Some(location) = github_app_strict_slash(path) {
        return vec![UiCandidate::get(UiAction::Redirect {
            location,
            code: 301,
            keep_query: true,
        })];
    }

    // Basic pages with static titles (`router.go:113-162`).
    const STATIC_PAGES: &[(&str, bool, &str, bool)] = &[
        // (path, is_prefix, title, indexed)
        ("/insights", true, "Insights", true),
        ("/search-jobs", true, "Search Jobs", true),
        ("/setup", true, "Setup", true),
        ("/batch-changes", true, "Batch Changes", true),
        ("/code-monitoring", true, "Code Monitoring", true),
        ("/notebooks", true, "Notebooks", true),
        ("/request-access", true, "Request access", true),
        ("/search/console", false, "Search console", true),
        ("/api/console", false, "API console", true),
        ("/sign-up", false, "Sign up", true),
        ("/organizations", true, "Organization", false),
        ("/teams", true, "Team", false),
        ("/settings", true, "Settings", false),
        ("/site-admin", true, "Admin", false),
        ("/contexts", true, "Search Contexts", false),
        ("/saved-searches", true, "Saved searches", false),
        ("/prompts", true, "Prompts", false),
        ("/cody/dashboard", false, "Cody Dashboard", false),
        ("/cody/manage", false, "Cody Manage", false),
        ("/cody/subscription", false, "Cody Pricing", false),
        ("/cody/chat", false, "Cody", false),
        ("/password-reset", false, "Reset password", false),
        ("/survey", false, "Survey", false),
        ("/post-sign-up", false, "Cody", false),
    ];
    if let Some((title, indexed, redirect)) = match_static_page(path, STATIC_PAGES) {
        return vec![match redirect {
            Some(location) => UiCandidate::get(UiAction::Redirect {
                location,
                code: 301,
                keep_query: true,
            }),
            None => UiCandidate::get(UiAction::Shell {
                title: title.to_owned(),
                indexed,
            }),
        }];
    }
    if let Some(candidate) = pattern_static_candidate(path) {
        return vec![candidate];
    }

    if path.starts_with("/embed") {
        return vec![UiCandidate::get(UiAction::Embed)];
    }

    // users (`router.go:174-183`). Note `PathPrefix("/user")` is registered
    // before `PathPrefix("/users/{username}")`, so it absorbs the latter.
    if path.starts_with("/users/") {
        if let Some(rest) = path.strip_prefix("/users/") {
            let username = rest.split('/').next().unwrap_or("");
            if !username.is_empty() && rest.starts_with(&format!("{username}/settings")) {
                return vec![UiCandidate::get(UiAction::Shell {
                    title: "User settings".to_owned(),
                    indexed: false,
                })];
            }
        }
    }
    if path.starts_with("/user") {
        return vec![UiCandidate::get(UiAction::Shell {
            title: "User".to_owned(),
            indexed: false,
        })];
    }

    // search (`router.go:220-230`).
    if path == "/search" {
        return vec![UiCandidate::get(UiAction::Search)];
    }
    if let Some(location) = strict_slash_location(path, "/search") {
        return vec![UiCandidate::get(UiAction::Redirect {
            location,
            code: 301,
            keep_query: true,
        })];
    }

    // Sourcegraph.com-only marketing/legacy routes (`router.go:232-275`).
    if env.dotcom {
        // `aboutRedirects` (`router.go:66-81`); the `/{Path:(?:...)}` pattern
        // also matches the two-segment `help/terms` key.
        const ABOUT: &[(&str, &str)] = &[
            ("/about", "about"),
            ("/blog", "blog"),
            ("/customers", "customers"),
            ("/docs", "docs"),
            ("/handbook", "handbook"),
            ("/news", "news"),
            ("/plan", "plan"),
            ("/contact", "contact"),
            ("/pricing", "pricing"),
            ("/privacy", "privacy"),
            ("/security", "security"),
            ("/terms", "terms"),
            ("/jobs", "jobs"),
            ("/help/terms", "terms"),
        ];
        if let Some((_, value)) = ABOUT.iter().find(|(candidate, _)| *candidate == path) {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: format!("https://sourcegraph.com/{value}"),
                code: 307,
                keep_query: true,
            })];
        }
        // `StrictSlash` also applies to these regex `Path` routes, so
        // `/about/` (and `/help/terms/`) redirect to the slashless form first.
        if let Some((candidate, _)) = ABOUT
            .iter()
            .find(|(candidate, _)| strict_slash_location(path, candidate).is_some())
        {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: (*candidate).to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
        const COMMUNITY: &[&str] = &[
            "kubernetes",
            "stanford",
            "stackstorm",
            "temporal",
            "o3de",
            "chakraui",
            "julia",
            "backstage",
        ];
        if COMMUNITY
            .iter()
            .any(|name| path.strip_prefix('/') == Some(*name))
        {
            return vec![UiCandidate::get(UiAction::Shell {
                title: "Community search context".to_owned(),
                indexed: false,
            })];
        }
        if let Some(location) = strict_slash_location(path, "/cncf") {
            return vec![UiCandidate::get(UiAction::Redirect {
                location,
                code: 301,
                keep_query: true,
            })];
        }
        if path == "/cncf" {
            return vec![UiCandidate::get(UiAction::Shell {
                title: "CNCF code search".to_owned(),
                indexed: true,
            })];
        }
        if path.starts_with("/devtooltime") {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: "https://info.sourcegraph.com/dev-tool-time".to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
        if let Some(location) = strict_slash_location(path, "/login") {
            return vec![UiCandidate::get(UiAction::Redirect {
                location,
                code: 301,
                keep_query: true,
            })];
        }
        if path == "/login" {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: "/sign-in".to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
        if let Some(location) = strict_slash_location(path, "/careers") {
            return vec![UiCandidate::get(UiAction::Redirect {
                location,
                code: 301,
                keep_query: true,
            })];
        }
        if path == "/careers" {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: "https://sourcegraph.com/jobs".to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
        if path.starts_with("/extensions") {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: "/".to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
    }

    // `/help` (`help.go:22-64`). `serveHelp` builds a fresh URL, so the
    // request query is dropped.
    if path.starts_with("/help") {
        return vec![UiCandidate::get(UiAction::Redirect {
            location: help_location(path, env),
            code: 307,
            keep_query: false,
        })];
    }

    // The repository routes are the last ones (`router.go:244-330`). The
    // DB/gitserver part is not available, so known repo routes keep the
    // `ui_not_found` fallback (errata §4.4); the method/StrictSlash routing
    // still matches the baseline. StrictSlash trims the trailing slash before
    // matching either the root or one of the `/-/` subroutes.
    if let Some(base) = path.strip_suffix('/') {
        if repo_route_like(base) || repo_subroute_kind(base, env).is_some() {
            return vec![UiCandidate::get(UiAction::Redirect {
                location: base.to_owned(),
                code: 301,
                keep_query: true,
            })];
        }
    }
    if repo_route_like(path) {
        return vec![UiCandidate::get(UiAction::RepoUnavailable)];
    }
    if let Some(kind) = repo_subroute_kind(path, env) {
        return match kind {
            RepoSubroute::Raw => vec![
                UiCandidate::new(Method::GET, UiAction::RepoUnavailable),
                UiCandidate::new(Method::HEAD, UiAction::RepoUnavailable),
            ],
            RepoSubroute::Get => vec![UiCandidate::get(UiAction::RepoUnavailable)],
        };
    }

    // Unknown route: the router's NotFoundHandler.
    candidates
}

fn sign_in_action() -> UiAction {
    UiAction::Shell {
        title: "Sign in".to_owned(),
        indexed: true,
    }
}

/// `(title, indexed, redirect)` for an exact or prefix static page.
fn match_static_page<'a>(
    path: &str,
    pages: &'a [(&'a str, bool, &'a str, bool)],
) -> Option<(&'a str, bool, Option<String>)> {
    for (template, is_prefix, title, indexed) in pages {
        if *is_prefix {
            if path.starts_with(template) {
                return Some((title, *indexed, None));
            }
        } else if path == *template {
            return Some((title, *indexed, None));
        } else if let Some(location) = strict_slash_location(path, template) {
            return Some((title, *indexed, Some(location)));
        }
    }
    None
}

/// The single-segment pattern routes (`/cody/chat/{chatID}`,
/// `/unlock-account/{token}`, `/survey/{score}`), including their
/// `StrictSlash` redirects.
fn pattern_static_candidate(path: &str) -> Option<UiCandidate> {
    const SINGLE_SEGMENT: &[(&str, &str, bool)] = &[
        ("/cody/chat/", "Cody", false),
        ("/unlock-account/", "Unlock Your Account", false),
        ("/survey/", "Survey", false),
    ];
    for (prefix, title, indexed) in SINGLE_SEGMENT {
        let Some(rest) = path.strip_prefix(prefix) else {
            continue;
        };
        let (segment, trailing) = match rest.strip_suffix('/') {
            Some(trimmed) => (trimmed, true),
            None => (rest, false),
        };
        if segment.is_empty() || segment.contains('/') {
            continue;
        }
        if trailing {
            return Some(UiCandidate::get(UiAction::Redirect {
                location: format!("{prefix}{segment}"),
                code: 301,
                keep_query: true,
            }));
        }
        return Some(UiCandidate::get(UiAction::Shell {
            title: title.to_string(),
            indexed: *indexed,
        }));
    }
    None
}

/// `StrictSlash` redirect target for an exact route template.
fn strict_slash_location(path: &str, template: &str) -> Option<String> {
    if template.ends_with('/') {
        return None;
    }
    if path.len() == template.len() + 1 && path.starts_with(template) && path.ends_with('/') {
        return Some(template.to_owned());
    }
    None
}

/// `serveHelp` (`help.go:22-64`).
fn help_location(path: &str, env: &AppEnv) -> String {
    let page = path.strip_prefix("/help").unwrap_or("");
    let is_dev = env.build_version == DEV_VERSION;
    let mut doc_rev_prefix = String::new();
    if !is_dev && !env.dotcom {
        if let Some((major, minor)) = parse_major_minor(&env.build_version) {
            doc_rev_prefix = format!("v/{major}.{minor}");
        }
    }
    if is_dev && !env.dotcom {
        let joined = sg_ui::path_join(&["/", "", page]);
        return format!("http://localhost:3000{joined}");
    }
    let joined = sg_ui::path_join(&["/", "docs", &doc_rev_prefix, page]);
    format!("https://sourcegraph.com{joined}")
}

/// `coreos/go-semver`'s `NewVersion` for the `v/<major>.<minor>` docs prefix.
/// It requires exactly three dotted numeric identifiers, so `4.5` fails and
/// the redirect falls back to the unversioned docs.
fn parse_major_minor(version: &str) -> Option<(u64, u64)> {
    let core = version.split('+').next().unwrap_or(version);
    let core = core.split('-').next().unwrap_or(core);
    let mut parts = core.splitn(3, '.');
    let major: i64 = parts.next()?.parse().ok()?;
    let minor: i64 = parts.next()?.parse().ok()?;
    let patch: i64 = parts.next()?.parse().ok()?;
    if major < 0 || minor < 0 || patch < 0 {
        return None;
    }
    Some((major as u64, minor as u64))
}

/// The `routevar.Repo`/`RepoRevSuffix` matcher (`routevar/repo.go:10-30`):
/// `/`-separated components, each either a single character other than `@`,
/// `/`, `-`, or any two-or-more characters without `@`/`/`.
fn repo_route_like(path: &str) -> bool {
    static REPO: OnceLock<Regex> = OnceLock::new();
    let repo = REPO.get_or_init(|| {
        Regex::new(r"^/(?:[^@/-]|[^/@]{2,})(?:/(?:[^@/-]|[^/@]{2,}))*(?:@(?:[^@/-]|[^/@]{2,})(?:/(?:[^@/-]|[^/@]{2,}))*)?$")
            .expect("repo regex")
    });
    repo.is_match(path)
}

/// One `/githubapp/` subroute (`githubapp/httpapi.go:64-68`), handled by
/// [`crate::githubapp_http_api`].
pub use crate::githubapp_http_api::GitHubAppRoute;

fn github_app_route(path: &str) -> Option<GitHubAppRoute> {
    crate::githubapp_http_api::route(path)
}

fn github_app_strict_slash(path: &str) -> Option<String> {
    for template in [
        "/githubapp/state",
        "/githubapp/new-app-state",
        "/githubapp/redirect",
        "/githubapp/setup",
    ] {
        if let Some(location) = strict_slash_location(path, template) {
            return Some(location);
        }
    }
    None
}

/// `servePingFromSelfHosted` (`handlers.go:566-635`): the CORS origin is the
/// `Referer`'s host (Go's `url.Parse` keeps `host:port`), OPTIONS is an empty
/// 204 and GET an empty 200. The HubSpot `SyncUser` call has no port and is
/// skipped (errata §4.4).
fn serve_ping_from_self_hosted<B>(
    req: &Request<B>,
    env: &AppEnv,
    _config: &AppConfig,
    _assets: &dyn sg_ui::AssetsProvider,
) -> Response<BoxBody> {
    let referer = req
        .headers()
        .get(header::REFERER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let host = match go_url_host(referer) {
        Ok(host) => host,
        Err(error) => {
            let shown = env.insecure_dev.then_some(error.as_str());
            return error_page_response(StatusCode::INTERNAL_SERVER_ERROR, shown);
        }
    };
    let mut headers = http::HeaderMap::new();
    headers.insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_bytes(host.as_bytes()).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    headers.insert(
        HeaderName::from_static("access-control-allow-credentials"),
        HeaderValue::from_static("true"),
    );
    if req.method() == Method::OPTIONS {
        return empty_response_with_headers(StatusCode::NO_CONTENT, headers);
    }
    empty_response_with_headers(StatusCode::OK, headers)
}

/// Go `url.Parse(referer).Host`, including the `host:port` form.
fn go_url_host(referer: &str) -> Result<String, String> {
    let bytes = referer.as_bytes();
    for byte in bytes {
        if *byte < 0x20 || *byte == 0x7f {
            return Err("net/url: invalid control character in URL".to_owned());
        }
    }
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            let end = (index + 3).min(bytes.len());
            return Err(format!(
                "invalid URL escape \"{}\"",
                &referer[index..end]
            ));
        }
        index += 1;
    }

    // A scheme is only followed by an authority after `//`; otherwise the URL
    // is opaque (`u.Host` is empty).
    let rest = match referer.find("://") {
        Some(pos) if is_url_scheme(&referer[..pos]) => &referer[pos + 3..],
        _ => match referer.strip_prefix("//") {
            Some(rest) => rest,
            None => return Ok(String::new()),
        },
    };
    let authority_end = rest
        .find(['/', '?', '#'])
        .unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let authority = match authority.rfind('@') {
        Some(pos) => &authority[pos + 1..],
        None => authority,
    };
    Ok(authority.to_owned())
}

fn is_url_scheme(candidate: &str) -> bool {
    let mut chars = candidate.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

/// `githubapp.httpapi`: delegate to the handler module. With the production
/// wiring injected through [`AppDeps::github_app`] the handlers use the real
/// Redis/Postgres/HTTP; otherwise they keep the offline-deterministic branches
/// (validated before any external call), exactly as the UI tests expect.
fn serve_github_app<B>(
    req: &Request<B>,
    route: GitHubAppRoute,
    deps: &AppDeps,
) -> Response<BoxBody> {
    match deps.github_app.as_deref() {
        Some(server) => server.serve(req, route),
        None => crate::githubapp_http_api::serve_offline(req, route),
    }
}

/// The `/-/` subroutes of the repo route (`router.go:268-320`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RepoSubroute {
    Raw,
    Get,
}

/// `Some` when `path` is `<repo>/-/<known subroute>`. Unknown `/-/` paths do
/// not match a gorilla route and keep the outer NotFoundHandler.
fn repo_subroute_kind(path: &str, env: &AppEnv) -> Option<RepoSubroute> {
    let (repo, rest) = path.split_once("/-/")?;
    if !repo_route_like(repo) {
        return None;
    }
    if rest == "raw" || rest.starts_with("raw") {
        return Some(RepoSubroute::Raw);
    }
    const GET_PREFIXES: &[&str] = &[
        "tree",
        "blob",
        "batch-changes",
        "settings",
        "code-graph",
        "commits",
        "commit",
        "branches",
        "tags",
        "compare",
        "stats",
        "own",
    ];
    if GET_PREFIXES
        .iter()
        .any(|prefix| rest.starts_with(prefix))
    {
        return Some(RepoSubroute::Get);
    }
    if env.dotcom
        && (rest == "info"
            || rest.starts_with("info/")
            || rest.starts_with("def/")
            || rest.starts_with("refs/")
            || rest.starts_with("land/"))
    {
        return Some(RepoSubroute::Get);
    }
    None
}

/// `gorilla/mux`'s ServeHTTP `cleanPath` redirect: `Location` + 301, no body.
fn clean_path_redirect<B>(req: &Request<B>, cleaned: &str) -> Response<BoxBody> {
    let location = match req.uri().query() {
        Some(query) if !query.is_empty() => format!("{cleaned}?{query}"),
        _ => cleaned.to_owned(),
    };
    let mut response = empty_response(StatusCode::MOVED_PERMANENTLY);
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

/// `http.StatusText` for the codes the UI emits.
fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        301 => "Moved Permanently",
        307 => "Temporary Redirect",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "",
    }
}

/// `html/template`'s `htmlReplacementTable` (text/attribute contexts),
/// including the UTF-7 protection for `+`.
fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\0' => escaped.push('\u{FFFD}'),
            '"' => escaped.push_str("&#34;"),
            '&' => escaped.push_str("&amp;"),
            '\'' => escaped.push_str("&#39;"),
            '+' => escaped.push_str("&#43;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            ch => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use http_body_util::BodyExt;
    use sg_ui::{MemoryAssets, UiAssets};
    use std::sync::Arc;

    fn assets(files: &[(&str, &[u8])]) -> MemoryAssets {
        MemoryAssets::from_paths(files.iter().copied(), [])
    }

    fn request(method: Method, target: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(target)
            .body(())
            .expect("request")
    }

    async fn body(response: Response<BoxBody>) -> Vec<u8> {
        response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec()
    }

    fn ui_fixture(files: &[(&str, &[u8])]) -> (AppEnv, AppConfig, AppDeps, TestDir) {
        let dir = TestDir::new("m910b_unit");
        std::fs::write(
            dir.path().join("web.manifest.json"),
            br#"{"main.js":"main.abc.js","main.css":"main.def.css"}"#,
        )
        .unwrap();
        let assets = UiAssets::new(dir.path(), Arc::new(assets(files)));
        let deps = AppDeps {
            ui_assets: Some(Arc::new(assets) as Arc<dyn sg_ui::AssetsProvider>),
            ..AppDeps::default()
        };
        (AppEnv::default(), AppConfig::default(), deps, dir)
    }

    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new(prefix: &str) -> Self {
                        static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "{prefix}_{}_{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn serves_js_with_override_and_cache() {
        let assets = assets(&[("/main.abc123.js", b"console.log(1);")]);
        let req = request(Method::GET, "/.assets/main.abc123.js");
        let response = serve_asset(&req, "/main.abc123.js", "", &assets);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/javascript"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "immutable, max-age=31536000, public"
        );
        assert_eq!(body(response).await, b"console.log(1);");
    }

    #[tokio::test]
    async fn missing_file_is_404_without_cache() {
        let assets = assets(&[]);
        let req = request(Method::GET, "/.assets/nope.js");
        let response = serve_asset(&req, "/nope.js", "", &assets);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response.headers().get(header::CACHE_CONTROL).is_none());
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(body(response).await, b"404 Not Found\n");
    }

    #[tokio::test]
    async fn directory_without_slash_redirects_and_with_slash_is_forbidden() {
        let assets = assets(&[("/img/logo.svg", b"<svg/>")]);
        let response = serve_asset(&request(Method::GET, "/.assets/img"), "/img", "", &assets);
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "img/");
        assert!(body(response).await.is_empty());

        let response = serve_asset(&request(Method::GET, "/.assets/img/"), "/img/", "", &assets);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(body(response).await, b"403 Forbidden\n");
    }

    #[tokio::test]
    async fn non_get_is_405() {
        let assets = assets(&[("/a.js", b"x")]);
        let response = serve_asset(&request(Method::POST, "/.assets/a.js"), "/a.js", "", &assets);
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get(header::ALLOW).unwrap(), "GET");
    }

    #[tokio::test]
    async fn precompressed_gzip_is_served_verbatim() {
        let assets = assets(&[("/a.js", b"raw"), ("/a.js.gz", b"gz")]);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/.assets/a.js")
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(())
            .unwrap();
        let response = serve_asset(&req, "/a.js", "", &assets);
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
        assert_eq!(response.headers().get(header::VARY).unwrap(), "gzip");
        assert!(response.headers().get(header::CONTENT_LENGTH).is_none());
        assert_eq!(body(response).await, b"gz");
    }

    #[tokio::test]
    async fn extension_host_frame_sets_csp() {
        let assets = assets(&[("/extensionHostFrame.html", b"<html>")]);
        let response = serve_asset(
            &request(Method::GET, "/.assets/extensionHostFrame.html"),
            "/extensionHostFrame.html",
            "https://example.com",
            &assets,
        );
        assert_eq!(
            response.headers().get("content-security-policy").unwrap(),
            "frame-ancestors https://example.com"
        );
        assert_eq!(
            response.headers().get(header::X_FRAME_OPTIONS).unwrap(),
            "allow-from https://example.com"
        );
    }

    #[tokio::test]
    async fn home_redirects_to_search_and_head_is_empty() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/?q=x&y=2"),
            "/",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/search?q=x&y=2"
        );

        let response = serve_ui(&request(Method::HEAD, "/"), "/", &env, &config, &deps);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body(response).await.is_empty());
    }

    #[tokio::test]
    async fn static_pages_render_the_shell_with_titles_and_noindex() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/insights/x"),
            "/insights/x",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::OK);
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(
            html.contains("<title>Insights - Sourcegraph</title>"),
            "{html}"
        );
        assert!(html.contains("href=\"/.assets/main.def.css\""));
        assert!(html.contains("src=\"/.assets/main.abc.js\" type=\"module\""));

        let response = serve_ui(
            &request(Method::GET, "/settings/x"),
            "/settings/x",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.headers().get("x-robots-tag").unwrap(), "noindex");
    }

    #[tokio::test]
    async fn search_title_uses_the_limited_query() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/search?q=abcdefghijklmnopqrstuvwxyz0123456789"),
            "/search",
            &env,
            &config,
            &deps,
        );
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(
            html.contains("<title>abcdefghijklmnopqrstuvwx… - Sourcegraph</title>"),
            "{html}"
        );
    }

    #[tokio::test]
    async fn unknown_route_is_a_404_error_shell() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/nope@@x"),
            "/nope@@x",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(
            html.contains("<title>404 Not Found - Sourcegraph</title>"),
            "{html}"
        );
    }

    #[tokio::test]
    async fn method_mismatch_is_an_empty_405() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::POST, "/search"),
            "/search",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(body(response).await.is_empty());
    }

    #[tokio::test]
    async fn strict_slash_redirects_exact_routes() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/sign-up/"),
            "/sign-up/",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/sign-up");
    }

    #[tokio::test]
    async fn help_redirects_to_the_dev_docs_server() {
        let (env, config, deps, _dir) = ui_fixture(&[]);
        let response = serve_ui(
            &request(Method::GET, "/help/foo/bar"),
            "/help/foo/bar",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "http://localhost:3000/foo/bar"
        );
    }

    #[tokio::test]
    async fn help_release_version_requires_three_components() {
        let (mut env, config, deps, _dir) = ui_fixture(&[]);
        env.dotcom = false;

        env.build_version = "4.5.1".to_owned();
        let response = serve_ui(
            &request(Method::GET, "/help/search"),
            "/help/search",
            &env,
            &config,
            &deps,
        );
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "https://sourcegraph.com/docs/v/4.5/search"
        );

        // `coreos/go-semver` rejects two-component versions.
        env.build_version = "4.5".to_owned();
        let response = serve_ui(
            &request(Method::GET, "/help/search"),
            "/help/search",
            &env,
            &config,
            &deps,
        );
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "https://sourcegraph.com/docs/search"
        );
    }

    #[tokio::test]
    async fn dotcom_strict_slash_applies_to_regex_and_exact_routes() {
        let (mut env, config, deps, _dir) = ui_fixture(&[]);
        env.dotcom = true;

        let response = serve_ui(
            &request(Method::GET, "/about/?x=1"),
            "/about/",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/about?x=1");

        for (path, target) in [
            ("/help/terms/", "/help/terms"),
            ("/cncf/", "/cncf"),
            ("/login/", "/login"),
            ("/careers/", "/careers"),
        ] {
            let response = serve_ui(
                &request(Method::GET, path),
                path,
                &env,
                &config,
                &deps,
            );
            assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{path}");
            assert_eq!(
                response.headers().get(header::LOCATION).unwrap(),
                target,
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn without_assets_the_ui_route_stays_404() {
        let response = serve_ui(
            &request(Method::GET, "/search"),
            "/search",
            &AppEnv::default(),
            &AppConfig::default(),
            &AppDeps::default(),
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn repo_route_matcher_matches_the_baseline_pattern() {
        assert!(repo_route_like("/github.com/foo/bar"));
        assert!(repo_route_like("/foo"));
        assert!(repo_route_like("/foo@master"));
        assert!(!repo_route_like("/nope@@x"));
        assert!(!repo_route_like("/x/-/y"));
    }
}
