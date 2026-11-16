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
use crate::graphql_resolvers::RepoLookup;
use crate::repository_store::RepoRevResolution;

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

/// Async `RouteName::Ui` dispatch (M9.49d). It is identical to [`serve_ui`]
/// except that repository pages go through `newCommon`'s DB/gitserver checks
/// (`serveRepoOrBlob`/`serveRepoLanding`) instead of the offline 404 fallback.
pub async fn serve_ui_async<B>(
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
        if let UiAction::RepoPage { repo, rev, page } = &candidate.action {
            return serve_repo_page(
                req,
                env,
                config,
                assets,
                deps,
                repo,
                rev.as_deref(),
                page,
            )
            .await;
        }
        return candidate.serve(req, env, config, assets, deps);
    }
    if method_mismatch {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    ui_not_found(env, config, assets)
}

/// The repository route's page kind, mirroring the gorilla route table
/// (`router.go:249-307`).
#[derive(Clone, Debug, PartialEq, Eq)]
enum RepoPageKind {
    /// `serveRepoOrBlob(routeRepo)` (`router.go:249`).
    Root,
    /// `serveTree` (`router.go:269`).
    Tree { path: String },
    /// `serveRepoOrBlob(routeBlob)` (`router.go:278`).
    Blob { path: String },
    /// `brandedIndex`/`brandedNoIndex` subroutes: an unconditional SPA shell
    /// (`router.go:290-307`), no repo/rev lookup.
    Subroute { title: &'static str, indexed: bool },
    /// dotcom-only `serveRepoLanding` (`router.go:320`): 301 to
    /// `/{repo}@{commit}`.
    Landing,
}

/// `serveRepoOrBlob`/`serveTree`/`serveRepoLanding`'s shared `newCommon` +
/// per-route logic (`handlers.go:214-292`, `:469-550`).
async fn serve_repo_page<B>(
    req: &Request<B>,
    env: &AppEnv,
    config: &AppConfig,
    assets: &dyn sg_ui::AssetsProvider,
    deps: &AppDeps,
    repo: &str,
    rev: Option<&str>,
    page: &RepoPageKind,
) -> Response<BoxBody> {
    // Without production wiring the async path keeps the historical 404, so
    // the offline router/UI differentials stay byte-identical.
    let Some(wiring) = deps.repo_pages.as_deref() else {
        return ui_not_found(env, config, assets);
    };

    // `newCommon` loads the manifest first: a missing/broken manifest is a
    // 500 even for repository routes (handlers.go:153).
    let manifest = match assets.load_web_build_manifest() {
        Ok(manifest) => manifest,
        Err(error) => return manifest_error_response(env, config, &error),
    };

    // `brandedIndex`/`brandedNoIndex` (`router.go:290-307`) only call
    // `serveBasicPage`: unlike root/tree/blob they run no `newCommon` lookup,
    // so their shell is unconditional (even for an unknown repo) and must not
    // consult Postgres or gitserver.
    if matches!(page, RepoPageKind::Subroute { .. }) {
        let indexed = rev.is_none()
            && !matches!(page, RepoPageKind::Subroute { indexed: false, .. });
        let title = repo_page_title(page, repo, config);
        let input = shell_input(env, config, &manifest, title, indexed);
        return shell_response(&input, StatusCode::OK);
    }

    // `handlerutil.GetRepo` → `repoStore.GetByName` with the authz predicate.
    // The actor's UID is filled per request by the middleware; 0 is anonymous.
    let actor_uid = deps.more.actor.uid().unwrap_or(0);
    let lookup = match wiring
        .repositories
        .lookup(repo, actor_uid, false)
        .await
    {
        Ok(lookup) => lookup,
        // `errcode.IsUnauthorized(err)` → 401 (handlers.go:285). The PG store
        // maps authz-filtered rows to `NotFound` (Go's repoStore behaves the
        // same), so this branch is only reachable through an injected error.
        Err(error) if is_unauthorized_error(&error) => {
            return ui_error_shell(StatusCode::UNAUTHORIZED, env, config, assets);
        }
        Err(error) => {
            let shown = env.insecure_dev.then_some(error.as_str());
            return error_page_response(StatusCode::INTERNAL_SERVER_ERROR, shown);
        }
    };
    match lookup {
        RepoLookup::Found(_) => {}
        // errcode.IsNotFound / errcode.IsBlocked → 404 (`handlers.go:279-283`).
        RepoLookup::NotFound | RepoLookup::Blocked { .. } => {
            return ui_not_found(env, config, assets);
        }
        // `URLMovedError` is not modelled by `PgRepositoryStore` (it returns
        // the canonical row); a see-other redirect is also outside this slice.
        RepoLookup::Redirect { .. } => return ui_not_found(env, config, assets),
        RepoLookup::Error(error) => {
            if is_unauthorized_error(&error) {
                return ui_error_shell(StatusCode::UNAUTHORIZED, env, config, assets);
            }
            let shown = env.insecure_dev.then_some(error.as_str());
            return error_page_response(StatusCode::INTERNAL_SERVER_ERROR, shown);
        }
    }

    let resolved = match &wiring.resolver {
        Some(resolver) => Some(resolver.resolve(repo, rev).await),
        // No gitserver configured: do not misclassify an existing repository
        // as 404 (the baseline's clone-in-progress branch also replies 200).
        None => None,
    };
    match &resolved {
        Some(RepoRevResolution::RevisionNotFound) if rev.is_some() => {
            // `errors.HasType[*gitdomain.RevisionNotFoundError]` → 404.
            return ui_not_found(env, config, assets);
        }
        // `isRepoEmptyError`: no explicit rev + RevisionNotFound → 200.
        Some(RepoRevResolution::RevisionNotFound) => {}
        // `gitdomain.IsCloneInProgress` → 200 (`handlers.go:275`).
        Some(RepoRevResolution::RepoNotExist {
            clone_in_progress: true,
        }) => {}
        Some(RepoRevResolution::RepoNotExist { .. }) => {
            return ui_not_found(env, config, assets);
        }
        Some(RepoRevResolution::Error(error)) => {
            let shown = env.insecure_dev.then_some(error.as_str());
            return error_page_response(StatusCode::INTERNAL_SERVER_ERROR, shown);
        }
        Some(RepoRevResolution::Resolved { .. }) | None => {}
    }

    if let RepoPageKind::Landing = page {
        // `serveRepoLanding`: 301 `/<repo>@<resolved commit>`; a failed
        // `GetRepoAndRev` is 404 for HTTP errors and 500 otherwise.
        return match resolved {
            Some(RepoRevResolution::Resolved { commit }) => app_handlers::redirect(
                req,
                &format!("/{repo}@{commit}"),
                StatusCode::MOVED_PERMANENTLY.as_u16(),
            ),
            Some(RepoRevResolution::RevisionNotFound)
            | Some(RepoRevResolution::RepoNotExist { .. }) => {
                ui_not_found(env, config, assets)
            }
            _ => ui_error_shell(
                StatusCode::INTERNAL_SERVER_ERROR,
                env,
                config,
                assets,
            ),
        };
    }

    // `serveRepoOrBlob`'s legacy `?q=` redirect (handlers.go:529-550): only
    // the root and blob routes, and only when no `sq` parameter is present.
    if matches!(page, RepoPageKind::Root | RepoPageKind::Blob { .. }) {
        if let Some(location) = legacy_search_redirect(req, repo) {
            return app_handlers::redirect(req, &location, 308);
        }
    }

    let indexed = rev.is_none()
        && !matches!(page, RepoPageKind::Subroute { indexed: false, .. });
    let title = repo_page_title(page, repo, config);
    let input = shell_input(env, config, &manifest, title, indexed);
    shell_response(&input, StatusCode::OK)
}

/// `repoShortName` (`handlers.go:92-98`): trim the first path component when
/// the name has at least two.
fn repo_short_name(name: &str) -> String {
    let mut split = name.splitn(2, '/');
    let _first = split.next().unwrap_or("");
    match split.next() {
        Some(rest) => rest.to_owned(),
        None => name.to_owned(),
    }
}

/// `path.Base` for page titles.
fn go_path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_owned();
    }
    trimmed
        .rfind('/')
        .map(|index| trimmed[index + 1..].to_owned())
        .unwrap_or_else(|| trimmed.to_owned())
}

fn repo_page_title(page: &RepoPageKind, repo: &str, config: &AppConfig) -> String {
    let short = repo_short_name(repo);
    match page {
        RepoPageKind::Root => format!("{} - {}", short, config.brand_name),
        RepoPageKind::Tree { path } | RepoPageKind::Blob { path } => format!(
            "{} - {} - {}",
            go_path_base(path),
            short,
            config.brand_name
        ),
        RepoPageKind::Subroute { title, .. } => {
            format!("{} - {}", title, config.brand_name)
        }
        RepoPageKind::Landing => format!("{} - {}", short, config.brand_name),
    }
}

/// `errcode.IsUnauthorized` reduced to the injected error text. The production
/// PG store never emits it (authz-filtered rows are `NotFound`, exactly like
/// Go's `repoStore`), so the 401 branch is covered by unit tests only.
fn is_unauthorized_error(error: &str) -> bool {
    error.to_ascii_lowercase().contains("unauthorized")
}

/// `serveError(w, r, ..., statusCode)` for a repository route: the SPA error
/// shell when the manifest loads, `error.html` otherwise.
fn ui_error_shell(
    status: StatusCode,
    env: &AppEnv,
    config: &AppConfig,
    assets: &dyn sg_ui::AssetsProvider,
) -> Response<BoxBody> {
    match assets.load_web_build_manifest() {
        Ok(manifest) => {
            let title = format!(
                "{} {} - {}",
                status.as_u16(),
                status_text(status.as_u16()),
                config.brand_name
            );
            let input = shell_input(env, config, &manifest, title, false);
            shell_response(&input, status)
        }
        Err(_) => error_page_response(status, None),
    }
}

/// `serveRepoOrBlob`'s `?q=` → `/search` 308 (`handlers.go:529-550`).
fn legacy_search_redirect<B>(req: &Request<B>, repo: &str) -> Option<String> {
    let query = req.uri().query()?;
    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let search = pairs
        .iter()
        .find(|(key, _)| key == "q")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    if search.is_empty() || pairs.iter().any(|(key, _)| key == "sq") {
        return None;
    }

    // `url.Values.Encode`: keys sorted, values in insertion order, `QueryEscape`.
    let mut values: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (key, value) in pairs {
        values.entry(key).or_default().push(value);
    }
    values
        .entry("sq".to_owned())
        .or_default()
        .push(format!("repo:^{}$", regexp_quote_meta(repo)));
    let encoded = values
        .iter()
        .flat_map(|(key, values)| {
            values.iter().map(move |value| {
                format!("{}={}", go_query_escape(key), go_query_escape(value))
            })
        })
        .collect::<Vec<_>>()
        .join("&");
    Some(format!("/search?{encoded}"))
}

/// `regexp.QuoteMeta` (`regexp/regexp.go`).
fn regexp_quote_meta(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(
            ch,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// `url.QueryEscape`: unreserved characters are `A-Za-z0-9-_.~`; a space
/// becomes `+`; everything else is `%XX` (upper-case hex).
fn go_query_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(*byte as char)
            }
            b' ' => escaped.push('+'),
            byte => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
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
    /// A repository page/subroute (`router.go:244-330`). The async dispatcher
    /// resolves the repository and revision against Postgres/gitserver
    /// ([`serve_ui_async`]); the sync `serve_ui` keeps the historical
    /// `ui_not_found` fallback when no [`RepoPagesWiring`] is injected.
    RepoPage {
        repo: String,
        rev: Option<String>,
        page: RepoPageKind,
    },
    /// Routes whose handlers are still out of scope here: `/-/raw` (a file
    /// response, not an SPA shell) and the dotcom-only legacy pages. They keep
    /// the `ui_not_found` fallback.
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
            // The sync entry point cannot query Postgres/gitserver; keep the
            // M9.8iu differential oracle's 404 for repo pages. The live
            // dispatcher calls [`serve_ui_async`], which overrides this arm.
            UiAction::RepoPage { .. } | UiAction::RepoUnavailable => {
                ui_not_found(env, config, assets)
            }
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
        if let Some((repo, rev)) = parse_repo_rev(path) {
            return vec![UiCandidate::get(UiAction::RepoPage {
                repo,
                rev,
                page: RepoPageKind::Root,
            })];
        }
        return vec![UiCandidate::get(UiAction::RepoUnavailable)];
    }
    if let Some((left, rest)) = path.split_once("/-/") {
        if repo_route_like(left) {
            if let (Some((repo, rev)), Some(page)) =
                (parse_repo_rev(left), repo_subroute_page(rest, env))
            {
                return vec![UiCandidate::get(UiAction::RepoPage { repo, rev, page })];
            }
        }
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
        // The session middleware fills `AppDeps.more.actor` per request; the
        // GitHub App middleware is now a real site-admin check, not allow-all.
        Some(server) => server.serve_with_actor(
            req,
            route,
            crate::githubapp_http_api::ActorIdentity::from_actor(deps.more.actor),
        ),
        None => crate::githubapp_http_api::serve_offline(req, route),
    }
}

/// The `/-/` subroutes of the repo route (`router.go:268-320`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RepoSubroute {
    Raw,
    Get,
}

/// `routevar.ToRepoRev`: split `/<repo>[@<rev>]` (the repo route's `Repo` +
/// `Rev` variables; a repo name cannot contain `@`).
fn parse_repo_rev(path: &str) -> Option<(String, Option<String>)> {
    let segment = path.strip_prefix('/')?;
    if segment.is_empty() || !repo_route_like(path) {
        return None;
    }
    match segment.split_once('@') {
        Some((repo, rev)) if !repo.is_empty() && !rev.is_empty() => {
            Some((repo.to_owned(), Some(rev.to_owned())))
        }
        Some(_) => None,
        None => Some((segment.to_owned(), None)),
    }
}

/// The repository `/-/` subroutes that render the SPA shell (`router.go`).
/// `raw` (a file response) and the dotcom-only legacy `def`/`refs`/`land`
/// pages stay out of scope and return `None`.
fn repo_subroute_page(rest: &str, env: &AppEnv) -> Option<RepoPageKind> {
    if rest == "raw" || rest.starts_with("raw") {
        return None;
    }
    if let Some(path) = rest.strip_prefix("tree") {
        return Some(RepoPageKind::Tree {
            path: path.to_owned(),
        });
    }
    if let Some(path) = rest.strip_prefix("blob") {
        return Some(RepoPageKind::Blob {
            path: path.to_owned(),
        });
    }
    // Registration order matters: `/commits` is registered before `/commit`.
    const BRANDED: &[(&str, &str, bool)] = &[
        ("batch-changes", "Batch Changes", true),
        ("settings", "Repository settings", false),
        ("code-graph", "Code graph", false),
        ("commits", "Commits", false),
        ("commit", "Commit", false),
        ("branches", "Branches", false),
        ("tags", "Tags", false),
        ("compare", "Compare", false),
        ("stats", "Stats", false),
        ("own", "Ownership", false),
    ];
    for (prefix, title, indexed) in BRANDED {
        if rest.starts_with(prefix) {
            return Some(RepoPageKind::Subroute {
                title,
                indexed: *indexed,
            });
        }
    }
    if env.dotcom && rest == "info" {
        return Some(RepoPageKind::Landing);
    }
    None
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
        401 => "Unauthorized",
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

    // -----------------------------------------------------------------
    // M9.49d: repository page shell (`serve_ui_async`/`newCommon`).
    // -----------------------------------------------------------------

    use crate::graphql_resolvers::{RepositoryData, RepositoryLookup};
    use crate::repository_store::{RepoPagesWiring, RepoRevResolver};

    fn repo_data() -> RepositoryData {
        RepositoryData {
            id: 1,
            name: "m949d/repo".to_owned(),
            uri: "https://example.com/m949d/repo".to_owned(),
            description: String::new(),
            fork: false,
            archived: false,
            private: false,
            stars: 0,
            external_id: String::new(),
            external_service_type: "other".to_owned(),
            external_service_id: "m949d".to_owned(),
        }
    }

    struct FakeLookup(Result<RepoLookup, String>);

    impl FakeLookup {
        fn found() -> Self {
            Self(Ok(RepoLookup::Found(repo_data())))
        }

        fn result(result: Result<RepoLookup, String>) -> Self {
            Self(result)
        }
    }

    #[async_trait::async_trait]
    impl RepositoryLookup for FakeLookup {
        async fn lookup(
            &self,
            _name_or_uri: &str,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            self.0.clone()
        }

        async fn by_id(
            &self,
            _repo_id: i32,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            self.0.clone()
        }

        async fn by_hashed_name(
            &self,
            _hashed_name: &str,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            self.0.clone()
        }
    }

    struct FakeResolver(RepoRevResolution);

    #[async_trait::async_trait]
    impl RepoRevResolver for FakeResolver {
        async fn resolve(&self, _repo: &str, _rev: Option<&str>) -> RepoRevResolution {
            self.0.clone()
        }
    }

    fn repo_deps(
        lookup: FakeLookup,
        resolver: Option<FakeResolver>,
        files: &[(&str, &[u8])],
    ) -> (AppEnv, AppConfig, AppDeps, TestDir) {
        let (env, config, mut deps, dir) = ui_fixture(files);
        deps.repo_pages = Some(Arc::new(RepoPagesWiring {
            repositories: Arc::new(lookup),
            resolver: resolver
                .map(|resolver| Arc::new(resolver) as Arc<dyn RepoRevResolver>),
        }));
        (env, config, deps, dir)
    }

    fn resolves() -> Option<FakeResolver> {
        Some(FakeResolver(RepoRevResolution::Resolved {
            commit: "abc123".to_owned(),
        }))
    }

    async fn get(path: &str, env: &AppEnv, config: &AppConfig, deps: &AppDeps) -> Response<BoxBody> {
        serve_ui_async(&request(Method::GET, path), path, env, config, deps).await
    }

    #[tokio::test]
    async fn repo_page_renders_the_shell_with_go_titles() {
        let (env, config, deps, _dir) = repo_deps(FakeLookup::found(), resolves(), &[]);

        let response = get("/m949d/repo", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-robots-tag").is_none());
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(html.contains("<title>repo - Sourcegraph</title>"), "{html}");
        assert!(html.contains("<div id=\"root\"></div>"));

        let response = get("/m949d/repo/-/blob/README.md", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(
            html.contains("<title>README.md - repo - Sourcegraph</title>"),
            "{html}"
        );

        let response = get("/m949d/repo/-/tree/src", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(
            html.contains("<title>src - repo - Sourcegraph</title>"),
            "{html}"
        );
    }

    #[tokio::test]
    async fn repo_page_rev_sets_noindex_and_commit_subroute_is_unconditional() {
        let (env, config, deps, _dir) = repo_deps(FakeLookup::found(), resolves(), &[]);

        let response = get("/m949d/repo@main/-/blob/README.md", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("x-robots-tag").unwrap(), "noindex");

        // `/commit` is a `brandedNoIndex` shell: no repo/rev lookup in Go.
        // A `NotFound` lookup proves the subroute never calls `newCommon`.
        let (env, config, deps, _dir) =
            repo_deps(FakeLookup::result(Ok(RepoLookup::NotFound)), resolves(), &[]);
        let response = get("/m949d/repo/-/commit/HEAD", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("x-robots-tag").unwrap(), "noindex");
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(html.contains("<title>Commit - Sourcegraph</title>"), "{html}");
    }

    #[tokio::test]
    async fn repo_page_lookup_errors_map_to_404_and_401() {
        let (env, config, deps, _dir) =
            repo_deps(FakeLookup::result(Ok(RepoLookup::NotFound)), resolves(), &[]);
        let response = get("/m949d/repo", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let html = String::from_utf8(body(response).await).unwrap();
        assert!(html.contains("<title>404 Not Found - Sourcegraph</title>"));

        let (env, config, deps, _dir) = repo_deps(
            FakeLookup::result(Ok(RepoLookup::Blocked {
                name: "m949d/repo".to_owned(),
                reason: "dmca".to_owned(),
            })),
            resolves(),
            &[],
        );
        let response = get("/m949d/repo/-/blob/README.md", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let (env, config, deps, _dir) = repo_deps(
            FakeLookup::result(Err("unauthorized: no access".to_owned())),
            resolves(),
            &[],
        );
        let response = get("/m949d/repo", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn repo_page_rev_resolution_branches() {
        // `RevisionNotFound` with an explicit rev → 404 (`serveError`).
        let (env, config, deps, _dir) = repo_deps(
            FakeLookup::found(),
            Some(FakeResolver(RepoRevResolution::RevisionNotFound)),
            &[],
        );
        let response = get("/m949d/repo@nope/-/blob/README.md", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Empty repository with no rev → 200 (`isRepoEmptyError`).
        let response = get("/m949d/repo", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);

        // Clone in progress → 200 (`handlers.go:275`).
        let (env, config, deps, _dir) = repo_deps(
            FakeLookup::found(),
            Some(FakeResolver(RepoRevResolution::RepoNotExist {
                clone_in_progress: true,
            })),
            &[],
        );
        let response = get("/m949d/repo/-/tree/src", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);

        // Repo missing from gitserver and not cloning → 404.
        let (env, config, deps, _dir) = repo_deps(
            FakeLookup::found(),
            Some(FakeResolver(RepoRevResolution::RepoNotExist {
                clone_in_progress: false,
            })),
            &[],
        );
        let response = get("/m949d/repo", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // No gitserver configured: an existing repo row stays a 200 shell.
        let (env, config, deps, _dir) = repo_deps(FakeLookup::found(), None, &[]);
        let response = get("/m949d/repo@main", &env, &config, &deps).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn repo_page_legacy_q_redirect_and_sync_fallback() {
        let (env, config, deps, _dir) = repo_deps(FakeLookup::found(), resolves(), &[]);
        let response = serve_ui_async(
            &request(Method::GET, "/m949d/repo/-/blob/README.md?q=M949D_MARKER"),
            "/m949d/repo/-/blob/README.md",
            &env,
            &config,
            &deps,
        )
        .await;
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/search?q=M949D_MARKER&sq=repo%3A%5Em949d%2Frepo%24"
        );

        // A present `sq` (even empty) suppresses the legacy redirect.
        let response = serve_ui_async(
            &request(
                Method::GET,
                "/m949d/repo/-/blob/README.md?q=x&sq=",
            ),
            "/m949d/repo/-/blob/README.md",
            &env,
            &config,
            &deps,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // The synchronous entry point keeps the offline 404.
        let response = serve_ui(
            &request(Method::GET, "/m949d/repo"),
            "/m949d/repo",
            &env,
            &config,
            &deps,
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
