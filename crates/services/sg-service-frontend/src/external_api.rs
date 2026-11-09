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
//! - `/.api/registry*` (GET) -> non-dotcom `no local extension registry
//!   exists` 404 (`nosniff`);
//! - `/.api/files/batch-changes/{spec}/{file}` (GET/HEAD) and
//!   `.../{spec}` (POST) -> enterprise `batches file ... handler` 404;
//! - `/.api/insights/export/{id}` (GET), `/.api/search/export/{id}.jsonl`
//!   and `{id}.log` (GET), `/.api/completions/{stream,code}` (POST) ->
//!   their enterprise default 404;
//! - `/.api/src-cli/versions/{rest:.*}` (GET/POST) -> the release cache is
//!   disabled with the default config, so `http.NotFound`;
//! - `/.api/src-cli/{rest:.*}` (GET) -> `version` JSON, the six GitHub
//!   download filenames (302), else `http.NotFound`;
//! - `/.api/client-config` and `/.api/modelconfig/supported-models.json`
//!   (GET) -> `401 Unauthorized` without the auth/session middleware;
//! - anything else -> gorilla's default `http.NotFoundHandler`
//!   (`404 page not found\n`, `nosniff`).
//!
//! gorilla enables `StrictSlash(true)` on the subrouter, but its PathPrefix
//! routes (`/scim/v2`, `/registry`) are unaffected; matched template routes
//! redirect a trailing slash away exactly like `http.Redirect`.

use http::{Method, Request, Response, StatusCode};
use tonic::body::BoxBody;

use sg_config::LiveConfig;

use crate::internal_api::{
    decode_path, empty_response, enterprise_not_found, escape_path, hex_escape_non_ascii,
    http_redirect, located, strict_slash_redirect, text_response,
};
use crate::mux;

const REGISTRY_PREFIX: &str = "/.api/registry";
const SCIM_PREFIX: &str = "/.api/scim/v2";
const BATCH_CHANGES_PREFIX: &str = "/.api/files/batch-changes/";
const LSIF_UPLOAD_PATH: &str = "/.api/lsif/upload";
const SCIP_UPLOAD_PATH: &str = "/.api/scip/upload";
const COMPUTE_STREAM_PATH: &str = "/.api/compute/stream";
const INSIGHTS_EXPORT_PREFIX: &str = "/.api/insights/export/";
const SEARCH_EXPORT_PREFIX: &str = "/.api/search/export/";
const CHAT_COMPLETIONS_PATH: &str = "/.api/completions/stream";
const CODE_COMPLETIONS_PATH: &str = "/.api/completions/code";
const SRC_CLI_VERSIONS_PREFIX: &str = "/.api/src-cli/versions/";
const SRC_CLI_PREFIX: &str = "/.api/src-cli/";
const CLIENT_CONFIG_PATH: &str = "/.api/client-config";
const MODEL_CONFIG_PATH: &str = "/.api/modelconfig/supported-models.json";

/// `srccli.MinimumVersion` (`internal/src-cli/consts.go:9`). The remote
/// version cache is not ported, so `src_cli_version_handler.Version()`
/// always takes the air-gapped fallback path.
const SRC_CLI_MINIMUM_VERSION: &str = "5.5.0";
/// `srcCliDownloadsURL` (`src_cli.go:27`).
const SRC_CLI_DOWNLOADS_URL: &str = "https://github.com/sourcegraph/src-cli/releases/download";
/// `allowedFilenames` (`src_cli.go:32-39`).
const SRC_CLI_FILENAMES: [&str; 6] = [
    "src_darwin_amd64",
    "src_darwin_arm64",
    "src_linux_amd64",
    "src_linux_arm64",
    "src_windows_amd64.exe",
    "src_windows_arm64.exe",
];

/// `lsifDeprecationHandler` (`httpapi.go:459-462`).
const LSIF_DEPRECATION_MESSAGE: &str = "Sourcegraph v4.5+ no longer accepts LSIF uploads. The Sourcegraph CLI v4.4.2+ will translate LSIF to SCIP prior to uploading. Please check the version of the CLI utility used to upload this artifact.";

/// The route a request resolved to, before the method is checked.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Registry,
    Scim,
    BatchesFile { spec: String, file: String },
    BatchesFileUpload { spec: String },
    LsifUpload,
    ScipUpload,
    ComputeStream,
    InsightsExport { id: String },
    SearchExportJsonl { id: String },
    SearchExportLog { id: String },
    ChatCompletionsStream,
    CodeCompletions,
    SrcCliVersions,
    SrcCli { rest: String },
    ClientConfig,
    ModelConfig,
}

impl Route {
    fn allows(&self, method: &Method) -> bool {
        match self {
            Route::Scim => matches!(
                *method,
                Method::GET | Method::POST | Method::PUT | Method::PATCH | Method::DELETE
            ),
            Route::Registry | Route::InsightsExport { .. } => method == Method::GET,
            Route::BatchesFile { .. } => method == Method::GET || method == Method::HEAD,
            Route::BatchesFileUpload { .. } => method == Method::POST,
            Route::SearchExportJsonl { .. } | Route::SearchExportLog { .. } => {
                method == Method::GET
            }
            Route::LsifUpload => method == Method::POST,
            Route::ScipUpload => method == Method::POST || method == Method::HEAD,
            Route::ComputeStream => method == Method::GET || method == Method::POST,
            Route::ChatCompletionsStream | Route::CodeCompletions => method == Method::POST,
            Route::SrcCliVersions => method == Method::GET || method == Method::POST,
            Route::SrcCli { .. } | Route::ClientConfig | Route::ModelConfig => {
                method == Method::GET
            }
        }
    }

    /// `PathPrefix` routes ignore `StrictSlash`; every other registered
    /// template takes the trailing-slash redirect.
    fn is_prefix(&self) -> bool {
        matches!(self, Route::Registry | Route::Scim)
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

    let route = match match_route(&decoded) {
        Some(route) => route,
        None => match decoded.strip_suffix('/').map(match_route) {
            Some(Some(route)) => route,
            _ => return page_not_found(),
        },
    };
    if !route.allows(req.method()) {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    // gorilla appends `[/]?` to every non-prefix path regexp and redirects
    // when the request's trailing slash differs from the template's. All the
    // registered templates end without a slash.
    if decoded.ends_with('/') && !route.is_prefix() {
        return strict_slash_redirect(
            req.method(),
            &decoded[..decoded.len() - 1],
            req.uri().query(),
        );
    }

    match route {
        Route::Registry => registry_response(),
        Route::Scim => enterprise_not_found("SCIM handler"),
        Route::BatchesFile { .. } if req.method() == Method::HEAD => {
            enterprise_not_found("batches file exists handler")
        }
        Route::BatchesFile { .. } => enterprise_not_found("batches file get handler"),
        Route::BatchesFileUpload { .. } => enterprise_not_found("batches file upload handler"),
        Route::LsifUpload => text_response(
            StatusCode::NOT_IMPLEMENTED,
            "text/plain; charset=utf-8",
            LSIF_DEPRECATION_MESSAGE,
            false,
        ),
        Route::ScipUpload if req.method() == Method::HEAD => empty_response(StatusCode::OK),
        Route::ScipUpload => enterprise_not_found("code intel upload"),
        Route::ComputeStream => enterprise_not_found("compute streaming endpoint"),
        Route::InsightsExport { .. } => enterprise_not_found("code insights data export handler"),
        Route::SearchExportJsonl { .. } => enterprise_not_found("search jobs data export handler"),
        Route::SearchExportLog { .. } => enterprise_not_found("search jobs logs handler"),
        Route::ChatCompletionsStream => enterprise_not_found("chat completions streaming endpoint"),
        Route::CodeCompletions => enterprise_not_found("code completions streaming endpoint"),
        Route::SrcCliVersions => page_not_found(),
        Route::SrcCli { rest } => src_cli_response(req.method(), &rest),
        Route::ClientConfig | Route::ModelConfig => unauthorized_response(),
    }
}

/// Registered order (`NewHandler`): registry and SCIM prefixes, batch-changes
/// templates, the exact upload/compute paths, then insights/search exports
/// and completions.
fn match_route(path: &str) -> Option<Route> {
    if path.starts_with(REGISTRY_PREFIX) {
        return Some(Route::Registry);
    }
    if path.starts_with(SCIM_PREFIX) {
        return Some(Route::Scim);
    }
    if let Some(rest) = path.strip_prefix(BATCH_CHANGES_PREFIX) {
        if let Some((spec, file)) = batch_changes_file(rest) {
            return Some(Route::BatchesFile { spec, file });
        }
        if let Some(spec) = single_segment(rest) {
            return Some(Route::BatchesFileUpload {
                spec: spec.to_owned(),
            });
        }
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
    if path.strip_prefix(SRC_CLI_VERSIONS_PREFIX).is_some() {
        return Some(Route::SrcCliVersions);
    }
    if let Some(rest) = path.strip_prefix(SRC_CLI_PREFIX) {
        return Some(Route::SrcCli {
            rest: rest.to_owned(),
        });
    }
    if let Some(rest) = path.strip_prefix(INSIGHTS_EXPORT_PREFIX) {
        if let Some(id) = single_segment(rest) {
            return Some(Route::InsightsExport { id: id.to_owned() });
        }
    }
    if let Some(rest) = path.strip_prefix(SEARCH_EXPORT_PREFIX) {
        if let Some(id) = with_suffix(rest, ".jsonl") {
            return Some(Route::SearchExportJsonl { id: id.to_owned() });
        }
        if let Some(id) = with_suffix(rest, ".log") {
            return Some(Route::SearchExportLog { id: id.to_owned() });
        }
    }
    if path == CHAT_COMPLETIONS_PATH {
        return Some(Route::ChatCompletionsStream);
    }
    if path == CODE_COMPLETIONS_PATH {
        return Some(Route::CodeCompletions);
    }
    if path == CLIENT_CONFIG_PATH {
        return Some(Route::ClientConfig);
    }
    if path == MODEL_CONFIG_PATH {
        return Some(Route::ModelConfig);
    }
    None
}

/// gorilla's default variable pattern (`[^/]+`): one non-empty segment.
fn single_segment(rest: &str) -> Option<&str> {
    if !rest.is_empty() && !rest.contains('/') {
        Some(rest)
    } else {
        None
    }
}

/// `{spec}/{file}`: exactly two non-empty segments.
fn batch_changes_file(rest: &str) -> Option<(String, String)> {
    let (spec, file) = rest.split_once('/')?;
    if single_segment(spec).is_some() && single_segment(file).is_some() {
        Some((spec.to_owned(), file.to_owned()))
    } else {
        None
    }
}

/// `{id}<suffix>`: `id` is a non-empty single segment.
fn with_suffix<'a>(rest: &'a str, suffix: &str) -> Option<&'a str> {
    let id = rest.strip_suffix(suffix)?;
    single_segment(id)
}

/// `registry.HandleRegistry`'s non-dotcom branch. Dotcom mode's frozen
/// extension registry is not ported; the OSS baseline answer is used in both
/// modes (documented divergence).
fn registry_response() -> Response<BoxBody> {
    text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "no local extension registry exists\n",
        true,
    )
}

/// `srcCliVersionHandler.ServeHTTP` (`src_cli.go:68-82`). The remote
/// `sourcegraph.com` version cache is not ported: `Version()` always falls
/// back to `srccli.MinimumVersion`, the same answer as an air-gapped
/// baseline.
fn src_cli_response(method: &Method, rest: &str) -> Response<BoxBody> {
    if rest == "version" {
        let body = format!("{{\"version\":\"{SRC_CLI_MINIMUM_VERSION}\"}}\n");
        return text_response(
            StatusCode::OK,
            "application/json; charset=utf-8",
            &body,
            false,
        );
    }
    if SRC_CLI_FILENAMES.contains(&rest) {
        let location = format!("{SRC_CLI_DOWNLOADS_URL}/{SRC_CLI_MINIMUM_VERSION}/{rest}");
        return http_redirect(method, StatusCode::FOUND, location);
    }
    page_not_found()
}

/// `clientconfig.GetClientConfigHandler` / `modelconfig.GetSupportedModelsHandler`
/// without the auth/session middleware: `actor.FromContext` returns an
/// unauthenticated actor, so both answer `http.Error(..., 401)`.
fn unauthorized_response() -> Response<BoxBody> {
    text_response(
        StatusCode::UNAUTHORIZED,
        "text/plain; charset=utf-8",
        "Unauthorized\n",
        true,
    )
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
    use http::header;
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
    async fn template_routes_and_registry() {
        let live = live();
        let registry = api_response(&request(Method::GET, "/.api/registry/extensions"), &live);
        assert_eq!(registry.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            registry.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            registry
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
        assert_eq!(
            api_response(
                &request(Method::HEAD, "/.api/files/batch-changes/a/b"),
                &live
            )
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            api_response(&request(Method::GET, "/.api/files/batch-changes/a"), &live).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            api_response(&request(Method::POST, "/.api/search/export/1.jsonl"), &live).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            api_response(&request(Method::GET, "/.api/search/export/.jsonl"), &live).status(),
            StatusCode::NOT_FOUND
        );
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

    #[tokio::test]
    async fn src_cli_and_config_routes() {
        let live = live();
        let version = api_response(&request(Method::GET, "/.api/src-cli/version"), &live);
        assert_eq!(version.status(), StatusCode::OK);
        assert_eq!(
            version.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json; charset=utf-8"
        );
        let body = version.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"{\"version\":\"5.5.0\"}\n");

        let download = api_response(
            &request(Method::GET, "/.api/src-cli/src_linux_amd64"),
            &live,
        );
        assert_eq!(download.status(), StatusCode::FOUND);
        assert_eq!(
            download.headers().get(header::LOCATION).unwrap(),
            "https://github.com/sourcegraph/src-cli/releases/download/5.5.0/src_linux_amd64"
        );

        // Template routes redirect a trailing slash even when `rest` is empty;
        // the PathPrefix routes never do.
        assert_eq!(
            api_response(&request(Method::GET, "/.api/src-cli/"), &live).status(),
            StatusCode::MOVED_PERMANENTLY
        );
        assert_eq!(
            api_response(&request(Method::GET, "/.api/registry/"), &live).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            api_response(&request(Method::GET, "/.api/src-cli/versions/"), &live)
                .headers()
                .get(header::LOCATION)
                .unwrap(),
            "/.api/src-cli/versions"
        );

        let unauthorized = api_response(&request(Method::GET, "/.api/client-config"), &live);
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthorized
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }
}
