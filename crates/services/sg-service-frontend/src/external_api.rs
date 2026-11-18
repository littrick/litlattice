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
//! - `/.api/compute/stream` (GET/POST) -> the ported SSE handler
//!   ([`crate::compute_stream`]); the compute search pipeline is narrowed to
//!   a recent `(repo, path)` example (see that module's ENV_GAP);
//! - `/.api/registry*` (GET) -> non-dotcom `no local extension registry
//!   exists` 404 (`nosniff`);
//! - `/.api/files/batch-changes/{spec}/{file}` (GET/HEAD) and
//!   `.../{spec}` (POST) -> enterprise `batches file ... handler` 404;
//! - `/.api/insights/export/{id}` (GET), `/.api/search/export/{id}.jsonl`
//!   and `{id}.log` (GET), `/.api/completions/{stream,code}` (POST) ->
//!   their enterprise default 404;
//! - `/.api/src-cli/versions/{rest:.*}` (GET/POST) -> the release cache is
//!   disabled with the default config, so `http.NotFound`;
//! - `/.api/src-cli/{rest:.*}` (GET) -> `srcCliVersionHandler`: `version`
//!   JSON (10-minute remote cache, air-gapped `5.5.0` fallback) and the six
//!   GitHub download filenames (302), else `http.NotFound`;
//! - `/.api/client-config` and `/.api/modelconfig/supported-models.json`
//!   (GET) -> `401 Unauthorized` without the auth/session middleware;
//! - `/.api/webhooks/{webhook_uuid}` (POST) -> invalid UUID 400 (`%q`), else
//!   the empty-table 404 `Could not find webhook with provided UUID.`;
//! - `/.api/{github,gitlab,bitbucket-server,bitbucket-cloud}-webhooks` (POST)
//!   -> GitHub: `github_webhook_response` with injected external-service and
//!   dispatch seams (the production default has an empty external-service
//!   table, so 400 `Error validating payload`); the others: the OSS empty
//!   handler's 200 empty response;
//! - `/.api/opencodegraph` (POST) -> the `opencodegraph` feature flag is off,
//!   so `JsonMiddleware`'s error handler answers 500 with an empty body;
//! - `/.api/repos/{Repo}/-/shield` (GET) -> `serveRepoShield`'s
//!   `CountGoImporters` error on self-hosted instances, mapped by the same
//!   `ErrorHandler.Handle` contract to a 500 with an empty body; the route
//!   reproduces `routevar.Repo`'s gorilla template;
//! - `/.api/blame/{Repo}{@rev}/-/stream/{Path:.*}` (GET) ->
//!   `handleStreamBlame`'s SSE stream (`event: hunk`/`event: done`), with
//!   `routevar.RepoRevSuffix`'s optional `@rev`; served by the async
//!   [`api_response_with_deps`], the synchronous [`api_response`] only
//!   classifies the route;
//! - anything else -> gorilla's default `http.NotFoundHandler`
//!   (`404 page not found\n`, `nosniff`).
//!
//! gorilla enables `StrictSlash(true)` on the subrouter, but its PathPrefix
//! routes (`/scim/v2`, `/registry`) are unaffected; matched template routes
//! redirect a trailing slash away exactly like `http.Redirect`.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use http::{header, HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use tonic::body::BoxBody;
use uuid::Uuid;

use sg_config::LiveConfig;

use crate::blame::BlameDeps;
use crate::graphql_api::{self, GraphqlDeps};
use crate::internal_api::{
    decode_path, empty_response, enterprise_not_found, escape_path, hex_escape_non_ascii,
    http_redirect, located, strict_slash_redirect, text_response,
};
use crate::mux;

const REGISTRY_PREFIX: &str = "/.api/registry";
const SCIM_PREFIX: &str = "/.api/scim/v2";
const GRAPHQL_PATH: &str = "/.api/graphql";
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
const WEBHOOKS_PREFIX: &str = "/.api/webhooks/";
const GITHUB_WEBHOOKS_PATH: &str = "/.api/github-webhooks";
const GITLAB_WEBHOOKS_PATH: &str = "/.api/gitlab-webhooks";
const BITBUCKET_SERVER_WEBHOOKS_PATH: &str = "/.api/bitbucket-server-webhooks";
const BITBUCKET_CLOUD_WEBHOOKS_PATH: &str = "/.api/bitbucket-cloud-webhooks";
const OPENCODEGRAPH_PATH: &str = "/.api/opencodegraph";
/// `httpapi.go:172`: `m.Path("/search/stream").Methods("GET")`.
const SEARCH_STREAM_PATH: &str = "/.api/search/stream";
/// `httpapi.go:325`: `llm := m.PathPrefix("/llm/").Subrouter()`.
const LLM_CHAT_COMPLETIONS_PATH: &str = "/.api/llm/chat/completions";
const LLM_MODELS_PATH: &str = "/.api/llm/models";
const LLM_MODELS_PREFIX: &str = "/.api/llm/models/";

/// `routevar.RepoPattern` (`routevar/repo.go:30-35`) after
/// `namedToNonCapturingGroups`: the gorilla `{Repo:...}` template body. A
/// component is one or more of `[^@/-]` or a two-or-more character run
/// without `@`/`/` — i.e. non-empty, no `@`, no `/`, and never exactly `-`.
const REPO_PATTERN: &str = "(?:(?:[^@/-]|(?:[^/@]{2,}))/)*(?:[^@/-]|(?:[^/@]{2,}))";

/// `srccli.MinimumVersion` (`internal/src-cli/consts.go:9`). This is the
/// air-gapped fallback of [`SrcCliVersionHandler`] when the 10-minute cache
/// is cold and the remote fetch fails.
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
pub(crate) enum Route {
    Registry,
    Scim,
    BatchesFile {
        spec: String,
        file: String,
    },
    BatchesFileUpload {
        spec: String,
    },
    LsifUpload,
    ScipUpload,
    ComputeStream,
    InsightsExport {
        id: String,
    },
    SearchExportJsonl {
        id: String,
    },
    SearchExportLog {
        id: String,
    },
    ChatCompletionsStream,
    CodeCompletions,
    SrcCliVersions,
    SrcCli {
        rest: String,
    },
    ClientConfig,
    ModelConfig,
    WebhookUuid {
        uuid: String,
    },
    GitHubWebhooks,
    GitLabWebhooks,
    BitbucketServerWebhooks,
    BitbucketCloudWebhooks,
    OpenCodeGraph,
    Graphql,
    RepoShield {
        repo: String,
    },
    RepoRefresh {
        repo: String,
    },
    LlmChatCompletions,
    LlmModels,
    LlmModel {
        id: String,
    },
    SearchStream,
    BlameStream {
        repo: String,
        /// The `Rev` route variable without its `@` (`None` when the URL had
        /// no revspec).
        rev: Option<String>,
        /// The raw `{Path:.*}` variable (still with its leading slash).
        path: String,
    },
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
            Route::RepoShield { .. } | Route::BlameStream { .. } | Route::SearchStream => {
                method == Method::GET
            }
            Route::RepoRefresh { .. } => method == Method::POST,
            Route::LlmChatCompletions => method == Method::POST,
            Route::LlmModels | Route::LlmModel { .. } => method == Method::GET,
            Route::Graphql => method == Method::POST,
            Route::WebhookUuid { .. }
            | Route::GitHubWebhooks
            | Route::GitLabWebhooks
            | Route::BitbucketServerWebhooks
            | Route::BitbucketCloudWebhooks
            | Route::OpenCodeGraph => method == Method::POST,
        }
    }

    /// `PathPrefix` routes ignore `StrictSlash`; every other registered
    /// template takes the trailing-slash redirect.
    fn is_prefix(&self) -> bool {
        matches!(self, Route::Registry | Route::Scim)
    }

    /// Stable route name for the cross-check with the Go gorilla oracle.
    #[doc(hidden)]
    pub fn diff_name(&self) -> &'static str {
        match self {
            Route::RepoRefresh { .. } => "repo-refresh",
            Route::LlmChatCompletions => "llm-chat-completions",
            Route::LlmModels => "llm-models",
            Route::LlmModel { .. } => "llm-model",
            other => other.variant_name(),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Route::Registry => "registry",
            Route::Scim => "scim",
            Route::BatchesFile { .. } => "batches-file",
            Route::BatchesFileUpload { .. } => "batches-file-upload",
            Route::LsifUpload => "lsif-upload",
            Route::ScipUpload => "scip-upload",
            Route::ComputeStream => "compute-stream",
            Route::InsightsExport { .. } => "insights-export",
            Route::SearchExportJsonl { .. } => "search-export-jsonl",
            Route::SearchExportLog { .. } => "search-export-log",
            Route::ChatCompletionsStream => "chat-completions-stream",
            Route::CodeCompletions => "code-completions",
            Route::SrcCliVersions => "src-cli-versions",
            Route::SrcCli { .. } => "src-cli",
            Route::ClientConfig => "client-config",
            Route::ModelConfig => "model-config",
            Route::WebhookUuid { .. } => "webhook-uuid",
            Route::GitHubWebhooks => "github-webhooks",
            Route::GitLabWebhooks => "gitlab-webhooks",
            Route::BitbucketServerWebhooks => "bitbucket-server-webhooks",
            Route::BitbucketCloudWebhooks => "bitbucket-cloud-webhooks",
            Route::OpenCodeGraph => "opencodegraph",
            Route::Graphql => "graphql",
            Route::RepoShield { .. } => "repo-shield",
            Route::RepoRefresh { .. } => "repo-refresh",
            Route::LlmChatCompletions => "llm-chat-completions",
            Route::LlmModels => "llm-models",
            Route::LlmModel { .. } => "llm-model",
            Route::SearchStream => "search-stream",
            Route::BlameStream { .. } => "blame-stream",
        }
    }
}

/// The subrouter's `ServeHTTP` + the registered handlers.
pub fn api_response<B>(req: &Request<B>, _live: &LiveConfig) -> Response<BoxBody> {
    match route_request(req) {
        Err(response) => response,
        Ok(route) => route_response(req, route),
    }
}

/// The async variant used by the external listener: `/.api/blame/...` needs
/// the database and gitserver (via [`BlameDeps`]) and streams its response,
/// the LLM routes need their model-config/forwarding seams, and
/// `POST /.api/repos/{Repo}/-/refresh` needs the repo updater. Every other
/// route is the same synchronous dispatch as [`api_response`].
#[allow(clippy::too_many_arguments)] // One argument per async subsystem seam.
pub async fn api_response_with_deps<B>(
    req: &mut Request<B>,
    live: &LiveConfig,
    deps: Arc<dyn BlameDeps>,
    search_deps: Arc<dyn crate::search_stream::SearchDeps>,
    graphql_deps: Arc<dyn GraphqlDeps>,
    repo_refresh_deps: Arc<dyn crate::repo_refresh::RepoRefreshDeps>,
    llm_deps: Arc<crate::llm_api::LlmDeps>,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = bytes::Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    match route_request(&*req) {
        Err(response) => response,
        Ok(Route::BlameStream { repo, rev, path }) => {
            crate::blame::blame_response(req, &repo, rev.as_deref(), &path, deps).await
        }
        Ok(Route::ComputeStream) => {
            crate::compute_stream::compute_stream_response(req, search_deps).await
        }
        Ok(Route::SearchStream) => crate::search_stream::search_response(req, search_deps).await,
        Ok(Route::Graphql) => graphql_api::graphql_response(req, live, false, graphql_deps).await,
        Ok(Route::RepoRefresh { repo }) => {
            crate::repo_refresh::repo_refresh_response(&*req, &repo, repo_refresh_deps.as_ref())
                .await
        }
        Ok(Route::LlmChatCompletions) => {
            crate::llm_api::chat_completions_response(req, &llm_deps).await
        }
        Ok(Route::LlmModels) => crate::llm_api::models_response(llm_deps.config.as_ref()),
        Ok(Route::LlmModel { id }) => {
            crate::llm_api::model_response(llm_deps.config.as_ref(), &id)
        }
        Ok(route) => route_response(req, route),
    }
}

/// The differential seam for `run_http_routes_inventory_diff.sh`: reduce
/// gorilla's observable answer to one line (`matched:<name>`,
/// `status:<code>`, `redirect:<location>` or `not_found`). Only the routes
/// added by M9.16c need names; the rest still classify deterministically.
#[doc(hidden)]
pub fn route_classification<B>(req: &Request<B>) -> String {
    match route_request(req) {
        Ok(route) => format!("matched:{}", route.diff_name()),
        Err(response) => match response.status() {
            StatusCode::MOVED_PERMANENTLY | StatusCode::PERMANENT_REDIRECT => response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .map(|location| format!("redirect:{location}"))
                .unwrap_or_else(|| format!("status:{}", response.status().as_u16())),
            StatusCode::METHOD_NOT_ALLOWED => "status:405".to_owned(),
            StatusCode::NOT_FOUND => "not_found".to_owned(),
            status => format!("status:{}", status.as_u16()),
        },
    }
}

/// The gorilla matching pipeline (clean redirect, route, method, trailing
/// slash), shared by both entry points.
#[allow(clippy::result_large_err)] // The error is the short-circuit response.
fn route_request<B>(req: &Request<B>) -> Result<Route, Response<BoxBody>> {
    let raw_path = req.uri().path();
    let decoded = decode_path(if raw_path.is_empty() { "/" } else { raw_path });

    // gorilla `Router.ServeHTTP`'s `path.Clean` redirect: `Location` only.
    let cleaned = mux::clean_path(&decoded);
    if cleaned != decoded {
        let location = match req.uri().query() {
            Some(query) => format!("{}?{query}", escape_path(&cleaned)),
            None => escape_path(&cleaned),
        };
        return Err(located(
            StatusCode::MOVED_PERMANENTLY,
            hex_escape_non_ascii(&location),
            None,
        ));
    }

    let route = match match_route(&decoded) {
        Some(route) => route,
        None => match decoded.strip_suffix('/').map(match_route) {
            Some(Some(route)) => route,
            _ => return Err(page_not_found()),
        },
    };
    if !route.allows(req.method()) {
        return Err(empty_response(StatusCode::METHOD_NOT_ALLOWED));
    }
    // gorilla appends `[/]?` to every non-prefix path regexp and redirects
    // when the request's trailing slash differs from the template's. All the
    // registered templates end without a slash.
    if decoded.ends_with('/') && !route.is_prefix() {
        return Err(strict_slash_redirect(
            req.method(),
            &decoded[..decoded.len() - 1],
            req.uri().query(),
        ));
    }

    Ok(route)
}

/// The synchronous handlers (everything but `/.api/blame/...`).
fn route_response<B>(req: &Request<B>, route: Route) -> Response<BoxBody> {
    match route {
        // Served by `api_response_with_deps`; the synchronous entry point
        // never has the dependencies and must not be used for blame or
        // search.
        // Served by `api_response_with_deps`; the synchronous entry point
        // never has the dependencies and must not be used for these routes.
        Route::BlameStream { .. }
        | Route::SearchStream
        | Route::Graphql
        | Route::RepoRefresh { .. }
        | Route::LlmChatCompletions
        | Route::LlmModels
        | Route::LlmModel { .. } => empty_response(StatusCode::INTERNAL_SERVER_ERROR),
        Route::Registry => registry_response(req),
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
        Route::WebhookUuid { uuid } => webhook_uuid_response(&uuid),
        Route::GitHubWebhooks => webhook_validation_error(),
        Route::GitLabWebhooks | Route::BitbucketServerWebhooks | Route::BitbucketCloudWebhooks => {
            empty_response(StatusCode::OK)
        }
        Route::OpenCodeGraph => error_handler_response(),
        Route::RepoShield { repo } => {
            debug_assert!(!repo.is_empty());
            error_handler_response()
        }
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
    if let Some(rest) = path.strip_prefix(WEBHOOKS_PREFIX) {
        if let Some(uuid) = single_segment(rest) {
            return Some(Route::WebhookUuid {
                uuid: uuid.to_owned(),
            });
        }
    }
    if path == GITHUB_WEBHOOKS_PATH {
        return Some(Route::GitHubWebhooks);
    }
    if path == GITLAB_WEBHOOKS_PATH {
        return Some(Route::GitLabWebhooks);
    }
    if path == BITBUCKET_SERVER_WEBHOOKS_PATH {
        return Some(Route::BitbucketServerWebhooks);
    }
    if path == BITBUCKET_CLOUD_WEBHOOKS_PATH {
        return Some(Route::BitbucketCloudWebhooks);
    }
    if path == GRAPHQL_PATH {
        return Some(Route::Graphql);
    }
    if path == OPENCODEGRAPH_PATH {
        return Some(Route::OpenCodeGraph);
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
    if path == SEARCH_STREAM_PATH {
        return Some(Route::SearchStream);
    }
    // `llm := m.PathPrefix("/llm/").Subrouter()` (`httpapi.go:325-326`) with
    // `Path("/chat/completions")`, `Path("/models")` and
    // `Path("/models/{modelId}")`. The subrouter consumes the whole prefix, so
    // an unknown `/.api/llm/...` path is a plain 404.
    if path == LLM_CHAT_COMPLETIONS_PATH {
        return Some(Route::LlmChatCompletions);
    }
    if path == LLM_MODELS_PATH {
        return Some(Route::LlmModels);
    }
    if let Some(rest) = path.strip_prefix(LLM_MODELS_PREFIX) {
        if let Some(id) = single_segment(rest) {
            return Some(Route::LlmModel { id: id.to_owned() });
        }
    }
    if let Some(caps) = repo_shield_regex().captures(path) {
        return Some(Route::RepoShield {
            repo: caps["Repo"].to_owned(),
        });
    }
    if let Some(caps) = repo_refresh_regex().captures(path) {
        return Some(Route::RepoRefresh {
            repo: caps["Repo"].to_owned(),
        });
    }
    if let Some(caps) = blame_regex().captures(path) {
        let rev = caps.name("Rev").map(|rev| rev.as_str().to_owned());
        return Some(Route::BlameStream {
            repo: caps["Repo"].to_owned(),
            rev,
            path: caps["Path"].to_owned(),
        });
    }
    None
}

/// Test-only access to the route classifier.
#[cfg(test)]
pub(crate) fn route_for_test(path: &str) -> Option<Route> {
    match_route(path)
}

/// `repo.Path("/shield")` under the `/.api/repos/{Repo}/-/` subrouter
/// (`httpapi.go:317-322`). gorilla concatenates the parent `PathPrefix`
/// template with the child path and quotes it, so the compiled route is
/// `^/\.api/repos/(?P<Repo>...)/-/shield$`; `StrictSlash`'s `[/]?` suffix is
/// equivalent to the caller's strip-and-redirect branch, so it is omitted
/// here. The Rust `regex` crate has the same leftmost-first semantics as Go's
/// `regexp`, so `routevar.RepoPattern` is reused verbatim.
fn repo_shield_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(&format!(
            r"^/\.api/repos/(?P<Repo>{REPO_PATTERN})/-/shield$"
        ))
        .expect("repo shield route regexp")
    })
}

/// `repo.Path("/refresh")` under the same `/.api/repos/{Repo}/-/`
/// subrouter (`httpapi.go:323`), i.e.
/// `^/\.api/repos/(?P<Repo>...)/-/refresh$`.
fn repo_refresh_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(&format!(
            r"^/\.api/repos/(?P<Repo>{REPO_PATTERN})/-/refresh$"
        ))
        .expect("repo refresh route regexp")
    })
}

/// `/.api/blame/{Repo}{Rev}/-/stream/{Path:.*}` (`httpapi.go:165`) with
/// `routevar.RepoRevSuffix` = `{Rev:(?:@<RevPattern>)?}`. `RevPattern` is the
/// same component pattern as `RepoPattern`; the gorilla var includes the `@`,
/// which the handler strips through `routevar.ToRepoRev`.
fn blame_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(&format!(
            r"^/\.api/blame/(?P<Repo>{REPO_PATTERN})(?:@(?P<Rev>{REPO_PATTERN}))?/-/stream/(?P<Path>.*)$"
        ))
        .expect("blame stream route regexp")
    })
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

// ---------------------------------------------------------------------------
// Extension registry seam (`cmd/frontend/internal/registry/http_api.go`)
// ---------------------------------------------------------------------------

/// `registry.APIVersion`.
pub const REGISTRY_API_VERSION: &str = "20180621";
/// `registry.AcceptHeader`.
pub const REGISTRY_ACCEPT_HEADER: &str =
    "application/vnd.sourcegraph.api+json;version=20180621";
/// `registry.MediaType`.
pub const REGISTRY_MEDIA_TYPE: &str = "sourcegraph.v20180621; format=json";

/// One `registry.Publisher`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryPublisher {
    pub name: String,
    #[serde(default)]
    pub url: String,
}

/// One `registry.Extension`. `created_at`/`updated_at`/`published_at` are the
/// RFC 3339 strings the Go `time.Time` marshaler produces (the frozen data is
/// copied into the injected seam, not embedded in the binary).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryExtension {
    pub uuid: String,
    #[serde(rename = "extensionID")]
    pub extension_id: String,
    #[serde(default)]
    pub publisher: RegistryPublisher,
    pub name: String,
    #[serde(default)]
    pub manifest: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "publishedAt")]
    pub published_at: String,
    #[serde(default)]
    pub url: String,
}

/// Go's `json.Encoder` escapes `<`, `>`, `&`, U+2028 and U+2029 (serde_json
/// does not). The frozen manifests contain these characters.
fn go_json_escape(body: &str) -> String {
    let mut escaped = String::with_capacity(body.len());
    for character in body.chars() {
        match character {
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// `dotcom.SourcegraphDotComMode()` plus `getFrozenRegistryData()`. Production
/// uses [`DefaultRegistrySource`]; the differential harness injects the frozen
/// data through [`StaticRegistrySource`].
pub trait ExtensionRegistrySource: Send + Sync {
    fn dotcom(&self) -> bool;
    fn extensions(&self) -> Vec<RegistryExtension>;
}

/// The non-dotcom default: the frozen legacy extension data is not shipped in
/// this port (see the M9.19c overview errata), so dotcom mode also answers
/// the non-dotcom 404.
#[derive(Debug, Default)]
pub struct DefaultRegistrySource;

impl ExtensionRegistrySource for DefaultRegistrySource {
    fn dotcom(&self) -> bool {
        matches!(
            std::env::var("SOURCEGRAPHDOTCOM_MODE").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
        )
    }

    fn extensions(&self) -> Vec<RegistryExtension> {
        Vec::new()
    }
}

/// A registry source over injected frozen data.
pub struct StaticRegistrySource {
    pub dotcom: bool,
    pub extensions: Vec<RegistryExtension>,
}

impl ExtensionRegistrySource for StaticRegistrySource {
    fn dotcom(&self) -> bool {
        self.dotcom
    }

    fn extensions(&self) -> Vec<RegistryExtension> {
        self.extensions.clone()
    }
}

/// `filterRegistryExtensions`: an empty query returns every extension; else
/// a case-insensitive substring match on `extensionID` (input order kept).
fn filter_registry_extensions(
    extensions: Vec<RegistryExtension>,
    query: &str,
) -> Vec<RegistryExtension> {
    if query.is_empty() {
        return extensions;
    }
    let query = query.to_lowercase();
    extensions
        .into_iter()
        .filter(|extension| extension.extension_id.to_lowercase().contains(&query))
        .collect()
}

/// `findRegistryExtension`.
fn find_registry_extension(
    extensions: &[RegistryExtension],
    uuid: Option<&str>,
    extension_id: Option<&str>,
) -> Option<RegistryExtension> {
    extensions
        .iter()
        .find(|extension| {
            uuid.is_some_and(|uuid| extension.uuid == uuid)
                || extension_id.is_some_and(|id| extension.extension_id == id)
        })
        .cloned()
}

/// `json.NewEncoder(w).Encode` for the registry responses: net/http sniffs
/// the JSON body as `text/plain; charset=utf-8`.
fn json_response(status: StatusCode, body: String) -> Response<BoxBody> {
    let mut response = text_response(status, "text/plain; charset=utf-8", &body, false);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=120, private"),
    );
    response
}

/// `w.WriteHeader(404)` with no body or content type.
fn empty_not_found() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(
        Bytes::new(),
    )));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}

/// `registry.HandleRegistry`, with the frozen data injected.
pub fn registry_response_with<B>(
    req: &Request<B>,
    source: &dyn ExtensionRegistrySource,
) -> Response<BoxBody> {
    if !source.dotcom() {
        return text_response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "no local extension registry exists\n",
            true,
        );
    }

    let accept = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let path = req.uri().path();
    let decoded = decode_path(path);
    let url_path = decoded.strip_prefix("/.api").unwrap_or(&decoded);
    let extensions = source.extensions();

    // The handler sets these before validating the Accept header.
    let mut response = if accept != REGISTRY_ACCEPT_HEADER {
        text_response(
            StatusCode::BAD_REQUEST,
            "text/plain; charset=utf-8",
            &format!("invalid Accept header: expected \"{REGISTRY_ACCEPT_HEADER}\"\n"),
            true,
        )
    } else {
        const EXTENSIONS_PATH: &str = "/registry/extensions";
        if url_path == EXTENSIONS_PATH {
            let query = req
                .uri()
                .query()
                .map(|query| {
                    url::form_urlencoded::parse(query.as_bytes())
                        .find(|(key, _)| key == "q")
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_default()
                })
                .unwrap_or_default();
            let filtered = filter_registry_extensions(extensions.clone(), &query);
            // An empty (nil) Go slice marshals as `null`, not `[]`.
            let body = if filtered.is_empty() {
                Ok("null".to_owned())
            } else {
                serde_json::to_string(&filtered).map(|body| go_json_escape(&body))
            };
            match body {
                Ok(body) => json_response(StatusCode::OK, format!("{body}\n")),
                Err(error) => text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "text/plain; charset=utf-8",
                    &format!("{error}\n"),
                    true,
                ),
            }
        } else if url_path == format!("{EXTENSIONS_PATH}/featured") {
            json_response(StatusCode::OK, "[]\n".to_owned())
        } else if let Some(spec) = url_path.strip_prefix(&format!("{EXTENSIONS_PATH}/")) {
            let found = if let Some(uuid) = spec.strip_prefix("uuid/") {
                find_registry_extension(&extensions, Some(uuid), None)
            } else if let Some(id) = spec.strip_prefix("extension-id/") {
                find_registry_extension(&extensions, None, Some(id))
            } else {
                None
            };
            match found {
                Some(extension) => match serde_json::to_string(&extension) {
                    Ok(body) => json_response(
                        StatusCode::OK,
                        format!("{}\n", go_json_escape(&body)),
                    ),
                    Err(error) => text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "text/plain; charset=utf-8",
                        &format!("{error}\n"),
                        true,
                    ),
                },
                // `w.WriteHeader(404)` for an unrecognized spec has an empty
                // body; a missing uuid/extension-id uses `http.Error`.
                None if !spec.starts_with("uuid/") && !spec.starts_with("extension-id/") => {
                    empty_not_found()
                }
                None => {
                    let mut response = text_response(
                        StatusCode::NOT_FOUND,
                        "text/plain; charset=utf-8",
                        "extension not found\n",
                        true,
                    );
                    response.headers_mut().insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("max-age=5, private"),
                    );
                    response
                }
            }
        } else {
            empty_not_found()
        }
    };

    // The baseline sets these before the route switch, even for empty 404s.
    response.headers_mut().insert(
        HeaderName::from_static("x-sourcegraph-media-type"),
        HeaderValue::from_static(REGISTRY_MEDIA_TYPE),
    );
    response.headers_mut().insert(
        header::VARY,
        HeaderValue::from_static("Accept, User-Agent"),
    );
    response
}

/// The production registry source (no frozen data; non-dotcom answer).
fn registry_response<B>(req: &Request<B>) -> Response<BoxBody> {
    registry_response_with(req, &DefaultRegistrySource)
}

// ---------------------------------------------------------------------------
// src-cli version cache seam (`cmd/frontend/internal/httpapi/src_cli.go`)
// ---------------------------------------------------------------------------

/// `time.Now`; the differential harness injects a fake clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// `glock.NewRealClock()`.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// `httpcli.ExternalClient` + the `sourcegraph.com` version endpoint.
pub trait SrcCliVersionSource: Send + Sync {
    /// Fetch the recommended version, mirroring `updateCachedVersion`'s
    /// errors.
    fn version(&self) -> Result<String, String>;
}

/// The production source: `GET https://sourcegraph.com/.api/src-cli/versions/
/// {major}.{minor}` and JSON-decode a string.
#[derive(Debug, Default)]
pub struct RemoteSrcCliVersionSource;

impl SrcCliVersionSource for RemoteSrcCliVersionSource {
    fn version(&self) -> Result<String, String> {
        use crate::external_api::SRC_CLI_MINIMUM_VERSION;
        let mut parts = SRC_CLI_MINIMUM_VERSION.split('.');
        let major = parts.next().unwrap_or("0");
        let minor = parts.next().unwrap_or("0");
        let url = format!("https://sourcegraph.com/.api/src-cli/versions/{major}.{minor}");
        let fetch = async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|error| format!("building request: {error}"))?;
            let response = client
                .get(&url)
                .send()
                .await
                .map_err(|error| format!("getting version from Sourcegraph: {error}"))?;
            let status = response.status();
            if !status.is_success() {
                return Err(format!("unexpected status code: {}", status.as_u16()));
            }
            response
                .json::<String>()
                .await
                .map_err(|error| format!("reading version from response payload: {error}"))
        };
        // `Version()` is synchronous. When called from a multi-thread runtime
        // (the HTTP server), run the fetch on a scratch runtime; otherwise
        // drive a current-thread runtime directly.
        let join = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(fetch)
        });
        join.join().unwrap_or_else(|_| {
            Err("getting version from Sourcegraph: request thread panicked".to_owned())
        })
    }
}

/// `srcCliVersionHandler`'s 10-minute lazy cache.
pub struct SrcCliVersionHandler {
    max_stale: Duration,
    source: Arc<dyn SrcCliVersionSource>,
    clock: Arc<dyn Clock>,
    state: Mutex<VersionCacheState>,
}

#[derive(Debug, Default)]
struct VersionCacheState {
    last_update: Option<Instant>,
    version: String,
}

impl SrcCliVersionHandler {
    /// `newSrcCliVersionHandler` (`srcCliCacheLifetime`).
    pub fn new(source: Arc<dyn SrcCliVersionSource>, clock: Arc<dyn Clock>) -> Self {
        Self::with_max_stale(source, clock, Duration::from_secs(600))
    }

    pub fn with_max_stale(
        source: Arc<dyn SrcCliVersionSource>,
        clock: Arc<dyn Clock>,
        max_stale: Duration,
    ) -> Self {
        Self {
            max_stale,
            source,
            clock,
            state: Mutex::new(VersionCacheState::default()),
        }
    }

    /// `cachedVersion`: a zero `lastUpdate` is alway stale, so `None`.
    fn cached_version(&self) -> Option<String> {
        let state = self.state.lock().expect("src-cli cache lock");
        let last_update = state.last_update?;
        if self.clock.now().duration_since(last_update) > self.max_stale {
            return None;
        }
        if state.version.is_empty() {
            None
        } else {
            Some(state.version.clone())
        }
    }

    /// `Version()`: cached, else fetch; a fetch error falls back to
    /// `srccli.MinimumVersion`.
    pub fn version(&self) -> String {
        if let Some(version) = self.cached_version() {
            return version;
        }
        match self.source.version() {
            Ok(version) => {
                let mut state = self.state.lock().expect("src-cli cache lock");
                state.last_update = Some(self.clock.now());
                state.version = version.clone();
                version
            }
            Err(_) => SRC_CLI_MINIMUM_VERSION.to_owned(),
        }
    }
}

/// The process-wide handler, so the cache survives across requests. The
/// production route keeps Go's lazy 10-minute fetch; an air-gapped instance
/// falls back to [`SRC_CLI_MINIMUM_VERSION`].
pub fn production_src_cli_handler() -> Arc<SrcCliVersionHandler> {
    static HANDLER: OnceLock<Arc<SrcCliVersionHandler>> = OnceLock::new();
    HANDLER
        .get_or_init(|| {
            Arc::new(SrcCliVersionHandler::new(
                Arc::new(RemoteSrcCliVersionSource),
                Arc::new(SystemClock),
            ))
        })
        .clone()
}

/// `srcCliVersionHandler.ServeHTTP` (`src_cli.go:68-82`) with the version
/// cache injected.
pub fn src_cli_response_with(
    method: &Method,
    rest: &str,
    handler: &SrcCliVersionHandler,
) -> Response<BoxBody> {
    if rest == "version" {
        // `writeJSON` uses `json.Encoder`, which emits the JSON string with
        // Go's HTML escaping (`<`, `>`, `&`, U+2028/2029).
        let encoded = serde_json::to_string(&handler.version())
            .unwrap_or_else(|_| "\"\"".to_owned());
        let body = format!("{{\"version\":{}}}\n", go_json_escape(&encoded));
        return text_response(
            StatusCode::OK,
            "application/json; charset=utf-8",
            &body,
            false,
        );
    }
    if SRC_CLI_FILENAMES.contains(&rest) {
        let location = format!(
            "{SRC_CLI_DOWNLOADS_URL}/{}/{rest}",
            handler.version()
        );
        return http_redirect(method, StatusCode::FOUND, location);
    }
    page_not_found()
}

fn src_cli_response(method: &Method, rest: &str) -> Response<BoxBody> {
    src_cli_response_with(method, rest, &production_src_cli_handler())
}

// ---------------------------------------------------------------------------
// GitHub webhook seam (`cmd/frontend/internal/webhooks/github_webhooks.go`)
// ---------------------------------------------------------------------------

/// One configured GitHub webhook secret (`schema.GitHubWebhook.Secret`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GitHubWebhookConfig {
    pub secret: String,
}

/// The subset of a GitHub external service's config the webhook path reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubExternalService {
    pub id: i64,
    /// `schema.GitHubConnection.Url`.
    pub url: String,
    pub webhooks: Vec<GitHubWebhookConfig>,
}

/// A dispatch failure mapped to the status the baseline uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookDispatchError {
    NotFound(String),
    Internal(String),
}

/// The DB/dispatch seam. The production default has no database connection
/// (the frontend's `service.rs` wiring is outside this slice): it reports an
/// empty `external_services` table, which is the same answer as the previous
/// fixed 400.
pub trait GitHubWebhookDeps: Send + Sync {
    fn get_external_service(&self, id: i64) -> Result<Option<GitHubExternalService>, String>;

    fn list_github_external_services(&self) -> Result<Vec<GitHubExternalService>, String>;

    fn dispatch(
        &self,
        event: &str,
        code_host_url: &str,
        body: &[u8],
    ) -> Result<(), WebhookDispatchError>;
}

/// The empty-table default.
#[derive(Debug, Default)]
pub struct EmptyGitHubWebhookDeps;

impl GitHubWebhookDeps for EmptyGitHubWebhookDeps {
    fn get_external_service(&self, _id: i64) -> Result<Option<GitHubExternalService>, String> {
        Ok(None)
    }

    fn list_github_external_services(&self) -> Result<Vec<GitHubExternalService>, String> {
        Ok(Vec::new())
    }

    fn dispatch(
        &self,
        _event: &str,
        _code_host_url: &str,
        _body: &[u8],
    ) -> Result<(), WebhookDispatchError> {
        Ok(())
    }
}

/// `github.ValidateSignature` (go-github `messages.go`): `sha1=`/`sha256=`/
/// `sha512=` hex HMAC over the raw body.
pub(crate) fn validate_github_signature(
    signature: &str,
    body: &[u8],
    secret: &[u8],
) -> Result<(), String> {
    if signature.is_empty() {
        return Err("missing signature".to_owned());
    }
    let (prefix, hex_tag) = signature
        .split_once('=')
        .ok_or_else(|| format!("error parsing signature \"{signature}\""))?;
    let tag =
        hex_decode(hex_tag).ok_or_else(|| format!("error decoding signature \"{signature}\""))?;
    let valid = match prefix {
        "sha1" => {
            let mut mac = Hmac::<Sha1>::new_from_slice(secret).map_err(|_| "invalid key")?;
            mac.update(body);
            mac.verify_slice(&tag).is_ok()
        }
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret).map_err(|_| "invalid key")?;
            mac.update(body);
            mac.verify_slice(&tag).is_ok()
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(secret).map_err(|_| "invalid key")?;
            mac.update(body);
            mac.verify_slice(&tag).is_ok()
        }
        other => return Err(format!("unknown hash type prefix: \"{other}\"")),
    };
    if valid {
        Ok(())
    } else {
        Err("unable to validate webhook signature".to_owned())
    }
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars: Vec<char> = value.chars().collect();
    for pair in chars.chunks(2) {
        let high = pair[0].to_digit(16)?;
        let low = pair[1].to_digit(16)?;
        bytes.push((high * 16 + low) as u8);
    }
    Some(bytes)
}

/// `validateAnyConfiguredSecret`.
fn validate_any_configured_secret(
    service: &GitHubExternalService,
    signature: &str,
    body: &[u8],
) -> Result<(), String> {
    if signature.is_empty() {
        return Ok(());
    }
    if service.webhooks.is_empty() {
        return Err("no webhooks defined".to_owned());
    }
    for hook in &service.webhooks {
        if hook.secret.is_empty() {
            continue;
        }
        if validate_github_signature(signature, body, hook.secret.as_bytes()).is_ok() {
            return Ok(());
        }
    }
    Err("unable to validate webhook signature".to_owned())
}

fn webhook_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    text_response(
        status,
        "text/plain; charset=utf-8",
        &format!("{message}\n"),
        true,
    )
}

/// `GitHubWebhook.getExternalService`: the `externalServiceID` form parameter
/// (legacy fallback when absent), then secret validation.
fn github_webhook_external_service(
    deps: &dyn GitHubWebhookDeps,
    query_pairs: &[(String, String)],
    signature: &str,
    body: &[u8],
) -> Result<GitHubExternalService, Response<BoxBody>> {
    // `r.FormValue` returns the first value for the key.
    let raw_id = query_pairs
        .iter()
        .find(|(key, _)| key == "externalServiceID")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();

    if raw_id.is_empty() {
        // Legacy path: try every configured GitHub external service.
        // `findAndValidateExternalService`'s error is not a NotFound error, so
        // `ServeHTTP` answers the constant `Error validating payload`.
        let services = deps.list_github_external_services().map_err(|_| {
            webhook_error(StatusCode::BAD_REQUEST, "Error validating payload")
        })?;
        for service in services {
            if validate_any_configured_secret(&service, signature, body).is_ok() {
                return Ok(service);
            }
        }
        return Err(webhook_error(
            StatusCode::BAD_REQUEST,
            "Error validating payload",
        ));
    }

    let Ok(id) = raw_id.parse::<i64>() else {
        return Err(webhook_error(
            StatusCode::BAD_REQUEST,
            "Error validating payload",
        ));
    };
    // `ServeHTTP` only surfaces a NotFound error as-is; every other
    // `getExternalService` failure (DB error, non-GitHub config, config
    // decode error, signature failure) answers `Error validating payload`.
    let service = deps
        .get_external_service(id)
        .map_err(|_| webhook_error(StatusCode::BAD_REQUEST, "Error validating payload"))?;
    let Some(service) = service else {
        return Err(webhook_error(
            StatusCode::NOT_FOUND,
            "External service not found",
        ));
    };
    // The wrapped signature error is not a NotFound error either, so the
    // response is the same constant.
    validate_any_configured_secret(&service, signature, body)
        .map_err(|_| webhook_error(StatusCode::BAD_REQUEST, "Error validating payload"))?;
    Ok(service)
}

/// `GitHubWebhook.ServeHTTP`, with the DB/dispatch seam injected.
pub async fn github_webhook_response<B>(
    req: &mut Request<B>,
    deps: &dyn GitHubWebhookDeps,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = bytes::Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    use http_body_util::BodyExt;

    let body = match req.body_mut().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => return webhook_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let body = body.to_vec();

    let signature = req
        .headers()
        .get("x-hub-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    // `ServeHTTP` reads the body into `body` before calling `getExternalService`,
    // so the later `r.FormValue` only sees the URL query: an urlencoded form
    // body is already exhausted and contributes no `externalServiceID`.
    let pairs: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default();

    let service = match github_webhook_external_service(deps, &pairs, &signature, &body) {
        Ok(service) => service,
        Err(response) => return response,
    };
    if url::Url::parse(&service.url).is_err() {
        return webhook_error(StatusCode::INTERNAL_SERVER_ERROR, "Invalid code host URL");
    }

    let event = req
        .headers()
        .get("x-github-event")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    match deps.dispatch(&event, &service.url, &body) {
        Ok(()) => empty_response(StatusCode::OK),
        Err(WebhookDispatchError::NotFound(message)) => {
            webhook_error(StatusCode::NOT_FOUND, &message)
        }
        Err(WebhookDispatchError::Internal(message)) => {
            webhook_error(StatusCode::INTERNAL_SERVER_ERROR, &message)
        }
    }
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

/// `webhooks.GitHubWebhook.ServeHTTP` with no GitHub external services
/// configured (`github_webhooks.go:26-50`): the legacy lookup returns a plain
/// (non-NotFound) error, so the handler answers
/// `http.Error(..., "Error validating payload", 400)`. This is the answer of
/// the synchronous [`api_response`] using [`EmptyGitHubWebhookDeps`]; the
/// full signature validation and event dispatch live in
/// [`github_webhook_response`].
fn webhook_validation_error() -> Response<BoxBody> {
    text_response(
        StatusCode::BAD_REQUEST,
        "text/plain; charset=utf-8",
        "Error validating payload\n",
        true,
    )
}

/// `webhooks.handler` (`webhooks.go:186-245`) with an empty webhooks table:
/// invalid UUIDs are rejected with `%q` quoting, well-formed UUIDs are not
/// found. The DB lookup, secret decryption and code-host dispatch are not
/// ported.
fn webhook_uuid_response(uuid: &str) -> Response<BoxBody> {
    if Uuid::parse_str(uuid).is_err() {
        let body = format!("Could not parse UUID from URL path {}.\n", go_quote(uuid));
        return text_response(
            StatusCode::BAD_REQUEST,
            "text/plain; charset=utf-8",
            &body,
            true,
        );
    }
    text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "Could not find webhook with provided UUID.\n",
        true,
    )
}

/// Go's `fmt.Sprintf("%q", value)` for the ASCII path segments seen here.
pub(crate) fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if ch.is_control() => quoted.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

/// `ErrorHandler.Handle` (`httpapi.go:405-441`) for a handler error under
/// `JsonMiddleware` with `WriteErrBody=false` (the default): `http.Error`
/// overrides the middleware's `application/json` to
/// `text/plain; charset=utf-8`, adds `nosniff`, `Cache-Control:
/// no-cache, max-age=0` is set, and only the trailing newline is written.
/// `/.api/opencodegraph` (feature flag off) and `/.api/repos/{Repo}/-/shield`
/// (`CountGoImporters` unsupported on self-hosted) both take this path with
/// an `errcode.HTTP` mapping of 500.
fn error_handler_response() -> Response<BoxBody> {
    let mut response = text_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        "\n",
        true,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, max-age=0"),
    );
    response
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

    #[test]
    fn webhook_routes() {
        let live = live();

        // OSS `emptyWebhookHandler`: raw 200, empty body, no Content-Type.
        let gitlab = api_response(&request(Method::POST, "/.api/gitlab-webhooks"), &live);
        assert_eq!(gitlab.status(), StatusCode::OK);
        assert!(gitlab.headers().get(header::CONTENT_TYPE).is_none());

        // No GitHub external services: `findAndValidateExternalService`
        // returns a plain error -> 400.
        let github = api_response(&request(Method::POST, "/.api/github-webhooks"), &live);
        assert_eq!(github.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            github.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );

        // UUID parsing (google/uuid formats) and the empty webhooks table.
        let invalid = api_response(&request(Method::POST, "/.api/webhooks/nope"), &live);
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        for uuid in [
            "/.api/webhooks/00000000-0000-0000-0000-000000000000",
            "/.api/webhooks/00000000000000000000000000000000",
            "/.api/webhooks/urn:uuid:00000000-0000-0000-0000-000000000000",
            "/.api/webhooks/%7B00000000-0000-0000-0000-000000000000%7D",
        ] {
            assert_eq!(
                api_response(&request(Method::POST, uuid), &live).status(),
                StatusCode::NOT_FOUND,
                "{uuid}"
            );
        }
        assert_eq!(
            api_response(
                &request(
                    Method::GET,
                    "/.api/webhooks/00000000-0000-0000-0000-000000000000"
                ),
                &live
            )
            .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            api_response(&request(Method::POST, "/.api/webhooks/"), &live).status(),
            StatusCode::NOT_FOUND
        );
        let trailing = api_response(&request(Method::POST, "/.api/gitlab-webhooks/"), &live);
        assert_eq!(trailing.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            trailing.headers().get(header::LOCATION).unwrap(),
            "/.api/gitlab-webhooks"
        );
        assert!(trailing.headers().get(header::CONTENT_TYPE).is_none());
    }

    #[tokio::test]
    async fn opencodegraph_error_response() {
        let live = live();
        let response = api_response(&request(Method::POST, "/.api/opencodegraph"), &live);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache, max-age=0"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"\n");

        assert_eq!(
            api_response(&request(Method::GET, "/.api/opencodegraph"), &live).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        let trailing = api_response(&request(Method::POST, "/.api/opencodegraph/"), &live);
        assert_eq!(trailing.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            trailing.headers().get(header::LOCATION).unwrap(),
            "/.api/opencodegraph"
        );
    }

    #[tokio::test]
    async fn repo_shield_route() {
        let live = live();
        let response = api_response(
            &request(Method::GET, "/.api/repos/github.com/foo/bar/-/shield"),
            &live,
        );
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache, max-age=0"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"\n");

        // `routevar.RepoPattern`: components are non-empty and free of `@`
        // and `/`; a component that is exactly `-` is rejected while longer
        // dashed components are accepted.
        assert_eq!(
            match_route("/.api/repos/foo/-/shield"),
            Some(Route::RepoShield {
                repo: "foo".to_owned(),
            })
        );
        assert_eq!(
            match_route("/.api/repos/foo-bar/-/shield"),
            Some(Route::RepoShield {
                repo: "foo-bar".to_owned(),
            })
        );
        assert_eq!(
            match_route("/.api/repos/foo/--/bar/-/shield"),
            Some(Route::RepoShield {
                repo: "foo/--/bar".to_owned(),
            })
        );
        assert_eq!(match_route("/.api/repos/-/-/shield"), None);
        assert_eq!(match_route("/.api/repos/foo@bar/-/shield"), None);
        assert_eq!(match_route("/.api/repos/foo/-/shield/extra"), None);

        assert_eq!(
            api_response(&request(Method::POST, "/.api/repos/foo/-/shield"), &live).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        let trailing = api_response(&request(Method::GET, "/.api/repos/foo/-/shield/"), &live);
        assert_eq!(trailing.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            trailing.headers().get(header::LOCATION).unwrap(),
            "/.api/repos/foo/-/shield"
        );

        // gorilla `Router.ServeHTTP` cleans the path first: a doubled slash
        // is redirected before route matching.
        let cleaned = api_response(&request(Method::GET, "/.api/repos//-/shield"), &live);
        assert_eq!(cleaned.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            cleaned.headers().get(header::LOCATION).unwrap(),
            "/.api/repos/-/shield"
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
