#![forbid(unsafe_code)]

//! M9.33c/M9.34c/M9.35c: the inbound webhook routes.
//!
//! Ports the generic webhook route (`/.api/webhooks/{webhook_uuid}`):
//!
//! * `cmd/frontend/internal/webhooks/webhooks.go` — `handler`, `Dispatch`
//!   (`defaultHandlers` registers `ping`/`installation` for `GITHUB` and
//!   `ping` for `BITBUCKETSERVER`; other kinds have no default handler);
//! * `cmd/frontend/internal/webhooks/github_webhooks.go` — secret branch,
//!   `github.ValidatePayload`, `github.ParseWebHook`;
//! * `internal/github_apps/store` — `GetByAppID` and the
//!   `github_app_installs` upsert (`Install`);
//! * `gitlab_webhooks.go` / `bitbucketcloud_webhooks.go` /
//!   `azuredevops_webhooks.go` (M9.35c) — token/signature checks, event
//!   parsing and `Dispatch(<event kind>)`;
//! * `internal/extsvc/{gitlab,bitbucketcloud,azuredevops}` event parsing.
//!
//! No new table, column or Redis key: the existing `webhooks`,
//! `github_apps` and `github_app_installs` rows are reused. The webhook
//! signature contract is the go-github v55 one (`X-Hub-Signature-256` then
//! `X-Hub-Signature`, HMAC-SHA1/256/512 over the raw body); GitLab uses the
//! shared `X-Gitlab-Token`, and Azure DevOps is unsigned.
//!
//! Errata (source wins):
//!
//! * The baseline registers **only** `installation` (GitHub). An
//!   `installation_repositories` event parses but dispatches to no handler,
//!   an HTTP 200 no-op; this module reproduces that.
//! * `gitlab_webhooks.go` never reads `X-Gitlab-Event`: the event kind is the
//!   payload's `object_kind`. GitLab's unknown object kind (and a
//!   `merge_request` with an unknown/missing action) answers **204** with the
//!   error text, not 200/400.
//! * In production `internal/httpapi/webhookhandlers` registers batches/sync
//!   handlers for these kinds; those handler side effects are outside this
//!   slice, so a successfully parsed event is a no-op here, matching
//!   `Dispatch` with no registered handler.

use bytes::Bytes;
use http::header::HeaderValue;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use serde::Deserialize;
use sha2::Sha256;
use tonic::body::BoxBody;

use crate::batches_credentials::BatchesCredentialsDatabase;
use crate::batches_graphql_store::PgBatchesGraphqlStore;
use crate::external_api::{go_quote, validate_github_signature};
use crate::permissions_webhook::{self, PermissionsWebhookDeps, PgPermissionsWebhookDeps};
use crate::webhook_authz_handlers::{dispatch_github, parse_event, AuthzDeps, PgAuthzDeps};

/// The generic webhook mount (`webhooks.NewHandler`).
pub const WEBHOOKS_PREFIX: &str = "/.api/webhooks/";

const X_HUB_SIGNATURE_256: &str = "x-hub-signature-256";
const X_HUB_SIGNATURE: &str = "x-hub-signature";
const X_GITHUB_EVENT: &str = "x-github-event";

/// `extsvc.KindGitHub`.
const KIND_GITHUB: &str = "GITHUB";
/// `extsvc.KindBitbucketServer` (`VariantBitbucketServer.AsKind()`).
const KIND_BITBUCKET_SERVER: &str = "BITBUCKETSERVER";
/// `extsvc.KindGitLab`.
const KIND_GITLAB: &str = "GITLAB";
/// `extsvc.KindBitbucketCloud`.
const KIND_BITBUCKET_CLOUD: &str = "BITBUCKETCLOUD";
/// `extsvc.KindAzureDevOps`.
const KIND_AZURE_DEVOPS: &str = "AZUREDEVOPS";

/// `gitlab.TokenHeaderName`.
const X_GITLAB_TOKEN: &str = "x-gitlab-token";
/// `gitlab_webhooks.go` reads the event key from `bitbucketcloud`'s
/// `X-Event-Key` header (never `X-Gitlab-Event`; see the module errata).
const X_EVENT_KEY: &str = "x-event-key";

/// `ghtypes.UserCredentialGitHubAppKind` / `SiteCredentialGitHubAppKind`.
const KIND_USER_CREDENTIAL: &str = "USER_CREDENTIAL";
const KIND_SITE_CREDENTIAL: &str = "SITE_CREDENTIAL";

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// The `webhooks` row fields the handler reads (`GetByUUID`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundWebhook {
    pub code_host_kind: String,
    pub code_host_urn: String,
    /// `webhook.Secret` after decryption; `None` is the baseline's `nil`.
    pub secret: Option<String>,
}

/// `GetByUUID` + secret decryption.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WebhookLookup {
    Found(InboundWebhook),
    /// Any lookup miss/error: the baseline answers 404 for all of them.
    NotFound,
    /// `webhook.Secret.Decrypt` failed: 500.
    DecryptError(String),
}

/// The slice of `github_apps` `handleInstallation` reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundApp {
    /// `github_apps.id` (the `github_app_installs.app_id` FK).
    pub id: i32,
    /// The GitHub App ID (`github_apps.app_id`), used to build the
    /// authenticator.
    pub app_id: i32,
    pub kind: String,
    /// `github_apps.base_url` (`app.BaseURL` for the credential URL).
    pub base_url: String,
    /// `github_apps.creator_id` (`app.CreatedByUserId`).
    pub created_by_user_id: i32,
}

/// `ghtypes.GitHubAppInstallation` for `Install`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundInstall {
    pub app_id: i32,
    pub installation_id: i32,
    pub url: String,
    pub account_login: String,
    pub account_avatar_url: String,
    pub account_url: String,
    pub account_type: String,
}

/// `database.Webhooks(...)` + `database.GitHubApps()` slice the inbound path
/// needs.
pub trait InboundDeps: Send + Sync {
    fn lookup_webhook(&self, uuid: &str) -> WebhookLookup;
    /// `GitHubApps().GetByAppID`; `Ok(None)` is `ErrNoGitHubAppFound`.
    fn get_app_by_app_id(
        &self,
        app_id: i32,
        base_url: &str,
    ) -> Result<Option<InboundApp>, String>;
    /// `GitHubApps().Install` (the `(app_id, installation_id)` upsert).
    fn install(&self, install: &InboundInstall) -> Result<(), String>;
    /// `service.CreateBatchChangesUserCredential` with the `GITHUB_APP`
    /// strategy, wrapping failures with the Go message prefix.
    fn create_user_credential(
        &self,
        app: &InboundApp,
        external_service_url: &str,
    ) -> Result<(), String>;
    /// `service.CreateBatchChangesSiteCredential` with the `GITHUB_APP`
    /// strategy, wrapping failures with the Go message prefix.
    fn create_site_credential(
        &self,
        app: &InboundApp,
        external_service_url: &str,
    ) -> Result<(), String>;
    /// The M9.44b authz side-effect seams (`webhookhandlers.Init` handlers).
    /// `None` leaves the authz events as dispatch no-ops.
    fn authz_deps(&self) -> Option<&dyn AuthzDeps> {
        None
    }
    /// The M9.45b `PermissionsGitHubWebhook` seams. `None` leaves its events
    /// as no-ops.
    fn permissions_webhook_deps(&self) -> Option<&dyn PermissionsWebhookDeps> {
        None
    }
}

// ---------------------------------------------------------------------------
// go-github payload shapes (only the fields the handler reads)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct InstallationEvent {
    #[serde(default)]
    installation: Option<Installation>,
}

#[derive(Deserialize, Default)]
struct Installation {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    app_id: Option<i64>,
    #[serde(default)]
    html_url: Option<String>,
    #[serde(default)]
    account: Option<Account>,
}

#[derive(Deserialize, Default)]
struct Account {
    #[serde(default)]
    login: Option<String>,
    #[serde(default)]
    avatar_url: Option<String>,
    #[serde(default)]
    html_url: Option<String>,
    #[serde(rename = "type", default)]
    account_type: Option<String>,
}

// ---------------------------------------------------------------------------
// Response helpers (mirror net/http)
// ---------------------------------------------------------------------------

fn plain_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers
}

/// `http.Error`: `<message>\n`, `text/plain; charset=utf-8`, `nosniff`.
fn http_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(format!(
        "{message}\n"
    )))));
    *response.status_mut() = status;
    *response.headers_mut() = plain_headers();
    response.headers_mut().insert(
        http::header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// A handler that writes nothing: net/http answers 200 with an empty body.
fn empty_ok() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = StatusCode::OK;
    response
}

fn header<'a, B>(req: &'a Request<B>, name: &str) -> &'a str {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `handler(logger, db, wh)` for the GitHub installation path (the mount is
/// `/.api/webhooks/{webhook_uuid}`).
pub async fn inbound_webhook_response<B>(
    req: &mut Request<B>,
    deps: &dyn InboundDeps,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let uuid = match req.uri().path().strip_prefix(WEBHOOKS_PREFIX) {
        Some(uuid) => uuid,
        None => "",
    };
    // A remainder containing `/` does not match the gorilla `{webhook_uuid}`
    // route, but treating it as a raw UUID is harmless: parsing fails 400.
    if uuid.is_empty() {
        return http_error(StatusCode::BAD_REQUEST, "missing uuid");
    }
    if uuid::Uuid::parse_str(uuid).is_err() {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("Could not parse UUID from URL path {}.", go_quote(uuid)),
        );
    }

    let webhook = match deps.lookup_webhook(uuid) {
        WebhookLookup::Found(webhook) => webhook,
        WebhookLookup::NotFound => {
            return http_error(
                StatusCode::NOT_FOUND,
                "Could not find webhook with provided UUID.",
            )
        }
        WebhookLookup::DecryptError(_) => {
            return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Could not decrypt webhook secret.")
        }
    };

    let secret = webhook.secret.as_deref().unwrap_or("");
    match webhook.code_host_kind.as_str() {
        KIND_GITHUB => github_response(req, deps, secret, &webhook.code_host_urn).await,
        KIND_BITBUCKET_SERVER => bitbucket_server_response(req, secret).await,
        KIND_GITLAB => gitlab_response(req, secret).await,
        KIND_BITBUCKET_CLOUD => bitbucket_cloud_response(req, secret).await,
        KIND_AZURE_DEVOPS => azure_devops_response(req).await,
        // Remaining kinds hit the same default in the baseline.
        other => http_error(
            StatusCode::NOT_IMPLEMENTED,
            &format!(
                "webhooks not implemented for code host kind {}",
                go_quote(other)
            ),
        ),
    }
}

/// `handleGitHubWebHook` + `HandleWebhook`.
async fn github_response<B>(
    req: &mut Request<B>,
    deps: &dyn InboundDeps,
    secret: &str,
    code_host_urn: &str,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let body = match read_body(req).await {
        Ok(body) => body,
        Err(_error) => {
            if secret.is_empty() {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error while reading request body.",
                );
            }
            return http_error(StatusCode::BAD_REQUEST, "Could not validate payload with secret.");
        }
    };

    let payload = if secret.is_empty() {
        body
    } else {
        match validate_payload(req, &body, secret.as_bytes()) {
            Ok(payload) => payload,
            Err(_) => {
                return http_error(
                    StatusCode::BAD_REQUEST,
                    "Could not validate payload with secret.",
                )
            }
        }
    };

    handle_github_event(req, &payload, deps, code_host_urn)
}

/// `handleBitbucketServerWebhook` + `HandleBitBucketServerWebhook`
/// (`bitbucketserver_webhooks.go`).
async fn bitbucket_server_response<B>(req: &mut Request<B>, secret: &str) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let body = match read_body(req).await {
        Ok(body) => body,
        Err(_error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error while reading request body.",
            )
        }
    };

    let signature = header(req, X_HUB_SIGNATURE);
    let event_key = header(req, "x-event-key");

    // Bitbucket Server `diagnostics:ping` test events are unsigned even when
    // a secret is configured.
    if !(signature.is_empty() && event_key == "diagnostics:ping")
        && !secret.is_empty()
        && validate_github_signature(signature, &body, secret.as_bytes()).is_err()
    {
        return http_error(StatusCode::BAD_REQUEST, "Could not validate payload with secret.");
    }

    match parse_bitbucket_server_event(event_key, &body) {
        Ok(()) => {
            // `Dispatch`: only `ping` has a handler; `diagnostics:ping` and
            // the parsable payload events are no-ops. Every successful
            // dispatch answers 200 with an empty body.
            empty_ok()
        }
        Err(error) => http_error(StatusCode::BAD_REQUEST, &format!("parsing webhook: {error}")),
    }
}

/// `bitbucketserver.ParseWebhookEvent`.
fn parse_bitbucket_server_event(event_key: &str, body: &[u8]) -> Result<(), String> {
    match event_key {
        // `PingEvent{}` ignores the payload entirely.
        "ping" | "diagnostics:ping" => Ok(()),
        "repo:refs_changed"
        | "repo:build_status"
        | "pr:activity:status"
        | "pr:activity:event"
        | "pr:activity:rescope"
        | "pr:activity:merge"
        | "pr:activity:comment"
        | "pr:activity:reviewers"
        | "pr:participant:status" => serde_json::from_slice::<serde_json::Value>(body)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        other => Err(format!(
            "unknown webhook event type: {}",
            go_quote(other)
        )),
    }
}

// ---------------------------------------------------------------------------
// GitLab (`gitlab_webhooks.go` + `internal/extsvc/gitlab/webhooks`)
// ---------------------------------------------------------------------------

/// `handleGitLabWebHook` + `HandleGitLabWebhook`.
///
/// Errata (source wins): the Go handler never reads `X-Gitlab-Event`; the event
/// kind is the payload's `object_kind`. The only header is the shared
/// `X-Gitlab-Token`, compared with `subtle.ConstantTimeCompare`.
async fn gitlab_response<B>(req: &mut Request<B>, secret: &str) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let body = if secret.is_empty() {
        match read_body(req).await {
            Ok(body) => body,
            Err(_error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error while reading request body.",
                )
            }
        }
    } else {
        if !constant_time_eq(header(req, X_GITLAB_TOKEN).as_bytes(), secret.as_bytes()) {
            return http_error(StatusCode::BAD_REQUEST, "Could not validate payload with secret.");
        }
        match read_body(req).await {
            Ok(body) => body,
            // Any `gitlabValidatePayload` error (including a read failure after
            // the token check) is answered as a 400 by the caller.
            Err(_error) => {
                return http_error(StatusCode::BAD_REQUEST, "Could not validate payload with secret.")
            }
        }
    };

    handle_gitlab_event(&body)
}

/// `gitlab/webhooks.UnmarshalEvent` dispatch, including the downcast path.
fn handle_gitlab_event(payload: &[u8]) -> Response<BoxBody> {
    let object_kind = match gitlab_object_kind(payload) {
        Ok(kind) => kind,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("determining object kind: {error}"),
            )
        }
    };

    match object_kind.as_str() {
        "merge_request" => match gitlab_merge_request_action(payload) {
            Ok(action) => match action.as_str() {
                "approved" | "close" | "merge" | "reopen" | "unapproved" | "update" => {
                    // `Dispatch` has no handler for these kinds -> no-op.
                    empty_ok()
                }
                other => {
                    // `downcast` wraps `ErrObjectKindUnknown`; the handler
                    // answers 204 with the error text.
                    no_content(format!(
                        "unknown merge request event action: {other}: unknown object kind"
                    ))
                }
            },
            // `mergeRequestEvent` itself failed to unmarshal.
            Err(error) => http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("unmarshalling event kind from webhook payload: {error}"),
            ),
        },
        // `PipelineEvent` / `PushEvent`: syntactically valid payloads unmarshal;
        // typed field-level decode differences are documented in the overview.
        "pipeline" | "push" => empty_ok(),
        other => no_content(format!("kind: {other}: unknown object kind")),
    }
}

/// The first `json.Unmarshal` into `struct{ ObjectKind string }`, with the
/// `encoding/json` error text the Go handler would produce for the shapes the
/// corpus covers.
fn gitlab_object_kind(payload: &[u8]) -> Result<String, String> {
    use serde_json::Value;

    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| go_json_syntax_error(payload, &error))?;

    match value {
        // `json.Unmarshal(null, &struct)` succeeds and leaves the zero value.
        Value::Null => Ok(String::new()),
        Value::Object(map) => match map.get("object_kind") {
            None | Some(Value::Null) => Ok(String::new()),
            Some(Value::String(kind)) => Ok(kind.clone()),
            Some(other) => Err(format!(
                "json: cannot unmarshal {} into Go struct field .object_kind of type string",
                go_json_value_kind(other)
            )),
        },
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type struct {{ ObjectKind string \"json:\\\"object_kind\\\"\" }}",
            go_json_value_kind(&other)
        )),
    }
}

/// `mergeRequestEvent.downcast`: the `object_attributes.action` string.
fn gitlab_merge_request_action(payload: &[u8]) -> Result<String, String> {
    use serde_json::Value;

    // Re-parsing cannot fail: `gitlab_object_kind` already accepted the payload.
    let value: Value = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
    match value {
        Value::Null => Ok(String::new()),
        Value::Object(map) => match map.get("object_attributes") {
            None | Some(Value::Null) => Ok(String::new()),
            Some(Value::Object(attributes)) => match attributes.get("action") {
                None | Some(Value::Null) => Ok(String::new()),
                Some(Value::String(action)) => Ok(action.clone()),
                Some(other) => Err(format!(
                    "json: cannot unmarshal {} into Go struct field mergeRequestEventObjectAttributes.action of type string",
                    go_json_value_kind(other)
                )),
            },
            Some(other) => Err(format!(
                "json: cannot unmarshal {} into Go struct field mergeRequestEvent.object_attributes of type webhooks.mergeRequestEventObjectAttributes",
                go_json_value_kind(other)
            )),
        },
        // Top-level non-objects were rejected by `gitlab_object_kind`.
        _ => Ok(String::new()),
    }
}

/// `encoding/json`'s value kind names used in unmarshal errors.
fn go_json_value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Best-effort `encoding/json` syntax error text. Only the shapes the corpus
/// exercises are reproduced exactly (empty/truncated input, trailing
/// characters).
fn go_json_syntax_error(payload: &[u8], error: &serde_json::Error) -> String {
    if String::from_utf8_lossy(payload).trim().is_empty() || error.is_eof() {
        return "unexpected end of JSON input".to_owned();
    }
    let message = error.to_string();
    if message.contains("trailing characters") {
        // serde's column is 1-based; Go reports the offending byte.
        let index = error.column().saturating_sub(1);
        if let Some(ch) = String::from_utf8_lossy(payload).chars().nth(index) {
            return format!("invalid character {ch:?} after top-level value");
        }
    }
    message
}

// ---------------------------------------------------------------------------
// Bitbucket Cloud (`bitbucketcloud_webhooks.go`)
// ---------------------------------------------------------------------------

/// `HandleBitbucketCloudWebhook`: body, optional `X-Hub-Signature` (go-github
/// HMAC), `X-Event-Key`, `bitbucketcloud.ParseWebhookEvent`, then `Dispatch`.
async fn bitbucket_cloud_response<B>(req: &mut Request<B>, secret: &str) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let body = match read_body(req).await {
        Ok(body) => body,
        Err(_error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error while reading request body.",
            )
        }
    };

    if !secret.is_empty() {
        let signature = header(req, X_HUB_SIGNATURE);
        if validate_github_signature(signature, &body, secret.as_bytes()).is_err() {
            return http_error(StatusCode::BAD_REQUEST, "Could not validate payload with secret.");
        }
    }

    let event_key = header(req, X_EVENT_KEY).to_owned();
    match parse_bitbucket_cloud_event(&event_key, &body) {
        Ok(()) => empty_ok(),
        Err(BitbucketCloudParseError::UnknownKey) => http_error(
            StatusCode::NOT_FOUND,
            &format!("unknown webhook event key: {event_key}"),
        ),
        Err(BitbucketCloudParseError::Json(text)) => {
            http_error(StatusCode::INTERNAL_SERVER_ERROR, &text)
        }
    }
}

enum BitbucketCloudParseError {
    /// `UnknownWebhookEventKey`: the handler answers 404.
    UnknownKey,
    /// Any `json.Unmarshal` error into the typed event: 500 with the raw text.
    Json(String),
}

/// `bitbucketcloud.ParseWebhookEvent`'s key switch.
fn parse_bitbucket_cloud_event(
    event_key: &str,
    payload: &[u8],
) -> Result<(), BitbucketCloudParseError> {
    match event_key {
        "pullrequest:approved"
        | "pullrequest:changes_request_created"
        | "pullrequest:changes_request_removed"
        | "pullrequest:comment_created"
        | "pullrequest:comment_deleted"
        | "pullrequest:comment_updated"
        | "pullrequest:fulfilled"
        | "pullrequest:rejected"
        | "pullrequest:unapproved"
        | "pullrequest:updated"
        | "repo:commit_status_created"
        | "repo:commit_status_updated"
        | "repo:push" => match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(_) => Ok(()),
            Err(error) => Err(BitbucketCloudParseError::Json(go_json_syntax_error(
                payload, &error,
            ))),
        },
        _ => Err(BitbucketCloudParseError::UnknownKey),
    }
}

// ---------------------------------------------------------------------------
// Azure DevOps (`azuredevops_webhooks.go`)
// ---------------------------------------------------------------------------

/// `HandleAzureDevOpsWebhook`: no secret/signature check, `BaseEvent` parse,
/// `azuredevops.ParseWebhookEvent`, then `Dispatch(event.EventType)`.
async fn azure_devops_response<B>(req: &mut Request<B>) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let body = match read_body(req).await {
        Ok(body) => body,
        Err(_error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error while reading request body.",
            )
        }
    };

    // The Go handler discards the concrete unmarshal error and always answers
    // with the same message.
    let event_type = match azure_event_type(&body) {
        Ok(event_type) => event_type,
        Err(_error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error while reading request body.",
            )
        }
    };

    match event_type.as_str() {
        "git.pullrequest.merged" | "git.pullrequest.updated" => {
            match azure_typed_event(&event_type, &body) {
                Ok(()) => empty_ok(),
                Err(text) => http_error(StatusCode::INTERNAL_SERVER_ERROR, &text),
            }
        }
        // `webhookNotFoundErr` implements `NotFound()`, so the handler answers
        // 404 with the literal message.
        _ => http_error(StatusCode::NOT_FOUND, "webhook not found"),
    }
}

/// `json.Unmarshal(payload, &azuredevops.BaseEvent)`.
fn azure_event_type(payload: &[u8]) -> Result<String, String> {
    use serde_json::Value;

    let value: Value = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
    match value {
        Value::Null => Ok(String::new()),
        Value::Object(map) => match map.get("eventType") {
            None | Some(Value::Null) => Ok(String::new()),
            Some(Value::String(event_type)) => Ok(event_type.clone()),
            Some(_) => Err("eventType is not a string".to_owned()),
        },
        _ => Err("BaseEvent is not an object".to_owned()),
    }
}

/// `azuredevops.ParseWebhookEvent`'s typed unmarshal. The simulated event
/// subtype (`message.text`) does not affect the dispatch outcome: none of the
/// derived types has a handler, so every successful parse is a 200 no-op.
fn azure_typed_event(event_type: &str, payload: &[u8]) -> Result<(), String> {
    use serde_json::Value;

    let value: Value = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
    match value {
        // `json.Unmarshal(null, &PullRequest*Event)` is a no-op.
        Value::Null | Value::Object(_) => {
            let _ = event_type;
            Ok(())
        }
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type azuredevops.PullRequestEvent",
            go_json_value_kind(&other)
        )),
    }
}

/// `subtle.ConstantTimeCompare(a, b) == 1`.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

/// A 204 response with a body (`w.WriteHeader(204)` + `fmt.Fprintf`). The
/// recorder the oracle uses captures the body even for 204.
fn no_content(body: String) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = StatusCode::NO_CONTENT;
    *response.headers_mut() = plain_headers();
    response
}

async fn read_body<B>(req: &mut Request<B>) -> Result<Vec<u8>, String>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    use http_body_util::BodyExt;
    let body = req
        .body_mut()
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    Ok(body.to_bytes().to_vec())
}

/// `github.ValidatePayloadFromBody`: content-type switch then HMAC validation.
fn validate_payload<B>(req: &Request<B>, body: &[u8], secret: &[u8]) -> Result<Vec<u8>, String> {
    let signature = {
        let sha256 = header(req, X_HUB_SIGNATURE_256);
        if sha256.is_empty() {
            header(req, X_HUB_SIGNATURE)
        } else {
            sha256
        }
    };
    let media_type = parse_media_type(header(req, "content-type"))?;
    let payload: Vec<u8> = match media_type.as_str() {
        "application/json" => body.to_vec(),
        "application/x-www-form-urlencoded" => url::form_urlencoded::parse(body)
            .find(|(key, _)| key == "payload")
            .map(|(_, value)| value.into_owned().into_bytes())
            .unwrap_or_default(),
        other => {
            return Err(format!(
                "webhook request has unsupported Content-Type {other:?}"
            ))
        }
    };
    validate_github_signature(signature, body, secret)?;
    Ok(payload)
}

/// `mime.ParseMediaType`: the lower-cased type before the first `;`.
fn parse_media_type(raw: &str) -> Result<String, String> {
    let media_type = raw
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if media_type.is_empty() {
        return Err("mime: no media type".to_owned());
    }
    Ok(media_type)
}

/// `HandleWebhook`: event-type lookup, payload parse and dispatch.
fn handle_github_event<B>(
    req: &Request<B>,
    payload: &[u8],
    deps: &dyn InboundDeps,
    code_host_urn: &str,
) -> Response<BoxBody> {
    let event = header(req, X_GITHUB_EVENT);
    match event {
        "installation" => {
            let parsed: InstallationEvent = match serde_json::from_slice(payload) {
                Ok(parsed) => parsed,
                Err(error) => {
                    // go-github returns the encoding/json error text; the
                    // corpus avoids malformed payloads.
                    return http_error(StatusCode::BAD_REQUEST, &json_error_text(payload, &error));
                }
            };
            if let Err(error) = handle_installation(parsed, deps) {
                return http_error(StatusCode::INTERNAL_SERVER_ERROR, &error);
            }
            empty_ok()
        }
        // Known go-github event types with no registered handler (the baseline
        // registers only `ping` and `installation`).
        "installation_repositories" | "ping" => {
            if let Err(error) = serde_json::from_slice::<serde_json::Value>(payload) {
                return http_error(StatusCode::BAD_REQUEST, &json_error_text(payload, &error));
            }
            empty_ok()
        }
        // M9.44b: `webhookhandlers.Init` registration. `organization` parses
        // but has no handler; the others land under bogus code-host keys (see
        // `webhook_authz_handlers` errata), so `dispatch_github` is a no-op.
        // `public` is only owned by the old handler.
        "public" => {
            let parsed = match parse_event("public", payload) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return http_error(StatusCode::BAD_REQUEST, &error.message());
                }
            };
            if let Some(authz) = deps.authz_deps() {
                if let Err(error) = dispatch_github("public", &parsed, authz) {
                    let status = if error.is_not_found() {
                        StatusCode::NOT_FOUND
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    };
                    return http_error(status, error.message());
                }
            }
            empty_ok()
        }
        // M9.45b: events registered by `PermissionsGitHubWebhook`. The old
        // handlers for these (except `team`) are inert, so only the newer
        // handler has side effects. Errors are swallowed by the handler, so
        // the response is 200 after a successful parse.
        authz_event @ ("repository" | "member" | "team_add" | "organization"
        | "membership" | "team") => {
            if authz_event != "team" {
                // Preserve the old handler's parse-error surface first.
                let parsed = match parse_event(authz_event, payload) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return http_error(StatusCode::BAD_REQUEST, &error.message());
                    }
                };
                if let Some(authz) = deps.authz_deps() {
                    let _ = dispatch_github(authz_event, &parsed, authz);
                }
            }
            let parsed = match permissions_webhook::parse_event(authz_event, payload) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return http_error(StatusCode::BAD_REQUEST, &error.message());
                }
            };
            if let Some(perms) = deps.permissions_webhook_deps() {
                let _ = permissions_webhook::handle_event(&parsed, code_host_urn, perms);
            }
            empty_ok()
        }
        other => http_error(
            StatusCode::BAD_REQUEST,
            &format!("unknown X-Github-Event in message: {other}"),
        ),
    }
}

/// Best-effort encoding/json error text (only reached for malformed payloads).
fn json_error_text(payload: &[u8], error: &serde_json::Error) -> String {
    if payload.is_empty() {
        "unexpected end of JSON input".to_owned()
    } else {
        error.to_string()
    }
}

/// `handleInstallation` (`webhooks.go:65-103`).
fn handle_installation(event: InstallationEvent, deps: &dyn InboundDeps) -> Result<(), String> {
    let installation = event.installation.unwrap_or_default();
    let html_url = installation.html_url.unwrap_or_default();
    let base_url = installation_base_url(&html_url)?;

    let app_id = installation.app_id.unwrap_or(0) as i32;
    let app = deps
        .get_app_by_app_id(app_id, &base_url)?
        .ok_or_else(|| {
            format!(
                "no app exists matching criteria: 'app_id = {app_id} AND trim(trailing '/' from base_url) = {}'",
                base_url.trim_end_matches('/')
            )
        })?;

    let account = installation.account.unwrap_or_default();
    let account_login = account.login.unwrap_or_default();
    deps.install(&InboundInstall {
        app_id: app.id,
        installation_id: installation.id.unwrap_or(0) as i32,
        url: html_url,
        account_login: account_login.clone(),
        account_avatar_url: account.avatar_url.unwrap_or_default(),
        account_url: account.html_url.unwrap_or_default(),
        account_type: account.account_type.unwrap_or_default(),
    })?;

    // `handleInstallation`: after the install upsert, credential-kind apps
    // create their batches credential (`handleCredentialCreation`).
    if app.kind == KIND_USER_CREDENTIAL || app.kind == KIND_SITE_CREDENTIAL {
        let external_service_url = external_service_url(&app.base_url)?;
        if app.kind == KIND_USER_CREDENTIAL {
            deps.create_user_credential(&app, &external_service_url)?;
        } else {
            deps.create_site_credential(&app, &external_service_url)?;
        }
    }

    Ok(())
}

/// `url.JoinPath(app.BaseURL, "/")`: always a trailing slash. The baseline
/// wraps a failure with `Unexpected error while creating external service url
/// for github app: <err>`.
fn external_service_url(base_url: &str) -> Result<String, String> {
    match url::Url::parse(base_url) {
        Ok(mut parsed) => {
            if !parsed.path().ends_with('/') {
                let path = format!("{}/", parsed.path());
                parsed.set_path(&path);
            }
            Ok(parsed.to_string())
        }
        Err(error) => Err(format!(
            "Unexpected error while creating external service url for github app: {error}"
        )),
    }
}

/// `url.Parse(htmlURL)` + `fmt.Sprintf("%s://%s/", Scheme, Host)`.
fn installation_base_url(html_url: &str) -> Result<String, String> {
    if html_url.is_empty() {
        // `url.Parse("")` succeeds with an empty URL; the sprintf yields ":///".
        return Ok(":///".to_owned());
    }
    let parsed = url::Url::parse(html_url).map_err(|error| format!("{error}"))?;
    let authority = match parsed.port() {
        Some(port) => format!("{}:{port}", parsed.host_str().unwrap_or("")),
        None => parsed.host_str().unwrap_or("").to_owned(),
    };
    Ok(format!("{}://{authority}/", parsed.scheme()))
}

/// `method` guard: the generic webhook route is POST-only.
pub fn method_allowed(method: &Method) -> bool {
    method == Method::POST
}

/// HMAC-SHA256 hex digest (one of the go-github signature prefixes).
pub fn hmac_sha256_hex(secret: &[u8], body: &[u8]) -> String {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key");
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// HMAC-SHA1 hex digest (`sha1=` prefix).
pub fn hmac_sha1_hex(secret: &[u8], body: &[u8]) -> String {
    use hmac::Mac;
    let mut mac =
        hmac::Hmac::<sha1::Sha1>::new_from_slice(secret).expect("hmac accepts any key");
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// HMAC-SHA512 hex digest (`sha512=` prefix).
pub fn hmac_sha512_hex(secret: &[u8], body: &[u8]) -> String {
    use hmac::Mac;
    let mut mac =
        hmac::Hmac::<sha2::Sha512>::new_from_slice(secret).expect("hmac accepts any key");
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Build a `X-Hub-Signature-256`-style header value from a corpus algorithm
/// tag. Shared by the differential harness so the Go oracle and the Rust
/// adapter sign identically.
pub fn sign(algorithm: &str, secret: &[u8], body: &[u8]) -> Option<String> {
    match algorithm {
        "sha256" => Some(format!(
            "sha256={}",
            hmac_sha256_hex(secret, body)
        )),
        "sha1" => Some(format!("sha1={}", hmac_sha1_hex(secret, body))),
        "sha512" => Some(format!("sha512={}", hmac_sha512_hex(secret, body))),
        "wrong_sha256" => {
            let mut key = secret.to_vec();
            key.extend_from_slice(b"-wrong");
            Some(format!("sha256={}", hmac_sha256_hex(&key, body)))
        }
        "wrong_sha1" => {
            let mut key = secret.to_vec();
            key.extend_from_slice(b"-wrong");
            Some(format!("sha1={}", hmac_sha1_hex(&key, body)))
        }
        "malformed" => Some("sha256=zzzz".to_owned()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Production adapter
// ---------------------------------------------------------------------------

/// `database.Webhooks(...)` + `database.GitHubApps()` backed by Postgres.
pub struct PgInboundDeps {
    pool: sqlx::PgPool,
    keyring: std::sync::Arc<sg_keyring::Keyring>,
    authz: PgAuthzDeps,
    perms_webhook: PgPermissionsWebhookDeps,
}

impl PgInboundDeps {
    pub fn new(pool: sqlx::PgPool, keyring: std::sync::Arc<sg_keyring::Keyring>) -> Self {
        let authz = PgAuthzDeps::new(pool.clone());
        let perms_webhook = PgPermissionsWebhookDeps::new(pool.clone());
        Self {
            pool,
            keyring,
            authz,
            perms_webhook,
        }
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("githubapp inbound blocking runtime")
            .block_on(future),
    }
}

impl InboundDeps for PgInboundDeps {
    fn authz_deps(&self) -> Option<&dyn AuthzDeps> {
        Some(&self.authz)
    }

    fn permissions_webhook_deps(&self) -> Option<&dyn PermissionsWebhookDeps> {
        Some(&self.perms_webhook)
    }

    fn lookup_webhook(&self, uuid: &str) -> WebhookLookup {
        use sqlx::Row;
        let row = match block_on(
            sqlx::query(
                "SELECT code_host_kind, code_host_urn, secret, encryption_key_id \
                 FROM webhooks WHERE uuid = $1::uuid",
            )
            .bind(uuid)
            .fetch_optional(&self.pool),
        ) {
            Ok(row) => row,
            Err(_) => return WebhookLookup::NotFound,
        };
        let Some(row) = row else {
            return WebhookLookup::NotFound;
        };
        let code_host_kind: String = row.try_get("code_host_kind").unwrap_or_default();
        let code_host_urn: String = row.try_get("code_host_urn").unwrap_or_default();
        let raw_secret: Option<String> = row.try_get("secret").unwrap_or(None);
        let key_id: Option<String> = row.try_get("encryption_key_id").unwrap_or(None);
        let secret = match (key_id.as_deref().unwrap_or(""), raw_secret) {
            ("", Some(raw)) if !raw.is_empty() => Some(raw),
            ("", _) => None,
            (key_id, Some(raw)) if !raw.is_empty() => {
                let key = self.keyring.webhook_key();
                match block_on(sg_encryption::maybe_decrypt(
                    key.as_deref(),
                    &raw,
                    key_id,
                )) {
                    Ok(value) => Some(value),
                    Err(error) => return WebhookLookup::DecryptError(error.to_string()),
                }
            }
            _ => None,
        };
        WebhookLookup::Found(InboundWebhook {
            code_host_kind,
            code_host_urn,
            secret,
        })
    }

    fn get_app_by_app_id(
        &self,
        app_id: i32,
        base_url: &str,
    ) -> Result<Option<InboundApp>, String> {
        use sqlx::Row;
        let row = block_on(
            sqlx::query(
                "SELECT id, app_id, kind::text AS kind, base_url, creator_id::int4 AS creator_id \
                 FROM github_apps \
                 WHERE app_id = $1 AND trim(trailing '/' from base_url) = $2 \
                 LIMIT 1",
            )
            .bind(app_id)
            .bind(base_url.trim_end_matches('/'))
            .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        Ok(row.map(|row| InboundApp {
            id: row.try_get("id").unwrap_or_default(),
            app_id: row.try_get("app_id").unwrap_or_default(),
            kind: row.try_get("kind").unwrap_or_default(),
            base_url: row.try_get("base_url").unwrap_or_default(),
            created_by_user_id: row.try_get("creator_id").unwrap_or_default(),
        }))
    }

    fn install(&self, install: &InboundInstall) -> Result<(), String> {
        let row = sg_db::github_apps::GitHubAppInstallation {
            id: 0,
            app_id: install.app_id,
            installation_id: install.installation_id,
            url: Some(install.url.clone()),
            account_login: Some(install.account_login.clone()),
            account_avatar_url: Some(install.account_avatar_url.clone()),
            account_url: Some(install.account_url.clone()),
            account_type: Some(install.account_type.clone()),
        };
        block_on(sg_db::github_apps::install(&self.pool, &row))
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn create_user_credential(
        &self,
        app: &InboundApp,
        external_service_url: &str,
    ) -> Result<(), String> {
        let wrap = |error: String| {
            format!("Unexpected error while creating user credential for github app: {error}")
        };
        // `store.New(db, ..., keyring.Default().BatchChangesCredentialKey)`.
        let store = PgBatchesGraphqlStore::new(self.pool.clone()).with_keyring(self.keyring.clone());
        let user_id = app.created_by_user_id;
        if store
            .get_user_credential_by_scope(user_id, GITHUB_APP_EXTERNAL_SERVICE_TYPE, external_service_url)
            .map_err(wrap)?
            .is_some()
        {
            return Err(wrap("a credential for this code host already exists".to_owned()));
        }
        // `MarshalAuthenticator(*ghauth.GitHubAppAuthenticator)`: the
        // authenticator's fields are unexported, so `Auth` is `{}`.
        let (credential, encryption_key_id) = store
            .encrypt_credential(GITHUB_APP_AUTHENTICATOR_JSON)
            .map_err(wrap)?;
        store
            .create_user_credential(
                user_id,
                GITHUB_APP_EXTERNAL_SERVICE_TYPE,
                external_service_url,
                &credential,
                &encryption_key_id,
                Some(i64::from(app.id)),
            )
            .map(|_| ())
            .map_err(wrap)
    }

    fn create_site_credential(
        &self,
        app: &InboundApp,
        external_service_url: &str,
    ) -> Result<(), String> {
        let wrap = |error: String| {
            format!("Unexpected error while creating site credential for github app: {error}")
        };
        let store = PgBatchesGraphqlStore::new(self.pool.clone()).with_keyring(self.keyring.clone());
        if store
            .get_site_credential_by_scope(GITHUB_APP_EXTERNAL_SERVICE_TYPE, external_service_url)
            .map_err(wrap)?
            .is_some()
        {
            return Err(wrap("a credential for this code host already exists".to_owned()));
        }
        // `SiteCredential.SetAuthenticator` returns early for GitHub Apps, so
        // the stored credential is empty and the key id is `''`.
        store
            .create_site_credential(
                GITHUB_APP_EXTERNAL_SERVICE_TYPE,
                external_service_url,
                &[],
                "",
                Some(i64::from(app.id)),
            )
            .map(|_| ())
            .map_err(wrap)
    }
}

/// `extsvc.VariantGitHub.AsType()`.
const GITHUB_APP_EXTERNAL_SERVICE_TYPE: &str = "github";

/// `MarshalAuthenticator(*ghauth.GitHubAppAuthenticator)`.
const GITHUB_APP_AUTHENTICATOR_JSON: &str = r#"{"Type":"GitHubApp","Auth":{}}"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_type_follows_go() {
        assert_eq!(
            parse_media_type("application/json; charset=utf-8").unwrap(),
            "application/json"
        );
        assert_eq!(
            parse_media_type("Application/X-WWW-Form-Urlencoded").unwrap(),
            "application/x-www-form-urlencoded"
        );
        assert!(parse_media_type("").is_err());
    }

    #[test]
    fn signature_round_trip() {
        let body = b"{\"installation\":{\"app_id\":501}}";
        let secret = b"s3cr3t";
        let signature = sign("sha256", secret, body).unwrap();
        assert_eq!(signature, format!("sha256={}", hmac_sha256_hex(secret, body)));
        assert!(validate_github_signature(&signature, body, secret).is_ok());
        assert!(validate_github_signature(&signature, body, b"other").is_err());
    }

    #[test]
    fn installation_origin_matches_go() {
        assert_eq!(
            installation_base_url("https://github.com/orgs/acme/installations/42").unwrap(),
            "https://github.com/"
        );
        assert_eq!(installation_base_url("").unwrap(), ":///");
    }
}
