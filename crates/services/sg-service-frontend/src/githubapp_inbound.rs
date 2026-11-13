#![forbid(unsafe_code)]

//! M9.33c: the inbound GitHub App installation webhook.
//!
//! Ports the `installation` path of the generic webhook route
//! (`/.api/webhooks/{webhook_uuid}`):
//!
//! * `cmd/frontend/internal/webhooks/webhooks.go` — `handler`, `Dispatch`
//!   (`defaultHandlers` registers `ping` and `installation` for `GITHUB`);
//! * `cmd/frontend/internal/webhooks/github_webhooks.go` — secret branch,
//!   `github.ValidatePayload`, `github.ParseWebHook`;
//! * `internal/github_apps/store` — `GetByAppID` and the
//!   `github_app_installs` upsert (`Install`).
//!
//! No new table, column or Redis key: the existing `webhooks`,
//! `github_apps` and `github_app_installs` rows are reused, and the webhook
//! signature contract is the go-github v55 one (`X-Hub-Signature-256` then
//! `X-Hub-Signature`, HMAC-SHA1/256/512 over the raw body).
//!
//! Errata (source wins): the baseline registers **only** `installation`. An
//! `installation_repositories` event parses (it is a known go-github event)
//! but dispatches to no handler, which is an HTTP 200 no-op; this module
//! reproduces that.

use bytes::Bytes;
use http::header::HeaderValue;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use serde::Deserialize;
use sha2::Sha256;
use tonic::body::BoxBody;

use crate::external_api::{go_quote, validate_github_signature};

/// The generic webhook mount (`webhooks.NewHandler`).
pub const WEBHOOKS_PREFIX: &str = "/.api/webhooks/";

const X_HUB_SIGNATURE_256: &str = "x-hub-signature-256";
const X_HUB_SIGNATURE: &str = "x-hub-signature";
const X_GITHUB_EVENT: &str = "x-github-event";

/// `extsvc.KindGitHub`.
const KIND_GITHUB: &str = "GITHUB";

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
    pub kind: String,
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

    if webhook.code_host_kind != KIND_GITHUB {
        return http_error(
            StatusCode::NOT_IMPLEMENTED,
            &format!(
                "webhooks not implemented for code host kind {}",
                go_quote(&webhook.code_host_kind)
            ),
        );
    }

    // `handleGitHubWebHook`: an empty secret skips signature validation.
    let secret = webhook.secret.as_deref().unwrap_or("");
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

    handle_github_event(req, &payload, deps)
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
    deps.install(&InboundInstall {
        app_id: app.id,
        installation_id: installation.id.unwrap_or(0) as i32,
        url: html_url,
        account_login: account.login.unwrap_or_default(),
        account_avatar_url: account.avatar_url.unwrap_or_default(),
        account_url: account.html_url.unwrap_or_default(),
        account_type: account.account_type.unwrap_or_default(),
    })?;
    Ok(())
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
}

impl PgInboundDeps {
    pub fn new(pool: sqlx::PgPool, keyring: std::sync::Arc<sg_keyring::Keyring>) -> Self {
        Self { pool, keyring }
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
                "SELECT id, kind::text AS kind FROM github_apps \
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
            kind: row.try_get("kind").unwrap_or_default(),
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
}

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
