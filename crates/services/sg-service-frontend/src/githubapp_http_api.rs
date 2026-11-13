#![forbid(unsafe_code)]

//! `cmd/frontend/internal/githubapp/httpapi.go`: the GitHub App
//! install/create HTTP API mounted at `/githubapp/{state,new-app-state,redirect,setup}`.
//!
//! The four handlers are synchronous, exactly like the gorilla/mux baseline,
//! and every external effect goes through a seam so the differential harness
//! can replay the same corpus offline:
//!
//! * [`StateStore`] — the production `rcache.Cache` (`github_app_state`, 1h TTL);
//! * [`GitHubAppStore`] — `GitHubApps().Create` / `GetByID` with the baseline
//!   domain/kind validation and the batches `COMMIT_SIGNING` uniqueness check;
//! * [`WebhookBackend`] — `backend.NewWebhookService(...).CreateWebhook` and
//!   `Webhooks(keyring.WebhookKey).GetByUUID/Update`;
//! * [`HttpDoer`] — the manifest-conversion `POST`;
//! * [`Sleeper`] — the unconditional 3s wait before the install redirect;
//! * [`StateGenerator`] — `randomState`;
//! * [`SiteAdminCheck`] — `siteAdminMiddleware`.
//!
//! Errors and response bodies follow Go (`http.Error` appends `\n`; the relay
//! ID helpers reproduce graph-gophers' `relay` text).

use std::time::Duration;

use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tonic::body::BoxBody;

use crate::app_handlers;

const CACHE_TTL_SECONDS: i64 = 60 * 60;
/// The production `rcache` key prefix (`github_app_state`).
pub const STATE_CACHE_PREFIX: &str = "github_app_state";

/// `extsvc.KindGitHub` (`internal/extsvc/types.go:225,290`): the
/// `code_host_kind` the webhook service sees. M9.31d passed the wire *type*
/// `"github"` here; the baseline passes the **kind** `"GITHUB"`, and the store
/// validates it (`validateCodeHostKindAndSecret`). Errata recorded in the
/// overview: the production adapter would reject `"github"`.
pub const CODE_HOST_KIND_GITHUB: &str = "GITHUB";

/// One `/githubapp/` subroute (`httpapi.go:64-68`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GitHubAppRoute {
    State,
    NewAppState,
    Redirect,
    Setup,
}

/// `githubapp/httpapi.go:64-68` path table.
pub fn route(path: &str) -> Option<GitHubAppRoute> {
    match path {
        "/githubapp/state" => Some(GitHubAppRoute::State),
        "/githubapp/new-app-state" => Some(GitHubAppRoute::NewAppState),
        "/githubapp/redirect" => Some(GitHubAppRoute::Redirect),
        "/githubapp/setup" => Some(GitHubAppRoute::Setup),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn text_plain_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers
}

/// `http.Error` (`net/http/server.go`): body `<message>\n`, `text/plain`,
/// `X-Content-Type-Options: nosniff`.
fn http_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(format!(
        "{message}\n"
    )))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn text_response(status: StatusCode, body: String) -> Response<BoxBody> {
    let mut response =
        Response::new(tonic::body::boxed(Full::new(Bytes::from(body.into_bytes()))));
    *response.status_mut() = status;
    *response.headers_mut() = text_plain_headers();
    response
}

/// `http.Redirect` (`net/http/server.go:2281`).
fn redirect<B>(req: &Request<B>, location: &str, code: u16) -> Response<BoxBody> {
    app_handlers::redirect(req, location, code)
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// `rcache.Cache` (`Get`/`Set`/`Delete`).
pub trait StateStore: Send + Sync {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: Vec<u8>);
    fn delete(&self, key: &str);
}

/// `GitHubApps()` (`internal/github_apps/store`).
pub trait GitHubAppStore: Send + Sync {
    /// `gitHubAppsStore.Create`: returns the new `github_apps.id`.
    fn create(&self, app: &NewGitHubApp) -> Result<i32, String>;
    /// `gitHubAppsStore.GetByID`; a miss is
    /// `no app exists matching criteria: 'id = <id>'`.
    fn get_by_id(&self, id: i32) -> Result<StoredApp, String>;
}

/// `ghtypes.GitHubApp` fields `Create`/`GetByID` need.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredApp {
    pub id: i32,
    pub name: String,
}

/// The values `createGitHubApp` puts into `ghtypes.GitHubApp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewGitHubApp {
    pub app_id: i32,
    pub name: String,
    pub slug: String,
    pub client_id: String,
    pub client_secret: String,
    pub webhook_secret: String,
    pub private_key: String,
    pub base_url: String,
    pub app_url: String,
    pub domain: String,
    pub kind: String,
    pub logo: String,
    pub created_by_user_id: i32,
}

/// `backend.WebhookService` + `Webhooks(...)` store.
pub trait WebhookBackend: Send + Sync {
    /// `CreateWebhook(ctx, name, "github", urn, nil)`; returns `hook.UUID.String()`.
    fn create(&self, name: &str, kind: &str, urn: &str) -> Result<String, String>;
    /// `Webhooks.GetByUUID`; returns the row used by the secret/name update.
    fn get_by_uuid(&self, uuid: &str) -> Result<WebhookRecord, String>;
    /// `Webhooks.Update`.
    fn update(&self, record: &WebhookRecord) -> Result<(), String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookRecord {
    pub uuid: String,
    pub name: String,
    pub secret: Option<String>,
}

/// The manifest-conversion `POST` (`httpcli.UncachedExternalClient`).
pub trait HttpDoer: Send + Sync {
    /// Returns `(status_code, body)`.
    fn post(&self, url: &str) -> Result<(u16, Vec<u8>), String>;
}

/// `time.Sleep(3 * time.Second)`.
pub trait Sleeper: Send + Sync {
    fn sleep(&self, duration: Duration);
}

/// `randomState` (`httpapi.go:95-106`).
pub trait StateGenerator: Send + Sync {
    fn generate(&self) -> Result<String, String>;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SiteAdminOutcome {
    Allowed,
    Forbidden,
    Unauthenticated,
    Error,
}

/// The request actor `siteAdminMiddleware` reads. The Go middleware runs after
/// the session middleware has put `actor.FromContext` on the request; the
/// synchronous port carries the same information explicitly. An internal actor
/// bypasses the check (`auth.CheckCurrentUserIsSiteAdmin`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum ActorIdentity {
    /// `actor.FromContext(ctx)` with no user (`UID == 0`).
    #[default]
    Anonymous,
    /// `actor.Actor{Internal: true}`.
    Internal,
    /// An authenticated user (`UID`).
    User(i32),
}

impl ActorIdentity {
    /// `actor.FromContext(ctx)` (as exposed to the app router) reduced to the
    /// fields the middleware reads.
    pub fn from_actor(actor: sg_graphql::exec::Actor) -> Self {
        if actor.is_internal() {
            Self::Internal
        } else if let Some(uid) = actor.uid() {
            Self::User(uid)
        } else {
            Self::Anonymous
        }
    }
}

/// `siteAdminMiddleware` (`httpapi.go:37-58`).
pub trait SiteAdminCheck: Send + Sync {
    /// Actor-less outcome (offline / allow-all seams).
    fn check(&self) -> SiteAdminOutcome;

    /// Actor-aware outcome. The default ignores the actor so the historical
    /// seams keep their behavior.
    fn check_actor(&self, _actor: ActorIdentity) -> SiteAdminOutcome {
        self.check()
    }
}

// ---------------------------------------------------------------------------
// Default seams
// ---------------------------------------------------------------------------

/// `crypto/rand` 128 bytes → SHA-256 → 64 lowercase hex.
pub struct SystemStateGenerator;

impl StateGenerator for SystemStateGenerator {
    fn generate(&self) -> Result<String, String> {
        random_state()
    }
}

pub fn random_state() -> Result<String, String> {
    use rand::RngCore;
    let mut data = [0u8; 128];
    rand::rngs::OsRng.fill_bytes(&mut data);
    Ok(hex_sha256(&data))
}

/// The body of `randomState`: SHA-256 of `data` as lowercase hex.
pub fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub struct RealSleeper;

impl Sleeper for RealSleeper {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

pub struct AllowAllSiteAdmin;

impl SiteAdminCheck for AllowAllSiteAdmin {
    fn check(&self) -> SiteAdminOutcome {
        SiteAdminOutcome::Allowed
    }
}

/// A cache that never contains the requested state (the offline UI path).
pub struct MissStateStore;

impl StateStore for MissStateStore {
    fn get(&self, _key: &str) -> Option<Vec<u8>> {
        None
    }
    fn set(&self, _key: &str, _value: Vec<u8>) {}
    fn delete(&self, _key: &str) {}
}

/// In-memory `rcache.Cache` adapter for tests and embedders.
#[derive(Default)]
pub struct MemoryStateStore {
    entries: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateStore for MemoryStateStore {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries.lock().unwrap().get(key).cloned()
    }
    fn set(&self, key: &str, value: Vec<u8>) {
        self.entries.lock().unwrap().insert(key.to_owned(), value);
    }
    fn delete(&self, key: &str) {
        self.entries.lock().unwrap().remove(key);
    }
}

/// Production adapter over the existing `rcache` `github_app_state` cache.
pub struct RcacheStateStore {
    cache: sg_rcache::Cache,
}

impl RcacheStateStore {
    pub fn new(cache: sg_rcache::Cache) -> Self {
        Self { cache }
    }

    /// The crate's own constructor: `rcache.NewWithTTL(Cache, "github_app_state", 3600)`.
    pub fn with_kv(kv: sg_redis::KeyValue) -> Self {
        Self::new(sg_rcache::Cache::new_with_ttl(
            kv,
            STATE_CACHE_PREFIX,
            CACHE_TTL_SECONDS,
        ))
    }
}

impl StateStore for RcacheStateStore {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        block_on(self.cache.get(key)).ok().flatten()
    }
    fn set(&self, key: &str, value: Vec<u8>) {
        let _ = block_on(self.cache.set(key, value));
    }
    fn delete(&self, key: &str) {
        let _ = block_on(self.cache.delete(key));
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("githubapp blocking runtime")
            .block_on(future),
    }
}

/// `githubapp/httpapi.go`'s offline UI branches: no Redis and no DB.
pub struct OfflineWebhooks;

impl WebhookBackend for OfflineWebhooks {
    fn create(&self, _name: &str, _kind: &str, _urn: &str) -> Result<String, String> {
        Err("external service setup is not available".to_owned())
    }
    fn get_by_uuid(&self, uuid: &str) -> Result<WebhookRecord, String> {
        Err(format!("webhook {uuid} not found"))
    }
    fn update(&self, _record: &WebhookRecord) -> Result<(), String> {
        Err("webhook store is not available".to_owned())
    }
}

pub struct OfflineHttpDoer;

impl HttpDoer for OfflineHttpDoer {
    fn post(&self, _url: &str) -> Result<(u16, Vec<u8>), String> {
        Err("external HTTP is not available".to_owned())
    }
}

/// In-memory `GitHubApps()` implementing the baseline `Create` validation.
#[derive(Default)]
pub struct MemoryGitHubApps {
    apps: std::sync::Mutex<Vec<MemoryApp>>,
    next_id: std::sync::Mutex<i32>,
}

#[derive(Clone, Debug)]
pub struct MemoryApp {
    pub id: i32,
    pub name: String,
    pub domain: String,
    pub kind: String,
    pub base_url: String,
}

impl MemoryGitHubApps {
    pub fn new() -> Self {
        Self {
            apps: std::sync::Mutex::new(Vec::new()),
            next_id: std::sync::Mutex::new(1),
        }
    }

    /// Seed an existing row (the differential's fixtures).
    pub fn insert(&self, id: i32, name: &str, domain: &str, kind: &str, base_url: &str) {
        let mut apps = self.apps.lock().unwrap();
        apps.push(MemoryApp {
            id,
            name: name.to_owned(),
            domain: domain.to_owned(),
            kind: kind.to_owned(),
            base_url: base_url.to_owned(),
        });
        let mut next = self.next_id.lock().unwrap();
        if id >= *next {
            *next = id + 1;
        }
    }

    fn find_by_domain_kind(&self, domain: &str, kind: &str, base_url: &str) -> Option<MemoryApp> {
        self.apps
            .lock()
            .unwrap()
            .iter()
            .find(|app| app.domain == domain && app.kind == kind && app.base_url == base_url)
            .cloned()
    }
}

impl GitHubAppStore for MemoryGitHubApps {
    fn create(&self, app: &NewGitHubApp) -> Result<i32, String> {
        let domain = if app.domain.is_empty() {
            "repos".to_owned()
        } else {
            app.domain.clone()
        };

        // `url.Parse` then `extsvc.NormalizeBaseURL`: lowercase host, trailing `/`.
        let base_url = match normalize_base_url(&app.base_url) {
            Ok(url) => url,
            Err(_) => return Err("unable to parse base URL: ".to_owned()),
        };

        let kind = if app.kind.is_empty() {
            "REPO_SYNC".to_owned()
        } else if !valid_kind(&app.kind) {
            return Err(format!("The GitHubAppKind {} is not valid.", app.kind));
        } else {
            app.kind.clone()
        };

        if domain == "batches" && kind == "COMMIT_SIGNING" {
            if self.find_by_domain_kind(&domain, &kind, &base_url).is_some() {
                // Baseline bug: `GetByDomainAndKind` returns `(app, nil)` on a
                // hit; the `err.(ErrNoGitHubAppFound)` assertion then fails and
                // `errors.Wrap(nil, ...)` is nil, so `Create` returns `(-1, nil)`
                // without inserting the duplicate. Confirmed by the oracle.
                return Ok(-1);
            }
        }

        let mut next = self.next_id.lock().unwrap();
        let id = *next;
        *next += 1;
        self.apps.lock().unwrap().push(MemoryApp {
            id,
            name: app.name.clone(),
            domain,
            kind,
            base_url,
        });
        Ok(id)
    }

    fn get_by_id(&self, id: i32) -> Result<StoredApp, String> {
        self.apps
            .lock()
            .unwrap()
            .iter()
            .find(|app| app.id == id)
            .map(|app| StoredApp {
                id: app.id,
                name: app.name.clone(),
            })
            .ok_or_else(|| format!("no app exists matching criteria: 'id = {id}'"))
    }
}

/// In-memory webhooks store.
#[derive(Default)]
pub struct MemoryWebhooks {
    records: std::sync::Mutex<Vec<WebhookRecord>>,
}

impl MemoryWebhooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, uuid: &str, name: &str) {
        self.records.lock().unwrap().push(WebhookRecord {
            uuid: uuid.to_owned(),
            name: name.to_owned(),
            secret: None,
        });
    }
}

impl WebhookBackend for MemoryWebhooks {
    fn create(&self, name: &str, _kind: &str, _urn: &str) -> Result<String, String> {
        let uuid = "33333333-3333-3333-3333-333333333333".to_owned();
        self.records.lock().unwrap().push(WebhookRecord {
            uuid: uuid.clone(),
            name: name.to_owned(),
            secret: None,
        });
        Ok(uuid)
    }

    fn get_by_uuid(&self, uuid: &str) -> Result<WebhookRecord, String> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.uuid == uuid)
            .cloned()
            .ok_or_else(|| format!("error while fetching webhook: no webhook found with UUID {uuid}"))
    }

    fn update(&self, record: &WebhookRecord) -> Result<(), String> {
        let mut records = self.records.lock().unwrap();
        match records.iter_mut().find(|row| row.uuid == record.uuid) {
            Some(row) => {
                *row = record.clone();
                Ok(())
            }
            None => Err(format!("webhook {} not found", record.uuid)),
        }
    }
}

// ---------------------------------------------------------------------------
// Domain / kind / URL helpers
// ---------------------------------------------------------------------------

pub fn parse_domain(value: Option<&str>) -> Result<Option<&'static str>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.to_ascii_uppercase().as_str() {
        "REPOS" => Ok(Some("repos")),
        "BATCHES" => Ok(Some("batches")),
        _ => Err(format!("unknown domain {value:?}")),
    }
}

pub fn parse_kind(value: Option<&str>) -> Result<Option<&'static str>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.to_ascii_uppercase().as_str() {
        "SITE_CREDENTIAL" => Ok(Some("SITE_CREDENTIAL")),
        "USER_CREDENTIAL" => Ok(Some("USER_CREDENTIAL")),
        "COMMIT_SIGNING" => Ok(Some("COMMIT_SIGNING")),
        "REPO_SYNC" => Ok(Some("REPO_SYNC")),
        _ => Err(format!("unknown kind {value:?}")),
    }
}

fn valid_kind(kind: &str) -> bool {
    matches!(
        kind,
        "SITE_CREDENTIAL" | "USER_CREDENTIAL" | "COMMIT_SIGNING" | "REPO_SYNC"
    )
}

/// `extsvc.NormalizeBaseURL(url.Parse(s))`.
fn normalize_base_url(raw: &str) -> Result<String, String> {
    let parsed = url::Url::parse(raw).map_err(|error| error.to_string())?;
    let mut normalized = parsed;
    if let Some(host) = normalized.host_str() {
        let lowered = host.to_ascii_lowercase();
        normalized.set_host(Some(&lowered)).map_err(|e| e.to_string())?;
    }
    let path = normalized.path();
    if !path.ends_with('/') {
        normalized.set_path(&format!("{path}/"));
    }
    Ok(normalized.to_string())
}

/// `github.APIRoot(baseURL)`: github.com→`https://api.github.com/`, an empty
/// or `/` path→`<base>/api/v3`, otherwise `<base>/api`.
fn api_root(base_url: &str) -> Result<String, String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Ok("/api/v3".to_owned());
    }
    let parsed = url::Url::parse(trimmed).map_err(|error| error.to_string())?;
    let hostname = parsed
        .host_str()
        .unwrap_or("")
        .to_ascii_lowercase();
    if hostname == "github.com" || hostname == "www.github.com" {
        return Ok("https://api.github.com/".to_owned());
    }
    let path = parsed.path();
    // `url.URL.ResolveReference` keeps the authority verbatim, including any
    // explicit port (M9.31d dropped it here, which breaks non-default ports).
    let authority = match parsed.port() {
        Some(port) => format!("{}:{port}", parsed.host_str().unwrap_or("")),
        None => parsed.host_str().unwrap_or("").to_owned(),
    };
    let base = format!(
        "{}://{}{}",
        parsed.scheme(),
        authority,
        if parsed.path().is_empty() { "/" } else { path }
    );
    if path.is_empty() || path == "/" {
        Ok(format!("{}/api/v3", base.trim_end_matches('/')))
    } else {
        Ok(format!("{}/api", base.trim_end_matches('/')))
    }
}

/// `url.JoinPath(base, elems...)`: returns a URL string, falling back to the
/// path-joined form for relative bases.
fn join_path(base: &str, elements: &[&str]) -> Result<String, String> {
    let mut parts: Vec<String> = Vec::new();
    let (prefix, base_path) = match url::Url::parse(base) {
        Ok(parsed) if parsed.scheme() == "http" || parsed.scheme() == "https" => {
            let mut prefix = format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""));
            if let Some(port) = parsed.port() {
                prefix = format!("{prefix}:{port}");
            }
            (prefix, parsed.path().to_owned())
        }
        _ => (String::new(), base.to_owned()),
    };
    if !base_path.is_empty() && base_path != "/" {
        parts.push(base_path.trim_matches('/').to_owned());
    }
    for element in elements {
        for segment in element.split('/') {
            if segment.is_empty() {
                continue;
            }
            parts.push(percent_encode_path(segment));
        }
    }
    let joined = parts.join("/");
    Ok(format!("{prefix}/{joined}"))
}

/// `url.PathEscape`-equivalent for a single path element.
fn percent_encode_path(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
            | b':' | b'@' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Go `url.QueryEscape`.
pub fn query_escape(value: &str) -> String {
    let mut escaped = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(byte as char)
            }
            b' ' => escaped.push('+'),
            _ => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
}

// ---------------------------------------------------------------------------
// Relay IDs (graph-gophers `relay`)
// ---------------------------------------------------------------------------

const GITHUB_APP_ID_KIND: &str = "GitHubApp";

/// Go's `base64.CorruptInputError`: `illegal base64 data at input byte N`.
fn go_base64_decode(raw: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    match base64::engine::general_purpose::URL_SAFE.decode(raw.as_bytes()) {
        Ok(bytes) => Ok(bytes),
        Err(_) => {
            let index = raw
                .bytes()
                .position(|byte| {
                    !(byte.is_ascii_alphanumeric()
                        || matches!(byte, b'-' | b'_' | b'='))
                })
                .unwrap_or_else(|| raw.len().saturating_sub(1));
            Err(format!("illegal base64 data at input byte {index}"))
        }
    }
}

fn unmarshal_kind(raw: &str) -> String {
    let Ok(decoded) = go_base64_decode(raw) else {
        return String::new();
    };
    let Ok(text) = String::from_utf8(decoded) else {
        return String::new();
    };
    match text.find(':') {
        Some(index) => text[..index].to_owned(),
        None => String::new(),
    }
}

fn unmarshal_spec_text(raw: &str) -> Result<String, String> {
    let decoded = go_base64_decode(raw)?;
    let text = String::from_utf8(decoded)
        .map_err(|_| "invalid graphql.ID".to_owned())?;
    match text.find(':') {
        Some(index) => Ok(text[index + 1..].to_owned()),
        None => Err("invalid graphql.ID".to_owned()),
    }
}

fn json_int_error(spec: &str, type_name: &str) -> String {
    let spec = spec.trim();
    if spec.starts_with('"') {
        return format!("json: cannot unmarshal string into Go value of type {type_name}");
    }
    if spec == "true" || spec == "false" {
        return format!("json: cannot unmarshal bool into Go value of type {type_name}");
    }
    if spec.starts_with('{') || spec.starts_with('[') {
        return format!(
            "json: cannot unmarshal {} into Go value of type {type_name}",
            if spec.starts_with('{') { "object" } else { "array" }
        );
    }
    match spec.parse::<f64>() {
        Ok(_) => format!("json: cannot unmarshal number {spec} into Go value of type {type_name}"),
        Err(_) => {
            let first = spec.chars().next().unwrap_or(' ');
            format!("invalid character '{first}' looking for beginning of value")
        }
    }
}

fn unmarshal_spec_int64(raw: &str) -> Result<i64, String> {
    let spec = unmarshal_spec_text(raw)?;
    spec.trim()
        .parse::<i64>()
        .map_err(|_| json_int_error(&spec, "int64"))
}

fn unmarshal_spec_int32(raw: &str) -> Result<i32, String> {
    let spec = unmarshal_spec_text(raw)?;
    spec.trim()
        .parse::<i32>()
        .map_err(|_| json_int_error(&spec, "int32"))
}

/// `githubapp.UnmarshalGitHubAppID`.
pub fn unmarshal_github_app_id(raw: &str) -> Result<i64, String> {
    let kind = unmarshal_kind(raw);
    if kind != GITHUB_APP_ID_KIND {
        return Err(format!(
            "expected graph ID to have kind {GITHUB_APP_ID_KIND:?}; got {kind:?}"
        ));
    }
    unmarshal_spec_int64(raw)
}

/// `githubapp.MarshalGitHubAppID`.
pub fn marshal_github_app_id(id: i64) -> String {
    use base64::Engine as _;
    let payload = format!(
        "{}:{}",
        GITHUB_APP_ID_KIND,
        serde_json::to_string(&id).unwrap_or_else(|_| "0".to_owned())
    );
    base64::engine::general_purpose::URL_SAFE.encode(payload.as_bytes())
}

/// `githubapp.unmarshalUserID`: `relay.UnmarshalSpec(id, &int32)`.
pub fn unmarshal_user_id(raw: &str) -> Result<i32, String> {
    unmarshal_spec_int32(raw)
}

// ---------------------------------------------------------------------------
// UUID validation (google/uuid)
// ---------------------------------------------------------------------------

/// `uuid.Parse`'s error text for the cases this API can surface.
fn validate_uuid(raw: &str) -> Result<(), String> {
    let mut text = raw.trim();
    if let Some(rest) = text.strip_prefix("urn:uuid:") {
        text = rest;
    }
    let text = text.trim_matches(|ch| ch == '{' || ch == '}');
    let original_len = raw.len();
    match text.len() {
        32 => {
            if text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(())
            } else {
                Err("invalid UUID format".to_owned())
            }
        }
        36 => {
            let bytes = text.as_bytes();
            let mut valid = true;
            for (index, byte) in bytes.iter().enumerate() {
                if matches!(index, 8 | 13 | 18 | 23) {
                    valid &= *byte == b'-';
                } else {
                    valid &= byte.is_ascii_hexdigit();
                }
            }
            if valid {
                Ok(())
            } else {
                Err("invalid UUID format".to_owned())
            }
        }
        _ => Err(format!("invalid UUID length: {original_len}")),
    }
}

// ---------------------------------------------------------------------------
// State details
// ---------------------------------------------------------------------------

/// `gitHubAppStateDetails`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct GitHubAppStateDetails {
    #[serde(rename = "webhookUUID", default, skip_serializing_if = "String::is_empty")]
    pub webhook_uuid: String,
    #[serde(default)]
    pub domain: String,
    #[serde(rename = "app_id", default, skip_serializing_if = "is_zero_i32")]
    pub app_id: i32,
    #[serde(rename = "base_url", default, skip_serializing_if = "String::is_empty")]
    pub base_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(rename = "user_id", default, skip_serializing_if = "is_zero_i32")]
    pub user_id: i32,
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub struct GitHubAppServer {
    pub cache: std::sync::Arc<dyn StateStore>,
    pub apps: std::sync::Arc<dyn GitHubAppStore>,
    pub webhooks: std::sync::Arc<dyn WebhookBackend>,
    pub http: std::sync::Arc<dyn HttpDoer>,
    pub sleeper: std::sync::Arc<dyn Sleeper>,
    pub state: std::sync::Arc<dyn StateGenerator>,
    pub site_admin: std::sync::Arc<dyn SiteAdminCheck>,
}

impl GitHubAppServer {
    /// The offline wiring used by `ui_assets` when no Redis/DB is available.
    pub fn offline() -> Self {
        Self {
            cache: std::sync::Arc::new(MissStateStore),
            apps: std::sync::Arc::new(MemoryGitHubApps::new()),
            webhooks: std::sync::Arc::new(OfflineWebhooks),
            http: std::sync::Arc::new(OfflineHttpDoer),
            sleeper: std::sync::Arc::new(RealSleeper),
            state: std::sync::Arc::new(SystemStateGenerator),
            site_admin: std::sync::Arc::new(AllowAllSiteAdmin),
        }
    }

    fn new_state(&self) -> Result<String, String> {
        self.state.generate()
    }

    // -- stateHandler -------------------------------------------------------

    pub fn state_handler<B>(&self, req: &Request<B>) -> Response<BoxBody> {
        let s = match self.new_state() {
            Ok(state) => state,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when generating state parameter: {error}"),
                )
            }
        };

        let query = req.uri().query();
        let gql_id = app_handlers::query_param(query, "id").unwrap_or_default();
        let domain = app_handlers::query_param(query, "domain").unwrap_or_default();
        let base_url = app_handlers::query_param(query, "baseURL").unwrap_or_default();

        if gql_id.is_empty() {
            let state_details = serde_json::to_vec(&GitHubAppStateDetails::default())
                .expect("empty state details");
            self.cache.set(&s, state_details);
            return text_response(StatusCode::OK, s);
        }

        let id64 = match unmarshal_github_app_id(&gql_id) {
            Ok(id) => id,
            Err(error) => {
                return http_error(
                    StatusCode::BAD_REQUEST,
                    &format!("Unexpected error while unmarshalling App ID: {error}"),
                )
            }
        };
        let details = GitHubAppStateDetails {
            app_id: id64 as i32,
            domain,
            base_url,
            ..Default::default()
        };
        let encoded = match serde_json::to_vec(&details) {
            Ok(encoded) => encoded,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when marshalling state: {error}"),
                )
            }
        };
        self.cache.set(&s, encoded);
        text_response(StatusCode::OK, s)
    }

    // -- newAppStateHandler -------------------------------------------------

    pub fn new_app_state_handler<B>(&self, req: &Request<B>) -> Response<BoxBody> {
        let query = req.uri().query();
        let webhook_urn = app_handlers::query_param(query, "webhookURN").unwrap_or_default();
        let app_name = app_handlers::query_param(query, "appName").unwrap_or_default();
        let domain = app_handlers::query_param(query, "domain").unwrap_or_default();
        let base_url = app_handlers::query_param(query, "baseURL").unwrap_or_default();
        let kind = app_handlers::query_param(query, "kind").unwrap_or_default();
        let marshalled_user_id = app_handlers::query_param(query, "userID").unwrap_or_default();

        let mut user_id = 0i32;
        if !marshalled_user_id.is_empty() {
            match unmarshal_user_id(&marshalled_user_id) {
                Ok(uid) => user_id = uid,
                Err(error) => {
                    return http_error(
                        StatusCode::BAD_REQUEST,
                        &format!("Unexpected error while unmarshalling user ID: {error}"),
                    )
                }
            }
        }

        let mut webhook_uuid = String::new();
        if !webhook_urn.is_empty() {
            match self
                .webhooks
                .create(&app_name, CODE_HOST_KIND_GITHUB, &webhook_urn)
            {
                Ok(uuid) => webhook_uuid = uuid,
                Err(error) => {
                    return http_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Unexpected error while setting up webhook endpoint: {error}"),
                    )
                }
            }
        }

        let s = match self.new_state() {
            Ok(state) => state,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when generating state parameter: {error}"),
                )
            }
        };
        let details = GitHubAppStateDetails {
            webhook_uuid: webhook_uuid.clone(),
            domain,
            base_url,
            kind,
            user_id,
            ..Default::default()
        };
        let encoded = match serde_json::to_vec(&details) {
            Ok(encoded) => encoded,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when marshalling state: {error}"),
                )
            }
        };
        self.cache.set(&s, encoded);

        #[derive(Serialize)]
        struct ResponseBody {
            state: String,
            #[serde(rename = "webhookUUID", skip_serializing_if = "String::is_empty")]
            webhook_uuid: String,
        }
        let body = serde_json::to_string(&ResponseBody {
            state: s.clone(),
            webhook_uuid: webhook_uuid.clone(),
        })
        .unwrap_or_else(|_| format!("{{\"state\":\"{s}\"}}"));
        text_response(StatusCode::OK, format!("{body}\n"))
    }

    // -- redirectHandler ----------------------------------------------------

    pub fn redirect_handler<B>(&self, req: &Request<B>) -> Response<BoxBody> {
        let query = req.uri().query();
        let state = app_handlers::query_param(query, "state").unwrap_or_default();
        let code = app_handlers::query_param(query, "code").unwrap_or_default();
        if state.is_empty() || code.is_empty() {
            return http_error(
                StatusCode::BAD_REQUEST,
                "Bad request, code and state query params must be present",
            );
        }
        if state.chars().count() != 64 {
            return http_error(
                StatusCode::BAD_REQUEST,
                "Bad request, state query param is wrong length",
            );
        }
        let Some(state_value) = self.cache.get(&state) else {
            return http_error(
                StatusCode::BAD_REQUEST,
                "Bad request, state query param does not match",
            );
        };
        let state_details: GitHubAppStateDetails = match serde_json::from_slice(&state_value) {
            Ok(details) => details,
            Err(_) => return http_error(StatusCode::BAD_REQUEST, "Bad request, invalid state"),
        };
        self.cache.delete(&state);

        if let Err(error) = validate_uuid(&state_details.webhook_uuid) {
            return http_error(
                StatusCode::BAD_REQUEST,
                &format!("Bad request, could not parse webhook UUID: {error}"),
            );
        }

        let base_url = match parse_go_url(&state_details.base_url) {
            Ok(url) => url,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!(
                        "Bad request, could not parse baseURL from state: {}, error: {error}",
                        state_details.base_url
                    ),
                )
            }
        };
        let api_url = match api_root(&base_url) {
            Ok(url) => url,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when building manifest endpoint URL: {error}"),
                )
            }
        };
        let conversion_url = match join_path(
            &api_url,
            &["/app-manifests", &code, "conversions"],
        ) {
            Ok(url) => url,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when building manifest endpoint URL: {error}"),
                )
            }
        };

        let domain = match parse_domain(Some(&state_details.domain)) {
            Ok(Some(domain)) => domain,
            Ok(None) => unreachable!("a domain string is always present"),
            Err(error) => {
                return http_error(
                    StatusCode::BAD_REQUEST,
                    &format!("Unable to parse domain: {error}"),
                )
            }
        };
        let kind = match parse_kind(Some(&state_details.kind)) {
            Ok(Some(kind)) => kind,
            Ok(None) => unreachable!("a kind string is always present"),
            Err(error) => {
                return http_error(
                    StatusCode::BAD_REQUEST,
                    &format!("Unable to parse kind: {error}"),
                )
            }
        };

        let app = match self.convert_manifest(&conversion_url, domain, kind, state_details.user_id)
        {
            Ok(app) => app,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error while converting github app: {error}"),
                )
            }
        };

        let id = match self.apps.create(&app) {
            Ok(id) => id,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error while storing github app in DB: {error}"),
                )
            }
        };

        let mut hook = match self.webhooks.get_by_uuid(&state_details.webhook_uuid) {
            Ok(hook) => hook,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Error while fetching webhook: {error}"),
                )
            }
        };
        hook.secret = Some(app.webhook_secret.clone());
        hook.name = app.name.clone();
        if let Err(error) = self.webhooks.update(&hook) {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Error while updating webhook secret: {error}"),
            );
        }

        let state = match self.new_state() {
            Ok(state) => state,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Unexpected error when creating state param: {error}"),
                )
            }
        };
        let new_state_details = GitHubAppStateDetails {
            domain: state_details.domain.clone(),
            app_id: id,
            kind: state_details.kind.clone(),
            base_url: state_details.base_url.clone(),
            user_id: state_details.user_id,
            ..Default::default()
        };
        let encoded = match serde_json::to_vec(&new_state_details) {
            Ok(encoded) => encoded,
            Err(error) => {
                return http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("unexpected error when marshalling state: {error}"),
                )
            }
        };
        self.cache.set(&state, encoded);

        // The installations page often takes a few seconds to become
        // available after the app is created.
        self.sleeper.sleep(Duration::from_secs(3));
        let redirect_url = join_path(&app.app_url, &["installations/new"])
            .unwrap_or_else(|_| app.app_url.clone());
        redirect(req, &format!("{redirect_url}?state={state}"), 303)
    }

    /// `createGitHubApp`: POST the conversion URL, require 201, decode the app.
    fn convert_manifest(
        &self,
        conversion_url: &str,
        domain: &str,
        kind: &str,
        user_id: i32,
    ) -> Result<NewGitHubApp, String> {
        let (status, body) = self.http.post(conversion_url)?;
        if status != 201 {
            return Err(format!("expected 201 statusCode, got: {status}"));
        }
        let response: ManifestResponse =
            serde_json::from_slice(&body).map_err(|error| error.to_string())?;
        let html_url = parse_go_url(&response.html_url)?;
        let parsed = url::Url::parse(&html_url).map_err(|error| error.to_string())?;
        let scheme = parsed.scheme();
        let host = parsed.host_str().unwrap_or("").to_owned();
        Ok(NewGitHubApp {
            app_id: response.id,
            name: response.name,
            slug: response.slug.clone(),
            client_id: response.client_id,
            client_secret: response.client_secret,
            webhook_secret: response.webhook_secret,
            private_key: response.pem,
            base_url: format!("{scheme}://{host}"),
            app_url: html_url.clone(),
            domain: domain.to_owned(),
            kind: kind.to_owned(),
            logo: format!("{scheme}://{host}/identicons/app/app/{}", response.slug),
            created_by_user_id: user_id,
        })
    }

    // -- setupHandler -------------------------------------------------------

    pub fn setup_handler<B>(&self, req: &Request<B>) -> Response<BoxBody> {
        let query = req.uri().query();
        let state = app_handlers::query_param(query, "state").unwrap_or_default();
        let inst_id = app_handlers::query_param(query, "installation_id").unwrap_or_default();
        if state.is_empty() || inst_id.is_empty() {
            return redirect(req, "/site-admin/github-apps", 302);
        }
        if state.chars().count() != 64 {
            return http_error(
                StatusCode::BAD_REQUEST,
                "Bad request, state query param is wrong length",
            );
        }

        let Some(setup_info) = self.cache.get(&state) else {
            let redirect_url = generate_redirect_url(
                &GitHubAppStateDetails::default(),
                None,
                None,
                Some("Bad request, state query param does not match"),
            );
            return redirect(req, &redirect_url, 302);
        };
        let state_details: GitHubAppStateDetails = match serde_json::from_slice(&setup_info) {
            Ok(details) => details,
            Err(_) => {
                let redirect_url = generate_redirect_url(
                    &GitHubAppStateDetails::default(),
                    None,
                    None,
                    Some("Bad request, invalid state"),
                );
                return redirect(req, &redirect_url, 302);
            }
        };
        // Wait until we've validated the type of state before deleting it.
        self.cache.delete(&state);

        if let Err(error) = parse_kind(Some(&state_details.kind)) {
            return http_error(
                StatusCode::BAD_REQUEST,
                &format!("Unable to parse kind: {error}"),
            );
        }

        let installation_id: i32 = match inst_id.parse::<i32>() {
            Ok(id) => id,
            Err(_) => {
                let redirect_url = generate_redirect_url(
                    &state_details,
                    None,
                    None,
                    Some("Bad request, could not parse installation ID"),
                );
                return redirect(req, &redirect_url, 302);
            }
        };

        let action = app_handlers::query_param(query, "setup_action").unwrap_or_default();
        if action != "install" {
            return http_error(
                StatusCode::BAD_REQUEST,
                &format!("Bad request; unsupported setup action: {action}"),
            );
        }

        let app = match self.apps.get_by_id(state_details.app_id) {
            Ok(app) => app,
            Err(error) => {
                let redirect_url = generate_redirect_url(
                    &state_details,
                    Some(installation_id),
                    None,
                    Some(&format!(
                        "Unexpected error while fetching GitHub App from DB: {error}"
                    )),
                );
                return redirect(req, &redirect_url, 302);
            }
        };
        let redirect_url = generate_redirect_url(
            &state_details,
            Some(installation_id),
            Some(&app.name),
            None,
        );
        redirect(req, &redirect_url, 302)
    }

    /// `siteAdminMiddleware`: returns `Some(response)` when the request is
    /// rejected.
    /// `siteAdminMiddleware` for an explicit request actor.
    pub fn site_admin_gate_for(&self, actor: ActorIdentity) -> Option<Response<BoxBody>> {
        match self.site_admin.check_actor(actor) {
            SiteAdminOutcome::Allowed => None,
            SiteAdminOutcome::Forbidden => Some(http_error(
                StatusCode::FORBIDDEN,
                "User must be site admin",
            )),
            SiteAdminOutcome::Unauthenticated => Some(http_error(
                StatusCode::UNAUTHORIZED,
                "User must be authenticated",
            )),
            SiteAdminOutcome::Error => {
                Some(http_error(StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong"))
            }
        }
    }

    /// `registerRoutes` + `router.Use(siteAdminMiddleware)`.
    pub fn serve<B>(&self, req: &Request<B>, route: GitHubAppRoute) -> Response<BoxBody> {
        self.serve_with_actor(req, route, ActorIdentity::Anonymous)
    }

    /// Same routes with the request actor from the session middleware.
    pub fn serve_with_actor<B>(
        &self,
        req: &Request<B>,
        route: GitHubAppRoute,
        actor: ActorIdentity,
    ) -> Response<BoxBody> {
        if let Some(response) = self.site_admin_gate_for(actor) {
            return response;
        }
        match route {
            GitHubAppRoute::State => self.state_handler(req),
            GitHubAppRoute::NewAppState => self.new_app_state_handler(req),
            GitHubAppRoute::Redirect => self.redirect_handler(req),
            GitHubAppRoute::Setup => self.setup_handler(req),
        }
    }
}

#[derive(serde::Deserialize)]
struct ManifestResponse {
    id: i32,
    #[serde(default)]
    slug: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    html_url: String,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default)]
    pem: String,
    #[serde(default)]
    webhook_secret: String,
}

/// Offline entry point wired from `ui_assets`.
pub fn serve_offline<B>(req: &Request<B>, route: GitHubAppRoute) -> Response<BoxBody> {
    GitHubAppServer::offline().serve(req, route)
}

// ---------------------------------------------------------------------------
// generateRedirectURL
// ---------------------------------------------------------------------------

/// `generateRedirectURL` (`httpapi.go:430-491`).
pub fn generate_redirect_url(
    state_details: &GitHubAppStateDetails,
    installation_id: Option<i32>,
    app_name: Option<&str>,
    creation_error: Option<&str>,
) -> String {
    if state_details.domain.is_empty() && creation_error.is_some() {
        return format!(
            "/site-admin/github-apps?kind={}&success=false&error={}",
            state_details.kind,
            query_escape(creation_error.unwrap_or_default())
        );
    }

    let parsed_domain = match parse_domain(Some(&state_details.domain)) {
        Ok(domain) => domain,
        Err(_) => {
            return format!(
                "/site-admin/github-apps?kind={}&success=false&error={}",
                state_details.kind,
                query_escape(&format!("invalid domain: {}", state_details.domain))
            )
        }
    };
    let Some(parsed_domain) = parsed_domain else {
        return format!(
            "/site-admin/github-apps?kind={}&success=false&error={}",
            state_details.kind,
            query_escape("unable to parse domain")
        );
    };

    let kind = match parse_kind(Some(&state_details.kind)) {
        Ok(kind) => kind,
        Err(error) => {
            return format!(
                "/site-admin/github-apps?success=false&kind={}&error={}",
                state_details.kind,
                query_escape(&error)
            )
        }
    };
    let Some(kind) = kind else {
        return format!(
            "/site-admin/github-apps?kind={}&success=false&error={}",
            state_details.kind,
            query_escape("unable to parse kind")
        );
    };

    match parsed_domain {
        "repos" => {
            let gh_app_page_url = "/site-admin/github-apps";
            if let Some(error) = creation_error {
                return format!(
                    "{gh_app_page_url}?success=false&error={}",
                    query_escape(error)
                );
            }
            if installation_id.is_none() || state_details.app_id == 0 {
                return format!(
                    "{gh_app_page_url}?success=false&error={}",
                    query_escape("missing installation ID or app ID")
                );
            }
            return format!(
                "{gh_app_page_url}/{}?installation_id={}",
                marshal_github_app_id(i64::from(state_details.app_id)),
                installation_id.unwrap_or_default()
            );
        }
        "batches" => {
            let mut gh_app_page_url = "/site-admin/batch-changes";
            if kind == "USER_CREDENTIAL" {
                gh_app_page_url = "/user/settings/batch-changes";
            }
            if let Some(error) = creation_error {
                return format!(
                    "{gh_app_page_url}?kind={kind}&success=false&error={}",
                    query_escape(error)
                );
            }
            // This shouldn't really happen unless we also had an error.
            let Some(app_name) = app_name else {
                return format!("{gh_app_page_url}?kind={kind}&success=true");
            };
            return format!("{gh_app_page_url}?kind={kind}&success=true&app_name={app_name}");
        }
        _ => format!(
            "/site-admin/github-apps?kind={kind}&success=false&error={}",
            query_escape(&format!(
                "unsupported github apps domain: {parsed_domain}"
            ))
        ),
    }
}

// ---------------------------------------------------------------------------
// Go URL parsing
// ---------------------------------------------------------------------------

/// `url.Parse(s).String()` minimal support: returns the raw string after
/// validating percent escapes (Go's only common parse error here).
fn parse_go_url(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                let end = (index + 3).min(bytes.len());
                return Err(format!(
                    "parse {raw:?}: invalid URL escape {:?}",
                    &raw[index..end]
                ));
            }
        }
        index += 1;
    }
    Ok(raw.to_owned())
}

/// Route method guard: the baseline routes are GET-only.
pub fn method_allowed(method: &Method) -> bool {
    method == Method::GET
}

// ---------------------------------------------------------------------------
// Production adapters (M9.32c)
//
// These replace the M9.31d in-memory seams with the real dependencies the Go
// server uses: `database.GitHubApps()` (`github_apps`), `database.Webhooks(...)`
// (`webhooks`, keyring-encrypted), `httpcli.UncachedExternalClient` and the
// `rcache.Cache`. No table, column or Redis key is added.
// ---------------------------------------------------------------------------

/// `gitHubAppsStore` behind [`GitHubAppStore`].
pub struct PgGitHubAppStore {
    pool: sqlx::PgPool,
    keyring: std::sync::Arc<sg_keyring::Keyring>,
}

impl PgGitHubAppStore {
    pub fn new(pool: sqlx::PgPool, keyring: std::sync::Arc<sg_keyring::Keyring>) -> Self {
        Self { pool, keyring }
    }

    fn github_app_key(&self) -> Option<std::sync::Arc<dyn sg_encryption::Key>> {
        self.keyring.github_app_key()
    }

    /// `gitHubAppsStore.GetByDomainAndKind` (`store.go:430-433`).
    /// `baseURLWhere` trims the trailing slash from the stored column.
    fn find_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<Option<StoredApp>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT id, name FROM github_apps \
                 WHERE domain = $1 AND kind = $2::github_app_kind \
                   AND trim(trailing '/' from base_url) = $3",
            )
            .bind(domain)
            .bind(kind)
            .bind(base_url.trim_end_matches('/'))
            .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        Ok(row.map(|row| StoredApp {
            id: sqlx::Row::get(&row, "id"),
            name: sqlx::Row::get(&row, "name"),
        }))
    }
}

impl GitHubAppStore for PgGitHubAppStore {
    fn create(&self, app: &NewGitHubApp) -> Result<i32, String> {
        let key = self.github_app_key();
        // `clientSecret, _, err := MaybeEncrypt(...)` — the client-secret key ID
        // is discarded; `privateKey, keyID, err` supplies `encryption_key_id`.
        let (client_secret, _) =
            block_on(sg_encryption::maybe_encrypt(key.as_deref(), &app.client_secret))
                .map_err(|err| err.to_string())?;
        let (private_key, key_id) =
            block_on(sg_encryption::maybe_encrypt(key.as_deref(), &app.private_key))
                .map_err(|err| err.to_string())?;

        let base_url = normalize_base_url(&app.base_url)
            .map_err(|_| "unable to parse base URL: ".to_owned())?;

        let domain = if app.domain.is_empty() {
            "repos".to_owned()
        } else {
            app.domain.clone()
        };
        let kind = if app.kind.is_empty() {
            "REPO_SYNC".to_owned()
        } else if !valid_kind(&app.kind) {
            return Err(format!("The GitHubAppKind {} is not valid.", app.kind));
        } else {
            app.kind.clone()
        };

        if domain == "batches" && kind == "COMMIT_SIGNING" {
            // `store.go:136-147` errata: a *hit* makes `GetByDomainAndKind`
            // return `(app, nil)`; the `ErrNoGitHubAppFound` assertion then
            // fails and `errors.Wrap(nil, ...)` is nil, so `Create` returns
            // `(-1, nil)` without inserting the duplicate.
            match self.find_by_domain_and_kind(&domain, &kind, &base_url) {
                Ok(Some(_)) => return Ok(-1),
                Ok(None) => {}
                Err(error) => {
                    return Err(format!("checking for existing batches app: {error}"))
                }
            }
        }

        // `store.go:150` passes the plain `keyID` string (not a NULL column),
        // and `github_apps.encryption_key_id` is NOT NULL: the no-key path
        // stores the empty string.
        let row = block_on(
            sqlx::query(
                "INSERT INTO github_apps \
                     (app_id, name, domain, slug, base_url, app_url, client_id, client_secret, \
                      private_key, encryption_key_id, logo, kind, creator_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, \
                         $12::github_app_kind, $13) \
                 RETURNING id",
            )
            .bind(app.app_id)
            .bind(&app.name)
            .bind(&domain)
            .bind(&app.slug)
            .bind(&base_url)
            .bind(&app.app_url)
            .bind(&app.client_id)
            .bind(&client_secret)
            .bind(&private_key)
            .bind(&key_id)
            .bind(&app.logo)
            .bind(&kind)
            .bind(app.created_by_user_id)
            .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        Ok(sqlx::Row::get(&row, "id"))
    }

    fn get_by_id(&self, id: i32) -> Result<StoredApp, String> {
        let row = block_on(
            sqlx::query("SELECT id, name FROM github_apps WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        match row {
            Some(row) => Ok(StoredApp {
                id: sqlx::Row::get(&row, "id"),
                name: sqlx::Row::get(&row, "name"),
            }),
            None => Err(format!("no app exists matching criteria: 'id = {id}'")),
        }
    }
}

/// `authcheck.CheckCurrentUserIsSiteAdmin` behind [`SiteAdminCheck`], reading
/// `users.site_admin` for the request actor.
pub struct PgSiteAdminCheck {
    pool: sqlx::PgPool,
    /// Actor used by the actor-less [`SiteAdminCheck::check`]
    /// (`production_with_actor` bakes the launch actor; per-request callers
    /// use [`SiteAdminCheck::check_actor`] via `serve_with_actor`).
    actor: ActorIdentity,
}

impl PgSiteAdminCheck {
    pub fn new(pool: sqlx::PgPool, actor: ActorIdentity) -> Self {
        Self { pool, actor }
    }

    /// `internal/auth/site_admin.go` `CheckCurrentUserIsSiteAdmin`: internal
    /// actor passes; an unauthenticated actor or a missing/soft-deleted user is
    /// `ErrNotAuthenticated` (401); a real non-admin is `ErrMustBeSiteAdmin`
    /// (403); other lookup failures are 500.
    fn evaluate(&self, actor: ActorIdentity) -> SiteAdminOutcome {
        match actor {
            ActorIdentity::Internal => SiteAdminOutcome::Allowed,
            ActorIdentity::Anonymous => SiteAdminOutcome::Unauthenticated,
            ActorIdentity::User(uid) if uid <= 0 => SiteAdminOutcome::Unauthenticated,
            ActorIdentity::User(uid) => {
                // `db.Users().GetByID(...).GetByCurrentAuthUser` filters
                // `deleted_at IS NULL`; `CurrentUser` maps not-found to nil.
                match block_on(sg_db::users::get_site_admin(&self.pool, uid)) {
                    Ok(Some(true)) => SiteAdminOutcome::Allowed,
                    Ok(Some(false)) => SiteAdminOutcome::Forbidden,
                    Ok(None) => SiteAdminOutcome::Unauthenticated,
                    Err(_) => SiteAdminOutcome::Error,
                }
            }
        }
    }
}

impl SiteAdminCheck for PgSiteAdminCheck {
    fn check(&self) -> SiteAdminOutcome {
        self.evaluate(self.actor)
    }

    fn check_actor(&self, actor: ActorIdentity) -> SiteAdminOutcome {
        self.evaluate(actor)
    }
}

/// `webhookStore` behind [`WebhookBackend`] (`internal/database/webhooks.go`)
/// plus `backend.NewWebhookService`'s kind/URN validation.
pub struct PgWebhookBackend {
    pool: sqlx::PgPool,
    keyring: std::sync::Arc<sg_keyring::Keyring>,
    /// `actor.FromContext(ctx).UID`; the internal actor is 0 (stored NULL).
    actor_uid: i32,
}

impl PgWebhookBackend {
    pub fn new(
        pool: sqlx::PgPool,
        keyring: std::sync::Arc<sg_keyring::Keyring>,
        actor_uid: i32,
    ) -> Self {
        Self {
            pool,
            keyring,
            actor_uid,
        }
    }

    fn webhook_key(&self) -> Option<std::sync::Arc<dyn sg_encryption::Key>> {
        self.keyring.webhook_key()
    }
}

/// `validateCodeHostKindAndSecret` (`internal/backend/webhooks.go:96-110`).
fn validate_code_host_kind(kind: &str, has_secret: bool) -> Result<(), String> {
    match kind {
        "GITHUB" | "GITLAB" | "BITBUCKETSERVER" | "BITBUCKETCLOUD" => Ok(()),
        "AZUREDEVOPS" => {
            if has_secret {
                Err(format!(
                    "webhooks do not support secrets for code host kind {kind}"
                ))
            } else {
                Ok(())
            }
        }
        _ => Err(format!(
            "webhooks are not supported for code host kind {kind}"
        )),
    }
}

/// `extsvc.NewCodeHostBaseURL`: lowercase host, trailing slash on the path.
fn normalize_code_host_url(raw: &str) -> Result<String, String> {
    let mut parsed = url::Url::parse(raw).map_err(|error| error.to_string())?;
    if let Some(host) = parsed.host_str() {
        let lowered = host.to_ascii_lowercase();
        if lowered != host {
            parsed
                .set_host(Some(&lowered))
                .map_err(|error| error.to_string())?;
        }
    }
    let path = parsed.path().to_owned();
    if !path.ends_with('/') {
        parsed.set_path(&format!("{path}/"));
    }
    Ok(parsed.to_string())
}

impl WebhookBackend for PgWebhookBackend {
    fn create(&self, name: &str, kind: &str, urn: &str) -> Result<String, String> {
        validate_code_host_kind(kind, false)?;
        let urn = normalize_code_host_url(urn)?;
        let created_by = if self.actor_uid == 0 {
            None
        } else {
            Some(self.actor_uid)
        };
        let row = block_on(
            sqlx::query(
                "INSERT INTO webhooks \
                     (name, code_host_kind, code_host_urn, secret, encryption_key_id, \
                      created_by_user_id) \
                 VALUES ($1, $2, $3, NULL, NULL, $4) RETURNING uuid::text AS uuid",
            )
            .bind(name)
            .bind(kind)
            .bind(&urn)
            .bind(created_by)
            .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        Ok(sqlx::Row::get(&row, "uuid"))
    }

    fn get_by_uuid(&self, uuid: &str) -> Result<WebhookRecord, String> {
        let row = block_on(
            sqlx::query(
                "SELECT uuid::text AS uuid, name, secret, encryption_key_id \
                 FROM webhooks WHERE uuid = $1::uuid",
            )
            .bind(uuid)
            .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let Some(row) = row else {
            return Err(format!("webhook with UUID {uuid} not found"));
        };
        let raw_secret: Option<String> = sqlx::Row::get(&row, "secret");
        let key_id: Option<String> = sqlx::Row::get(&row, "encryption_key_id");
        let secret = match (key_id.as_deref().unwrap_or(""), raw_secret) {
            ("", Some(raw)) if !raw.is_empty() => Some(raw),
            ("", _) => None,
            (key_id, Some(raw)) if !raw.is_empty() => Some(
                block_on(sg_encryption::maybe_decrypt(
                    self.webhook_key().as_deref(),
                    &raw,
                    key_id,
                ))
                .map_err(|err| err.to_string())?,
            ),
            _ => None,
        };
        Ok(WebhookRecord {
            uuid: sqlx::Row::get(&row, "uuid"),
            name: sqlx::Row::get::<Option<String>, _>(&row, "name").unwrap_or_default(),
            secret,
        })
    }

    fn update(&self, record: &WebhookRecord) -> Result<(), String> {
        // `webhookStore.Update` writes the whole fetched row back, including
        // `webhook.CodeHostURN.String()` (re-normalized by `scanWebhook`) and
        // `CodeHostKind`; fetch them so the stored URN gains its trailing `/`.
        let current = block_on(
            sqlx::query(
                "SELECT code_host_kind, code_host_urn FROM webhooks WHERE uuid = $1::uuid",
            )
            .bind(&record.uuid)
            .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let Some(current) = current else {
            return Err(format!("webhook with UUID {} not found", record.uuid));
        };
        let code_host_kind: String = sqlx::Row::get(&current, "code_host_kind");
        let raw_urn: String = sqlx::Row::get(&current, "code_host_urn");
        let code_host_urn = normalize_code_host_url(&raw_urn).unwrap_or(raw_urn);

        let (encrypted_secret, key_id) = match record.secret.as_deref() {
            Some(secret) => {
                let (value, key_id) = block_on(sg_encryption::maybe_encrypt(
                    self.webhook_key().as_deref(),
                    secret,
                ))
                .map_err(|err| format!("encrypting secret: {err}"))?;
                if value.is_empty() && key_id.is_empty() {
                    return Err("empty secret and key provided".to_owned());
                }
                (Some(value), if key_id.is_empty() { None } else { Some(key_id) })
            }
            None => (None, None),
        };
        let updated_by = if self.actor_uid == 0 {
            None
        } else {
            Some(self.actor_uid)
        };
        let result = block_on(
            sqlx::query(
                "UPDATE webhooks SET name = $1, code_host_urn = $2, code_host_kind = $3, \
                     secret = $4, encryption_key_id = $5, updated_at = NOW(), \
                     updated_by_user_id = $6 \
                 WHERE uuid = $7::uuid",
            )
            .bind(&record.name)
            .bind(&code_host_urn)
            .bind(&code_host_kind)
            .bind(encrypted_secret)
            .bind(key_id)
            .bind(updated_by)
            .bind(&record.uuid)
            .execute(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        if result.rows_affected() == 0 {
            return Err(format!("webhook with UUID {} not found", record.uuid));
        }
        Ok(())
    }
}

/// `httpcli.UncachedExternalClient` behind [`HttpDoer`].
pub struct ReqwestHttpDoer {
    client: reqwest::Client,
}

impl ReqwestHttpDoer {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestHttpDoer {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpDoer for ReqwestHttpDoer {
    fn post(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let client = self.client.clone();
        let url = url.to_owned();
        block_on(async move {
            let response = client
                .post(&url)
                .body(reqwest::Body::from(Vec::new()))
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map_err(|error| error.to_string())?
                .to_vec();
            Ok((status, body))
        })
    }
}

impl GitHubAppServer {
    /// The production wiring (`SetupGitHubAppRoutes`): real Redis, Postgres and
    /// the external HTTP client. `keyring` may be the default empty ring, in
    /// which case secrets are stored plaintext exactly like Go with no keys.
    pub fn production(
        pool: sqlx::PgPool,
        keyring: std::sync::Arc<sg_keyring::Keyring>,
        kv: sg_redis::KeyValue,
    ) -> Self {
        // Deny-by-default: the routes are only reachable with a site-admin
        // actor (`serve_with_actor`). The launch server carries no actor, so
        // the actor-less `serve` path answers 401 until the session middleware
        // supplies `AppDeps.more.actor`.
        Self::production_with_actor(pool, keyring, kv, ActorIdentity::Anonymous)
    }

    /// Production wiring with the request actor that writes the `webhooks`
    /// audit columns (`created_by_user_id` / `updated_by_user_id`) and backs
    /// the site-admin gate.
    pub fn production_with_actor(
        pool: sqlx::PgPool,
        keyring: std::sync::Arc<sg_keyring::Keyring>,
        kv: sg_redis::KeyValue,
        actor: ActorIdentity,
    ) -> Self {
        let actor_uid = match actor {
            ActorIdentity::User(uid) => uid,
            ActorIdentity::Anonymous | ActorIdentity::Internal => 0,
        };
        Self {
            cache: std::sync::Arc::new(RcacheStateStore::with_kv(kv)),
            apps: std::sync::Arc::new(PgGitHubAppStore::new(pool.clone(), keyring.clone())),
            webhooks: std::sync::Arc::new(PgWebhookBackend::new(
                pool.clone(),
                keyring,
                actor_uid,
            )),
            http: std::sync::Arc::new(ReqwestHttpDoer::new()),
            sleeper: std::sync::Arc::new(RealSleeper),
            state: std::sync::Arc::new(SystemStateGenerator),
            site_admin: std::sync::Arc::new(PgSiteAdminCheck::new(pool, actor)),
        }
    }
}
