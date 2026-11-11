//! M9.8js differential of the OpenID Connect login/callback middleware against
//! the real Go baseline (`cmd/frontend/internal/auth/openidconnect/zz_m9js_oracle_test.go`,
//! run by `run_oidc_middleware_diff.sh`).
//!
//! The Go oracle drives `Middleware(...).App` (the `/.auth/callback` rewrite,
//! the `/login` handler, `AuthCallback`, the session writes and the security
//! events) against an in-process RESP Redis, dbmocks and a fixed OP transport.
//! It writes a `corpus.json` (site config, OP responses, session seed, license
//! and provisioning knobs) plus one normalized JSON file per case. This harness
//! replays the corpus through the parent wiring
//! [`oauth_routes::auth_prefix_response_at`] with the [`oidc_middleware`]
//! runtime installed, using a recording session store, fixed CSRF/session-ID
//! sources and a fixed clock, and writes the same JSON for a byte-for-byte
//! `cmp`.
//!
//! Env:
//!
//! * `SG_OIDC_MIDDLEWARE_HARNESS=1`
//! * `OIDC_MIDDLEWARE_CORPUS` — `corpus.json` written by the Go oracle
//! * `OIDC_MIDDLEWARE_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{header, HeaderValue, Method, Request};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_session::session::GoTime;
use sg_session::write::{SessionManager, WriteOptions};
use sg_session::{FixedSessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::actor_chain::LicenseReader;
use sg_service_frontend::authz::{html_escape, LicenseState};
use sg_service_frontend::oauth_login::{
    encode_query_sorted, go_json_escape, go_parse_url, go_url_string, parse_query, query_get,
    GoUrl, OAuthAuthStyle, OAuthHttpClient, OAuthHttpResponse, OAuthSecurityEventRecord,
    OAuthSecurityEventSink, OAuthUserLookup, OAuthUserRecord, TokenExchangeRequest,
};
use sg_service_frontend::oauth_routes::{
    auth_prefix_response_at, OAuthRouteConfig, OAuthRouteDeps, OAuthUserResolverRegistry,
};
use sg_service_frontend::oidc_login::{
    ExternalIdentity, OidcHttpClient, OidcHttpResponse, OpenIdConnectAuthProvider, ProvisionError,
    ProvisionOutcome, Provisioner,
};
use sg_service_frontend::oidc_middleware::{
    AuthnState, FixedAuthnStateSource, OidcMiddlewareRuntime, OidcProvisionerFactory,
};

const GOOD_KEY: &[u8] = b"m9js-session-key";
const GENERATED_ID: &str = "M9JSGENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const FIXED_CSRF: &str = "m9js-fixed-csrf";

// ---------------------------------------------------------------------------
// Corpus (mirrors `m9jsCase` in the Go oracle).
// ---------------------------------------------------------------------------

#[derive(Deserialize, Clone)]
struct Case {
    name: String,
    #[serde(default = "default_method")]
    method: String,
    path: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    cookie: String,
    site: String,
    #[serde(default)]
    op: BTreeMap<String, OpJson>,
    #[serde(default)]
    seed: String,
    #[serde(default)]
    license: String,
    #[serde(default)]
    user: Option<UserJson>,
    provision: ProvisionJson,
    #[serde(rename = "nowUnix")]
    now_unix: i64,
}

fn default_method() -> String {
    "GET".to_owned()
}

#[derive(Deserialize, Clone)]
struct HttpJson {
    status: u16,
    body: String,
}

#[derive(Deserialize, Clone)]
struct OpJson {
    discovery: HttpJson,
    jwks: HttpJson,
    userinfo: HttpJson,
    token: HttpJson,
    #[serde(rename = "tokenMode", default)]
    token_mode: String,
}

#[derive(Deserialize, Clone)]
struct ProvisionJson {
    outcome: String,
    user_id: i32,
    safe_message: String,
}

#[derive(Deserialize, Clone, Default)]
struct UserJson {
    #[serde(rename = "siteAdmin", default)]
    site_admin: bool,
    #[serde(rename = "getError", default)]
    get_error: bool,
}

// ---------------------------------------------------------------------------
// Output (field order matches `m9jsOutput`).
// ---------------------------------------------------------------------------

#[derive(Serialize, Default, Clone, PartialEq, Eq)]
struct StateOut {
    ok: bool,
    csrf: String,
    redirect: String,
    #[serde(rename = "p")]
    provider_id: String,
    #[serde(rename = "nonceEqualsState")]
    nonce_equals_state: bool,
}

#[derive(Serialize, Clone)]
struct EventOut {
    name: String,
    url: String,
    #[serde(rename = "userId")]
    user_id: i32,
    anonymous: String,
    source: String,
    argument: String,
}

#[derive(Serialize, Clone)]
struct IdentityOut {
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    login: String,
    email: String,
    #[serde(rename = "emailVerified")]
    email_verified: bool,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "createIfNotExist")]
    create_if_not_exist: bool,
}

#[derive(Serialize, Default, Clone)]
struct SessionOut {
    #[serde(rename = "hasCookie")]
    has_cookie: bool,
    #[serde(rename = "stateOK")]
    state_ok: bool,
    state: StateOut,
    #[serde(rename = "actorPresent")]
    actor_present: bool,
    #[serde(rename = "actorUID")]
    actor_uid: i32,
    #[serde(rename = "oidcData")]
    oidc_data: String,
}

#[derive(Serialize)]
struct Output {
    name: String,
    status: u16,
    #[serde(rename = "contentType")]
    content_type: String,
    location: String,
    body: String,
    cookies: Vec<String>,
    vary: Vec<String>,
    nosniff: String,
    state: StateOut,
    events: Vec<EventOut>,
    identity: Option<IdentityOut>,
    session: SessionOut,
}

// ---------------------------------------------------------------------------
// Fakes.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingStore {
    data: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl SessionStore for RecordingStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        Ok(self.data.lock().unwrap().get(id).cloned())
    }

    async fn save(
        &self,
        id: &str,
        payload: Vec<u8>,
        _ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        self.data.lock().unwrap().insert(id.to_owned(), payload);
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
        self.data.lock().unwrap().remove(id);
        Ok(())
    }
}

/// Serves discovery/JWKS/userinfo like the Go oracle's `m9jsTransport`.
struct FakeOidcHttp {
    routes: HashMap<String, (u16, Vec<u8>)>,
}

#[async_trait]
impl OidcHttpClient for FakeOidcHttp {
    async fn get(&self, url: &str, _bearer: Option<&str>) -> Result<OidcHttpResponse, String> {
        match self.routes.get(url) {
            Some((status, body)) => Ok(OidcHttpResponse {
                status: *status,
                body: body.clone(),
            }),
            None => Err(format!("unexpected request to {url}")),
        }
    }
}

/// Token endpoint. `header_401` rejects the Basic-auth style so the middleware
/// has to retry with the credentials in the form, like x/oauth2's probe.
struct FakeToken {
    routes: HashMap<String, (u16, Vec<u8>)>,
    modes: HashMap<String, String>,
}

#[async_trait]
impl OAuthHttpClient for FakeToken {
    async fn exchange_code(
        &self,
        request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String> {
        let (status, body) = self
            .routes
            .get(&request.token_url)
            .cloned()
            .ok_or_else(|| format!("unexpected token request to {}", request.token_url))?;
        let header_rejected =
            self.modes.get(&request.token_url).map(String::as_str) == Some("header_401");
        let (status, body) = if header_rejected && request.auth_style == OAuthAuthStyle::InHeader {
            (401, br#"{"error":"invalid_client"}"#.to_vec())
        } else {
            (status, body)
        };
        Ok(OAuthHttpResponse {
            status,
            content_type: "application/json".to_owned(),
            body,
        })
    }
}

#[derive(Default)]
struct FactoryState {
    client_id: String,
    create_if_not_exist: bool,
    seen: Option<IdentityOut>,
}

struct FakeProvisioner {
    state: Arc<Mutex<FactoryState>>,
    outcome: String,
    user_id: i32,
    safe_message: String,
}

#[async_trait]
impl Provisioner for FakeProvisioner {
    async fn get_or_create_user(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError> {
        {
            let mut state = self.state.lock().unwrap();
            state.seen = Some(IdentityOut {
                service_type: identity.service_type.to_owned(),
                service_id: identity.service_id.to_owned(),
                client_id: String::new(),
                account_id: identity.account_id.to_owned(),
                login: identity.login.to_owned(),
                email: identity.email.to_owned(),
                email_verified: identity.email_verified,
                display_name: identity.display_name.unwrap_or_default().to_owned(),
                create_if_not_exist: false,
            });
        }
        match self.outcome.as_str() {
            "error" => Err(ProvisionError {
                safe_message: self.safe_message.clone(),
                message: "provision failed".to_owned(),
            }),
            "existing" => Ok(ProvisionOutcome::Existing {
                user_id: self.user_id,
            }),
            _ => Ok(ProvisionOutcome::Created {
                user_id: self.user_id,
            }),
        }
    }
}

struct FakeFactory {
    state: Arc<Mutex<FactoryState>>,
    outcome: String,
    user_id: i32,
    safe_message: String,
}

impl OidcProvisionerFactory for FakeFactory {
    fn for_config(&self, config: &OpenIdConnectAuthProvider) -> Arc<dyn Provisioner> {
        {
            let mut state = self.state.lock().unwrap();
            state.client_id = config.client_id.clone();
            state.create_if_not_exist = config.allow_signup.unwrap_or(true);
        }
        Arc::new(FakeProvisioner {
            state: Arc::clone(&self.state),
            outcome: self.outcome.clone(),
            user_id: self.user_id,
            safe_message: self.safe_message.clone(),
        })
    }
}

struct FakeUsers {
    site_admin: bool,
    get_error: bool,
    now: OffsetDateTime,
}

#[async_trait]
impl OAuthUserLookup for FakeUsers {
    async fn get_by_id(&self, uid: i32) -> Result<OAuthUserRecord, String> {
        if self.get_error {
            return Err("user store down".to_owned());
        }
        Ok(OAuthUserRecord {
            id: uid,
            created_at: GoTime(self.now),
            site_admin: self.site_admin,
        })
    }
}

struct FakeLicense(LicenseState);

#[async_trait]
impl LicenseReader for FakeLicense {
    async fn product_license(&self) -> LicenseState {
        self.0
    }
}

#[derive(Default)]
struct RecordingEvents {
    events: Mutex<Vec<OAuthSecurityEventRecord>>,
}

#[async_trait]
impl OAuthSecurityEventSink for RecordingEvents {
    async fn log_security_event(&self, event: OAuthSecurityEventRecord) -> Result<(), String> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

struct NoExchange;

#[async_trait]
impl OAuthHttpClient for NoExchange {
    async fn exchange_code(
        &self,
        _request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String> {
        Err("unused".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Request/response plumbing.
// ---------------------------------------------------------------------------

fn build_request(case: &Case, cookie: Option<&str>) -> Request<()> {
    let uri = if case.query.is_empty() {
        case.path.clone()
    } else {
        format!("{}?{}", case.path, case.query)
    };
    let mut builder = Request::builder()
        .method(Method::from_bytes(case.method.as_bytes()).unwrap_or(Method::GET))
        .uri(uri);
    if let Some(cookie) = cookie {
        builder = builder.header(header::COOKIE, cookie);
    }
    builder.body(()).expect("request")
}

/// `session.SetData(w, r, "oidcState", value)` on a fresh session, returning the
/// `sgs` cookie the seed response would set.
async fn seed_cookie(
    store: &dyn SessionStore,
    options: WriteOptions,
    now: OffsetDateTime,
    value: &[u8],
) -> String {
    let ids = FixedSessionIdSource(GENERATED_ID.to_owned());
    let mut manager = SessionManager::new(store, &ids, GOOD_KEY, options, now, None, false);
    manager.set_data("oidcState", value).await.expect("seed");
    let cookie = manager
        .cookies
        .iter()
        .find(|cookie| cookie.name == "sgs")
        .expect("seed cookie");
    format!("sgs={}", cookie.value)
}

fn header_value(headers: &http::HeaderMap, name: http::HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn header_values(headers: &http::HeaderMap, name: http::HeaderName) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok().map(str::to_owned))
        .collect()
}

fn next_response() -> http::Response<tonic::body::BoxBody> {
    let mut response = http::Response::new(tonic::body::boxed(http_body_util::Full::new(
        bytes::Bytes::from_static(b"NEXT"),
    )));
    *response.status_mut() = http::StatusCode::IM_A_TEAPOT;
    response
}

fn append_outer_vary(
    mut response: http::Response<tonic::body::BoxBody>,
) -> http::Response<tonic::body::BoxBody> {
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Authorization"));
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Cookie"));
    response
}

// ---------------------------------------------------------------------------
// Normalization (identical to `m9jsNormalize*` in the Go oracle).
// ---------------------------------------------------------------------------

fn normalize_location(location: &str) -> String {
    if location.is_empty() {
        return String::new();
    }
    let Some(parsed) = go_parse_url(location) else {
        return location.to_owned();
    };
    let mut pairs = parse_query(&parsed.raw_query);
    for key in ["state", "nonce"] {
        if let Some((_, value)) = pairs.iter_mut().find(|(name, _)| name == key) {
            if !value.is_empty() {
                *value = "<state>".to_owned();
            }
        }
    }
    let raw_query = encode_query_sorted(&pairs);
    go_url_string(&GoUrl {
        raw_query,
        ..parsed
    })
}

fn normalize_body(status: u16, location: &str, body: &str) -> String {
    if status == 302 && !location.is_empty() && body.starts_with("<a href=") {
        return format!("<a href=\"{}\">Found</a>.\n\n", html_escape(location));
    }
    body.to_owned()
}

fn normalize_cookie(header: &str) -> String {
    let mut parts = header.split("; ");
    let first = parts.next().unwrap_or("");
    let (name, _value) = first.split_once('=').unwrap_or((first, ""));
    let mut out = Vec::new();
    if name == "sgs" {
        out.push("sgs=<generated>".to_owned());
    } else {
        out.push(first.to_owned());
    }
    for part in parts {
        if part.starts_with("Expires=") {
            continue;
        }
        out.push(part.to_owned());
    }
    out.join("; ")
}

// ---------------------------------------------------------------------------
// AuthnState decode / session inspection.
// ---------------------------------------------------------------------------

fn decode_state(value: &str) -> StateOut {
    if value.is_empty() {
        return StateOut::default();
    }
    match AuthnState::decode(value) {
        Some(state) => StateOut {
            ok: true,
            csrf: if state.csrf_token.is_empty() {
                String::new()
            } else {
                "<csrf>".to_owned()
            },
            redirect: state.redirect,
            provider_id: state.provider_id,
            nonce_equals_state: false,
        },
        None => StateOut::default(),
    }
}

fn state_from_request(case: &Case, location: &str) -> StateOut {
    let query = parse_query(&case.query);
    let mut value = query_get(&query, "state");
    let mut nonce = String::new();
    if case.path.ends_with("/login") && !location.is_empty() {
        if let Some(parsed) = go_parse_url(location) {
            let location_query = parse_query(&parsed.raw_query);
            let state_param = query_get(&location_query, "state");
            if !state_param.is_empty() {
                value = state_param;
                nonce = query_get(&location_query, "nonce");
            }
        }
    }
    let mut out = decode_state(&value);
    if !nonce.is_empty() && !value.is_empty() {
        out.nonce_equals_state = nonce == value;
    }
    out
}

/// `session.GetData`'s `json.Unmarshal(..., &string)`: `null` is a no-op
/// (empty string), any other non-string is an error (also an empty string).
fn decode_stored_string(bytes: &[u8]) -> String {
    if bytes == b"null" {
        return String::new();
    }
    serde_json::from_slice::<String>(bytes).unwrap_or_default()
}

async fn read_session(
    store: &dyn SessionStore,
    options: WriteOptions,
    now: OffsetDateTime,
    cookie: &str,
) -> SessionOut {
    let ids = FixedSessionIdSource(GENERATED_ID.to_owned());
    let mut manager = SessionManager::new(store, &ids, GOOD_KEY, options, now, Some(cookie), false);
    let mut out = SessionOut {
        has_cookie: true,
        ..SessionOut::default()
    };
    let stored = match manager.get_data("oidcState").await {
        Ok(Some(bytes)) => decode_stored_string(&bytes),
        _ => String::new(),
    };
    if !stored.is_empty() {
        let state = decode_state(&stored);
        out.state_ok = state.ok;
        out.state = state;
    }
    if let Ok(Some(bytes)) = manager.get_data("actor").await {
        if bytes != b"null" {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if let Some(uid) = value
                    .get("actor")
                    .and_then(|actor| actor.get("UID").or_else(|| actor.get("uid")))
                    .and_then(serde_json::Value::as_i64)
                {
                    out.actor_present = true;
                    out.actor_uid = uid as i32;
                }
            }
        }
    }
    if let Ok(Some(bytes)) = manager.get_data("oidc@0").await {
        out.oidc_data = String::from_utf8_lossy(&bytes).into_owned();
    }
    out
}

// ---------------------------------------------------------------------------
// Case execution.
// ---------------------------------------------------------------------------

fn build_clients(case: &Case) -> (Arc<dyn OidcHttpClient>, Arc<dyn OAuthHttpClient>) {
    let mut routes = HashMap::new();
    let mut token_routes = HashMap::new();
    let mut token_modes = HashMap::new();
    for (issuer, op) in &case.op {
        routes.insert(
            format!(
                "{}/.well-known/openid-configuration",
                issuer.trim_end_matches('/')
            ),
            (op.discovery.status, op.discovery.body.clone().into_bytes()),
        );
        if let Ok(document) = serde_json::from_str::<serde_json::Value>(&op.discovery.body) {
            if let Some(jwks_uri) = document.get("jwks_uri").and_then(|value| value.as_str()) {
                routes.insert(
                    jwks_uri.to_owned(),
                    (op.jwks.status, op.jwks.body.clone().into_bytes()),
                );
            }
            if let Some(userinfo_endpoint) = document
                .get("userinfo_endpoint")
                .and_then(|value| value.as_str())
            {
                routes.insert(
                    userinfo_endpoint.to_owned(),
                    (op.userinfo.status, op.userinfo.body.clone().into_bytes()),
                );
            }
            if let Some(token_endpoint) = document
                .get("token_endpoint")
                .and_then(|value| value.as_str())
            {
                token_routes.insert(
                    token_endpoint.to_owned(),
                    (op.token.status, op.token.body.clone().into_bytes()),
                );
                token_modes.insert(token_endpoint.to_owned(), op.token_mode.clone());
            }
        }
    }
    (
        Arc::new(FakeOidcHttp { routes }),
        Arc::new(FakeToken {
            routes: token_routes,
            modes: token_modes,
        }),
    )
}

async fn run_case(case: &Case) -> Output {
    let site = SiteConfiguration::parse(&case.site).expect("site configuration");
    let config = OAuthRouteConfig::from_site(&site, false);
    let now = OffsetDateTime::from_unix_timestamp(case.now_unix).expect("timestamp");
    let options = WriteOptions::new(config.secure);

    let store: Arc<dyn SessionStore> = Arc::new(RecordingStore::default());
    let mut cookie = if case.cookie.is_empty() {
        None
    } else {
        Some(case.cookie.clone())
    };
    if !case.seed.is_empty() {
        let value = match case.seed.as_str() {
            "state" => {
                let query = parse_query(&case.query);
                serde_json::to_vec(&query_get(&query, "state")).expect("state json")
            }
            raw => {
                let raw = raw.strip_prefix("raw:").expect("raw seed");
                raw.as_bytes().to_vec()
            }
        };
        cookie = Some(seed_cookie(store.as_ref(), options, now, &value).await);
    }

    let (oidc_http, token_http) = build_clients(case);
    let factory_state = Arc::new(Mutex::new(FactoryState::default()));
    let factory = Arc::new(FakeFactory {
        state: Arc::clone(&factory_state),
        outcome: case.provision.outcome.clone(),
        user_id: case.provision.user_id,
        safe_message: case.provision.safe_message.clone(),
    });
    let events = Arc::new(RecordingEvents::default());
    let license_state = match case.license.as_str() {
        "expired" => LicenseState::Expired,
        "error" => LicenseState::Error,
        _ => LicenseState::Valid,
    };
    let license = Arc::new(FakeLicense(license_state));
    let user = case.user.clone().unwrap_or_default();
    let users: Arc<dyn OAuthUserLookup> = Arc::new(FakeUsers {
        site_admin: user.site_admin,
        get_error: user.get_error,
        now,
    });
    let oidc_events: Arc<dyn OAuthSecurityEventSink> = events.clone();
    let oidc_license: Arc<dyn LicenseReader> = license.clone();
    let oidc_users: Arc<dyn OAuthUserLookup> = users.clone();
    let deps_events: Arc<dyn OAuthSecurityEventSink> = events.clone();
    let deps_license: Arc<dyn LicenseReader> = license.clone();
    let runtime = OidcMiddlewareRuntime {
        discovery_http: oidc_http,
        token_http,
        store: Arc::clone(&store),
        ids: Arc::new(FixedSessionIdSource(GENERATED_ID.to_owned())),
        cookie_key: GOOD_KEY.to_vec(),
        state_source: Arc::new(FixedAuthnStateSource(FIXED_CSRF.to_owned())),
        provisioners: factory,
        users: oidc_users,
        events: oidc_events,
        license: oidc_license,
    };
    let deps = OAuthRouteDeps {
        store: Arc::clone(&store),
        ids: Arc::new(FixedSessionIdSource(GENERATED_ID.to_owned())),
        state_source: Arc::new(sg_service_frontend::oauth_login::FixedStateSource(
            "unused".to_owned(),
        )),
        cookie_key: GOOD_KEY.to_vec(),
        license: deps_license,
        http: Arc::new(NoExchange),
        users,
        events: deps_events,
        resolvers: Arc::new(OAuthUserResolverRegistry::new()),
        oidc: Some(Arc::new(runtime)),
    };

    let request = build_request(case, cookie.as_deref());
    let response = match auth_prefix_response_at(&request, &config, &deps, now).await {
        Some(response) => response,
        None => append_outer_vary(next_response()),
    };
    output_for(
        case,
        response,
        store.as_ref(),
        options,
        now,
        &factory_state,
        &events,
    )
    .await
}

async fn output_for(
    case: &Case,
    response: http::Response<tonic::body::BoxBody>,
    store: &dyn SessionStore,
    options: WriteOptions,
    now: OffsetDateTime,
    factory_state: &Arc<Mutex<FactoryState>>,
    events: &RecordingEvents,
) -> Output {
    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let location = normalize_location(&header_value(&parts.headers, header::LOCATION));
    let body_bytes = http_body_util::BodyExt::collect(body)
        .await
        .expect("body")
        .to_bytes();
    let body = normalize_body(status, &location, &String::from_utf8_lossy(&body_bytes));
    let raw_cookies = header_values(&parts.headers, header::SET_COOKIE);
    let last_sgs = raw_cookies.iter().rev().find_map(|header| {
        let pair = header.split(';').next()?;
        let (name, value) = pair.split_once('=')?;
        (name == "sgs").then(|| value.to_owned())
    });
    let session = match last_sgs {
        Some(cookie) => read_session(store, options, now, &cookie).await,
        None => SessionOut::default(),
    };

    let event_out: Vec<EventOut> = events
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|event| {
            let anonymous = if event.anonymous_user_id.starts_with("unknown OIDC @ ") {
                "unknown OIDC @ <time>".to_owned()
            } else {
                event.anonymous_user_id.clone()
            };
            EventOut {
                name: event.name.clone(),
                url: event.url.clone(),
                user_id: event.user_id,
                anonymous,
                source: event.source.clone(),
                argument: serde_json::to_string(&event.argument).expect("argument"),
            }
        })
        .collect();

    let identity = {
        let state = factory_state.lock().unwrap();
        state.seen.as_ref().map(|seen| IdentityOut {
            service_type: seen.service_type.clone(),
            service_id: seen.service_id.clone(),
            client_id: state.client_id.clone(),
            account_id: seen.account_id.clone(),
            login: seen.login.clone(),
            email: seen.email.clone(),
            email_verified: seen.email_verified,
            display_name: seen.display_name.clone(),
            create_if_not_exist: state.create_if_not_exist,
        })
    };

    Output {
        name: case.name.clone(),
        status,
        content_type: header_value(&parts.headers, header::CONTENT_TYPE),
        location: location.clone(),
        body,
        cookies: raw_cookies
            .iter()
            .map(|header| normalize_cookie(header))
            .collect(),
        vary: header_values(&parts.headers, header::VARY),
        nosniff: header_value(&parts.headers, header::X_CONTENT_TYPE_OPTIONS),
        state: state_from_request(case, &location),
        events: event_out,
        identity,
        session,
    }
}

fn go_escape(input: &str) -> String {
    go_json_escape(input)
}

fn write_output(out: &str, output: &Output) {
    let mut json = serde_json::to_string_pretty(output).expect("encode");
    json = go_escape(&json);
    std::fs::write(
        Path::new(out).join(format!("{}.json", output.name)),
        json.as_bytes(),
    )
    .expect("write");
    println!(
        "RUST {} status={} events={} cookies={}",
        output.name,
        output.status,
        output.events.len(),
        output.cookies.len()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn oidc_middleware_diff() {
    if std::env::var("SG_OIDC_MIDDLEWARE_HARNESS").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("OIDC_MIDDLEWARE_CORPUS").expect("OIDC_MIDDLEWARE_CORPUS");
    let out_dir = std::env::var("OIDC_MIDDLEWARE_OUT").expect("OIDC_MIDDLEWARE_OUT");
    let corpus: Vec<Case> =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    for case in &corpus {
        let output = run_case(case).await;
        write_output(&out_dir, &output);
    }
    println!("RUST_CASES {}", corpus.len());
}
