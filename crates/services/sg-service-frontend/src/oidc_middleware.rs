#![forbid(unsafe_code)]

//! OpenID Connect login/callback middleware
//! (`cmd/frontend/internal/auth/openidconnect/middleware.go`), M9.8js.
//!
//! The baseline mounts a provider-specific middleware on the App listener:
//!
//! * it rewrites the legacy `/.auth/callback` redirect URI to
//!   `/.auth/openidconnect/callback` and takes over every
//!   `/.auth/openidconnect/` path (`middleware.go:103-117`);
//! * `/login` selects a provider by `pc`, runs discovery, stores an
//!   [`AuthnState`] in the `oidcState` session value and redirects to the OP's
//!   authorization endpoint with the state as nonce (`middleware.go:158-182`,
//!   `487-530`);
//! * `/callback` validates the state cookie, exchanges the code, verifies the
//!   id_token/nonce, fetches userinfo, provisions the user, writes the actor
//!   into the same session and redirects through `SafeRedirectURL`
//!   (`middleware.go:184-232`, `249-457`).
//!
//! Discovery/JWKS/userinfo and the id_token checks are reused from
//! [`crate::oidc_login`] (M9.8jr); the session, redirect, URL and token
//! exchange helpers are reused from [`crate::oauth_login`] (M9.8jk/M9.8jo).
//! This module only adds the request-level orchestration and the exact
//! ordering of security events, which differs from the generic OAuth flow.

use std::io::Read as _;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use http::Request;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_session::write::{SessionManager, WriteOptions};
use sg_session::{SessionIdSource, SessionStore, COOKIE_NAME};

use crate::actor_chain::LicenseReader;
use crate::authz::LicenseState;
use crate::gitlab_oauth::go_quote;
use crate::oauth_login::{
    add_post_auth_redirect_parameters_to_string, encode_query_sorted, error_response,
    go_json_escape, parse_query, parse_token_response, query_get, redirect_response,
    safe_redirect_url, OAuthAuthStyle, OAuthFlowResponse, OAuthHttpClient,
    OAuthSecurityEventRecord, OAuthSecurityEventSink, OAuthToken, OAuthUserLookup,
    TokenExchangeRequest,
};
use crate::oidc_login::{
    discover, get_display_name, get_login, provider_config_id, require_email_domain_safe,
    unverified_email_safe, ExternalIdentity, OidcAuthResult, OidcHttpClient, OidcProvider,
    OpenIdConnectAuthProvider, ProvisionOutcome, Provisioner, DISCOVERY_SAFE, FAILURE_EVENT_NAME,
    MISCONFIGURED_SAFE, NO_EMAIL_SAFE, NO_ID_TOKEN_SAFE, SUCCESS_EVENT_NAME, VERIFY_SAFE,
};
use crate::user_provisioning::{normalize_username, OsUsernameSuffix};

/// `authPrefix` (`middleware.go:36`).
const AUTH_PREFIX: &str = "/.auth/openidconnect";
/// The legacy redirect URI the baseline rewrites (`middleware.go:108-111`).
const CALLBACK_ALIAS: &str = "/.auth/callback";
/// The rewritten callback path (`r.URL.Path` used by security events).
const CALLBACK_PATH: &str = "/.auth/openidconnect/callback";
const SESSION_KEY: &str = "oidcState";
const OIDC_SESSION_KEY: &str = "oidc@0";
const SOURCE: &str = "BACKEND";
const SESSION_ERROR: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: could not initiate session.";
const TOKEN_EXCHANGE_SAFE: &str =
    "Authentication failed. Try signing in again. The error was: unable to obtain access token from issuer.";
const STATE_MISSING_SAFE: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). No OpenID Connect state query parameter specified.";
const STATE_COOKIE_SAFE: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: no state cookie found.";
const STATE_MISMATCH_SAFE: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: state parameter did not match the expected value (possible request forgery).";
const STATE_MALFORMED_SAFE: &str =
    "Authentication failed. OpenID Connect state parameter was malformed.";
const SAVE_STATE_SAFE: &str = "Failed to saving state to session";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// `AuthnState` (`middleware.go:461-467`). Field order matters because
/// [`Self::encode`] writes Go's `json.Marshal` order; the baseline uses
/// `base64.StdEncoding` (not RawURL).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthnState {
    #[serde(rename = "csrfToken", default)]
    pub csrf_token: String,
    #[serde(rename = "redirect", default)]
    pub redirect: String,
    /// `AuthnState.p`: allows `/.auth/callback` to demux multiple OPs.
    #[serde(rename = "p", default)]
    pub provider_id: String,
}

impl AuthnState {
    /// `AuthnState.Encode`: Go JSON (HTML-escaped) then `base64.StdEncoding`.
    pub fn encode(&self) -> String {
        let json = serde_json::to_string(self).expect("AuthnState serializes");
        STANDARD.encode(go_json_escape(&json).as_bytes())
    }

    /// `AuthnState.Decode`. `json.Unmarshal` treats a literal `null` as a
    /// successful no-op, so `null` decodes to the zero value.
    pub fn decode(encoded: &str) -> Option<Self> {
        let bytes = STANDARD.decode(encoded).ok()?;
        if bytes == b"null" {
            return Some(Self::default());
        }
        serde_json::from_slice(&bytes).ok()
    }
}

/// `uuid.NewV4().String()` for `AuthnState.CSRFToken` (`middleware.go:502`).
pub trait AuthnStateSource: Send + Sync {
    fn csrf_token(&self) -> String;
}

pub struct OsAuthnStateSource;

impl AuthnStateSource for OsAuthnStateSource {
    fn csrf_token(&self) -> String {
        let mut bytes = [0u8; 16];
        let mut file = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
        file.read_exact(&mut bytes).expect("read /dev/urandom");
        uuid_v4(&mut bytes)
    }
}

/// Deterministic source for the differential harness.
#[derive(Debug, Clone)]
pub struct FixedAuthnStateSource(pub String);

impl AuthnStateSource for FixedAuthnStateSource {
    fn csrf_token(&self) -> String {
        self.0.clone()
    }
}

fn uuid_v4(bytes: &mut [u8; 16]) -> String {
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

/// Builds the M9.8jp `GetAndSaveUser` adapter for one provider config. The
/// baseline takes `CreateIfNotExist` from `AllowSignup` (`user.go:92`), so the
/// factory receives the config.
pub trait OidcProvisionerFactory: Send + Sync {
    fn for_config(&self, config: &OpenIdConnectAuthProvider) -> Arc<dyn Provisioner>;
}

/// Process-wide dependencies of the OIDC middleware. Per-configuration values
/// (providers, external URL, secure cookie flag, expiry) live in
/// [`OidcMiddlewareConfig`], which is rebuilt per configuration generation.
pub struct OidcMiddlewareRuntime {
    pub discovery_http: Arc<dyn OidcHttpClient>,
    pub token_http: Arc<dyn OAuthHttpClient>,
    pub store: Arc<dyn SessionStore>,
    pub ids: Arc<dyn SessionIdSource>,
    /// `SRC_SESSION_COOKIE_KEY`.
    pub cookie_key: Vec<u8>,
    pub state_source: Arc<dyn AuthnStateSource>,
    pub provisioners: Arc<dyn OidcProvisionerFactory>,
    pub users: Arc<dyn OAuthUserLookup>,
    pub events: Arc<dyn OAuthSecurityEventSink>,
    pub license: Arc<dyn LicenseReader>,
}

/// Everything the middleware derives from the live site configuration.
#[derive(Debug, Clone, Default)]
pub struct OidcMiddlewareConfig {
    pub configs: Vec<OpenIdConnectAuthProvider>,
    pub external_url: String,
    /// `conf.ExternalURLParsed().Scheme == "https"`.
    pub secure: bool,
    /// `conf.Get().AuthSessionExpiry`.
    pub configured_expiry: Option<String>,
    /// `dotcom.SourcegraphDotComMode()`; consumed by `NormalizeUsername`.
    pub dotcom: bool,
}

/// `auth.providers` entries of type `openidconnect`.
pub fn oidc_configs(site: &SiteConfiguration) -> Vec<OpenIdConnectAuthProvider> {
    let entries = site
        .raw()
        .get("auth.providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut configs = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some("openidconnect") {
            continue;
        }
        let Ok(mut config) =
            serde_json::from_value::<OpenIdConnectAuthProvider>(Value::Object(obj.clone()))
        else {
            continue;
        };
        if config.provider_type.is_empty() {
            config.provider_type = "openidconnect".to_owned();
        }
        configs.push(config);
    }
    configs
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Handles `/.auth/openidconnect/{login,callback}` and the legacy
/// `/.auth/callback` alias. `None` means the path is not taken over (the exact
/// prefix without a trailing slash is not delegated in the baseline either).
pub async fn handle_oidc_auth<B>(
    req: &Request<B>,
    config: &OidcMiddlewareConfig,
    deps: &OidcMiddlewareRuntime,
    now: OffsetDateTime,
) -> Option<OAuthFlowResponse> {
    let path = req.uri().path();
    if path == CALLBACK_ALIAS {
        return Some(handle_callback(req, config, deps, now).await);
    }
    let subpath = path.strip_prefix(AUTH_PREFIX)?.strip_prefix('/')?;
    match subpath {
        "login" => Some(handle_login(req, config, deps, now).await),
        "callback" => Some(handle_callback(req, config, deps, now).await),
        // `http.Error(w, "", http.StatusNotFound)` (`middleware.go:230-231`).
        _ => Some(error_response(404, "")),
    }
}

// ---------------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------------

async fn handle_login<B>(
    req: &Request<B>,
    config: &OidcMiddlewareConfig,
    deps: &OidcMiddlewareRuntime,
    now: OffsetDateTime,
) -> OAuthFlowResponse {
    let query = parse_query(req.uri().query().unwrap_or_default());
    let mut redirect = query_get(&query, "redirect");
    if redirect.is_empty() {
        redirect = query_get(&query, "returnTo");
    }

    let Some(provider) = find_config(&config.configs, &query_get(&query, "pc")) else {
        return redirect_response(&format!("/sign-in?returnTo={redirect}"));
    };
    if provider.issuer.is_empty() {
        return error_response(500, MISCONFIGURED_SAFE);
    }
    let Ok(discovery) = discover(deps.discovery_http.as_ref(), &provider.issuer).await else {
        return error_response(500, DISCOVERY_SAFE);
    };

    if redirect.is_empty() || redirect == "/" {
        redirect = "/search".to_owned();
    }

    let state = AuthnState {
        csrf_token: deps.state_source.csrf_token(),
        redirect,
        provider_id: provider_config_id(provider),
    }
    .encode();

    let request_cookie = cookie_value(req, COOKIE_NAME);
    let mut manager = session_manager(config, deps, now, request_cookie.as_deref());
    let json = serde_json::to_vec(&state).expect("string serializes");
    if manager.set_data(SESSION_KEY, &json).await.is_err() {
        return error_response(500, SAVE_STATE_SAFE);
    }

    let location = authorization_url(
        &discovery.document.authorization_endpoint,
        provider,
        &callback_redirect_url(&config.external_url),
        &state,
        &query_get(&query, "prompt_auth"),
    );
    let mut response = redirect_response(&location);
    response.set_cookies = manager.cookies;
    response
}

fn find_config<'a>(
    configs: &'a [OpenIdConnectAuthProvider],
    id: &str,
) -> Option<&'a OpenIdConnectAuthProvider> {
    configs
        .iter()
        .find(|config| provider_config_id(config) == id)
}

/// `oauth2.Config.AuthCodeURL(state, oidc.Nonce(state))` plus the raw
/// `&prompt_auth=` suffix (`middleware.go:522-528`).
fn authorization_url(
    authorization_endpoint: &str,
    config: &OpenIdConnectAuthProvider,
    redirect_uri: &str,
    state: &str,
    prompt_auth: &str,
) -> String {
    let pairs = vec![
        ("response_type".to_owned(), "code".to_owned()),
        ("client_id".to_owned(), config.client_id.clone()),
        ("redirect_uri".to_owned(), redirect_uri.to_owned()),
        ("scope".to_owned(), "openid profile email".to_owned()),
        ("state".to_owned(), state.to_owned()),
        ("nonce".to_owned(), state.to_owned()),
    ];
    let mut url = authorization_endpoint.to_owned();
    url.push(if url.contains('?') { '&' } else { '?' });
    url.push_str(&encode_query_sorted(&pairs));
    if !prompt_auth.is_empty() {
        url.push_str("&prompt_auth=");
        url.push_str(prompt_auth);
    }
    url
}

/// `conf.ExternalURLParsed().ResolveReference(&url.URL{Path:"/.auth/callback"})`.
/// The redirect URI sent to the OP stays the legacy alias; the middleware
/// rewrites the *incoming* request path on the callback.
fn callback_redirect_url(external_url: &str) -> String {
    match url::Url::parse(external_url) {
        Ok(mut url) => {
            url.set_path(CALLBACK_ALIAS);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => format!("{external_url}{CALLBACK_ALIAS}"),
    }
}

// ---------------------------------------------------------------------------
// Callback
// ---------------------------------------------------------------------------

async fn handle_callback<B>(
    req: &Request<B>,
    config: &OidcMiddlewareConfig,
    deps: &OidcMiddlewareRuntime,
    now: OffsetDateTime,
) -> OAuthFlowResponse {
    let query = parse_query(req.uri().query().unwrap_or_default());

    if let Some(auth_error) = non_empty(query_get(&query, "error")) {
        let description = query_get(&query, "error_description");
        let safe = format!(
            "Authentication failed. Try signing in again (and clearing cookies for the current site). The authentication provider reported the following problems.\n\n{auth_error}\n\n{description}"
        );
        return callback_failure(deps, now, 401, &safe).await;
    }

    let state_param = query_get(&query, "state");
    if state_param.is_empty() {
        return callback_failure(deps, now, 401, STATE_MISSING_SAFE).await;
    }

    let request_cookie = cookie_value(req, COOKIE_NAME);
    let mut manager = session_manager(config, deps, now, request_cookie.as_deref());
    let stored_state: String = match manager.get_data(SESSION_KEY).await {
        Err(_) => return callback_failure(deps, now, 401, STATE_COOKIE_SAFE).await,
        Ok(None) => String::new(),
        Ok(Some(bytes)) => match decode_stored_string(&bytes) {
            Some(value) => value,
            // `session.GetData`'s `json.Unmarshal` failure is the same error as
            // a bad cookie in the baseline.
            None => return callback_failure(deps, now, 401, STATE_COOKIE_SAFE).await,
        },
    };
    if stored_state != state_param {
        return callback_failure(deps, now, 401, STATE_MISMATCH_SAFE).await;
    }

    let Some(state) = AuthnState::decode(&state_param) else {
        return callback_failure(deps, now, 400, STATE_MALFORMED_SAFE).await;
    };

    let Some(provider) = find_config(&config.configs, &state.provider_id) else {
        return callback_failure(deps, now, 500, MISCONFIGURED_SAFE).await;
    };
    if provider.issuer.is_empty() {
        return callback_failure(deps, now, 500, MISCONFIGURED_SAFE).await;
    }
    let Ok(discovery) = discover(deps.discovery_http.as_ref(), &provider.issuer).await else {
        return callback_failure(deps, now, 500, DISCOVERY_SAFE).await;
    };

    let token = match exchange_code(
        deps,
        provider,
        &discovery,
        &callback_redirect_url(&config.external_url),
        &query_get(&query, "code"),
    )
    .await
    {
        Ok(token) => token,
        Err(()) => return callback_failure(deps, now, 401, TOKEN_EXCHANGE_SAFE).await,
    };
    let Some(Value::String(raw_id_token)) = token.extra("id_token") else {
        return callback_failure(deps, now, 401, NO_ID_TOKEN_SAFE).await;
    };

    // Reuses jr's verification chain with the state as the expected nonce.
    let resolver = crate::oidc_login::OidcLoginResolver::new(
        provider.clone(),
        Arc::clone(&deps.discovery_http),
        Arc::new(NoopProvisioner),
        now.unix_timestamp(),
    )
    .with_expected_nonce(state_param.clone());
    let auth = resolver
        .authorize_with_provider(
            &discovery,
            &token.access_token,
            &token.token_type,
            &raw_id_token,
        )
        .await;
    if let Some(safe) = &auth.safe_message {
        let status = validation_error_status(&auth, provider);
        return callback_failure(deps, now, status, safe).await;
    }
    let Some(userinfo) = &auth.userinfo else {
        return callback_failure(deps, now, 500, VERIFY_SAFE).await;
    };
    let Some(id_token) = &auth.id_token else {
        return callback_failure(deps, now, 401, VERIFY_SAFE).await;
    };

    let login = get_login(&userinfo.claims, userinfo);
    let display_name = get_display_name(&userinfo.claims, &login);
    // `openidconnect/user.go:68-75`: the login is normalized inside
    // `getOrCreateUser`; the OIDC middleware passes no username prefix.
    let login = match normalize_username(&login, &OsUsernameSuffix, config.dotcom) {
        Ok(login) => login,
        Err(_) => {
            // `NormalizeUsername` returns `""` alongside every error, and
            // `getOrCreateUser` assigns that result back to `login` before
            // formatting the message (`user.go:72-75`).
            let safe = format!(
                "Error normalizing the username {}. See https://sourcegraph.com/docs/admin/auth/#username-normalization.",
                go_quote("")
            );
            return callback_failure(deps, now, 500, &safe).await;
        }
    };
    let provisioner = deps.provisioners.for_config(provider);
    let outcome = provisioner
        .get_or_create_user(&ExternalIdentity {
            service_type: &provider.provider_type,
            service_id: &provider.issuer,
            client_id: &provider.client_id,
            account_id: &id_token.subject,
            login: &login,
            email: &userinfo.email,
            email_verified: true,
            display_name: Some(&display_name),
            avatar_url: &userinfo.claims.picture,
        })
        .await;
    let (user_id, new_user) = match outcome {
        Ok(ProvisionOutcome::Created { user_id }) => (user_id, true),
        Ok(ProvisionOutcome::Existing { user_id }) => (user_id, false),
        Err(error) => return callback_failure(deps, now, 500, &error.safe_message).await,
    };

    let user = match deps.users.get_by_id(user_id).await {
        Ok(user) => user,
        Err(_) => {
            return callback_failure(deps, now, 500, "Failed to retrieve user from database").await
        }
    };

    // `middleware.go:200-202`: the success event is logged before the session
    // is written (unlike the generic OAuth flow), so a session/license failure
    // still leaves a success event.
    log_event(deps, now, SUCCESS_EVENT_NAME, user.id, "", Value::Null).await;

    match deps.license.product_license().await {
        LicenseState::Error => return error_response(500, SESSION_ERROR),
        LicenseState::Expired if !user.site_admin => return error_response(500, SESSION_ERROR),
        _ => {}
    }

    let actor = sg_session::session::Actor {
        uid: user.id,
        ..Default::default()
    };
    if manager
        .set_actor(
            Some(&actor),
            0,
            config.configured_expiry.as_deref(),
            user.created_at,
        )
        .await
        .is_err()
    {
        return error_response(500, SESSION_ERROR);
    }

    // `SessionData{ID providers.ConfigID; AccessToken; TokenType}`
    // (`openidconnect/session.go:35-45`); a failure is not fatal. A struct (not
    // a `json!` map) is required because the workspace does not enable
    // `serde_json`'s `preserve_order`, so map keys would be sorted instead of
    // following Go's field order.
    #[derive(Serialize)]
    struct SessionConfigId<'a> {
        #[serde(rename = "Type")]
        provider_type: &'a str,
        #[serde(rename = "ID")]
        id: String,
    }
    #[derive(Serialize)]
    struct SessionData<'a> {
        #[serde(rename = "ID")]
        id: SessionConfigId<'a>,
        #[serde(rename = "AccessToken")]
        access_token: &'a str,
        #[serde(rename = "TokenType")]
        token_type: &'a str,
    }
    let session_data = SessionData {
        id: SessionConfigId {
            provider_type: &provider.provider_type,
            id: provider_config_id(provider),
        },
        access_token: &token.access_token,
        token_type: &token.token_type,
    };
    let bytes = serde_json::to_vec(&session_data).expect("session data serializes");
    let _ = manager.set_data(OIDC_SESSION_KEY, &bytes).await;

    let redirect =
        add_post_auth_redirect_parameters_to_string(&state.redirect, new_user, "OpenIDConnect");
    let mut response = redirect_response(&safe_redirect_url(&redirect));
    response.set_cookies = manager.cookies;
    response
}

/// The status each `AuthCallback`/`getOrCreateUser` failure branch uses.
fn validation_error_status(auth: &OidcAuthResult, config: &OpenIdConnectAuthProvider) -> u16 {
    if auth.id_token.is_none() {
        return 401; // verifier error
    }
    if auth.nonce_checked && !auth.nonce_ok {
        return 401; // nonce mismatch
    }
    if auth.userinfo.is_none() {
        return 500; // `UserInfo` error is surfaced with InternalServerError
    }
    let Some(message) = auth.safe_message.as_deref() else {
        return 401;
    };
    if message == NO_EMAIL_SAFE {
        return 500;
    }
    if let Some(userinfo) = &auth.userinfo {
        if message == unverified_email_safe(&userinfo.email) {
            return 500;
        }
    }
    if !config.require_email_domain.is_empty()
        && message == require_email_domain_safe(&config.require_email_domain)
    {
        return 401;
    }
    401
}

async fn callback_failure(
    deps: &OidcMiddlewareRuntime,
    now: OffsetDateTime,
    status: u16,
    safe_message: &str,
) -> OAuthFlowResponse {
    log_event(
        deps,
        now,
        FAILURE_EVENT_NAME,
        0,
        &format!("unknown OIDC @ {}", go_time_now_string(now)),
        json!({"safe_error_msg": safe_message}),
    )
    .await;
    error_response(status, safe_message)
}

async fn log_event(
    deps: &OidcMiddlewareRuntime,
    now: OffsetDateTime,
    name: &str,
    user_id: i32,
    anonymous_user_id: &str,
    argument: Value,
) {
    let _ = deps
        .events
        .log_security_event(OAuthSecurityEventRecord {
            name: name.to_owned(),
            url: CALLBACK_PATH.to_owned(),
            user_id,
            anonymous_user_id: anonymous_user_id.to_owned(),
            source: SOURCE.to_owned(),
            argument,
            timestamp: now,
        })
        .await;
}

/// `x/oauth2.Config.Exchange` + `internal.RetrieveToken`'s auth-style probe:
/// try the Basic-auth style first, retry with credentials in the form on any
/// error (same helper shape as the generic flow).
async fn exchange_code(
    deps: &OidcMiddlewareRuntime,
    provider: &OpenIdConnectAuthProvider,
    discovery: &OidcProvider,
    redirect_url: &str,
    code: &str,
) -> Result<OAuthToken, ()> {
    let (_, token_url) = discovery.endpoint();
    let base = TokenExchangeRequest {
        token_url,
        client_id: provider.client_id.clone(),
        client_secret: provider.client_secret.clone(),
        code: code.to_owned(),
        redirect_url: redirect_url.to_owned(),
        auth_style: OAuthAuthStyle::InHeader,
    };
    match deps.token_http.exchange_code(&base).await {
        Ok(response) => match parse_token_response(&response) {
            Ok(token) => Ok(token),
            Err(_) => retry_in_params(deps, &base).await,
        },
        Err(_) => retry_in_params(deps, &base).await,
    }
}

async fn retry_in_params(
    deps: &OidcMiddlewareRuntime,
    base: &TokenExchangeRequest,
) -> Result<OAuthToken, ()> {
    let retry = TokenExchangeRequest {
        auth_style: OAuthAuthStyle::InParams,
        ..base.clone()
    };
    let response = deps
        .token_http
        .exchange_code(&retry)
        .await
        .map_err(|_| ())?;
    parse_token_response(&response).map_err(|_| ())
}

struct NoopProvisioner;

#[async_trait]
impl Provisioner for NoopProvisioner {
    async fn get_or_create_user(
        &self,
        _identity: &ExternalIdentity<'_>,
    ) -> Result<ProvisionOutcome, crate::oidc_login::ProvisionError> {
        Err(crate::oidc_login::ProvisionError {
            safe_message: String::new(),
            message: "not used".to_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// `json.Unmarshal(data, &string)` used by `session.GetData`: `null` is a
/// successful no-op (empty string), any other non-string is an error.
fn decode_stored_string(bytes: &[u8]) -> Option<String> {
    if bytes == b"null" {
        return Some(String::new());
    }
    serde_json::from_slice(bytes).ok()
}

fn session_manager<'a>(
    config: &OidcMiddlewareConfig,
    deps: &'a OidcMiddlewareRuntime,
    now: OffsetDateTime,
    request_cookie: Option<&'a str>,
) -> SessionManager<'a> {
    SessionManager::new(
        deps.store.as_ref(),
        deps.ids.as_ref(),
        &deps.cookie_key,
        WriteOptions::new(config.secure),
        now,
        request_cookie,
        false,
    )
}

fn cookie_value<B>(req: &Request<B>, name: &str) -> Option<String> {
    for value in req.headers().get_all(http::header::COOKIE) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for part in value.split(';') {
            let part = part.trim();
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            if key == name {
                return Some(value.to_owned());
            }
        }
    }
    None
}

/// `time.Now().String()` (UTC) without the unstable monotonic suffix. The
/// differential normalizes the whole anonymous ID, so only the prefix is
/// compared.
fn go_time_now_string(now: OffsetDateTime) -> String {
    let now = now.to_offset(time::UtcOffset::UTC);
    let nanos = now.nanosecond();
    let fraction = if nanos == 0 {
        String::new()
    } else {
        let mut fraction = format!(".{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        fraction
    };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}{fraction} +0000 UTC",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

// Response conversion is done by `oauth_routes::flow_response` (the same
// `net/http` emulation as the generic flow).
