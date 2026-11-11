#![forbid(unsafe_code)]

//! OAuth login/callback (`cmd/frontend/internal/auth/oauth`), shared by the
//! GitHub, GitLab and OpenID Connect providers.
//!
//! The baseline mounts two handlers below `/.auth/<provider>`:
//!
//! * `/login` (`newOAuthFlowHandler`): `pc` selects a provider; a synthetic
//!   provider returns `302 /sign-in?returnTo=…`, otherwise the provider's
//!   `stateHandler` runs. It generates a random CSRF value, resolves the
//!   post-login redirect, encodes a `LoginState` into `oauthState` in the
//!   Redis session (and therefore the `sgs` cookie), then redirects to the
//!   provider's authorization URL with that encoded state.
//! * `/callback`: decodes the URL `state`, finds the provider by
//!   `state.ProviderID`, checks it against the `oauthState` session value, and
//!   exchanges the code with `x/oauth2`. On success it calls the provider's
//!   `SessionIssuer`, which resolves/creates the user, writes the actor into
//!   the same session (`SetActorFromUser`), logs a success security event and
//!   redirects to `SafeRedirectURL(redirect + signin|signup)`.
//!
//! Everything outside the HTTP/session/state machinery is injected:
//! [`StateSource`] (CSRF), [`OAuthHttpClient`] (token exchange; no real
//! network is required), [`OAuthUserResolver`] (provider-specific id_token /
//! user API work), [`OAuthUserLookup`] (the `Users().GetByID` the issuer
//! performs) and [`OAuthSecurityEventSink`]. This slice deliberately does not
//! port provider REST calls, OIDC discovery/JWKS or SAML; see the overview's
//! errata.

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_db::security_event_logs::{insert_security_event, SecurityEvent};
use sg_session::session::{Actor, GoTime};
use sg_session::write::{SessionManager, SetCookie, WriteOptions};
use sg_session::{SessionIdSource, SessionStore};

/// `oauth.Provider.ConfigID().ID`.
pub fn provider_config_id(service_id: &str, client_id: &str) -> String {
    format!("{service_id}::{client_id}")
}

/// One configured OAuth provider (`oauth.Provider` + `oauth2.Config`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthProviderConfig {
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub client_secret: String,
    pub auth_url: String,
    pub token_url: String,
    pub scopes: Vec<String>,
    pub redirect_url: String,
    /// `providers.ConfigID.ID` (usually `ServiceID::ClientID`; OIDC may use an
    /// explicit/hashed config ID).
    pub config_id: String,
    /// `gitlaboauth`'s `ssoURL`; empty for every other service type. When set,
    /// the provider's login handler rewrites the redirect to the SSO URL
    /// (`SSOLoginHandler`).
    pub sso_url: String,
}

impl OAuthProviderConfig {
    /// `providers.ConfigID.ID`.
    pub fn config_id(&self) -> String {
        self.config_id.clone()
    }

    /// `oauth2.Config.AuthCodeURL` (`oauth2.go:160-186`).
    pub fn auth_code_url(&self, state: &str) -> String {
        let mut values: Vec<(String, String)> = vec![
            ("response_type".to_owned(), "code".to_owned()),
            ("client_id".to_owned(), self.client_id.clone()),
        ];
        if !self.redirect_url.is_empty() {
            values.push(("redirect_uri".to_owned(), self.redirect_url.clone()));
        }
        if !self.scopes.is_empty() {
            values.push(("scope".to_owned(), self.scopes.join(" ")));
        }
        if !state.is_empty() {
            values.push(("state".to_owned(), state.to_owned()));
        }
        let separator = if self.auth_url.contains('?') {
            '&'
        } else {
            '?'
        };
        format!(
            "{}{}{}",
            self.auth_url,
            separator,
            encode_query_sorted(&values)
        )
    }
}

/// The `auth.providers` derived OAuth provider set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthProviders {
    pub providers: Vec<OAuthProviderConfig>,
}

impl OAuthProviders {
    pub fn from_configs(providers: Vec<OAuthProviderConfig>) -> Self {
        Self { providers }
    }

    /// `oauth.GetProvider(serviceType, id)` where `id` is the encoded
    /// `ConfigID` (`ServiceID::ClientID`).
    pub fn find(&self, service_type: &str, id: &str) -> Option<&OAuthProviderConfig> {
        self.providers
            .iter()
            .find(|provider| provider.service_type == service_type && provider.config_id() == id)
    }

    /// Reads `auth.providers` for the three OAuth families this slice shares.
    ///
    /// Config IDs come from the M9.8jg catalog (`AuthProvidersRoot`), so the
    /// duplicate-elimination/hash rules stay in one place. GitHub/GitLab
    /// derive their endpoints and scopes from the code host URL
    /// (`githuboauth/provider.go`, `gitlaboauth/provider.go`). OpenID Connect
    /// endpoint discovery is network-bound and stays a seam: the entry keeps
    /// its issuer/config ID but no auth/token URL.
    pub fn from_site(site: &SiteConfiguration, sourcegraph_dotcom: bool) -> Self {
        let raw = site.raw();
        let external_url = raw
            .get("externalURL")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let catalog = crate::auth_providers_graphql::AuthProvidersRoot::from_site(site);
        let entries = raw
            .get("auth.providers")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();

        let mut providers = Vec::new();
        for entry in entries {
            let Some(obj) = entry.as_object() else {
                continue;
            };
            let service_type = obj.get("type").and_then(Value::as_str).unwrap_or_default();
            let client_id = string_field(obj, "clientID");
            let client_secret = string_field(obj, "clientSecret");
            match service_type {
                "github" => {
                    let service_id =
                        normalize_codehost_url(&string_field(obj, "url"), "https://github.com/");
                    if client_id.is_empty() {
                        continue;
                    }
                    let base = service_id.trim_end_matches('/').to_owned();
                    let mut scopes = vec!["user:email".to_owned()];
                    if !sourcegraph_dotcom {
                        scopes.push("repo".to_owned());
                    }
                    if obj
                        .get("allowOrgs")
                        .and_then(Value::as_array)
                        .is_some_and(|orgs| !orgs.is_empty())
                        || obj
                            .get("allowGroupsPermissionsSync")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                    {
                        scopes.push("read:org".to_owned());
                    }
                    providers.push(OAuthProviderConfig {
                        service_type: service_type.to_owned(),
                        config_id: provider_config_id(&service_id, &client_id),
                        service_id,
                        client_id,
                        client_secret,
                        auth_url: format!("{base}/login/oauth/authorize"),
                        token_url: format!("{base}/login/oauth/access_token"),
                        scopes,
                        // `githuboauth/provider.go` does *not* set
                        // `oauth2.Config.RedirectURL` (unlike GitLab), so
                        // `AuthCodeURL` has no `redirect_uri` parameter.
                        redirect_url: String::new(),
                        sso_url: String::new(),
                    });
                }
                "gitlab" => {
                    // `gitlaboauth.parseConfig` drops every GitLab entry when
                    // `externalURL` is empty (`gitlaboauth/config.go:62-65`):
                    // it cannot build the OAuth callback URL.
                    if external_url.is_empty() {
                        continue;
                    }
                    let service_id =
                        normalize_codehost_url(&string_field(obj, "url"), "https://gitlab.com/");
                    if client_id.is_empty() {
                        continue;
                    }
                    let base = service_id.trim_end_matches('/').to_owned();
                    let api_scope = string_field(obj, "apiScope");
                    let mut scopes = vec!["read_user".to_owned()];
                    scopes.push(if api_scope.is_empty() {
                        "api".to_owned()
                    } else {
                        api_scope
                    });
                    providers.push(OAuthProviderConfig {
                        service_type: service_type.to_owned(),
                        config_id: provider_config_id(&service_id, &client_id),
                        service_id,
                        client_id,
                        client_secret,
                        auth_url: format!("{base}/oauth/authorize"),
                        token_url: format!("{base}/oauth/token"),
                        scopes,
                        redirect_url: oauth_callback_url(external_url, service_type),
                        sso_url: string_field(obj, "ssoURL"),
                    });
                }
                "openidconnect" => {
                    let issuer = string_field(obj, "issuer");
                    if client_id.is_empty() {
                        continue;
                    }
                    let Some(config_id) =
                        catalog_config_id(&catalog, service_type, &issuer, &client_id)
                    else {
                        continue;
                    };
                    providers.push(OAuthProviderConfig {
                        service_type: service_type.to_owned(),
                        config_id,
                        service_id: issuer,
                        client_id,
                        client_secret,
                        // Filled by OIDC discovery in the baseline; seam.
                        auth_url: String::new(),
                        token_url: String::new(),
                        scopes: vec![
                            "openid".to_owned(),
                            "profile".to_owned(),
                            "email".to_owned(),
                        ],
                        redirect_url: oauth_callback_url(external_url, service_type),
                        sso_url: String::new(),
                    });
                }
                _ => {}
            }
        }
        Self { providers }
    }
}

fn string_field(obj: &serde_json::Map<String, Value>, key: &str) -> String {
    obj.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn normalize_codehost_url(url: &str, default: &str) -> String {
    let url = if url.is_empty() { default } else { url };
    let mut normalized = url.to_owned();
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn oauth_callback_url(external_url: &str, service_type: &str) -> String {
    if external_url.is_empty() {
        return String::new();
    }
    format!(
        "{}/.auth/{}/callback",
        external_url.trim_end_matches('/'),
        service_type
    )
}

fn catalog_config_id(
    catalog: &crate::auth_providers_graphql::AuthProvidersRoot,
    service_type: &str,
    service_id: &str,
    client_id: &str,
) -> Option<String> {
    catalog
        .providers
        .iter()
        .find(|info| {
            info.service_type == service_type
                && info.service_id == service_id
                && info.client_id == client_id
        })
        .map(|info| {
            // `AuthProviderInfo` exposes the rendered authentication URL, not
            // the ConfigID, for OIDC. Recover it from `?pc=`.
            info.authentication_url
                .as_deref()
                .and_then(|url| {
                    let (_, query) = url.split_once('?')?;
                    parse_query(query).into_iter().find(|(key, _)| key == "pc")
                })
                .map(|(_, value)| value)
                .unwrap_or_else(|| provider_config_id(service_id, client_id))
        })
}

/// `oauth.LoginState` (field order matters: it is JSON-marshalled into the
/// base64 state).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginState {
    #[serde(rename = "Redirect", default)]
    pub redirect: String,
    #[serde(rename = "ProviderID", default)]
    pub provider_id: String,
    #[serde(rename = "CSRF", default)]
    pub csrf: String,
    #[serde(rename = "Op", default)]
    pub op: String,
}

impl LoginState {
    /// Go `json.Marshal` bytes (HTML-escaped), before base64.
    pub fn to_go_json(&self) -> Vec<u8> {
        let json = serde_json::to_string(self).expect("LoginState serializes");
        go_json_escape(&json).into_bytes()
    }

    /// `LoginState.Encode`: JSON → base64 RawURL.
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.to_go_json())
    }

    /// `DecodeState`: base64 RawURL → JSON. `None` is a decode error.
    pub fn decode(encoded: &str) -> Option<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

/// 32 CSPRNG bytes, base64 RawURL (`oauth.randomState`).
pub trait StateSource: Send + Sync {
    fn generate(&self) -> String;
}

pub struct OsStateSource;

impl StateSource for OsStateSource {
    fn generate(&self) -> String {
        use std::io::Read as _;
        let mut bytes = [0u8; 32];
        let mut file = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
        file.read_exact(&mut bytes).expect("read /dev/urandom");
        URL_SAFE_NO_PAD.encode(bytes)
    }
}

/// Deterministic source for the differential harness.
#[derive(Debug, Clone)]
pub struct FixedStateSource(pub String);

impl StateSource for FixedStateSource {
    fn generate(&self) -> String {
        self.0.clone()
    }
}

/// The token endpoint response, before `x/oauth2` parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthHttpResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// `oauth2.Config.Exchange`'s POST. `auth_style` mirrors
/// `internal.AuthStyleInHeader`/`AuthStyleInParams`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthAuthStyle {
    InHeader,
    InParams,
}

#[derive(Debug, Clone)]
pub struct TokenExchangeRequest {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub code: String,
    pub redirect_url: String,
    pub auth_style: OAuthAuthStyle,
}

impl TokenExchangeRequest {
    /// `url.Values` sent in the POST body (`internal.newTokenRequest`).
    pub fn form(&self) -> Vec<(String, String)> {
        let mut form = vec![
            ("grant_type".to_owned(), "authorization_code".to_owned()),
            ("code".to_owned(), self.code.clone()),
        ];
        if !self.redirect_url.is_empty() {
            form.push(("redirect_uri".to_owned(), self.redirect_url.clone()));
        }
        if self.auth_style == OAuthAuthStyle::InParams {
            if !self.client_id.is_empty() {
                form.push(("client_id".to_owned(), self.client_id.clone()));
            }
            if !self.client_secret.is_empty() {
                form.push(("client_secret".to_owned(), self.client_secret.clone()));
            }
        }
        form
    }
}

#[async_trait]
pub trait OAuthHttpClient: Send + Sync {
    /// `Config.Exchange`: POST the form to the token URL. A transport error is
    /// `Err`; HTTP status/content-type/body are returned verbatim for
    /// `x/oauth2`-compatible parsing.
    async fn exchange_code(
        &self,
        request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String>;
}

/// A parsed `oauth2.Token`, including the raw payload for `Extra("id_token")`.
#[derive(Debug, Clone, PartialEq)]
pub struct OAuthToken {
    pub access_token: String,
    pub token_type: String,
    pub refresh_token: String,
    /// `expirationTime` seconds; `Some` only when the response had a non-zero
    /// `expires_in`.
    pub expires_in: Option<i64>,
    json: Option<Value>,
    form: Vec<(String, String)>,
}

impl OAuthToken {
    /// `Token.Extra(key)`'s useful case: a JSON string field (id_token).
    pub fn extra_str(&self, key: &str) -> String {
        if let Some(json) = &self.json {
            return json
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
        }
        self.form
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }

    /// Builds a token from a parsed JSON token-endpoint body exactly as
    /// `parse_token_response` does for an `application/json` response. This is
    /// the seam provider resolvers (and their differential harnesses) use to
    /// construct an [`OAuthToken`] without going through the flow. It does not
    /// change any flow semantics.
    pub fn from_json_value(json: Value) -> Option<Self> {
        if !json.is_object() {
            return None;
        }
        let expires_in = match json.get("expires_in") {
            None | Some(Value::Null) => None,
            Some(Value::Number(number)) => number.as_i64().filter(|value| *value != 0),
            Some(_) => None,
        };
        let field = |key: &str| {
            json.get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        Some(Self {
            access_token: field("access_token"),
            token_type: field("token_type"),
            refresh_token: field("refresh_token"),
            expires_in,
            json: Some(json),
            form: Vec::new(),
        })
    }
}

/// The provider's `SessionIssuerHelper.GetOrCreateUser` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserResolution {
    /// `newUserCreated` plus the actor UID (`actor.FromUser(...)`).
    Created { user_id: i32, new_user: bool },
    /// `safeErrMsg`; the issuer returns it as the 500 body.
    Failed { safe_message: String },
    /// A provider callback failure that the baseline surfaces *before* the
    /// session issuer (e.g. `gitlaboauth.CallbackHandler`'s
    /// `gologin.DefaultFailureHandler`, which answers 400 with the error
    /// text). Such a failure logs no security event and does not delete the
    /// `oauthState` session value.
    FailedWithStatus { status: u16, safe_message: String },
}

#[async_trait]
pub trait OAuthUserResolver: Send + Sync {
    /// `GetServiceID` for the `OAuth::<serviceID>` redirect parameter.
    fn service_id(&self) -> &str;
    fn success_event_name(&self) -> &str;
    fn failure_event_name(&self) -> &str;
    /// `GetOrCreateUser`; id_token/email validation lives here.
    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution;
}

/// One `Users().GetByID` row (only the fields the issuer reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthUserRecord {
    pub id: i32,
    pub created_at: GoTime,
    pub site_admin: bool,
}

#[async_trait]
pub trait OAuthUserLookup: Send + Sync {
    /// `Users().GetByID`; the baseline treats a missing user as an error.
    async fn get_by_id(&self, uid: i32) -> Result<OAuthUserRecord, String>;
}

/// `database.SecurityEvent` (`LogSecurityEvent`).
#[derive(Debug, Clone, PartialEq)]
pub struct OAuthSecurityEventRecord {
    pub name: String,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    pub argument: Value,
    pub timestamp: OffsetDateTime,
}

#[async_trait]
pub trait OAuthSecurityEventSink: Send + Sync {
    async fn log_security_event(&self, event: OAuthSecurityEventRecord) -> Result<(), String>;
}

/// Production `Users().GetByID` + `LogSecurityEvent`.
pub struct PgOAuthLoginBackend {
    pool: sqlx::PgPool,
    version: String,
    security_event_logs_in_database: bool,
}

impl PgOAuthLoginBackend {
    pub fn new(
        pool: sqlx::PgPool,
        version: impl Into<String>,
        security_event_logs_in_database: bool,
    ) -> Self {
        Self {
            pool,
            version: version.into(),
            security_event_logs_in_database,
        }
    }
}

#[async_trait]
impl OAuthUserLookup for PgOAuthLoginBackend {
    async fn get_by_id(&self, uid: i32) -> Result<OAuthUserRecord, String> {
        let user = sg_db::users::get_session_user(&self.pool, uid)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "user not found".to_owned())?;
        Ok(OAuthUserRecord {
            id: user.id,
            created_at: GoTime(user.created_at),
            site_admin: user.site_admin,
        })
    }
}

#[async_trait]
impl OAuthSecurityEventSink for PgOAuthLoginBackend {
    async fn log_security_event(&self, event: OAuthSecurityEventRecord) -> Result<(), String> {
        if !self.security_event_logs_in_database {
            return Ok(());
        }
        insert_security_event(
            &self.pool,
            &SecurityEvent {
                name: &event.name,
                url: &event.url,
                user_id: event.user_id,
                anonymous_user_id: &event.anonymous_user_id,
                source: &event.source,
                argument: event.argument,
                version: &self.version,
                timestamp: event.timestamp,
            },
        )
        .await
        .map_err(|error| error.to_string())
    }
}

/// Everything the flow needs. All non-HTTP state is injected so the
/// differential harness can pin CSRF, session IDs and time.
pub struct OAuthFlowDeps<'a> {
    pub providers: &'a OAuthProviders,
    pub store: &'a dyn SessionStore,
    pub ids: &'a dyn SessionIdSource,
    pub cookie_key: &'a [u8],
    pub secure: bool,
    pub external_url: &'a str,
    pub configured_expiry: Option<&'a str>,
    pub license_expired: bool,
    pub state_source: &'a dyn StateSource,
    pub http: &'a dyn OAuthHttpClient,
    pub resolver: &'a dyn OAuthUserResolver,
    pub users: &'a dyn OAuthUserLookup,
    pub events: &'a dyn OAuthSecurityEventSink,
    pub now: OffsetDateTime,
}

pub struct OAuthLoginRequest<'a> {
    pub service_type: &'a str,
    pub query: &'a [(String, String)],
    pub referer: Option<&'a str>,
    pub session_cookie: Option<&'a str>,
}

pub struct OAuthCallbackRequest<'a> {
    pub service_type: &'a str,
    /// `r.URL.Path` inside the flow mux (`/callback`), used for events.
    pub path: &'a str,
    pub query: &'a [(String, String)],
    pub session_cookie: Option<&'a str>,
    pub anonymous_uid: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct OAuthFlowResponse {
    pub status: u16,
    pub location: String,
    pub content_type: String,
    pub body: Vec<u8>,
    pub set_cookies: Vec<SetCookie>,
}

const CONTENT_TYPE_HTML: &str = "text/html; charset=utf-8";
const CONTENT_TYPE_TEXT: &str = "text/plain; charset=utf-8";
const SOURCE: &str = "BACKEND";

const ERR_DECODE_STATE: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: could not decode OAuth state from URL parameter.";
const ERR_NO_PROVIDER: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: could not find provider that matches the OAuth state parameter.";
const ERR_MISSING_CODE: &str = "oauth2: Request missing code or state";
const ERR_MISSING_STATE_CTX: &str = "oauth2: Context missing state value";
const ERR_INVALID_STATE: &str = "oauth2: Invalid OAuth2 state parameter";
const ERR_TOKEN_EXPIRED: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: OAuth token was expired.";
const ERR_GET_USER: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: could not initiate session.";
const ERR_SAVE_STATE: &str = "Failed to saving state to session";
const ERR_REFERER: &str = "Failed to parse URL from Referrer header.";
const ERR_DECODE_CTX_STATE: &str =
    "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: could not get decode OAuth state.";

/// The baseline `Users().GetByID` failure body ("could not initiate session.").
/// Exposed for [`crate::oauth_routes`]'s unregistered-resolver seam so a
/// provider without a resolver reports the baseline error instead of faking a
/// successful login. M9.8jo; no change to the flow.
pub fn could_not_initiate_session_message() -> &'static str {
    ERR_GET_USER
}

/// The provider-selection step shared by [`handle_login`] and the GitLab SSO
/// branch in [`crate::oauth_routes`].
pub enum LoginProviderSelection<'a> {
    /// A configured provider matched and the post-login redirect was resolved.
    Matched {
        provider: &'a OAuthProviderConfig,
        redirect: String,
    },
    /// The baseline response to return as-is (`/sign-in?returnTo=…` or the
    /// `Referer` parse error).
    Response(OAuthFlowResponse),
}

/// `newOAuthFlowHandler`'s `/login` provider lookup (`pc`) and the provider
/// `stateHandler`'s [`get_redirect`]; no state/cookie is written here.
pub fn select_login_provider<'a>(
    deps: &'a OAuthFlowDeps<'_>,
    request: &OAuthLoginRequest<'_>,
) -> LoginProviderSelection<'a> {
    let id = query_get(request.query, "pc");
    let Some(provider) = deps.providers.find(request.service_type, &id) else {
        let mut redirect = query_get(request.query, "redirect");
        if redirect.is_empty() {
            redirect = query_get(request.query, "returnTo");
        }
        return LoginProviderSelection::Response(redirect_response(&format!(
            "/sign-in?returnTo={redirect}"
        )));
    };
    let redirect = match get_redirect(request, deps.external_url) {
        Ok(redirect) => redirect,
        Err(()) => return LoginProviderSelection::Response(error_response(500, ERR_REFERER)),
    };
    LoginProviderSelection::Matched { provider, redirect }
}

/// `newOAuthFlowHandler`'s `/login` branch + the provider's `stateHandler` +
/// `gologin.LoginHandler`.
pub async fn handle_login(
    deps: &OAuthFlowDeps<'_>,
    request: &OAuthLoginRequest<'_>,
) -> OAuthFlowResponse {
    let (provider, redirect) = match select_login_provider(deps, request) {
        LoginProviderSelection::Matched { provider, redirect } => (provider, redirect),
        LoginProviderSelection::Response(response) => return response,
    };

    let csrf = deps.state_source.generate();
    let state = LoginState {
        redirect,
        provider_id: provider.config_id(),
        csrf,
        op: query_get(request.query, "op"),
    };
    let encoded = state.encode();

    let mut manager = SessionManager::new(
        deps.store,
        deps.ids,
        deps.cookie_key,
        WriteOptions::new(deps.secure),
        deps.now,
        request.session_cookie,
        false,
    );
    let json = serde_json::to_vec(&encoded).expect("string serializes");
    if manager.set_data("oauthState", &json).await.is_err() {
        return error_response(500, ERR_SAVE_STATE);
    }

    let mut response = redirect_response(&provider.auth_code_url(&encoded));
    response.set_cookies = manager.cookies;
    response
}

/// `newOAuthFlowHandler`'s `/callback` branch + `stateHandler(false)` +
/// `CallbackHandler` + `SessionIssuer`.
pub async fn handle_callback(
    deps: &OAuthFlowDeps<'_>,
    request: &OAuthCallbackRequest<'_>,
) -> OAuthFlowResponse {
    let state_param = query_get(request.query, "state");
    let Some(state) = LoginState::decode(&state_param) else {
        return error_response(400, ERR_DECODE_STATE);
    };
    let Some(provider) = deps
        .providers
        .find(request.service_type, &state.provider_id)
    else {
        return error_response(400, ERR_NO_PROVIDER);
    };

    let mut manager = SessionManager::new(
        deps.store,
        deps.ids,
        deps.cookie_key,
        WriteOptions::new(deps.secure),
        deps.now,
        request.session_cookie,
        false,
    );
    // `stateHandler(false)`: on a GetData error the state is absent from the
    // context; a missing key leaves the destination empty (state "").
    let owner_state: Option<String> = match manager.get_data("oauthState").await {
        Err(_) => None,
        Ok(None) => Some(String::new()),
        Ok(Some(bytes)) => serde_json::from_slice::<String>(&bytes).ok(),
    };

    let code = query_get(request.query, "code");
    if code.is_empty() || state_param.is_empty() {
        return error_response(400, ERR_MISSING_CODE);
    }
    let Some(owner_state) = owner_state else {
        return error_response(400, ERR_MISSING_STATE_CTX);
    };
    if state_param != owner_state || state_param.is_empty() {
        return error_response(400, ERR_INVALID_STATE);
    }

    let token = match exchange_code(deps, provider, &code).await {
        Ok(token) => token,
        Err(message) => return error_response(400, &message),
    };
    if token.expires_in.is_some_and(|seconds| seconds < 0) {
        return error_response(500, ERR_TOKEN_EXPIRED);
    }

    let Some(owner_state) = LoginState::decode(&owner_state) else {
        return error_response(500, ERR_DECODE_CTX_STATE);
    };

    let resolution = deps.resolver.get_or_create_user(&token).await;
    let (user_id, new_user, failure) = match resolution {
        UserResolution::Created { user_id, new_user } => (user_id, new_user, None),
        UserResolution::Failed { safe_message } => (0, false, Some((500, safe_message, true))),
        UserResolution::FailedWithStatus {
            status,
            safe_message,
        } => (0, false, Some((status, safe_message, false))),
    };
    if let Some((status, safe_message, issuer_failure)) = failure {
        let mut response = error_response(status, &safe_message);
        if issuer_failure {
            log_event(
                deps,
                request,
                deps.resolver.failure_event_name(),
                0,
                request.anonymous_uid.unwrap_or_default(),
            )
            .await;
            finish_and_delete_state(&mut manager, &mut response).await;
        }
        return response;
    }

    let user = match deps.users.get_by_id(user_id).await {
        Ok(user) => user,
        Err(_) => {
            let mut response = error_response(500, ERR_GET_USER);
            finish_and_delete_state(&mut manager, &mut response).await;
            return response;
        }
    };

    if deps.license_expired && !user.site_admin {
        let mut response = set_actor_error_response(
            "Sourcegraph license is expired. Only admins are allowed to sign in.",
        );
        finish_and_delete_state(&mut manager, &mut response).await;
        return response;
    }

    let expiry_period = token
        .expires_in
        .map(|seconds| seconds.saturating_mul(1_000_000_000))
        .unwrap_or(0);
    let actor = Actor {
        uid: user.id,
        ..Default::default()
    };
    if let Err(error) = manager
        .set_actor(
            Some(&actor),
            expiry_period,
            deps.configured_expiry,
            user.created_at,
        )
        .await
    {
        let mut response = set_actor_error_response(&error);
        finish_and_delete_state(&mut manager, &mut response).await;
        return response;
    }

    log_event(
        deps,
        request,
        deps.resolver.success_event_name(),
        user.id,
        "",
    )
    .await;

    let redirect = add_post_auth_redirect_parameters_to_string(
        &owner_state.redirect,
        new_user,
        &format!("OAuth::{}", deps.resolver.service_id()),
    );
    let safe_redirect = safe_redirect_url(&redirect);
    let mut response = redirect_response(&safe_redirect);
    let visible = manager.cookies.len();
    let _ = manager.set_data("oauthState", b"\"\"").await;
    manager.cookies.truncate(visible);
    response.set_cookies = manager.cookies;
    response
}

/// Runs the deferred `DeleteStateCookie` (`SetData("oauthState", "")`) and
/// hides its cookie from the response, exactly like `http.ResponseRecorder`'s
/// header snapshot taken at `WriteHeader`.
async fn finish_and_delete_state(
    manager: &mut SessionManager<'_>,
    response: &mut OAuthFlowResponse,
) {
    let visible = manager.cookies.len();
    let _ = manager.set_data("oauthState", b"\"\"").await;
    manager.cookies.truncate(visible);
    response.set_cookies = manager.cookies.clone();
}

async fn log_event(
    deps: &OAuthFlowDeps<'_>,
    request: &OAuthCallbackRequest<'_>,
    name: &str,
    user_id: i32,
    anonymous_user_id: &str,
) {
    let _ = deps
        .events
        .log_security_event(OAuthSecurityEventRecord {
            name: name.to_owned(),
            url: request.path.to_owned(),
            user_id,
            anonymous_user_id: anonymous_user_id.to_owned(),
            source: SOURCE.to_owned(),
            argument: Value::Null,
            timestamp: deps.now,
        })
        .await;
}

/// `x/oauth2.Config.Exchange` + `internal.RetrieveToken`: try the Basic-auth
/// style first and retry with the credentials in the form on any error (the
/// baseline's AuthStyle probe).
async fn exchange_code(
    deps: &OAuthFlowDeps<'_>,
    provider: &OAuthProviderConfig,
    code: &str,
) -> Result<OAuthToken, String> {
    let base = TokenExchangeRequest {
        token_url: provider.token_url.clone(),
        client_id: provider.client_id.clone(),
        client_secret: provider.client_secret.clone(),
        code: code.to_owned(),
        redirect_url: provider.redirect_url.clone(),
        auth_style: OAuthAuthStyle::InHeader,
    };
    match deps.http.exchange_code(&base).await {
        Ok(response) => match parse_token_response(&response) {
            Ok(token) => Ok(token),
            Err(_) => retry_in_params(deps, &base).await,
        },
        Err(_) => retry_in_params(deps, &base).await,
    }
}

async fn retry_in_params(
    deps: &OAuthFlowDeps<'_>,
    base: &TokenExchangeRequest,
) -> Result<OAuthToken, String> {
    let retry = TokenExchangeRequest {
        auth_style: OAuthAuthStyle::InParams,
        ..base.clone()
    };
    let response = deps.http.exchange_code(&retry).await?;
    parse_token_response(&response)
}

/// `internal.doTokenRoundTrip` + `RetrieveError.Error`.
fn parse_token_response(response: &OAuthHttpResponse) -> Result<OAuthToken, String> {
    let failure_status = !(200..=299).contains(&response.status);
    let media_type = response
        .content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    let (token, error_code, error_description, error_uri) =
        if media_type == "application/x-www-form-urlencoded" || media_type == "text/plain" {
            let form = parse_query(&String::from_utf8_lossy(&response.body));
            let get = |key: &str| {
                form.iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            let expires_in = get("expires_in").parse::<i64>().ok().filter(|n| *n != 0);
            let access_token = get("access_token");
            let token_type = get("token_type");
            let refresh_token = get("refresh_token");
            let error_code = get("error");
            let error_description = get("error_description");
            let error_uri = get("error_uri");
            (
                OAuthToken {
                    access_token,
                    token_type,
                    refresh_token,
                    expires_in,
                    json: None,
                    form,
                },
                error_code,
                error_description,
                error_uri,
            )
        } else {
            let body = String::from_utf8_lossy(&response.body);
            let json: Value = match serde_json::from_str(&body) {
                Ok(json) => json,
                Err(error) => {
                    if failure_status {
                        return Err(retrieve_error(response, "", "", ""));
                    }
                    return Err(format!("oauth2: cannot parse json: {error}"));
                }
            };
            if !json.is_object() {
                if failure_status {
                    return Err(retrieve_error(response, "", "", ""));
                }
                return Err("oauth2: cannot parse json: expected JSON object".to_owned());
            }
            let expires_in = match json.get("expires_in") {
                None | Some(Value::Null) => None,
                Some(Value::Number(number)) => {
                    let value = number.as_i64().unwrap_or_default();
                    if value == 0 {
                        None
                    } else {
                        Some(value.min(i64::from(i32::MAX)))
                    }
                }
                Some(_) => {
                    if failure_status {
                        return Err(retrieve_error(response, "", "", ""));
                    }
                    return Err("oauth2: cannot parse json: invalid expires_in".to_owned());
                }
            };
            let field = |key: &str| {
                json.get(key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            (
                OAuthToken {
                    access_token: field("access_token"),
                    token_type: field("token_type"),
                    refresh_token: field("refresh_token"),
                    expires_in,
                    json: Some(json.clone()),
                    form: Vec::new(),
                },
                field("error"),
                field("error_description"),
                field("error_uri"),
            )
        };

    if failure_status || !error_code.is_empty() {
        return Err(retrieve_error(
            response,
            &error_code,
            &error_description,
            &error_uri,
        ));
    }
    if token.access_token.is_empty() {
        return Err("oauth2: server response missing access_token".to_owned());
    }
    Ok(token)
}

fn retrieve_error(
    response: &OAuthHttpResponse,
    error_code: &str,
    error_description: &str,
    error_uri: &str,
) -> String {
    if !error_code.is_empty() {
        let mut out = format!("oauth2: {}", go_quote(error_code));
        if !error_description.is_empty() {
            out.push(' ');
            out.push_str(&go_quote(error_description));
        }
        if !error_uri.is_empty() {
            out.push(' ');
            out.push_str(&go_quote(error_uri));
        }
        return out;
    }
    format!(
        "oauth2: cannot fetch token: {} {}\nResponse: {}",
        response.status,
        status_text(response.status),
        String::from_utf8_lossy(&response.body)
    )
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// Redirect / URL helpers (`auth.SafeRedirectURL`,
// `auth.AddPostAuthRedirectParametersToString`, `oauth.getRedirect`).
// ---------------------------------------------------------------------------

/// `oauth.getRedirect` (`provider.go:237-257`).
fn get_redirect(request: &OAuthLoginRequest<'_>, external_url: &str) -> Result<String, ()> {
    let redirect = query_get(request.query, "redirect");
    if !redirect.is_empty() {
        return Ok(redirect);
    }
    let Some(referer) = request.referer else {
        return Ok(String::new());
    };
    if referer.is_empty() {
        return Ok(String::new());
    }
    let referrer = go_parse_url(referer).ok_or(())?;
    let return_to = query_get(&parse_query(&referrer.raw_query), "returnTo");
    if !can_redirect(&return_to, external_url) {
        return Err(());
    }
    Ok(return_to)
}

/// `oauth.canRedirect` (`provider.go:261-276`).
fn can_redirect(redirect: &str, external_url: &str) -> bool {
    let Some(unescaped) = query_unescape(redirect) else {
        return false;
    };
    let Some(parsed) = go_parse_url(&unescaped) else {
        return false;
    };
    if !parsed.host.is_empty() {
        let Some(external) = go_parse_url(external_url) else {
            return false;
        };
        if parsed.host != external.host {
            return false;
        }
    }
    true
}

/// `auth.AddPostAuthRedirectParametersToString` (`redirect.go:31-41`).
pub fn add_post_auth_redirect_parameters_to_string(
    url: &str,
    new_user_created: bool,
    service_type: &str,
) -> String {
    let Some(parsed) = go_parse_url(url) else {
        return url.to_owned();
    };
    let mut pairs = parse_query(&parsed.raw_query);
    pairs.push((
        if new_user_created { "signup" } else { "signin" }.to_owned(),
        service_type.to_owned(),
    ));
    let raw_query = encode_query_sorted(&pairs);
    go_url_string(&GoUrl {
        raw_query,
        ..parsed
    })
}

/// `auth.SafeRedirectURL` (`redirect.go:12-26`).
pub fn safe_redirect_url(url: &str) -> String {
    let Some(parsed) = go_parse_url(url) else {
        return "/".to_owned();
    };
    if !parsed.path.starts_with('/') {
        return "/".to_owned();
    }
    let path = clean_path(&parsed.path);
    if parsed.raw_query.is_empty() {
        path
    } else {
        format!("{path}?{}", parsed.raw_query)
    }
}

/// `path.Clean`.
fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => match parts.last() {
                Some(last) if *last != ".." => {
                    parts.pop();
                }
                Some(_) => {}
                None if !rooted => parts.push(".."),
                None => {}
            },
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_owned()
    } else {
        joined
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoUrl {
    pub scheme: String,
    pub host: String,
    pub path: String,
    pub raw_query: String,
}

/// A Go-compatible `url.Parse` for the shapes this flow produces: absolute
/// URLs, rooted paths, and optional query/fragment. Returns `None` on the few
/// inputs Go rejects (bad scheme).
pub fn go_parse_url(input: &str) -> Option<GoUrl> {
    let mut rest = input;
    let mut scheme = String::new();
    let mut host = String::new();
    if let Some(index) = rest.find("://") {
        scheme = rest[..index].to_owned();
        if scheme.is_empty()
            || !scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        {
            return None;
        }
        rest = &rest[index + 3..];
        let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        host = rest[..end].to_owned();
        rest = &rest[end..];
    } else if let Some(stripped) = rest.strip_prefix("//") {
        let end = stripped.find(['/', '?', '#']).unwrap_or(stripped.len());
        host = stripped[..end].to_owned();
        rest = &stripped[end..];
    }
    let rest = rest.split('#').next().unwrap_or(rest);
    let (path, raw_query) = match rest.split_once('?') {
        Some((path, query)) => (path, query),
        None => (rest, ""),
    };
    Some(GoUrl {
        scheme,
        host,
        path: path.to_owned(),
        raw_query: raw_query.to_owned(),
    })
}

pub fn go_url_string(url: &GoUrl) -> String {
    let mut out = String::new();
    if !url.scheme.is_empty() {
        out.push_str(&url.scheme);
        out.push(':');
    }
    if !url.scheme.is_empty() || !url.host.is_empty() {
        out.push_str("//");
        out.push_str(&url.host);
    }
    out.push_str(&url.path);
    if !url.raw_query.is_empty() {
        out.push('?');
        out.push_str(&url.raw_query);
    }
    out
}

pub fn query_get(query: &[(String, String)], key: &str) -> String {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// `url.ParseQuery`.
pub fn parse_query(raw: &str) -> Vec<(String, String)> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((query_unescape(key)?, query_unescape(value)?))
        })
        .collect()
}

/// `url.Values.Encode`: keys sorted, values in insertion order.
pub fn encode_query_sorted(pairs: &[(String, String)]) -> String {
    let mut sorted = pairs.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    sorted
        .iter()
        .map(|(key, value)| format!("{}={}", query_escape(key), query_escape(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// `url.QueryEscape`.
pub fn query_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// `url.QueryUnescape`.
pub fn query_unescape(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return None;
                }
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                out.push(high * 16 + low);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// `net/http.htmlEscape` (`server.go`'s redirect body).
pub fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&#34;")
        .replace('\'', "&#39;")
}

fn redirect_response(location: &str) -> OAuthFlowResponse {
    OAuthFlowResponse {
        status: 302,
        location: location.to_owned(),
        content_type: CONTENT_TYPE_HTML.to_owned(),
        body: format!("<a href=\"{}\">Found</a>.\n\n", html_escape(location)).into_bytes(),
        set_cookies: Vec::new(),
    }
}

fn error_response(status: u16, message: &str) -> OAuthFlowResponse {
    OAuthFlowResponse {
        status,
        location: String::new(),
        content_type: CONTENT_TYPE_TEXT.to_owned(),
        body: format!("{message}\n").into_bytes(),
        set_cookies: Vec::new(),
    }
}

fn set_actor_error_response(error: &str) -> OAuthFlowResponse {
    error_response(
        500,
        &format!(
            "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: {error}"
        ),
    )
}

/// Go's `encoding/json` HTML escaping (applied before base64 and to the
/// harness's pretty output).
pub fn go_json_escape(input: &str) -> String {
    input
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_state_field_order_and_escaping_match_go() {
        let state = LoginState {
            redirect: "/search".to_owned(),
            provider_id: "https://idp.example/::cid".to_owned(),
            csrf: "<csrf>".to_owned(),
            op: "create".to_owned(),
        };
        let json = String::from_utf8(state.to_go_json()).unwrap();
        assert_eq!(
            json,
            "{\"Redirect\":\"/search\",\"ProviderID\":\"https://idp.example/::cid\",\"CSRF\":\"\\u003ccsrf\\u003e\",\"Op\":\"create\"}"
        );
        assert_eq!(LoginState::decode(&state.encode()), Some(state));
    }

    #[test]
    fn query_and_url_encoding_matches_go() {
        assert_eq!(query_escape("openid profile email"), "openid+profile+email");
        assert_eq!(
            query_escape("https://sourcegraph.example/.auth/x/callback"),
            "https%3A%2F%2Fsourcegraph.example%2F.auth%2Fx%2Fcallback"
        );
        assert_eq!(
            encode_query_sorted(&[
                ("state".to_owned(), "a".to_owned()),
                ("client_id".to_owned(), "c".to_owned()),
            ]),
            "client_id=c&state=a"
        );
        assert_eq!(
            safe_redirect_url("/search?signin=OAuth%3A%3Ahttps%3A%2F%2Fidp.example%2F"),
            "/search?signin=OAuth%3A%3Ahttps%3A%2F%2Fidp.example%2F"
        );
        assert_eq!(safe_redirect_url("https://evil.example/path"), "/path");
        assert_eq!(
            add_post_auth_redirect_parameters_to_string("/search", false, "OAuth::x"),
            "/search?signin=OAuth%3A%3Ax"
        );
    }

    #[test]
    fn auth_code_url_sorts_and_escapes_like_go() {
        let provider = OAuthProviderConfig {
            service_type: "fakeoauth".to_owned(),
            service_id: "https://idp.example/".to_owned(),
            client_id: "cid-123".to_owned(),
            client_secret: "csecret".to_owned(),
            auth_url: "https://idp.example/authorize".to_owned(),
            token_url: "https://idp.example/token".to_owned(),
            scopes: vec![
                "openid".to_owned(),
                "profile".to_owned(),
                "email".to_owned(),
            ],
            redirect_url: "https://sourcegraph.example/.auth/fakeoauth/callback".to_owned(),
            config_id: "https://idp.example/::cid-123".to_owned(),
            sso_url: String::new(),
        };
        assert_eq!(
            provider.auth_code_url("abc"),
            "https://idp.example/authorize?client_id=cid-123&redirect_uri=https%3A%2F%2Fsourcegraph.example%2F.auth%2Ffakeoauth%2Fcallback&response_type=code&scope=openid+profile+email&state=abc"
        );
    }
}
