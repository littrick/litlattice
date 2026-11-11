#![forbid(unsafe_code)]

//! OAuth route mounting and production assembly (M9.8jo).
//!
//! The baseline mounts an OAuth middleware per provider package on the App
//! listener (`cmd/frontend/internal/auth/{githuboauth,gitlaboauth}` use
//! `oauth.NewMiddleware`; OpenID Connect has its own middleware but the same
//! `/.auth/openidconnect` prefix). The generic middleware:
//!
//! 1. delegates any path under `authPrefix + "/"` to
//!    `newOAuthFlowHandler(serviceType)` (exact `/login` and `/callback`;
//!    anything else is the flow mux's 404), before looking at the actor;
//! 2. lets an authenticated actor through;
//! 3. otherwise, when exactly one sign-in provider exists, it is a generic
//!    OAuth provider, the site is not `AuthPublic`, there is no `sg-signout`
//!    cookie, the request looks human and access requests are disabled,
//!    redirects to `authPrefix + "/login?pc=…&redirect=…"`.
//!
//! The actual flow lives in [`crate::oauth_login`] (M9.8jk); this module only
//! provides the routing decision, the HTTP response conversion and the
//! production dependency assembly. Provider-specific user resolution stays
//! behind [`OAuthUserResolverRegistry`] so the GitHub/GitLab/OIDC resolvers
//! (M9.8jp/jq/jr) can be registered by the parent without faking success.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::Full;
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_session::{parse_go_duration, SessionStore, COOKIE_NAME, SIGN_OUT_COOKIE};

use crate::actor_chain::LicenseReader;
use crate::authz::LicenseState;
use crate::oauth_login::{
    encode_query_sorted, handle_callback, handle_login, parse_query, query_get, safe_redirect_url,
    select_login_provider, LoginProviderSelection, OAuthCallbackRequest, OAuthFlowDeps,
    OAuthFlowResponse, OAuthHttpClient, OAuthHttpResponse, OAuthLoginRequest, OAuthProviderConfig,
    OAuthProviders, OAuthSecurityEventSink, OAuthUserLookup, OAuthUserResolver, StateSource,
    TokenExchangeRequest, UserResolution,
};

/// `cmd/frontend/auth.AuthURLPrefix`.
pub const AUTH_URL_PREFIX: &str = "/.auth";

/// The provider families this slice mounts (`authPrefix` suffixes).
pub const MOUNTED_SERVICE_TYPES: [&str; 3] = ["github", "gitlab", "openidconnect"];

/// `extsvc` types whose Provider is the generic `oauth.Provider` (the only
/// ones `getExactlyOneOAuthProvider` accepts). Only `github`/`gitlab` are
/// modelled by [`OAuthProviders`]; see the overview's errata for the rest.
const GENERIC_OAUTH_TYPES: [&str; 5] = [
    "github",
    "gitlab",
    "azureDevOps",
    "bitbucketcloud",
    "bitbucketserver",
];

/// `cookie.AnonymousUID`'s cookie name.
pub const ANONYMOUS_UID_COOKIE: &str = "sourcegraphAnonymousUid";

const CONTENT_TYPE_HTML: &str = "text/html; charset=utf-8";
const CONTENT_TYPE_TEXT: &str = "text/plain; charset=utf-8";

// ---------------------------------------------------------------------------
// Path matching
// ---------------------------------------------------------------------------

/// One delegated OAuth flow request: the provider service type and the path
/// inside the flow mux (`/login` or `/callback`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthFlowPath<'a> {
    pub service_type: &'a str,
    /// The path after `http.StripPrefix(authPrefix)`; starts with `/`.
    pub subpath: &'a str,
}

/// `!isAPI && strings.HasPrefix(r.URL.Path, authPrefix+"/")` for the mounted
/// provider prefixes. `/.auth/<provider>` without the trailing slash does not
/// match.
pub fn match_auth_prefix(path: &str) -> Option<AuthFlowPath<'_>> {
    let rest = path.strip_prefix(AUTH_URL_PREFIX)?.strip_prefix('/')?;
    let (service_type, _) = rest.split_once('/')?;
    if !MOUNTED_SERVICE_TYPES.contains(&service_type) {
        return None;
    }
    Some(AuthFlowPath {
        service_type,
        subpath: &rest[service_type.len()..],
    })
}

// ---------------------------------------------------------------------------
// Configuration (per live-configuration generation)
// ---------------------------------------------------------------------------

/// `providers.SignInProviders` reduced to provider types. The baseline filters
/// `sourcegraph-operator` when `skipSoap` and every provider whose config sets
/// `hidden`/`noSignIn`.
pub fn sign_in_provider_types(site: &SiteConfiguration, skip_soap: bool) -> Vec<String> {
    let raw = site.raw();
    let external_url = raw
        .get("externalURL")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let entries = raw
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut types = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        let provider_type = obj
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if skip_soap && provider_type == "sourcegraph-operator" {
            continue;
        }
        let hidden = obj
            .get("hidden")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let no_sign_in = obj
            .get("noSignIn")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if hidden || no_sign_in {
            continue;
        }
        if !registered_provider(provider_type, obj, external_url) {
            continue;
        }
        types.push(provider_type.to_owned());
    }
    types
}

fn string_field(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn parses_as_url(value: &str) -> bool {
    url::Url::parse(value).is_ok()
}

/// Whether the provider package would register a provider for this config
/// entry. Approximate (see the overview's errata); the differential corpus
/// only uses configurations whose validity is unambiguous.
fn registered_provider(
    provider_type: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    external_url: &str,
) -> bool {
    let with_default = |key: &str, default: &str| {
        let value = string_field(obj, key);
        if value.is_empty() {
            default.to_owned()
        } else {
            value
        }
    };
    match provider_type {
        // `githuboauth.parseProvider` only drops unparsable URLs (the default
        // is `https://github.com/`).
        "github" => parses_as_url(&with_default("url", "https://github.com/")),
        // `gitlaboauth.parseConfig` drops every entry when `externalURL` is
        // empty.
        "gitlab" => {
            !external_url.is_empty() && parses_as_url(&with_default("url", "https://gitlab.com/"))
        }
        // OIDC providers register regardless of issuer/client ID;
        // `GetProviderAndClient` reports the misconfiguration per request.
        "openidconnect" => true,
        "azureDevOps" => true,
        "bitbucketcloud" => parses_as_url(&with_default("url", "https://bitbucket.org/")),
        "bitbucketserver" => !external_url.is_empty() && parses_as_url(&string_field(obj, "url")),
        "builtin" | "http-header" | "saml" | "gerrit" | "sourcegraph-operator" => true,
        // Unknown discriminants are rejected by the baseline configuration.
        _ => false,
    }
}

/// `conf.IsAccessRequestEnabled()`: `auth.accessRequest == nil || enabled ==
/// nil || *enabled`.
fn access_requests_enabled(raw: &serde_json::Value) -> bool {
    match raw.get("auth.accessRequest") {
        None | Some(serde_json::Value::Null) => true,
        Some(config) => config
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
    }
}

/// The single sign-in provider that triggers the automatic redirect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInOAuth {
    pub service_type: String,
    pub config_id: String,
    pub auth_prefix: String,
}

/// Everything the OAuth middleware derives from `conf.Get()`.
#[derive(Debug, Clone, Default)]
pub struct OAuthRouteConfig {
    pub providers: OAuthProviders,
    pub external_url: String,
    /// `conf.Get().AuthSessionExpiry` (a Go duration string).
    pub configured_expiry: Option<String>,
    /// `conf.ExternalURLParsed().Scheme == "https"`.
    pub secure: bool,
    /// `conf.AuthPublic()`, i.e. `dotcom.SourcegraphDotComMode()`.
    pub dotcom: bool,
    pub access_requests_enabled: bool,
    /// `auth.providers` entries with `openidconnect` set (M9.8js).
    pub oidc_configs: Vec<crate::oidc_login::OpenIdConnectAuthProvider>,
    sign_in_without_soap: Vec<String>,
    sign_in_with_soap: Vec<String>,
}

impl OAuthRouteConfig {
    pub fn from_site(site: &SiteConfiguration, dotcom: bool) -> Self {
        let raw = site.raw();
        let external_url = raw
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let secure = url::Url::parse(&external_url)
            .map(|url| url.scheme() == "https")
            .unwrap_or(false);
        let configured_expiry = raw
            .get("auth.sessionExpiry")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        Self {
            providers: OAuthProviders::from_site(site, dotcom),
            external_url,
            configured_expiry,
            secure,
            dotcom,
            access_requests_enabled: access_requests_enabled(raw),
            oidc_configs: crate::oidc_middleware::oidc_configs(site),
            sign_in_without_soap: sign_in_provider_types(site, true),
            sign_in_with_soap: sign_in_provider_types(site, false),
        }
    }

    /// `getExactlyOneOAuthProvider(skipSoap)`.
    pub fn single_oauth(&self, skip_soap: bool) -> Option<SignInOAuth> {
        let types = if skip_soap {
            &self.sign_in_without_soap
        } else {
            &self.sign_in_with_soap
        };
        if types.len() != 1 {
            return None;
        }
        let service_type = types[0].as_str();
        if !GENERIC_OAUTH_TYPES.contains(&service_type) {
            return None;
        }
        let provider = self.find_provider(service_type)?;
        Some(SignInOAuth {
            service_type: service_type.to_owned(),
            config_id: provider.config_id(),
            auth_prefix: format!("{AUTH_URL_PREFIX}/{service_type}"),
        })
    }

    fn find_provider(&self, service_type: &str) -> Option<&OAuthProviderConfig> {
        self.providers
            .providers
            .iter()
            .find(|provider| provider.service_type == service_type)
    }
}

// ---------------------------------------------------------------------------
// Dependency seams
// ---------------------------------------------------------------------------

/// The process-wide OAuth dependencies. Per-configuration values live in
/// [`OAuthRouteConfig`]; this struct is rebuilt per configuration generation
/// because the `securityEventLog.location` switch can change.
pub struct OAuthRouteDeps {
    pub store: Arc<dyn SessionStore>,
    pub ids: Arc<dyn sg_session::SessionIdSource>,
    pub state_source: Arc<dyn StateSource>,
    /// `SRC_SESSION_COOKIE_KEY` (empty by default, like the baseline).
    pub cookie_key: Vec<u8>,
    pub license: Arc<dyn LicenseReader>,
    pub http: Arc<dyn OAuthHttpClient>,
    pub users: Arc<dyn OAuthUserLookup>,
    pub events: Arc<dyn OAuthSecurityEventSink>,
    pub resolvers: Arc<OAuthUserResolverRegistry>,
    /// When set, `/.auth/openidconnect/{login,callback}` and the legacy
    /// `/.auth/callback` alias are served by [`crate::oidc_middleware`]
    /// instead of the generic OAuth flow (M9.8js). `None` preserves the old
    /// dispatch, which the M9.8jo 46-case differential pins.
    pub oidc: Option<Arc<crate::oidc_middleware::OidcMiddlewareRuntime>>,
}

/// Builds [`OAuthRouteDeps`] for one site configuration generation.
pub type OAuthRouteFactory = Arc<dyn Fn(&SiteConfiguration) -> Arc<OAuthRouteDeps> + Send + Sync>;

/// Builds the per-request resolver for one service type. The callback's state
/// parameter is passed because OpenID Connect uses it as the id_token nonce
/// (`openidconnect/middleware.go:338-345`).
pub type OAuthUserResolverFactory =
    Arc<dyn Fn(Option<&str>) -> Arc<dyn OAuthUserResolver> + Send + Sync>;

/// Provider-specific `GetOrCreateUser` implementations, keyed by
/// `service_type` (`"github"` | `"gitlab"` | `"openidconnect"`).
///
/// A static registration is enough for providers whose resolver does not
/// depend on the request; `register_factory` wins over `register` for the
/// same key and is rebuilt per callback request.
#[derive(Default)]
pub struct OAuthUserResolverRegistry {
    resolvers: Mutex<HashMap<String, Arc<dyn OAuthUserResolver>>>,
    factories: Mutex<HashMap<String, OAuthUserResolverFactory>>,
}

impl OAuthUserResolverRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, service_type: impl Into<String>, resolver: Arc<dyn OAuthUserResolver>) {
        if let Ok(mut resolvers) = self.resolvers.lock() {
            resolvers.insert(service_type.into(), resolver);
        }
    }

    pub fn register_factory(
        &self,
        service_type: impl Into<String>,
        factory: OAuthUserResolverFactory,
    ) {
        if let Ok(mut factories) = self.factories.lock() {
            factories.insert(service_type.into(), factory);
        }
    }

    pub fn get(&self, service_type: &str) -> Option<Arc<dyn OAuthUserResolver>> {
        self.resolvers
            .lock()
            .ok()
            .and_then(|resolvers| resolvers.get(service_type).cloned())
    }

    /// The factory for `service_type`, falling back to a static registration.
    pub fn get_for(
        &self,
        service_type: &str,
        state: Option<&str>,
    ) -> Option<Arc<dyn OAuthUserResolver>> {
        if let Some(factory) = self
            .factories
            .lock()
            .ok()
            .and_then(|factories| factories.get(service_type).cloned())
        {
            return Some(factory(state));
        }
        self.get(service_type)
    }
}

/// Used when no provider-specific resolver is registered. The callback answers
/// the baseline `Users().GetByID` failure (500 "could not initiate session.")
/// instead of faking a successful login.
struct MissingResolver;

#[async_trait::async_trait]
impl OAuthUserResolver for MissingResolver {
    fn service_id(&self) -> &str {
        ""
    }

    fn success_event_name(&self) -> &str {
        "OAuthSucceeded"
    }

    fn failure_event_name(&self) -> &str {
        "OAuthFailed"
    }

    async fn get_or_create_user(&self, _token: &crate::oauth_login::OAuthToken) -> UserResolution {
        UserResolution::Failed {
            safe_message: crate::oauth_login::could_not_initiate_session_message().to_owned(),
        }
    }
}

// ---------------------------------------------------------------------------
// Token endpoint transport
// ---------------------------------------------------------------------------

/// `oauth2.Config.Exchange`'s POST through a reqwest client.
///
/// Deviations from `httpcli.ExternalClient` (documented in the overview):
/// no Redis response cache, no retry loop and no dynamic `tls.external`
/// configuration. The timeout (default 5m, `SRC_HTTP_CLI_EXTERNAL_TIMEOUT`),
/// `User-Agent: Sourcegraph-Bot` and Go's default 10-redirect limit are kept.
pub struct ReqwestOAuthHttpClient {
    client: reqwest::Client,
}

impl ReqwestOAuthHttpClient {
    pub fn new(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent("Sourcegraph-Bot")
            .build()
            .unwrap_or_default();
        Self { client }
    }

    /// `SRC_HTTP_CLI_EXTERNAL_TIMEOUT`, default `5m` like `internal/httpcli`.
    pub fn from_env() -> Self {
        let timeout = std::env::var("SRC_HTTP_CLI_EXTERNAL_TIMEOUT")
            .ok()
            .and_then(|value| parse_go_duration(&value))
            .filter(|nanoseconds| *nanoseconds > 0)
            .map(|nanoseconds| Duration::from_nanos(nanoseconds as u64))
            .unwrap_or(Duration::from_secs(300));
        Self::new(timeout)
    }
}

#[async_trait::async_trait]
impl OAuthHttpClient for ReqwestOAuthHttpClient {
    async fn exchange_code(
        &self,
        request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String> {
        let body = encode_query_sorted(&request.form());
        let mut post = self
            .client
            .post(&request.token_url)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body);
        // `internal.newTokenRequest` uses `SetBasicAuth(url.QueryEscape(...))`
        // for the header style; the flow retries in params on any failure.
        if request.auth_style == crate::oauth_login::OAuthAuthStyle::InHeader
            && !request.client_id.is_empty()
        {
            post = post.basic_auth(
                crate::oauth_login::query_escape(&request.client_id),
                Some(crate::oauth_login::query_escape(&request.client_secret)),
            );
        }
        let response = post.send().await.map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response
            .bytes()
            .await
            .map_err(|error| error.to_string())?
            .to_vec();
        Ok(OAuthHttpResponse {
            status,
            content_type,
            body,
        })
    }
}

// ---------------------------------------------------------------------------
// HTTP response conversion
// ---------------------------------------------------------------------------

/// The `net/http` response for a flow result. `http.Redirect` only writes a
/// body for GET (and a Content-Type for GET/HEAD); `http.Error` adds
/// `X-Content-Type-Options: nosniff`.
pub(crate) fn flow_response(mut flow: OAuthFlowResponse, method: &Method) -> Response<BoxBody> {
    let is_redirect = flow.status == StatusCode::FOUND.as_u16() && !flow.location.is_empty();
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() =
        StatusCode::from_u16(flow.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    if is_redirect {
        if let Ok(value) = HeaderValue::from_str(&flow.location) {
            response.headers_mut().insert(header::LOCATION, value);
        }
        if (method == Method::GET || method == Method::HEAD) && !flow.content_type.is_empty() {
            if let Ok(value) = HeaderValue::from_str(&flow.content_type) {
                response.headers_mut().insert(header::CONTENT_TYPE, value);
            }
        }
        if method == Method::GET {
            let body = std::mem::take(&mut flow.body);
            *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body)));
        }
    } else {
        if !flow.content_type.is_empty() {
            if let Ok(value) = HeaderValue::from_str(&flow.content_type) {
                response.headers_mut().insert(header::CONTENT_TYPE, value);
            }
        }
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        let body = std::mem::take(&mut flow.body);
        *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body)));
    }
    for cookie in &flow.set_cookies {
        if let Ok(value) = HeaderValue::from_str(&cookie.to_header()) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
}

fn not_found_response() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from_static(
        b"404 page not found\n",
    ))));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(CONTENT_TYPE_TEXT),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `http.Redirect(w, r, location, http.StatusFound)`.
fn redirect_response(method: &Method, location: &str) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = StatusCode::FOUND;
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if method == Method::GET || method == Method::HEAD {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(CONTENT_TYPE_HTML),
        );
    }
    if method == Method::GET {
        let body = format!(
            "<a href=\"{}\">Found</a>.\n\n",
            crate::authz::html_escape(location)
        );
        *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body)));
    }
    response
}

// ---------------------------------------------------------------------------
// Request facts (net/http helpers)
// ---------------------------------------------------------------------------

pub(crate) fn cookie_value<B>(req: &Request<B>, name: &str) -> Option<String> {
    for value in req.headers().get_all(header::COOKIE) {
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

fn query_pairs<B>(req: &Request<B>) -> Vec<(String, String)> {
    req.uri().query().map(parse_query).unwrap_or_default()
}

pub(crate) fn query_has<B>(req: &Request<B>, name: &str) -> bool {
    query_pairs(req).iter().any(|(key, _)| key == name)
}

/// `isHuman`: `strings.Contains(strings.ToLower(UserAgent), "mozilla")`.
fn is_human<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains("mozilla"))
        .unwrap_or(false)
}

/// `r.URL.String()` for a server request (path plus raw query).
fn request_uri<B>(req: &Request<B>) -> String {
    match req.uri().query() {
        Some(query) if !query.is_empty() => format!("{}?{query}", req.uri().path()),
        _ => req.uri().path().to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// The `/login` and `/callback` branches of `newOAuthFlowHandler`, plus its
/// ServeMux 404. Returns `None` when the path is not under a mounted
/// `authPrefix + "/"`.
pub async fn auth_prefix_response_at<B>(
    req: &Request<B>,
    config: &OAuthRouteConfig,
    deps: &OAuthRouteDeps,
    now: OffsetDateTime,
) -> Option<Response<BoxBody>> {
    let method = req.method().clone();
    // M9.8js: the OpenID Connect provider has its own middleware in the
    // baseline (including the `/.auth/callback` alias) and takes over its whole
    // prefix before the generic flow mux. When the parent wires the runtime,
    // hand those paths to it; otherwise keep the M9.8jo dispatch.
    if let Some(oidc) = &deps.oidc {
        let oidc_config = crate::oidc_middleware::OidcMiddlewareConfig {
            configs: config.oidc_configs.clone(),
            external_url: config.external_url.clone(),
            secure: config.secure,
            configured_expiry: config.configured_expiry.clone(),
            dotcom: config.dotcom,
        };
        if let Some(flow) =
            crate::oidc_middleware::handle_oidc_auth(req, &oidc_config, oidc, now).await
        {
            return Some(with_outer_vary(flow_response(flow, &method)));
        }
    }
    let AuthFlowPath {
        service_type,
        subpath,
    } = match_auth_prefix(req.uri().path())?;
    let flow = match subpath {
        "/login" => {
            let query = query_pairs(req);
            let referer = req
                .headers()
                .get(header::REFERER)
                .and_then(|value| value.to_str().ok());
            let session_cookie = cookie_value(req, COOKIE_NAME);
            let request = OAuthLoginRequest {
                service_type,
                query: &query,
                referer,
                session_cookie: session_cookie.as_deref(),
            };
            let resolver = resolver_for(service_type, None, deps);
            let flow_deps = flow_deps(config, deps, resolver.as_ref(), false, now);
            // `gitlaboauth.parseProvider` selects `SSOLoginHandler` when
            // `ssoURL` is set; every other provider uses the plain login.
            if service_type == crate::gitlab_oauth::GITLAB_SERVICE_TYPE {
                if let LoginProviderSelection::Matched { provider, .. } =
                    select_login_provider(&flow_deps, &request)
                {
                    if !provider.sso_url.is_empty() {
                        return Some(with_outer_vary(flow_response(
                            crate::gitlab_oauth::handle_gitlab_login(
                                &flow_deps,
                                &request,
                                &provider.sso_url,
                            )
                            .await,
                            &method,
                        )));
                    }
                }
            }
            handle_login(&flow_deps, &request).await
        }
        "/callback" => {
            let query = query_pairs(req);
            let session_cookie = cookie_value(req, COOKIE_NAME);
            let anonymous_uid = cookie_value(req, ANONYMOUS_UID_COOKIE);
            let request = OAuthCallbackRequest {
                service_type,
                path: "/callback",
                query: &query,
                session_cookie: session_cookie.as_deref(),
                anonymous_uid: anonymous_uid.as_deref(),
            };
            let state_param = query_get(&query, "state");
            let resolver = resolver_for(service_type, Some(&state_param), deps);
            let license_expired = matches!(
                deps.license.product_license().await,
                LicenseState::Expired | LicenseState::Error
            );
            let flow_deps = flow_deps(config, deps, resolver.as_ref(), license_expired, now);
            handle_callback(&flow_deps, &request).await
        }
        // `http.ServeMux` only registers `/login` and `/callback`; every
        // other path under the stripped prefix is a 404 from the flow mux.
        _ => return Some(with_outer_vary(not_found_response())),
    };
    Some(with_outer_vary(flow_response(flow, &method)))
}

/// The App listener runs `AccessTokenAuthMiddleware` and
/// `session.CookieMiddleware` outside the provider middlewares, so every
/// response (including the flow's) carries their `Vary` values in this order.
fn with_outer_vary(mut response: Response<BoxBody>) -> Response<BoxBody> {
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Authorization"));
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Cookie"));
    response
}

pub async fn auth_prefix_response<B>(
    req: &Request<B>,
    config: &OAuthRouteConfig,
    deps: &OAuthRouteDeps,
) -> Option<Response<BoxBody>> {
    auth_prefix_response_at(req, config, deps, OffsetDateTime::now_utc()).await
}

fn resolver_for(
    service_type: &str,
    state: Option<&str>,
    deps: &OAuthRouteDeps,
) -> Arc<dyn OAuthUserResolver> {
    deps.resolvers
        .get_for(service_type, state)
        .unwrap_or_else(|| Arc::new(MissingResolver))
}

fn flow_deps<'a>(
    config: &'a OAuthRouteConfig,
    deps: &'a OAuthRouteDeps,
    resolver: &'a dyn OAuthUserResolver,
    license_expired: bool,
    now: OffsetDateTime,
) -> OAuthFlowDeps<'a> {
    OAuthFlowDeps {
        providers: &config.providers,
        store: deps.store.as_ref(),
        ids: deps.ids.as_ref(),
        cookie_key: &deps.cookie_key,
        secure: config.secure,
        external_url: &config.external_url,
        configured_expiry: config.configured_expiry.as_deref(),
        license_expired,
        state_source: deps.state_source.as_ref(),
        http: deps.http.as_ref(),
        resolver,
        users: deps.users.as_ref(),
        events: deps.events.as_ref(),
        now,
    }
}

/// `getExactlyOneOAuthProvider` + the automatic sign-in redirect. `authenticated`
/// is the actor state after the AccessToken/Cookie middlewares, which run
/// before the provider middlewares in the baseline.
pub fn single_provider_redirect<B>(
    req: &Request<B>,
    config: &OAuthRouteConfig,
    authenticated: bool,
) -> Option<Response<BoxBody>> {
    if authenticated {
        return None;
    }
    // `conf.AuthPublic()`: never redirect on dotcom, to keep anonymous access.
    if config.dotcom {
        return None;
    }
    // Access requests are enabled by default; the redirect only happens when
    // they are explicitly disabled.
    if config.access_requests_enabled {
        return None;
    }
    // `!session.HasSignOutCookie(r)`.
    if cookie_value(req, SIGN_OUT_COOKIE).is_some() {
        return None;
    }
    // `isHuman(r)`.
    if !is_human(req) {
        return None;
    }
    // `getExactlyOneOAuthProvider(!r.URL.Query().Has("sourcegraph-operator"))`.
    let skip_soap = !query_has(req, "sourcegraph-operator");
    let single = config.single_oauth(skip_soap)?;

    let redirect = safe_redirect_url(&request_uri(req));
    let location = format!(
        "{}/login?{}",
        single.auth_prefix,
        encode_query_sorted(&[
            ("redirect".to_owned(), redirect),
            ("pc".to_owned(), single.config_id),
        ])
    );
    // `AccessTokenAuth` and `CookieMiddleware` run before the provider
    // middleware in the baseline (`AnonymousUID`/`featureflag` run after it).
    Some(with_outer_vary(redirect_response(req.method(), &location)))
}

/// Whether a chain rejection is `RequireAuthMiddleware`'s sign-in redirect
/// (`/sign-in?returnTo=…`), which the provider middlewares run before.
pub fn is_sign_in_redirect(response: &Response<BoxBody>) -> bool {
    if response.status() != StatusCode::FOUND && response.status() != StatusCode::UNAUTHORIZED {
        return false;
    }
    response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("/sign-in?returnTo="))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn paths_match_only_under_a_mounted_prefix_with_a_slash() {
        assert_eq!(
            match_auth_prefix("/.auth/github/login"),
            Some(AuthFlowPath {
                service_type: "github",
                subpath: "/login"
            })
        );
        assert_eq!(
            match_auth_prefix("/.auth/openidconnect/callback"),
            Some(AuthFlowPath {
                service_type: "openidconnect",
                subpath: "/callback"
            })
        );
        assert_eq!(
            match_auth_prefix("/.auth/github/other").map(|path| path.subpath),
            Some("/other")
        );
        assert_eq!(match_auth_prefix("/.auth/github"), None);
        assert_eq!(match_auth_prefix("/.auth"), None);
        assert_eq!(match_auth_prefix("/.auth/azureDevOps/login"), None);
    }

    #[test]
    fn single_oauth_requires_exactly_one_sign_in_provider() {
        let config = OAuthRouteConfig::from_site(
            &site(
                r#"{"externalURL":"https://sg.example","auth.providers":[{"type":"github","clientID":"cid"}]}"#,
            ),
            false,
        );
        let single = config.single_oauth(true).expect("single github provider");
        assert_eq!(single.auth_prefix, "/.auth/github");
        assert_eq!(single.config_id, "https://github.com/::cid");

        let two = OAuthRouteConfig::from_site(
            &site(
                r#"{"externalURL":"https://sg.example","auth.providers":[{"type":"github","clientID":"cid"},{"type":"builtin"}]}"#,
            ),
            false,
        );
        assert!(two.single_oauth(true).is_none());

        let builtin =
            OAuthRouteConfig::from_site(&site(r#"{"auth.providers":[{"type":"builtin"}]}"#), false);
        assert!(builtin.single_oauth(true).is_none());
    }

    #[test]
    fn sign_in_providers_skip_sourcegraph_operator() {
        let config = OAuthRouteConfig::from_site(
            &site(
                r#"{"auth.providers":[{"type":"github","clientID":"cid"},{"type":"sourcegraph-operator"}]}"#,
            ),
            false,
        );
        assert!(config.single_oauth(true).is_some());
        assert!(config.single_oauth(false).is_none());
    }

    #[test]
    fn gitlab_requires_an_external_url() {
        let config = OAuthRouteConfig::from_site(
            &site(r#"{"auth.providers":[{"type":"gitlab","clientID":"cid"}]}"#),
            false,
        );
        assert!(config.single_oauth(true).is_none());
    }

    #[test]
    fn access_requests_default_to_enabled() {
        assert!(access_requests_enabled(&serde_json::json!({})));
        assert!(access_requests_enabled(
            &serde_json::json!({"auth.accessRequest": {"enabled": true}})
        ));
        assert!(access_requests_enabled(
            &serde_json::json!({"auth.accessRequest": {}})
        ));
        assert!(!access_requests_enabled(
            &serde_json::json!({"auth.accessRequest": {"enabled": false}})
        ));
    }

    #[test]
    fn http_redirect_body_and_headers_match_net_http() {
        let response = redirect_response(&Method::GET, "/.auth/github/login?pc=x");
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()[header::CONTENT_TYPE], CONTENT_TYPE_HTML);
        let response = redirect_response(&Method::POST, "/.auth/github/login");
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn flow_mux_unknown_subpath_is_a_not_found() {
        let response = not_found_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(!is_sign_in_redirect(&response));
    }
}
