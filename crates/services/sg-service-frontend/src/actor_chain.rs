#![forbid(unsafe_code)]

//! M9.8jc: the external actor/authz middleware chain wired in the baseline's
//! order, plus the internal actor pass-through.
//!
//! The decision primitives live in [`crate::actor_middleware`] and
//! [`crate::authz`]; this module only composes them with the production
//! dependency seams and reproduces the baseline's HTTP plumbing (ordered
//! `Vary` values, 401/302/500 responses). See
//! `docs/overviews/m9_8jc_actor_chain_wiring_overview.md`.
//!
//! ```text
//! AccessTokenAuth → Cookie[/WithCSRFSafety] → Auth → AnonymousUID →
//! featureflag → PostAuth → dispatcher
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::Full;
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_session::auth::{SessionStore, SessionUserLookup};

use crate::actor_middleware::{
    apply_access_token, apply_anonymous_uid_middleware, basic_auth_username, cookie_anonymous_uid,
    is_trusted_request, resolve_session_actor, AccessTokenAuth, AccessTokenConfig,
    AccessTokenDecision, AccessTokenInput, AccessTokensAllow, Actor, LicenseForToken,
    SessionActorOutcome,
};
use crate::authz::{
    authorization_header_value, http_error, post_auth_middleware, post_auth_response, LicenseState,
    PostAuthDecision, SiteAdminCheck,
};
use crate::mux::SecurityConfig;

/// The per-configuration-snapshot values the chain reads (`conf.Get()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainConfig {
    pub access_token: AccessTokenConfig,
    /// `dotcom.SourcegraphDotComMode() || conf.Get().AuthPublic`.
    pub auth_public: bool,
    pub security: SecurityConfig,
}

impl ChainConfig {
    pub fn from_site(site: &SiteConfiguration, dotcom: bool) -> Self {
        let raw = site.raw();
        Self {
            access_token: AccessTokenConfig {
                allow: access_tokens_allow(raw),
                dotcom,
            },
            auth_public: dotcom
                || raw
                    .get("auth.public")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            security: SecurityConfig::from_site_config(site),
        }
    }
}

/// `conf.AccessTokensAllow()` (`internal/conf/computed.go:67-81`).
fn access_tokens_allow(raw: &serde_json::Value) -> AccessTokensAllow {
    let Some(config) = raw
        .get("auth.accessTokens")
        .filter(|value| !value.is_null())
    else {
        return AccessTokensAllow::All;
    };
    match config.get("allow").and_then(serde_json::Value::as_str) {
        None | Some("") | Some("all-users-create") => AccessTokensAllow::All,
        Some("site-admin-create") => AccessTokensAllow::Admin,
        Some(_) => AccessTokensAllow::None,
    }
}

// ---------------------------------------------------------------------------
// Dependency seams
// ---------------------------------------------------------------------------

/// The license seam. M9.8je replaces the production implementation; this slice
/// needs only the two observable states the middleware gates on.
#[async_trait]
pub trait LicenseReader: Send + Sync {
    /// `licensing.GetConfiguredProductLicenseInfo()` reduced to
    /// [`LicenseState`]. The baseline never returns `(nil, nil)`; an empty key
    /// is the non-expired free tier.
    async fn product_license(&self) -> LicenseState;
}

/// `auth.CheckCurrentUserIsSiteAdmin` / `auth.CheckUserIsSiteAdmin`.
#[async_trait]
pub trait SiteAdminChecker: Send + Sync {
    /// The current actor's site-admin check (`PostAuthMiddleware`).
    async fn current_user(&self, actor: &Actor) -> SiteAdminCheck;

    /// `CheckUserIsSiteAdmin(subject_user_id)`; `false` for a missing user,
    /// `Err` only for a database failure (both make the sudo token fail).
    async fn subject_is_site_admin(&self, uid: i32) -> Result<bool, String>;
}

/// The production seams; see [`crate::actor_chain_adapters`].
pub struct ChainDeps {
    pub access_token: Arc<dyn AccessTokenAuth>,
    pub session_store: Arc<dyn SessionStore>,
    pub session_users: Arc<dyn SessionUserLookup>,
    /// `SRC_SESSION_COOKIE_KEY` (empty by default, like the baseline).
    pub session_cookie_key: Vec<u8>,
    pub license: Arc<dyn LicenseReader>,
    pub site_admin: Arc<dyn SiteAdminChecker>,
}

impl ChainDeps {
    pub fn new(
        access_token: Arc<dyn AccessTokenAuth>,
        session_store: Arc<dyn SessionStore>,
        session_users: Arc<dyn SessionUserLookup>,
        session_cookie_key: Vec<u8>,
        license: Arc<dyn LicenseReader>,
        site_admin: Arc<dyn SiteAdminChecker>,
    ) -> Self {
        Self {
            access_token,
            session_store,
            session_users,
            session_cookie_key,
            license,
            site_admin,
        }
    }
}

/// Which external chain to run (`API` mounts `/.api/`, `App` everything else).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalMode {
    Api,
    App,
}

/// The request extension carrying the chain-resolved actor to the dispatcher
/// (`graphql_api` prefers it over re-reading the session cookie).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainActor(pub Actor);

/// The chain's outcome. `Proceed.vary` is the ordered list of `Vary` header
/// values the middleware added; the dispatcher appends them to the final
/// response. `Reject` is final and already carries every `Vary` value added
/// before the rejecting middleware.
#[derive(Debug)]
pub enum ChainOutcome {
    Proceed {
        actor: Actor,
        vary: Vec<&'static str>,
    },
    Reject(Response<BoxBody>),
}

/// The chain at `now` (session expiry checks); production uses
/// [`run_external_chain`].
pub async fn run_external_chain_at<B>(
    req: &Request<B>,
    mode: ExternalMode,
    config: &ChainConfig,
    deps: &ChainDeps,
    now: OffsetDateTime,
) -> ChainOutcome {
    let headers = req.headers();
    let path = req.uri().path();
    let mut vary: Vec<&'static str> = Vec::new();

    // --- AccessTokenAuthMiddleware -------------------------------------
    let query_token = query_get(req.uri().query(), "token");
    let basic_auth = basic_auth_username(headers);
    let authorization = authorization_header_value(headers, path);
    let has_token_input = query_token
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        || basic_auth.as_deref().is_some_and(|value| !value.is_empty())
        || authorization
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    // The baseline reads the license only when a token is present; the
    // access-token decision ignores it on the bypass/unrecognized paths.
    let license_for_token = if has_token_input {
        license_for_token(deps.license.product_license().await)
    } else {
        LicenseForToken::Ok { expired: false }
    };
    // `r.URL.RequestURI()` and `cookie.AnonymousUID(r)` for the security-event
    // rows; only the production adapter persists them.
    let request_uri = req
        .uri()
        .path_and_query()
        .map(|path_and_query| path_and_query.as_str())
        .unwrap_or(path);
    let anonymous_uid = cookie_anonymous_uid(headers);
    let scoped_auth = deps
        .access_token
        .request_auth(request_uri, anonymous_uid.as_deref());
    let token_auth: &dyn AccessTokenAuth = match scoped_auth.as_deref() {
        Some(auth) => auth,
        None => deps.access_token.as_ref(),
    };
    let mut actor = match apply_access_token(
        AccessTokenInput {
            path,
            query_token: query_token.as_deref(),
            basic_auth_username: basic_auth.as_deref(),
            authorization: authorization.as_deref(),
        },
        config.access_token,
        license_for_token,
        Actor::default(),
        token_auth,
    )
    .await
    {
        AccessTokenDecision::Proceed {
            actor,
            vary: vary_auth,
        } => {
            if vary_auth {
                vary.push("Authorization");
            }
            actor
        }
        // `token_reject` already records `Vary: Authorization`.
        AccessTokenDecision::Respond(response) => return ChainOutcome::Reject(response),
    };

    // --- session.CookieMiddleware[/WithCSRFSafety] ---------------------
    match mode {
        ExternalMode::Api => {
            vary.push("Cookie, Authorization, X-Requested-With");
            if is_trusted_request(headers, &config.security) {
                match resolve_session_actor(
                    headers,
                    true,
                    deps.session_store.as_ref(),
                    deps.session_users.as_ref(),
                    &deps.session_cookie_key,
                    now,
                )
                .await
                {
                    SessionActorOutcome::Authenticated { uid } => {
                        actor = Actor {
                            uid,
                            from_session_cookie: true,
                            ..Actor::default()
                        };
                    }
                    SessionActorOutcome::StoreUnavailable => {
                        return ChainOutcome::Reject(response_with_vary(
                            session_store_error(),
                            &vary,
                        ));
                    }
                    _ => {}
                }
            }
        }
        ExternalMode::App => {
            vary.push("Cookie");
            match resolve_session_actor(
                headers,
                true,
                deps.session_store.as_ref(),
                deps.session_users.as_ref(),
                &deps.session_cookie_key,
                now,
            )
            .await
            {
                SessionActorOutcome::Authenticated { uid } => {
                    actor = Actor {
                        uid,
                        from_session_cookie: true,
                        ..Actor::default()
                    };
                }
                SessionActorOutcome::StoreUnavailable => {
                    return ChainOutcome::Reject(response_with_vary(session_store_error(), &vary));
                }
                _ => {}
            }
        }
    }

    // --- authMiddlewares.{API,App} -------------------------------------
    if !actor.is_authenticated() && !allow_anonymous_request(config, req.method(), path) {
        let response = match mode {
            ExternalMode::Api => http_error(
                StatusCode::UNAUTHORIZED,
                "Private mode requires authentication.",
            ),
            ExternalMode::App => {
                // `anonymousUIStatusCode`: the raw route acts like the API, so
                // the sign-in redirect carries 401 instead of 302.
                let status = if is_repo_raw_path(req.method(), path) {
                    StatusCode::UNAUTHORIZED
                } else {
                    StatusCode::FOUND
                };
                sign_in_redirect(req, status)
            }
        };
        return ChainOutcome::Reject(response_with_vary(response, &vary));
    }

    // --- actor.AnonymousUIDMiddleware ----------------------------------
    actor = apply_anonymous_uid_middleware(headers, actor);

    // --- featureflag.Middleware ----------------------------------------
    vary.push("Cookie");

    // --- authz.PostAuthMiddleware --------------------------------------
    if actor.is_authenticated() {
        let license = deps.license.product_license().await;
        let site_admin = if matches!(license, LicenseState::Error | LicenseState::Expired) {
            deps.site_admin.current_user(&actor).await
        } else {
            SiteAdminCheck::IsSiteAdmin
        };
        match post_auth_middleware(&actor, license, site_admin) {
            PostAuthDecision::Proceed => {}
            reject => {
                let response = post_auth_response(reject).expect("post-auth rejection response");
                return ChainOutcome::Reject(response_with_vary(response, &vary));
            }
        }
    }

    ChainOutcome::Proceed { actor, vary }
}

/// [`run_external_chain_at`] with `time.Now()`.
pub async fn run_external_chain<B>(
    req: &Request<B>,
    mode: ExternalMode,
    config: &ChainConfig,
    deps: &ChainDeps,
) -> ChainOutcome {
    run_external_chain_at(req, mode, config, deps, OffsetDateTime::now_utc()).await
}

fn license_for_token(license: LicenseState) -> LicenseForToken {
    match license {
        LicenseState::Error => LicenseForToken::Error,
        LicenseState::Expired => LicenseForToken::Ok { expired: true },
        LicenseState::None | LicenseState::Valid => LicenseForToken::Ok { expired: false },
    }
}

/// Append `Vary` as separate header values, in the order the middleware added
/// them (Go's `Header().Add`).
pub fn response_with_vary(
    mut response: Response<BoxBody>,
    vary: &[&'static str],
) -> Response<BoxBody> {
    for value in vary {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static(value));
    }
    response
}

/// `authenticateByCookie`'s Redis failure: `WriteHeader(500)` then the raw
/// message (no `Content-Type`, no newline, cookie untouched).
fn session_store_error() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from_static(
        b"failed to read session actor",
    ))));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

/// `url.Values{"returnTo": ...}.Encode()` for the app login redirect.
fn sign_in_redirect<B>(req: &Request<B>, status: StatusCode) -> Response<BoxBody> {
    let return_to = match req.uri().query() {
        Some(query) if !query.is_empty() => format!("{}?{query}", req.uri().path()),
        _ => req.uri().path().to_owned(),
    };
    let location = format!("/sign-in?returnTo={}", query_escape(&return_to));
    go_redirect(req.method(), &location, status)
}

/// `http.Redirect` for the methods the app router serves (GET/HEAD).
fn go_redirect(method: &Method, location: &str, status: StatusCode) -> Response<BoxBody> {
    let get = method == Method::GET;
    let head = method == Method::HEAD;
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if get || head {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    if get {
        // `http.Redirect`: `fmt.Fprintln(w, "<a href=\"…\">"+StatusText+"</a>.\n")`.
        let body = format!(
            "<a href=\"{}\">{}</a>.\n\n",
            crate::authz::html_escape(location),
            status.canonical_reason().unwrap_or(""),
        );
        let (parts, _) = response.into_parts();
        return Response::from_parts(parts, tonic::body::boxed(Full::new(Bytes::from(body))));
    }
    response
}

/// Go `net/url.QueryEscape`: unreserved bytes plus `-_.~` are literal, space
/// becomes `+`, everything else is `%XX` (uppercase hex).
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `r.URL.Query().Get("token")`.
fn query_get(query: Option<&str>, name: &str) -> Option<String> {
    let query = query?;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

// ---------------------------------------------------------------------------
// AllowAnonymousRequest / anonymousStatusCode
// ---------------------------------------------------------------------------

/// `AllowAnonymousRequest` (`cmd/frontend/auth/non_public.go`).
pub fn allow_anonymous_request(config: &ChainConfig, method: &Method, path: &str) -> bool {
    if config.auth_public {
        return true;
    }
    if path.starts_with("/.assets/") {
        return true;
    }
    if path.starts_with("/.api/lsif/upload")
        || path.starts_with("/.api/scip/upload")
        || path.starts_with("/.api/src-cli")
        || path.starts_with("/.executors")
        || path.starts_with("/.api/scim/v2")
    {
        return true;
    }
    for prefix in [
        "/.api/webhooks",
        "/.api/github-webhooks",
        "/.api/gitlab-webhooks",
        "/.api/bitbucket-server-webhooks",
        "/.api/bitbucket-cloud-webhooks",
    ] {
        if path.starts_with(prefix) {
            return true;
        }
    }

    match crate::app_router::resolve(method, path) {
        crate::app_router::Resolution::Route { name, .. } => match name {
            crate::app_router::RouteName::Ui => ui_anonymous_request(method, path),
            other => api_anonymous_route(other),
        },
        // A StrictSlash redirect is answered before the chain; not anonymous.
        crate::app_router::Resolution::Redirect { .. } => false,
    }
}

/// `anonymousAccessibleAPIRoutes`: the app-router route names.
fn api_anonymous_route(name: crate::app_router::RouteName) -> bool {
    use crate::app_router::RouteName;
    matches!(
        name,
        RouteName::RobotsTxt
            | RouteName::OpenSearch
            | RouteName::SitemapXmlGz
            | RouteName::Favicon
            | RouteName::Logout
            | RouteName::SignUp
            | RouteName::RequestAccess
            | RouteName::SiteInit
            | RouteName::SignIn
            | RouteName::SignOut
            | RouteName::UnlockAccount
            | RouteName::ResetPasswordInit
            | RouteName::ResetPasswordCode
            | RouteName::CheckUsernameTaken
    )
}

/// `anonymousAccessibleUIRoutes`. The UI router itself is not ported; the
/// paths below are its anonymous entries. `RouteRaw` is deliberately absent
/// (it is the one UI route whose unauthenticated answer is a 401; see
/// [`is_repo_raw_path`]).
fn ui_anonymous_request(method: &Method, path: &str) -> bool {
    // The two routes registered with explicit methods.
    if path == "/sign-in" {
        return *method == Method::GET || *method == Method::HEAD;
    }
    if path == "/ping-from-self-hosted" {
        return *method == Method::GET || *method == Method::OPTIONS;
    }
    // The remaining entries are registered without a method matcher, so any
    // method is anonymous-accessible.
    path == "/sign-up"
        || path == "/password-reset"
        || path.starts_with("/request-access")
        || path
            .strip_prefix("/unlock-account/")
            .is_some_and(|token| !token.is_empty() && !token.contains('/'))
}

/// `uirouter.RouteRaw`: `/<repo>[@<rev>]/-/raw...` (GET/HEAD). Approximate
/// because the UI router's repo pattern is not ported.
fn is_repo_raw_path(method: &Method, path: &str) -> bool {
    if method != Method::GET && method != Method::HEAD {
        return false;
    }
    let Some(prefix) = path.strip_suffix("/-/raw").or_else(|| {
        path.find("/-/raw/")
            .map(|index| &path[..index + "/-/raw".len()])
    }) else {
        return false;
    };
    prefix.len() > 1 && !prefix.starts_with("/-/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(auth_public: bool) -> ChainConfig {
        ChainConfig {
            access_token: AccessTokenConfig {
                allow: AccessTokensAllow::All,
                dotcom: false,
            },
            auth_public,
            security: SecurityConfig::default(),
        }
    }

    #[test]
    fn access_token_allow_matches_the_baseline() {
        assert_eq!(
            access_tokens_allow(&serde_json::json!({})),
            AccessTokensAllow::All
        );
        assert_eq!(
            access_tokens_allow(&serde_json::json!({"auth.accessTokens": null})),
            AccessTokensAllow::All
        );
        assert_eq!(
            access_tokens_allow(&serde_json::json!({"auth.accessTokens": {"allow": ""}})),
            AccessTokensAllow::All
        );
        assert_eq!(
            access_tokens_allow(
                &serde_json::json!({"auth.accessTokens": {"allow": "site-admin-create"}})
            ),
            AccessTokensAllow::Admin
        );
        assert_eq!(
            access_tokens_allow(&serde_json::json!({"auth.accessTokens": {"allow": "none"}})),
            AccessTokensAllow::None
        );
        assert_eq!(
            access_tokens_allow(&serde_json::json!({"auth.accessTokens": {"allow": "bogus"}})),
            AccessTokensAllow::None
        );
    }

    #[test]
    fn anonymous_routes_match_the_baseline() {
        let private = config(false);
        for (method, path) in [
            (Method::GET, "/robots.txt"),
            (Method::GET, "/sitemap_1.xml.gz"),
            (Method::GET, "/favicon.ico"),
            (Method::GET, "/opensearch.xml"),
            (Method::GET, "/-/logout"),
            (Method::POST, "/-/sign-up"),
            (Method::POST, "/-/request-access"),
            (Method::POST, "/-/site-init"),
            (Method::POST, "/-/sign-in"),
            (Method::GET, "/-/sign-out"),
            (Method::POST, "/-/unlock-account"),
            (Method::POST, "/-/reset-password-init"),
            (Method::POST, "/-/reset-password-code"),
            (Method::GET, "/-/check-username-taken/alice"),
            (Method::GET, "/sign-in"),
            (Method::GET, "/sign-up"),
            (Method::GET, "/password-reset"),
            (Method::GET, "/request-access"),
            (Method::GET, "/unlock-account/token"),
            (Method::GET, "/.assets/foo.js"),
            (Method::GET, "/.api/lsif/upload"),
            (Method::GET, "/.api/scim/v2/Users"),
            (Method::POST, "/.api/webhooks/foo"),
        ] {
            assert!(
                allow_anonymous_request(&private, &method, path),
                "{method} {path}"
            );
        }
        for (method, path) in [
            (Method::POST, "/.api/graphql"),
            (Method::GET, "/"),
            (Method::GET, "/search"),
            (Method::GET, "/-/verify-email"),
            (Method::POST, "/-/unlock-user-account"),
            (Method::GET, "/sign-in/extra"),
            (Method::GET, "/unlock-account/a/b"),
            // `RouteRaw` is not in `anonymousAccessibleUIRoutes`; it is served
            // to anonymous clients only by the 401 sign-in redirect.
            (Method::GET, "/github.com/org/repo/-/raw/main/file.go"),
        ] {
            assert!(
                !allow_anonymous_request(&private, &method, path),
                "{method} {path}"
            );
        }
        assert!(allow_anonymous_request(&config(true), &Method::GET, "/"));
    }

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(query_escape("/a b?x=1&y=2"), "%2Fa+b%3Fx%3D1%26y%3D2");
        assert_eq!(query_escape("a-_.~"), "a-_.~");
        assert_eq!(query_escape("é"), "%C3%A9");
    }

    #[test]
    fn raw_paths_are_detected() {
        assert!(is_repo_raw_path(
            &Method::GET,
            "/github.com/org/repo@main/-/raw/file.go"
        ));
        assert!(is_repo_raw_path(&Method::GET, "/github.com/org/repo/-/raw"));
        assert!(!is_repo_raw_path(&Method::POST, "/a/b/-/raw"));
        assert!(!is_repo_raw_path(&Method::GET, "/-/raw/file"));
    }
}
