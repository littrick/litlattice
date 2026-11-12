#![forbid(unsafe_code)]

//! Ports of the external HTTP chain's actor semantics
//! (`internal/actor/{actor.go,http.go}`) plus the trusted-session and access
//! token decisions that `newExternalHTTPHandler` composes around them.
//!
//! Baseline chain (LIFO, outermost first), relevant parts only:
//!
//! ```text
//! AccessTokenAuthMiddleware → CookieMiddleware[/WithCSRFSafety] →
//! auth middleware → AnonymousUIDMiddleware → featureflag →
//! authz.PostAuthMiddleware → handler
//! ```
//!
//! The internal listener instead runs `actor.HTTPMiddleware` (the
//! `X-Sourcegraph-Actor-UID` header is only trusted there).
//!
//! This module exposes the individual decisions, each with its dependencies
//! injected:
//!
//! * [`apply_actor_header`] — `actor.HTTPMiddleware`;
//! * [`apply_anonymous_uid_middleware`] — `actor.AnonymousUIDMiddleware`;
//! * [`transport_headers`] — `actor.HTTPTransport.RoundTrip`;
//! * [`is_trusted_request`], [`session_cookie`], [`resolve_session_actor`] —
//!   `session.CookieMiddlewareWithCSRFSafety` / `authenticateByCookie`
//!   (the decision only; the session write path is M9.8iv);
//! * [`apply_access_token`] — `httpapi.AccessTokenAuthMiddleware`.
//!
//! The production wiring (which middleware wraps which handler) is left to the
//! parent slice; the functions here are the seams it composes.

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::Full;
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_session::auth::{SessionAuthOutcome, SessionStore, SessionUserLookup};
use sg_session::COOKIE_NAME;

use crate::audit::{AuditActor, ClientInfo};
use crate::authz::{http_error, parse_authorization_header, SCOPE_SITE_ADMIN_SUDO, SCOPE_USER_ALL};
use crate::mux::SecurityConfig;

// ---------------------------------------------------------------------------
// Actor (internal/actor/actor.go)
// ---------------------------------------------------------------------------

/// `actor.Actor` reduced to the fields the middleware chain observes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Actor {
    /// `UID`: authenticated user ID; `0` means none.
    pub uid: i32,
    /// `AnonymousUID`: semi-stable anonymous ID (may coexist with `uid`).
    pub anonymous_uid: String,
    /// `Internal`: an internal service actor.
    pub internal: bool,
    /// `SourcegraphOperator`.
    pub sourcegraph_operator: bool,
    /// `FromSessionCookie` (`json:"-"`): the actor came from `sgs`.
    pub from_session_cookie: bool,
}

impl Actor {
    /// `actor.FromUser`.
    pub fn from_user(uid: i32) -> Self {
        Self {
            uid,
            ..Self::default()
        }
    }

    /// `actor.FromAnonymousUser`.
    pub fn from_anonymous_user(anonymous_uid: impl Into<String>) -> Self {
        Self {
            anonymous_uid: anonymous_uid.into(),
            ..Self::default()
        }
    }

    /// `actor.Internal`.
    pub fn internal() -> Self {
        Self {
            internal: true,
            ..Self::default()
        }
    }

    /// `(*Actor).IsAuthenticated`.
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }

    /// `(*Actor).IsInternal`.
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// `(*Actor).UIDString`.
    pub fn uid_string(&self) -> String {
        self.uid.to_string()
    }

    /// The `audit.Log` view of this actor (`actor.FromContext` reduced to the
    /// fields `audit.actorId` and the internal-actor filter read).
    ///
    /// `audit.actorId` only honours a **positive** UID (`audit.go:61-69`), so a
    /// `UID == 0` actor keeps its anonymous UID.
    pub fn to_audit_actor(&self) -> AuditActor {
        AuditActor {
            uid: (self.uid > 0).then_some(self.uid),
            anonymous_uid: self.anonymous_uid.clone(),
            internal: self.internal,
        }
    }

    /// The `sg-graphql` executor's actor. Precedence mirrors Go's
    /// `IsInternal`/`IsAuthenticated`: internal wins, then `uid != 0`, then
    /// anonymous.
    pub fn to_graphql(&self) -> sg_graphql::exec::Actor {
        if self.is_internal() {
            sg_graphql::exec::Actor::Internal
        } else if self.is_authenticated() {
            sg_graphql::exec::Actor::User { uid: self.uid }
        } else {
            sg_graphql::exec::Actor::Anonymous
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound actor header (actor.HTTPMiddleware)
// ---------------------------------------------------------------------------

/// `headerKeyActorUID`.
pub const HEADER_ACTOR_UID: &str = "x-sourcegraph-actor-uid";
/// `headerKeyActorAnonymousUID`.
pub const HEADER_ACTOR_ANONYMOUS_UID: &str = "x-sourcegraph-actor-anonymous-uid";
/// `headerValueInternalActor`.
pub const ACTOR_INTERNAL: &str = "internal";
/// `headerValueNoActor`.
pub const ACTOR_NONE: &str = "none";

/// `actor.HTTPMiddleware`'s outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorHeaderDecision {
    Proceed(Actor),
    /// Invalid numeric UID: the baseline answers 403 with no `Content-Type`
    /// (`WriteHeader` followed by `Write`).
    Reject {
        status: u16,
        body: String,
    },
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
}

/// `actor.HTTPMiddleware`'s actor resolution. The baseline always starts from
/// an empty actor (it ignores any actor already in the request context).
pub fn apply_actor_header(headers: &HeaderMap) -> ActorHeaderDecision {
    let mut actor = Actor::default();
    match header_text(headers, HEADER_ACTOR_UID).as_deref() {
        Some(ACTOR_INTERNAL) => actor = Actor::internal(),
        // `case "", headerValueNoActor`: no authenticated actor.
        None | Some("") | Some(ACTOR_NONE) => {}
        Some(uid_str) => match uid_str.parse::<i64>() {
            // `strconv.Atoi` is 64-bit; `FromUser(int32(uid))` truncates.
            Ok(uid) => actor = Actor::from_user(uid as i32),
            Err(_) => {
                return ActorHeaderDecision::Reject {
                    status: StatusCode::FORBIDDEN.as_u16(),
                    body: "X-Sourcegraph-Actor-UID was provided, but the value was invalid"
                        .to_owned(),
                };
            }
        },
    }

    // Always preserve the AnonymousUID.
    if let Some(anonymous_uid) = header_text(headers, HEADER_ACTOR_ANONYMOUS_UID) {
        if !anonymous_uid.is_empty() {
            actor.anonymous_uid = anonymous_uid;
        }
    }
    ActorHeaderDecision::Proceed(actor)
}

// ---------------------------------------------------------------------------
// Anonymous UID middleware (actor.AnonymousUIDMiddleware)
// ---------------------------------------------------------------------------

/// `cookie.AnonymousUID`: the first `sourcegraphAnonymousUid` cookie.
pub fn cookie_anonymous_uid(headers: &HeaderMap) -> Option<String> {
    first_cookie(headers, "sourcegraphAnonymousUid")
}

/// `actor.AnonymousUIDMiddleware`: the cookie is authoritative, the propagation
/// header is consulted only when there is no cookie. An authenticated or
/// internal actor is never cleared.
pub fn apply_anonymous_uid_middleware(headers: &HeaderMap, existing: Actor) -> Actor {
    let anonymous_uid = cookie_anonymous_uid(headers)
        .or_else(|| {
            header_text(headers, HEADER_ACTOR_ANONYMOUS_UID).filter(|value| !value.is_empty())
        })
        .unwrap_or_default();

    if !existing.is_authenticated() && !existing.is_internal() {
        if anonymous_uid.is_empty() {
            return existing;
        }
        return Actor::from_anonymous_user(anonymous_uid);
    }

    let mut actor = existing;
    if !anonymous_uid.is_empty() {
        actor.anonymous_uid = anonymous_uid;
    }
    actor
}

// ---------------------------------------------------------------------------
// Outbound propagation (actor.HTTPTransport)
// ---------------------------------------------------------------------------

/// `actor.HTTPTransport.RoundTrip`'s headers, in insertion order.
pub fn transport_headers(actor: &Actor) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    if !actor.anonymous_uid.is_empty() {
        headers.push((
            HEADER_ACTOR_ANONYMOUS_UID.to_owned(),
            actor.anonymous_uid.clone(),
        ));
    }
    let uid = if actor.is_internal() {
        ACTOR_INTERNAL.to_owned()
    } else if actor.is_authenticated() {
        actor.uid_string()
    } else {
        ACTOR_NONE.to_owned()
    };
    headers.push((HEADER_ACTOR_UID.to_owned(), uid));
    headers
}

// ---------------------------------------------------------------------------
// Session / trusted decisions (session.go, cli/http.go)
// ---------------------------------------------------------------------------

/// `isTrustedOrigin` (`cmd/frontend/internal/cli/http.go:352`).
pub fn is_trusted_origin(origin: &str, security: &SecurityConfig) -> bool {
    security.is_trusted_origin(origin)
}

/// `CookieMiddlewareWithCSRFSafety`'s trusted predicate: an `X-Requested-With`
/// header (any value) or a trusted `Origin`.
pub fn is_trusted_request(headers: &HeaderMap, security: &SecurityConfig) -> bool {
    if headers.contains_key("x-requested-with") {
        return true;
    }
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    is_trusted_origin(origin, security)
}

/// `r.Cookie("sgs")`: the first matching `Cookie` header value, unquoted.
pub fn session_cookie(headers: &HeaderMap) -> Option<String> {
    first_cookie(headers, COOKIE_NAME)
}

/// `net/http` strips one pair of surrounding double quotes from request cookie
/// values.
pub fn unquote_cookie_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .to_owned()
}

/// The first cookie named `name` across the request's `Cookie` headers.
fn first_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(cookies) = value.to_str() else {
            continue;
        };
        for cookie in cookies.split(';') {
            let cookie = cookie.trim();
            let Some((cookie_name, cookie_value)) = cookie.split_once('=') else {
                continue;
            };
            if cookie_name.trim() == name {
                return Some(unquote_cookie_value(cookie_value.trim()));
            }
        }
    }
    None
}

/// `cookie.AnonymousUID` followed by `AnonymousUIDMiddleware`'s header
/// fallback (used for the audit actor ID by `graphql_api`).
pub fn actor_anonymous_uid(headers: &HeaderMap) -> String {
    if let Some(uid) = cookie_anonymous_uid(headers) {
        return uid;
    }
    header_text(headers, HEADER_ACTOR_ANONYMOUS_UID).unwrap_or_default()
}

/// `session.authenticateByCookie`'s actor decision, gated on
/// `CookieMiddlewareWithCSRFSafety`'s trusted predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionActorOutcome {
    /// Trusted request with a valid session.
    Authenticated { uid: i32 },
    /// Untrusted request: the cookie is never read.
    Untrusted,
    /// The session decision kept the request anonymous.
    Anonymous,
    /// Redis failed: the baseline answers 500 `failed to read session actor`
    /// and keeps the cookie.
    StoreUnavailable,
}

/// Resolves the session actor for a request, reading the `sgs` cookie only when
/// the request is trusted.
pub async fn resolve_session_actor(
    headers: &HeaderMap,
    trusted: bool,
    store: &dyn SessionStore,
    users: &dyn SessionUserLookup,
    cookie_key: &[u8],
    now: OffsetDateTime,
) -> SessionActorOutcome {
    if !trusted {
        return SessionActorOutcome::Untrusted;
    }
    let Some(cookie) = session_cookie(headers) else {
        return SessionActorOutcome::Anonymous;
    };
    match sg_session::auth::authenticate(store, users, cookie_key, Some(&cookie), now).await {
        SessionAuthOutcome::Authenticated { uid } => SessionActorOutcome::Authenticated { uid },
        SessionAuthOutcome::Anonymous(_) => SessionActorOutcome::Anonymous,
        SessionAuthOutcome::StoreUnavailable => SessionActorOutcome::StoreUnavailable,
    }
}

// ---------------------------------------------------------------------------
// Access token middleware (httpapi.AccessTokenAuthMiddleware)
// ---------------------------------------------------------------------------

/// One access-token identity (`types.User` reduced to what sudo needs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessTokenUser {
    pub uid: i32,
    pub username: String,
}

/// `db.AccessTokens().Lookup` error classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTokenLookupError {
    /// `database.ErrAccessTokenNotFound`.
    NotFound,
    /// `database.InvalidTokenError`.
    Invalid,
    /// Any other error (500).
    Other(String),
}

/// `db.Users().GetByUsername`'s outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTokenUserLookup {
    Found(AccessTokenUser),
    /// `errcode.IsNotFound`: 403 `Unable to sudo to nonexistent user.`
    NotFound,
    /// Any other error: 403 unexpected-error text.
    Error(String),
}

/// The details of a security event emitted by `AccessTokenAuthMiddleware`.
/// The production adapter adds the request URI/anonymous UID it captured when
/// the per-request wrapper was built; the default trait implementation only
/// forwards the event name (the M9.8ja seam).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessTokenSecurityEvent<'a> {
    pub name: &'a str,
    /// `SecurityEvent.UserID`.
    pub user_id: i32,
    /// The token's subject user (`0` when the token was not found).
    pub subject_user_id: i32,
    pub sudo_user_id: i32,
    pub sudo_user: &'a str,
    pub token_subject_user_id: i32,
    pub token_subject_user_name: &'a str,
    /// `actor.SourcegraphOperator` for the event's audit context: false for
    /// the two pre-lookup failures, the looked-up value for
    /// `AccessTokenImpersonated` (`auth.go:245-249,281-287`).
    pub sourcegraph_operator: bool,
}

/// The pre-token request actor and request client used for the
/// `security_event_logs` downstream sinks. The baseline resolves
/// `actor.FromContext` inside `AccessTokenAuthMiddleware`, which runs outside
/// `session.CookieMiddleware`/`AnonymousUIDMiddleware`; the actor is therefore
/// the zero actor, and only `AccessTokenImpersonated` overrides the UID.
#[derive(Debug, Clone, Copy)]
pub struct AccessTokenAuditContext<'a> {
    pub actor: &'a AuditActor,
    pub client: Option<&'a ClientInfo>,
}

/// The database seams `AccessTokenAuthMiddleware` reads. The production
/// adapter is `db.AccessTokens`/`db.Users`/`db.UserExternalAccounts`/
/// `db.SecurityEventLogs`.
#[async_trait]
pub trait AccessTokenAuth: Send + Sync {
    /// `AccessTokens().Lookup(token, {RequiredScope, OnlyAdmin})`.
    async fn lookup(
        &self,
        token: &str,
        required_scope: &str,
        only_admin: bool,
    ) -> Result<i32, AccessTokenLookupError>;

    /// `UserExternalAccounts().Count(UserID, ServiceType=sourcegraphoperator)`.
    async fn count_soap_accounts(&self, uid: i32) -> Result<usize, String>;

    /// `auth.CheckUserIsSiteAdmin(ctx, db, subjectUserID)`.
    async fn is_subject_site_admin(&self, subject_uid: i32) -> Result<bool, String>;

    /// `Users().GetByUsername`.
    async fn get_user_by_username(&self, username: &str) -> AccessTokenUserLookup;

    /// `SecurityEventLogs().LogSecurityEvent` (errors are logged, not returned).
    async fn log_security_event(&self, name: &str);

    /// A per-request view that adds the request URI and anonymous UID to
    /// security events (the production adapter overrides this; the default
    /// keeps the decision-level seam unchanged).
    fn request_auth<'a>(
        &'a self,
        _url: &'a str,
        _anonymous_uid: Option<&'a str>,
        _audit: Option<AccessTokenAuditContext<'a>>,
    ) -> Option<Box<dyn AccessTokenAuth + 'a>> {
        None
    }

    /// `db.Users().GetByID(subjectUID)`'s username, used only for the
    /// `AccessTokenImpersonated` argument (the baseline ignores errors).
    async fn get_user_by_id(&self, _uid: i32) -> Option<String> {
        None
    }

    /// The full event. Default implementations (including the M9.8ja
    /// differential double) only need [`Self::log_security_event`].
    async fn security_event(&self, event: AccessTokenSecurityEvent<'_>) {
        self.log_security_event(event.name).await;
    }
}

/// `schema.AuthAccessTokens.Allow`:
/// `all-users-create` (default) / `site-admin-create` / `none`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessTokensAllow {
    All,
    Admin,
    None,
}

/// `licensing.GetConfiguredProductLicenseInfo()` reduced to the access-token
/// gate's needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseForToken {
    /// The license lookup failed (500).
    Error,
    /// The license is readable; `expired` selects `OnlyAdmin`.
    Ok { expired: bool },
}

/// `conf.AccessTokenAllow` plus `dotcom.SourcegraphDotComMode()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessTokenConfig {
    pub allow: AccessTokensAllow,
    pub dotcom: bool,
}

/// The inputs the middleware reads from the request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccessTokenInput<'a> {
    pub path: &'a str,
    /// `r.URL.Query().Get("token")`.
    pub query_token: Option<&'a str>,
    /// `r.BasicAuth()`'s username.
    pub basic_auth_username: Option<&'a str>,
    /// `parseAuthorizationHeaderValue(r)`; `None` when absent/empty.
    pub authorization: Option<&'a str>,
}

/// `AccessTokenAuthMiddleware`'s outcome.
#[derive(Debug)]
pub enum AccessTokenDecision {
    Proceed {
        actor: Actor,
        /// `Vary: Authorization` is written for every non-bypass request.
        vary: bool,
    },
    Respond(Response<BoxBody>),
}

impl PartialEq for AccessTokenDecision {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                AccessTokenDecision::Proceed { actor, vary },
                AccessTokenDecision::Proceed {
                    actor: other_actor,
                    vary: other_vary,
                },
            ) => actor == other_actor && vary == other_vary,
            (AccessTokenDecision::Respond(left), AccessTokenDecision::Respond(right)) => {
                left.status() == right.status() && left.headers() == right.headers()
            }
            _ => false,
        }
    }
}

impl Eq for AccessTokenDecision {}

/// `database.SecurityEventAccessTokenInvalid`.
pub const SECURITY_EVENT_ACCESS_TOKEN_INVALID: &str = "AccessTokenInvalid";
/// `database.SecurityEventAccessTokenSubjectNotSiteAdmin`.
pub const SECURITY_EVENT_ACCESS_TOKEN_SUBJECT_NOT_SITE_ADMIN: &str =
    "AccessTokenSubjectNotSiteAdmin";
/// `database.SecurityEventAccessTokenImpersonated`.
pub const SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED: &str = "AccessTokenImpersonated";

fn token_reject(status: StatusCode, message: &str) -> AccessTokenDecision {
    let mut response = http_error(status, message);
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Authorization"));
    AccessTokenDecision::Respond(response)
}

/// `AccessTokenAuthMiddleware` without the HTTP plumbing. `initial` is the
/// actor the inner chain would otherwise see (bypass and no-token paths return
/// it unchanged; a valid token overwrites it).
pub async fn apply_access_token(
    input: AccessTokenInput<'_>,
    config: AccessTokenConfig,
    license: LicenseForToken,
    initial: Actor,
    auth: &dyn AccessTokenAuth,
) -> AccessTokenDecision {
    // SCIM uses an auth token checked separately in the SCIM package.
    if input.path.starts_with("/.api/scim/v2") {
        return AccessTokenDecision::Proceed {
            actor: initial,
            vary: false,
        };
    }
    // Temporary(sourcegraph#59625) SSC API uses SAMS auth token.
    if input.path.starts_with("/.api/ssc/") {
        return AccessTokenDecision::Proceed {
            actor: initial,
            vary: false,
        };
    }
    // The license check handler checks its own Bearer token.
    if config.dotcom && input.path.starts_with("/.api/license/check") {
        return AccessTokenDecision::Proceed {
            actor: initial,
            vary: false,
        };
    }

    let mut token = input.query_token.unwrap_or("").to_owned();
    let mut sudo_user = String::new();

    if token.is_empty() {
        // Basic auth (https://<token>@sourcegraph.com/foobar).
        if let Some(username) = input.basic_auth_username {
            if !username.is_empty() {
                token = username.to_owned();
            }
        }
    }

    if token.is_empty() {
        if let Some(header_value) = input.authorization.filter(|value| !value.is_empty()) {
            match parse_authorization_header(header_value, config.dotcom) {
                Ok((parsed_token, parsed_sudo)) => {
                    token = parsed_token;
                    sudo_user = parsed_sudo;
                }
                Err(error) => {
                    if !config.dotcom && error.is_unrecognized_scheme() {
                        // Ignore Authorization headers we don't handle.
                        return AccessTokenDecision::Proceed {
                            actor: initial,
                            vary: true,
                        };
                    }
                    return token_reject(StatusCode::UNAUTHORIZED, "Invalid Authorization header.");
                }
            }
        }
    }

    if !token.is_empty() {
        if config.allow == AccessTokensAllow::None {
            return token_reject(
                StatusCode::UNAUTHORIZED,
                "Access token authorization is disabled.",
            );
        }

        let required_scope = if sudo_user.is_empty() {
            SCOPE_USER_ALL
        } else {
            SCOPE_SITE_ADMIN_SUDO
        };

        let only_admin = match license {
            LicenseForToken::Error => {
                return token_reject(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Could not check license for access token authorization.",
                );
            }
            LicenseForToken::Ok { expired } => expired,
        };

        let subject_user_id = match auth.lookup(&token, required_scope, only_admin).await {
            Ok(uid) => uid,
            Err(AccessTokenLookupError::NotFound | AccessTokenLookupError::Invalid) => {
                auth.security_event(AccessTokenSecurityEvent {
                    name: SECURITY_EVENT_ACCESS_TOKEN_INVALID,
                    user_id: initial.uid,
                    subject_user_id: 0,
                    sudo_user_id: 0,
                    sudo_user: "",
                    token_subject_user_id: 0,
                    token_subject_user_name: "",
                    sourcegraph_operator: false,
                })
                .await;
                return token_reject(StatusCode::UNAUTHORIZED, "Invalid access token.");
            }
            Err(AccessTokenLookupError::Other(message)) => {
                return token_reject(StatusCode::INTERNAL_SERVER_ERROR, &message);
            }
        };

        let sourcegraph_operator = match auth.count_soap_accounts(subject_user_id).await {
            Ok(count) => count > 0,
            Err(message) => {
                return token_reject(StatusCode::INTERNAL_SERVER_ERROR, &message);
            }
        };

        let actor_user_id = if sudo_user.is_empty() {
            subject_user_id
        } else {
            // 🚨 SECURITY: the sudo token's subject must still be a site
            // admin (a demoted user must not retain sudo privileges).
            if !matches!(auth.is_subject_site_admin(subject_user_id).await, Ok(true)) {
                auth.security_event(AccessTokenSecurityEvent {
                    name: SECURITY_EVENT_ACCESS_TOKEN_SUBJECT_NOT_SITE_ADMIN,
                    user_id: subject_user_id,
                    subject_user_id,
                    sudo_user_id: 0,
                    sudo_user: "",
                    token_subject_user_id: 0,
                    token_subject_user_name: "",
                    sourcegraph_operator: false,
                })
                .await;
                return token_reject(
                    StatusCode::FORBIDDEN,
                    "The subject user of a sudo access token must be a site admin.",
                );
            }
            match auth.get_user_by_username(&sudo_user).await {
                AccessTokenUserLookup::Found(user) => {
                    // The baseline reads the subject's username for the audit
                    // argument and ignores a lookup failure.
                    let token_subject_user_name = auth
                        .get_user_by_id(subject_user_id)
                        .await
                        .unwrap_or_default();
                    auth.security_event(AccessTokenSecurityEvent {
                        name: SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED,
                        user_id: subject_user_id,
                        subject_user_id,
                        sudo_user_id: user.uid,
                        sudo_user: &user.username,
                        token_subject_user_id: subject_user_id,
                        token_subject_user_name: &token_subject_user_name,
                        sourcegraph_operator,
                    })
                    .await;
                    user.uid
                }
                AccessTokenUserLookup::NotFound => {
                    return token_reject(
                        StatusCode::FORBIDDEN,
                        "Unable to sudo to nonexistent user.",
                    );
                }
                AccessTokenUserLookup::Error(_) => {
                    return token_reject(
                        StatusCode::FORBIDDEN,
                        "Unable to sudo to the specified user due to an unexpected error.",
                    );
                }
            }
        };

        // The baseline overwrites the actor, dropping any AnonymousUID.
        return AccessTokenDecision::Proceed {
            actor: Actor {
                uid: actor_user_id,
                sourcegraph_operator,
                ..Actor::default()
            },
            vary: true,
        };
    }

    AccessTokenDecision::Proceed {
        actor: initial,
        vary: true,
    }
}

/// `r.BasicAuth()`'s username (`Authorization: Basic base64(user:pass)`).
pub fn basic_auth_username(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, _) = decoded.split_once(':')?;
    Some(username.to_owned())
}

/// `http.StatusForbidden` body response for an invalid actor header (no
/// `Content-Type`, matching `WriteHeader` before `Write`).
pub fn actor_header_reject_response(status: u16, body: String) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(headers: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn actor_header_cases() {
        assert_eq!(
            apply_actor_header(&request(&[])),
            ActorHeaderDecision::Proceed(Actor::default())
        );
        assert_eq!(
            apply_actor_header(&request(&[(HEADER_ACTOR_UID, "none")])),
            ActorHeaderDecision::Proceed(Actor::default())
        );
        assert_eq!(
            apply_actor_header(&request(&[(HEADER_ACTOR_UID, "internal")])),
            ActorHeaderDecision::Proceed(Actor::internal())
        );
        assert_eq!(
            apply_actor_header(&request(&[(HEADER_ACTOR_UID, "42")])),
            ActorHeaderDecision::Proceed(Actor::from_user(42))
        );
        // 64-bit parse then int32 truncation, like `strconv.Atoi` + cast.
        assert_eq!(
            apply_actor_header(&request(&[(HEADER_ACTOR_UID, "4294967297")])),
            ActorHeaderDecision::Proceed(Actor::from_user(1))
        );
        let reject = apply_actor_header(&request(&[(HEADER_ACTOR_UID, "abc")]));
        assert!(matches!(
            reject,
            ActorHeaderDecision::Reject { status: 403, .. }
        ));
        // Anonymous UID is preserved on every path.
        assert_eq!(
            apply_actor_header(&request(&[
                (HEADER_ACTOR_UID, "7"),
                (HEADER_ACTOR_ANONYMOUS_UID, "anon"),
            ])),
            ActorHeaderDecision::Proceed(Actor {
                uid: 7,
                anonymous_uid: "anon".to_owned(),
                ..Actor::default()
            })
        );
    }

    #[test]
    fn anonymous_uid_precedence_and_non_clobber() {
        let cookie = request(&[("cookie", "sourcegraphAnonymousUid=cookie")]);
        let header_only = request(&[(HEADER_ACTOR_ANONYMOUS_UID, "header")]);
        let both = request(&[
            ("cookie", "sourcegraphAnonymousUid=cookie"),
            (HEADER_ACTOR_ANONYMOUS_UID, "header"),
        ]);
        assert_eq!(
            apply_anonymous_uid_middleware(&cookie, Actor::default()),
            Actor::from_anonymous_user("cookie")
        );
        assert_eq!(
            apply_anonymous_uid_middleware(&header_only, Actor::default()),
            Actor::from_anonymous_user("header")
        );
        assert_eq!(
            apply_anonymous_uid_middleware(&both, Actor::default()),
            Actor::from_anonymous_user("cookie")
        );
        // Authenticated actor: only AnonymousUID is filled in.
        let authenticated = Actor::from_user(9);
        assert_eq!(
            apply_anonymous_uid_middleware(&cookie, authenticated.clone()),
            Actor {
                anonymous_uid: "cookie".to_owned(),
                ..authenticated
            }
        );
        // Internal actor: also only AnonymousUID is filled in.
        assert_eq!(
            apply_anonymous_uid_middleware(&header_only, Actor::internal()),
            Actor {
                anonymous_uid: "header".to_owned(),
                ..Actor::internal()
            }
        );
    }

    #[test]
    fn transport_header_cases() {
        assert_eq!(
            transport_headers(&Actor::from_user(5)),
            vec![(HEADER_ACTOR_UID.to_owned(), "5".to_owned())]
        );
        assert_eq!(
            transport_headers(&Actor::default()),
            vec![(HEADER_ACTOR_UID.to_owned(), ACTOR_NONE.to_owned())]
        );
        assert_eq!(
            transport_headers(&Actor::internal()),
            vec![(HEADER_ACTOR_UID.to_owned(), ACTOR_INTERNAL.to_owned())]
        );
        assert_eq!(
            transport_headers(&Actor {
                anonymous_uid: "a".to_owned(),
                ..Actor::from_user(5)
            }),
            vec![
                (HEADER_ACTOR_ANONYMOUS_UID.to_owned(), "a".to_owned()),
                (HEADER_ACTOR_UID.to_owned(), "5".to_owned()),
            ]
        );
    }

    #[test]
    fn trusted_predicate() {
        let security = SecurityConfig {
            external_url: "https://sourcegraph.example".to_owned(),
            cors_origin: "https://trusted.example".to_owned(),
        };
        assert!(is_trusted_request(
            &request(&[("x-requested-with", "anything")]),
            &security
        ));
        assert!(is_trusted_request(
            &request(&[("origin", "https://trusted.example")]),
            &security
        ));
        assert!(is_trusted_request(
            &request(&[("origin", "https://sourcegraph.example")]),
            &security
        ));
        assert!(!is_trusted_request(
            &request(&[("origin", "https://evil.example")]),
            &security
        ));
        assert!(!is_trusted_request(&request(&[]), &security));
    }

    #[test]
    fn cookie_parsing_matches_net_http() {
        let headers = request(&[("cookie", "a=b; sgs=quoted; c=d")]);
        assert_eq!(session_cookie(&headers).as_deref(), Some("quoted"));
        let headers = request(&[("cookie", "sgs=\"quoted\"")]);
        assert_eq!(session_cookie(&headers).as_deref(), Some("quoted"));
        let headers = request(&[("cookie", "sourcegraphAnonymousUid=anon")]);
        assert_eq!(cookie_anonymous_uid(&headers).as_deref(), Some("anon"));
    }

    #[test]
    fn basic_auth_username_parsing() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("token:secret");
        let headers = request(&[("authorization", &format!("Basic {encoded}"))]);
        assert_eq!(basic_auth_username(&headers).as_deref(), Some("token"));
        assert_eq!(basic_auth_username(&request(&[])), None);
    }

    struct FakeAuth {
        lookup: Result<i32, AccessTokenLookupError>,
        soap: Result<usize, String>,
        subject_admin: Result<bool, String>,
        user: AccessTokenUserLookup,
    }

    #[async_trait]
    impl AccessTokenAuth for FakeAuth {
        async fn lookup(
            &self,
            _token: &str,
            _required_scope: &str,
            _only_admin: bool,
        ) -> Result<i32, AccessTokenLookupError> {
            self.lookup.clone()
        }
        async fn count_soap_accounts(&self, _uid: i32) -> Result<usize, String> {
            self.soap.clone()
        }
        async fn is_subject_site_admin(&self, _subject_uid: i32) -> Result<bool, String> {
            self.subject_admin.clone()
        }
        async fn get_user_by_username(&self, _username: &str) -> AccessTokenUserLookup {
            self.user.clone()
        }
        async fn log_security_event(&self, _name: &str) {}
    }

    fn config(allow: AccessTokensAllow) -> AccessTokenConfig {
        AccessTokenConfig {
            allow,
            dotcom: false,
        }
    }

    fn ok_auth() -> FakeAuth {
        FakeAuth {
            lookup: Ok(123),
            soap: Ok(0),
            subject_admin: Ok(true),
            user: AccessTokenUserLookup::Found(AccessTokenUser {
                uid: 9,
                username: "alice".to_owned(),
            }),
        }
    }

    #[tokio::test]
    async fn access_token_no_token_keeps_the_actor() {
        let decision = apply_access_token(
            AccessTokenInput::default(),
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::from_user(7),
            &ok_auth(),
        )
        .await;
        assert_eq!(
            decision,
            AccessTokenDecision::Proceed {
                actor: Actor::from_user(7),
                vary: true
            }
        );
    }

    #[tokio::test]
    async fn access_token_valid_and_sudo() {
        let decision = apply_access_token(
            AccessTokenInput {
                authorization: Some("token abc"),
                ..Default::default()
            },
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::default(),
            &ok_auth(),
        )
        .await;
        assert_eq!(
            decision,
            AccessTokenDecision::Proceed {
                actor: Actor::from_user(123),
                vary: true
            }
        );

        let decision = apply_access_token(
            AccessTokenInput {
                authorization: Some("token-sudo token=\"abc\",user=\"alice\""),
                ..Default::default()
            },
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::default(),
            &ok_auth(),
        )
        .await;
        assert_eq!(
            decision,
            AccessTokenDecision::Proceed {
                actor: Actor::from_user(9),
                vary: true
            }
        );
    }

    #[tokio::test]
    async fn access_token_error_paths() {
        let invalid = FakeAuth {
            lookup: Err(AccessTokenLookupError::Invalid),
            ..ok_auth()
        };
        let decision = apply_access_token(
            AccessTokenInput {
                authorization: Some("token abc"),
                ..Default::default()
            },
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::default(),
            &invalid,
        )
        .await;
        match decision {
            AccessTokenDecision::Respond(response) => {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED)
            }
            _ => panic!("expected a rejection"),
        }

        let decision = apply_access_token(
            AccessTokenInput {
                authorization: Some("token abc"),
                ..Default::default()
            },
            config(AccessTokensAllow::None),
            LicenseForToken::Ok { expired: false },
            Actor::default(),
            &ok_auth(),
        )
        .await;
        match decision {
            AccessTokenDecision::Respond(response) => {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED)
            }
            _ => panic!("expected a rejection"),
        }
    }

    #[tokio::test]
    async fn access_token_bypass_and_unrecognized() {
        let bypass = apply_access_token(
            AccessTokenInput {
                path: "/.api/scim/v2/Users",
                ..Default::default()
            },
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::from_user(1),
            &ok_auth(),
        )
        .await;
        assert_eq!(
            bypass,
            AccessTokenDecision::Proceed {
                actor: Actor::from_user(1),
                vary: false
            }
        );

        let unrecognized = apply_access_token(
            AccessTokenInput {
                authorization: Some("Basic abcd"),
                ..Default::default()
            },
            config(AccessTokensAllow::All),
            LicenseForToken::Ok { expired: false },
            Actor::from_user(1),
            &ok_auth(),
        )
        .await;
        assert_eq!(
            unrecognized,
            AccessTokenDecision::Proceed {
                actor: Actor::from_user(1),
                vary: true
            }
        );
    }
}
