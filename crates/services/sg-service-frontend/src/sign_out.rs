#![forbid(unsafe_code)]

//! `/-/sign-out` (`cmd/frontend/internal/app/sign_out.go`).
//!
//! The baseline handler:
//!
//! 1. `recordSecurityEvent(SignOutAttempted)`;
//! 2. `session.InvalidateSessionCurrentUser` → `Users().InvalidateSessionsByID`
//!    then `deleteSession` (Redis `DEL` + expired `sgs` cookie);
//! 3. `session.SetActor(nil)` → stores the JSON literal `null` under `actor`
//!    (on the cached, already-expired session this is another `DEL`, otherwise
//!    a fresh session / `SETEX` with a new ID);
//! 4. `session.SetSignOutCookie` (`sg-signout=true; Path=/; Secure`);
//! 5. `ssoSignOutHandler`: `openidconnect.SignOut` clears the `oidc@0` session
//!    value and (when it held data) revokes the provider token; SAML builds a
//!    logout redirect; the Sourcegraph Operator provider (`soap@0`) goes
//!    through the same OpenID Connect path;
//! 6. `recordSecurityEvent(SignOutSucceeded)` when the *last* assignment to
//!    `err` (`SetActor`) succeeded, even if invalidation or SSO failed;
//! 7. `http.Redirect(…, "/search", 303)`.
//!
//! Only step 5's session-value clearing and the flow around it are portable.
//! Token revocation, the SAML `LogoutRequest` redirect and Sourcegraph
//! Accounts (`SAMS`) are seams in [`SsoRemoteSignOut`]; see the errata in the
//! slice overview.
//!
//! The handler is HTTP-framework independent: [`serve_sign_out`] consumes a
//! [`SignOutRequest`] and returns a [`SignOutResponse`] (status, redirect
//! location, body and `Set-Cookie` header values). Wiring it into the router
//! is left to the parent slice, which owns the request/response adapters.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use sg_config::SiteConfiguration;
use sg_db::security_event_logs::{
    insert_security_event, security_event_logs_in_database, SecurityEvent,
};
use sg_db::telemetry_events::{bulk_insert_event_logs, EventLogRow};
use sg_session::write::{SessionManager, SetCookie, WriteOptions};
use sg_session::{GoTime, OsSessionIdSource, SessionIdSource, SessionStore};

use crate::log_events::sanitize_event_url;
use crate::session_auth::RedisSessionStore;

/// `router.SignOut` (`cmd/frontend/internal/app/router/router.go`).
pub const SIGN_OUT_PATH: &str = "/-/sign-out";

/// `http.Redirect`'s target.
pub const SIGN_OUT_REDIRECT_LOCATION: &str = "/search";
/// `http.Redirect`'s content type.
pub const SIGN_OUT_CONTENT_TYPE: &str = "text/html; charset=utf-8";
/// `http.Redirect`'s body for `303 See Other`.
pub const SIGN_OUT_BODY: &str = "<a href=\"/search\">See Other</a>.\n\n";
/// The redirect status.
pub const SIGN_OUT_STATUS: u16 = 303;

/// `database.SecurityEventNameSignOutAttempted`.
pub const SECURITY_EVENT_SIGN_OUT_ATTEMPTED: &str = "SignOutAttempted";
/// `database.SecurityEventNameSignOutFailed`.
pub const SECURITY_EVENT_SIGN_OUT_FAILED: &str = "SignOutFailed";
/// `database.SecurityEventNameSignOutSucceeded`.
pub const SECURITY_EVENT_SIGN_OUT_SUCCEEDED: &str = "SignOutSucceeded";

/// `openidconnect.SessionKey`.
pub const OIDC_SESSION_KEY: &str = "oidc@0";
/// `sourcegraphoperator.SessionKey`.
pub const SOAP_SESSION_KEY: &str = "soap@0";

const SOURCE: &str = "BACKEND";

/// One `security_event_logs` row, owned so it can cross an async trait
/// boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct SecurityEventRecord {
    pub name: String,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    pub argument: Value,
    pub timestamp: OffsetDateTime,
}

/// The database writes `serveSignOutHandler` performs. Recording errors are
/// ignored by the baseline (`LogSecurityEvent` logs and returns nil;
/// `EventLogs().Insert` is `_ =`-discarded), so the handler ignores them too.
#[async_trait]
pub trait SignOutDb: Send + Sync {
    /// `Users().InvalidateSessionsByID`.
    async fn invalidate_sessions(&self, uid: i32) -> Result<(), String>;
    /// `SecurityEventLogs().LogSecurityEvent`.
    async fn log_security_event(&self, event: SecurityEventRecord) -> Result<(), String>;
    /// `EventLogs().Insert` (legacy `event_logs` row).
    async fn insert_event_log(&self, event: EventLogRow) -> Result<(), String>;
}

/// The production [`SignOutDb`].
pub struct PgSignOutDb {
    pool: PgPool,
    version: String,
    security_event_logs_in_database: bool,
}

impl PgSignOutDb {
    pub fn new(
        pool: PgPool,
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
impl SignOutDb for PgSignOutDb {
    async fn invalidate_sessions(&self, uid: i32) -> Result<(), String> {
        sg_db::users::invalidate_sessions_by_ids(&self.pool, &[uid])
            .await
            .map_err(|error| error.to_string())
    }

    async fn log_security_event(&self, event: SecurityEventRecord) -> Result<(), String> {
        // `audit.SecurityEventLocation` defaults to the audit log; only
        // `database`/`all` write the table.
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

    async fn insert_event_log(&self, event: EventLogRow) -> Result<(), String> {
        bulk_insert_event_logs(&self.pool, std::slice::from_ref(&event))
            .await
            .map_err(|error| error.to_string())
    }
}

/// An authentication provider whose sign-out path the baseline walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsoProviderKind {
    OpenIdConnect,
    Saml,
    SourcegraphOperator,
}

/// One `conf.Get().AuthProviders` entry (plus the cloud-only Sourcegraph
/// Operator provider) reduced to what sign-out needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsoPlan {
    pub kind: SsoProviderKind,
    /// The session key `openidconnect.SignOut` clears; SAML has none.
    pub session_key: Option<&'static str>,
}

/// The provider plans `ssoSignOutHandler` iterates, in baseline order: every
/// `auth.providers` entry, then the Sourcegraph Operator provider when the
/// signed cloud site configuration enables it.
pub fn sso_plans(site: &SiteConfiguration) -> Vec<SsoPlan> {
    let mut plans = Vec::new();
    if let Some(providers) = site.raw().get("auth.providers").and_then(Value::as_array) {
        for provider in providers {
            if provider
                .get("openidconnect")
                .is_some_and(|value| !value.is_null())
            {
                plans.push(SsoPlan {
                    kind: SsoProviderKind::OpenIdConnect,
                    session_key: Some(OIDC_SESSION_KEY),
                });
            }
            if provider.get("saml").is_some_and(|value| !value.is_null()) {
                plans.push(SsoPlan {
                    kind: SsoProviderKind::Saml,
                    session_key: None,
                });
            }
        }
    }

    // `sourcegraphoperator.GetOIDCProvider` returns a provider only when
    // `sourcegraphoperator.Init` registered one, which requires the signed
    // cloud site config to enable it.
    let operator_enabled = sg_config::cloud::site_config()
        .is_ok_and(|cloud| cloud.sourcegraph_operator_auth_provider_enabled());
    if operator_enabled {
        plans.push(SsoPlan {
            kind: SsoProviderKind::SourcegraphOperator,
            session_key: Some(SOAP_SESSION_KEY),
        });
    }
    plans
}

/// The remote side of SSO sign-out: OpenID Connect token revocation, the SAMS
/// session revocation, and the SAML `LogoutRequest` redirect.
///
/// The Rust frontend does not port those HTTP clients yet; [`UnportedSsoRemote`]
/// returns `Ok(())`, which keeps the session-value clearing (the observable
/// part of `openidconnect.SignOut` in the baseline's Redis/cookie state)
/// identical. See the errata in the slice overview.
#[async_trait]
pub trait SsoRemoteSignOut: Send + Sync {
    /// `openidconnect.SignOut`'s revocation step for a provider whose session
    /// data was `Some`, or `saml.SignOut` when the plan has no session key.
    async fn sign_out(
        &self,
        kind: SsoProviderKind,
        session_data: Option<Value>,
    ) -> Result<(), String>;
}

/// No-op remote sign-out (see the trait docs).
pub struct UnportedSsoRemote;

#[async_trait]
impl SsoRemoteSignOut for UnportedSsoRemote {
    async fn sign_out(
        &self,
        _kind: SsoProviderKind,
        _session_data: Option<Value>,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// Everything the handler needs, injected so the differential harness can
/// supply recording fakes.
pub struct SignOutDeps {
    pub store: Arc<dyn SessionStore>,
    pub ids: Arc<dyn SessionIdSource>,
    /// `SRC_SESSION_COOKIE_KEY`.
    pub cookie_key: Vec<u8>,
    /// `conf.ExternalURLParsed().Scheme == "https"`.
    pub secure: bool,
    pub db: Arc<dyn SignOutDb>,
    pub sso_plans: Vec<SsoPlan>,
    pub sso_remote: Arc<dyn SsoRemoteSignOut>,
    /// `conf.ExternalURL()`, for `SanitizeEventURL`.
    pub external_url: String,
}

impl SignOutDeps {
    /// The production wiring: the Redis session store, `SRC_SESSION_COOKIE_KEY`,
    /// the site configuration's providers and `securityEventLog` location.
    pub fn production(pool: PgPool, site: &SiteConfiguration, external_url: &str) -> Self {
        let cookie_key = std::env::var("SRC_SESSION_COOKIE_KEY")
            .unwrap_or_default()
            .into_bytes();
        let secure = url::Url::parse(external_url)
            .map(|url| url.scheme() == "https")
            .unwrap_or(false);
        Self {
            store: Arc::new(RedisSessionStore::from_env()),
            ids: Arc::new(OsSessionIdSource),
            cookie_key,
            secure,
            db: Arc::new(PgSignOutDb::new(
                pool,
                sg_metrics::VERSION,
                security_event_logs_in_database(site.raw()),
            )),
            sso_plans: sso_plans(site),
            sso_remote: Arc::new(UnportedSsoRemote),
            external_url: external_url.to_owned(),
        }
    }
}

/// The request facts the handler reads. The HTTP layer resolves the actor,
/// the cookies and the URL; `now` is `time.Now()`.
pub struct SignOutRequest<'a> {
    /// `actor.FromContext(ctx).UID` (0 for anonymous).
    pub actor_uid: i32,
    /// `r.URL.Path`.
    pub path: &'a str,
    /// `r.URL.Host` (the legacy event's unsanitized URL).
    pub host: &'a str,
    /// `cookie.AnonymousUID(r)`.
    pub anonymous_uid: Option<&'a str>,
    /// The raw `sgs` cookie value, if present.
    pub session_cookie: Option<&'a str>,
    /// Whether `sg-signout` is present.
    pub sign_out_cookie: bool,
    pub now: OffsetDateTime,
}

/// The framework-independent response: `http.Redirect` plus the `Set-Cookie`
/// header values written before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignOutResponse {
    pub status: u16,
    pub location: String,
    pub content_type: String,
    pub body: Vec<u8>,
    /// `Set-Cookie` header values, in write order.
    pub set_cookies: Vec<String>,
}

impl SignOutResponse {
    fn redirect(cookies: &[SetCookie]) -> Self {
        Self {
            status: SIGN_OUT_STATUS,
            location: SIGN_OUT_REDIRECT_LOCATION.to_owned(),
            content_type: SIGN_OUT_CONTENT_TYPE.to_owned(),
            body: SIGN_OUT_BODY.as_bytes().to_vec(),
            set_cookies: cookies.iter().map(SetCookie::to_header).collect(),
        }
    }
}

/// `serveSignOutHandler`.
pub async fn serve_sign_out(deps: &SignOutDeps, req: &SignOutRequest<'_>) -> SignOutResponse {
    let mut manager = SessionManager::new(
        deps.store.as_ref(),
        deps.ids.as_ref(),
        &deps.cookie_key,
        WriteOptions::new(deps.secure),
        req.now,
        req.session_cookie,
        req.sign_out_cookie,
    );

    record_security_event(deps, req, SECURITY_EVENT_SIGN_OUT_ATTEMPTED, None).await;

    // Invalidate all user sessions first so later failures cannot leave a
    // valid session behind. `InvalidateSessionCurrentUser` returns on an
    // invalidation error and only then calls `deleteSession`.
    match deps.db.invalidate_sessions(req.actor_uid).await {
        Ok(()) => {
            if let Err(error) = manager.delete_session().await {
                record_security_event(deps, req, SECURITY_EVENT_SIGN_OUT_FAILED, Some(&error))
                    .await;
                tracing::error!(error = %error, "serveSignOutHandler");
            }
        }
        Err(error) => {
            record_security_event(deps, req, SECURITY_EVENT_SIGN_OUT_FAILED, Some(&error)).await;
            // telemetry recorder `signOut` / Failed: no Rust recorder yet.
            tracing::error!(error = %error, "serveSignOutHandler");
        }
    }

    // The baseline's final `err` is only the `SetActor` result: a successful
    // SetActor after a failed invalidation still records SignOutSucceeded.
    let last_error = match manager.set_actor(None, 0, None, GoTime(req.now)).await {
        Ok(()) => None,
        Err(error) => {
            record_security_event(deps, req, SECURITY_EVENT_SIGN_OUT_FAILED, Some(&error)).await;
            tracing::error!(error = %error, "serveSignOutHandler");
            Some(error)
        }
    };

    manager.set_sign_out_cookie();

    sso_sign_out(&mut manager, &deps.sso_plans, deps.sso_remote.as_ref()).await;

    if last_error.is_none() {
        record_security_event(deps, req, SECURITY_EVENT_SIGN_OUT_SUCCEEDED, None).await;
        // telemetry recorder `signOut` / Succeeded: no Rust recorder yet.
    }

    SignOutResponse::redirect(&manager.cookies)
}

/// `ssoSignOutHandler`. Errors are logged (and, for OpenID Connect, trigger
/// the deferred `SetData(nil)` retry) but never affect the redirect.
async fn sso_sign_out(
    manager: &mut SessionManager<'_>,
    plans: &[SsoPlan],
    remote: &dyn SsoRemoteSignOut,
) {
    for plan in plans {
        match plan.session_key {
            Some(key) => clear_oidc_session(manager, key, plan.kind, remote).await,
            None => {
                // `saml.SignOut` only builds a redirect URL; it touches no
                // session state. The redirect itself is the unported seam.
                let _ = remote.sign_out(plan.kind, None).await;
            }
        }
    }
}

/// `openidconnect.SignOut` without its HTTP token-revocation side effects:
/// read the provider's session data, clear it, and hand non-nil data to the
/// remote seam. The deferred `SetData(nil)` runs on every error path.
async fn clear_oidc_session(
    manager: &mut SessionManager<'_>,
    key: &str,
    kind: SsoProviderKind,
    remote: &dyn SsoRemoteSignOut,
) {
    let mut failed = false;
    let mut data = None;
    match manager.get_data(key).await {
        Err(error) => {
            tracing::error!(error = %error, "failed to clear auth provider session data");
            failed = true;
        }
        Ok(Some(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) if !value.is_null() => data = Some(value),
            // `"null"` (a previous sign-out) decodes to a nil pointer: no
            // remote call, but the value is still re-cleared below.
            Ok(_) => {}
            Err(error) => {
                tracing::error!(error = %error, "reading OpenID Connect session data");
                failed = true;
            }
        },
        Ok(None) => {}
    }

    if !failed && manager.set_data(key, b"null").await.is_err() {
        failed = true;
    }
    if failed {
        let _ = manager.set_data(key, b"null").await;
        return;
    }

    if let Some(data) = data {
        if let Err(error) = remote.sign_out(kind, Some(data)).await {
            tracing::error!(error = %error, "failed to clear auth provider session data");
            // The baseline's deferred `SetData(nil)` runs after the error.
            let _ = manager.set_data(key, b"null").await;
        }
    }
}

/// `recordSecurityEvent`: one `security_event_logs` row (`r.URL.Path`) and
/// one legacy `event_logs` row (`r.URL.Host`, sanitized by the DB layer),
/// both with `{"error": "..."}`.
async fn record_security_event(
    deps: &SignOutDeps,
    req: &SignOutRequest<'_>,
    name: &str,
    error: Option<&str>,
) {
    let argument = json!({ "error": error.unwrap_or("") });
    let anonymous_user_id = req.anonymous_uid.unwrap_or("");

    let _ = deps
        .db
        .log_security_event(SecurityEventRecord {
            name: name.to_owned(),
            url: req.path.to_owned(),
            user_id: req.actor_uid,
            anonymous_user_id: anonymous_user_id.to_owned(),
            source: SOURCE.to_owned(),
            argument: argument.clone(),
            timestamp: OffsetDateTime::now_utc(),
        })
        .await;

    let _ = deps
        .db
        .insert_event_log(EventLogRow {
            name: name.to_owned(),
            url: sanitize_event_url(req.host, &deps.external_url),
            user_id: req.actor_uid,
            anonymous_user_id: "backend".to_owned(),
            source: SOURCE.to_owned(),
            argument,
            public_argument: json!({}),
            version: sg_metrics::VERSION.to_owned(),
            timestamp: OffsetDateTime::now_utc(),
            feature_flags: Value::Null,
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: Uuid::new_v4().to_string(),
            insert_id: Uuid::new_v4().to_string(),
            client: None,
            billing_product_category: None,
            billing_event_id: None,
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn provider_plans_match_the_baseline_loop() {
        assert!(sso_plans(&site("{}")).is_empty());
        assert_eq!(
            sso_plans(&site(
                r#"{"auth.providers":[{"openidconnect":{}},{"saml":{}}]}"#
            )),
            vec![
                SsoPlan {
                    kind: SsoProviderKind::OpenIdConnect,
                    session_key: Some(OIDC_SESSION_KEY),
                },
                SsoPlan {
                    kind: SsoProviderKind::Saml,
                    session_key: None,
                },
            ]
        );
        // A JSON `null` provider entry is a nil Go pointer.
        assert!(sso_plans(&site(r#"{"auth.providers":[{"openidconnect":null}]}"#)).is_empty());
    }

    #[test]
    fn response_matches_http_redirect() {
        let response = SignOutResponse::redirect(&[]);
        assert_eq!(response.status, 303);
        assert_eq!(response.location, "/search");
        assert_eq!(response.content_type, "text/html; charset=utf-8");
        assert_eq!(
            String::from_utf8(response.body).unwrap(),
            "<a href=\"/search\">See Other</a>.\n\n"
        );
    }
}
