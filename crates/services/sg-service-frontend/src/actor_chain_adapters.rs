#![forbid(unsafe_code)]

//! Production adapters for [`crate::actor_chain`]: the Postgres access-token
//! lookup, the site-admin checks, the security-event write and the license
//! reader.
//!
//! The license reader is intentionally a thin seam: M9.8je replaces it with
//! the full licensing implementation. This slice reads
//! `siteConfiguration.licenseKey`, verifies it with `sg-licensing` and reduces
//! it to the `expired` bit the middlewares read; an empty key is the baseline's
//! non-expired free tier.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use sg_config::LiveConfig;
use sg_db::users::SOURCEGRAPH_OPERATOR_PROVIDER_TYPE;

use crate::actor_chain::{LicenseReader, SiteAdminChecker};
use crate::actor_middleware::{
    AccessTokenAuditContext, AccessTokenAuth, AccessTokenLookupError, AccessTokenSecurityEvent,
    AccessTokenUser, AccessTokenUserLookup, Actor, SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED,
    SECURITY_EVENT_ACCESS_TOKEN_INVALID, SECURITY_EVENT_ACCESS_TOKEN_SUBJECT_NOT_SITE_ADMIN,
};
use crate::audit::{format_go_time, log_level_from_env, AuditActor};
use crate::authz::{LicenseState, SiteAdminCheck};
use crate::security_event_logs::{
    prepare_security_events, write_prepared_security_events, SecurityEventInput,
    SecurityEventPolicy,
};

/// `database.MaxAccessTokenLastUsedAtAge`.
const MAX_ACCESS_TOKEN_LAST_USED_AT_AGE: Duration = Duration::minutes(5);

// ---------------------------------------------------------------------------
// Token parsing and hashing (lib/accesstoken, internal/hashutil)
// ---------------------------------------------------------------------------

fn token_regex() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new("^(?:sgp_|sgph_)?(?:[a-fA-F0-9]{16}_|local_)?([a-fA-F0-9]{40})$")
            .expect("access token regex")
    })
}

fn hex_decode_20(value: &str) -> Option<[u8; 20]> {
    if value.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (chunk[0] as char).to_digit(16)?;
        let low = (chunk[1] as char).to_digit(16)?;
        out[index] = ((high << 4) | low) as u8;
    }
    Some(out)
}

/// `tokenSHA256Hash`: parse the personal-access-token prefix, hex-decode the
/// 20-byte value and SHA-256 it.
pub fn token_sha256_hash(token: &str) -> Result<[u8; 32], String> {
    let captures = token_regex()
        .captures(token)
        .ok_or_else(|| "invalid token format".to_owned())?;
    let value = captures.get(1).expect("capture group").as_str();
    let decoded = hex_decode_20(value).ok_or_else(|| "invalid token".to_owned())?;
    Ok(Sha256::digest(decoded).into())
}

// ---------------------------------------------------------------------------
// Access tokens
// ---------------------------------------------------------------------------

/// `db.AccessTokens`/`db.Users`/`db.UserExternalAccounts` behind
/// [`AccessTokenAuth`]. Build the per-request wrapper with [`Self::request`]
/// so security events carry the request URI and anonymous UID.
pub struct PgAccessTokenAuth {
    pool: PgPool,
    live: Arc<LiveConfig>,
}

impl PgAccessTokenAuth {
    pub fn new(pool: PgPool, live: Arc<LiveConfig>) -> Self {
        Self { pool, live }
    }

    pub fn request<'a>(
        &'a self,
        url: &'a str,
        anonymous_uid: Option<&'a str>,
        audit: Option<AccessTokenAuditContext<'a>>,
    ) -> RequestAccessTokenAuth<'a> {
        RequestAccessTokenAuth {
            db: self,
            url,
            anonymous_uid,
            audit,
            version: version_string(),
        }
    }
}

#[async_trait]
impl AccessTokenAuth for PgAccessTokenAuth {
    async fn lookup(
        &self,
        token: &str,
        required_scope: &str,
        only_admin: bool,
    ) -> Result<i32, AccessTokenLookupError> {
        let hash = token_sha256_hash(token).map_err(|_| AccessTokenLookupError::Invalid)?;
        let found = sg_db::access_tokens::lookup_access_token(
            &self.pool,
            &hash,
            required_scope,
            only_admin,
        )
        .await
        .map_err(|error| AccessTokenLookupError::Other(error.to_string()))?
        .ok_or(AccessTokenLookupError::NotFound)?;

        let now = OffsetDateTime::now_utc();
        let stale = found
            .last_used_at
            .is_none_or(|last_used| now - last_used > MAX_ACCESS_TOKEN_LAST_USED_AT_AGE);
        if stale {
            if let Err(error) =
                sg_db::access_tokens::update_access_token_last_used(&self.pool, found.id).await
            {
                // The baseline logs and ignores this failure.
                tracing::warn!(error = %error, "failed to update access token last_used_at");
            }
        }
        Ok(found.subject_user_id)
    }

    async fn count_soap_accounts(&self, uid: i32) -> Result<usize, String> {
        sg_db::user_external_accounts::count_for_user_service(
            &self.pool,
            uid,
            SOURCEGRAPH_OPERATOR_PROVIDER_TYPE,
        )
        .await
        .map(|count| count.max(0) as usize)
        .map_err(|error| error.to_string())
    }

    async fn is_subject_site_admin(&self, subject_uid: i32) -> Result<bool, String> {
        sg_db::users::get_site_admin(&self.pool, subject_uid)
            .await
            .map(|site_admin| site_admin.unwrap_or(false))
            .map_err(|error| error.to_string())
    }

    async fn get_user_by_username(&self, username: &str) -> AccessTokenUserLookup {
        match sg_db::users::lookup_by_username(&self.pool, username).await {
            Ok(Some((uid, username))) => {
                AccessTokenUserLookup::Found(AccessTokenUser { uid, username })
            }
            Ok(None) => AccessTokenUserLookup::NotFound,
            Err(error) => AccessTokenUserLookup::Error(error.to_string()),
        }
    }

    async fn get_user_by_id(&self, uid: i32) -> Option<String> {
        // The baseline ignores a failed lookup and leaves the audit name empty.
        sg_db::users::get_username_by_id(&self.pool, uid)
            .await
            .ok()
            .flatten()
    }

    async fn log_security_event(&self, _name: &str) {
        // The per-request wrapper overrides `security_event` with the audit
        // arguments; this name-only method has nothing to persist.
    }

    fn request_auth<'a>(
        &'a self,
        url: &'a str,
        anonymous_uid: Option<&'a str>,
        audit: Option<AccessTokenAuditContext<'a>>,
    ) -> Option<Box<dyn AccessTokenAuth + 'a>> {
        Some(Box::new(self.request(url, anonymous_uid, audit)))
    }
}

/// A per-request view of [`PgAccessTokenAuth`] that adds the request URI and
/// anonymous UID to `security_event_logs` rows.
pub struct RequestAccessTokenAuth<'a> {
    db: &'a PgAccessTokenAuth,
    url: &'a str,
    anonymous_uid: Option<&'a str>,
    audit: Option<AccessTokenAuditContext<'a>>,
    version: &'a str,
}

#[async_trait]
impl AccessTokenAuth for RequestAccessTokenAuth<'_> {
    async fn lookup(
        &self,
        token: &str,
        required_scope: &str,
        only_admin: bool,
    ) -> Result<i32, AccessTokenLookupError> {
        self.db.lookup(token, required_scope, only_admin).await
    }

    async fn count_soap_accounts(&self, uid: i32) -> Result<usize, String> {
        self.db.count_soap_accounts(uid).await
    }

    async fn is_subject_site_admin(&self, subject_uid: i32) -> Result<bool, String> {
        self.db.is_subject_site_admin(subject_uid).await
    }

    async fn get_user_by_username(&self, username: &str) -> AccessTokenUserLookup {
        self.db.get_user_by_username(username).await
    }

    async fn get_user_by_id(&self, uid: i32) -> Option<String> {
        self.db.get_user_by_id(uid).await
    }

    async fn log_security_event(&self, name: &str) {
        let event = AccessTokenSecurityEvent {
            name,
            user_id: 0,
            subject_user_id: 0,
            sudo_user_id: 0,
            sudo_user: "",
            token_subject_user_id: 0,
            token_subject_user_name: "",
            sourcegraph_operator: false,
        };
        self.security_event(event).await;
    }

    /// `SecurityEventLogs().LogSecurityEvent` with the baseline's arguments
    /// and both downstream sinks (`database`/`all` and `auditlog`/`all`).
    async fn security_event(&self, event: AccessTokenSecurityEvent<'_>) {
        let snapshot = self.db.live.current();
        let policy = SecurityEventPolicy::from_site(&snapshot.site);

        let timestamp = OffsetDateTime::now_utc();
        let anonymous_user_id = if event.name == SECURITY_EVENT_ACCESS_TOKEN_INVALID {
            self.anonymous_uid
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    // The baseline has no reliable identifier at this point.
                    format!("unknown user @ {}", format_go_time(timestamp))
                })
        } else {
            String::new()
        };
        let input = SecurityEventInput {
            name: event.name.to_owned(),
            url: self.url.to_owned(),
            user_id: event.user_id,
            anonymous_user_id,
            source: "BACKEND".to_owned(),
            argument: security_event_argument(&event),
            timestamp,
        };

        // `AccessTokenImpersonated` logs on a context whose actor is the
        // token subject (and carries `SourcegraphOperator`); the earlier
        // failures log on the pre-token, zero actor.
        let (actor, sourcegraph_operator) =
            if event.name == SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED {
                (
                    AuditActor {
                        uid: (event.subject_user_id > 0).then_some(event.subject_user_id),
                        anonymous_uid: String::new(),
                        internal: false,
                    },
                    event.sourcegraph_operator,
                )
            } else {
                (
                    self.audit
                        .map(|audit| audit.actor.clone())
                        .unwrap_or_default(),
                    false,
                )
            };
        let client = self.audit.and_then(|audit| audit.client);

        let prepared = match prepare_security_events(
            &snapshot.site,
            policy,
            &actor,
            sourcegraph_operator,
            client,
            &[input],
            self.version,
            || uuid::Uuid::new_v4().to_string(),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                // `LogEvent` logs the error and does not return it.
                tracing::error!(error = %error, "failed to write access-token security event");
                return;
            }
        };

        if let Err(error) = write_prepared_security_events(&self.db.pool, &prepared).await {
            tracing::error!(error = %error, "failed to write access-token security event");
        }
        prepared.emit_audits(&log_level_from_env());
    }
}

fn security_event_argument(event: &AccessTokenSecurityEvent<'_>) -> Option<serde_json::Value> {
    match event.name {
        SECURITY_EVENT_ACCESS_TOKEN_SUBJECT_NOT_SITE_ADMIN => {
            Some(json!({ "subject_user_id": event.subject_user_id }))
        }
        SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED => Some(json!({
            "sudo_user": event.sudo_user,
            "sudo_user_id": event.sudo_user_id,
            "token_subject_user_id": event.token_subject_user_id,
            "token_subject_user_name": event.token_subject_user_name,
        })),
        // `AccessTokenInvalid` passes nil arguments (`{}` after marshalling).
        _ => None,
    }
}

/// `version.Version()` cached once per process (it reads `/version.txt`).
pub fn version_string() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| {
        String::from_utf8_lossy(crate::version::Version::load().as_bytes()).into_owned()
    })
}

// ---------------------------------------------------------------------------
// License reader (M9.8je replaces this seam)
// ---------------------------------------------------------------------------

/// `licensing.GetConfiguredProductLicenseInfo` for the access-token and
/// PostAuth gates.
pub struct LiveLicenseReader {
    live: Arc<LiveConfig>,
    cache: Mutex<Option<(String, LicenseState)>>,
}

impl LiveLicenseReader {
    pub fn new(live: Arc<LiveConfig>) -> Self {
        Self {
            live,
            cache: Mutex::new(None),
        }
    }
}

#[async_trait]
impl LicenseReader for LiveLicenseReader {
    async fn product_license(&self) -> LicenseState {
        let key = self.live.current().site.license_key();
        if key.is_empty() {
            // The baseline's free-tier fallback is never expired.
            return LicenseState::Valid;
        }
        if let Ok(cache) = self.cache.lock() {
            if let Some((cached_key, state)) = cache.as_ref() {
                if *cached_key == key {
                    return *state;
                }
            }
        }
        let state = match sg_licensing::configured_license_info(&key) {
            Err(_) => LicenseState::Error,
            Ok(info) => match info.expires_at.as_deref() {
                Some(text) => match OffsetDateTime::parse(text, &Rfc3339) {
                    Ok(expires_at) if expires_at < OffsetDateTime::now_utc() => {
                        LicenseState::Expired
                    }
                    Ok(_) => LicenseState::Valid,
                    Err(_) => LicenseState::Error,
                },
                // `license.Info.ExpiresAt`'s zero value is in the past.
                None => LicenseState::Expired,
            },
        };
        if state != LicenseState::Error {
            if let Ok(mut cache) = self.cache.lock() {
                *cache = Some((key, state));
            }
        }
        state
    }
}

// ---------------------------------------------------------------------------
// Site admin
// ---------------------------------------------------------------------------

/// `db.Users().GetByID(...).SiteAdmin`.
pub struct PgSiteAdminChecker {
    pool: PgPool,
}

impl PgSiteAdminChecker {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SiteAdminChecker for PgSiteAdminChecker {
    async fn current_user(&self, actor: &Actor) -> SiteAdminCheck {
        if actor.is_internal() {
            return SiteAdminCheck::IsSiteAdmin;
        }
        if !actor.is_authenticated() {
            // `CheckCurrentUserIsSiteAdmin` → ErrNotAuthenticated.
            return SiteAdminCheck::Error;
        }
        match sg_db::users::get_site_admin(&self.pool, actor.uid).await {
            Ok(Some(true)) => SiteAdminCheck::IsSiteAdmin,
            Ok(Some(false)) => SiteAdminCheck::MustBeSiteAdmin,
            // Not found is ErrNotAuthenticated; other rows are database errors.
            Ok(None) | Err(_) => SiteAdminCheck::Error,
        }
    }

    async fn subject_is_site_admin(&self, uid: i32) -> Result<bool, String> {
        sg_db::users::get_site_admin(&self.pool, uid)
            .await
            .map(|site_admin| site_admin.unwrap_or(false))
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prefixed_tokens_like_the_baseline() {
        let raw = "a".repeat(40);
        let local = format!("local_{raw}");
        let sgp = format!("sgp_{local}");
        let instance = format!("sgp_{}_{raw}", "b".repeat(16));
        let expected = token_sha256_hash(&raw).unwrap();
        assert_eq!(token_sha256_hash(&local).unwrap(), expected);
        assert_eq!(token_sha256_hash(&sgp).unwrap(), expected);
        assert_eq!(token_sha256_hash(&instance).unwrap(), expected);
        assert!(token_sha256_hash("not-a-token").is_err());
        assert!(token_sha256_hash(&"a".repeat(39)).is_err());
    }

    #[test]
    fn sha256_matches_the_baseline_encoding() {
        // SHA-256 of 20 zero bytes.
        let hash = token_sha256_hash(&"0".repeat(40)).unwrap();
        assert_eq!(
            hash,
            [
                0xde, 0x47, 0xc9, 0xb2, 0x7e, 0xb8, 0xd3, 0x00, 0xdb, 0xb5, 0xf2, 0xc3, 0x53, 0xe6,
                0x32, 0xc3, 0x93, 0x26, 0x2c, 0xf0, 0x63, 0x40, 0xc4, 0xfa, 0x7f, 0x1b, 0x40, 0xc4,
                0xcb, 0xd3, 0x6f, 0x90,
            ]
        );
    }
}
