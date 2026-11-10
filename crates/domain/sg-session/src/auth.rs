//! `session.authenticateByCookie`'s read path.
//!
//! The baseline middleware (`session.go:370-503`) authenticates a request from
//! the `sgs` cookie only when the request is trusted (the caller decides
//! that). The checks, in order, are:
//!
//! 1. decode the cookie; a bad MAC, malformed value, decode max-age or gob
//!    error means "invalid cookie" and the request stays anonymous;
//! 2. load `session_<id>` from Redis; a Redis error is a 500 in the baseline
//!    and must *not* clear the cookie (an outage must not sign users out);
//! 3. read the `actor` JSON; no key or a `null` document means anonymous;
//! 4. `lastActive + expiryPeriod < now` → expired session;
//! 5. the user must still exist (`deleted_at IS NULL`); a database error is
//!    anonymous without deleting the session;
//! 6. the license check (`licensing.GetConfiguredProductLicenseInfo`) is not
//!    ported; OSS instances carry a non-expired free license in practice;
//! 7. `lastActive < user.invalidated_sessions_at` → invalidated session;
//! 8. a zero `userCreatedAt` is backfilled in the session (write path not
//!    ported) before comparing it with `users.created_at`; a mismatch
//!    invalidates the session.
//!
//! The baseline's writes on failure (delete the session + clear the cookie)
//! and the renewal when `lastActive` is older than five minutes are HTTP
//! concerns handled by `deleteSession`/`SetData`; this slice reports the
//! decision and leaves the cookie untouched.

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::securecookie;
use crate::session::{GoTime, COOKIE_NAME};

/// The session's Redis store (`redistore.RediStore.load`). Only the read path
/// is needed by authentication.
#[async_trait]
pub trait SessionStore: Send + Sync {
    /// `GET session_<id>`: `Ok(None)` when the key does not exist.
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError>;
}

/// A Redis failure. The baseline distinguishes `*net.OpError` (connection
/// refused) from other errors only to decide whether to log; both are a 500
/// and both leave the cookie in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStoreError(pub String);

impl std::fmt::Display for SessionStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SessionStoreError {}

/// The `users` fields `authenticateByCookie` reads (`db.Users().GetByID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionUser {
    pub id: i32,
    pub created_at: OffsetDateTime,
    pub invalidated_sessions_at: OffsetDateTime,
}

/// `db.Users().GetByID`: `Ok(None)` is the baseline's not-found error.
#[async_trait]
pub trait SessionUserLookup: Send + Sync {
    async fn get(&self, uid: i32) -> Result<Option<SessionUser>, String>;
}

/// Why a request stayed anonymous. Useful for tests and for the errata: the
/// baseline would clear the session cookie for every variant except
/// [`AnonymousReason::RedisUnavailable`] (a 500) and
/// [`AnonymousReason::UserLookupFailed`] (no delete, ephemeral DB error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnonymousReason {
    NoCookie,
    /// Bad MAC, malformed value, older than 30 days, or malformed gob.
    InvalidCookie,
    /// The Redis key is gone (e.g. expired server-side).
    NoSession,
    /// The session has no `actor` key, or it is `null`.
    NoActor,
    /// `lastActive + expiryPeriod < now`.
    SessionExpired,
    /// `GetByID` returned not-found (deleted user).
    UserNotFound,
    /// `GetByID` returned an error; the cookie is not cleared.
    UserLookupFailed,
    /// `lastActive < invalidated_sessions_at`.
    Invalidated,
    /// The session's `userCreatedAt` differs from `users.created_at`.
    CreatedAtMismatch,
}

/// The result of [`authenticate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAuthOutcome {
    /// The request is unauthenticated (the baseline may also clear the cookie;
    /// see [`AnonymousReason`]).
    Anonymous(AnonymousReason),
    /// `actor.WithActor(ctx, info.Actor)` with `FromSessionCookie = true`.
    Authenticated { uid: i32 },
    /// Redis failed: the baseline writes 500 `failed to read session actor`
    /// and does not clear the cookie.
    StoreUnavailable,
}

/// `authenticateByCookie` without its writes. `cookie` is the raw `sgs` value
/// (never a quoted string), or `None` when the request has no such cookie.
/// `now` is the baseline's `time.Now()`.
pub async fn authenticate(
    store: &dyn SessionStore,
    users: &dyn SessionUserLookup,
    cookie_key: &[u8],
    cookie: Option<&str>,
    now: OffsetDateTime,
) -> SessionAuthOutcome {
    let Some(cookie) = cookie else {
        return SessionAuthOutcome::Anonymous(AnonymousReason::NoCookie);
    };

    // `sessionStore.Get` decodes the securecookie value first.
    let now_seconds = now.unix_timestamp();
    let (timestamp, id) =
        match securecookie::decode_session_id(COOKIE_NAME, cookie_key, cookie, now_seconds) {
            Ok(decoded) => decoded,
            Err(_) => {
                return SessionAuthOutcome::Anonymous(AnonymousReason::InvalidCookie);
            }
        };

    // `GetData` → `store.Get` → `redistore.load`: a Redis error is fatal for
    // this request (500), a missing key is a fresh session (no actor).
    let payload = match store.load(&id).await {
        Ok(Some(payload)) => payload,
        Ok(None) => return SessionAuthOutcome::Anonymous(AnonymousReason::NoSession),
        Err(_) => return SessionAuthOutcome::StoreUnavailable,
    };
    let session = match crate::session::Session::from_redis(id, timestamp, &payload) {
        Ok(session) => session,
        Err(_) => return SessionAuthOutcome::Anonymous(AnonymousReason::InvalidCookie),
    };

    let Some(info) = (match session.actor_info() {
        Some(Ok(info)) => info,
        Some(Err(_)) => return SessionAuthOutcome::Anonymous(AnonymousReason::InvalidCookie),
        // `GetData` returns nil when the session has no `actor` key.
        None => None,
    }) else {
        return SessionAuthOutcome::Anonymous(AnonymousReason::NoActor);
    };

    // Check expiry: `info.LastActive.Add(info.ExpiryPeriod).Before(now)`.
    let last_active = info.last_active.0;
    let expires = last_active + time::Duration::nanoseconds(info.expiry_period);
    if expires < now {
        return SessionAuthOutcome::Anonymous(AnonymousReason::SessionExpired);
    }

    // Check that the user still exists.
    let Some(actor) = info.actor else {
        return SessionAuthOutcome::Anonymous(AnonymousReason::NoActor);
    };
    if !actor.is_authenticated() {
        return SessionAuthOutcome::Anonymous(AnonymousReason::NoActor);
    }
    let user = match users.get(actor.uid).await {
        Ok(Some(user)) => user,
        Ok(None) => return SessionAuthOutcome::Anonymous(AnonymousReason::UserNotFound),
        // The baseline does not delete the session for a database error.
        Err(_) => return SessionAuthOutcome::Anonymous(AnonymousReason::UserLookupFailed),
    };

    // Check that the session is still valid. The comparison is `Before`, so
    // an equal instant is still valid.
    if last_active < user.invalidated_sessions_at {
        return SessionAuthOutcome::Anonymous(AnonymousReason::Invalidated);
    }

    // A zero `userCreatedAt` is an old session from before the check existed;
    // the baseline backfills it from the database and continues. The write is
    // deferred, so only the comparison matters here.
    let created_at = info.user_created_at;
    if !created_at.is_zero() && !created_at.same_instant(&GoTime(user.created_at)) {
        return SessionAuthOutcome::Anonymous(AnonymousReason::CreatedAtMismatch);
    }

    SessionAuthOutcome::Authenticated { uid: actor.uid }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Actor, Session, SessionInfo};
    use time::format_description::well_known::Rfc3339;

    const ID: &str = "OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA";
    const KEY: &[u8] = b"test-key-123";

    fn at(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).unwrap()
    }

    struct Store {
        payload: Result<Option<Vec<u8>>, SessionStoreError>,
    }

    #[async_trait]
    impl SessionStore for Store {
        async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
            self.payload.clone()
        }
    }

    struct Users {
        user: Result<Option<SessionUser>, String>,
    }

    #[async_trait]
    impl SessionUserLookup for Users {
        async fn get(&self, _uid: i32) -> Result<Option<SessionUser>, String> {
            self.user.clone()
        }
    }

    fn user(created_at: &str, invalidated_at: &str) -> SessionUser {
        SessionUser {
            id: 1,
            created_at: at(created_at),
            invalidated_sessions_at: at(invalidated_at),
        }
    }

    fn info(last_active: &str, created_at: &str) -> SessionInfo {
        SessionInfo {
            actor: Some(Actor {
                uid: 1,
                ..Default::default()
            }),
            last_active: GoTime(at(last_active)),
            expiry_period: 90 * 24 * 3_600_000_000_000,
            user_created_at: GoTime(at(created_at)),
        }
    }

    fn payload(info: &SessionInfo) -> Vec<u8> {
        let json = serde_json::to_vec(info).unwrap();
        Session {
            timestamp: 0,
            id: ID.to_owned(),
            values: vec![("actor".to_owned(), json)],
        }
        .encode_redis()
    }

    fn raw_payload(value: &str) -> Vec<u8> {
        Session {
            timestamp: 0,
            id: ID.to_owned(),
            values: vec![("actor".to_owned(), value.as_bytes().to_vec())],
        }
        .encode_redis()
    }

    fn cookie(now: OffsetDateTime) -> String {
        securecookie::encode_session_id(COOKIE_NAME, KEY, now.unix_timestamp(), ID)
    }

    async fn run(
        now: OffsetDateTime,
        cookie: Option<&str>,
        payload: Result<Option<Vec<u8>>, SessionStoreError>,
        user: Result<Option<SessionUser>, String>,
    ) -> SessionAuthOutcome {
        authenticate(&Store { payload }, &Users { user }, KEY, cookie, now).await
    }

    #[tokio::test]
    async fn no_cookie_is_anonymous_without_redis() {
        let now = at("2026-11-10T00:00:00Z");
        assert_eq!(
            run(
                now,
                None,
                Err(SessionStoreError("must not be called".into())),
                Ok(None)
            )
            .await,
            SessionAuthOutcome::Anonymous(AnonymousReason::NoCookie)
        );
    }

    #[tokio::test]
    async fn invalid_cookie_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        for cookie in ["garbage", ""] {
            assert_eq!(
                run(now, Some(cookie), Ok(None), Ok(None)).await,
                SessionAuthOutcome::Anonymous(AnonymousReason::InvalidCookie)
            );
        }
        // A cookie signed with another key.
        let other =
            securecookie::encode_session_id(COOKIE_NAME, b"other", now.unix_timestamp(), ID);
        assert_eq!(
            run(now, Some(&other), Ok(None), Ok(None)).await,
            SessionAuthOutcome::Anonymous(AnonymousReason::InvalidCookie)
        );
    }

    #[tokio::test]
    async fn redis_failure_is_store_unavailable() {
        let now = at("2026-11-10T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Err(SessionStoreError("connection refused".into())),
                Ok(None)
            )
            .await,
            SessionAuthOutcome::StoreUnavailable
        );
    }

    #[tokio::test]
    async fn missing_session_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        assert_eq!(
            run(now, Some(&cookie(now)), Ok(None), Ok(None)).await,
            SessionAuthOutcome::Anonymous(AnonymousReason::NoSession)
        );
    }

    #[tokio::test]
    async fn valid_session_authenticates() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "2026-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Ok(Some(user("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")))
            )
            .await,
            SessionAuthOutcome::Authenticated { uid: 1 }
        );
    }

    #[tokio::test]
    async fn expired_session_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        // lastActive + 90d < now
        let info = info("2026-01-01T00:00:00Z", "2025-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Ok(Some(user("2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z")))
            )
            .await,
            SessionAuthOutcome::Anonymous(AnonymousReason::SessionExpired)
        );
    }

    #[tokio::test]
    async fn missing_user_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "2026-01-01T00:00:00Z");
        assert_eq!(
            run(now, Some(&cookie(now)), Ok(Some(payload(&info))), Ok(None)).await,
            SessionAuthOutcome::Anonymous(AnonymousReason::UserNotFound)
        );
    }

    #[tokio::test]
    async fn database_error_does_not_clear_the_session() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "2026-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Err("connection reset".into())
            )
            .await,
            SessionAuthOutcome::Anonymous(AnonymousReason::UserLookupFailed)
        );
    }

    #[tokio::test]
    async fn invalidated_session_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "2026-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Ok(Some(user("2026-01-01T00:00:00Z", "2026-11-09T23:30:00Z")))
            )
            .await,
            SessionAuthOutcome::Anonymous(AnonymousReason::Invalidated)
        );
    }

    #[tokio::test]
    async fn created_at_mismatch_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "2026-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Ok(Some(user("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z")))
            )
            .await,
            SessionAuthOutcome::Anonymous(AnonymousReason::CreatedAtMismatch)
        );
    }

    #[tokio::test]
    async fn zero_created_at_is_backfilled_and_accepted() {
        let now = at("2026-11-10T00:00:00Z");
        let info = info("2026-11-09T23:00:00Z", "0001-01-01T00:00:00Z");
        assert_eq!(
            run(
                now,
                Some(&cookie(now)),
                Ok(Some(payload(&info))),
                Ok(Some(user("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")))
            )
            .await,
            SessionAuthOutcome::Authenticated { uid: 1 }
        );
    }

    #[tokio::test]
    async fn null_or_missing_actor_is_anonymous() {
        let now = at("2026-11-10T00:00:00Z");
        for raw in [
            "null",
            "{\"actor\":null,\"lastActive\":\"2026-11-09T23:00:00Z\",\"expiryPeriod\":7776000000000000,\"userCreatedAt\":\"2026-01-01T00:00:00Z\"}",
        ] {
            assert_eq!(
                run(
                    now,
                    Some(&cookie(now)),
                    Ok(Some(raw_payload(raw))),
                    Ok(Some(user("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")))
                )
                .await,
                SessionAuthOutcome::Anonymous(AnonymousReason::NoActor)
            );
        }
    }

    #[tokio::test]
    async fn go_fixture_redis_payload_authenticates() {
        // `testdata/sessions/redis-single.gob` is the Go generator's payload
        // for actor UID 1 with the same lastActive/createdAt values.
        let path = format!(
            "{}/../../../testdata/sessions/redis-single.gob",
            env!("CARGO_MANIFEST_DIR")
        );
        let Ok(bytes) = std::fs::read(path) else {
            return;
        };
        let now = at("2026-11-10T00:00:00Z");
        let created = user("2026-11-07T13:56:25.553432Z", "2026-11-07T00:00:00Z");
        // The cookie fixture's ID is the Go baseID, not this payload's; the
        // store is keyed by whatever the cookie contains.
        assert_eq!(
            run(
                now,
                Some(&securecookie::encode_session_id(
                    COOKIE_NAME,
                    KEY,
                    now.unix_timestamp(),
                    ID
                )),
                Ok(Some(bytes)),
                Ok(Some(created))
            )
            .await,
            SessionAuthOutcome::Authenticated { uid: 1 }
        );
    }
}
