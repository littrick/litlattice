#![forbid(unsafe_code)]

//! Sourcegraph session cookies and Redis-backed sessions.
//!
//! The baseline (`cmd/frontend/internal/auth/session`) stores a session ID in
//! the `sgs` cookie with `gorilla/securecookie` and keeps the session payload
//! in Redis under `session_<id>` as a gob-encoded `map[interface{}]interface{}`
//! (`boj/redistore`). This crate ports both sides:
//!
//! * [`gob`] — the restricted `encoding/gob` codec for the cookie's string
//!   value and the Redis map payload;
//! * [`securecookie`] — the HMAC-SHA256 cookie codec (no encryption, 30-day
//!   decode max age);
//! * [`session`] — the `sessionInfo` model exactly as `SetData` marshals it;
//! * [`auth`] — `authenticateByCookie`'s read-path decisions (expiry, deleted
//!   user, session invalidation, creation-date mismatch);
//! * [`write`] — `SetData`/`SetActor`/`deleteSession`/`SetSignOutCookie`
//!   against the same redistore `SETEX`/`DEL`/cookie semantics, including
//!   gorilla's per-request session caching.
//!
//! The Redis I/O is abstracted by [`auth::SessionStore`] so both sides are
//! testable without a live Redis.

pub mod auth;
pub mod gob;
pub mod securecookie;
pub mod session;
pub mod write;

pub use auth::{
    authenticate, AnonymousReason, SessionAuthOutcome, SessionStore, SessionStoreError,
    SessionUser, SessionUserLookup,
};
pub use session::{Actor, GoTime, Session, SessionInfo, COOKIE_NAME, KEY_PREFIX, TTL_SECONDS};
pub use write::{
    load_session, parse_go_duration, resolve_expiry_period, save_session, session_info_json,
    FixedSessionIdSource, OsSessionIdSource, SessionIdSource, SessionManager, SetCookie,
    WritableSession, WriteOptions, SIGN_OUT_COOKIE,
};
