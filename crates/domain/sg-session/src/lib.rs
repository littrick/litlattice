#![forbid(unsafe_code)]

//! Sourcegraph session cookies and Redis-backed sessions.
//!
//! The baseline (`cmd/frontend/internal/auth/session`) stores a session ID in
//! the `sgs` cookie with `gorilla/securecookie` and keeps the session payload
//! in Redis under `session_<id>` as a gob-encoded `map[interface{}]interface{}`
//! (`boj/redistore`). This crate ports the read path used by the frontend's
//! session-cookie authentication:
//!
//! * [`gob`] — the restricted `encoding/gob` codec for the cookie's string
//!   value and the Redis map payload;
//! * [`securecookie`] — the HMAC-SHA256 cookie codec (no encryption, 30-day
//!   decode max age);
//! * [`session`] — the `sessionInfo` model exactly as `SetData` marshals it;
//! * [`auth`] — `authenticateByCookie`'s read-path decisions (expiry, deleted
//!   user, session invalidation, creation-date mismatch).
//!
//! Session **writes** (Set-Cookie renewal/clearing, `SetData`, `deleteSession`)
//! are not part of this slice: [`auth`] reports what the baseline would do, and
//! the HTTP layer keeps serving the unauthenticated request. The Redis I/O is
//! abstracted by [`auth::SessionStore`] so the decision logic is testable
//! against Go fixtures without a live Redis.

pub mod auth;
pub mod gob;
pub mod securecookie;
pub mod session;

pub use auth::{
    authenticate, AnonymousReason, SessionAuthOutcome, SessionStore, SessionStoreError,
    SessionUser, SessionUserLookup,
};
pub use session::{Actor, GoTime, Session, SessionInfo, COOKIE_NAME, KEY_PREFIX, TTL_SECONDS};
