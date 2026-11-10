//! M9.8fe differential of `authenticateByCookie`'s decision logic against the
//! real Go baseline (`cmd/frontend/internal/auth/session/m9fe_oracle_test.go`,
//! run by `run_session_auth_diff.sh`).
//!
//! The Go oracle records `{name, uid, done, status, body}` for every corpus
//! case; this harness feeds the same `actor` JSON and user row through
//! [`sg_session::authenticate`] with an in-memory store and writes the same
//! shape, so the script can compare the files byte-for-byte.
//!
//! Env:
//!
//! * `SG_SESSION_AUTH_HARNESS=1`
//! * `SESSION_AUTH_CORPUS` — corpus written by the Go oracle
//! * `SESSION_AUTH_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_session::auth::{SessionStore, SessionStoreError, SessionUser, SessionUserLookup};
use sg_session::securecookie::encode_session_id;
use sg_session::{authenticate, Session, SessionAuthOutcome, COOKIE_NAME};

/// The key is irrelevant to the decision (the fake store ignores the ID), but
/// the cookie must decode.
const KEY: &[u8] = b"m9fe-session-oracle";
const ID: &str = "OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA";

#[derive(Deserialize)]
struct UserRow {
    id: i32,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "invalidatedSessionsAt")]
    invalidated_at: String,
    #[serde(rename = "siteAdmin", default)]
    #[allow(dead_code)]
    site_admin: bool,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    /// Exact bytes the Go baseline stored under the session's `actor` key;
    /// absent when the key is missing.
    #[serde(rename = "actorJson")]
    actor_json: Option<String>,
    user: Option<UserRow>,
    #[serde(rename = "userError", default)]
    user_error: String,
    #[serde(rename = "storeError", default)]
    store_error: bool,
}

#[derive(Serialize)]
struct Outcome {
    name: String,
    uid: i32,
    done: bool,
    status: i32,
    body: String,
}

struct Store {
    payload: Result<Vec<u8>, SessionStoreError>,
}

#[async_trait]
impl SessionStore for Store {
    async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        self.payload.clone().map(Some)
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

fn at(s: &str) -> OffsetDateTime {
    OffsetDateTime::parse(s, &Rfc3339).unwrap()
}

#[test]
fn session_auth_harness() {
    if std::env::var("SG_SESSION_AUTH_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("SESSION_AUTH_CORPUS").expect("SESSION_AUTH_CORPUS");
    let out = std::env::var("SESSION_AUTH_OUT").expect("SESSION_AUTH_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let now = OffsetDateTime::now_utc();
    let cookie = encode_session_id(COOKIE_NAME, KEY, now.unix_timestamp(), ID);

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case in cases {
        // The Redis payload is the gob map; a missing `actor` key is an empty
        // map, not a missing session (which is `Ok(None)`).
        let values: Vec<(String, Vec<u8>)> = match &case.actor_json {
            Some(json) => vec![("actor".to_owned(), json.as_bytes().to_vec())],
            None => Vec::new(),
        };
        let payload = Session {
            timestamp: now.unix_timestamp(),
            id: ID.to_owned(),
            values,
        }
        .encode_redis();
        let store = Store {
            payload: if case.store_error {
                Err(SessionStoreError("connection refused".to_owned()))
            } else {
                Ok(payload)
            },
        };
        let user = if case.user_error == "not_found" {
            Ok(None)
        } else if case.user_error == "db_error" {
            Err("database is down".to_owned())
        } else {
            let u = case.user.as_ref().expect("user");
            Ok(Some(SessionUser {
                id: u.id,
                created_at: at(&u.created_at),
                invalidated_sessions_at: at(&u.invalidated_at),
            }))
        };

        let result = runtime.block_on(authenticate(
            &store,
            &Users { user },
            KEY,
            Some(&cookie),
            now,
        ));
        let outcome = match result {
            SessionAuthOutcome::Authenticated { uid } => Outcome {
                name: case.name.clone(),
                uid,
                done: false,
                status: 200,
                body: String::new(),
            },
            SessionAuthOutcome::Anonymous(_) => Outcome {
                name: case.name.clone(),
                uid: 0,
                done: false,
                status: 200,
                body: String::new(),
            },
            SessionAuthOutcome::StoreUnavailable => Outcome {
                name: case.name.clone(),
                uid: 0,
                done: true,
                status: 500,
                body: "failed to read session actor".to_owned(),
            },
        };
        let body = serde_json::to_vec(&outcome).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!(
            "RUST {} uid={} done={} status={}",
            outcome.name, outcome.uid, outcome.done, outcome.status
        );
    }
}
