//! M9.8iv differential of the session write path and `/-/sign-out` against the
//! real Go baseline (`cmd/frontend/internal/app/zz_m9iv_oracle_test.go`, run by
//! `run_sign_out_diff.sh`).
//!
//! The Go oracle drives `serveSignOutHandler` with a real `redistore` store
//! (against an in-process RESP server) and mocked user/security-event/event
//! log stores, recording the normalized HTTP response, every Redis
//! `SETEX`/`DEL`, the security/legacy events and the invalidation calls. This
//! harness feeds the same corpus through [`sign_out::serve_sign_out`] with a
//! recording [`SessionStore`] and fake [`SignOutDb`], and writes the same JSON
//! shape so the script can compare the files byte-for-byte.
//!
//! Env:
//!
//! * `SG_SIGN_OUT_HARNESS=1`
//! * `SIGN_OUT_CORPUS` — `corpus.json` written by the Go oracle
//! * `SIGN_OUT_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use sg_db::telemetry_events::EventLogRow;
use sg_session::gob::{decode_session_map, encode_session_map};
use sg_session::securecookie::{decode_session_id, encode_session_id};
use sg_session::{FixedSessionIdSource, SessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::sign_out::{
    serve_sign_out, SecurityEventRecord, SignOutDb, SignOutDeps, SignOutRequest, SignOutResponse,
    SsoPlan, SsoProviderKind, SsoRemoteSignOut,
};

const COOKIE: &str = "sgs";
const SEED_ID: &str = "OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA";
const GOOD_KEY: &[u8] = b"m9iv-session-key";
const WRONG_KEY: &[u8] = b"m9iv-wrong-key";
const GENERATED_ID: &str = "GENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const NOW_UNIX: i64 = 1_760_000_000;

#[derive(Deserialize)]
struct CaseKv {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct CaseSession {
    id: String,
    #[serde(rename = "wrongKey", default)]
    wrong_key: bool,
    /// `null`/missing means the Redis key is absent.
    redis: Option<Vec<CaseKv>>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    session: Option<CaseSession>,
    uid: i32,
    #[serde(rename = "anonymousUid")]
    anonymous_uid: Option<String>,
    #[serde(rename = "signOutCookie", default)]
    sign_out_cookie: bool,
    #[serde(default)]
    secure: bool,
    /// Go marshals a nil `[]string` as `null`.
    providers: Option<Vec<String>>,
    #[serde(rename = "invalidateError", default)]
    invalidate_error: String,
    #[serde(rename = "loadError", default)]
    load_error: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct Op {
    op: String,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize)]
struct SecurityEventOut {
    name: String,
    url: String,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "anonymousUserId")]
    anonymous_user_id: String,
    source: String,
    argument: Value,
}

#[derive(Debug, Clone, Serialize)]
struct EventLogOut {
    name: String,
    url: String,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "anonymousUserId")]
    anonymous_user_id: String,
    source: String,
    argument: Value,
    #[serde(rename = "publicArgument")]
    public_argument: Value,
    #[serde(rename = "featureFlags")]
    feature_flags: Value,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "insertId")]
    insert_id: String,
}

#[derive(Serialize)]
struct Output {
    name: String,
    status: u16,
    location: String,
    #[serde(rename = "contentType")]
    content_type: String,
    body: String,
    cookies: Vec<String>,
    #[serde(rename = "redisOps")]
    redis_ops: Vec<Op>,
    #[serde(rename = "securityEvents")]
    security_events: Vec<SecurityEventOut>,
    #[serde(rename = "eventLogs")]
    event_logs: Vec<EventLogOut>,
    invalidations: Vec<i32>,
}

#[derive(Default)]
struct RecordingStore {
    data: Mutex<BTreeMap<String, Vec<u8>>>,
    load_error: Option<String>,
    ops: Mutex<Vec<Op>>,
}

#[async_trait]
impl SessionStore for RecordingStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        if let Some(error) = &self.load_error {
            return Err(SessionStoreError(format!("ERR {error}")));
        }
        Ok(self.data.lock().unwrap().get(id).cloned())
    }

    async fn save(
        &self,
        id: &str,
        payload: Vec<u8>,
        ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        let values: BTreeMap<String, String> = decode_session_map(&payload)
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, String::from_utf8_lossy(&value).into_owned()))
            .collect();
        self.ops.lock().unwrap().push(Op {
            op: "setex".to_owned(),
            id: normalize_id(id),
            ttl: Some(ttl_seconds),
            values: Some(values),
        });
        self.data.lock().unwrap().insert(id.to_owned(), payload);
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
        self.ops.lock().unwrap().push(Op {
            op: "del".to_owned(),
            id: normalize_id(id),
            ttl: None,
            values: None,
        });
        self.data.lock().unwrap().remove(id);
        Ok(())
    }
}

#[derive(Default)]
struct FakeDb {
    invalidate_error: String,
    invalidations: Mutex<Vec<i32>>,
    security_events: Mutex<Vec<SecurityEventOut>>,
    event_logs: Mutex<Vec<EventLogOut>>,
}

#[async_trait]
impl SignOutDb for FakeDb {
    async fn invalidate_sessions(&self, uid: i32) -> Result<(), String> {
        self.invalidations.lock().unwrap().push(uid);
        if self.invalidate_error.is_empty() {
            Ok(())
        } else {
            Err(self.invalidate_error.clone())
        }
    }

    async fn log_security_event(&self, event: SecurityEventRecord) -> Result<(), String> {
        self.security_events.lock().unwrap().push(SecurityEventOut {
            name: event.name,
            url: event.url,
            user_id: event.user_id,
            anonymous_user_id: event.anonymous_user_id,
            source: event.source,
            argument: event.argument,
        });
        Ok(())
    }

    async fn insert_event_log(&self, event: EventLogRow) -> Result<(), String> {
        self.event_logs.lock().unwrap().push(EventLogOut {
            name: event.name,
            url: event.url,
            user_id: event.user_id,
            anonymous_user_id: event.anonymous_user_id,
            source: event.source,
            argument: event.argument,
            public_argument: event.public_argument,
            feature_flags: event.feature_flags,
            device_id: "<uuid>".to_owned(),
            insert_id: "<uuid>".to_owned(),
        });
        Ok(())
    }
}

struct FakeRemote {
    calls: Mutex<Vec<(SsoProviderKind, Option<Value>)>>,
}

#[async_trait]
impl SsoRemoteSignOut for FakeRemote {
    async fn sign_out(
        &self,
        kind: SsoProviderKind,
        session_data: Option<Value>,
    ) -> Result<(), String> {
        self.calls.lock().unwrap().push((kind, session_data));
        Ok(())
    }
}

fn normalize_id(id: &str) -> String {
    match id {
        "" => String::new(),
        SEED_ID => SEED_ID.to_owned(),
        _ => "<generated>".to_owned(),
    }
}

/// Go's `encoding/json` HTML-escapes `<`, `>` and `&`; these bytes can only
/// occur inside JSON strings, so a blind replace is safe.
fn go_json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_string(value)
        .expect("encode")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .into_bytes()
}

/// The Go oracle's `normalizeCookie`: drop `Expires` and replace the `sgs`
/// value with its decoded, normalized ID.
fn normalize_cookie(header: &str, now: i64) -> String {
    let mut parts = header.split("; ");
    let first = parts.next().unwrap_or("");
    let (name, value) = first.split_once('=').unwrap_or((first, ""));
    let mut out = Vec::new();
    if name == COOKIE {
        let id = if value.is_empty() {
            String::new()
        } else {
            decode_session_id(COOKIE, GOOD_KEY, value, now)
                .map(|(_, id)| id)
                .unwrap_or_else(|_| "<undecodable>".to_owned())
        };
        out.push(format!("sgs.id={}", normalize_id(&id)));
    } else {
        out.push(first.to_owned());
    }
    for part in parts {
        if part.starts_with("Expires=") {
            continue;
        }
        out.push(part.to_owned());
    }
    out.join("; ")
}

fn plans(providers: &[String]) -> Vec<SsoPlan> {
    providers
        .iter()
        .filter_map(|provider| match provider.as_str() {
            "openidconnect" => Some(SsoPlan {
                kind: SsoProviderKind::OpenIdConnect,
                session_key: Some("oidc@0"),
            }),
            "saml" => Some(SsoPlan {
                kind: SsoProviderKind::Saml,
                session_key: None,
            }),
            _ => None,
        })
        .collect()
}

fn cookies_from(response: &SignOutResponse, now: i64) -> Vec<String> {
    response
        .set_cookies
        .iter()
        .map(|header| normalize_cookie(header, now))
        .collect()
}

#[test]
fn sign_out_harness() {
    if std::env::var("SG_SIGN_OUT_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("SIGN_OUT_CORPUS").expect("SIGN_OUT_CORPUS");
    let out = std::env::var("SIGN_OUT_OUT").expect("SIGN_OUT_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let now = OffsetDateTime::from_unix_timestamp(NOW_UNIX).expect("timestamp");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    for case in cases {
        let store = Arc::new(RecordingStore {
            load_error: if case.load_error.is_empty() {
                None
            } else {
                Some(case.load_error.clone())
            },
            ..Default::default()
        });
        if let Some(session) = &case.session {
            if let Some(redis) = &session.redis {
                let refs: Vec<(&str, &[u8])> = redis
                    .iter()
                    .map(|kv| (kv.key.as_str(), kv.value.as_bytes()))
                    .collect();
                store
                    .data
                    .lock()
                    .unwrap()
                    .insert(session.id.clone(), encode_session_map(&refs));
            }
        }

        let session_cookie = case.session.as_ref().map(|session| {
            let key = if session.wrong_key {
                WRONG_KEY
            } else {
                GOOD_KEY
            };
            encode_session_id(COOKIE, key, now.unix_timestamp(), &session.id)
        });

        let db = Arc::new(FakeDb {
            invalidate_error: case.invalidate_error.clone(),
            ..Default::default()
        });
        let remote = Arc::new(FakeRemote {
            calls: Mutex::new(Vec::new()),
        });
        let secure = case.secure;
        let store_trait: Arc<dyn SessionStore> = store.clone();
        let ids: Arc<dyn SessionIdSource> = Arc::new(FixedSessionIdSource(GENERATED_ID.to_owned()));
        let deps = SignOutDeps {
            store: store_trait,
            ids,
            cookie_key: GOOD_KEY.to_vec(),
            secure,
            db: db.clone(),
            sso_plans: plans(case.providers.as_deref().unwrap_or(&[])),
            sso_remote: remote,
            external_url: format!("{}://example.com", if secure { "https" } else { "http" }),
        };
        let request = SignOutRequest {
            actor_uid: case.uid,
            path: "/-/sign-out",
            // `httptest.NewRequest("GET", "/-/sign-out", nil)` has no Host for
            // a path-only target, so the legacy event URL is empty.
            host: "",
            anonymous_uid: case.anonymous_uid.as_deref(),
            session_cookie: session_cookie.as_deref(),
            sign_out_cookie: case.sign_out_cookie,
            now,
        };

        let response = runtime.block_on(serve_sign_out(&deps, &request));

        let output = Output {
            name: case.name.clone(),
            status: response.status,
            location: response.location.clone(),
            content_type: response.content_type.clone(),
            body: String::from_utf8(response.body.clone()).expect("utf8 body"),
            cookies: cookies_from(&response, now.unix_timestamp()),
            redis_ops: store.ops.lock().unwrap().clone(),
            security_events: db.security_events.lock().unwrap().clone(),
            event_logs: db.event_logs.lock().unwrap().clone(),
            invalidations: db.invalidations.lock().unwrap().clone(),
        };
        let body = go_json(&output);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!(
            "RUST {} status={} cookies={} ops={}",
            output.name,
            output.status,
            output.cookies.len(),
            output.redis_ops.len()
        );
    }
}
