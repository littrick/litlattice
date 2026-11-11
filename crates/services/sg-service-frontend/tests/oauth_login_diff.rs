//! M9.8jk differential of the generic OAuth login/callback flow against the
//! real Go baseline (`cmd/frontend/internal/auth/oauth/zz_m9jk_oracle_test.go`,
//! run by `run_oauth_login_diff.sh`).
//!
//! The Go oracle drives `oauth.NewMiddleware` with a synthetic `fakeoauth`
//! provider, a fake IdP transport (no network), an in-process RESP server for
//! `redistore`, and mocked user/security-event stores. It records the
//! normalized status, headers, Location, body, cookies, Redis ops and security
//! events. This harness feeds the same corpus through
//! [`oauth_login::handle_login`] / [`oauth_login::handle_callback`] with a
//! recording [`SessionStore`], fixed CSRF/session-ID sources, a fixed clock, a
//! fake token endpoint and fake user/event backends, and writes the same JSON
//! shape for a byte-for-byte `cmp`.
//!
//! Env:
//!
//! * `SG_OAUTH_LOGIN_HARNESS=1`
//! * `OAUTH_LOGIN_CORPUS` — `corpus.json` written by the Go oracle
//! * `OAUTH_LOGIN_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_session::gob::decode_session_map;
use sg_session::securecookie::decode_session_id;
use sg_session::session::GoTime;
use sg_session::{FixedSessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::oauth_login::{
    encode_query_sorted, go_json_escape, go_parse_url, go_url_string, html_escape, parse_query,
    FixedStateSource, GoUrl, LoginState, OAuthCallbackRequest, OAuthFlowDeps, OAuthFlowResponse,
    OAuthHttpClient, OAuthHttpResponse, OAuthLoginRequest, OAuthProviderConfig, OAuthProviders,
    OAuthSecurityEventRecord, OAuthSecurityEventSink, OAuthToken, OAuthUserLookup, OAuthUserRecord,
    OAuthUserResolver, TokenExchangeRequest, UserResolution,
};

const COOKIE: &str = "sgs";
const GOOD_KEY: &[u8] = b"m9jk-session-key";
const GENERATED_ID: &str = "GENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const NOW_UNIX: i64 = 1_760_000_000;
const FIXED_CSRF: &str = "m9jk-fixed-csrf";

const NO_ID_TOKEN_MSG: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: the issuer's authorization response did not contain an ID token.";
const UNVERIFIED_EMAIL_MSG: &str = "Could not get verified email for GitHub user. Check that your GitHub account has a verified email that matches one of your Sourcegraph verified emails.";

#[derive(Deserialize)]
struct CaseProvider {
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "clientSecret")]
    client_secret: String,
    #[serde(rename = "authUrl")]
    auth_url: String,
    #[serde(rename = "tokenUrl")]
    token_url: String,
    scopes: Vec<String>,
    #[serde(rename = "redirectUrl")]
    redirect_url: String,
}

#[derive(Deserialize)]
struct CaseLogin {
    #[serde(default)]
    skip: bool,
    #[serde(rename = "pcMode", default)]
    pc_mode: String,
    #[serde(default)]
    redirect: String,
    #[serde(rename = "returnTo", default)]
    return_to: String,
    #[serde(default)]
    referer: String,
    #[serde(default)]
    op: String,
}

#[derive(Deserialize, Default)]
struct CaseExchange {
    #[serde(default)]
    status: u16,
    #[serde(rename = "contentType", default)]
    content_type: String,
    #[serde(default)]
    body: String,
}

#[derive(Deserialize)]
struct CaseCallback {
    #[serde(default)]
    invoke: bool,
    #[serde(default)]
    code: String,
    #[serde(rename = "stateMode", default)]
    state_mode: String,
    #[serde(rename = "customState", default)]
    custom_state: Option<CaseStateSpec>,
    #[serde(rename = "includeCookie", default)]
    include_cookie: bool,
    #[serde(default)]
    exchange: Option<CaseExchange>,
}

#[derive(Deserialize)]
struct CaseStateSpec {
    #[serde(rename = "Redirect", default)]
    redirect: String,
    #[serde(rename = "ProviderID", default)]
    provider_id: String,
    #[serde(rename = "CSRF", default)]
    csrf: String,
    #[serde(rename = "Op", default)]
    op: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    provider: CaseProvider,
    login: CaseLogin,
    callback: CaseCallback,
    #[serde(default)]
    secure: bool,
    #[serde(rename = "anonymousUid")]
    anonymous_uid: Option<String>,
    #[serde(rename = "newUser", default)]
    new_user: bool,
    #[serde(rename = "createError", default)]
    create_error: String,
    #[serde(rename = "userMissing", default)]
    user_missing: bool,
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
}

#[derive(Default)]
struct RecordingStore {
    data: Mutex<BTreeMap<String, Vec<u8>>>,
    ops: Mutex<Vec<Op>>,
}

#[async_trait]
impl SessionStore for RecordingStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
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
            .map(|(key, value)| {
                let raw = String::from_utf8_lossy(&value).into_owned();
                let normalized = match key.as_str() {
                    "actor" => normalize_actor_json(&raw),
                    "oauthState" => normalize_oauth_state_json(&raw),
                    _ => raw,
                };
                (key, normalized)
            })
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

struct FakeHttp {
    response: Option<OAuthHttpResponse>,
}

#[async_trait]
impl OAuthHttpClient for FakeHttp {
    async fn exchange_code(
        &self,
        _request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String> {
        self.response
            .clone()
            .ok_or_else(|| "no fake response".to_owned())
    }
}

struct FakeResolver {
    service_id: String,
    new_user: bool,
    create_error: String,
}

#[async_trait]
impl OAuthUserResolver for FakeResolver {
    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn success_event_name(&self) -> &str {
        "FakeOAuthSucceeded"
    }

    fn failure_event_name(&self) -> &str {
        "FakeOAuthFailed"
    }

    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution {
        let raw = token.extra_str("id_token");
        if raw.is_empty() {
            return UserResolution::Failed {
                safe_message: NO_ID_TOKEN_MSG.to_owned(),
            };
        }
        let Some((email, verified)) = decode_id_token(&raw) else {
            return UserResolution::Failed {
                safe_message: "Authentication failed. Could not decode ID token.".to_owned(),
            };
        };
        if !verified {
            return UserResolution::Failed {
                safe_message: UNVERIFIED_EMAIL_MSG.to_owned(),
            };
        }
        if email.is_empty() {
            return UserResolution::Failed {
                safe_message: UNVERIFIED_EMAIL_MSG.to_owned(),
            };
        }
        if !self.create_error.is_empty() {
            return UserResolution::Failed {
                safe_message: self.create_error.clone(),
            };
        }
        UserResolution::Created {
            user_id: 1,
            new_user: self.new_user,
        }
    }
}

struct FakeUsers {
    missing: bool,
}

#[async_trait]
impl OAuthUserLookup for FakeUsers {
    async fn get_by_id(&self, uid: i32) -> Result<OAuthUserRecord, String> {
        if self.missing {
            return Err("user not found".to_owned());
        }
        let created_at = OffsetDateTime::parse("2020-01-02T03:04:05Z", &Rfc3339).unwrap();
        Ok(OAuthUserRecord {
            id: uid,
            created_at: GoTime(created_at),
            site_admin: false,
        })
    }
}

#[derive(Default)]
struct FakeEvents {
    events: Mutex<Vec<SecurityEventOut>>,
}

#[async_trait]
impl OAuthSecurityEventSink for FakeEvents {
    async fn log_security_event(&self, event: OAuthSecurityEventRecord) -> Result<(), String> {
        self.events.lock().unwrap().push(SecurityEventOut {
            name: event.name,
            url: event.url,
            user_id: event.user_id,
            anonymous_user_id: event.anonymous_user_id,
            source: event.source,
            argument: event.argument,
        });
        Ok(())
    }
}

fn decode_id_token(raw: &str) -> Option<(String, bool)> {
    let parts: Vec<&str> = raw.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: Value = serde_json::from_slice(&payload).ok()?;
    let email = claims
        .get("email")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let verified = claims
        .get("email_verified")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some((email, verified))
}

fn normalize_id(id: &str) -> String {
    match id {
        "" => String::new(),
        _ => "<generated>".to_owned(),
    }
}

/// Go's `encoding/json` HTML escaping, applied to the pretty output too.
fn go_escape(input: &str) -> String {
    go_json_escape(input)
}

fn normalize_encoded_state(encoded: &str) -> String {
    match LoginState::decode(encoded) {
        None => "<invalid-state>".to_owned(),
        Some(mut state) => {
            if !state.csrf.is_empty() {
                state.csrf = "<csrf>".to_owned();
            }
            URL_SAFE_NO_PAD.encode(state.to_go_json())
        }
    }
}

fn normalize_oauth_state_json(value: &str) -> String {
    let Ok(encoded) = serde_json::from_str::<String>(value) else {
        return value.to_owned();
    };
    let normalized = normalize_encoded_state(&encoded);
    go_escape(&serde_json::to_string(&normalized).expect("string serializes"))
}

fn normalize_actor_json(value: &str) -> String {
    if value == "null" {
        return value.to_owned();
    }
    let Ok(mut map) = serde_json::from_str::<BTreeMap<String, Value>>(value) else {
        return value.to_owned();
    };
    map.insert("lastActive".to_owned(), Value::String("<now>".to_owned()));
    go_escape(&serde_json::to_string(&map).expect("actor serializes"))
}

fn normalize_location(location: &str) -> String {
    if location.is_empty() {
        return String::new();
    }
    let Some(parsed) = go_parse_url(location) else {
        return location.to_owned();
    };
    let mut pairs = parse_query(&parsed.raw_query);
    if let Some((_, value)) = pairs.iter_mut().find(|(key, _)| key == "state") {
        if !value.is_empty() {
            *value = normalize_encoded_state(value);
        }
    }
    let raw_query = encode_query_sorted(&pairs);
    go_url_string(&GoUrl {
        raw_query,
        ..parsed
    })
}

fn normalize_body(status: u16, location: &str, body: &str) -> String {
    if status == 302 && !location.is_empty() && body.starts_with("<a href=") {
        return format!("<a href=\"{}\">Found</a>.\n\n", html_escape(location));
    }
    body.to_owned()
}

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

fn cookies_from(response: &OAuthFlowResponse, now: i64) -> Vec<String> {
    response
        .set_cookies
        .iter()
        .map(|cookie| normalize_cookie(&cookie.to_header(), now))
        .collect()
}

fn login_cookie_value(response: &OAuthFlowResponse) -> Option<String> {
    response
        .set_cookies
        .iter()
        .find(|cookie| cookie.name == COOKIE && !cookie.value.is_empty())
        .map(|cookie| cookie.value.clone())
}

fn login_state_from_location(location: &str) -> Option<String> {
    let parsed = go_parse_url(location)?;
    parse_query(&parsed.raw_query)
        .into_iter()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
}

fn output_from(
    case: &Case,
    response: &OAuthFlowResponse,
    ops: Vec<Op>,
    events: Vec<SecurityEventOut>,
) -> Output {
    let location = normalize_location(&response.location);
    let body = String::from_utf8_lossy(&response.body).into_owned();
    Output {
        name: case.name.clone(),
        status: response.status,
        location: location.clone(),
        content_type: response.content_type.clone(),
        body: normalize_body(response.status, &location, &body),
        cookies: cookies_from(response, NOW_UNIX),
        redis_ops: ops,
        security_events: events,
    }
}

#[test]
fn oauth_login_harness() {
    if std::env::var("SG_OAUTH_LOGIN_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("OAUTH_LOGIN_CORPUS").expect("OAUTH_LOGIN_CORPUS");
    let out = std::env::var("OAUTH_LOGIN_OUT").expect("OAUTH_LOGIN_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let now = OffsetDateTime::from_unix_timestamp(NOW_UNIX).expect("timestamp");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    for case in cases {
        let provider = OAuthProviderConfig {
            service_type: case.provider.service_type.clone(),
            service_id: case.provider.service_id.clone(),
            client_id: case.provider.client_id.clone(),
            client_secret: case.provider.client_secret.clone(),
            auth_url: case.provider.auth_url.clone(),
            token_url: case.provider.token_url.clone(),
            scopes: case.provider.scopes.clone(),
            redirect_url: case.provider.redirect_url.clone(),
            config_id: format!("{}::{}", case.provider.service_id, case.provider.client_id),
            sso_url: String::new(),
        };
        let providers = OAuthProviders::from_configs(vec![provider.clone()]);
        let store = Arc::new(RecordingStore::default());
        let ids = FixedSessionIdSource(GENERATED_ID.to_owned());
        let state_source = FixedStateSource(FIXED_CSRF.to_owned());
        let http = FakeHttp {
            response: case
                .callback
                .exchange
                .as_ref()
                .map(|exchange| OAuthHttpResponse {
                    status: exchange.status,
                    content_type: exchange.content_type.clone(),
                    body: exchange.body.clone().into_bytes(),
                }),
        };
        let resolver = FakeResolver {
            service_id: case.provider.service_id.clone(),
            new_user: case.new_user,
            create_error: case.create_error.clone(),
        };
        let users = FakeUsers {
            missing: case.user_missing,
        };
        let events = Arc::new(FakeEvents::default());
        let store_trait: Arc<dyn SessionStore> = store.clone();
        let deps = OAuthFlowDeps {
            providers: &providers,
            store: store_trait.as_ref(),
            ids: &ids,
            cookie_key: GOOD_KEY,
            secure: case.secure,
            external_url: if case.secure {
                "https://sourcegraph.example"
            } else {
                "http://sourcegraph.example"
            },
            configured_expiry: None,
            license_expired: false,
            state_source: &state_source,
            http: &http,
            resolver: &resolver,
            users: &users,
            events: events.as_ref(),
            now,
        };

        let mut login_cookie = None;
        let mut login_state = None;
        if !case.login.skip {
            let mut query: Vec<(String, String)> = Vec::new();
            match case.login.pc_mode.as_str() {
                "normal" => query.push(("pc".to_owned(), provider.config_id())),
                "unknown" => query.push(("pc".to_owned(), "unknown::provider".to_owned())),
                _ => {}
            }
            if !case.login.redirect.is_empty() {
                query.push(("redirect".to_owned(), case.login.redirect.clone()));
            }
            if !case.login.return_to.is_empty() {
                query.push(("returnTo".to_owned(), case.login.return_to.clone()));
            }
            if !case.login.op.is_empty() {
                query.push(("op".to_owned(), case.login.op.clone()));
            }
            let request = OAuthLoginRequest {
                service_type: &case.provider.service_type,
                query: &query,
                referer: if case.login.referer.is_empty() {
                    None
                } else {
                    Some(case.login.referer.as_str())
                },
                session_cookie: None,
            };
            let response = runtime.block_on(sg_service_frontend::oauth_login::handle_login(
                &deps, &request,
            ));
            login_cookie = login_cookie_value(&response);
            login_state = login_state_from_location(&response.location);

            if !case.callback.invoke {
                let output = output_from(
                    &case,
                    &response,
                    store.ops.lock().unwrap().clone(),
                    events.events.lock().unwrap().clone(),
                );
                write_output(&out, &output);
                continue;
            }
            store.ops.lock().unwrap().clear();
            events.events.lock().unwrap().clear();
        }

        let callback = &case.callback;
        let mut query: Vec<(String, String)> = Vec::new();
        if !callback.code.is_empty() {
            query.push(("code".to_owned(), callback.code.clone()));
        }
        match callback.state_mode.as_str() {
            "from_login" => {
                if let Some(state) = login_state.clone() {
                    query.push(("state".to_owned(), state));
                }
            }
            "mismatch" => {
                let state = LoginState {
                    redirect: "/search".to_owned(),
                    provider_id: provider.config_id(),
                    csrf: "other-csrf".to_owned(),
                    op: String::new(),
                };
                query.push(("state".to_owned(), state.encode()));
            }
            "malformed" => query.push(("state".to_owned(), "!!!".to_owned())),
            "custom" => {
                if let Some(custom) = &callback.custom_state {
                    let state = LoginState {
                        redirect: custom.redirect.clone(),
                        provider_id: custom.provider_id.clone(),
                        csrf: custom.csrf.clone(),
                        op: custom.op.clone(),
                    };
                    query.push(("state".to_owned(), state.encode()));
                }
            }
            _ => {}
        }
        let anonymous_uid = case.anonymous_uid.clone();
        let request = OAuthCallbackRequest {
            service_type: &case.provider.service_type,
            path: "/callback",
            query: &query,
            session_cookie: if callback.include_cookie {
                login_cookie.as_deref()
            } else {
                None
            },
            anonymous_uid: anonymous_uid.as_deref(),
        };
        let response = runtime.block_on(sg_service_frontend::oauth_login::handle_callback(
            &deps, &request,
        ));
        let output = output_from(
            &case,
            &response,
            store.ops.lock().unwrap().clone(),
            events.events.lock().unwrap().clone(),
        );
        write_output(&out, &output);
    }
}

fn write_output(out: &str, output: &Output) {
    let mut json = serde_json::to_string_pretty(output).expect("encode");
    json = go_escape(&json);
    std::fs::write(
        Path::new(out).join(format!("{}.json", output.name)),
        json.as_bytes(),
    )
    .expect("write");
    println!(
        "RUST {} status={} cookies={} ops={} events={}",
        output.name,
        output.status,
        output.cookies.len(),
        output.redis_ops.len(),
        output.security_events.len()
    );
}
