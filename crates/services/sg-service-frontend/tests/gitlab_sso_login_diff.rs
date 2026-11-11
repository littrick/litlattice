//! M9.8jv differential of the GitLab `ssoURL` production login wiring against
//! the real Go baseline
//! (`cmd/frontend/internal/auth/gitlaboauth/zz_m9jv_sso_login_oracle_test.go`,
//! run by `run_gitlab_sso_login_diff.sh`).
//!
//! The Go oracle drives the real `parseConfig -> providers.MockProviders ->
//! oauth.NewMiddleware` path so `SSOLoginHandler` is selected exactly as in
//! production. This harness goes through the rewrite's production assembly
//! instead: `OAuthProviders::from_site` (which fills
//! [`OAuthProviderConfig::sso_url`]) -> `OAuthRouteConfig` -> `flow_deps` ->
//! [`auth_prefix_response_at`] (`/.auth/<provider>/login`). Both sides parse
//! the same flat site JSON and write the same normalized JSON for a
//! byte-for-byte `cmp`.
//!
//! Env:
//!
//! * `SG_M9JV_SSO_HARNESS=1`
//! * `M9JV_SSO_CORPUS` — `corpus.json` written by the Go oracle
//! * `M9JV_SSO_OUT` — output directory (one `<name>.json` per case under
//!   `<M9JV_SSO_OUT>/rust/`)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use http::{header, Method, Request};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use sg_session::gob::decode_session_map;
use sg_session::securecookie::decode_session_id;
use sg_session::{FixedSessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::actor_chain::LicenseReader;
use sg_service_frontend::authz::{html_escape, LicenseState};
use sg_service_frontend::oauth_login::{
    encode_query_sorted, go_json_escape, go_parse_url, go_url_string, parse_query,
    FixedStateSource, GoUrl, LoginState, OAuthSecurityEventRecord, OAuthSecurityEventSink,
    OAuthUserLookup, OAuthUserRecord,
};
use sg_service_frontend::oauth_routes::{
    auth_prefix_response_at, OAuthRouteConfig, OAuthRouteDeps, OAuthUserResolverRegistry,
};

const COOKIE: &str = "sgs";
const GOOD_KEY: &[u8] = b"m9jv-session-key";
const GENERATED_ID: &str = "M9JVGENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const FIXED_CSRF: &str = "m9jv-fixed-csrf";
const NOW_UNIX: i64 = 1_760_000_000;

#[derive(Deserialize)]
struct Case {
    name: String,
    site: String,
    path: String,
    #[serde(rename = "pcMode", default)]
    pc_mode: String,
    #[serde(default)]
    pc: String,
    #[serde(default)]
    redirect: String,
    #[serde(default)]
    op: String,
    #[serde(default)]
    referer: String,
}

#[derive(Debug, Clone, Serialize)]
struct Op {
    op: String,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<BTreeMap<String, String>>,
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
    #[serde(rename = "providerCount")]
    provider_count: usize,
    #[serde(rename = "configProblems")]
    config_problems: Vec<String>,
}

// ---------------------------------------------------------------------------
// Recording session store; same normalization as the jq/jv harnesses.
// ---------------------------------------------------------------------------

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

struct FakeLicense;

#[async_trait]
impl LicenseReader for FakeLicense {
    async fn product_license(&self) -> LicenseState {
        LicenseState::None
    }
}

struct FakeUsers;

#[async_trait]
impl OAuthUserLookup for FakeUsers {
    async fn get_by_id(&self, _uid: i32) -> Result<OAuthUserRecord, String> {
        Err("user not found".to_owned())
    }
}

struct FakeEvents;

#[async_trait]
impl OAuthSecurityEventSink for FakeEvents {
    async fn log_security_event(&self, _event: OAuthSecurityEventRecord) -> Result<(), String> {
        Ok(())
    }
}

struct NoExchange;

#[async_trait]
impl sg_service_frontend::oauth_login::OAuthHttpClient for NoExchange {
    async fn exchange_code(
        &self,
        _request: &sg_service_frontend::oauth_login::TokenExchangeRequest,
    ) -> Result<sg_service_frontend::oauth_login::OAuthHttpResponse, String> {
        Err("no token exchange in login differential".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Normalization (mirrors the Go oracle).
// ---------------------------------------------------------------------------

fn normalize_id(id: &str) -> String {
    if id.is_empty() {
        String::new()
    } else {
        "<generated>".to_owned()
    }
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
    go_json_escape(&serde_json::to_string(&normalized).expect("string serializes"))
}

/// `url.Parse` + `Values.Set("state")` for the inner authorize URL.
fn normalize_redirect(redirect: &str) -> String {
    let Some(mut inner) = go_parse_url(redirect) else {
        return redirect.to_owned();
    };
    let mut pairs = parse_query(&inner.raw_query);
    if let Some((_, value)) = pairs.iter_mut().find(|(key, _)| key == "state") {
        if !value.is_empty() {
            *value = normalize_encoded_state(value);
        }
    }
    inner.raw_query = encode_query_sorted(&pairs);
    go_url_string(&inner)
}

/// Mirrors the oracle: every `redirect` value is normalized individually (an
/// SSO URL may already carry one), so duplicate-key semantics stay visible.
fn normalize_location(location: &str) -> String {
    if location.is_empty() {
        return String::new();
    }
    let Some(parsed) = go_parse_url(location) else {
        return location.to_owned();
    };
    let mut pairs = parse_query(&parsed.raw_query);
    for (key, value) in pairs.iter_mut() {
        if value.is_empty() {
            continue;
        }
        if key == "state" {
            *value = normalize_encoded_state(value);
        } else if key == "redirect" {
            *value = normalize_redirect(value);
        }
    }
    go_url_string(&GoUrl {
        raw_query: encode_query_sorted(&pairs),
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

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

fn build_request(case: &Case, pc: &str) -> Request<()> {
    let mut pairs: Vec<(String, String)> = vec![("pc".to_owned(), pc.to_owned())];
    if !case.redirect.is_empty() {
        pairs.push(("redirect".to_owned(), case.redirect.clone()));
    }
    if !case.op.is_empty() {
        pairs.push(("op".to_owned(), case.op.clone()));
    }
    let query = encode_query_sorted(&pairs);
    let uri = if query.is_empty() {
        case.path.clone()
    } else {
        format!("{}?{}", case.path, query)
    };
    let mut builder = Request::builder().method(Method::GET).uri(uri);
    if !case.referer.is_empty() {
        builder = builder.header(header::REFERER, &case.referer);
    }
    builder.body(()).expect("request")
}

async fn run_case(case: &Case) -> Output {
    let site = sg_config::SiteConfiguration::parse(&case.site).expect("site configuration");
    // The login differential never uses dotcom mode; the Go oracle does not
    // mock `dotcom.SourcegraphDotComMode` either.
    let config = OAuthRouteConfig::from_site(&site, false);
    let provider_count = config.providers.providers.len();

    let pc = match case.pc_mode.as_str() {
        "first" => config
            .providers
            .providers
            .first()
            .map(|provider| provider.config_id())
            .unwrap_or_else(|| "unknown::provider".to_owned()),
        "literal" => case.pc.clone(),
        _ => "unknown::provider".to_owned(),
    };

    let store = Arc::new(RecordingStore::default());
    let deps = OAuthRouteDeps {
        store: store.clone(),
        ids: Arc::new(FixedSessionIdSource(GENERATED_ID.to_owned())),
        state_source: Arc::new(FixedStateSource(FIXED_CSRF.to_owned())),
        cookie_key: GOOD_KEY.to_vec(),
        license: Arc::new(FakeLicense),
        http: Arc::new(NoExchange),
        users: Arc::new(FakeUsers),
        events: Arc::new(FakeEvents),
        resolvers: Arc::new(OAuthUserResolverRegistry::new()),
        oidc: None,
    };
    let request = build_request(case, &pc);
    let now = OffsetDateTime::from_unix_timestamp(NOW_UNIX).expect("timestamp");
    let response = auth_prefix_response_at(&request, &config, &deps, now)
        .await
        .expect("auth prefix response");

    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let location = normalize_location(
        parts
            .headers
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
    );
    let body_bytes = http_body_util::BodyExt::collect(body)
        .await
        .expect("body")
        .to_bytes();
    let cookie_headers: Vec<String> = parts
        .headers
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok().map(str::to_owned))
        .collect();
    let body = normalize_body(status, &location, &String::from_utf8_lossy(&body_bytes));
    let redis_ops = store.ops.lock().unwrap().clone();
    Output {
        name: case.name.clone(),
        status,
        location,
        content_type: parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
        body,
        cookies: cookie_headers
            .iter()
            .map(|header| normalize_cookie(header, NOW_UNIX))
            .collect(),
        redis_ops,
        provider_count,
        config_problems: Vec::new(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn gitlab_sso_login_diff() {
    if std::env::var("SG_M9JV_SSO_HARNESS").ok().as_deref() != Some("1") {
        eprintln!("SG_M9JV_SSO_HARNESS=1 not set; skipping");
        return;
    }
    let corpus = std::env::var("M9JV_SSO_CORPUS").expect("M9JV_SSO_CORPUS");
    let out = std::env::var("M9JV_SSO_OUT").expect("M9JV_SSO_OUT");
    let rust_dir = Path::new(&out).join("rust");
    std::fs::create_dir_all(&rust_dir).expect("rust dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    for case in &cases {
        let output = run_case(case).await;
        let mut json = serde_json::to_string_pretty(&output).expect("encode");
        json = go_json_escape(&json);
        std::fs::write(
            rust_dir.join(format!("{}.json", output.name)),
            json.as_bytes(),
        )
        .expect("write");
        println!(
            "RUST {} status={} cookies={} ops={} problems={} providers={}",
            output.name,
            output.status,
            output.cookies.len(),
            output.redis_ops.len(),
            output.config_problems.len(),
            output.provider_count
        );
    }
}
