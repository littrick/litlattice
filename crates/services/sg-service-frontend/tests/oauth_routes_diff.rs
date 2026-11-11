//! M9.8jo differential of the OAuth route mounting/auto-redirect against the
//! real Go baseline (`cmd/frontend/internal/auth/oauth/zz_m9jo_oracle_test.go`,
//! run by `run_oauth_routes_diff.sh`).
//!
//! The Go oracle composes the real `oauth.NewMiddleware`s (GitHub, GitLab,
//! OpenID Connect prefixes) over a `418 NEXT` handler, adds the outer
//! `AccessTokenAuth`/`CookieMiddleware` `Vary` values, and drives the corpus
//! through `httptest`. This harness replays the same corpus through
//! [`oauth_routes::auth_prefix_response`] (flow delegation) and
//! [`oauth_routes::single_provider_redirect`] (auto-redirect), with a
//! recording session store, fixed CSRF/session-ID sources and a fixed clock,
//! then writes the same normalized JSON for a byte-for-byte `cmp`.
//!
//! Env:
//!
//! * `SG_OAUTH_ROUTES_HARNESS=1`
//! * `OAUTH_ROUTES_CORPUS` — `corpus.json` written by the Go oracle
//! * `OAUTH_ROUTES_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{header, HeaderName, HeaderValue, Method, Request};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_session::{FixedSessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::actor_chain::LicenseReader;
use sg_service_frontend::authz::{html_escape, LicenseState};
use sg_service_frontend::oauth_login::{
    encode_query_sorted, go_json_escape, go_parse_url, go_url_string, parse_query,
    FixedStateSource, GoUrl, LoginState, OAuthSecurityEventRecord, OAuthSecurityEventSink,
    OAuthUserLookup, OAuthUserRecord,
};
use sg_service_frontend::oauth_routes::{
    auth_prefix_response_at, single_provider_redirect, OAuthRouteConfig, OAuthRouteDeps,
    OAuthUserResolverRegistry,
};

const GOOD_KEY: &[u8] = b"m9jo-session-key";
const GENERATED_ID: &str = "M9JOGENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const FIXED_STATE: &str = "m9jo-fixed-state";
const NOW_UNIX: i64 = 1_760_000_000;

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default = "default_method")]
    method: String,
    path: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    referer: String,
    #[serde(rename = "userAgent", default)]
    user_agent: String,
    #[serde(default)]
    cookie: String,
    #[serde(default)]
    authenticated: bool,
    #[serde(default)]
    dotcom: bool,
    /// API listener: the provider middlewares are never mounted there.
    #[serde(default)]
    api: bool,
    /// Raw flat site configuration (the rewrite's internal `{"type": …}` form).
    site: String,
    #[serde(default)]
    state: Option<StateSpec>,
    #[serde(rename = "rawState", default)]
    raw_state: String,
}

fn default_method() -> String {
    "GET".to_owned()
}

#[derive(Deserialize)]
struct StateSpec {
    #[serde(rename = "Redirect", default)]
    redirect: String,
    #[serde(rename = "ProviderID", default)]
    provider_id: String,
    #[serde(rename = "CSRF", default)]
    csrf: String,
    #[serde(rename = "Op", default)]
    op: String,
}

#[derive(Serialize)]
struct Output {
    name: String,
    status: u16,
    #[serde(rename = "contentType")]
    content_type: String,
    location: String,
    body: String,
    cookies: Vec<String>,
    vary: Vec<String>,
    nosniff: String,
}

#[derive(Default)]
struct RecordingStore {
    data: Mutex<BTreeMap<String, Vec<u8>>>,
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
        _ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        self.data.lock().unwrap().insert(id.to_owned(), payload);
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
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

fn build_request(case: &Case) -> Request<()> {
    let uri = if case.query.is_empty() {
        case.path.clone()
    } else {
        format!("{}?{}", case.path, case.query)
    };
    let mut builder = Request::builder()
        .method(Method::from_bytes(case.method.as_bytes()).unwrap_or(Method::GET))
        .uri(uri);
    if !case.referer.is_empty() {
        builder = builder.header(header::REFERER, &case.referer);
    }
    if !case.user_agent.is_empty() {
        builder = builder.header(header::USER_AGENT, &case.user_agent);
    }
    if !case.cookie.is_empty() {
        builder = builder.header(header::COOKIE, &case.cookie);
    }
    builder.body(()).expect("request")
}

fn state_param(case: &Case) -> Option<String> {
    if !case.raw_state.is_empty() {
        return Some(case.raw_state.clone());
    }
    case.state.as_ref().map(|state| {
        LoginState {
            redirect: state.redirect.clone(),
            provider_id: state.provider_id.clone(),
            csrf: state.csrf.clone(),
            op: state.op.clone(),
        }
        .encode()
    })
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
            *value = "<state>".to_owned();
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

fn normalize_cookie(header: &str) -> String {
    let mut parts = header.split("; ");
    let first = parts.next().unwrap_or("");
    let (name, _value) = first.split_once('=').unwrap_or((first, ""));
    let mut out = Vec::new();
    if name == "sgs" {
        out.push("sgs=<generated>".to_owned());
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

fn header_value(headers: &http::HeaderMap, name: HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn header_values(headers: &http::HeaderMap, name: HeaderName) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok().map(str::to_owned))
        .collect()
}

/// The `418 NEXT` handler used by the Go oracle. It calls `WriteHeader` before
/// writing the body, so no `Content-Type` is sniffed.
fn next_response() -> http::Response<BoxBody> {
    let mut response = http::Response::new(tonic::body::boxed(http_body_util::Full::new(
        bytes::Bytes::from_static(b"NEXT"),
    )));
    *response.status_mut() = http::StatusCode::IM_A_TEAPOT;
    response
}

/// The App listener's outer `AccessTokenAuthMiddleware` and
/// `session.CookieMiddleware` add their `Vary` values before the provider
/// middlewares run, so they are present on passthrough responses too.
fn append_outer_vary(mut response: http::Response<BoxBody>) -> http::Response<BoxBody> {
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Authorization"));
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Cookie"));
    response
}

async fn run_case(case: &Case) -> Output {
    // The App-only route takeover is not installed on the API listener
    // (`middleware.rs` gates the dispatch on `ExternalMode::App`).
    if case.api {
        return output_for(case, next_response()).await;
    }
    let site = sg_config::SiteConfiguration::parse(&case.site).expect("site configuration");
    let config = OAuthRouteConfig::from_site(&site, case.dotcom);
    let deps = OAuthRouteDeps {
        store: Arc::new(RecordingStore::default()),
        ids: Arc::new(FixedSessionIdSource(GENERATED_ID.to_owned())),
        state_source: Arc::new(FixedStateSource(FIXED_STATE.to_owned())),
        cookie_key: GOOD_KEY.to_vec(),
        license: Arc::new(FakeLicense),
        http: Arc::new(crate::NoExchange),
        users: Arc::new(FakeUsers),
        events: Arc::new(FakeEvents),
        resolvers: Arc::new(OAuthUserResolverRegistry::new()),
        // M9.8jo pins the pre-M9.8js dispatch; the OIDC middleware has its own
        // differential (`run_oidc_middleware_diff.sh`).
        oidc: None,
    };
    let mut request = build_request(case);
    if let Some(state) = state_param(case) {
        request = with_state(request, case, &state);
    }
    let now = OffsetDateTime::from_unix_timestamp(NOW_UNIX).expect("timestamp");
    let response = match auth_prefix_response_at(&request, &config, &deps, now).await {
        Some(response) => response,
        None => match single_provider_redirect(&request, &config, case.authenticated) {
            Some(response) => response,
            None => append_outer_vary(next_response()),
        },
    };
    output_for(case, response).await
}

/// Normalize one response exactly like the Go oracle's `m9joRunCase`.
async fn output_for(case: &Case, response: http::Response<BoxBody>) -> Output {
    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let location = normalize_location(&header_value(&parts.headers, header::LOCATION));
    let body_bytes = http_body_util::BodyExt::collect(body)
        .await
        .expect("body")
        .to_bytes();
    let body = normalize_body(status, &location, &String::from_utf8_lossy(&body_bytes));
    Output {
        name: case.name.clone(),
        status,
        content_type: header_value(&parts.headers, header::CONTENT_TYPE),
        location,
        body,
        cookies: header_values(&parts.headers, header::SET_COOKIE)
            .iter()
            .map(|header| normalize_cookie(header))
            .collect(),
        vary: header_values(&parts.headers, header::VARY),
        nosniff: header_value(&parts.headers, header::X_CONTENT_TYPE_OPTIONS),
    }
}

/// Rebuild the request with the callback's `state` query parameter appended.
fn with_state(request: Request<()>, case: &Case, state: &str) -> Request<()> {
    let uri = format!(
        "{}?{}",
        case.path,
        if case.query.is_empty() {
            format!(
                "state={}",
                sg_service_frontend::oauth_login::query_escape(state)
            )
        } else {
            format!(
                "{}&state={}",
                case.query,
                sg_service_frontend::oauth_login::query_escape(state)
            )
        }
    );
    let (mut parts, ()) = request.into_parts();
    parts.uri = uri.parse().expect("callback uri");
    Request::from_parts(parts, ())
}

struct NoExchange;

#[async_trait]
impl sg_service_frontend::oauth_login::OAuthHttpClient for NoExchange {
    async fn exchange_code(
        &self,
        _request: &sg_service_frontend::oauth_login::TokenExchangeRequest,
    ) -> Result<sg_service_frontend::oauth_login::OAuthHttpResponse, String> {
        Err("no token exchange in route differential".to_owned())
    }
}

fn go_escape(input: &str) -> String {
    go_json_escape(input)
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
        "RUST {} status={} vary={} cookies={}",
        output.name,
        output.status,
        output.vary.len(),
        output.cookies.len()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn oauth_routes_diff() {
    if std::env::var("SG_OAUTH_ROUTES_HARNESS").ok().as_deref() != Some("1") {
        eprintln!("SG_OAUTH_ROUTES_HARNESS=1 not set; skipping");
        return;
    }
    let corpus_path = std::env::var("OAUTH_ROUTES_CORPUS").expect("OAUTH_ROUTES_CORPUS");
    let out_dir = std::env::var("OAUTH_ROUTES_OUT").expect("OAUTH_ROUTES_OUT");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    let raw = std::fs::read_to_string(&corpus_path).expect("corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("corpus json");
    let mut count = 0;
    for case in &cases {
        let output = run_case(case).await;
        write_output(&out_dir, &output);
        count += 1;
    }
    println!("RUST_CASES {count}");
}
