//! M9.8jq differential of the GitLab OAuth provider against the real Go
//! baseline (`cmd/frontend/internal/auth/gitlaboauth/zz_m9jq_oracle_test.go`,
//! run by `run_gitlab_oauth_resolver_diff.sh`).
//!
//! The Go oracle drives `gitlaboauth.parseConfig` -> `oauth.NewMiddleware` ->
//! the real `gitlabHandler`/`sessionIssuerHelper` with a fake GitLab API
//! (custom `RoundTripper`, no network), an in-process RESP server, a fake
//! `auth.GetAndSaveUser` and mocked user/security-event stores. This harness
//! replays the same corpus through `gitlab_oauth::parse_gitlab_providers`,
//! `gitlab_oauth::handle_gitlab_login`, `oauth_login::handle_callback` and
//! `gitlab_oauth::GitlabUserResolver`, with a fake `GitlabApiClient` and a
//! fake `Provisioner`, writing the same JSON shape for a byte-for-byte `cmp`.
//!
//! Env:
//!
//! * `SG_GITLAB_OAUTH_RESOLVER_HARNESS=1`
//! * `GITLAB_OAUTH_RESOLVER_CORPUS` — `corpus.json` written by the Go oracle
//! * `GITLAB_OAUTH_RESOLVER_OUT` — output directory (one `<name>.json`/case)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use sg_session::gob::decode_session_map;
use sg_session::securecookie::decode_session_id;
use sg_session::{FixedSessionIdSource, SessionStore, SessionStoreError};

use sg_service_frontend::gitlab_oauth::{
    handle_gitlab_login, parse_gitlab_providers, ExternalIdentity, GitlabApiClient,
    GitlabHttpResponse, GitlabOAuthProvider, GitlabProviderSpec, GitlabUserRequest,
    GitlabUserResolver, ProvisionError, ProvisionOutcome, Provisioner,
};
use sg_service_frontend::oauth_login::{
    encode_query_sorted, go_json_escape, go_parse_url, go_url_string, html_escape, parse_query,
    FixedStateSource, GoUrl, LoginState, OAuthCallbackRequest, OAuthFlowDeps, OAuthFlowResponse,
    OAuthHttpClient, OAuthHttpResponse, OAuthLoginRequest, OAuthProviders,
    OAuthSecurityEventRecord, OAuthSecurityEventSink, OAuthToken, OAuthUserLookup, OAuthUserRecord,
    OAuthUserResolver, TokenExchangeRequest, UserResolution,
};

const COOKIE: &str = "sgs";
const GOOD_KEY: &[u8] = b"m9jq-session-key";
const GENERATED_ID: &str = "GENERATEDSESSIONIDGENERATEDSESSIONIDGENERATEDB2";
const NOW_UNIX: i64 = 1_760_000_000;
const FIXED_CSRF: &str = "m9jq-fixed-csrf";
const TOKEN_BODY: &str = r#"{"access_token":"m9jq-access-token","token_type":"bearer"}"#;

// ---------------------------------------------------------------------------
// Corpus (matches the Go oracle's structs).
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct Spec {
    #[serde(default)]
    url: String,
    #[serde(rename = "clientID", default)]
    client_id: String,
    #[serde(rename = "clientSecret", default)]
    client_secret: String,
    #[serde(rename = "apiScope", default)]
    api_scope: String,
    #[serde(rename = "allowSignup", default)]
    allow_signup: Option<bool>,
    #[serde(rename = "allowGroups", default)]
    allow_groups: Vec<String>,
    #[serde(rename = "ssoURL", default)]
    sso_url: String,
}

impl Spec {
    fn to_spec(&self) -> GitlabProviderSpec {
        GitlabProviderSpec {
            url: self.url.clone(),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            api_scope: self.api_scope.clone(),
            allow_signup: self.allow_signup,
            allow_groups: self.allow_groups.clone(),
            sso_url: self.sso_url.clone(),
        }
    }
}

#[derive(Deserialize, Default)]
struct ApiSpec {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: String,
}

#[derive(Deserialize, Default)]
struct ProvisionSpec {
    #[serde(default)]
    mode: String,
    #[serde(rename = "userId", default)]
    user_id: i32,
    #[serde(rename = "safeErrMsg", default)]
    safe_err_msg: String,
}

#[derive(Deserialize, Default)]
struct LoginSpec {
    #[serde(default)]
    skip: bool,
    #[serde(rename = "pcMode", default)]
    pc_mode: String,
    #[serde(default)]
    redirect: String,
    #[serde(default)]
    referer: String,
    #[serde(default)]
    op: String,
}

#[derive(Deserialize, Default)]
struct CallbackSpec {
    #[serde(default)]
    invoke: bool,
    #[serde(default)]
    code: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(rename = "externalURL", default)]
    external_url: String,
    #[serde(default)]
    providers: Vec<Spec>,
    #[serde(default)]
    login: LoginSpec,
    #[serde(default)]
    callback: CallbackSpec,
    #[serde(default)]
    user: ApiSpec,
    #[serde(default)]
    groups: ApiSpec,
    #[serde(default)]
    provision: ProvisionSpec,
    #[serde(rename = "userLookupMissing", default)]
    user_lookup_missing: bool,
    #[serde(rename = "anonymousUid", default)]
    anonymous_uid: Option<String>,
}

// ---------------------------------------------------------------------------
// Output (same field order as the Go structs).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
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
struct ApiRequestOut {
    method: String,
    path: String,
    authorization: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProvisionedOut {
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    login: String,
    email: String,
    #[serde(rename = "emailVerified")]
    email_verified: bool,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "createIfNotExist")]
    create_if_not_exist: bool,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "avatarUrl")]
    avatar_url: String,
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
    #[serde(rename = "apiRequests")]
    api_requests: Vec<ApiRequestOut>,
    provisioned: Vec<ProvisionedOut>,
    #[serde(rename = "configProblems")]
    config_problems: Vec<String>,
    #[serde(rename = "providerCount")]
    provider_count: usize,
}

// ---------------------------------------------------------------------------
// Recording session store; same normalization as oauth_login_diff.rs.
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

// ---------------------------------------------------------------------------
// Fakes.
// ---------------------------------------------------------------------------

struct FakeHttp;

#[async_trait]
impl OAuthHttpClient for FakeHttp {
    async fn exchange_code(
        &self,
        _request: &TokenExchangeRequest,
    ) -> Result<OAuthHttpResponse, String> {
        Ok(OAuthHttpResponse {
            status: 200,
            content_type: "application/json".to_owned(),
            body: TOKEN_BODY.as_bytes().to_vec(),
        })
    }
}

struct FakeApi {
    user: (u16, Vec<u8>),
    groups: (u16, Vec<u8>),
    requests: Mutex<Vec<ApiRequestOut>>,
}

impl FakeApi {
    fn record(&self, request: &GitlabUserRequest) {
        let path = request_path(&request.url);
        self.requests.lock().unwrap().push(ApiRequestOut {
            method: "GET".to_owned(),
            path,
            authorization: format!("Bearer {}", request.access_token),
        });
    }
}

#[async_trait]
impl GitlabApiClient for FakeApi {
    async fn get_user(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String> {
        self.record(request);
        Ok(fake_api_response(self.user.0, &self.user.1))
    }

    async fn list_groups(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String> {
        self.record(request);
        // `ListGroups` pages until an empty page; the corpus groups only have a
        // first page.
        let page = go_parse_url(&request.url)
            .map(|parsed| {
                parse_query(&parsed.raw_query)
                    .into_iter()
                    .find(|(key, _)| key == "page")
                    .map(|(_, value)| value)
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        if page != "1" {
            return Ok(fake_api_response(200, b"[]"));
        }
        Ok(fake_api_response(self.groups.0, &self.groups.1))
    }
}

fn fake_api_response(status: u16, body: &[u8]) -> GitlabHttpResponse {
    GitlabHttpResponse {
        // The Go oracle treats 0 as the default 200.
        status: if status == 0 { 200 } else { status },
        content_type: "application/json".to_owned(),
        body: body.to_vec(),
    }
}

fn request_path(url: &str) -> String {
    let Some(parsed) = go_parse_url(url) else {
        return url.to_owned();
    };
    if parsed.raw_query.is_empty() {
        parsed.path
    } else {
        format!("{}?{}", parsed.path, parsed.raw_query)
    }
}

struct FakeProvisioner {
    mode: String,
    user_id: i32,
    safe_err_msg: String,
    calls: Mutex<Vec<ProvisionedOut>>,
}

#[async_trait]
impl Provisioner for FakeProvisioner {
    async fn get_or_create_user(
        &self,
        identity: &ExternalIdentity,
        create_if_not_exist: bool,
    ) -> Result<ProvisionOutcome, ProvisionError> {
        self.calls.lock().unwrap().push(ProvisionedOut {
            service_type: identity.service_type.clone(),
            service_id: identity.service_id.clone(),
            account_id: identity.account_id.clone(),
            login: identity.login.clone(),
            email: identity.email.clone(),
            email_verified: identity.email_verified,
            display_name: identity.display_name.clone().unwrap_or_default(),
            create_if_not_exist,
            client_id: identity.client_id.clone(),
            avatar_url: identity.avatar_url.clone(),
        });
        match self.mode.as_str() {
            "created" => Ok(ProvisionOutcome::Created {
                user_id: self.user_id,
            }),
            "existing" => Ok(ProvisionOutcome::Existing {
                user_id: self.user_id,
            }),
            "error" => Err(ProvisionError {
                safe_message: self.safe_err_msg.clone(),
            }),
            _ => Err(ProvisionError {
                safe_message: "unknown provision mode".to_owned(),
            }),
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
        // The Go oracle's `types.User` has the zero `CreatedAt`.
        let created_at = OffsetDateTime::from_unix_timestamp(-62_135_596_800).unwrap();
        Ok(OAuthUserRecord {
            id: uid,
            created_at: sg_session::session::GoTime(created_at),
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

/// Used when the corpus yields no provider (login-only config-error cases).
struct UnusedResolver;

#[async_trait]
impl OAuthUserResolver for UnusedResolver {
    fn service_id(&self) -> &str {
        ""
    }

    fn success_event_name(&self) -> &str {
        "GitLabAuthSucceeded"
    }

    fn failure_event_name(&self) -> &str {
        "GitLabAuthFailed"
    }

    async fn get_or_create_user(&self, _token: &OAuthToken) -> UserResolution {
        UserResolution::Failed {
            safe_message: "unused".to_owned(),
        }
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
    if let Some((_, value)) = pairs.iter_mut().find(|(key, _)| key == "redirect") {
        if !value.is_empty() {
            if let Some(mut inner) = go_parse_url(value) {
                let mut inner_pairs = parse_query(&inner.raw_query);
                if let Some((_, inner_value)) =
                    inner_pairs.iter_mut().find(|(key, _)| key == "state")
                {
                    if !inner_value.is_empty() {
                        *inner_value = normalize_encoded_state(inner_value);
                    }
                }
                inner.raw_query = encode_query_sorted(&inner_pairs);
                *value = go_url_string(&inner);
            }
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

fn state_from_location(location: &str) -> Option<String> {
    let parsed = go_parse_url(location)?;
    let pairs = parse_query(&parsed.raw_query);
    if let Some((_, value)) = pairs.iter().find(|(key, _)| key == "state") {
        if !value.is_empty() {
            return Some(value.clone());
        }
    }
    let (_, redirect) = pairs.iter().find(|(key, _)| key == "redirect")?;
    let inner = go_parse_url(redirect)?;
    parse_query(&inner.raw_query)
        .into_iter()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
}

/// Everything recorded during one login or callback phase, plus the parsed
/// config summary.
struct Recorded {
    ops: Vec<Op>,
    events: Vec<SecurityEventOut>,
    api_requests: Vec<ApiRequestOut>,
    provisioned: Vec<ProvisionedOut>,
    config_problems: Vec<String>,
    provider_count: usize,
}

fn output_from(name: &str, response: &OAuthFlowResponse, recorded: Recorded) -> Output {
    let location = normalize_location(&response.location);
    let body = String::from_utf8_lossy(&response.body).into_owned();
    Output {
        name: name.to_owned(),
        status: response.status,
        location: location.clone(),
        content_type: response.content_type.clone(),
        body: normalize_body(response.status, &location, &body),
        cookies: cookies_from(response, NOW_UNIX),
        redis_ops: recorded.ops,
        security_events: recorded.events,
        api_requests: recorded.api_requests,
        provisioned: recorded.provisioned,
        config_problems: recorded.config_problems,
        provider_count: recorded.provider_count,
    }
}

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

#[test]
fn gitlab_oauth_resolver_harness() {
    if std::env::var("SG_GITLAB_OAUTH_RESOLVER_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("GITLAB_OAUTH_RESOLVER_CORPUS").expect("corpus env");
    let out = std::env::var("GITLAB_OAUTH_RESOLVER_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let now = OffsetDateTime::from_unix_timestamp(NOW_UNIX).expect("timestamp");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    for case in cases {
        let specs: Vec<GitlabProviderSpec> = case.providers.iter().map(Spec::to_spec).collect();
        let (parsed, config_problems) = parse_gitlab_providers(&case.external_url, &specs);
        let provider_configs: Vec<_> = parsed
            .iter()
            .map(GitlabOAuthProvider::to_oauth_provider_config)
            .collect();
        let providers = OAuthProviders::from_configs(provider_configs);
        let provider_count = parsed.len();

        let store = Arc::new(RecordingStore::default());
        let ids = FixedSessionIdSource(GENERATED_ID.to_owned());
        let state_source = FixedStateSource(FIXED_CSRF.to_owned());
        let api = Arc::new(FakeApi {
            user: (case.user.status, case.user.body.clone().into_bytes()),
            groups: (case.groups.status, case.groups.body.clone().into_bytes()),
            requests: Mutex::new(Vec::new()),
        });
        let provisioner = Arc::new(FakeProvisioner {
            mode: case.provision.mode.clone(),
            user_id: case.provision.user_id,
            safe_err_msg: case.provision.safe_err_msg.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let users = FakeUsers {
            missing: case.user_lookup_missing,
        };
        let events = Arc::new(FakeEvents::default());

        let resolver: Box<dyn OAuthUserResolver> = match parsed.first() {
            Some(provider) => Box::new(GitlabUserResolver::new(
                provider,
                api.clone(),
                provisioner.clone(),
                false,
            )),
            None => Box::new(UnusedResolver),
        };

        let store_trait: Arc<dyn SessionStore> = store.clone();
        let deps = OAuthFlowDeps {
            providers: &providers,
            store: store_trait.as_ref(),
            ids: &ids,
            cookie_key: GOOD_KEY,
            // `session.setSessionSecureOptions`: the cookie is `Secure`
            // (and `SameSite=None`) iff `externalURL` is https.
            secure: case.external_url.starts_with("https://"),
            external_url: &case.external_url,
            configured_expiry: None,
            license_expired: false,
            state_source: &state_source,
            http: &FakeHttp,
            resolver: resolver.as_ref(),
            users: &users,
            events: events.as_ref(),
            now,
        };

        let mut login_cookie = None;
        let mut login_state = None;
        let mut response = None;

        if !case.login.skip {
            let mut query: Vec<(String, String)> = Vec::new();
            let pc = if case.login.pc_mode == "normal" {
                parsed
                    .first()
                    .map(GitlabOAuthProvider::config_id)
                    .unwrap_or_else(|| "unknown::provider".to_owned())
            } else {
                "unknown::provider".to_owned()
            };
            query.push(("pc".to_owned(), pc));
            if !case.login.redirect.is_empty() {
                query.push(("redirect".to_owned(), case.login.redirect.clone()));
            }
            if !case.login.op.is_empty() {
                query.push(("op".to_owned(), case.login.op.clone()));
            }
            let request = OAuthLoginRequest {
                service_type: "gitlab",
                query: &query,
                referer: if case.login.referer.is_empty() {
                    None
                } else {
                    Some(case.login.referer.as_str())
                },
                session_cookie: None,
            };
            let sso_url = parsed
                .first()
                .map(|provider| provider.sso_url.as_str())
                .unwrap_or("");
            let login_response = runtime.block_on(handle_gitlab_login(&deps, &request, sso_url));
            login_cookie = login_cookie_value(&login_response);
            login_state = state_from_location(&login_response.location);
            response = Some(login_response);
        }

        if !case.callback.invoke {
            let login_response = response.as_ref().expect("login response");
            let output = output_from(
                &case.name,
                login_response,
                Recorded {
                    ops: store.ops.lock().unwrap().clone(),
                    events: events.events.lock().unwrap().clone(),
                    api_requests: api.requests.lock().unwrap().clone(),
                    provisioned: provisioner.calls.lock().unwrap().clone(),
                    config_problems: config_problems.clone(),
                    provider_count,
                },
            );
            write_output(&out, &output);
            continue;
        }

        store.ops.lock().unwrap().clear();
        events.events.lock().unwrap().clear();
        api.requests.lock().unwrap().clear();
        provisioner.calls.lock().unwrap().clear();

        let mut query: Vec<(String, String)> = Vec::new();
        if !case.callback.code.is_empty() {
            query.push(("code".to_owned(), case.callback.code.clone()));
        }
        if let Some(state) = login_state.clone() {
            query.push(("state".to_owned(), state));
        }
        let request = OAuthCallbackRequest {
            service_type: "gitlab",
            path: "/callback",
            query: &query,
            session_cookie: login_cookie.as_deref(),
            anonymous_uid: case.anonymous_uid.as_deref(),
        };
        let callback_response = runtime.block_on(
            sg_service_frontend::oauth_login::handle_callback(&deps, &request),
        );
        let output = output_from(
            &case.name,
            &callback_response,
            Recorded {
                ops: store.ops.lock().unwrap().clone(),
                events: events.events.lock().unwrap().clone(),
                api_requests: api.requests.lock().unwrap().clone(),
                provisioned: provisioner.calls.lock().unwrap().clone(),
                config_problems,
                provider_count,
            },
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
        "RUST {} status={} cookies={} ops={} events={} apis={} provisioned={} problems={} providers={}",
        output.name,
        output.status,
        output.cookies.len(),
        output.redis_ops.len(),
        output.security_events.len(),
        output.api_requests.len(),
        output.provisioned.len(),
        output.config_problems.len(),
        output.provider_count
    );
}
