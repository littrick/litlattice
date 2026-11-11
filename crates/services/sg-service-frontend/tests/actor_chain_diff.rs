//! M9.8jc differential of the complete external actor/authz chain and the
//! internal tenant/actor chain against the real Go baseline
//! (`cmd/frontend/internal/httpapi/zz_m9jc_oracle_test.go`, run by
//! `run_actor_chain_diff.sh`).
//!
//! Env:
//!
//! * `M9JC_CORPUS` — `corpus.json` written by the Go oracle
//! * `M9JC_OUT` — output directory (one `<name>.json` per case under `<out>/rust/`)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, Response};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_service_frontend::actor_chain::{
    run_external_chain_at, ChainActor, ChainConfig, ChainDeps, ChainOutcome, ExternalMode,
    LicenseReader, SiteAdminChecker,
};
use sg_service_frontend::actor_middleware::{
    actor_header_reject_response, apply_actor_header, AccessTokenAuth, AccessTokenLookupError,
    AccessTokenSecurityEvent, AccessTokenUser, AccessTokenUserLookup, Actor, ActorHeaderDecision,
};
use sg_service_frontend::authz::{LicenseState, SiteAdminCheck};
use sg_service_frontend::tenant::TenantId;
use sg_session::auth::{SessionStore, SessionStoreError, SessionUser, SessionUserLookup};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct Case {
    name: String,
    mode: String,
    method: String,
    path: String,
    headers: Option<BTreeMap<String, String>>,
    allow: String,
    auth_public: bool,
    dotcom: bool,
    license: String,
    lookup: String,
    lookup_uid: i32,
    lookup_error: String,
    soap_count: usize,
    soap_error: String,
    subject_admin: bool,
    subject_error: bool,
    sudo_user: String,
    sudo_uid: i32,
    sudo_username: String,
    site_admin: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ActorOut {
    uid: i32,
    anonymous_uid: String,
    internal: bool,
    sourcegraph_operator: bool,
    from_session_cookie: bool,
    authenticated: bool,
    is_internal: bool,
}

impl From<&Actor> for ActorOut {
    fn from(actor: &Actor) -> Self {
        Self {
            uid: actor.uid,
            anonymous_uid: actor.anonymous_uid.clone(),
            internal: actor.internal,
            sourcegraph_operator: actor.sourcegraph_operator,
            from_session_cookie: actor.from_session_cookie,
            authenticated: actor.is_authenticated(),
            is_internal: actor.is_internal(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Output {
    name: String,
    mode: String,
    status: u16,
    content_type: String,
    cache_control: String,
    location: String,
    vary: String,
    body: String,
    actor: Option<ActorOut>,
    tenant_set: bool,
    tenant_id: i32,
    lookup_token: String,
    lookup_scope: String,
    lookup_only_admin: bool,
    lookup_called: bool,
    security_events: Vec<String>,
}

impl Output {
    fn empty(case: &Case) -> Self {
        Self {
            name: case.name.clone(),
            mode: case.mode.clone(),
            status: 0,
            content_type: String::new(),
            cache_control: String::new(),
            location: String::new(),
            vary: String::new(),
            body: String::new(),
            actor: None,
            tenant_set: false,
            tenant_id: 0,
            lookup_token: String::new(),
            lookup_scope: String::new(),
            lookup_only_admin: false,
            lookup_called: false,
            security_events: Vec::new(),
        }
    }
}

/// Go's `encoding/json` HTML-escapes `<`, `>` and `&`.
fn go_json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_string(value)
        .expect("encode")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .into_bytes()
}

fn build_headers(case: &Case) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in case.headers.iter().flatten() {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    headers
}

fn build_request(case: &Case) -> Request<()> {
    let method = if case.method.is_empty() {
        "GET"
    } else {
        &case.method
    };
    let path = if case.path.is_empty() {
        "/"
    } else {
        &case.path
    };
    let mut request = Request::builder()
        .method(Method::from_bytes(method.as_bytes()).expect("method"))
        .uri(path)
        .body(())
        .expect("request");
    *request.headers_mut() = build_headers(case);
    request
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

async fn response_parts(
    response: Response<BoxBody>,
) -> (u16, String, String, String, String, String) {
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.expect("body").to_bytes();
    let vary = parts
        .headers
        .get_all(header::VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect::<Vec<_>>()
        .join(",");
    (
        parts.status.as_u16(),
        header_value(&parts.headers, "content-type"),
        header_value(&parts.headers, "cache-control"),
        header_value(&parts.headers, "location"),
        vary,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

fn apply_response_parts(output: &mut Output, parts: (u16, String, String, String, String, String)) {
    output.status = parts.0;
    output.content_type = parts.1;
    output.cache_control = parts.2;
    output.location = parts.3;
    output.vary = parts.4;
    output.body = parts.5;
}

/// The terminal handler both chains reach: `200`, the same body/type the Go
/// oracle's handler writes.
fn terminal(vary: &[&'static str]) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(
        bytes::Bytes::from_static(b"ok"),
    )));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    for value in vary {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static(value));
    }
    response
}

#[derive(Debug, Default)]
struct Capture {
    lookup_token: String,
    lookup_scope: String,
    lookup_only_admin: bool,
    lookup_called: bool,
    security_events: Vec<String>,
}

struct FakeAuth {
    case: Case,
    capture: Mutex<Capture>,
}

#[async_trait]
impl AccessTokenAuth for FakeAuth {
    async fn lookup(
        &self,
        token: &str,
        required_scope: &str,
        only_admin: bool,
    ) -> Result<i32, AccessTokenLookupError> {
        {
            let mut capture = self.capture.lock().unwrap();
            if !capture.lookup_called {
                capture.lookup_token = token.to_owned();
                capture.lookup_scope = required_scope.to_owned();
                capture.lookup_only_admin = only_admin;
            }
            capture.lookup_called = true;
        }
        match self.case.lookup.as_str() {
            "notFound" => Err(AccessTokenLookupError::NotFound),
            "invalid" => Err(AccessTokenLookupError::Invalid),
            "error" => Err(AccessTokenLookupError::Other(
                if self.case.lookup_error.is_empty() {
                    "lookup failed".to_owned()
                } else {
                    self.case.lookup_error.clone()
                },
            )),
            _ => Ok(self.case.lookup_uid),
        }
    }

    async fn count_soap_accounts(&self, _uid: i32) -> Result<usize, String> {
        if self.case.soap_error.is_empty() {
            Ok(self.case.soap_count)
        } else {
            Err(self.case.soap_error.clone())
        }
    }

    async fn is_subject_site_admin(&self, _uid: i32) -> Result<bool, String> {
        if self.case.subject_error {
            Err("db error".to_owned())
        } else {
            Ok(self.case.subject_admin)
        }
    }

    async fn get_user_by_username(&self, _username: &str) -> AccessTokenUserLookup {
        match self.case.sudo_user.as_str() {
            "notFound" => AccessTokenUserLookup::NotFound,
            "error" => AccessTokenUserLookup::Error("sudo lookup failed".to_owned()),
            _ => AccessTokenUserLookup::Found(AccessTokenUser {
                uid: self.case.sudo_uid,
                username: self.case.sudo_username.clone(),
            }),
        }
    }

    async fn get_user_by_id(&self, _uid: i32) -> Option<String> {
        Some("subject".to_owned())
    }

    async fn log_security_event(&self, name: &str) {
        self.capture
            .lock()
            .unwrap()
            .security_events
            .push(name.to_owned());
    }

    async fn security_event(&self, event: AccessTokenSecurityEvent<'_>) {
        self.log_security_event(event.name).await;
    }
}

struct FakeSessionStore;

#[async_trait]
impl SessionStore for FakeSessionStore {
    async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        Err(SessionStoreError(
            "unexpected session store read".to_owned(),
        ))
    }

    async fn save(
        &self,
        _id: &str,
        _payload: Vec<u8>,
        _ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        Err(SessionStoreError(
            "unexpected session store write".to_owned(),
        ))
    }

    async fn delete(&self, _id: &str) -> Result<(), SessionStoreError> {
        Err(SessionStoreError(
            "unexpected session store delete".to_owned(),
        ))
    }
}

struct FakeSessionUsers;

#[async_trait]
impl SessionUserLookup for FakeSessionUsers {
    async fn get(&self, _uid: i32) -> Result<Option<SessionUser>, String> {
        Ok(None)
    }
}

struct FakeLicense(Case);

#[async_trait]
impl LicenseReader for FakeLicense {
    async fn product_license(&self) -> LicenseState {
        match self.0.license.as_str() {
            "error" => LicenseState::Error,
            "expired" => LicenseState::Expired,
            "valid" => LicenseState::Valid,
            _ => LicenseState::None,
        }
    }
}

struct FakeSiteAdmin(Case);

#[async_trait]
impl SiteAdminChecker for FakeSiteAdmin {
    async fn current_user(&self, _actor: &Actor) -> SiteAdminCheck {
        match self.0.site_admin.as_str() {
            "no" => SiteAdminCheck::MustBeSiteAdmin,
            "error" => SiteAdminCheck::Error,
            _ => SiteAdminCheck::IsSiteAdmin,
        }
    }

    async fn subject_is_site_admin(&self, _uid: i32) -> Result<bool, String> {
        if self.0.subject_error {
            Err("db error".to_owned())
        } else {
            Ok(self.0.subject_admin)
        }
    }
}

fn chain_config(case: &Case) -> ChainConfig {
    let allow = match case.allow.as_str() {
        "admin" => "site-admin-create",
        "none" => "none",
        _ => "all-users-create",
    };
    let raw = serde_json::json!({
        "auth.public": case.auth_public,
        "auth.accessTokens": { "allow": allow },
    });
    let site = SiteConfiguration::parse(&raw.to_string()).expect("site config");
    ChainConfig::from_site(&site, case.dotcom)
}

async fn external_output(case: &Case) -> (Output, Capture) {
    let mut output = Output::empty(case);
    let config = chain_config(case);
    let fake = std::sync::Arc::new(FakeAuth {
        case: case.clone(),
        capture: Mutex::new(Capture::default()),
    });
    let deps = ChainDeps::new(
        fake.clone(),
        std::sync::Arc::new(FakeSessionStore),
        std::sync::Arc::new(FakeSessionUsers),
        Vec::new(),
        std::sync::Arc::new(FakeLicense(case.clone())),
        std::sync::Arc::new(FakeSiteAdmin(case.clone())),
    );
    let mode = match case.mode.as_str() {
        "app" => ExternalMode::App,
        _ => ExternalMode::Api,
    };
    let request = build_request(case);
    match run_external_chain_at(&request, mode, &config, &deps, OffsetDateTime::now_utc()).await {
        ChainOutcome::Proceed { actor, vary } => {
            output.actor = Some(ActorOut::from(&actor));
            let response = terminal(&vary);
            apply_response_parts(&mut output, response_parts(response).await);
        }
        ChainOutcome::Reject(response) => {
            apply_response_parts(&mut output, response_parts(response).await);
        }
    }
    let capture = fake.capture.lock().unwrap();
    let capture = Capture {
        lookup_token: capture.lookup_token.clone(),
        lookup_scope: capture.lookup_scope.clone(),
        lookup_only_admin: capture.lookup_only_admin,
        lookup_called: capture.lookup_called,
        security_events: capture.security_events.clone(),
    };
    (output, capture)
}

async fn internal_output(case: &Case) -> Output {
    let mut output = Output::empty(case);
    let mut request = build_request(case);
    if let Err(response) = sg_service_frontend::tenant::internal_tenant_from_header(&mut request) {
        apply_response_parts(&mut output, response_parts(response).await);
        return output;
    }
    if let Some(TenantId(id)) = request.extensions().get::<TenantId>() {
        output.tenant_set = true;
        output.tenant_id = (*id).try_into().unwrap_or_default();
    }
    let actor = match apply_actor_header(request.headers()) {
        ActorHeaderDecision::Proceed(actor) => actor,
        ActorHeaderDecision::Reject { status, body } => {
            let response = actor_header_reject_response(status, body);
            apply_response_parts(&mut output, response_parts(response).await);
            return output;
        }
    };
    request.extensions_mut().insert(ChainActor(actor.clone()));
    output.actor = Some(ActorOut::from(&actor));
    // `featureflag.Middleware`.
    let response = terminal(&["Cookie"]);
    apply_response_parts(&mut output, response_parts(response).await);
    output
}

#[tokio::test]
async fn actor_chain_diff() {
    let Ok(corpus_path) = std::env::var("M9JC_CORPUS") else {
        println!("RUST actor_chain_diff skipped (M9JC_CORPUS not set)");
        return;
    };
    let out = std::env::var("M9JC_OUT").expect("M9JC_OUT");
    let out = Path::new(&out).join("rust");
    std::fs::create_dir_all(&out).expect("create output dir");

    let corpus = std::fs::read(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_slice(&corpus).expect("decode corpus");

    for case in &cases {
        let output = match case.mode.as_str() {
            "internal" => internal_output(case).await,
            _ => {
                let (mut output, capture) = external_output(case).await;
                output.lookup_token = capture.lookup_token;
                output.lookup_scope = capture.lookup_scope;
                output.lookup_only_admin = capture.lookup_only_admin;
                output.lookup_called = capture.lookup_called;
                output.security_events = capture.security_events;
                output
            }
        };
        debug_assert_eq!(output.name, case.name);

        let encoded = go_json(&output);
        std::fs::write(out.join(format!("{}.json", case.name)), encoded).expect("write output");
        println!(
            "RUST {} mode={} status={} body={}",
            output.name,
            output.mode,
            output.status,
            output.body.len()
        );
    }
}
