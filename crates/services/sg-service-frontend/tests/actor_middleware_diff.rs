//! M9.8ja differential of the external HTTP chain's actor/authz semantics
//! against the real Go baseline
//! (`cmd/frontend/internal/httpapi/zz_m9ja_oracle_test.go`, run by
//! `run_actor_middleware_diff.sh`).
//!
//! Corpus kinds (defined by the Go oracle):
//!
//! * `actor_http` — `actor.HTTPMiddleware`;
//! * `anonymous_uid` — `actor.AnonymousUIDMiddleware`;
//! * `transport` — `actor.HTTPTransport`;
//! * `access_token` — `httpapi.AccessTokenAuthMiddleware`;
//! * `post_auth` — `cmd/frontend/internal/authz.PostAuthMiddleware`.
//!
//! Env:
//!
//! * `M9JA_CORPUS` — `corpus.json` written by the Go oracle
//! * `M9JA_OUT` — output directory (one `<name>.json` per case under `<out>/rust/`)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use base64::Engine;
use http::{header, HeaderMap, HeaderName, HeaderValue, Response};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use tonic::body::BoxBody;

use sg_service_frontend::actor_middleware::{
    actor_header_reject_response, apply_access_token, apply_actor_header,
    apply_anonymous_uid_middleware, basic_auth_username, transport_headers, AccessTokenAuth,
    AccessTokenConfig, AccessTokenDecision, AccessTokenInput, AccessTokenLookupError,
    AccessTokenUser, AccessTokenUserLookup, AccessTokensAllow, Actor, ActorHeaderDecision,
    LicenseForToken,
};
use sg_service_frontend::authz::{
    authorization_header_value, post_auth_middleware, post_auth_response, LicenseState,
    PostAuthDecision, SiteAdminCheck,
};

const ACTOR_UID_HEADER: &str = "x-sourcegraph-actor-uid";
const ANONYMOUS_UID_HEADER: &str = "x-sourcegraph-actor-anonymous-uid";

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct Case {
    name: String,
    kind: String,
    /// Go marshals a nil `map[string]string` as `null`.
    headers: Option<BTreeMap<String, String>>,
    existing_uid: i32,
    existing_anonymous_uid: String,
    existing_internal: bool,
    existing_sourcegraph_operator: bool,
    path: String,
    query_token: String,
    basic_auth_user: String,
    authorization: String,
    allow: String,
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
    actor_uid: i32,
    actor_internal: bool,
    site_admin: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct HeaderOut {
    name: String,
    value: String,
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
    kind: String,
    status: u16,
    content_type: String,
    cache_control: String,
    vary: String,
    body: String,
    lookup_token: String,
    lookup_scope: String,
    lookup_only_admin: bool,
    lookup_called: bool,
    security_events: Vec<String>,
    actor: Option<ActorOut>,
    headers: Vec<HeaderOut>,
}

impl Output {
    fn empty(case: &Case) -> Self {
        Self {
            name: case.name.clone(),
            kind: case.kind.clone(),
            status: 0,
            content_type: String::new(),
            cache_control: String::new(),
            vary: String::new(),
            body: String::new(),
            lookup_token: String::new(),
            lookup_scope: String::new(),
            lookup_only_admin: false,
            lookup_called: false,
            security_events: Vec::new(),
            actor: None,
            headers: Vec::new(),
        }
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

fn build_headers(case: &Case) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in case.headers.iter().flatten() {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    if !case.authorization.is_empty() {
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&case.authorization).expect("authorization"),
        );
    }
    if !case.basic_auth_user.is_empty() {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{}:", case.basic_auth_user));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).expect("basic authorization"),
        );
    }
    headers
}

fn existing_actor(case: &Case) -> Actor {
    Actor {
        uid: case.existing_uid,
        anonymous_uid: case.existing_anonymous_uid.clone(),
        internal: case.existing_internal,
        sourcegraph_operator: case.existing_sourcegraph_operator,
        from_session_cookie: false,
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

async fn response_parts(response: Response<BoxBody>) -> (u16, String, String, String, String) {
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
        vary,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

fn apply_response_parts(output: &mut Output, parts: (u16, String, String, String, String)) {
    output.status = parts.0;
    output.content_type = parts.1;
    output.cache_control = parts.2;
    output.vary = parts.3;
    output.body = parts.4;
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

    async fn is_subject_site_admin(&self, _subject_uid: i32) -> Result<bool, String> {
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

    async fn log_security_event(&self, name: &str) {
        self.capture
            .lock()
            .unwrap()
            .security_events
            .push(name.to_owned());
    }
}

fn license_for_token(case: &Case) -> LicenseForToken {
    match case.license.as_str() {
        "error" => LicenseForToken::Error,
        "expired" => LicenseForToken::Ok { expired: true },
        _ => LicenseForToken::Ok { expired: false },
    }
}

fn allow_for(case: &Case) -> AccessTokensAllow {
    match case.allow.as_str() {
        "admin" => AccessTokensAllow::Admin,
        "none" => AccessTokensAllow::None,
        _ => AccessTokensAllow::All,
    }
}

async fn access_token_output(case: &Case) -> Output {
    let mut output = Output::empty(case);
    let headers = build_headers(case);
    let path = if case.path.is_empty() {
        "/".to_owned()
    } else {
        case.path.clone()
    };
    let authorization = authorization_header_value(&headers, &path);
    let basic_user = basic_auth_username(&headers);
    let query_token = if case.query_token.is_empty() {
        None
    } else {
        Some(case.query_token.as_str())
    };
    let input = AccessTokenInput {
        path: &path,
        query_token,
        basic_auth_username: basic_user.as_deref(),
        authorization: authorization.as_deref(),
    };
    let fake = FakeAuth {
        case: case.clone(),
        capture: Mutex::new(Capture::default()),
    };

    let decision = apply_access_token(
        input,
        AccessTokenConfig {
            allow: allow_for(case),
            dotcom: case.dotcom,
        },
        license_for_token(case),
        existing_actor(case),
        &fake,
    )
    .await;

    match decision {
        AccessTokenDecision::Proceed { actor, vary } => {
            output.status = 200;
            output.vary = if vary {
                "Authorization".to_owned()
            } else {
                String::new()
            };
            output.actor = Some(ActorOut::from(&actor));
        }
        AccessTokenDecision::Respond(response) => {
            let parts = response_parts(response).await;
            apply_response_parts(&mut output, parts);
        }
    }

    let capture = fake.capture.lock().unwrap();
    output.lookup_token = capture.lookup_token.clone();
    output.lookup_scope = capture.lookup_scope.clone();
    output.lookup_only_admin = capture.lookup_only_admin;
    output.lookup_called = capture.lookup_called;
    output.security_events = capture.security_events.clone();
    output
}

async fn post_auth_output(case: &Case) -> Output {
    let mut output = Output::empty(case);
    let actor = Actor {
        uid: case.actor_uid,
        internal: case.actor_internal,
        ..Actor::default()
    };
    let license = match case.license.as_str() {
        "error" => LicenseState::Error,
        "expired" => LicenseState::Expired,
        "valid" => LicenseState::Valid,
        _ => LicenseState::None,
    };
    let site_admin = if case.actor_internal {
        SiteAdminCheck::IsSiteAdmin
    } else {
        match case.site_admin.as_str() {
            "no" => SiteAdminCheck::MustBeSiteAdmin,
            "error" => SiteAdminCheck::Error,
            _ => SiteAdminCheck::IsSiteAdmin,
        }
    };
    match post_auth_middleware(&actor, license, site_admin) {
        PostAuthDecision::Proceed => {
            output.status = 200;
            output.content_type = "text/plain; charset=utf-8".to_owned();
            output.body = "ok".to_owned();
        }
        decision @ PostAuthDecision::Reject { .. } => {
            let response = post_auth_response(decision).expect("rejection response");
            let parts = response_parts(response).await;
            apply_response_parts(&mut output, parts);
        }
    }
    output
}

#[tokio::test]
async fn actor_middleware_diff() {
    let Ok(corpus_path) = std::env::var("M9JA_CORPUS") else {
        println!("RUST actor_middleware_diff skipped (M9JA_CORPUS not set)");
        return;
    };
    let out = std::env::var("M9JA_OUT").expect("M9JA_OUT");
    let out = Path::new(&out).join("rust");
    std::fs::create_dir_all(&out).expect("create output dir");

    let corpus = std::fs::read(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_slice(&corpus).expect("decode corpus");

    for case in &cases {
        let mut output = Output::empty(case);
        match case.kind.as_str() {
            "actor_http" => {
                let headers = build_headers(case);
                match apply_actor_header(&headers) {
                    ActorHeaderDecision::Proceed(actor) => {
                        output.status = 200;
                        output.actor = Some(ActorOut::from(&actor));
                    }
                    ActorHeaderDecision::Reject { status, body } => {
                        let response = actor_header_reject_response(status, body);
                        let parts = response_parts(response).await;
                        apply_response_parts(&mut output, parts);
                    }
                }
            }
            "anonymous_uid" => {
                let headers = build_headers(case);
                let actor = apply_anonymous_uid_middleware(&headers, existing_actor(case));
                output.status = 200;
                output.actor = Some(ActorOut::from(&actor));
            }
            "transport" => {
                let actor = existing_actor(case);
                let headers = transport_headers(&actor);
                let value = |name: &str| {
                    headers
                        .iter()
                        .find(|(header, _)| header == name)
                        .map(|(_, value)| value.clone())
                        .unwrap_or_default()
                };
                output.headers = vec![
                    HeaderOut {
                        name: ACTOR_UID_HEADER.to_owned(),
                        value: value(ACTOR_UID_HEADER),
                    },
                    HeaderOut {
                        name: ANONYMOUS_UID_HEADER.to_owned(),
                        value: value(ANONYMOUS_UID_HEADER),
                    },
                ];
            }
            "access_token" => output = access_token_output(case).await,
            "post_auth" => output = post_auth_output(case).await,
            other => panic!("unknown kind {other}"),
        }

        let encoded = go_json(&output);
        std::fs::write(out.join(format!("{}.json", case.name)), encoded).expect("write output");
        println!(
            "RUST {} kind={} status={} body={}",
            output.name,
            output.kind,
            output.status,
            output.body.len()
        );
    }
}
