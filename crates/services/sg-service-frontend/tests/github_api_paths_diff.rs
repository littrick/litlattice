//! M9.36b differential harness for the real GitHub API success paths:
//! * `credential`  — `sg_extsvc::batches_sources::GitHubSource::validate_authenticator`
//!   (`POST {apiRoot}/graphql` `GetAuthenticatedUser`, Bearer auth);
//! * `manifest`    — `sg_service_frontend::githubapp_http_api::convert_github_app_manifest`
//!   (`POST .../app-manifests/{code}/conversions`, require 201, decode);
//! * `installation`— `sg_extsvc::auth::InstallationAuthenticator::refresh`
//!   (`POST {apiRoot}/app/installations/{id}/access_tokens`, RS256 App JWT).
//!
//! Mirrors the UNTRACKED gated Go oracles under
//! `internal/batches/sources`, `cmd/frontend/internal/githubapp` and
//! `internal/github_apps/auth`. Both sides consume
//! `test_ws/p9-go/data/github_api_paths/corpus.json` (with `{{nonce}}`
//! already substituted) and write one normalized JSON document per case; the
//! shell script compares them case-by-case. No network: each case's recorded
//! response is served by a capturing [`HttpDoer`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::auth::{
    GitHubAppAuthenticator, InstallationAuthenticator, InstallationTokenCache,
};
use sg_extsvc::batches_sources::{GitHubSource, SourceAuth};
use sg_extsvc::github::api_root;
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};
use sg_service_frontend::githubapp_http_api::{
    convert_github_app_manifest, HttpDoer as ManifestHttpDoer, NewGitHubApp,
};

#[derive(Debug, Deserialize)]
struct Corpus {
    installation_private_key: String,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Default)]
struct Case {
    kind: String,
    name: String,
    #[serde(default)]
    base_url: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    conversion_path: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    app_kind: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    installation_id: i32,
    #[serde(default)]
    app_id: i32,
    #[serde(default)]
    second_refresh: bool,
    response: Response,
}

#[derive(Debug, Deserialize, Default)]
struct Response {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    headers: HashMap<String, String>,
}

impl Response {
    fn body_bytes(&self) -> Vec<u8> {
        match &self.body {
            Some(value) => serde_json::to_vec(value).expect("serialize response body"),
            None => Vec::new(),
        }
    }

    fn header_pairs(&self) -> Vec<(String, String)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
struct Seen {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

struct RecordingDoer {
    response: Response,
    seen: Mutex<Option<Seen>>,
    fail: bool,
    calls: Mutex<usize>,
}

impl RecordingDoer {
    fn new(response: Response) -> Self {
        Self {
            response,
            seen: Mutex::new(None),
            fail: false,
            calls: Mutex::new(0),
        }
    }

    fn failing() -> Self {
        Self {
            response: Response::default(),
            seen: Mutex::new(None),
            fail: true,
            calls: Mutex::new(0),
        }
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl HttpDoer for RecordingDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        *self.calls.lock().unwrap() += 1;
        if self.fail {
            return Err("unexpected HTTP request (cache miss)".to_owned());
        }
        *self.seen.lock().unwrap() = Some(Seen {
            method: request.method,
            url: request.url,
            headers: request.headers,
            body: request.body,
        });
        Ok(HttpResponse {
            status: self.response.status,
            headers: self.response.header_pairs(),
            body: self.response.body_bytes(),
        })
    }
}

struct ManifestRecording {
    response: Response,
    seen: Mutex<Option<String>>,
}

impl ManifestHttpDoer for ManifestRecording {
    fn post(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        *self.seen.lock().unwrap() = Some(url.to_owned());
        Ok((self.response.status, self.response.body_bytes()))
    }
}

#[derive(Default)]
struct MemCache {
    values: Mutex<HashMap<String, Vec<u8>>>,
    keys: Mutex<Vec<String>>,
}

impl InstallationTokenCache for MemCache {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.values.lock().unwrap().get(key).cloned()
    }

    fn set(&self, key: &str, value: Vec<u8>) {
        self.keys.lock().unwrap().push(key.to_owned());
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), value);
    }
}

/// Replace the scheme+authority of any URL in `value` with `<base>`.
fn normalize_host(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(index) = find_scheme(rest) {
        out.push_str(&rest[..index]);
        let after_scheme = &rest[index..];
        let authority_end = after_scheme[index_of_slash(after_scheme)..]
            .find('/')
            .map(|offset| index_of_slash(after_scheme) + offset)
            .unwrap_or(after_scheme.len());
        out.push_str("<base>");
        rest = &after_scheme[authority_end..];
    }
    out.push_str(rest);
    out
}

fn find_scheme(value: &str) -> Option<usize> {
    value.find("http://").or_else(|| value.find("https://"))
}

fn index_of_slash(value: &str) -> usize {
    value.find("://").map(|index| index + 3).unwrap_or(0)
}

fn path_and_query(raw: &str) -> (String, String) {
    match url::Url::parse(raw) {
        Ok(url) => (
            url.path().to_owned(),
            url.query().unwrap_or_default().to_owned(),
        ),
        Err(_) => (raw.to_owned(), String::new()),
    }
}

fn selected_headers(headers: &[(String, String)]) -> Map<String, Value> {
    let mut map = Map::new();
    for name in [
        "authorization",
        "accept",
        "content-type",
        "cache-control",
    ] {
        let value = headers
            .iter()
            .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        map.insert(name.to_owned(), Value::String(value));
    }
    map
}

fn decode_base64url(segment: &str) -> Option<Vec<u8>> {
    let table = |byte: u8| -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    };
    let mut bits = 0u32;
    let mut count = 0u8;
    let mut out = Vec::new();
    for byte in segment.bytes() {
        if byte == b'=' {
            break;
        }
        let value = table(byte)?;
        bits = (bits << 6) | u32::from(value);
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
            bits &= (1 << count) - 1;
        }
    }
    Some(out)
}

fn jwt_info(headers: &[(String, String)]) -> Value {
    let authorization = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.as_str())
        .unwrap_or_default();
    let Some(token) = authorization.strip_prefix("Bearer ") else {
        return json!({});
    };
    let mut parts = token.split('.');
    let (Some(header_b64), Some(payload_b64), Some(_)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return json!({});
    };
    let Some(header_bytes) = decode_base64url(header_b64) else {
        return json!({});
    };
    let Some(payload_bytes) = decode_base64url(payload_b64) else {
        return json!({});
    };
    let header: Value = serde_json::from_slice(&header_bytes).unwrap_or(Value::Null);
    let payload: Value = serde_json::from_slice(&payload_bytes).unwrap_or(Value::Null);
    let iat = payload.get("iat").and_then(Value::as_i64);
    let exp = payload.get("exp").and_then(Value::as_i64);
    json!({
        "scheme": "Bearer",
        "alg": header.get("alg").cloned().unwrap_or(Value::Null),
        "typ": header.get("typ").cloned().unwrap_or(Value::Null),
        "iss": payload.get("iss").cloned().unwrap_or(Value::Null),
        "iat_present": payload.get("iat").is_some(),
        "exp_present": payload.get("exp").is_some(),
        "lifetime": match (iat, exp) { (Some(iat), Some(exp)) => json!(exp - iat), _ => Value::Null },
    })
}

fn app_json(app: &NewGitHubApp) -> Value {
    json!({
        "app_id": app.app_id,
        "name": app.name,
        "slug": app.slug,
        "client_id": app.client_id,
        "client_secret": app.client_secret,
        "webhook_secret": app.webhook_secret,
        "private_key": app.private_key,
        "base_url": app.base_url,
        "app_url": app.app_url,
        "domain": app.domain,
        "kind": app.kind,
        "logo": app.logo,
        "created_by_user_id": app.created_by_user_id,
    })
}

async fn run_credential(case: &Case) -> Value {
    let base_url = url::Url::parse(&case.base_url).expect("base_url");
    let (api_url, _) = api_root(&base_url);
    let doer = Arc::new(RecordingDoer::new(Response {
        status: case.response.status,
        body: case.response.body.clone(),
        headers: case.response.headers.clone(),
    }));
    let source = GitHubSource::new(
        api_url,
        SourceAuth::OAuthToken(case.token.clone()),
        Arc::clone(&doer) as Arc<dyn HttpDoer>,
    );
    let result = match source.validate_authenticator().await {
        Ok(()) => json!({"ok": true}),
        Err(error) => json!({"error": normalize_host(&error.to_string())}),
    };
    let seen = doer.seen.lock().unwrap().clone().unwrap_or_default();
    let (path, query) = path_and_query(&seen.url);
    let body = if seen.body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&seen.body).unwrap_or(Value::Null)
    };
    json!({
        "request": {
            "method": seen.method,
            "path": path,
            "query": query,
            "headers": selected_headers(&seen.headers),
            "body": body,
        },
        "result": result,
    })
}

fn run_manifest(case: &Case) -> Value {
    let http = ManifestRecording {
        response: Response {
            status: case.response.status,
            body: case.response.body.clone(),
            headers: case.response.headers.clone(),
        },
        seen: Mutex::new(None),
    };
    let conversion_url = format!("https://ghe.example.com{}", case.conversion_path);
    let outcome = convert_github_app_manifest(
        &http,
        &conversion_url,
        &case.domain,
        &case.app_kind,
        case.user_id,
    );
    let result = match outcome {
        Ok(app) => json!({"ok": true, "app": app_json(&app)}),
        Err(error) => json!({"error": normalize_host(&error)}),
    };
    let (path, _) = path_and_query(&http.seen.lock().unwrap().clone().unwrap_or_default());
    json!({
        "request": {
            "method": "POST",
            "path": path,
            "content_type": "",
            "body_len": 0,
        },
        "result": result,
    })
}

async fn run_installation(case: &Case, key: &str) -> Value {
    let base_url = url::Url::parse(&case.base_url).expect("base_url");
    let app = Arc::new(
        GitHubAppAuthenticator::new(case.app_id, key.to_owned()).expect("valid app key"),
    );
    let cache = Arc::new(MemCache::default());
    let recording = Arc::new(RecordingDoer::new(Response {
        status: case.response.status,
        body: case.response.body.clone(),
        headers: case.response.headers.clone(),
    }));
    let doer: Arc<dyn HttpDoer> = Arc::clone(&recording) as Arc<dyn HttpDoer>;
    let mut authenticator = InstallationAuthenticator::new(
        base_url.clone(),
        case.installation_id,
        Arc::clone(&app),
        Some(Arc::clone(&cache) as Arc<dyn InstallationTokenCache>),
        doer,
    );
    let result = match authenticator.refresh().await {
        Ok(()) => {
            let token = authenticator.get_token();
            let raw_expires_at = case
                .response
                .body
                .as_ref()
                .and_then(|body| body.get("expires_at"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let expires_at = if token.expires_at.is_some() {
                raw_expires_at.to_owned()
            } else {
                String::new()
            };
            json!({"ok": true, "token": token.token, "expires_at": expires_at})
        }
        Err(error) => json!({"error": normalize_host(&error)}),
    };
    let seen = recording.seen.lock().unwrap().clone().unwrap_or_default();
    let (path, _) = path_and_query(&seen.url);
    let cache_keys = cache.keys.lock().unwrap().clone();
    let cache_base = base_url.to_string();
    let recorded = !cache_keys.is_empty();
    let last_key = cache_keys.last().cloned().unwrap_or_default();
    let prefix_matches = recorded && last_key.starts_with(&cache_base);
    let suffix = if recorded {
        last_key.strip_prefix(&cache_base).unwrap_or("").to_owned()
    } else {
        String::new()
    };
    let mut output = json!({
        "request": {
            "method": seen.method,
            "path": path,
            "body_len": seen.body.len(),
            "jwt": jwt_info(&seen.headers),
        },
        "cache_key": {
            "recorded": recorded,
            "prefix_matches_base": prefix_matches,
            "suffix": suffix,
        },
        "result": result,
    });
    if case.second_refresh {
        let failing_record = Arc::new(RecordingDoer::failing());
        let failing: Arc<dyn HttpDoer> =
            Arc::clone(&failing_record) as Arc<dyn HttpDoer>;
        let mut second = InstallationAuthenticator::new(
            base_url,
            case.installation_id,
            app,
            Some(Arc::clone(&cache) as Arc<dyn InstallationTokenCache>),
            failing,
        );
        let second_ok = second.refresh().await.is_ok();
        output["second_refresh"] = json!({
            "ok": second_ok,
            "requests": failing_record.calls(),
        });
    }
    output
}

#[tokio::test]
async fn github_api_paths_diff() {
    let (Ok(corpus_path), Ok(out)) = (
        std::env::var("M936B_CORPUS"),
        std::env::var("M936B_OUT"),
    ) else {
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    for case in &corpus.cases {
        let document = match case.kind.as_str() {
            "credential" => run_credential(case).await,
            "manifest" => run_manifest(case),
            "installation" => run_installation(case, &corpus.installation_private_key).await,
            other => panic!("unknown case kind {other}"),
        };
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            serde_json::to_vec_pretty(&document).expect("serialize output"),
        )
        .expect("write rust output");
    }
}
