//! Shared plumbing for the M9.14a package-source differential harnesses.
//!
//! Each `<kind>_packages_source_diff.rs` test replays
//! `test_ws/p9-go/data/<kind>_packages_source/corpus.json` against the Rust
//! port with an injectable [`HttpDoer`] and writes one envelope per case under
//! `<M914A_OUT>/rust/`. The untracked Go oracle
//! (`internal/repos/zz_m9_14a_packages_oracle_test.go`) writes `<M914A_OUT>/go/`
//! and `run_<kind>_packages_source_diff.sh` compares them.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse, SourceService};

#[derive(Debug, Clone, Deserialize)]
pub struct Case {
    pub name: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub responses: Vec<Fixture>,
    #[serde(default)]
    pub check: bool,
    /// Run the source's configuration-dependency enumeration (default true).
    #[serde(default = "default_true")]
    pub source: bool,
    #[serde(default)]
    pub ops: Vec<Op>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Op {
    pub op: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub module: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fixture {
    pub method: String,
    #[serde(rename = "requestUri")]
    pub request_uri: String,
    #[serde(rename = "bodyContains", default)]
    pub body_contains: String,
    #[serde(default)]
    pub status: i64,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: String,
}

#[derive(Debug, Default)]
pub struct FakeState {
    used: Vec<bool>,
    pub requests: Vec<Value>,
}

#[derive(Debug)]
pub struct FakeDoer {
    responses: Vec<Fixture>,
    state: Mutex<FakeState>,
}

impl FakeDoer {
    pub fn new(responses: Vec<Fixture>) -> Self {
        let used = vec![false; responses.len()];
        Self {
            responses,
            state: Mutex::new(FakeState {
                used,
                requests: Vec::new(),
            }),
        }
    }

    pub fn requests(&self) -> Vec<Value> {
        self.state.lock().expect("fake state").requests.clone()
    }
}

/// `net/url.URL.RequestURI`: Go's path escaping plus the raw query.
///
/// Go escapes everything outside RFC 3986's path set, notably `!` -> `%21`,
/// which is what `module.EscapePath` produces for upper-case module paths.
pub fn request_uri(url: &str) -> String {
    let parsed = url::Url::parse(url).expect("request url");
    let mut uri = go_escape_path(parsed.path());
    if let Some(query) = parsed.query() {
        uri.push('?');
        uri.push_str(query);
    }
    uri
}

fn go_escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';'
                    | b'=' | b'?' | b'@'
            );
        if keep {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .unwrap_or("")
}

#[async_trait]
impl HttpDoer for FakeDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let uri = request_uri(&request.url);
        let body_text = String::from_utf8_lossy(&request.body).into_owned();

        let log = json!({
            "method": request.method,
            "uri": uri,
            "authorization": header_value(&request.headers, "authorization"),
        });

        let mut state = self.state.lock().expect("fake state");
        let mut matched = None;
        for (index, fixture) in self.responses.iter().enumerate() {
            if state.used[index] {
                continue;
            }
            if fixture.method != request.method || fixture.request_uri != uri {
                continue;
            }
            if !fixture.body_contains.is_empty() && !body_text.contains(&fixture.body_contains) {
                continue;
            }
            state.used[index] = true;
            matched = Some(index);
            break;
        }
        state.requests.push(log);
        drop(state);

        let Some(index) = matched else {
            return Ok(HttpResponse {
                status: 500,
                headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
                body: br#"{"message":"no fixture"}"#.to_vec(),
            });
        };

        let fixture = &self.responses[index];
        let status = if fixture.status == 0 {
            200
        } else {
            fixture.status as u16
        };
        let headers = fixture
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        Ok(HttpResponse {
            status,
            headers,
            body: fixture.body.as_bytes().to_vec(),
        })
    }
}

pub fn repo_json(repo: &sg_extsvc::Repo) -> Value {
    let sources: Map<String, Value> = repo
        .sources
        .iter()
        .map(|(urn, info)| {
            (
                urn.clone(),
                json!({"id": info.id, "cloneUrl": info.clone_url}),
            )
        })
        .collect();
    json!({
        "name": repo.name,
        "uri": repo.uri,
        "description": repo.description,
        "externalRepo": {
            "id": repo.external_repo.id,
            "serviceType": repo.external_repo.service_type,
            "serviceID": repo.external_repo.service_id,
        },
        "fork": repo.fork,
        "archived": repo.archived,
        "stars": repo.stars,
        "private": repo.private,
        "sources": Value::Object(sources),
        "metadata": repo.metadata.clone().unwrap_or(Value::Null),
    })
}

fn request_sort_key(value: &Value) -> (String, String, String) {
    (
        value["method"].as_str().unwrap_or("").to_owned(),
        value["uri"].as_str().unwrap_or("").to_owned(),
        value["authorization"].as_str().unwrap_or("").to_owned(),
    )
}

pub fn sort_repos(repos: &mut [Value]) {
    repos.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
}

pub fn sort_errors(errors: &mut [Value]) {
    errors.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
}

pub fn sort_requests(requests: &mut [Value]) {
    requests.sort_by_key(request_sort_key);
}

/// The source service for a case, using the case's config JSON.
pub fn service(case: &Case, kind: &str) -> SourceService {
    SourceService {
        id: 1,
        kind: kind.to_owned(),
        display_name: kind.to_lowercase(),
        config: serde_json::to_string(&case.config).expect("config json"),
        encryption_key_id: String::new(),
        unrestricted: false,
    }
}

pub fn check_json(check: Option<bool>, result: Result<(), String>) -> Option<Value> {
    check.map(|_| Value::String(result.err().unwrap_or_default()))
}

/// Reads the corpus and prepares `<M914A_OUT>/rust/`.
pub fn setup() -> Option<(Vec<Case>, std::path::PathBuf)> {
    let corpus_path = std::env::var("M914A_CORPUS").ok()?;
    let out_dir = std::env::var("M914A_OUT").ok()?;
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let rust_dir = std::path::Path::new(&out_dir).join("rust");
    std::fs::create_dir_all(&rust_dir).expect("create output dir");
    Some((cases, rust_dir))
}

pub fn write_case(rust_dir: &std::path::Path, name: &str, envelope: &Value) {
    let encoded = serde_json::to_vec(envelope).expect("encode envelope");
    std::fs::write(rust_dir.join(format!("{name}.json")), encoded).expect("write envelope");
    println!("RUST {name}");
}

/// Envelope builder shared by all six harnesses.
pub struct Envelope {
    pub repos: Vec<Value>,
    pub errors: Vec<Value>,
    pub requests: Vec<Value>,
    pub check: Option<Value>,
    pub results: Vec<Value>,
}

impl Envelope {
    pub fn new() -> Self {
        Self {
            repos: Vec::new(),
            errors: Vec::new(),
            requests: Vec::new(),
            check: None,
            results: Vec::new(),
        }
    }

    pub fn finish(mut self, doer: &FakeDoer) -> Value {
        sort_repos(&mut self.repos);
        sort_errors(&mut self.errors);
        self.requests = doer.requests();
        sort_requests(&mut self.requests);
        json!({
            "repos": self.repos,
            "errors": self.errors,
            "requests": self.requests,
            "check": self.check,
            "results": self.results,
        })
    }
}

pub fn error_result(err: String) -> Value {
    json!({"error": err})
}

pub fn body_result(body: &[u8]) -> Value {
    json!({"body": String::from_utf8_lossy(body)})
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("runtime")
}

pub fn arc_doer(case: &Case) -> Arc<FakeDoer> {
    Arc::new(FakeDoer::new(case.responses.clone()))
}
