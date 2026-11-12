//! Shared plumbing for the M9.14b remaining-code-host differential harnesses
//! (`awscodecommit_source_diff`, `bitbucketserver_source_diff`,
//! `pagure_source_diff`, `phabricator_source_diff`).
//!
//! Each harness replays a corpus of cases through the Rust port with an
//! injectable [`HttpDoer`] and writes one JSON envelope per case under
//! `<M914B_OUT>/rust/`; the matching Go oracle
//! (`internal/repos/zz_m9_14b_*_oracle_test.go`) writes `<M914B_OUT>/go/`.
//! The `run_*_source_diff.sh` scripts compare the two with a Python
//! deep-equality over parsed JSON.
//!
//! Both sides sort repos (by name), errors and requests before writing the
//! envelope. Requests carry the auth/target header as a third sort key.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};

#[derive(Debug, Clone, Deserialize)]
pub struct Case {
    pub name: String,
    pub config: Value,
    #[serde(default)]
    pub responses: Vec<Fixture>,
    #[serde(default)]
    pub check: bool,
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
struct FakeState {
    used: Vec<bool>,
    requests: Vec<Value>,
}

#[derive(Debug)]
pub struct FakeDoer {
    responses: Vec<Fixture>,
    header_name: &'static str,
    log_key: &'static str,
    state: Mutex<FakeState>,
}

impl FakeDoer {
    /// `header_name` is the request header logged in the envelope (matched
    /// case-insensitively); `log_key` is the JSON field it is stored under
    /// (`"authorization"` or `"target"`).
    pub fn new(responses: Vec<Fixture>, header_name: &'static str, log_key: &'static str) -> Self {
        let used = vec![false; responses.len()];
        Self {
            responses,
            header_name,
            log_key,
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

pub fn request_uri(url: &str) -> String {
    let parsed = url::Url::parse(url).expect("request url");
    let mut uri = parsed.path().to_owned();
    if let Some(query) = parsed.query() {
        uri.push('?');
        uri.push_str(query);
    }
    uri
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
        let logged = header_value(&request.headers, self.header_name);

        let mut log = Map::new();
        log.insert("method".to_owned(), json!(request.method));
        log.insert("uri".to_owned(), json!(uri));
        log.insert(self.log_key.to_owned(), json!(logged));

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
        state.requests.push(Value::Object(log));
        drop(state);

        let Some(index) = matched else {
            return Ok(HttpResponse {
                status: 500,
                headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
                // Mirror the Go oracles' no-fixture response so error text
                // matches when a case intentionally runs out of fixtures.
                body: br#"{"error_code":"no fixture","error_info":"no fixture"}"#.to_vec(),
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

pub fn request_sort_key(value: &Value, key: &str) -> (String, String, String) {
    (
        value["method"].as_str().unwrap_or("").to_owned(),
        value["uri"].as_str().unwrap_or("").to_owned(),
        value[key].as_str().unwrap_or("").to_owned(),
    )
}

/// Sort repos by name, errors lexicographically, and requests by
/// (method, uri, third key), then assemble the envelope shared with the Go
/// oracle.
pub fn envelope(
    mut repos: Vec<Value>,
    mut errors: Vec<String>,
    mut requests: Vec<Value>,
    log_key: &str,
    check: Option<String>,
) -> Value {
    repos.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
    errors.sort();
    requests.sort_by_key(|value| request_sort_key(value, log_key));
    json!({
        "repos": repos,
        "errors": errors,
        "requests": requests,
        "check": check,
    })
}

pub fn load_cases() -> Option<(Vec<Case>, std::path::PathBuf)> {
    let Ok(corpus_path) = std::env::var("M914B_CORPUS") else {
        return None;
    };
    let Ok(out_dir) = std::env::var("M914B_OUT") else {
        return None;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    Some((cases, std::path::Path::new(&out_dir).join("rust")))
}

pub fn write_envelope(dir: &std::path::Path, name: &str, envelope: &Value) {
    let encoded = serde_json::to_vec(envelope).expect("encode envelope");
    std::fs::write(dir.join(format!("{name}.json")), encoded).expect("write envelope");
    println!("RUST {name}");
}
