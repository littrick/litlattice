//! Differential harness for `azuredevops_source.rs` / `azuredevops.rs`.
//!
//! Replays `test_ws/p9-go/data/azuredevops_source/corpus.json` through the
//! Rust port with an injectable [`HttpDoer`] and writes one JSON envelope per
//! case under `<M913C_OUT>/rust/`; the Go oracle
//! (`internal/repos/zz_m9_13c_sources_oracle_test.go`) writes
//! `<M913C_OUT>/go/`. `run_azuredevops_source_diff.sh` compares them.
//!
//! The source preserves the configured project/org order, but both sides sort
//! repos, errors and requests deterministically before writing the envelope.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::azuredevops_source::AzureDevOpsSource;
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse, Source, SourceService};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    config: Value,
    #[serde(default)]
    responses: Vec<Fixture>,
    #[serde(default)]
    check: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct Fixture {
    method: String,
    #[serde(rename = "requestUri")]
    request_uri: String,
    #[serde(rename = "bodyContains", default)]
    body_contains: String,
    #[serde(default)]
    status: i64,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: String,
}

#[derive(Debug, Default)]
struct FakeState {
    used: Vec<bool>,
    requests: Vec<Value>,
}

#[derive(Debug)]
struct FakeDoer {
    responses: Vec<Fixture>,
    state: Mutex<FakeState>,
}

impl FakeDoer {
    fn new(responses: Vec<Fixture>) -> Self {
        let used = vec![false; responses.len()];
        Self {
            responses,
            state: Mutex::new(FakeState {
                used,
                requests: Vec::new(),
            }),
        }
    }
}

fn request_uri(url: &str) -> String {
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

fn repo_json(repo: &sg_extsvc::Repo) -> Value {
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

async fn run_case(case: &Case) -> Value {
    let config = serde_json::to_string(&case.config).expect("config json");
    let service = SourceService {
        id: 1,
        kind: sg_extsvc::KIND_AZUREDEVOPS.to_owned(),
        display_name: "azuredevops".to_owned(),
        config,
        encryption_key_id: String::new(),
        unrestricted: false,
    };
    let doer = Arc::new(FakeDoer::new(case.responses.clone()));
    let source = match AzureDevOpsSource::new(service, doer.clone()) {
        Ok(source) => source,
        Err(err) => panic!("case {}: constructing source: {err}", case.name),
    };

    let mut repos = Vec::new();
    let mut errors = Vec::new();
    for result in source.list_repos().await {
        match (result.repo, result.err) {
            (Some(repo), _) => repos.push(repo_json(&repo)),
            (None, Some(err)) => errors.push(Value::String(err)),
            (None, None) => errors.push(Value::String(String::new())),
        }
    }
    repos.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
    errors.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));

    let check = if case.check {
        Some(match source.check_connection().await {
            Ok(()) => String::new(),
            Err(err) => err,
        })
    } else {
        None
    };

    let mut requests = {
        let state = doer.state.lock().expect("fake state");
        state.requests.clone()
    };
    requests.sort_by_key(request_sort_key);

    json!({
        "repos": repos,
        "errors": errors,
        "requests": requests,
        "check": check,
    })
}

#[test]
fn azuredevops_source_diff() {
    let Ok(corpus_path) = std::env::var("M913C_CORPUS") else {
        return;
    };
    let Ok(out_dir) = std::env::var("M913C_OUT") else {
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let rust_dir = std::path::Path::new(&out_dir).join("rust");
    std::fs::create_dir_all(&rust_dir).expect("create output dir");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case in &cases {
        let envelope = runtime.block_on(run_case(case));
        let encoded = serde_json::to_vec(&envelope).expect("encode envelope");
        std::fs::write(rust_dir.join(format!("{}.json", case.name)), encoded)
            .expect("write envelope");
        println!("RUST {}", case.name);
    }
}
