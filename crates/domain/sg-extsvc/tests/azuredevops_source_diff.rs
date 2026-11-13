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
//!
//! M9.24b adds a second test, gated by `M924B_CORPUS`/`M924B_OUT`, that drives
//! the Batch Changes `AzureDevOpsSource` (`sg_extsvc::batches_sources`) with
//! `data/azuredevops_source/batches_corpus.json`; the Go oracle is
//! `internal/batches/sources/zz_m924b_azure_oracle_test.go`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::azuredevops_source::AzureDevOpsSource;
use sg_extsvc::batches_sources as bs;
use sg_extsvc::batches_sources::ChangesetSource as _;
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

// ---------------------------------------------------------------------------
// M9.24b: Batch Changes Azure DevOps changeset source
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct BatchCase {
    name: String,
    op: String,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    auto_delete_branch: bool,
    #[serde(default)]
    repo: Value,
    #[serde(default)]
    sources: BTreeMap<String, String>,
    #[serde(default)]
    changeset: BatchChangeset,
    #[serde(default)]
    squash: bool,
    #[serde(default)]
    comment: String,
    #[serde(default)]
    fork: BatchForkArgs,
    #[serde(default)]
    responses: Vec<BatchFixture>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BatchChangeset {
    #[serde(default)]
    external_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    head_ref: String,
    #[serde(default)]
    base_ref: String,
    #[serde(default)]
    external_fork_namespace: String,
    #[serde(default)]
    external_fork_name: String,
    #[serde(default)]
    sync_head_ref_oid: String,
    #[serde(default)]
    remote_is_fork: bool,
    #[serde(default)]
    remote_repo: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BatchForkArgs {
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BatchFixture {
    method: String,
    #[serde(rename = "requestUri")]
    request_uri: String,
    #[serde(rename = "bodyContains", default)]
    body_contains: String,
    #[serde(default)]
    status: i64,
    #[serde(default)]
    body: String,
}

#[derive(Debug, Default)]
struct BatchFakeState {
    used: Vec<bool>,
    requests: Vec<Value>,
}

#[derive(Debug)]
struct BatchFakeDoer {
    responses: Vec<BatchFixture>,
    state: Mutex<BatchFakeState>,
}

impl BatchFakeDoer {
    fn new(responses: Vec<BatchFixture>) -> Self {
        let used = vec![false; responses.len()];
        Self {
            responses,
            state: Mutex::new(BatchFakeState {
                used,
                requests: Vec::new(),
            }),
        }
    }
}

#[async_trait]
impl HttpDoer for BatchFakeDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let uri = request_uri(&request.url);
        let body_text = String::from_utf8_lossy(&request.body).into_owned();
        // Go's oracle decodes the request body into `interface{}`; an empty
        // body becomes JSON `null` there and `Value::Null` here.
        let parsed_body: Value = if request.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&request.body).unwrap_or(Value::Null)
        };

        let log = json!({
            "method": request.method,
            "uri": uri,
            "authorization": header_value(&request.headers, "authorization"),
            "contentType": header_value(&request.headers, "content-type"),
            "body": parsed_body,
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
        Ok(HttpResponse {
            status,
            headers: Vec::new(),
            body: fixture.body.as_bytes().to_vec(),
        })
    }
}

fn batch_repo(meta: Value, sources: &BTreeMap<String, String>, name: &str, id: i32) -> bs::Repo {
    bs::Repo {
        id,
        name: name.to_owned(),
        external_repo: bs::ExternalRepoSpec::default(),
        sources: sources
            .iter()
            .map(|(urn, clone_url)| {
                (
                    urn.clone(),
                    bs::RepoSource {
                        id: urn.clone(),
                        clone_url: clone_url.clone(),
                    },
                )
            })
            .collect(),
        metadata: meta,
    }
}

/// The port's stored metadata is already the canonical projection (see
/// `azure_devops_metadata`); anything else (the initial sync-state map) is
/// `null`, matching the Go oracle's type assertion.
fn batch_metadata_projection(meta: &Value) -> Value {
    if meta.get("kind").and_then(Value::as_str) == Some("azuredevops") {
        meta.clone()
    } else {
        Value::Null
    }
}

async fn run_batch_case(case: &BatchCase) -> Value {
    let conn = bs::source_connection_from_config("AZUREDEVOPS", &case.config)
        .unwrap_or_else(|err| panic!("case {}: source config: {err}", case.name));
    let doer = Arc::new(BatchFakeDoer::new(case.responses.clone()));
    // Exercise the factory wiring as well as the direct constructor.
    bs::new_source("AZUREDEVOPS", &conn.base_url, conn.auth.clone(), doer.clone())
        .unwrap_or_else(|err| panic!("case {}: new_source: {err}", case.name));
    let source = bs::AzureDevOpsSource::new(&conn.base_url, conn.auth.clone(), doer.clone())
        .unwrap_or_else(|err| panic!("case {}: source: {err}", case.name))
        .with_auto_delete_branch(case.auto_delete_branch);

    let mut ok = false;
    let mut existed: Option<bool> = None;
    let mut error: Option<String> = None;
    let mut changeset_out: Value = Value::Null;
    let mut fork_out: Value = Value::Null;
    let mut config_out: Value = Value::Null;

    let build_changeset = || {
        let target = batch_repo(case.repo.clone(), &case.sources, "myorg/myproj/repo1", 1);
        let remote = if case.changeset.remote_is_fork {
            batch_repo(
                case.changeset.remote_repo.clone(),
                &case.sources,
                "myorg/myproj/repo1-fork",
                2,
            )
        } else {
            target.clone()
        };
        let changeset = bs::SourceChangeset {
            title: case.changeset.title.clone(),
            body: case.changeset.body.clone(),
            head_ref: case.changeset.head_ref.clone(),
            base_ref: case.changeset.base_ref.clone(),
            remote_repo: remote,
            target_repo: target,
            changeset: bs::Changeset {
                id: 0,
                external_id: case.changeset.external_id.clone(),
                external_branch: String::new(),
                external_fork_namespace: case.changeset.external_fork_namespace.clone(),
                external_fork_name: case.changeset.external_fork_name.clone(),
                metadata: json!({
                    "lastMergeSourceCommit": {
                        "commitId": case.changeset.sync_head_ref_oid,
                    },
                }),
            },
        };
        changeset
    };

    match case.op.as_str() {
        "config" => {
            ok = true;
            let (auth_type, username, password) = match &conn.auth {
                bs::SourceAuth::Basic { username, password } => {
                    ("basic".to_owned(), username.clone(), password.clone())
                }
                other => (format!("{other:?}"), String::new(), String::new()),
            };
            config_out = json!({
                "base_url": conn.base_url.as_str(),
                "auth_type": auth_type,
                "username": username,
                "password": password,
            });
        }
        "validate" => match source.validate_authenticator().await {
            Ok(()) => ok = true,
            Err(err) => error = Some(err.to_string()),
        },
        "load" => {
            let mut cs = build_changeset();
            match source.load_changeset(&mut cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "create" | "create_draft" => {
            let mut cs = build_changeset();
            let result = if case.op == "create_draft" {
                source.create_draft_changeset(&mut cs).await
            } else {
                source.create_changeset(&mut cs).await
            };
            match result {
                Ok(value) => {
                    ok = true;
                    existed = Some(value);
                }
                Err(err) => {
                    existed = Some(false);
                    error = Some(err.to_string());
                }
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "update" => {
            let mut cs = build_changeset();
            match source.update_changeset(&mut cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "close" => {
            let mut cs = build_changeset();
            match source.close_changeset(&mut cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "reopen" => {
            let mut cs = build_changeset();
            match source.reopen_changeset(&mut cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "undraft" => {
            let mut cs = build_changeset();
            match source.undraft_changeset(&mut cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "comment" => {
            let cs = build_changeset();
            match source.create_comment(&cs, &case.comment).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "merge" => {
            let mut cs = build_changeset();
            match source.merge_changeset(&mut cs, case.squash).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        "get_fork" => {
            let cs = build_changeset();
            match source
                .get_fork(
                    &cs.target_repo,
                    case.fork.namespace.as_ref(),
                    case.fork.name.as_ref(),
                )
                .await
            {
                Ok(fork) => {
                    ok = true;
                    let sources: Map<String, Value> = fork
                        .sources
                        .iter()
                        .map(|(urn, info)| {
                            (urn.clone(), Value::String(info.clone_url.clone()))
                        })
                        .collect();
                    fork_out = json!({
                        "name": fork.name,
                        "sources": sources,
                        "metadata": fork.metadata,
                    });
                }
                Err(err) => error = Some(err.to_string()),
            }
            changeset_out = batch_changeset_out(&cs);
        }
        other => panic!("case {}: unknown op {other}", case.name),
    }

    let requests = {
        let state = doer.state.lock().expect("fake state");
        state.requests.clone()
    };

    json!({
        "name": case.name,
        "op": case.op,
        "ok": ok,
        "existed": existed,
        "error": error,
        "changeset": changeset_out,
        "fork": fork_out,
        "config": config_out,
        "requests": requests,
    })
}

fn batch_changeset_out(cs: &bs::SourceChangeset) -> Value {
    json!({
        "external_id": cs.changeset.external_id,
        "external_branch": cs.changeset.external_branch,
        "external_fork_namespace": cs.changeset.external_fork_namespace,
        "external_fork_name": cs.changeset.external_fork_name,
        "metadata": batch_metadata_projection(&cs.changeset.metadata),
    })
}

#[test]
fn azuredevops_batches_source_diff() {
    let Ok(corpus_path) = std::env::var("M924B_CORPUS") else {
        return;
    };
    let Ok(out_dir) = std::env::var("M924B_OUT") else {
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read batches corpus");
    let cases: Vec<BatchCase> = serde_json::from_str(&raw).expect("parse batches corpus");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut results = Vec::with_capacity(cases.len());
    for case in &cases {
        let envelope = runtime.block_on(run_batch_case(case));
        println!("RUST {}", case.name);
        results.push(envelope);
    }
    let encoded = serde_json::to_vec(&results).expect("encode results");
    std::fs::write(std::path::Path::new(&out_dir).join("rust.json"), encoded)
        .expect("write rust.json");
}
