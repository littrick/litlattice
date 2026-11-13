//! Differential harness for the M9.25a changeset sources (Gerrit, Bitbucket
//! Cloud, Perforce) in `sg_extsvc::batches_sources`.
//!
//! Replays `test_ws/p9-go/data/bbcloud_perforce_gerrit_source/corpus.json`
//! through the Rust port with an injectable [`HttpDoer`] (and, for Perforce, an
//! injectable gitserver RPC seam), then writes one JSON envelope per case to
//! `<M925A_OUT>/rust.json`. The untracked Go oracle
//! `internal/batches/sources/zz_m925a_sources_oracle_test.go` writes
//! `<M925A_OUT>/go.json`; `run_bbcloud_perforce_gerrit_source_diff.sh`
//! compares them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::batches_sources as bs;
use sg_extsvc::batches_sources::ChangesetSource as _;
use sg_extsvc::perforce;
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    op: String,
    kind: String,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    auto_delete_branch: bool,
    #[serde(default)]
    repo: Value,
    #[serde(default)]
    sources: BTreeMap<String, String>,
    #[serde(default)]
    changeset: Changeset,
    #[serde(default)]
    squash: bool,
    #[serde(default)]
    comment: String,
    #[serde(default)]
    fork: ForkArgs,
    #[serde(default)]
    preload: bool,
    #[serde(default)]
    responses: Vec<Fixture>,
    #[serde(default)]
    perforce: PerforceCase,
    #[serde(default)]
    with_auth: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct Changeset {
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
    remote_is_fork: bool,
    #[serde(default)]
    remote_repo: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ForkArgs {
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PerforceCase {
    #[serde(default)]
    changelist: Value,
    #[serde(default)]
    get_error: String,
    #[serde(default)]
    check_error: String,
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
    body: Value,
}

impl Fixture {
    fn body_text(&self) -> String {
        match &self.body {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            other => serde_json::to_string(other).expect("fixture body serialises"),
        }
    }
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
            if fixture.method != request.method {
                continue;
            }
            if fixture.request_uri != "*" && fixture.request_uri != uri {
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
            body: fixture.body_text().into_bytes(),
        })
    }
}

#[derive(Debug)]
struct FakePerforce {
    changelist: Value,
    get_error: String,
    check_error: String,
    calls: Mutex<Vec<String>>,
}

impl FakePerforce {
    fn changelist(&self) -> perforce::Changelist {
        let text = |key: &str| {
            self.changelist
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        perforce::Changelist {
            id: text("ID"),
            creation_date: text("CreationDate"),
            state: text("State"),
            author: text("Author"),
            title: text("Title"),
            message: text("Message"),
        }
    }
}

#[async_trait]
impl bs::PerforceChangesetGitserver for FakePerforce {
    async fn perforce_get_changelist(
        &self,
        _connection: &perforce::PerforceConnectionDetails,
        changelist: &str,
    ) -> Result<perforce::Changelist, String> {
        self.calls
            .lock()
            .expect("calls")
            .push(format!("get:{changelist}"));
        if !self.get_error.is_empty() {
            return Err(self.get_error.clone());
        }
        Ok(self.changelist())
    }

    async fn check_perforce_credentials(
        &self,
        connection: &perforce::PerforceConnectionDetails,
    ) -> Result<(), String> {
        self.calls.lock().expect("calls").push(format!(
            "check:{}:{}",
            connection.p4_user, connection.p4_port
        ));
        if !self.check_error.is_empty() {
            return Err(self.check_error.clone());
        }
        Ok(())
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

fn build_changeset(case: &Case) -> bs::SourceChangeset {
    let target = batch_repo(case.repo.clone(), &case.sources, "myorg/repo1", 1);
    let remote = if case.changeset.remote_is_fork {
        batch_repo(
            case.changeset.remote_repo.clone(),
            &case.sources,
            "myorg/repo1-fork",
            2,
        )
    } else {
        target.clone()
    };
    bs::SourceChangeset {
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
            metadata: Value::Null,
        },
    }
}

fn changeset_out(cs: &bs::SourceChangeset) -> Value {
    json!({
        "external_id": cs.changeset.external_id,
        "external_branch": cs.changeset.external_branch,
        "external_fork_namespace": cs.changeset.external_fork_namespace,
        "external_fork_name": cs.changeset.external_fork_name,
        "metadata": cs.changeset.metadata,
    })
}

fn config_out(kind: &str, conn: &bs::SourceConnection, p4: Option<&bs::PerforceSource>) -> Value {
    match kind {
        "GERRIT" => {
            let (auth_type, username, password) = match &conn.auth {
                bs::SourceAuth::Basic { username, password } => {
                    ("basic".to_owned(), username.clone(), password.clone())
                }
                other => (format!("{other:?}"), String::new(), String::new()),
            };
            json!({
                "base_url": conn.base_url.as_str(),
                "auth_type": auth_type,
                "username": username,
                "password": password,
            })
        }
        "BITBUCKETCLOUD" => match &conn.auth {
            bs::SourceAuth::Basic { username, password } => json!({
                "base_url": conn.base_url.as_str(),
                "auth_type": "basic",
                "username": username,
                "password": password,
            }),
            bs::SourceAuth::BearerToken(token) => json!({
                "base_url": conn.base_url.as_str(),
                "auth_type": "bearer",
                "token": token,
            }),
            other => json!({
                "base_url": conn.base_url.as_str(),
                "auth_type": format!("{other:?}"),
            }),
        },
        "PERFORCE" => match p4 {
            Some(source) => {
                let config = source.config();
                json!({
                    "p4_port": config.p4_port,
                    "p4_user": config.p4_user,
                    "p4_passwd": config.p4_passwd,
                })
            }
            None => Value::Null,
        },
        _ => Value::Null,
    }
}

async fn run_case(case: &Case) -> Value {
    let conn = bs::source_connection_from_config(&case.kind, &case.config)
        .unwrap_or_else(|err| panic!("case {}: source config: {err}", case.name));
    let doer = Arc::new(FakeDoer::new(case.responses.clone()));
    if case.kind != "PERFORCE" {
        bs::new_source(&case.kind, &conn.base_url, conn.auth.clone(), doer.clone())
            .unwrap_or_else(|err| panic!("case {}: new_source: {err}", case.name));
    }

    let perforce_fake = Arc::new(FakePerforce {
        changelist: case.perforce.changelist.clone(),
        get_error: case.perforce.get_error.clone(),
        check_error: case.perforce.check_error.clone(),
        calls: Mutex::new(Vec::new()),
    });

    let mut gerrit: Option<bs::GerritSource> = None;
    let mut bbcloud: Option<bs::BitbucketCloudSource> = None;
    let mut perforce_source: Option<bs::PerforceSource> = None;
    let mut p4_config: Option<bs::PerforceSource> = None;

    match case.kind.as_str() {
        "GERRIT" => {
            gerrit = Some(
                bs::GerritSource::new(&conn.base_url, conn.auth.clone(), doer.clone())
                    .unwrap_or_else(|err| panic!("case {}: source: {err}", case.name))
                    .with_auto_delete_branch(case.auto_delete_branch),
            );
        }
        "BITBUCKETCLOUD" => {
            bbcloud = Some(
                bs::BitbucketCloudSource::new(&conn.base_url, conn.auth.clone(), doer.clone())
                    .unwrap_or_else(|err| panic!("case {}: source: {err}", case.name))
                    .with_auto_delete_branch(case.auto_delete_branch),
            );
        }
        "PERFORCE" => {
            let base = bs::PerforceSource::from_config(&case.config)
                .unwrap_or_else(|err| panic!("case {}: source: {err}", case.name))
                .with_gitserver(perforce_fake.clone());
            p4_config = Some(
                bs::PerforceSource::from_config(&case.config)
                    .unwrap_or_else(|err| panic!("case {}: config source: {err}", case.name)),
            );
            if case.with_auth {
                perforce_source = Some(
                    base.with_authenticator(bs::SourceAuth::Basic {
                        username: "p4user".to_owned(),
                        password: "secret".to_owned(),
                    })
                    .unwrap_or_else(|err| panic!("case {}: with_authenticator: {err}", case.name)),
                );
            } else {
                perforce_source = Some(base);
            }
        }
        other => panic!("case {}: unknown kind {other}", case.name),
    }

    let mut ok = false;
    let mut existed: Option<bool> = None;
    let mut error: Option<String> = None;
    let mut changeset_value: Value = Value::Null;
    let mut fork_value: Value = Value::Null;
    let config_value: Value = config_out(
        &case.kind,
        &conn,
        p4_config.as_ref(),
    );

    let mut cs = if case.op != "config" && case.op != "validate" && case.op != "auth_bad" {
        Some(build_changeset(case))
    } else {
        None
    };

    // Dispatch through a small helper so the borrow checker sees one mutable
    // changeset at a time.
    let trait_source: Option<&dyn bs::ChangesetSource> = match case.kind.as_str() {
        "GERRIT" => gerrit.as_ref().map(|source| source as &dyn bs::ChangesetSource),
        "BITBUCKETCLOUD" => bbcloud
            .as_ref()
            .map(|source| source as &dyn bs::ChangesetSource),
        "PERFORCE" => perforce_source
            .as_ref()
            .map(|source| source as &dyn bs::ChangesetSource),
        _ => None,
    };

    if case.preload && case.kind != "PERFORCE" {
        let cs = cs.as_mut().expect("preload changeset");
        trait_source
            .expect("preload source")
            .load_changeset(cs)
            .await
            .unwrap_or_else(|err| panic!("case {}: preload: {err}", case.name));
    }

    match case.op.as_str() {
        "config" => {
            ok = true;
        }
        "auth_bad" => {
            let base = bs::PerforceSource::from_config(&case.config)
                .unwrap_or_else(|err| panic!("case {}: source: {err}", case.name));
            match base.with_authenticator(bs::SourceAuth::BearerToken("t".to_owned())) {
                Ok(_) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "validate" => {
            let result = match case.kind.as_str() {
                "GERRIT" => gerrit.as_ref().unwrap().validate_authenticator().await,
                "BITBUCKETCLOUD" => bbcloud.as_ref().unwrap().validate_authenticator().await,
                _ => perforce_source.as_ref().unwrap().validate_authenticator().await,
            };
            match result {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "load" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().load_changeset(cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "create" | "create_draft" => {
            let cs = cs.as_mut().unwrap();
            let result = match case.kind.as_str() {
                "PERFORCE" if case.op == "create_draft" => {
                    trait_source.unwrap().create_draft_changeset(cs).await
                }
                "GERRIT" if case.op == "create_draft" => {
                    trait_source.unwrap().create_draft_changeset(cs).await
                }
                _ => trait_source.unwrap().create_changeset(cs).await,
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
        }
        "update" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().update_changeset(cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "close" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().close_changeset(cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "reopen" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().reopen_changeset(cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "undraft" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().undraft_changeset(cs).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "comment" => {
            let cs = cs.as_ref().unwrap();
            match trait_source
                .unwrap()
                .create_comment(cs, &case.comment)
                .await
            {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "merge" => {
            let cs = cs.as_mut().unwrap();
            match trait_source.unwrap().merge_changeset(cs, case.squash).await {
                Ok(()) => ok = true,
                Err(err) => error = Some(err.to_string()),
            }
        }
        "get_fork" => {
            let cs = cs.as_ref().unwrap();
            let source = match case.kind.as_str() {
                "BITBUCKETCLOUD" => bbcloud.as_ref().unwrap(),
                other => panic!("case {}: get_fork unsupported for {other}", case.name),
            };
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
                    fork_value = json!({
                        "name": fork.name,
                        "sources": sources,
                        "metadata": fork.metadata,
                    });
                }
                Err(err) => error = Some(err.to_string()),
            }
        }
        other => panic!("case {}: unknown op {other}", case.name),
    }

    if let Some(cs) = &cs {
        changeset_value = changeset_out(cs);
    }

    let requests = {
        let state = doer.state.lock().expect("fake state");
        state.requests.clone()
    };
    let gitserver_calls = if case.kind == "PERFORCE" {
        perforce_fake.calls.lock().expect("calls").clone()
    } else {
        Vec::new()
    };

    json!({
        "name": case.name,
        "op": case.op,
        "kind": case.kind,
        "ok": ok,
        "existed": existed,
        "error": error,
        "changeset": changeset_value,
        "fork": fork_value,
        "config": if case.op == "config" { config_value } else { Value::Null },
        "gitserver_calls": gitserver_calls,
        "requests": requests,
    })
}

#[test]
fn bbcloud_perforce_gerrit_source_diff() {
    let Ok(corpus_path) = std::env::var("M925A_CORPUS") else {
        return;
    };
    let Ok(out_dir) = std::env::var("M925A_OUT") else {
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut results = Vec::with_capacity(cases.len());
    for case in &cases {
        let envelope = runtime.block_on(run_case(case));
        println!("RUST {}", case.name);
        results.push(envelope);
    }
    let encoded = serde_json::to_vec(&results).expect("encode results");
    std::fs::write(std::path::Path::new(&out_dir).join("rust.json"), encoded)
        .expect("write rust.json");
}
