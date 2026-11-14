//! M9.35b differential harness: Batch reconciler `pushCommit` integration.
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/reconciler/zz_m935b_push_commit_oracle_test.go`. Both
//! sides consume `test_ws/p9-go/data/batches_push_commit/corpus.json`, write
//! one normalized JSON document per case, and
//! `run_batches_push_commit_diff.sh` compares them.
//!
//! The Rust side runs the worker's real `ConfigSourceFactory` /
//! `ExtsvcReconcilerSource::push_commit`: `GitserverPushConfig` →
//! `BuildCommitOptsCommon` → `sg_extsvc::push_commit` with a fake
//! `GitserverCommitClient`. Gated by `SG_M935B_PUSH_COMMIT_DIFF=1`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sg_extsvc::batches_sources as extsvc;
use sg_extsvc::HttpDoer;
use sg_service_worker::jobs::batches_reconciler::{
    Changeset, ChangesetPublicationState, ChangesetSourceFactory, ChangesetSpec,
    ConfigSourceFactory, Repo, RepoMetadataProvider, SourceError, StaticSourceConfigProvider,
};

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Spec {
    #[serde(default)]
    diff: String,
    #[serde(default)]
    base_rev: String,
    #[serde(default)]
    head_ref: String,
    #[serde(default)]
    commit_message: String,
    #[serde(default)]
    author_name: String,
    #[serde(default)]
    author_email: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Outcome {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    changelist_id: String,
    #[serde(default)]
    internal_error: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    combined_output: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Case {
    name: String,
    kind: String,
    base_url: String,
    clone_url: String,
    auth_type: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    target_repo: String,
    remote_repo: String,
    #[serde(default)]
    metadata_name_with_owner: String,
    #[serde(default)]
    spec: Spec,
    #[serde(default)]
    outcome: Outcome,
}

struct NoHttp;

#[async_trait]
impl HttpDoer for NoHttp {
    async fn do_request(
        &self,
        _request: sg_extsvc::HttpRequest,
    ) -> Result<sg_extsvc::HttpResponse, String> {
        Err("no HTTP expected in the M9.35b corpus".to_owned())
    }
}

#[derive(Clone)]
struct CorpusMetadataProvider {
    metadata: Value,
    sources: BTreeMap<String, extsvc::RepoSource>,
}

#[async_trait]
impl RepoMetadataProvider for CorpusMetadataProvider {
    async fn metadata(&self, _kind: &str, _repo: &Repo) -> Result<Value, SourceError> {
        Ok(self.metadata.clone())
    }

    async fn repo(&self, kind: &str, repo: &Repo) -> Result<extsvc::Repo, SourceError> {
        Ok(extsvc::Repo {
            id: repo.id,
            name: repo.name.clone(),
            external_repo: extsvc::ExternalRepoSpec {
                id: String::new(),
                service_type: extsvc::kind_to_type(kind),
                service_id: String::new(),
            },
            sources: self.sources.clone(),
            metadata: self.metadata.clone(),
        })
    }
}

#[derive(Default)]
struct FakeGitserver {
    request: Mutex<Option<extsvc::CreateCommitFromPatchRequest>>,
    outcome: Outcome,
}

#[async_trait]
impl extsvc::GitserverCommitClient for FakeGitserver {
    async fn create_commit_from_patch(
        &self,
        request: extsvc::CreateCommitFromPatchRequest,
    ) -> Result<extsvc::CreateCommitFromPatchResponse, extsvc::GitserverPushError> {
        *self.request.lock().unwrap() = Some(request);
        match self.outcome.kind.as_str() {
            "success" => Ok(extsvc::CreateCommitFromPatchResponse {
                rev: self.outcome.rev.clone(),
                error: None,
                changelist_id: self.outcome.changelist_id.clone(),
            }),
            "patch_error" => Err(extsvc::GitserverPushError::Patch(
                extsvc::CreateCommitFromPatchError {
                    repository_name: self.request_repo_name(),
                    internal_error: self.outcome.internal_error.clone(),
                    command: self.outcome.command.clone(),
                    combined_output: self.outcome.combined_output.clone(),
                },
            )),
            "other_error" => Err(extsvc::GitserverPushError::Other(
                self.outcome.message.clone(),
            )),
            other => panic!("unknown outcome type {other}"),
        }
    }
}

impl FakeGitserver {
    fn request_repo_name(&self) -> String {
        self.request
            .lock()
            .unwrap()
            .as_ref()
            .map(|request| request.repo.clone())
            .unwrap_or_default()
    }
}

fn source_auth(case: &Case) -> extsvc::SourceAuth {
    match case.auth_type.as_str() {
        "oauth" => extsvc::SourceAuth::OAuthToken(case.token.clone()),
        "basic" => extsvc::SourceAuth::Basic {
            username: case.username.clone(),
            password: case.password.clone(),
        },
        _ => extsvc::SourceAuth::None,
    }
}

fn request_json(request: &extsvc::CreateCommitFromPatchRequest) -> Value {
    json!({
        "repo": request.repo,
        "base_commit": request.base_commit,
        "patch": String::from_utf8_lossy(&request.patch),
        "target_ref": request.target_ref,
        "commit_info": {
            "messages": request.commit_info.messages,
            "author_name": request.commit_info.author_name,
            "author_email": request.commit_info.author_email,
            "date": request.commit_info.date,
        },
        "patch_filenames_no_prefix": request.patch_filenames_no_prefix,
        "push": request.push.as_ref().map(|push| json!({
            "remote_url": push.remote_url,
            "private_key": push.private_key,
            "passphrase": push.passphrase,
        })),
    })
}

fn error_json(err: &SourceError) -> Value {
    json!({
        "message": err.message,
        "non_retryable": err.non_retryable,
        "push_commit_error": err
            .message
            .starts_with("creating commit from patch for repository "),
    })
}

fn changeset(case: &Case) -> Changeset {
    Changeset {
        id: 1,
        repo_id: 1,
        batch_changes: Vec::new(),
        external_id: String::new(),
        external_service_type: case.kind.clone(),
        external_branch: String::new(),
        external_state: None,
        owned_by_batch_change_id: 0,
        current_spec_id: 0,
        previous_spec_id: 0,
        publication_state: ChangesetPublicationState::Unpublished,
        ui_publication_state: None,
        closing: false,
        detached_at: None,
        failure_message: None,
        previous_failure_message: None,
    }
}

#[tokio::test]
async fn push_commit_corpus() {
    if std::env::var("SG_M935B_PUSH_COMMIT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M935B_CORPUS").expect("M935B_CORPUS");
    let out_dir = std::env::var("M935B_OUT").expect("M935B_OUT");
    std::fs::create_dir_all(&out_dir).expect("create out dir");
    let raw = std::fs::read_to_string(corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    for case in &corpus.cases {
        let mut out = serde_json::Map::new();
        out.insert("name".to_owned(), json!(case.name));

        let base_url = url::Url::parse(&case.base_url).expect("base_url");
        let connection = extsvc::SourceConnection {
            kind: case.kind.clone(),
            base_url,
            auth: source_auth(case),
        };
        let sources = BTreeMap::from([(
            "1".to_owned(),
            extsvc::RepoSource {
                id: "1".to_owned(),
                clone_url: case.clone_url.clone(),
            },
        )]);
        let provider = Arc::new(CorpusMetadataProvider {
            metadata: json!({"NameWithOwner": case.metadata_name_with_owner}),
            sources,
        });
        let configs = Arc::new(StaticSourceConfigProvider::new(vec![connection]));
        let fake = Arc::new(FakeGitserver {
            request: Mutex::new(None),
            outcome: case.outcome.clone(),
        });
        let factory = ConfigSourceFactory::new(configs, Arc::new(NoHttp), provider)
            .with_gitserver_commit_client(fake.clone());

        let target = Repo {
            id: 1,
            name: case.target_repo.clone(),
            archived: false,
        };
        let remote = Repo {
            id: 1,
            name: case.remote_repo.clone(),
            archived: false,
        };
        let ch = changeset(case);
        let spec = ChangesetSpec {
            id: 1,
            external_id: String::new(),
            base_rev: case.spec.base_rev.clone(),
            base_ref: String::new(),
            head_ref: case.spec.head_ref.clone(),
            title: String::new(),
            body: String::new(),
            published: Default::default(),
            diff: case.spec.diff.clone().into_bytes(),
            commit_message: case.spec.commit_message.clone(),
            commit_author_name: case.spec.author_name.clone(),
            commit_author_email: case.spec.author_email.clone(),
        };

        let source = factory.for_changeset(&ch, &target).await.expect("source");
        let result = source
            .push_commit(&target, &ch, Some(&spec), &remote)
            .await;

        if let Some(request) = fake.request.lock().unwrap().as_ref() {
            out.insert("request".to_owned(), request_json(request));
        }
        match result {
            Ok(response) => {
                out.insert(
                    "result".to_owned(),
                    json!({
                        "rev": response.rev.unwrap_or_default(),
                        "changelist_id": response.changelist_id.unwrap_or_default(),
                    }),
                );
            }
            Err(err) => {
                out.insert("error".to_owned(), error_json(&err));
            }
        }
        let encoded = serde_json::to_string_pretty(&Value::Object(out)).expect("encode");
        std::fs::write(format!("{out_dir}/{}.json", case.name), encoded).expect("write case");
    }
}

// ---------------------------------------------------------------------------
// Rust-only: `runAfterCommit`'s duplicate step consumes the push request.
//
// The corpus cannot drive `runAfterCommit` (the Go executor binds a concrete
// store), so this test exercises the new integration point directly: the
// `PushResult.request` produced by `push_commit` feeds M9.34b's REST
// `duplicate_commit`, and the commit message comes from `CommitInfo.Messages`.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ScriptedGithubHttp {
    calls: Mutex<Vec<(String, String, Value)>>,
}

#[async_trait]
impl HttpDoer for ScriptedGithubHttp {
    async fn do_request(
        &self,
        request: sg_extsvc::HttpRequest,
    ) -> Result<sg_extsvc::HttpResponse, String> {
        let path = url::Url::parse(&request.url)
            .map(|url| url.path().to_owned())
            .unwrap_or_default();
        let body: Value = if request.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&request.body).unwrap_or(Value::Null)
        };
        self.calls
            .lock()
            .unwrap()
            .push((request.method.clone(), path, body));
        let (status, body) = match request.method.as_str() {
            "GET" => (
                200,
                r#"{"sha":"old","commit":{"tree":{"sha":"tree-sha"}},"parents":[{"sha":"parent-1"}]}"#,
            ),
            "POST" => (
                201,
                r#"{"sha":"new-sha","verification":{"verified":true}}"#,
            ),
            "PATCH" => (200, r#"{}"#),
            other => return Err(format!("unexpected method {other}")),
        };
        Ok(sg_extsvc::HttpResponse {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        })
    }
}

#[tokio::test]
async fn duplicate_commit_uses_push_request() {
    let http = Arc::new(ScriptedGithubHttp::default());
    let provider = Arc::new(CorpusMetadataProvider {
        metadata: json!({"NameWithOwner": "org/repo"}),
        sources: BTreeMap::from([(
            "1".to_owned(),
            extsvc::RepoSource {
                id: "1".to_owned(),
                clone_url: "https://github.com/org/repo.git".to_owned(),
            },
        )]),
    });
    let configs = Arc::new(StaticSourceConfigProvider::new(vec![
        extsvc::SourceConnection {
            kind: "GITHUB".to_owned(),
            base_url: url::Url::parse("https://github.com/").unwrap(),
            auth: extsvc::SourceAuth::OAuthToken("ghp_token".to_owned()),
        },
    ]));
    let fake = Arc::new(FakeGitserver {
        request: Mutex::new(None),
        outcome: Outcome {
            kind: "success".to_owned(),
            rev: "old".to_owned(),
            ..Default::default()
        },
    });
    let factory = ConfigSourceFactory::new(configs, http.clone(), provider)
        .with_gitserver_commit_client(fake);
    let target = Repo {
        id: 1,
        name: "github.com/org/repo".to_owned(),
        archived: false,
    };
    let remote = target.clone();
    let case = Case {
        kind: "GITHUB".to_owned(),
        base_url: "https://github.com/".to_owned(),
        clone_url: "https://github.com/org/repo.git".to_owned(),
        auth_type: "oauth".to_owned(),
        token: "ghp_token".to_owned(),
        target_repo: target.name.clone(),
        remote_repo: remote.name.clone(),
        metadata_name_with_owner: "org/repo".to_owned(),
        spec: Spec {
            diff: "--- a/f\n+++ b/f\n".to_owned(),
            base_rev: "base".to_owned(),
            head_ref: "refs/heads/branch".to_owned(),
            commit_message: "signed message".to_owned(),
            author_name: "Alice".to_owned(),
            author_email: "alice@example.com".to_owned(),
        },
        ..Default::default()
    };
    let ch = changeset(&case);
    let spec = ChangesetSpec {
        id: 1,
        external_id: String::new(),
        base_rev: case.spec.base_rev.clone(),
        base_ref: String::new(),
        head_ref: case.spec.head_ref.clone(),
        title: String::new(),
        body: String::new(),
        published: Default::default(),
        diff: case.spec.diff.clone().into_bytes(),
        commit_message: case.spec.commit_message.clone(),
        commit_author_name: case.spec.author_name.clone(),
        commit_author_email: case.spec.author_email.clone(),
    };
    let source = factory.for_changeset(&ch, &target).await.expect("source");
    let resp = source
        .push_commit(&target, &ch, Some(&spec), &remote)
        .await
        .expect("push");
    let verification = source
        .duplicate_commit(&remote, &resp)
        .await
        .expect("duplicate");

    assert!(verification.verified);
    assert_eq!(verification.sha, "new-sha");
    let calls = http.calls.lock().unwrap();
    let methods: Vec<&str> = calls.iter().map(|(method, _, _)| method.as_str()).collect();
    assert_eq!(methods, vec!["GET", "POST", "PATCH"]);
    assert_eq!(calls[1].2["message"], json!("signed message"));
    assert_eq!(calls[1].2["tree"], json!("tree-sha"));
    assert_eq!(calls[1].2["parents"], json!(["parent-1"]));
}
