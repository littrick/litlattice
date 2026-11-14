//! M9.34b differential harness: Batch push credentials and GitHub REST
//! duplicate commit.
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/sources/zz_m934b_push_oracle_test.go`. Both sides consume
//! `test_ws/p9-go/data/batches_push/corpus.json`, write one normalized JSON
//! document per case, and `run_batches_push_diff.sh` compares them.
//!
//! Cases cover `BuildCommitOptsCommon`, `GitserverPushConfig`,
//! `withAuthenticatorForUser`/`withSiteAuthenticator` and
//! `GitHubSource.DuplicateCommit`. Gated by `SG_M934B_PUSH_DIFF=1`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_extsvc::batches_sources::{
    build_commit_opts_common, duplicate_commit, gitserver_push_config,
    push_authenticator_for_source_auth, with_authenticator_for_user, BatchesCredentialStore,
    ChangesetSpec, CreateCommitFromPatchRequest, ExternalRepoSpec, PatchCommitInfo,
    PushAuthenticator, PushConfig, Repo as SourceRepo, RepoSource, SourceAuth,
    SourceErrorKind,
};
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<CorpusCase>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct CorpusCase {
    name: String,
    kind: String,

    // build_opts
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
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    push_token: String,

    // push_config / credential
    #[serde(default)]
    auth_type: String,
    #[serde(default)]
    auth_token: String,
    #[serde(default)]
    auth_username: String,
    #[serde(default)]
    auth_password: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    passphrase: String,
    #[serde(default)]
    ext_type: String,
    #[serde(default)]
    clone_url: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    user_type: String,
    #[serde(default)]
    user_token: String,
    #[serde(default)]
    user_username: String,
    #[serde(default)]
    user_password: String,
    #[serde(default)]
    user_error: String,
    #[serde(default)]
    site_type: String,
    #[serde(default)]
    site_token: String,
    #[serde(default)]
    site_username: String,
    #[serde(default)]
    site_password: String,
    #[serde(default)]
    site_error: String,

    // duplicate
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    ref_status: u16,
    #[serde(default)]
    ref_body: String,
    #[serde(default)]
    create_status: u16,
    #[serde(default)]
    create_body: String,
    #[serde(default)]
    update_status: u16,
    #[serde(default)]
    update_body: String,
}

fn repo(case: &CorpusCase) -> SourceRepo {
    let ext_type = if case.ext_type.is_empty() {
        "github"
    } else {
        &case.ext_type
    };
    let clone = if case.clone_url.is_empty() {
        "https://github.com/acme/widgets.git"
    } else {
        &case.clone_url
    };
    let mut sources = BTreeMap::new();
    sources.insert(
        "1".to_owned(),
        RepoSource {
            id: "1".to_owned(),
            clone_url: clone.to_owned(),
        },
    );
    SourceRepo {
        id: 1,
        name: "acme/widgets".to_owned(),
        external_repo: ExternalRepoSpec {
            id: String::new(),
            service_type: ext_type.to_owned(),
            service_id: String::new(),
        },
        sources,
        metadata: json!({"NameWithOwner": "acme/widgets"}),
    }
}

fn auth_from(
    kind: &str,
    token: &str,
    username: &str,
    password: &str,
) -> Option<SourceAuth> {
    match kind {
        "" => None,
        "oauth" => Some(SourceAuth::OAuthToken(token.to_owned())),
        "basic" => Some(SourceAuth::Basic {
            username: username.to_owned(),
            password: password.to_owned(),
        }),
        other => panic!("unknown credential auth type {other}"),
    }
}

fn push_auth_from(
    kind: &str,
    case: &CorpusCase,
    token: &str,
    username: &str,
    password: &str,
) -> Option<PushAuthenticator> {
    match kind {
        "" => None,
        "oauth_ssh" => Some(PushAuthenticator::OAuthBearerTokenWithSSH {
            token: token.to_owned(),
            private_key: case.private_key.clone(),
            passphrase: case.passphrase.clone(),
        }),
        "basic_ssh" => Some(PushAuthenticator::BasicAuthWithSSH {
            username: username.to_owned(),
            password: password.to_owned(),
            private_key: case.private_key.clone(),
            passphrase: case.passphrase.clone(),
        }),
        _ => auth_from(kind, token, username, password)
            .as_ref()
            .and_then(push_authenticator_for_source_auth),
    }
}

fn out(case: &CorpusCase) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("name".into(), json!(case.name));
    map.insert("kind".into(), json!(case.kind));
    map
}

fn push_config_fields(
    map: &mut Map<String, Value>,
    config: Result<PushConfig, String>,
    non_retryable: bool,
) {
    match config {
        Ok(config) => {
            map.insert("ok".into(), json!(true));
            map.insert("remote_url".into(), json!(config.remote_url));
            if !config.private_key.is_empty() {
                map.insert("private_key".into(), json!(config.private_key));
            }
            if !config.passphrase.is_empty() {
                map.insert("passphrase".into(), json!(config.passphrase));
            }
        }
        Err(message) => {
            map.insert("ok".into(), json!(false));
            map.insert("error".into(), json!(message));
            map.insert("non_retryable".into(), json!(non_retryable));
        }
    }
}

fn build_opts_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(case);
    let repo = repo(case);
    let spec = ChangesetSpec {
        diff: case.diff.clone().into_bytes(),
        base_rev: case.base_rev.clone(),
        head_ref: case.head_ref.clone(),
        commit_message: case.commit_message.clone(),
        commit_author_name: case.author_name.clone(),
        commit_author_email: case.author_email.clone(),
        created_at: case.created_at.clone(),
        fork_namespace: None,
    };
    let push = if case.push_token.is_empty() {
        None
    } else {
        Some(PushConfig {
            remote_url: format!(
                "https://{}@github.com/acme/widgets.git",
                case.push_token
            ),
            private_key: String::new(),
            passphrase: String::new(),
        })
    };
    let opts = match &push {
        Some(push) => build_commit_opts_common(&repo, &spec, push),
        None => build_commit_opts_common(
            &repo,
            &spec,
            &PushConfig::default(),
        ),
    };
    map.insert("ok".into(), json!(true));
    map.insert("repo".into(), json!(opts.repo));
    map.insert("base_commit".into(), json!(opts.base_commit));
    map.insert(
        "patch".into(),
        json!(String::from_utf8_lossy(&opts.patch)),
    );
    map.insert("target_ref".into(), json!(opts.target_ref));
    map.insert("messages".into(), json!(opts.commit_info.messages));
    map.insert("author_name".into(), json!(opts.commit_info.author_name));
    map.insert("author_email".into(), json!(opts.commit_info.author_email));
    map.insert("date".into(), json!(opts.commit_info.date));
    map.insert(
        "patch_filenames_no_prefix".into(),
        json!(opts.patch_filenames_no_prefix),
    );
    if push.is_some() {
        if let Some(push) = opts.push {
            map.insert(
                "push".into(),
                json!({
                    "remote_url": push.remote_url,
                    "private_key": push.private_key,
                    "passphrase": push.passphrase,
                }),
            );
        }
    }
    map
}

fn push_config_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(case);
    let repo = repo(case);
    let push_auth = push_auth_from(
        &case.auth_type,
        case,
        &case.auth_token,
        &case.auth_username,
        &case.auth_password,
    );
    match gitserver_push_config(&repo, push_auth.as_ref()) {
        Ok(config) => push_config_fields(&mut map, Ok(config), false),
        Err(err) => push_config_fields(
            &mut map,
            Err(err.message),
            err.kind == SourceErrorKind::NonRetryable,
        ),
    }
    map
}

struct FakeCredentialStore {
    user: Result<Option<SourceAuth>, String>,
    site: Result<Option<SourceAuth>, String>,
}

#[async_trait::async_trait]
impl BatchesCredentialStore for FakeCredentialStore {
    async fn user_credential(
        &self,
        _user_id: i32,
        _repo: &SourceRepo,
    ) -> Result<Option<SourceAuth>, String> {
        self.user.clone()
    }

    async fn site_credential(
        &self,
        _repo: &SourceRepo,
    ) -> Result<Option<SourceAuth>, String> {
        self.site.clone()
    }
}

fn auth_kind(auth: &SourceAuth) -> &'static str {
    match auth {
        SourceAuth::OAuthToken(_) => "oauth",
        SourceAuth::Basic { .. } => "basic",
        _ => "other",
    }
}

async fn credential_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(case);
    let repo = repo(case);
    let user = if !case.user_error.is_empty() {
        Err(case.user_error.clone())
    } else {
        Ok(auth_from(
            &case.user_type,
            &case.user_token,
            &case.user_username,
            &case.user_password,
        ))
    };
    let site = if !case.site_error.is_empty() {
        Err(case.site_error.clone())
    } else {
        Ok(auth_from(
            &case.site_type,
            &case.site_token,
            &case.site_username,
            &case.site_password,
        ))
    };
    let store = FakeCredentialStore { user, site };

    let auth = match with_authenticator_for_user(&store, case.user_id, &repo).await {
        Ok(auth) => auth,
        Err(err) => {
            map.insert("ok".into(), json!(false));
            map.insert("error".into(), json!(err.message));
            map.insert(
                "non_retryable".into(),
                json!(err.kind == SourceErrorKind::NonRetryable),
            );
            return map;
        }
    };
    map.insert("ok".into(), json!(true));
    map.insert("auth".into(), json!(auth_kind(&auth)));
    match &auth {
        SourceAuth::OAuthToken(token) => {
            map.insert("token".into(), json!(token));
        }
        SourceAuth::Basic { username, .. } => {
            map.insert("username".into(), json!(username));
        }
        _ => {}
    }
    let push_auth = push_authenticator_for_source_auth(&auth);
    match gitserver_push_config(&repo, push_auth.as_ref()) {
        Ok(config) => {
            map.insert("push_ok".into(), json!(true));
            map.insert("remote_url".into(), json!(config.remote_url));
        }
        Err(err) => {
            map.insert("push_ok".into(), json!(false));
            map.insert("push_error".into(), json!(err.message));
            map.insert(
                "push_non_retryable".into(),
                json!(err.kind == SourceErrorKind::NonRetryable),
            );
        }
    }
    map
}

struct FakeDoer {
    case: CorpusCase,
    requests: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl HttpDoer for FakeDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let body = if request.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&request.body).unwrap_or(Value::Null)
        };
        let path = url::Url::parse(&request.url)
            .map(|url| url.path().to_owned())
            .unwrap_or_default();
        self.requests.lock().unwrap().push(json!({
            "method": request.method,
            "path": path,
            "body": body,
        }));

        let (status, body) = match request.method.as_str() {
            "GET" => (self.case.ref_status, self.case.ref_body.clone()),
            "POST" => (self.case.create_status, self.case.create_body.clone()),
            "PATCH" => (self.case.update_status, self.case.update_body.clone()),
            _ => (200, String::new()),
        };
        let status = if status == 0 { 200 } else { status };
        Ok(HttpResponse {
            status,
            headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body: body.into_bytes(),
        })
    }
}

async fn duplicate_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(case);
    let doer = Arc::new(FakeDoer {
        case: case.clone(),
        requests: Mutex::new(Vec::new()),
    });
    let api_url = url::Url::parse("https://api.github.com/").expect("api url");
    let auth = SourceAuth::OAuthToken("oracle-token".to_owned());
    let repo = SourceRepo {
        id: 1,
        name: "acme/widgets".to_owned(),
        external_repo: ExternalRepoSpec {
            id: String::new(),
            service_type: "github".to_owned(),
            service_id: String::new(),
        },
        sources: BTreeMap::new(),
        metadata: json!({"NameWithOwner": case.repo_name}),
    };
    let request = CreateCommitFromPatchRequest {
        commit_info: PatchCommitInfo {
            messages: vec!["commit message".to_owned()],
            ..Default::default()
        },
        ..Default::default()
    };

    let result = duplicate_commit(&api_url, &auth, doer.clone(), &request, &repo, &case.rev).await;
    let requests = doer.requests.lock().unwrap().clone();
    map.insert("requests".into(), Value::Array(requests));
    match result {
        Ok(commit) => {
            map.insert("ok".into(), json!(true));
            map.insert("sha".into(), json!(commit.sha));
            map.insert("verified".into(), json!(commit.verification.verified));
        }
        Err(err) => {
            map.insert("ok".into(), json!(false));
            map.insert("error".into(), json!(err.message));
        }
    }
    map
}

#[tokio::test]
async fn batches_push_diff() {
    if std::env::var("SG_M934B_PUSH_DIFF").as_deref() != Ok("1") {
        eprintln!("set SG_M934B_PUSH_DIFF=1 to run");
        return;
    }
    let corpus_path = std::env::var("M934B_CORPUS").expect("M934B_CORPUS");
    let out_dir = std::env::var("M934B_OUT").expect("M934B_OUT");
    std::fs::create_dir_all(&out_dir).expect("create out dir");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    for case in &corpus.cases {
        let output = match case.kind.as_str() {
            "build_opts" => build_opts_case(case),
            "push_config" => push_config_case(case),
            "credential" => credential_case(case).await,
            "duplicate" => duplicate_case(case).await,
            other => panic!("unknown case kind {other}"),
        };
        let encoded = serde_json::to_string_pretty(&output).expect("encode");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .expect("write output");
    }
}
