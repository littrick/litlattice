//! M9.17b differential harness: `sg-extsvc::batches_sources` pure helpers and
//! the source abstraction (clone URL selection, push config, fork handling,
//! draft capability, kind dispatch errors).
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/sources/zz_m917b_sources_oracle_test.go`. Both sides
//! consume `test_ws/p9-go/data/batches_sources/corpus.json`, write one
//! normalized JSON array, and `run_batches_sources_diff.sh` compares them with
//! `cmp`.
//!
//! Gated by `SG_M917B_SOURCES=1`.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_extsvc::batches_sources::{
    abbreviate_ref, build_commit_opts_common, build_create_pull_request_input, copy_repo_as_fork,
    default_fork_name, ensure_ref_prefix, get_clone_url, get_remote_repo, gitserver_push_config,
    is_outdated, set_basic_auth, set_oauth_token_auth, to_draft_changeset_source,
    unsupported_authenticator_error, Changeset, ChangesetMetadata, ChangesetSpec,
    CreateCommitFromPatchRequest, ExternalRepoSpec, PushAuthenticator, PushConfig, Repo, RepoSource,
    SourceChangeset, SourceError,
};

#[derive(Debug, Default, Deserialize)]
struct CorpusSourceInfo {
    #[serde(default)]
    id: String,
    #[serde(default)]
    clone_url: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusRepo {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    external_service_type: String,
    #[serde(default)]
    graphql_id: String,
    #[serde(default)]
    sources: BTreeMap<String, CorpusSourceInfo>,
    #[serde(default)]
    metadata: Value,
}

impl CorpusRepo {
    fn to_repo(&self) -> Repo {
        Repo {
            id: self.id,
            name: self.name.clone(),
            external_repo: ExternalRepoSpec {
                id: String::new(),
                service_type: self.external_service_type.clone(),
                service_id: String::new(),
            },
            sources: self
                .sources
                .iter()
                .map(|(urn, source)| {
                    (
                        urn.clone(),
                        RepoSource {
                            id: source.id.clone(),
                            clone_url: source.clone_url.clone(),
                        },
                    )
                })
                .collect(),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct CorpusChangeset {
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    base_ref: String,
    #[serde(default)]
    external_fork_namespace: String,
    #[serde(default)]
    external_fork_name: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusMetadata {
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    base_ref_name: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusSpec {
    #[serde(default)]
    fork_namespace: Option<String>,
    #[serde(default)]
    diff: String,
    #[serde(default)]
    base_rev: String,
    #[serde(default)]
    head_ref: String,
    #[serde(default)]
    commit_message: String,
    #[serde(default)]
    commit_author_name: String,
    #[serde(default)]
    commit_author_email: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusPush {
    #[serde(default)]
    remote_url: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    passphrase: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusCase {
    #[serde(default)]
    op: String,
    #[serde(default)]
    r#ref: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    passphrase: String,
    #[serde(default)]
    auth_kind: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    ext_svc_type: String,
    #[serde(default)]
    name_and_owner: String,
    #[serde(default)]
    fork_name_and_owner: String,
    #[serde(default)]
    repo: CorpusRepo,
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    head_ref: String,
    #[serde(default)]
    base_ref: String,
    #[serde(default)]
    changeset: CorpusChangeset,
    #[serde(default)]
    metadata: CorpusMetadata,
    #[serde(default)]
    spec: Option<CorpusSpec>,
    #[serde(default)]
    supports_fork: bool,
    #[serde(default)]
    supports_draft: bool,
    #[serde(default)]
    source: String,
    #[serde(default)]
    have: String,
    #[serde(default)]
    push: CorpusPush,
}

fn err_value(err: &SourceError) -> Value {
    Value::String(err.message.clone())
}

fn error_string(err: &SourceError) -> Value {
    Value::String(err.to_string())
}

fn auth_from_case(case: &CorpusCase) -> Option<PushAuthenticator> {
    match case.auth_kind.as_str() {
        "oauth" => Some(PushAuthenticator::OAuthBearerToken {
            token: case.token.clone(),
        }),
        "oauth_ssh" => Some(PushAuthenticator::OAuthBearerTokenWithSSH {
            token: case.token.clone(),
            private_key: case.private_key.clone(),
            passphrase: case.passphrase.clone(),
        }),
        "basic" => Some(PushAuthenticator::BasicAuth {
            username: case.username.clone(),
            password: case.password.clone(),
        }),
        "basic_ssh" => Some(PushAuthenticator::BasicAuthWithSSH {
            username: case.username.clone(),
            password: case.password.clone(),
            private_key: case.private_key.clone(),
            passphrase: case.passphrase.clone(),
        }),
        _ => None,
    }
}

/// Minimal `ChangesetSource` for `get_remote_repo`/`to_draft_changeset_source`.
struct FakeSource {
    forkable: bool,
    draftable: bool,
}

#[async_trait]
impl sg_extsvc::batches_sources::ChangesetSource for FakeSource {
    fn kind(&self) -> &'static str {
        "fake"
    }

    async fn load_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Ok(())
    }

    async fn create_changeset(&self, _cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        Ok(false)
    }

    async fn update_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Ok(())
    }

    async fn close_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Ok(())
    }

    async fn reopen_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Ok(())
    }

    async fn create_comment(
        &self,
        _cs: &SourceChangeset,
        _text: &str,
    ) -> Result<(), SourceError> {
        Ok(())
    }

    async fn merge_changeset(
        &self,
        _cs: &mut SourceChangeset,
        _squash: bool,
    ) -> Result<(), SourceError> {
        Ok(())
    }

    fn supports_fork(&self) -> bool {
        self.forkable
    }

    fn supports_draft(&self) -> bool {
        self.draftable
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let mut parts = target_repo.name.splitn(2, '/');
        let owner = parts.next().unwrap_or_default();
        let repo_name = parts.next().unwrap_or_default();
        let fork_namespace = namespace.cloned().unwrap_or_else(|| owner.to_owned());
        let fork_name = name
            .cloned()
            .unwrap_or_else(|| default_fork_name(owner, repo_name));
        Ok(Repo {
            name: format!("{fork_namespace}/{fork_name}"),
            metadata: json!({"fork": true}),
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn batches_sources_diff() {
    if std::env::var("SG_M917B_SOURCES").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M917B_CORPUS").expect("M917B_CORPUS");
    let out_path = std::env::var("M917B_SOURCES_OUT").expect("M917B_SOURCES_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("decode corpus");

    let mut results: Vec<Value> = Vec::with_capacity(cases.len());
    for case in &cases {
        let mut out = Map::new();
        out.insert("op".to_owned(), Value::String(case.op.clone()));
        match case.op.as_str() {
            "abbreviate_ref" => {
                out.insert("result".to_owned(), Value::String(abbreviate_ref(&case.r#ref)));
            }
            "ensure_ref_prefix" => {
                out.insert(
                    "result".to_owned(),
                    Value::String(ensure_ref_prefix(&case.r#ref)),
                );
            }
            "default_fork_name" => {
                out.insert(
                    "result".to_owned(),
                    Value::String(default_fork_name(&case.namespace, &case.name)),
                );
            }
            "get_clone_url" => match get_clone_url(&case.repo.to_repo()) {
                Ok(url) => {
                    out.insert("url".to_owned(), Value::String(url.to_string()));
                    out.insert("error".to_owned(), Value::Null);
                }
                Err(err) => {
                    out.insert("error".to_owned(), err_value(&err));
                }
            },
            "gitserver_push_config" => {
                let auth = auth_from_case(case);
                match gitserver_push_config(&case.repo.to_repo(), auth.as_ref()) {
                    Ok(config) => {
                        out.insert("remote_url".to_owned(), Value::String(config.remote_url));
                        out.insert("private_key".to_owned(), Value::String(config.private_key));
                        out.insert("passphrase".to_owned(), Value::String(config.passphrase));
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "set_oauth_token_auth" => {
                let mut url = url::Url::parse(&case.url).expect("parse url");
                match set_oauth_token_auth(&mut url, &case.ext_svc_type, &case.token) {
                    Ok(()) => {
                        out.insert("url".to_owned(), Value::String(url.to_string()));
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "set_basic_auth" => {
                let mut url = url::Url::parse(&case.url).expect("parse url");
                match set_basic_auth(&mut url, &case.ext_svc_type, &case.username, &case.password) {
                    Ok(()) => {
                        out.insert("url".to_owned(), Value::String(url.to_string()));
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "copy_repo_as_fork" => {
                match copy_repo_as_fork(
                    &case.repo.to_repo(),
                    case.repo.metadata.clone(),
                    &case.name_and_owner,
                    &case.fork_name_and_owner,
                ) {
                    Ok(fork) => {
                        let sources: Map<String, Value> = fork
                            .sources
                            .iter()
                            .map(|(urn, source)| {
                                (urn.clone(), Value::String(source.clone_url.clone()))
                            })
                            .collect();
                        out.insert("sources".to_owned(), Value::Object(sources));
                        out.insert("metadata".to_owned(), fork.metadata);
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "get_remote_repo" => {
                let source = FakeSource {
                    forkable: case.supports_fork,
                    draftable: false,
                };
                let changeset = Changeset {
                    external_fork_namespace: case.changeset.external_fork_namespace.clone(),
                    external_fork_name: case.changeset.external_fork_name.clone(),
                    ..Default::default()
                };
                let spec = case.spec.as_ref().map(|spec| ChangesetSpec {
                    fork_namespace: spec.fork_namespace.clone(),
                    ..Default::default()
                });
                match get_remote_repo(
                    &source,
                    &case.repo.to_repo(),
                    &changeset,
                    spec.as_ref(),
                )
                .await
                {
                    Ok(repo) => {
                        out.insert("name".to_owned(), Value::String(repo.name));
                        out.insert("metadata".to_owned(), repo.metadata);
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "build_create_pull_request_input" => {
                let target = Repo {
                    id: case.repo.id,
                    name: case.repo.name.clone(),
                    metadata: json!({"id": case.repo.graphql_id}),
                    ..Default::default()
                };
                let cs = SourceChangeset {
                    title: case.title.clone(),
                    body: case.body.clone(),
                    head_ref: case.head_ref.clone(),
                    base_ref: case.base_ref.clone(),
                    remote_repo: target.clone(),
                    target_repo: target,
                    ..Default::default()
                };
                match build_create_pull_request_input(&cs) {
                    Ok(input) => {
                        out.insert("input".to_owned(), input);
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "is_outdated" => {
                let current = ChangesetMetadata {
                    title: case.metadata.title.clone(),
                    body: case.metadata.body.clone(),
                    base_ref_name: ensure_ref_prefix(&case.metadata.base_ref_name),
                    ..Default::default()
                };
                let cs = SourceChangeset {
                    title: case.changeset.title.clone(),
                    body: case.changeset.body.clone(),
                    base_ref: case.changeset.base_ref.clone(),
                    ..Default::default()
                };
                out.insert("outdated".to_owned(), Value::Bool(is_outdated(&current, &cs)));
                out.insert("error".to_owned(), Value::Null);
            }
            "build_commit_opts_common" => {
                let spec = ChangesetSpec {
                    diff: case.spec.as_ref().unwrap().diff.clone().into_bytes(),
                    base_rev: case.spec.as_ref().unwrap().base_rev.clone(),
                    head_ref: case.spec.as_ref().unwrap().head_ref.clone(),
                    commit_message: case.spec.as_ref().unwrap().commit_message.clone(),
                    commit_author_name: case.spec.as_ref().unwrap().commit_author_name.clone(),
                    commit_author_email: case.spec.as_ref().unwrap().commit_author_email.clone(),
                    ..Default::default()
                };
                let push = PushConfig {
                    remote_url: case.push.remote_url.clone(),
                    private_key: case.push.private_key.clone(),
                    passphrase: case.push.passphrase.clone(),
                };
                let req: CreateCommitFromPatchRequest =
                    build_commit_opts_common(&case.repo.to_repo(), &spec, &push);
                out.insert("repo".to_owned(), Value::String(req.repo));
                out.insert("base_commit".to_owned(), Value::String(req.base_commit));
                out.insert(
                    "patch".to_owned(),
                    Value::String(String::from_utf8_lossy(&req.patch).into_owned()),
                );
                out.insert("target_ref".to_owned(), Value::String(req.target_ref));
                out.insert(
                    "messages".to_owned(),
                    Value::Array(
                        req.commit_info
                            .messages
                            .iter()
                            .map(|message| Value::String(message.clone()))
                            .collect(),
                    ),
                );
                out.insert(
                    "author_name".to_owned(),
                    Value::String(req.commit_info.author_name),
                );
                out.insert(
                    "author_email".to_owned(),
                    Value::String(req.commit_info.author_email),
                );
                out.insert(
                    "patch_filenames_no_prefix".to_owned(),
                    Value::Bool(req.patch_filenames_no_prefix),
                );
                out.insert(
                    "push_remote_url".to_owned(),
                    Value::String(req.push.map(|push| push.remote_url).unwrap_or_default()),
                );
            }
            "to_draft_source" => {
                let source = FakeSource {
                    forkable: false,
                    draftable: case.supports_draft,
                };
                match to_draft_changeset_source(&source) {
                    Ok(()) => {
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            "unsupported_authenticator" => {
                let err = unsupported_authenticator_error(&case.source, &case.have);
                out.insert("error".to_owned(), error_string(&err));
            }
            other => panic!("unknown op {other:?}"),
        }
        results.push(Value::Object(out));
    }

    let encoded = serde_json::to_string_pretty(&results).expect("encode results");
    std::fs::write(out_path, format!("{encoded}\n")).expect("write output");
}
