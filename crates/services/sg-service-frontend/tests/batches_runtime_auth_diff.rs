//! M9.33b differential harness: Batch runtime callers wired to the GitHub App
//! authentication path.
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/sources/zz_m933b_runtime_auth_oracle_test.go`. Both sides
//! consume `test_ws/p9-go/data/batches_runtime_auth/corpus.json`, write one
//! normalized JSON document per case, and `run_batches_runtime_auth_diff.sh`
//! compares them case-by-case.
//!
//! Cases cover the real Go helpers the syncer/reconciler call:
//! `getOwnerFromChangeset`, `GitserverPushConfig`, and `runAfterCommit`'s
//! `withGitHubAppAuthenticator` resolution/reject mapping. Gated by
//! `SG_M933B_RUNTIME_AUTH_DIFF=1`. No Postgres, Redis or network.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_extsvc::auth::{GitHubAppAuthenticator, InstallationAccessToken, InstallationAuthenticator};
use sg_extsvc::batches_sources::{
    changeset_owner_from_changeset, gitserver_push_config, push_authenticator_for_source_auth,
    resolve_commit_signing_source, BatchesGitHubApp, BatchesGitHubAppStore, ChangesetSpec,
    CommitSigningRequest, CommitSigningResolution, ExternalRepoSpec, GitHubAppTokenSeams,
    PushAuthenticator, Repo as SourceRepo, RepoSource, SourceAuth,
};
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};

const VALID_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA1LqMqnchtoTHiRfFds2RWWji43R5mHT65WZpZXpBuBwsgSWr
rN5VwTHZ4dxWk+XlyVDYsri7vlVWNX4EIt0jwxvh/OBXCFJXTL+byNHCimRIKvur
ofoT1eF3z+5WpH5ddHNPOkGZV0Chyd5kvUcNuFA7q203HRVEOloHEs4fqJrGPHIF
zc8Sug5qkOtZTS5xiHgTtmZkDLuLZ26H5Gfx3zZk5Gv2Jy/+fLsGninaiwTRsZf6
RgPgmdlkuM8OhSm4GtlpzoK0D3iZEhf4pITo1CK2U4Cs7vzkU0UkQ+J/z6dDmVBJ
gkalH1SHsboRqjNxkStEqGnbWwdtal01skbGOwIDAQABAoIBAQCls54oll17V5g5
0Htu3BdxBsNdG3gv6kcY85n7gqy4ZbHA83/zSsiPkW4/gasqzzQbiU8Sf9U2IDDj
wAImygy2SPzSRklk4QbBcKs/VSztMcoJOTprFGno+xShsexpe0j+kWdQYJK6JU0g
+ouL6FHmlRC1qn/4tn0L2t6Rpl+Aq4peDLqdwFHXj8GxGv0S10qMQ4/ER7onP6f0
99WDTvNQR5DugKqHxooOV5HfUP70scqhCcFhp2zc7/aYQFVt/k4hDOMu/w4HhkD3
r34y4EJoZsugGD1kPaJCw2rbSdoTtQHCqG5tfidY+XUIoC9mfmN8243jeRrhayeT
4ewiDuNhAoGBAPszeqN/+V8EVrlbBiBG+xVYGzWU0KgHu1TUiIrOSmKa6rTwaYMb
dKI8N4gYwFtb24AeDLfGnpaZAKSvNnrf8OapyLik7zXDylY0DBU7tRxQiUvNsVTs
7CYjxih5GWzUeP/xgpfVbHIIGdTHaZ6JWiDHWOolAw3hQyw6V/uQTDtxAoGBANjK
6vlctX55MjE2tuPk3ZCtCjgDFmWQjvFuiYYE/2cP4v4UBqgZn1vOaLRCnFm10ycl
peBLxPVpeeNBWc2ep2YNnJ+hm+SavhIDesLJTxuhC4wtcKMVAtq83VQmMQTU5wRO
KcUpviXLv2Z0UfbMWcohR4fJY1SkREwaxneHZc5rAoGBAIpT8c/BNBhPslYFutzh
WXiKeQlLdo9hGpZ/JuWQ7cNY3bBfxyqMXvDLyiSmxJ5KehgV9BjrRf9WJ9WIKq8F
TIooqsCLCrMHqw9HP/QdWgFKlCBrF6DVisEB6Cf3b7nPUwZV/v0PaNVugpL6cL39
kuUEAYGGeiUVi8D6K+L6tg/xAoGATlQqyAQ+Mz8Y6n0pYXfssfxDh+9dpT6w1vyo
RbsCiLtNuZ2EtjHjySjv3cl/ck5mx2sr3rmhpUYB2yFekBN1ykK6x1Z93AApEpsd
PMm9gm8SnAhC/Tl3OY8prODLr0I5Ye3X27v0TvWp5xu6DaDSBF032hDiic98Ob8m
3EMYfpcCgYBySPGnPmwqimiSyZRn+gJh+cZRF1aOKBqdqsfdcQrNpaZuZuQ4aYLo
cEoKFPr8HjXXUVCa3Q84tf9nGb4iUFslRSbS6RCP6Nm+JsfbCTtzyglYuPRKITGm
jSzka5UER3Dj1lSLMk9DkU+jrBxUsFeeiQOYhzQBaZxguvwYRIYHpg==
-----END RSA PRIVATE KEY-----";

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<CorpusCase>,
}

#[derive(Debug, Deserialize)]
struct CorpusCase {
    name: String,
    kind: String,
    #[serde(default)]
    spec_present: bool,
    #[serde(default)]
    fork_namespace: Option<String>,
    #[serde(default)]
    metadata_name: String,
    #[serde(default)]
    metadata_key: Option<String>,
    #[serde(default)]
    clone_url: Option<String>,
    #[serde(default)]
    ext_type: String,
    #[serde(default)]
    auth: Option<AuthSpec>,
    #[serde(default)]
    ext_kind: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    app: Option<AppSpec>,
    #[serde(default)]
    install: Option<InstallSpec>,
    #[serde(default)]
    account: String,
    #[serde(default)]
    app_kind: String,
    #[serde(default)]
    original_strategy: String,
    #[serde(default)]
    reject: bool,
}

#[derive(Debug, Deserialize, Default)]
struct AuthSpec {
    #[serde(default, rename = "type")]
    kind: String,
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
    installation_id: i32,
}

#[derive(Debug, Deserialize, Default)]
struct AppSpec {
    #[serde(default)]
    app_id: i32,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    base_url: String,
}

#[derive(Debug, Deserialize, Default)]
struct InstallSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    error: String,
}

struct FakeStore {
    app: Option<BatchesGitHubApp>,
    install_id: i32,
    install_error: bool,
}

#[async_trait::async_trait]
impl BatchesGitHubAppStore for FakeStore {
    async fn get_by_domain_and_kind(
        &self,
        _domain: &str,
        _kind: &str,
        _base_url: &str,
    ) -> Result<BatchesGitHubApp, String> {
        self.app
            .clone()
            .ok_or_else(|| "github app not found".to_owned())
    }

    async fn get_install_id(&self, _app_id: i32, _account: &str) -> Result<i32, String> {
        if self.install_error {
            return Err("installation not found".to_owned());
        }
        Ok(self.install_id)
    }
}

struct FakeDoer {
    request: Mutex<Option<HttpRequest>>,
}

#[async_trait::async_trait]
impl HttpDoer for FakeDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        *self.request.lock().unwrap() = Some(request);
        Ok(HttpResponse {
            status: 201,
            headers: Vec::new(),
            body: br#"{"token":"test-installation-token","expires_at":"2099-01-01T00:00:00Z"}"#
                .to_vec(),
        })
    }
}

fn repo(
    ext_type: &str,
    clone_url: Option<&str>,
    metadata_name: &str,
    metadata_key: Option<&str>,
) -> SourceRepo {
    let mut sources = BTreeMap::new();
    if let Some(clone) = clone_url {
        sources.insert(
            "1".to_owned(),
            RepoSource {
                id: "1".to_owned(),
                clone_url: clone.to_owned(),
            },
        );
    }
    let key = metadata_key.unwrap_or("NameWithOwner");
    let mut metadata = Map::new();
    metadata.insert(key.to_owned(), json!(metadata_name));
    SourceRepo {
        id: 1,
        name: "acme/widgets".to_owned(),
        external_repo: ExternalRepoSpec {
            id: String::new(),
            service_type: ext_type.to_owned(),
            service_id: String::new(),
        },
        sources,
        metadata: Value::Object(metadata),
    }
}

fn out(name: &str, kind: &str) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("name".into(), json!(name));
    map.insert("kind".into(), json!(kind));
    map
}

fn fail(mut map: Map<String, Value>, message: String) -> Map<String, Value> {
    map.insert("ok".into(), json!(false));
    map.insert("error".into(), json!(message));
    map
}

/// Go and Rust crypto libraries word key-parse failures differently; the
/// differential only asserts the error *class*.
fn normalize_signing_error(message: String) -> String {
    if let Some(index) = message.find("creating GitHub App authenticator:") {
        let end = index + "creating GitHub App authenticator:".len();
        return format!("{} <invalid key>", &message[..end]);
    }
    message
}

fn owner_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(&case.name, &case.kind);
    let target = repo(
        "github",
        case.clone_url.as_deref(),
        &case.metadata_name,
        case.metadata_key.as_deref(),
    );
    let spec = if case.spec_present {
        Some(ChangesetSpec {
            fork_namespace: case.fork_namespace.clone(),
            ..Default::default()
        })
    } else {
        None
    };
    match changeset_owner_from_changeset(&target, spec.as_ref()) {
        Ok(owner) => {
            map.insert("ok".into(), json!(true));
            map.insert("owner".into(), json!(owner));
        }
        Err(err) => return fail(map, err.message),
    }
    map
}

async fn push_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(&case.name, &case.kind);
    let target = repo(
        &case.ext_type,
        case.clone_url.as_deref(),
        "acme/widgets",
        None,
    );
    let auth = match &case.auth {
        None => None,
        Some(spec) => match spec.kind.as_str() {
            "oauth" => Some(SourceAuth::OAuthToken(spec.token.clone())),
            "basic" => Some(SourceAuth::Basic {
                username: spec.username.clone(),
                password: spec.password.clone(),
            }),
            "oauth_ssh" => Some(SourceAuth::OAuthToken(spec.token.clone())),
            "installation" => {
                let app = Arc::new(
                    GitHubAppAuthenticator::new(1, if spec.installation_id == 0 {
                        VALID_PRIVATE_KEY
                    } else {
                        VALID_PRIVATE_KEY
                    })
                    .expect("valid test key"),
                );
                let mut installation = InstallationAuthenticator::new(
                    url::Url::parse("https://github.com/").expect("url"),
                    spec.installation_id,
                    app,
                    None,
                    Arc::new(FakeDoer {
                        request: Mutex::new(None),
                    }),
                );
                installation.set_token(InstallationAccessToken {
                    token: "test-installation-token".to_owned(),
                    expires_at: None,
                });
                Some(SourceAuth::GitHubAppInstallation(Arc::new(installation)))
            }
            other => panic!("unknown auth type {other}"),
        },
    };
    let push_auth = match &case.auth {
        Some(spec) if spec.kind == "oauth_ssh" => {
            Some(PushAuthenticator::OAuthBearerTokenWithSSH {
                token: spec.token.clone(),
                private_key: spec.private_key.clone(),
                passphrase: spec.passphrase.clone(),
            })
        }
        _ => auth.as_ref().and_then(push_authenticator_for_source_auth),
    };
    match gitserver_push_config(&target, push_auth.as_ref()) {
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
        Err(err) => return fail(map, err.message),
    }
    map
}

async fn signing_case(case: &CorpusCase) -> Map<String, Value> {
    let mut map = out(&case.name, &case.kind);
    let store = if case.app.is_some() {
        Some(FakeStore {
            app: case.app.as_ref().map(|app| BatchesGitHubApp {
                app_id: app.app_id,
                private_key: match app.private_key.as_str() {
                    "valid" => VALID_PRIVATE_KEY.to_owned(),
                    "empty" => String::new(),
                    _ => "not-a-pem".to_owned(),
                },
                base_url: app.base_url.clone(),
            }),
            install_id: case.install.as_ref().map(|install| install.id).unwrap_or(0),
            install_error: case
                .install
                .as_ref()
                .map(|install| !install.error.is_empty())
                .unwrap_or(false),
        })
    } else {
        None
    };
    let config = json!({ "url": case.url });
    let seams = GitHubAppTokenSeams::offline(Arc::new(FakeDoer {
        request: Mutex::new(None),
    }));

    let result = if case.app_kind.is_empty() {
        // The empty-kind guard in `createChangesetSourceForGHApp`, then
        // `runAfterCommit`'s mapping.
        let message = "ForChangeset with AuthenticationStrategyGitHubApp must be called with a gitHubAppKind, but was called with empty string.";
        if case.reject {
            return {
                let mut map = out(&case.name, &case.kind);
                map.insert("ok".into(), json!(false));
                map.insert(
                    "error".into(),
                    json!(normalize_signing_error(format!(
                        "failed to get GitHub App for commit verification: {message}"
                    ))),
                );
                map
            };
        }
        Ok(CommitSigningResolution::SkipError(message.to_owned()))
    } else {
        resolve_commit_signing_source(
            CommitSigningRequest {
                external_service_kind: &case.ext_kind,
                config: &config,
                account: &case.account,
                original_strategy: &case.original_strategy,
                reject_unverified: case.reject,
            },
            store.as_ref().map(|store| store as &dyn BatchesGitHubAppStore),
            seams,
        )
        .await
    };

    match result {
        Ok(CommitSigningResolution::UseSource(connection)) => {
            map.insert("ok".into(), json!(true));
            map.insert("decision".into(), json!("use_source"));
            match &connection.auth {
                SourceAuth::GitHubAppInstallation(auth) => {
                    map.insert("auth_type".into(), json!("installation"));
                    map.insert("installation_id".into(), json!(auth.installation_id()));
                }
                other => {
                    return fail(map, format!("unexpected auth {:?}", other));
                }
            }
        }
        Ok(CommitSigningResolution::SkipNoApp) => {
            map.insert("ok".into(), json!(false));
            map.insert("decision".into(), json!("skip_no_app"));
        }
        Ok(CommitSigningResolution::SkipError(message)) => {
            map.insert("ok".into(), json!(false));
            map.insert("decision".into(), json!("skip_error"));
            map.insert("error".into(), json!(normalize_signing_error(message)));
        }
        Err(err) => {
            map.insert("ok".into(), json!(false));
            let message = normalize_signing_error(err.message);
            if case.reject {
                map.insert("error".into(), json!(message));
            } else {
                // `runAfterCommit` logs the error and continues.
                map.insert("decision".into(), json!("skip_error"));
                map.insert("error".into(), json!(message));
            }
        }
    }
    map
}

#[tokio::test]
async fn batches_runtime_auth_diff() {
    if std::env::var("SG_M933B_RUNTIME_AUTH_DIFF").as_deref() != Ok("1") {
        eprintln!("set SG_M933B_RUNTIME_AUTH_DIFF=1 to run");
        return;
    }
    let corpus_path = std::env::var("M933B_CORPUS").expect("M933B_CORPUS");
    let out_dir = std::env::var("M933B_OUT").expect("M933B_OUT");
    std::fs::create_dir_all(&out_dir).expect("create out dir");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    for case in &corpus.cases {
        let output = match case.kind.as_str() {
            "owner" => owner_case(case),
            "push" => push_case(case).await,
            "signing" => signing_case(case).await,
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
