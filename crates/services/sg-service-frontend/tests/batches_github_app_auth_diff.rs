//! M9.32b differential harness: Batch Changes GitHub App runtime
//! authentication (`internal/batches/sources.withGitHubAppAuthenticator`).
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/sources/zz_m932b_github_app_auth_oracle_test.go`. Both
//! sides consume `test_ws/p9-go/data/batches_github_app_auth/corpus.json`,
//! write one normalized JSON document per case, and
//! `run_batches_github_app_auth_diff.sh` compares them case-by-case.
//!
//! Gated by `SG_M932B_GITHUB_APP_AUTH_DIFF=1`. No Postgres, Redis or network:
//! the app store and HTTP doer are in-memory fakes; `refresh` exercises the
//! injected transport only.

use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_extsvc::auth::Authenticator as _;
use sg_extsvc::batches_sources::{
    with_github_app_authenticator, BatchesGitHubApp, BatchesGitHubAppStore, GitHubAppTokenSeams,
    SourceAuth, EXTERNAL_SERVICE_NOT_GITHUB, NO_GITHUB_APP_CONFIGURED,
    NO_GITHUB_APP_INSTALLATION,
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
    ext_kind: String,
    config: Value,
    app: Option<AppSpec>,
    install: Option<InstallSpec>,
    account: String,
    kind: String,
}

#[derive(Debug, Deserialize, Default)]
struct AppSpec {
    #[serde(default)]
    app_id: i32,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    base_url: String,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Deserialize, Default)]
struct InstallSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    error: String,
}

fn private_key(kind: &str) -> String {
    match kind {
        "valid" => VALID_PRIVATE_KEY.to_owned(),
        "empty" => String::new(),
        _ => "not-a-pem".to_owned(),
    }
}

struct FakeStore {
    app: Option<BatchesGitHubApp>,
    app_error: bool,
    install_id: i32,
    install_error: bool,
    app_call: Mutex<Option<Value>>,
    install_call: Mutex<Option<Value>>,
}

#[async_trait::async_trait]
impl BatchesGitHubAppStore for FakeStore {
    async fn get_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<BatchesGitHubApp, String> {
        *self.app_call.lock().unwrap() = Some(json!({
            "domain": domain,
            "kind": kind,
            "base_url": base_url,
        }));
        if self.app_error {
            return Err("github app not found".to_owned());
        }
        self.app.clone().ok_or_else(|| "github app not found".to_owned())
    }

    async fn get_install_id(&self, app_id: i32, account: &str) -> Result<i32, String> {
        *self.install_call.lock().unwrap() = Some(json!({
            "app_id": app_id,
            "account": account,
        }));
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

fn classify_error(err: &sg_extsvc::batches_sources::SourceError) -> Map<String, Value> {
    let mut out = Map::new();
    if err.message == EXTERNAL_SERVICE_NOT_GITHUB {
        out.insert("error_kind".into(), json!("external_service_not_github"));
        out.insert("error_message".into(), json!(err.message));
        return out;
    }
    if err.message == NO_GITHUB_APP_CONFIGURED {
        out.insert("error_kind".into(), json!("no_github_app_configured"));
        out.insert("error_message".into(), json!(err.message));
        return out;
    }
    if err.message == NO_GITHUB_APP_INSTALLATION {
        out.insert("error_kind".into(), json!("no_github_app_installation"));
        out.insert("error_message".into(), json!(err.message));
        return out;
    }
    for (prefix, kind) in [
        (
            "parsing GitHub connection URL",
            "parsing_github_connection_url",
        ),
        (
            "creating GitHub App authenticator",
            "creating_github_app_authenticator",
        ),
        (
            "parsing GitHub App base URL",
            "parsing_github_app_base_url",
        ),
        (
            "loading external service configuration",
            "loading_external_service_configuration",
        ),
    ] {
        if err.message.starts_with(prefix) {
            out.insert("error_kind".into(), json!(kind));
            out.insert("error_prefix".into(), json!(prefix));
            return out;
        }
    }
    out.insert("error_kind".into(), json!("other"));
    out.insert("error_message".into(), json!(err.message));
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_github_app_auth_diff() {
    if std::env::var("SG_M932B_GITHUB_APP_AUTH_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M932B_CORPUS").expect("M932B_CORPUS");
    let out_dir = std::env::var("M932B_OUT").expect("M932B_OUT");
    let nonce = std::env::var("M932B_NONCE").unwrap_or_default();

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("decode corpus");
    std::fs::create_dir_all(&out_dir).expect("create out dir");

    for case in &corpus.cases {
        let config_text = serde_json::to_string(&case.config)
            .expect("encode config")
            .replace("{{nonce}}", &nonce);
        let config: Value = serde_json::from_str(&config_text).expect("decode config");

        let app = case.app.as_ref().and_then(|app| {
            if !app.error.is_empty() {
                return None;
            }
            Some(BatchesGitHubApp {
                app_id: app.app_id,
                private_key: private_key(&app.private_key),
                base_url: app.base_url.replace("{{nonce}}", &nonce),
            })
        });
        let app_error = case.app.as_ref().map_or(true, |app| !app.error.is_empty());

        let (install_id, install_error) = match case.install.as_ref() {
            Some(install) if install.error.is_empty() => (install.id, false),
            Some(_) => (0, true),
            None => (0, true),
        };

        let store = FakeStore {
            app,
            app_error,
            install_id,
            install_error,
            app_call: Mutex::new(None),
            install_call: Mutex::new(None),
        };
        let doer = Arc::new(FakeDoer {
            request: Mutex::new(None),
        });
        let seams = GitHubAppTokenSeams::offline(doer.clone());

        let outcome = with_github_app_authenticator(
            &case.ext_kind,
            &config,
            &store,
            &case.account,
            &case.kind,
            seams,
        )
        .await;

        let mut result = Map::new();
        match outcome {
            Ok(auth) => {
                result.insert("ok".into(), json!(true));
                result.insert("auth_type".into(), json!("installation"));
                match auth {
                    SourceAuth::GitHubAppInstallation(mut installation) => {
                        result.insert(
                            "installation_id".into(),
                            json!(installation.installation_id()),
                        );
                        result.insert("auth_hash".into(), json!(installation.hash()));
                        let initial_token = installation.get_token().token;
                        result.insert(
                            "needs_refresh_initial".into(),
                            json!(installation.needs_refresh()),
                        );
                        if !initial_token.is_empty() {
                            result.insert("initial_token".into(), json!(initial_token));
                        }
                        let auth = Arc::get_mut(&mut installation).expect("sole owner");
                        if let Err(err) = auth.refresh().await {
                            panic!("refresh failed for {}: {err}", case.name);
                        }
                        let recorded = doer
                            .request
                            .lock()
                            .unwrap()
                            .clone()
                            .expect("refresh request recorded");
                        let bearer_jwt = recorded
                            .headers
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case("Authorization"))
                            .map(|(_, value)| {
                                value.starts_with("Bearer ") && value.matches('.').count() == 2
                            })
                            .unwrap_or(false);
                        result.insert(
                            "refresh".into(),
                            json!({
                                "method": recorded.method,
                                "url": recorded.url,
                                "bearer_jwt": bearer_jwt,
                                "token": auth.get_token().token,
                                "needs_refresh_after": auth.needs_refresh(),
                            }),
                        );
                    }
                    other => panic!(
                        "unexpected authenticator for {}: {other:?}",
                        case.name
                    ),
                }
            }
            Err(err) => {
                result = classify_error(&err);
                result.insert("ok".into(), json!(false));
            }
        }

        let mut out = Map::new();
        out.insert("name".into(), json!(case.name));
        out.insert(
            "app_lookup".into(),
            store.app_call.lock().unwrap().clone().unwrap_or(Value::Null),
        );
        out.insert(
            "install_lookup".into(),
            store
                .install_call
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(Value::Null),
        );
        out.insert("result".into(), Value::Object(result));

        let encoded = serde_json::to_string_pretty(&Value::Object(out)).expect("encode output");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            format!("{encoded}\n"),
        )
        .expect("write output");
    }
}
