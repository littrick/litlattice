//! M9.19b differential harness: external-service config → code-host source
//! connection resolution (`internal/batches/sources/{github,gitlab,
//! bitbucketserver}.go` constructors before their first request).
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/sources/zz_m919b_source_config_oracle_test.go`. Both sides
//! consume `test_ws/p9-go/data/batches_source_config/corpus.json`, write one
//! normalized JSON array, and `run_batches_source_config_diff.sh` compares them
//! with `cmp`.
//!
//! Gated by `SG_M919B_SOURCE_CONFIG=1`. No request is made: the Go side reads
//! the resolved authenticator/base URL off the real constructors, the Rust side
//! is pure.

use serde::Deserialize;
use serde_json::{Map, Value};
use sg_extsvc::batches_sources::{source_connection_from_config, SourceAuth};

#[derive(Debug, Deserialize)]
struct CorpusCase {
    id: String,
    kind: String,
    #[serde(default)]
    config: Value,
}

fn auth_json(auth: &SourceAuth) -> Value {
    let mut out = Map::new();
    match auth {
        SourceAuth::None => {
            out.insert("type".to_owned(), Value::String("none".to_owned()));
        }
        SourceAuth::OAuthToken(token) => {
            out.insert(
                "type".to_owned(),
                Value::String("oauth_bearer".to_owned()),
            );
            out.insert("token".to_owned(), Value::String(token.clone()));
        }
        SourceAuth::GitLabPrivateToken(token) => {
            out.insert(
                "type".to_owned(),
                Value::String("sudoable_token".to_owned()),
            );
            out.insert("token".to_owned(), Value::String(token.clone()));
        }
        SourceAuth::Basic { username, password } => {
            out.insert("type".to_owned(), Value::String("basic".to_owned()));
            out.insert("username".to_owned(), Value::String(username.clone()));
            out.insert("password".to_owned(), Value::String(password.clone()));
        }
        SourceAuth::BearerToken(token) => {
            out.insert(
                "type".to_owned(),
                Value::String("oauth_bearer".to_owned()),
            );
            out.insert("token".to_owned(), Value::String(token.clone()));
        }
        SourceAuth::GitHubAppInstallation(auth) => {
            out.insert(
                "type".to_owned(),
                Value::String("github_app_installation".to_owned()),
            );
            out.insert(
                "installation_id".to_owned(),
                Value::Number(auth.installation_id().into()),
            );
            out.insert(
                "base_url".to_owned(),
                Value::String(auth.base_url().to_string()),
            );
        }
    }
    Value::Object(out)
}

#[test]
fn source_config_diff() {
    if std::env::var("SG_M919B_SOURCE_CONFIG").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path =
        std::env::var("M919B_SOURCE_CONFIG_CORPUS").expect("M919B_SOURCE_CONFIG_CORPUS");
    let out_path = std::env::var("M919B_SOURCE_CONFIG_OUT").expect("M919B_SOURCE_CONFIG_OUT");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("decode corpus");

    let mut results = Vec::with_capacity(cases.len());
    for case in &cases {
        let mut out = Map::new();
        out.insert(
            "op".to_owned(),
            Value::String("source_connection".to_owned()),
        );
        out.insert("id".to_owned(), Value::String(case.id.clone()));
        out.insert("kind".to_owned(), Value::String(case.kind.clone()));

        match source_connection_from_config(&case.kind, &case.config) {
            Ok(connection) => {
                let base = match case.kind.as_str() {
                    "GITHUB" => sg_extsvc::github::api_root(&connection.base_url).0,
                    _ => connection.base_url.clone(),
                };
                let base = sg_extsvc::github::normalize_base_url(&base);
                out.insert("base_url".to_owned(), Value::String(base.to_string()));
                out.insert("auth".to_owned(), auth_json(&connection.auth));
            }
            Err(err) => {
                out.insert("error".to_owned(), Value::String(err.message));
            }
        }
        results.push(Value::Object(out));
    }

    let encoded = serde_json::to_string_pretty(&results).expect("encode results");
    std::fs::write(out_path, format!("{encoded}\n")).expect("write output");
}
