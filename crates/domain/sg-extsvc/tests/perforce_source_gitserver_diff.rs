//! Differential harness for `perforce_source.rs` with an injected gitserver
//! boundary (`IsPerforcePathCloneable`).
//!
//! Replays `test_ws/p9-go/data/perforce_source/corpus.json` through the Rust
//! port and writes one JSON envelope per case under `<M915A_OUT>/rust/`; the Go
//! oracle (`internal/repos/zz_m9_15a_perforce_source_oracle_test.go`) writes
//! `<M915A_OUT>/go/`. `run_perforce_source_gitserver_diff.sh` compares them.
//!
//! `CheckConnection` is not in the corpus: the baseline's `PerforceSource`
//! builds an unscoped client internally (`gitserver.NewClient`), so it cannot
//! be driven against a fake offline. The Rust `check_connection` wrap is
//! covered by unit tests and the repo-updater in-process gRPC test.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::perforce::PerforceGitserver;
use sg_extsvc::perforce::PerforceConnectionDetails;
use sg_extsvc::perforce_source::PerforceSource;
use sg_extsvc::{Source, SourceService};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    config: Value,
}

struct FakeGitserver {
    cloneable_errors: BTreeMap<String, String>,
}

#[async_trait]
impl PerforceGitserver for FakeGitserver {
    async fn is_perforce_path_cloneable(
        &self,
        _connection: &PerforceConnectionDetails,
        depot: &str,
    ) -> Result<(), String> {
        match self.cloneable_errors.get(depot) {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    async fn check_perforce_credentials(
        &self,
        _connection: &PerforceConnectionDetails,
    ) -> Result<(), String> {
        Ok(())
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
        "externalRepo": {
            "id": repo.external_repo.id,
            "serviceType": repo.external_repo.service_type,
            "serviceID": repo.external_repo.service_id,
        },
        "sources": Value::Object(sources),
        "metadata": repo.metadata.clone().unwrap_or(Value::Null),
        "private": repo.private,
    })
}

fn collect(results: Vec<sg_extsvc::SourceResult>) -> Vec<Value> {
    results
        .into_iter()
        .map(|result| match (result.repo, result.err) {
            (Some(repo), _) => json!({"repo": repo_json(&repo)}),
            (None, Some(error)) => json!({"error": error}),
            (None, None) => json!({"error": ""}),
        })
        .collect()
}

#[test]
fn perforce_source_gitserver_diff() {
    let (Ok(corpus_path), Ok(out_dir)) = (
        std::env::var("M915A_CORPUS"),
        std::env::var("M915A_OUT"),
    ) else {
        return;
    };
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let rust_dir = std::path::Path::new(&out_dir).join("rust");
    std::fs::create_dir_all(&rust_dir).expect("create output dir");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case in &cases {
        let cloneable_errors: BTreeMap<String, String> = case
            .config
            .get("cloneableErrors")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .filter_map(|(depot, message)| {
                        message.as_str().map(|message| (depot.clone(), message.to_owned()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        // `newPerforceSource` in the baseline: one service, one connection.
        let service = SourceService {
            id: 9,
            kind: sg_extsvc::KIND_PERFORCE.to_owned(),
            display_name: "perforce".to_owned(),
            config: serde_json::to_string(&case.config).expect("config json"),
            encryption_key_id: String::new(),
            unrestricted: false,
        };
        let source = PerforceSource::new_with_gitserver(
            service,
            Arc::new(FakeGitserver { cloneable_errors }),
        )
        .expect("construct source");
        let results = runtime.block_on(source.list_repos());
        let envelope = json!({"results": collect(results)});
        let encoded = serde_json::to_vec(&envelope).expect("encode envelope");
        std::fs::write(rust_dir.join(format!("{}.json", case.name)), encoded)
            .expect("write envelope");
        println!("RUST {}", case.name);
    }
}
