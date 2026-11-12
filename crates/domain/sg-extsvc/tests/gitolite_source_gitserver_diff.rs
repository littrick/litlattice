//! Differential harness for `gitolite_source.rs` with an injected gitserver
//! boundary (`ListGitoliteRepos`).
//!
//! Replays `test_ws/p9-go/data/gitolite_source/corpus.json` through the Rust
//! port and writes one JSON envelope per case under `<M915A_OUT>/rust/`; the Go
//! oracle (`internal/repos/zz_m9_15a_gitolite_source_oracle_test.go`) writes
//! `<M915A_OUT>/go/`. `run_gitolite_source_gitserver_diff.sh` compares them.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_extsvc::gitolite::{Repo as GitoliteRepo, GitoliteRepoLister};
use sg_extsvc::gitolite_source::GitoliteSource;
use sg_extsvc::{Source, SourceService};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    config: Value,
    #[serde(default)]
    repos: Vec<RepoFixture>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    unrestricted: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RepoFixture {
    name: String,
    url: String,
}

struct FakeLister {
    repos: Vec<GitoliteRepo>,
    error: Option<String>,
}

#[async_trait]
impl GitoliteRepoLister for FakeLister {
    async fn list_repos(&self, _host: &str) -> Result<Vec<GitoliteRepo>, String> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.repos.clone()),
        }
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
fn gitolite_source_gitserver_diff() {
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
        let service = SourceService {
            id: 7,
            kind: sg_extsvc::KIND_GITOLITE.to_owned(),
            display_name: "gitolite".to_owned(),
            config: serde_json::to_string(&case.config).expect("config json"),
            encryption_key_id: String::new(),
            unrestricted: case.unrestricted,
        };
        let lister = Arc::new(FakeLister {
            repos: case
                .repos
                .iter()
                .map(|repo| GitoliteRepo {
                    name: repo.name.clone(),
                    url: repo.url.clone(),
                })
                .collect(),
            error: case.error.clone(),
        });
        let source = GitoliteSource::new(service, lister).expect("construct source");
        let results = runtime.block_on(source.list_repos());
        let envelope = json!({"results": collect(results)});
        let encoded = serde_json::to_vec(&envelope).expect("encode envelope");
        std::fs::write(rust_dir.join(format!("{}.json", case.name)), encoded)
            .expect("write envelope");
        println!("RUST {}", case.name);
    }
}
