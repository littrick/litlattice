//! M9.15a differential: `ListGitolite` decoding, host validation and gRPC
//! error mapping against the Go baseline.
//!
//! The corpus (`data/gitserver_list_gitolite/corpus.json` in the test
//! workspace) has three case kinds:
//! - `decode`: `gitolite.decodeRepos` vs [`sg_service_gitserver::gitolite::decode_repos`].
//! - `validation`: `gitoliteFetcher.listRepos` vs [`GitoliteFetcher`] with an
//!   injected [`SshInfoCommand`].
//! - `service`: the gRPC handler, exercised through an in-process tonic
//!   server with a fake [`GitoliteRepoLister`] (no SSH/network).
//!
//! The Go oracle is an untracked, env-gated test; the `run_*` script compares
//! `<M915A_OUT>/{go,rust}/<case>.json`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_proto::gitserver as gpb;
use sg_service_gitserver::gitolite::{
    decode_repos, GitoliteFetcher, GitoliteRepo, GitoliteRepoLister, SshGitoliteLister,
    SshInfoCommand, SshInfoOutput,
};
use sg_service_gitserver::grpc::Grpc;

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    info: String,
    #[serde(default)]
    repos: Vec<RepoFixture>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepoFixture {
    name: String,
    url: String,
}

impl RepoFixture {
    fn to_repo(&self) -> GitoliteRepo {
        GitoliteRepo {
            name: self.name.clone(),
            url: self.url.clone(),
        }
    }
}

fn repos_json(repos: &[GitoliteRepo]) -> Value {
    Value::Array(
        repos
            .iter()
            .map(|repo| json!({"name": repo.name, "url": repo.url}))
            .collect(),
    )
}

struct FakeCommand {
    info: String,
    error: Option<String>,
}

#[async_trait]
impl SshInfoCommand for FakeCommand {
    async fn run(&self, _host: &str) -> SshInfoOutput {
        SshInfoOutput {
            stdout: self.info.as_bytes().to_vec(),
            error: self.error.clone(),
        }
    }
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

fn grpc_with_lister(lister: Arc<dyn GitoliteRepoLister>) -> Grpc {
    Grpc {
        repos_dir: Arc::new(PathBuf::from("/nonexistent-m915a")),
        tasks: tokio_util::task::TaskTracker::new(),
        locker: sg_service_gitserver::locker::RepositoryLocker::new(),
        fetch: None,
        cloneable: None,
        patch: None,
        perforce: None,
        gitolite: lister,
    }
}

async fn run_service_case(case: &Case) -> Value {
    // Go's `defaultGitolite` is always a `gitoliteFetcher`, so the handler
    // validates the host before the fake client sees it.
    let lister: Arc<dyn GitoliteRepoLister> = Arc::new(GitoliteFetcher::new(Arc::new(FakeLister {
        repos: case.repos.iter().map(RepoFixture::to_repo).collect(),
        error: case.error.clone(),
    })));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");
    let server = tonic::transport::Server::builder().add_service(
        gpb::gitserver_service_server::GitserverServiceServer::new(grpc_with_lister(lister)),
    );
    let handle = tokio::spawn(async move { server.serve_with_incoming(incoming).await });
    let mut client = gpb::gitserver_service_client::GitserverServiceClient::connect(format!(
        "http://{addr}"
    ))
    .await
    .expect("connect");
    let response = client
        .list_gitolite(gpb::ListGitoliteRequest {
            gitolite_host: case.host.clone(),
        })
        .await;
    handle.abort();
    match response {
        Ok(response) => {
            let repos: Vec<GitoliteRepo> = response
                .into_inner()
                .repos
                .into_iter()
                .map(|repo| GitoliteRepo {
                    name: repo.name,
                    url: repo.url,
                })
                .collect();
            json!({"repos": repos_json(&repos)})
        }
        Err(status) => json!({
            "code": format!("{:?}", status.code()),
            "error": status.message(),
        }),
    }
}

async fn run_case(case: &Case) -> Value {
    match case.kind.as_str() {
        "decode" => json!({"repos": repos_json(&decode_repos(&case.host, &case.info))}),
        "validation" => {
            let fetcher = GitoliteFetcher::new(Arc::new(SshGitoliteLister::new(Arc::new(
                FakeCommand {
                    info: String::new(),
                    error: None,
                },
            ))));
            match fetcher.list_repos(&case.host).await {
                Ok(repos) => json!({"repos": repos_json(&repos)}),
                Err(error) => json!({"error": error}),
            }
        }
        "service" => run_service_case(case).await,
        other => panic!("unknown case kind {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_gitolite_matches_the_go_oracle() {
    let (Ok(corpus), Ok(out)) = (
        std::env::var("M915A_CORPUS"),
        std::env::var("M915A_OUT"),
    ) else {
        eprintln!("M915A_CORPUS/M915A_OUT unset; skipping");
        return;
    };
    let raw = std::fs::read_to_string(&corpus).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let out = PathBuf::from(out).join("rust");
    std::fs::create_dir_all(&out).expect("mkdir rust");

    for case in &cases {
        let envelope = run_case(case).await;
        let encoded = serde_json::to_vec(&envelope).expect("encode envelope");
        std::fs::write(out.join(format!("{}.json", case.name)), encoded).expect("write envelope");
        println!("RUST {}", case.name);
    }
}
