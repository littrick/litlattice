#![forbid(unsafe_code)]

//! Offline end-to-end test for [`GitserverSourcesClient`] — the production
//! gitserver client the Perforce and Gitolite repo-updater sources use.
//!
//! An in-process tonic server (the real `sg-service-gitserver::grpc::Grpc`
//! with a fake Gitolite lister) is bound on localhost; the client reaches it
//! through the same `LiveConfig` address selection and lazy per-address
//! channel cache as the production repo-updater. No SSH, Perforce or network
//! beyond the loopback socket is involved.
//!
//! Perforce success cannot be exercised here: `Grpc.perforce` is the concrete
//! `PerforceService` (which shells out to `p4`), so leaving it `None` makes
//! the handler answer `Unimplemented`. That still covers the client's
//! proto-error unwrapping (`status.Message()`) over a real gRPC round trip;
//! the source-level behaviour is covered by
//! `run_perforce_source_gitserver_diff.sh`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, RawUnified, ServiceConnections, Unified};
use sg_extsvc::gitolite::{GitoliteRepoLister, Repo as GitoliteRepo};
use sg_extsvc::perforce::{PerforceConnectionDetails, PerforceGitserver};
use sg_proto::gitserver as gpb;
use sg_service_gitserver::gitolite::GitoliteRepoLister as ServerRepoLister;
use sg_service_gitserver::grpc::Grpc;
use sg_service_repo_updater::gitserver::{GitserverRepoClient, GitserverSourcesClient};
use sg_service_repo_updater::metrics::Metrics;

const UNIMPLEMENTED_CLONEABLE: &str =
    "is_perforce_path_cloneable is not implemented by the Rust gitserver yet";
const UNIMPLEMENTED_CREDENTIALS: &str =
    "check_perforce_credentials is not implemented by the Rust gitserver yet";

struct FakeLister {
    hosts: Mutex<Vec<String>>,
}

#[async_trait]
impl ServerRepoLister for FakeLister {
    async fn list_repos(
        &self,
        host: &str,
    ) -> Result<Vec<sg_service_gitserver::gitolite::GitoliteRepo>, String> {
        self.hosts.lock().expect("hosts").push(host.to_owned());
        if host == "ssh://broken" {
            return Err("exit status 255".to_owned());
        }
        Ok(vec![
            sg_service_gitserver::gitolite::GitoliteRepo {
                name: "gitolite-admin".to_owned(),
                url: format!("{host}:gitolite-admin"),
            },
            sg_service_gitserver::gitolite::GitoliteRepo {
                name: "testing".to_owned(),
                url: format!("{host}:testing"),
            },
        ])
    }
}

struct FakeServer {
    addr: String,
    hosts: Arc<FakeLister>,
}

async fn start_fake_server() -> FakeServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");

    let hosts = Arc::new(FakeLister {
        hosts: Mutex::new(Vec::new()),
    });
    let grpc = Grpc {
        repos_dir: Arc::new(PathBuf::from("/nonexistent-m915a")),
        tasks: tokio_util::task::TaskTracker::new(),
        locker: sg_service_gitserver::locker::RepositoryLocker::new(),
        fetch: None,
        cloneable: None,
        patch: None,
        perforce: None,
        gitolite: hosts.clone(),
    };
    let server = tonic::transport::Server::builder().add_service(
        gpb::gitserver_service_server::GitserverServiceServer::new(grpc),
    );
    let handle = tokio::spawn(async move { server.serve_with_incoming(incoming).await });
    // Keep the server task alive for the test; aborting the handle at the end
    // is enough because the runtime drops the socket then.
    std::mem::forget(handle);
    FakeServer { addr, hosts }
}

fn live_config(addresses: &[String]) -> Arc<LiveConfig> {
    let unified = Unified::parse(&RawUnified {
        id: 0,
        site: "{}".to_owned(),
        service_connections: ServiceConnections {
            git_servers: addresses.to_vec(),
            ..Default::default()
        },
    })
    .expect("parse config");
    Arc::new(LiveConfig::new(unified))
}

fn sources_client(
    live: Arc<LiveConfig>,
) -> (
    Arc<GitserverRepoClient>,
    GitserverSourcesClient,
    Arc<Metrics>,
) {
    let metrics = Arc::new(Metrics::new());
    let inner = Arc::new(GitserverRepoClient::new(live, metrics.clone()));
    (inner.clone(), GitserverSourcesClient::production(inner), metrics)
}

async fn wait_for_server(addr: &str) {
    let mut client = gpb::gitserver_service_client::GitserverServiceClient::connect(format!(
        "http://{addr}"
    ))
    .await
    .expect("connect");
    for _ in 0..50 {
        if client
            .list_gitolite(gpb::ListGitoliteRequest {
                gitolite_host: "warmup".to_owned(),
            })
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("fake gitserver never became ready");
}

#[tokio::test]
async fn gitolite_reaches_the_address_selected_shard() {
    let server = start_fake_server().await;
    wait_for_server(&server.addr).await;

    let (_, sources, metrics) = sources_client(live_config(&[server.addr.clone()]));
    let repos = GitoliteRepoLister::list_repos(&sources, "git@gitolite.example.com")
        .await
        .expect("list gitolite over gRPC");
    assert_eq!(
        repos,
        vec![
            GitoliteRepo {
                name: "gitolite-admin".to_owned(),
                url: "git@gitolite.example.com:gitolite-admin".to_owned(),
            },
            GitoliteRepo {
                name: "testing".to_owned(),
                url: "git@gitolite.example.com:testing".to_owned(),
            },
        ]
    );
    assert!(server
        .hosts
        .hosts
        .lock()
        .expect("hosts")
        .contains(&"git@gitolite.example.com".to_owned()));
    assert!(metrics.gitserver_addr_for_repo_invoked.get() >= 1.0);
}

#[tokio::test]
async fn gitolite_rpc_errors_keep_the_grpc_text() {
    let server = start_fake_server().await;
    wait_for_server(&server.addr).await;

    let (_, sources, _) = sources_client(live_config(&[server.addr.clone()]));
    let error = GitoliteRepoLister::list_repos(&sources, "ssh://broken")
        .await
        .expect_err("handler maps the lister error to Internal");
    assert_eq!(error, "rpc error: code = Internal desc = exit status 255");
}

#[tokio::test]
async fn perforce_errors_unwrap_the_status_message() {
    let server = start_fake_server().await;
    wait_for_server(&server.addr).await;

    let (_, sources, _) = sources_client(live_config(&[server.addr.clone()]));
    let connection = PerforceConnectionDetails {
        p4_port: "ssl:111.222.333.444:1666".to_owned(),
        p4_user: "u".to_owned(),
        p4_passwd: "p".to_owned(),
    };
    let cloneable = sources
        .is_perforce_path_cloneable(&connection, "//Sourcegraph/")
        .await
        .expect_err("no Perforce backend in the fake server");
    assert_eq!(cloneable, UNIMPLEMENTED_CLONEABLE);
    let credentials = sources
        .check_perforce_credentials(&connection)
        .await
        .expect_err("no Perforce backend in the fake server");
    assert_eq!(credentials, UNIMPLEMENTED_CREDENTIALS);
}

#[tokio::test]
async fn no_configured_gitserver_is_reported_per_call() {
    let (_, sources, metrics) = sources_client(live_config(&[]));
    assert_eq!(
        GitoliteRepoLister::list_repos(&sources, "git@host")
            .await
            .expect_err("no endpoints"),
        "no gitserver endpoints configured"
    );
    let connection = PerforceConnectionDetails::default();
    assert_eq!(
        sources
            .is_perforce_path_cloneable(&connection, "//Depot/")
            .await
            .expect_err("no endpoints"),
        "no gitserver endpoints configured"
    );
    assert!(metrics.gitserver_addr_for_repo_invoked.get() >= 2.0);
}
