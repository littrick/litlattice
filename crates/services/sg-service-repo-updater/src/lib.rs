#![forbid(unsafe_code)]

//! `repo-updater` service: the singleton update scheduler and the four
//! `repoupdater.v1.RepoUpdaterService` RPCs.
//!
//! Covered by this slice: the scheduler (schedule heap, update queue,
//! hydration from `gitserver_repos`, `FetchRepository` driving, interval
//! heuristics), the single-instance gRPC listener (`:3182`), the debug
//! `/repo-updater-state` dumper, and config propagation from the frontend.
//!
//! Not yet covered (later slices): external-service source syncing,
//! `repos.Syncer` bridge, repository purge, Batch Changes sync registry.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{Request, Response, StatusCode};
use hyper::body::Incoming;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use sg_config::{ConfigClient, Unified};
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_proto::repoupdater as pb;

pub mod config;
pub mod debug;
pub mod gitserver;
pub mod grpc;
pub mod metrics;
pub mod scheduler;
pub mod store;

use config::RepoUpdaterConfig;
use gitserver::GitserverRepoClient;
use grpc::RepoUpdaterGrpc;
use metrics::Metrics;
use scheduler::{SystemClock, UpdateScheduler};
use store::{DbRepoNameStore, DbSchedulerStore};

pub fn service_name() -> &'static str {
    "repo-updater"
}

/// How long to wait between failed frontend-configuration fetches.
const CONFIG_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct RepoUpdaterService {
    addr_override: Option<String>,
    config: Option<RepoUpdaterConfig>,
}

impl RepoUpdaterService {
    pub fn new() -> Self {
        Self {
            addr_override: None,
            config: None,
        }
    }

    /// Test/rollout hook: bind a different address.
    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr_override: Some(addr.into()),
            config: None,
        }
    }
}

impl Default for RepoUpdaterService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for RepoUpdaterService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        // `sanitycheck.Pass()` registers SANITY_CHECK in every service binary.
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        self.config = Some(RepoUpdaterConfig::load(env, self.addr_override.clone())?);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.config
            .as_ref()
            .ok_or_else(|| ConfigError::new("repo-updater configuration was not loaded"))?
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("repo-updater configuration was not loaded"))?;

        // `shared.Main` → `getDB` blocks in `conf.Get()` until the frontend
        // sends the initial configuration.
        let config_client = Arc::new(ConfigClient::from_env());
        let unified = wait_for_initial_config(&config_client, &ctx.shutdown)
            .await
            .ok_or_else(|| {
                ServiceError::new(
                    "repo-updater stopped while waiting for the frontend configuration",
                )
            })?;
        let live = Arc::new(sg_config::LiveConfig::new((*unified).clone()));

        let dsn = unified.service_connections.postgres_dsn.clone();
        if dsn.is_empty() {
            return Err(ServiceError::new(
                "postgres_dsn is not configured by the frontend",
            ));
        }
        let pool = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| ServiceError::with_source("connecting to the frontend database", err))?;

        let repo_store = Arc::new(sg_db::repos::RepoStore::new(pool.clone()));
        let gitserver_repos = sg_db::gitserver_repos::GitserverReposStore::new(pool);

        let git_servers = unified.service_connections.git_servers.clone();
        let gitserver_client =
            Arc::new(GitserverRepoClient::connect(&git_servers).map_err(|err| {
                ServiceError::new(format!("configuring the gitserver client: {err}"))
            })?);

        let metrics = Arc::new(Metrics::new());
        let scheduler = Arc::new(UpdateScheduler::new(
            Arc::new(SystemClock),
            Arc::new(DbSchedulerStore::new(gitserver_repos)),
            gitserver_client.clone(),
            Arc::clone(&live),
            metrics,
        ));

        let grpc = RepoUpdaterGrpc {
            scheduler: Arc::clone(&scheduler),
            repos: Arc::new(DbRepoNameStore::new((*repo_store).clone())),
            gitserver: gitserver_client,
            // Batch Changes is a separate (enterprise) subsystem; without it
            // the RPC reports `changeset syncer is not configured`, matching
            // the baseline with `batches.IsEnabled() == false`.
            changeset_sync: None,
        };

        let addr = config.listen_addr.clone();
        let listener = sg_http::listen(&addr)
            .await
            .map_err(|err| ServiceError::with_source(format!("failed to listen on {addr}"), err))?;

        let routes = tonic::service::Routes::new(
            pb::repo_updater_service_server::RepoUpdaterServiceServer::new(grpc),
        );
        // repo-updater has no plain-HTTP API on its serving port: anything
        // that is not gRPC gets Go's `http.NotFound` from the debug mux.
        let http: sg_http::HttpHandler = Arc::new(|_req: Request<Incoming>| {
            Box::pin(async {
                let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from_static(b"404 page not found\n"),
                )));
                *response.status_mut() = StatusCode::NOT_FOUND;
                response
            })
        });

        tracing::info!(
            addr = %addr,
            gitservers = ?git_servers,
            "repo-updater listening"
        );
        ctx.ready.mark_ready();
        sg_metrics::register_service_metadata(sg_metrics::VERSION);

        let tasks = TaskTracker::new();
        {
            let scheduler = Arc::clone(&scheduler);
            tasks.spawn(async move { scheduler.hydrate().await });
        }
        {
            let scheduler = Arc::clone(&scheduler);
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move { scheduler.run_schedule_loop(shutdown).await });
        }
        {
            let scheduler = Arc::clone(&scheduler);
            let shutdown = ctx.shutdown.clone();
            let tracker = tasks.clone();
            tasks.spawn(async move { scheduler.run_update_loop(shutdown, tracker).await });
        }
        {
            let watcher =
                sg_config::ConfigWatcher::new(Arc::clone(&config_client), Arc::clone(&live));
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move { watcher.run(shutdown).await });
        }
        if let Some(prof_addr) = config.prof_addr.clone() {
            let scheduler = Arc::clone(&scheduler);
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move { debug::serve(&prof_addr, scheduler, shutdown).await });
        }

        let served = sg_http::serve(listener, routes, http, ctx.shutdown.clone()).await;
        ctx.shutdown.cancel();
        tasks.close();
        tasks.wait().await;
        served.map_err(|err| ServiceError::with_source("repo-updater grpc server failed", err))
    }
}

/// `conf.Get()` startup behaviour: block until the frontend serves the first
/// configuration, or the process starts shutting down.
async fn wait_for_initial_config(
    client: &ConfigClient,
    shutdown: &CancellationToken,
) -> Option<Arc<Unified>> {
    loop {
        if shutdown.is_cancelled() {
            return None;
        }
        match client.refresh().await {
            Ok(unified) => return Some(unified),
            Err(err) => {
                tracing::warn!(
                    addr = client.addr(),
                    error = %err,
                    "waiting for the initial frontend configuration"
                );
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(CONFIG_RETRY_INTERVAL) => {}
        }
    }
}
