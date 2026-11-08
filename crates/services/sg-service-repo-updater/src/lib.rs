#![forbid(unsafe_code)]

//! `repo-updater` service: the singleton update scheduler and the four
//! `repoupdater.v1.RepoUpdaterService` RPCs.
//!
//! Covered by this slice: the scheduler (schedule heap, update queue,
//! hydration from `gitserver_repos`, `FetchRepository` driving, interval
//! heuristics), external-service source syncing (`repos.Syncer` and its
//! worker/resetter/janitor), scheduled repository purging, the single-instance
//! gRPC listener (`:3182`), and the debug `/repo-updater-state` and
//! `/manual-purge` endpoints.
//!
//! Not yet covered (later slices): Batch Changes sync registry.

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
pub mod purge;
pub mod scheduler;
pub mod store;
pub mod sync_worker;
pub mod syncer;
pub mod uncloned;

use config::RepoUpdaterConfig;
use debug::DebugContext;
use gitserver::GitserverRepoClient;
use grpc::RepoUpdaterGrpc;
use metrics::Metrics;
use purge::PurgeWorker;
use scheduler::{RepoUpdateClient, SystemClock, UpdateScheduler};
use store::{DbRepoNameStore, DbSchedulerStore};
use sync_worker::SyncWorker;
use syncer::{ExtsvcSourceFactory, Syncer};
use uncloned::UnclonedReposManager;

/// `httpcli.Doer` adapter for the `OTHER` source's src-expose request.
struct HttpClientGetter(sg_http::HttpClient);

#[async_trait]
impl sg_extsvc::HttpGetter for HttpClientGetter {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        self.0.get(url).await
    }
}

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
        let gitserver_repos = sg_db::gitserver_repos::GitserverReposStore::new(pool.clone());
        let purge_gitserver_repos = sg_db::gitserver_repos::GitserverReposStore::new(pool.clone());
        let external_services = sg_db::external_services::ExternalServiceStore::new(pool.clone());
        let external_service_repos =
            sg_db::external_service_repos::ExternalServiceReposStore::new(pool.clone());
        let sync_jobs = sg_db::external_service_sync_jobs::SyncJobsStore::new(pool);

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

        // `repos.NewSyncer` + `repos.NewSourcer`. `OTHER` fetches src-expose
        // listings over HTTP; code-host kinds are not implemented yet.
        let http: Arc<dyn sg_extsvc::HttpGetter> =
            Arc::new(HttpClientGetter(sg_http::HttpClient::new()));
        let source_factory = Arc::new(ExtsvcSourceFactory::new(Some(http)));
        // The relay below subscribes before any sync runs, so no diff is
        // dropped. The buffer only matters when the relay lags.
        let (diff_tx, _) = tokio::sync::broadcast::channel(64);
        let syncer = Arc::new(Syncer::new(
            external_services,
            external_service_repos,
            source_factory,
            Some(diff_tx.clone()),
            Arc::new(SystemClock),
        ));
        let sync_worker = Arc::new(
            SyncWorker::new(
                sync_jobs.clone(),
                Arc::clone(&syncer),
                Arc::clone(&live),
                sync_worker::hostname(),
            )
            .with_num_handlers(
                live.current()
                    .site
                    .repo_concurrent_external_service_syncers(),
            ),
        );
        let purge_worker = Arc::new(PurgeWorker::new(
            purge_gitserver_repos,
            gitserver_client.clone() as Arc<dyn RepoUpdateClient>,
            Arc::clone(&live),
        ));
        // `newUnclonedReposManager`: priority boost for repos gitserver has
        // not cloned yet.
        let uncloned_manager = Arc::new(UnclonedReposManager::new(
            Arc::clone(&scheduler),
            (*repo_store).clone(),
            Arc::clone(&live),
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

        // `watchSyncer`: feed sync diffs into the update scheduler unless
        // `disableAutoGitUpdates` is set (checked per diff, like the baseline).
        {
            let mut diffs = diff_tx.subscribe();
            let scheduler = Arc::clone(&scheduler);
            let live = Arc::clone(&live);
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move {
                loop {
                    let diff = tokio::select! {
                        _ = shutdown.cancelled() => return,
                        received = diffs.recv() => match received {
                            Ok(diff) => diff,
                            // The baseline's unbuffered channel cannot lag;
                            // dropping a diff is the documented divergence.
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                                tracing::warn!(lagged, "dropping external service sync diffs");
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                        },
                    };
                    if !live.current().site.disable_auto_git_updates() {
                        scheduler.update_from_diff(&diff);
                    }
                }
            });
        }

        // `syncer.Routines`: enqueue loop, dequeue loops, heartbeat,
        // resetter and janitor.
        Arc::clone(&sync_worker).spawn(&ctx.shutdown, &tasks);

        // `purge.NewRepositoryPurgeWorker`, unless `DISABLE_REPO_PURGE` is
        // set (parsed like `strconv.ParseBool`).
        if !config.disable_repo_purge {
            Arc::clone(&purge_worker).spawn(ctx.shutdown.clone(), &tasks);
        } else {
            tracing::info!("repository purger is disabled via env DISABLE_REPO_PURGE");
        }

        // `newUnclonedReposManager`: every 30s, prioritise uncloned repos.
        uncloned_manager.spawn(ctx.shutdown.clone(), &tasks);

        if let Some(prof_addr) = config.prof_addr.clone() {
            let context = Arc::new(DebugContext {
                scheduler: Arc::clone(&scheduler),
                sync_jobs: Arc::new(sync_jobs),
                purge: Arc::clone(&purge_worker),
                tasks: tasks.clone(),
            });
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move { debug::serve(&prof_addr, context, shutdown).await });
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
