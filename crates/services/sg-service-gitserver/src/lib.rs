#![forbid(unsafe_code)]

//! `gitserver` service: `gitserver.v1.GitserverService`, HTTP `/ping` and the
//! `/git/` smart Git HTTP transport multiplexed on one cleartext port (`:3178`
//! by default), matching the Go baseline.
//!
//! Unimplemented RPCs report `UNIMPLEMENTED` until later M9 slices land.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio_util::task::TaskTracker;

use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_proto::gitserver as gpb;

pub mod addrs;
pub mod cacert;
pub mod cloneurl;
pub mod config;
pub mod customfetch;
pub mod debug;
mod details;
pub mod diff;
pub mod disk;
pub mod fetchservice;
pub mod frontendconfig;
pub mod fs;
pub mod git;
pub mod gitservice;
pub mod grpc;
pub mod keyring;
pub mod limiter;
pub mod locker;
pub mod memobs;
pub mod metrics;
mod mux;
pub mod postfetch;
pub mod progress;
pub mod remoteurl;
pub mod repo;
pub mod repositoryservice;
pub mod search;
pub mod statesyncer;
pub mod tlsexternal;
pub mod trace;
pub mod update;
pub mod vcssyncer;
pub mod vcsurl;

use config::GitserverConfig;
use gitservice::GitService;
use grpc::Grpc;

pub fn service_name() -> &'static str {
    "gitserver"
}

/// `cmd/gitserver/shared`: `127.0.0.1:3178` in insecure dev, `:3178`
/// otherwise.
pub const DEFAULT_PORT: &str = "3178";

pub struct GitserverService {
    addr: Option<String>,
    /// `SRC_PROF_HTTP`; `None` disables the debug server (baseline noop
    /// server).
    prof_addr: Option<String>,
    config: Option<GitserverConfig>,
}

impl GitserverService {
    pub fn new() -> Self {
        Self {
            addr: None,
            prof_addr: None,
            config: None,
        }
    }

    /// Test hook: bind a different address.
    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr: Some(addr.into()),
            prof_addr: None,
            config: None,
        }
    }
}

impl Default for GitserverService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for GitserverService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        // Registered by the baseline `internal/debugserver` init; an empty
        // value disables the server.
        let prof_addr = env.get(
            "SRC_PROF_HTTP",
            debug::DEFAULT_ADDR,
            "net/http/pprof http bind address.",
        );
        self.prof_addr = Some(prof_addr).filter(|addr| !addr.is_empty());
        let config = GitserverConfig::load(env, self.addr.clone())?;
        self.addr = Some(config.listen_addr.clone());
        self.config = Some(config);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.addr.as_deref().unwrap_or_default().is_empty() {
            return Err(ConfigError::new(
                "gitserver listen address must not be empty",
            ));
        }
        self.config
            .as_ref()
            .ok_or_else(|| ConfigError::new("gitserver configuration was not loaded"))?
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("gitserver configuration was not loaded"))?;
        let addr = self
            .addr
            .clone()
            .ok_or_else(|| ServiceError::new("gitserver listen address was not loaded"))?;

        // `shared.Main` blocks in `conf.Get()` (via `getDB`) until the frontend
        // has provided the initial configuration, and fails when the database
        // DSN is missing. Mirror that before binding the port.
        let config_client = sg_config::ConfigClient::from_env();
        let unified = frontendconfig::wait_for_initial_config(&config_client, &ctx.shutdown)
            .await
            .ok_or_else(|| {
                ServiceError::new("gitserver stopped while waiting for the frontend configuration")
            })?;
        let dsn = unified.service_connections.postgres_dsn.clone();
        if dsn.is_empty() {
            return Err(ServiceError::new(
                "postgres_dsn is not configured by the frontend",
            ));
        }
        let pool = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| ServiceError::with_source("connecting to the frontend database", err))?;
        let store = Arc::new(sg_db::gitserver_repos::GitserverReposStore::new(
            pool.clone(),
        ));
        let repo_store = Arc::new(sg_db::repos::RepoStore::new(pool.clone()));
        let external_services = Arc::new(sg_db::external_services::ExternalServiceStore::new(pool));

        // `shared.Main` → `keyring.Init`: a configured key that cannot be
        // constructed fails startup.
        let external_service_key = keyring::external_service_key(&unified.site).map_err(|err| {
            ServiceError::with_source("initializing keyring", std::io::Error::other(err))
        })?;

        tracing::info!(
            addr = config_client.addr(),
            postgres = !dsn.is_empty(),
            "loaded the frontend configuration"
        );

        let listener = sg_http::listen(&addr)
            .await
            .map_err(|err| ServiceError::with_source(format!("failed to listen on {addr}"), err))?;

        // `git` may be slow: the baseline allows 1m per command and 2h for
        // long commands; the RPC layer here has no deadline of its own yet.
        let tasks = TaskTracker::new();
        let locker = locker::RepositoryLocker::new();
        let remote_urls = Arc::new(remoteurl::DbRemoteUrlSource {
            repos: repo_store,
            external_services,
            key: external_service_key,
        });
        let backend = Arc::new(update::LocalBackend {
            repos_dir: config.repos_dir.clone(),
            remote_urls,
            store: Some(Arc::clone(&store)),
            shard_id: config.external_address.clone(),
            tls: tlsexternal::TlsConfig::from_site(&unified.site),
            custom_fetch: customfetch::CustomFetch::build(
                unified
                    .site
                    .custom_git_fetch_configured()
                    .then(|| unified.site.custom_git_fetch())
                    .as_deref(),
                config.enable_custom_git_fetch,
            ),
        });
        // `NewServer` builds the clone limiter from `conf.GitMaxConcurrentClones`
        // and `conf.Watch` updates it when the site config changes. The Rust
        // config client has no watcher yet, so the limit is fixed at startup.
        let updater = Arc::new(update::UpdateServer {
            locker: Arc::clone(&locker),
            limiter: limiter::MutableLimiter::new(unified.site.git_max_concurrent_clones()),
            backend,
            cancelled: ctx.shutdown.clone(),
        });
        // `Server.svc`: shared by `Server.FetchRepository` and the read path's
        // `ensure_revision` retry.
        let fetch = Arc::new(fetchservice::FetchService {
            repos_dir: Arc::new(config.repos_dir.clone()),
            updater,
            tasks: tasks.clone(),
            store: Some(Arc::clone(&store)),
            disable_auto_git_updates: unified.site.disable_auto_git_updates(),
        });
        let grpc = Grpc {
            repos_dir: Arc::new(config.repos_dir.clone()),
            tasks: tasks.clone(),
            locker: Arc::clone(&locker),
            fetch: Some(Arc::clone(&fetch)),
        };
        let repository_service = repositoryservice::RepositoryService {
            repos_dir: Arc::new(config.repos_dir.clone()),
            store: Some(Arc::clone(&store)),
            hostname: config.external_address.clone(),
            fetch: Some(fetch),
        };
        // `NewRepoStateSyncer`: reconcile clone state with the database. The
        // address set and pins are startup snapshots (the baseline re-reads
        // them from `conf` on every run; the Rust config client has no
        // watcher yet).
        let state_syncer = Arc::new(statesyncer::RepoStateSyncer {
            repos: Arc::clone(&store),
            locker: Arc::clone(&locker),
            repos_dir: Arc::new(config.repos_dir.clone()),
            shard_id: config.external_address.clone(),
            addresses: addrs::GitserverAddresses {
                addresses: unified.service_connections.git_servers.clone(),
                pinned_servers: unified.site.git_server_pinned_repos(),
            },
            interval: config.sync_repo_state_interval,
            batch_size: config.sync_repo_state_batch_size,
            per_second: config.sync_repo_state_update_per_second,
        });
        let routes = tonic::service::Routes::new(
            gpb::gitserver_service_server::GitserverServiceServer::new(grpc),
        )
        .add_service(
            gpb::gitserver_repository_service_server::GitserverRepositoryServiceServer::new(
                repository_service,
            ),
        );
        let git_service = GitService {
            repos_dir: Arc::new(config.repos_dir.clone()),
            tasks: tasks.clone(),
        };
        let http: sg_http::HttpHandler = Arc::new(move |req: Request<Incoming>| {
            let service = git_service.clone();
            Box::pin(async move {
                // `instrumentation.HTTPMiddleware` + `trace.HTTPMiddleware`:
                // every plain-HTTP response carries the request's trace IDs.
                let trace = trace::TraceIds::from_headers(req.headers());
                let mut response = http_response(service, req).await;
                trace.apply(response.headers_mut());
                response
            })
        });

        tracing::info!(
            addr = %addr,
            repos_dir = %config.repos_dir.display(),
            "gitserver listening"
        );
        ctx.ready.mark_ready();
        sg_metrics::register_service_metadata(sg_metrics::VERSION);
        // Register every family up front: gauges and counters exist at zero
        // from startup, like the Go process.
        let metrics = metrics::Metrics::global(&config.repos_dir);

        let mut background = tokio::task::JoinSet::new();
        if let Some(prof_addr) = self.prof_addr.clone() {
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move { debug::serve(&prof_addr, shutdown).await });
        }
        {
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move { metrics::echo_loop(metrics, shutdown).await });
        }
        {
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move { state_syncer.run(shutdown).await });
        }

        let served = sg_http::serve(listener, routes, http, ctx.shutdown.clone()).await;
        // The background tasks may outlive an early main-server failure.
        ctx.shutdown.cancel();
        while background.join_next().await.is_some() {}
        tasks.close();
        tokio::time::timeout(Duration::from_secs(30), tasks.wait())
            .await
            .ok();
        served.map_err(|err| ServiceError::with_source("gitserver http server failed", err))
    }
}

/// Baseline `NewHTTPHandler`: `/ping` answers 200 with an empty body and
/// `/git/` serves the smart Git HTTP transport.
///
/// The baseline registers both patterns on a `ServeMux`, which first cleans
/// the escaped path and redirects when that changed it, then redirects the
/// `/git` subtree root to `/git/`. `http.StripPrefix` passes the handler the
/// decoded path.
async fn http_response(
    service: GitService,
    req: Request<Incoming>,
) -> Response<tonic::body::BoxBody> {
    let query = req.uri().query().map(str::to_owned);
    let path = req.uri().path().to_owned();
    let cleaned = mux::clean_path(&path);
    if cleaned != path {
        return mux::redirect(&req, &mux::cleaned_location(&cleaned, query.as_deref()));
    }
    if cleaned == "/git" {
        // `ServeMux` appends the slash for the registered subtree.
        return mux::redirect(&req, &mux::slash_location("/git/", query.as_deref()));
    }
    match cleaned.as_str() {
        "/ping" => {
            let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
            *response.status_mut() = StatusCode::OK;
            response
        }
        _ => {
            if cleaned.starts_with("/git/") {
                // `http.StripPrefix("/git", …)` keeps the leading slash and
                // operates on the decoded path (`%2F` becomes `/`).
                let decoded = mux::decode_path(&cleaned);
                let stripped = decoded.strip_prefix("/git").unwrap_or(&decoded);
                return service.handle(req, stripped).await;
            }
            not_found()
        }
    }
}

/// Go `http.NotFound`.
fn not_found() -> Response<tonic::body::BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from_static(
        b"404 page not found\n",
    ))));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static("x-content-type-options"),
        header::HeaderValue::from_static("nosniff"),
    );
    response
}
