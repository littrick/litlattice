#![forbid(unsafe_code)]

//! `worker` service.
//!
//! Ported so far:
//!
//! * 9.8a — the startup skeleton: the baseline framework configuration
//!   (`WORKER_JOB_ALLOWLIST` / `WORKER_JOB_BLOCKLIST` with their exact
//!   `safeSplit` / validation / precedence semantics), the sorted registry of
//!   the 56 compiled-in jobs, frontend configuration discovery, keyring
//!   initialization, the frontend database connection and the `/healthz`
//!   server on `:3189`;
//! * 9.8b — `internal/goroutine.PeriodicGoroutine`, the `job.Job` /
//!   `env.Config` contracts, `loadConfigs` / `validateConfigs` /
//!   `createBackgroundRoutines` / `emitJobCountMetrics`, the live
//!   configuration watcher and the first job (`webhook-log-janitor`);
//! * 9.8c-9.8g — the database-backed janitor/statistics jobs, the
//!   `gitserver-metrics` gauges, the `SRC_PROF_HTTP` debug server, and the
//!   `background-job-logger` recorder on top of `sg-rcache`.
//!
//! Jobs that are not ported yet fail startup when enabled; the pilot narrows
//! `WORKER_JOB_ALLOWLIST` to the ported jobs.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{ConfigClient, ConfigWatcher, LiveConfig};
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod config;
pub mod debug;
pub mod env;
pub mod framework;
pub mod health;
pub mod jobs;
pub mod registry;
pub mod telemetry;

use config::WorkerConfigs;
use framework::recorder::Recorder;
use jobs::{Job, JobDeps};

pub const SERVICE_NAME: &str = "worker";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}

/// How long to wait between failed frontend-configuration fetches
/// (`conf.Get` retries in the baseline).
const CONFIG_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct WorkerService {
    /// Rust-only rollout knob (not part of the baseline registry) so the pilot
    /// can listen on a side port while the Go binary keeps `:3189`.
    addr_override: Option<String>,
    /// `SRC_PROF_HTTP`; `None` disables the debug server (baseline empty addr).
    prof_addr: Option<String>,
    config: Option<WorkerConfigs>,
    jobs: Vec<(&'static str, Arc<dyn Job>)>,
    /// `validateConfigs`'s error. `LoadConfig` stores it on a shadowed local
    /// config that nothing reads; kept here so the differential harness can
    /// show what the baseline discards.
    job_config_error: Option<String>,
}

impl WorkerService {
    pub fn new() -> Self {
        Self::with_addr_override(None)
    }

    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self::with_addr_override(Some(addr.into()))
    }

    fn with_addr_override(addr_override: Option<String>) -> Self {
        Self {
            addr_override,
            prof_addr: None,
            config: None,
            jobs: Vec::new(),
            job_config_error: None,
        }
    }

    /// The per-job validation error, which the baseline never surfaces.
    pub fn job_config_error(&self) -> Option<&str> {
        self.job_config_error.as_deref()
    }
}

impl Default for WorkerService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for WorkerService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        // `internal/env`'s package init: INSECURE_DEV is registered (and
        // parsed) before any `Configure`.
        env::register(env);
        // `sanitycheck.Pass()` registers SANITY_CHECK in every service binary.
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        // Registered by the baseline `internal/debugserver` init in every
        // service binary; an empty value disables the server.
        let prof_addr = env.get(
            "SRC_PROF_HTTP",
            debug::DEFAULT_ADDR,
            "net/http/pprof http bind address.",
        );
        self.prof_addr = Some(prof_addr).filter(|addr| !addr.is_empty());
        // `loadConfigs`: the worker config first, then every job's configs
        // (regardless of whether the job is enabled).
        let config = WorkerConfigs::load(env, self.addr_override.clone());
        let jobs = jobs::all_jobs();
        jobs::load_configs(env, &jobs);
        // `validateConfigs`; its error is discarded by the baseline (§1.1).
        self.job_config_error = jobs::validate_configs(&jobs, &config);
        self.config = Some(config);
        self.jobs = jobs;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        // `svcmain` validates the value `Configure` returned, i.e. the local
        // `shared.Config` inside `LoadConfig`. Because that function shadows
        // the package-level `config`, the returned value has an empty
        // allowlist/blocklist and its `Validate` override can only ever
        // succeed. The unknown-job / per-job-config error is stored on the
        // same value via `AddError` (`base_validation_error`) but is never
        // read. Mirror that observable behavior exactly.
        self.config
            .as_ref()
            .ok_or_else(|| ConfigError::new("worker configuration was not loaded"))?
            .returned
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .ok_or_else(|| ServiceError::new("worker configuration was not loaded"))?;

        // `shared.Start` → `keyring.Init` calls `conf.Get()`, which blocks
        // until the frontend sends the initial configuration.
        let config_client = Arc::new(ConfigClient::from_env());
        let unified = wait_for_initial_config(&config_client, &ctx.shutdown)
            .await
            .ok_or_else(|| {
                ServiceError::new("worker stopped while waiting for the frontend configuration")
            })?;

        // `keyring.Init(ctx)` reads `EncryptionKeys` from site config.
        let keyring = sg_keyring::init(&unified.site).await.map_err(|err| {
            ServiceError::with_source("initializing keyring", std::io::Error::other(err))
        })?;

        // `workerdb.InitDB`: `connections.EnsureNewFrontendDB` against the DSN
        // published through service connections.
        let dsn = unified.service_connections.postgres_dsn.clone();
        if dsn.is_empty() {
            return Err(ServiceError::new(
                "postgres_dsn is not configured by the frontend",
            ));
        }
        let frontend_pool = sg_db::connect(&dsn, 10).await.map_err(|err| {
            ServiceError::with_source("failed to create database connection", err)
        })?;

        // `authz.DefaultSubRepoPermsChecker` is not ported yet: no ported job
        // builds a sub-repo permissions client.

        // `src_worker_jobs` gauges, emitted before the routines are built.
        jobs::emit_job_count_metrics(&self.jobs, &config);

        // The live configuration is seeded with the initial fetch above; the
        // watcher replaces it when the frontend publishes a change.
        let live_config = Arc::new(LiveConfig::new((*unified).clone()));
        let tracker = TaskTracker::new();
        {
            let watcher = ConfigWatcher::new(Arc::clone(&config_client), Arc::clone(&live_config));
            let watcher_shutdown = ctx.shutdown.clone();
            tracker.spawn(async move { watcher.run(watcher_shutdown).await });
        }

        // `createBackgroundRoutines`: may take a while when jobs wait for
        // external services / migrations.
        let deps = JobDeps {
            frontend: frontend_pool,
            live_config,
            keyring,
        };
        let routines = jobs::create_background_routines(&deps, &self.jobs, &config)
            .await
            .map_err(ServiceError::new)?;
        let routine_count = routines.len();

        // `recorder.GetCache()` + `recorder.New(logger, env.MyName, cache)`:
        // register every recordable routine and publish `knownJobNames`,
        // `knownHostNames` and `knownRoutines` before the routines start.
        let _recorder = match sg_redis::KeyValue::connect(&sg_redis::cache_endpoint()).await {
            Ok(kv) => {
                let recorder = Recorder::new(Recorder::cache(kv), Recorder::process_name());
                for routine in &routines {
                    // Only periodic routines are `recorder.Recordable`; the
                    // httpserver-based routines are not registered by the
                    // baseline either.
                    let Some(recordable) = routine.recordable() else {
                        continue;
                    };
                    recordable.register_recorder(Arc::clone(&recorder));
                    recorder.register(recordable);
                }
                recorder.registration_done().await;
                Some(recorder)
            }
            Err(err) => {
                // The Go pool connects lazily, so an unreachable Redis never
                // fails startup; here the recorder is simply disabled.
                tracing::warn!(
                    error = %err,
                    "background-job-logger disabled: redis unavailable"
                );
                None
            }
        };

        for routine in routines {
            routine.spawn(&tracker, ctx.shutdown.clone());
        }

        // `ready()`: every routine is set up.
        ctx.ready.mark_ready();

        // `internal/debugserver`: exposes the process registry on
        // `SRC_PROF_HTTP` (pprof and `/vars` are not ported).
        if let Some(prof_addr) = self.prof_addr.clone() {
            let shutdown = ctx.shutdown.clone();
            tracker.spawn(async move { debug::serve(&prof_addr, shutdown).await });
        }

        tracing::info!(
            addr = config.listen_addr,
            jobs = registry::count(),
            routines = routine_count,
            "serving worker health endpoint"
        );

        let served = serve_health(&config.listen_addr, ctx.shutdown.clone())
            .await
            .map_err(|err| ServiceError::with_source("serving HTTP", err));

        // `MonitorBackgroundRoutines` returns once cancelled; drain every
        // routine before the process exits.
        ctx.shutdown.cancel();
        tracker.close();
        tracker.wait().await;

        served
    }
}

async fn serve_health(addr: &str, shutdown: CancellationToken) -> std::io::Result<()> {
    health::serve_health(addr, shutdown).await
}

async fn wait_for_initial_config(
    client: &ConfigClient,
    shutdown: &CancellationToken,
) -> Option<Arc<sg_config::Unified>> {
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
            _ = sleep(CONFIG_RETRY_INTERVAL) => {}
        }
    }
}
