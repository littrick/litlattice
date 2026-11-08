#![forbid(unsafe_code)]

//! `worker` service.
//!
//! This slice (9.8a) is the startup skeleton: the baseline framework
//! configuration (`WORKER_JOB_ALLOWLIST` / `WORKER_JOB_BLOCKLIST` with their
//! exact `safeSplit` / validation / precedence semantics), the sorted registry
//! of the 56 compiled-in jobs, frontend configuration discovery, keyring
//! initialization, the frontend database connection and the `/healthz` server
//! on `:3189`.
//!
//! No job routines are started yet: `createBackgroundRoutines` returns an
//! empty set. Later slices add the framework (`framework/`) and the job groups
//! one by one; until a job is ported, the Rust worker runs only the health
//! server, which is what the pilot differential exercises.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::ConfigClient;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub mod config;
pub mod health;
pub mod registry;

use config::WorkerConfigs;

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
    config: Option<WorkerConfigs>,
}

impl WorkerService {
    pub fn new() -> Self {
        Self {
            addr_override: None,
            config: None,
        }
    }

    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr_override: Some(addr.into()),
            config: None,
        }
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
        // `sanitycheck.Pass()` registers SANITY_CHECK in every service binary.
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        self.config = Some(WorkerConfigs::load(env, self.addr_override.clone()));
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
        let _keyring = sg_keyring::init(&unified.site).await.map_err(|err| {
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
        let _frontend_pool = sg_db::connect(&dsn, 10).await.map_err(|err| {
            ServiceError::with_source("failed to create database connection", err)
        })?;

        // `createBackgroundRoutines` has no ported job groups yet, so the
        // routine set is empty. `authz.DefaultSubRepoPermsChecker`, the
        // `src_worker_jobs` gauges and the `background-job-logger` recorder are
        // added with the framework / job slices.

        // `ready()`: all routines (here just the health server) are set up.
        ctx.ready.mark_ready();

        tracing::info!(
            addr = config.listen_addr,
            jobs = registry::count(),
            "serving worker health endpoint (no job routines ported yet)"
        );
        serve_health(&config.listen_addr, ctx.shutdown.clone())
            .await
            .map_err(|err| ServiceError::with_source("serving HTTP", err))
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
