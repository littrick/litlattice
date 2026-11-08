#![forbid(unsafe_code)]

//! `cmd/worker/internal/batches`: the `batches-janitor` job.
//!
//! The baseline `janitorJob.Routines` returns eight routines (or none when
//! `batches.IsEnabled()` is false):
//!
//! 1. `executors.autoscaler-metrics` — `executorqueue.NewMetricReporter` for
//!    the `batches` queue (5s), reusing the emitter from
//!    [`super::executors_multiqueue_metrics`];
//! 2. `batches_reconciler_worker_resetter` — `dbworker.Resetter` over
//!    `changesets` (`reconciler_state`, 60s stalled, 10 resets), 1m;
//! 3. `batches_bulk_worker_resetter` — over `changeset_jobs` (60s, 60), 1m;
//! 4. `batch_spec_workspace_execution_worker_resetter` — over
//!    `batch_spec_workspace_execution_jobs` (25s, 3), 1m;
//! 5. `batch_changes_batch_spec_resolution_worker_resetter` — over
//!    `batch_spec_resolution_jobs` (60s, 60), 1m;
//! 6. `batchchanges.spec-expirer` (1h) — deletes unattached expired changeset
//!    specs, expired attached changeset specs, then expired batch specs;
//! 7. `batchchanges.cache-cleaner` (1h) — LRU-cleans
//!    `batch_spec_execution_cache_entries` down to
//!    `SRC_BATCH_CHANGES_MAX_CACHE_SIZE_MB` (default 5000) and drops rows with
//!    an outdated cache version;
//! 8. `batchchanges.detached-cleaner` (24h) — deletes changesets detached
//!    longer than `batchChanges.changesetsRetention`.
//!
//! Not ported (workerutil metrics slice): the four `dbworker.ResetterMetrics`
//! counter families and the two scrape-time `GaugeFunc`s
//! (`src_executor_total{queue="batches"}`,
//! `src_executor_queued_duration_seconds_total{queue="batches"}`). Neither
//! affects the routines' database behaviour.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::executors_multiqueue_metrics::{
    configure_reporters, ExternalEmitter, MultiqueueMetricsConfig, QueueCountSource,
};
use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::dbworker::{DbBackedRoutine, DbResetter, ResetterOptions, ResetterRoutine};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `janitorJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `cacheCleanInterval`.
pub const CACHE_CLEAN_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// `specExpireInteral` (sic, baseline spelling).
pub const SPEC_EXPIRE_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// `changesetCleanInterval`.
pub const CHANGESET_CLEAN_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// `executorqueue.NewMetricReporter`'s interval (`initExternalMetricReporters`).
pub const METRICS_REPORTER_INTERVAL: Duration = Duration::from_secs(5);
/// `dbworker.ResetterOptions.Interval` of all four resetters.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

pub const ROUTINE_METRICS_REPORTER: &str = "executors.autoscaler-metrics";
pub const METRICS_REPORTER_DESCRIPTION: &str = "emits metrics to GCP/AWS for auto-scaling";
pub const ROUTINE_RECONCILER_RESETTER: &str = "batches_reconciler_worker_resetter";
pub const ROUTINE_BULK_RESETTER: &str = "batches_bulk_worker_resetter";
pub const ROUTINE_WORKSPACE_EXECUTION_RESETTER: &str =
    "batch_spec_workspace_execution_worker_resetter";
pub const ROUTINE_RESOLUTION_RESETTER: &str = "batch_changes_batch_spec_resolution_worker_resetter";
pub const ROUTINE_SPEC_EXPIRER: &str = "batchchanges.spec-expirer";
pub const SPEC_EXPIRER_DESCRIPTION: &str = "expire batch changes specs";
pub const ROUTINE_CACHE_CLEANER: &str = "batchchanges.cache-cleaner";
pub const CACHE_CLEANER_DESCRIPTION: &str = "cleaning up LRU batch spec execution cache entries";
pub const ROUTINE_DETACHED_CLEANER: &str = "batchchanges.detached-cleaner";
pub const DETACHED_CLEANER_DESCRIPTION: &str = "cleaning detached changeset entries";

/// `maxCacheEntriesSize`'s environment variable.
pub const MAX_CACHE_SIZE_VAR: &str = "SRC_BATCH_CHANGES_MAX_CACHE_SIZE_MB";
const MAX_CACHE_SIZE_DESCRIPTION: &str =
    "Maximum size of the batch_spec_execution_cache_entries.value column. Value is megabytes.";

/// `janitorConfig` (the job config) plus the package-level
/// `maxCacheEntriesSize`, which the baseline reads with `env.MustGetInt` while
/// the package initializes.
#[derive(Debug, Default)]
pub struct BatchesJanitorConfig {
    max_cache_entries_size_mb: Mutex<i64>,
    errs: Mutex<Vec<String>>,
    /// `executorqueue.InitMetricsConfig()`.
    metrics: MultiqueueMetricsConfig,
}

impl BatchesJanitorConfig {
    pub fn max_cache_entries_size_mb(&self) -> i64 {
        *self
            .max_cache_entries_size_mb
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn metrics(&self) -> &MultiqueueMetricsConfig {
        &self.metrics
    }
}

impl JobConfig for BatchesJanitorConfig {
    fn load(&self, env: &mut EnvReader) {
        self.metrics.load(env);

        let raw = env.get(MAX_CACHE_SIZE_VAR, "5000", MAX_CACHE_SIZE_DESCRIPTION);
        let mut errs = Vec::new();
        let megabytes = match raw.parse::<i64>() {
            Ok(value) => value,
            Err(_) => {
                // `env.MustGetInt` panics with this message during package
                // init; the Rust job reports it as a configuration error
                // instead (docs/modules/06_worker.md §27).
                errs.push(format!(
                    "parsing environment variable {MAX_CACHE_SIZE_VAR:?}. Expected valid integer, got {raw:?}"
                ));
                5000
            }
        };

        *self
            .max_cache_entries_size_mb
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = megabytes;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    /// `janitorConfig.Validate`: `BaseConfig.Validate` then
    /// `MetricsConfig.Validate`.
    fn validate(&self) -> Result<(), ConfigError> {
        let mut errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Err(err) = self.metrics.validate() {
            errs.push(err.to_string());
        }
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `batches.IsEnabled`: `DISABLE_BATCH_CHANGES` read with `os.Getenv` and
/// `strconv.ParseBool`; parse errors leave Batch Changes enabled.
pub fn batches_enabled() -> bool {
    match std::env::var("DISABLE_BATCH_CHANGES") {
        Ok(raw) => !parse_strconv_bool(&raw).unwrap_or(false),
        Err(_) => true,
    }
}

/// `strconv.ParseBool`.
pub fn parse_strconv_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `janitorJob`.
pub struct BatchesJanitor {
    config: Arc<BatchesJanitorConfig>,
}

impl BatchesJanitor {
    pub fn new() -> Self {
        Self {
            config: Arc::new(BatchesJanitorConfig::default()),
        }
    }

    pub fn config(&self) -> &BatchesJanitorConfig {
        &self.config
    }
}

impl Default for BatchesJanitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for BatchesJanitor {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !batches_enabled() {
            return Ok(Vec::new());
        }

        let values = self.config.metrics.snapshot();
        let reporters = configure_reporters(&values)?;
        let allocation = values
            .allocations
            .get("batches")
            .copied()
            .unwrap_or_default();
        let emitter = ExternalEmitter::new(
            "batches".to_owned(),
            vec![Arc::new(QueueCountSource::new(
                deps.frontend.clone(),
                sg_db::worker_queue::BATCH_SPEC_WORKSPACE_EXECUTION_JOBS_VIEW,
            ))],
            reporters,
            allocation,
        );
        let metrics_reporter = PeriodicRoutine::new(Arc::new(emitter))
            .with_name(ROUTINE_METRICS_REPORTER)
            .with_description(METRICS_REPORTER_DESCRIPTION)
            .with_interval(METRICS_REPORTER_INTERVAL);

        let reset_store = |options: sg_db::batches_janitor::ResetWorkerOptions| {
            Arc::new(BatchesResetStore {
                pool: deps.frontend.clone(),
                options,
            }) as Arc<dyn DbResetter>
        };
        let resetter = |name: &str, options: sg_db::batches_janitor::ResetWorkerOptions| {
            BackgroundRoutine::DbBacked(Arc::new(ResetterRoutine::new(
                reset_store(options),
                ResetterOptions {
                    name: name.to_owned(),
                    interval: RESETTER_INTERVAL,
                },
            )) as Arc<dyn DbBackedRoutine>)
        };

        let max_cache_bytes = self
            .config
            .max_cache_entries_size_mb()
            .saturating_mul(1024 * 1024);

        Ok(vec![
            metrics_reporter.into(),
            resetter(
                ROUTINE_RECONCILER_RESETTER,
                sg_db::batches_janitor::RECONCILER_WORKER,
            ),
            resetter(
                ROUTINE_BULK_RESETTER,
                sg_db::batches_janitor::BULK_OPERATION_WORKER,
            ),
            resetter(
                ROUTINE_WORKSPACE_EXECUTION_RESETTER,
                sg_db::batches_janitor::WORKSPACE_EXECUTION_WORKER,
            ),
            resetter(
                ROUTINE_RESOLUTION_RESETTER,
                sg_db::batches_janitor::BATCH_SPEC_RESOLUTION_WORKER,
            ),
            PeriodicRoutine::new(Arc::new(SpecExpirerHandler {
                pool: deps.frontend.clone(),
            }))
            .with_name(ROUTINE_SPEC_EXPIRER)
            .with_description(SPEC_EXPIRER_DESCRIPTION)
            .with_interval(SPEC_EXPIRE_INTERVAL)
            .into(),
            PeriodicRoutine::new(Arc::new(CacheEntryCleanerHandler {
                pool: deps.frontend.clone(),
                max_cache_bytes,
            }))
            .with_name(ROUTINE_CACHE_CLEANER)
            .with_description(CACHE_CLEANER_DESCRIPTION)
            .with_interval(CACHE_CLEAN_INTERVAL)
            .into(),
            PeriodicRoutine::new(Arc::new(ChangesetDetachedCleanerHandler {
                pool: deps.frontend.clone(),
                live_config: Arc::clone(&deps.live_config),
            }))
            .with_name(ROUTINE_DETACHED_CLEANER)
            .with_description(DETACHED_CLEANER_DESCRIPTION)
            .with_interval(CHANGESET_CLEAN_INTERVAL)
            .into(),
        ])
    }
}

/// One of the four `dbworker.Store` resetters.
struct BatchesResetStore {
    pool: PgPool,
    options: sg_db::batches_janitor::ResetWorkerOptions,
}

#[async_trait]
impl DbResetter for BatchesResetStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        sg_db::batches_janitor::reset_stalled(&self.pool, self.options, OffsetDateTime::now_utc())
            .await
    }
}

/// `janitor.NewSpecExpirer`'s handler.
struct SpecExpirerHandler {
    pool: PgPool,
}

#[async_trait]
impl Handler for SpecExpirerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let now = OffsetDateTime::now_utc();
        sg_db::batches_janitor::delete_unattached_expired_changeset_specs(&self.pool, now)
            .await
            .map_err(|err| HandlerError::new(format!("DeleteExpiredChangesetSpecs: {err}")))?;
        sg_db::batches_janitor::delete_expired_changeset_specs(&self.pool, now)
            .await
            .map_err(|err| HandlerError::new(format!("DeleteExpiredChangesetSpecs: {err}")))?;
        sg_db::batches_janitor::delete_expired_batch_specs(&self.pool, now)
            .await
            .map_err(|err| HandlerError::new(format!("DeleteExpiredBatchSpecs: {err}")))?;
        Ok(())
    }
}

/// `janitor.NewCacheEntryCleaner`'s handler.
struct CacheEntryCleanerHandler {
    pool: PgPool,
    max_cache_bytes: i64,
}

#[async_trait]
impl Handler for CacheEntryCleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        sg_db::batches_janitor::clean_batch_spec_execution_cache_entries(
            &self.pool,
            self.max_cache_bytes,
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `janitor.NewChangesetDetachedCleaner`'s handler.
struct ChangesetDetachedCleanerHandler {
    pool: PgPool,
    live_config: Arc<sg_config::LiveConfig>,
}

#[async_trait]
impl Handler for ChangesetDetachedCleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let current = self.live_config.current();
        let retentions = current
            .site
            .raw()
            .get("batchChanges")
            .and_then(|batch_changes| batch_changes.get("changesetsRetention"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if retentions.is_empty() {
            return Ok(());
        }

        let retention = sg_core::parse_go_duration(retentions).map_err(|err| {
            HandlerError::new(format!(
                "failed to parse config value batchChanges.changesetsRetention as duration: {err}"
            ))
        })?;
        // `CleanDetachedChangesets(ctx, d)` passes `retention/time.Second`.
        sg_db::batches_janitor::clean_detached_changesets(&self.pool, retention.as_secs() as i64)
            .await
            .map_err(HandlerError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = BatchesJanitor::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn routine_contract_matches_the_baseline() {
        assert_eq!(ROUTINE_METRICS_REPORTER, "executors.autoscaler-metrics");
        assert_eq!(METRICS_REPORTER_INTERVAL, Duration::from_secs(5));
        assert_eq!(
            ROUTINE_RECONCILER_RESETTER,
            "batches_reconciler_worker_resetter"
        );
        assert_eq!(ROUTINE_BULK_RESETTER, "batches_bulk_worker_resetter");
        assert_eq!(
            ROUTINE_WORKSPACE_EXECUTION_RESETTER,
            "batch_spec_workspace_execution_worker_resetter"
        );
        assert_eq!(
            ROUTINE_RESOLUTION_RESETTER,
            "batch_changes_batch_spec_resolution_worker_resetter"
        );
        assert_eq!(ROUTINE_SPEC_EXPIRER, "batchchanges.spec-expirer");
        assert_eq!(SPEC_EXPIRER_DESCRIPTION, "expire batch changes specs");
        assert_eq!(ROUTINE_CACHE_CLEANER, "batchchanges.cache-cleaner");
        assert_eq!(
            CACHE_CLEANER_DESCRIPTION,
            "cleaning up LRU batch spec execution cache entries"
        );
        assert_eq!(ROUTINE_DETACHED_CLEANER, "batchchanges.detached-cleaner");
        assert_eq!(
            DETACHED_CLEANER_DESCRIPTION,
            "cleaning detached changeset entries"
        );
        assert_eq!(RESETTER_INTERVAL, Duration::from_secs(60));
        assert_eq!(SPEC_EXPIRE_INTERVAL, Duration::from_secs(3600));
        assert_eq!(CACHE_CLEAN_INTERVAL, Duration::from_secs(3600));
        assert_eq!(CHANGESET_CLEAN_INTERVAL, Duration::from_secs(86400));
    }

    #[test]
    fn strconv_bool_matches_go() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert_eq!(parse_strconv_bool(raw), Some(true), "{raw}");
        }
        for raw in ["0", "f", "F", "FALSE", "false", "False"] {
            assert_eq!(parse_strconv_bool(raw), Some(false), "{raw}");
        }
        for raw in ["", "yes", "on", "2", "true "] {
            assert_eq!(parse_strconv_bool(raw), None, "{raw}");
        }
    }

    #[test]
    fn max_cache_size_defaults_to_5000_megabytes() {
        let config = BatchesJanitorConfig::default();
        // Before `Load` the mutex holds the zero value; `Load` sets it.
        let mut env = EnvReader::new();
        config.load(&mut env);
        assert_eq!(config.max_cache_entries_size_mb(), 5000);
    }
}
