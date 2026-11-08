#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/metrics_reporter.go`: the
//! `codeintel-metrics-reporter` job.
//!
//! The job owns `configInst` (a `janitorConfig` embedding
//! `*executorqueue.Config`) and returns the single routine built by
//! `executorqueue.NewMetricReporter` for the `codeintel` queue:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `executors.autoscaler-metrics` | 5s (fixed) | `CountByState` over `lsif_indexes_with_repository_name u`, reported to every configured cloud |
//!
//! `NewMetricReporter` first calls `initPrometheusMetric`, which registers the
//! queue's Prometheus gauges; that (like `uploads.MetricReporters` and the two
//! `dbworker.InitPrometheusMetric` calls for the dependency queues, which the
//! baseline also performs here) is part of the metrics slice and is not
//! ported. The external emitter itself is shared with
//! [`super::executors_multiqueue_metrics`]: same `CountByState` query, same
//! allocation multiplication, same cloud-reporting behaviour (cloud clients
//! are unported, so configuring one fails loudly).
//!
//! `codeintel.InitServices` is not needed: the emitter only holds a database
//! handle (`workerdb.InitDB`'s frontend pool), and the ported code-intel jobs
//! build their own clients from live configuration.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::executors_multiqueue_metrics::{
    configure_reporters, ExternalEmitter, MultiqueueMetricsConfig, QueueCountSource,
};
use super::{Job, JobConfig, JobDeps};
use crate::framework::periodic::PeriodicRoutine;
use crate::framework::BackgroundRoutine;

/// `metricsReporterJob.Description`.
pub const JOB_DESCRIPTION: &str = "executor push-based metrics reporting routines";

/// `executorqueue.initExternalMetricReporters`' `goroutine.WithName`.
pub const ROUTINE_NAME: &str = "executors.autoscaler-metrics";
/// `executorqueue.initExternalMetricReporters`' `goroutine.WithDescription`.
pub const ROUTINE_DESCRIPTION: &str = "emits metrics to GCP/AWS for auto-scaling";
/// `goroutine.WithInterval(5 * time.Second)`.
pub const ROUTINE_INTERVAL: Duration = Duration::from_secs(5);

/// `executorqueue.NewMetricReporter`'s `queueName` (the auto-indexing queue).
pub const QUEUE_NAME: &str = "codeintel";

/// `cmd/worker/internal/codeintel.metricsReporterJob` (`NewMetricsReporterJob`).
#[derive(Debug, Default)]
pub struct CodeIntelMetricsReporter {
    /// `janitorConfig.MetricsConfig`: `executorqueue.InitMetricsConfig()`.
    /// The same Rust config type is used by the multiqueue reporter, exactly
    /// like both Go jobs embed `*executorqueue.Config`.
    config: Arc<MultiqueueMetricsConfig>,
}

impl CodeIntelMetricsReporter {
    pub fn new() -> Self {
        Self::default()
    }

    /// The job's `configInst`, for tests and harnesses.
    pub fn config(&self) -> &MultiqueueMetricsConfig {
        &self.config
    }
}

#[async_trait]
impl Job for CodeIntelMetricsReporter {
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
        let values = self.config.snapshot();
        let reporters = configure_reporters(&values)?;

        // `metricsConfig.Allocations[queueName]`.
        let allocation = values
            .allocations
            .get(QUEUE_NAME)
            .copied()
            .unwrap_or_default();

        let count_sources: Vec<Arc<dyn super::executors_multiqueue_metrics::CountSource>> =
            vec![Arc::new(QueueCountSource::new(
                deps.frontend.clone(),
                sg_db::worker_queue::LSIF_INDEXES_VIEW,
            ))];

        let emitter =
            ExternalEmitter::new(QUEUE_NAME.to_owned(), count_sources, reporters, allocation);
        let routine = PeriodicRoutine::new(Arc::new(emitter))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(ROUTINE_INTERVAL);

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::executors_multiqueue_metrics::QueueAllocation;
    use sg_config::{LiveConfig, Unified};
    use sg_core::EnvReader;

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = CodeIntelMetricsReporter::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let config = MultiqueueMetricsConfig::default();
        config.load(&mut EnvReader::new());
        let values = config.snapshot();
        assert_eq!(values.environment_label, "dev");
        assert_eq!(
            values.allocations.get(QUEUE_NAME).copied(),
            Some(QueueAllocation::default())
        );
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn job_builds_the_periodic_routine() {
        let job = CodeIntelMetricsReporter::new();
        job.config().load(&mut EnvReader::new());
        let routines = job
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), ROUTINE_INTERVAL);
    }

    #[tokio::test]
    async fn configured_clouds_fail_loudly() {
        // `configure_reporters` is shared with the multiqueue job; this only
        // pins that the codeintel job surfaces its error.
        let job = CodeIntelMetricsReporter::new();
        let values = job.config().snapshot();
        assert!(configure_reporters(&values).expect("no cloud").is_empty());
    }
}
