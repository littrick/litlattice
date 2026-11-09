#![forbid(unsafe_code)]

//! `insights-job` (`cmd/worker/internal/insights/job.go` +
//! `internal/insights/background/background.go`).
//!
//! The job itself is a thin assembly:
//!
//! * gated by `internal/insights.IsEnabled()` (`DISABLE_CODE_INSIGHTS`,
//!   `DEPLOY_TYPE`, `ALLOW_SINGLE_DOCKER_CODE_INSIGHTS`); disabled means no
//!   routines at all;
//! * connects to the codeinsights database (`ServiceConnections.CodeInsightsDSN`);
//! * returns the five base routines of `background.GetBackgroundJobs`, in
//!   order: `insights.enqueuer`, `insights.retention.enqueuer`,
//!   `insights.pings_emitter`, `insights.data_prune`,
//!   `insights.license_check`.
//!
//! `GetBackgroundJobs` then appends the four historical-backfill monitor
//! routines unless `DISABLE_CODE_INSIGHTS_HISTORICAL` parses true:
//! `backfill_in_progress_worker`, its resetter, `backfill_new_backfill_worker`
//! and its resetter (see [`crate::jobs::insights_scheduler`]). The monitor is
//! assembled exactly like the baseline: an insights gitserver client feeding
//! both the commit client and the compression filter, the streaming search
//! provider and its sub-repo checker behind the three search handlers, the
//! codeinsights results store, the default pipeline backfiller, the
//! all-repositories iterator over the frontend database and the streaming repo
//! query executor.
//!
//! The license monitor runs the real `licensing.Check(FeatureCodeInsights)`
//! (`sg_licensing::check_code_insights`) against the configured license key
//! and the Redis validity flag set by `license-check`
//! (`licensing:is_license_valid`, valid when absent).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::insights_backfill_inprogress::{HandlerConfig, SystemClock};
use crate::jobs::insights_data_prune::{
    DataPruner, DATA_PRUNER_DESCRIPTION, DATA_PRUNER_INTERVAL, DATA_PRUNER_NAME,
};
use crate::jobs::insights_data_retention::{insights_enabled, parse_go_bool};
use crate::jobs::insights_enqueuer::{
    InsightEnqueuer, PgJobEnqueuer, RetentionEnqueuer, ENQUEUER_DESCRIPTION, ENQUEUER_INTERVAL,
    ENQUEUER_NAME, RETENTION_ENQUEUER_DESCRIPTION, RETENTION_ENQUEUER_INTERVAL,
    RETENTION_ENQUEUER_NAME,
};
use crate::jobs::insights_license_check::{
    LicenseCheck, LICENSE_CHECK_DESCRIPTION, LICENSE_CHECK_INTERVAL, LICENSE_CHECK_NAME,
};
use crate::jobs::insights_pings::{
    InsightsPingEmitter, PINGS_EMITTER_DESCRIPTION, PINGS_EMITTER_INTERVAL, PINGS_EMITTER_NAME,
};
use crate::jobs::insights_pipeline::{new_default_backfiller, BackfillerConfig};
use crate::jobs::insights_query_runner::limiter;
use crate::jobs::insights_query_runner::search;
use crate::jobs::insights_scheduler::{
    new_background_job_monitor, MonitorConfig, PgDiscoveryRepoStore, PgInsightRepoStore,
    PgResultsStore, PipelineBackfiller,
};
use crate::jobs::licensecheck::{is_license_valid, SharedStore};
use crate::jobs::{Job, JobDeps};

use sg_insights::compression::{DataFrameFilter, GitserverFilter};
use sg_insights::discovery::{
    AllReposIterator, Clock, RepoStore as DiscoveryRepoStore, SeriesRepoIterator,
};
use sg_insights::gitserver::GitCommitClient;

/// `insightsJob.Description` (empty in the baseline).
pub const JOB_DESCRIPTION: &str = "";

/// `DISABLE_CODE_INSIGHTS_HISTORICAL`.
pub const HISTORICAL_ENV: &str = "DISABLE_CODE_INSIGHTS_HISTORICAL";

/// `strconv.ParseBool(os.Getenv("DISABLE_CODE_INSIGHTS_HISTORICAL"))`: only a
/// parseable true disables the monitor; an unset or unparseable value keeps
/// it enabled.
pub fn historical_backfill_disabled(raw: Option<&str>) -> bool {
    match raw {
        Some(value) => parse_go_bool(value).0,
        None => false,
    }
}

/// `newInsightEnqueuer`'s handler: build the enqueuer per run, exactly like
/// the baseline's `HandlerFunc` close over `time.Now`.
struct EnqueuerHandler {
    frontend: PgPool,
    insights: PgPool,
}

#[async_trait]
impl Handler for EnqueuerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let enqueuer = InsightEnqueuer::new(
            OffsetDateTime::now_utc,
            OffsetDateTime::now_utc,
            PgJobEnqueuer::new(self.frontend.clone()),
        );
        enqueuer
            .discover_and_enqueue_insights(&self.insights)
            .await
            .map_err(HandlerError::new)
    }
}

/// `newRetentionEnqueuer`'s handler.
struct RetentionEnqueuerHandler {
    insights: PgPool,
}

#[async_trait]
impl Handler for RetentionEnqueuerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        RetentionEnqueuer::new(self.insights.clone())
            .enqueue_all()
            .await
            .map_err(HandlerError::new)
    }
}

/// `pings.NewInsightsPingEmitterJob`'s handler. The baseline's `emit` logs
/// every emitter failure and always returns nil; so does this handler.
struct PingsHandler {
    frontend: PgPool,
    insights: PgPool,
}

#[async_trait]
impl Handler for PingsHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        tracing::info!("Emitting Code Insights Pings");
        let emitter = InsightsPingEmitter::new(
            self.frontend.clone(),
            self.insights.clone(),
            OffsetDateTime::now_utc,
        );
        let failures = emitter.emit().await;
        for failure in &failures {
            tracing::error!("{}", failure.message);
        }
        if !failures.is_empty() {
            tracing::error!("Code Insights ping emitter encountered errors. Errors were skipped");
        }
        Ok(())
    }
}

/// `NewInsightsDataPrunerJob`'s handler.
struct DataPrunerHandler {
    frontend: PgPool,
    insights: PgPool,
}

#[async_trait]
impl Handler for DataPrunerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        DataPruner::new(
            self.frontend.clone(),
            self.insights.clone(),
            OffsetDateTime::now_utc,
        )
        .run()
        .await
        .map_err(HandlerError::new)
    }
}

/// `NewLicenseCheckJob`'s handler, running the real licensing check.
struct LicenseCheckHandler {
    insights: PgPool,
    live_config: Arc<LiveConfig>,
    store: Arc<SharedStore>,
}

#[async_trait]
impl Handler for LicenseCheckHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        // `licensing.Check` re-reads `conf.Get().LicenseKey` and the Redis
        // validity flag on every run.
        let license_key = self.live_config.current().site.license_key();
        let valid = match self.store.get().await {
            Ok(kv) => is_license_valid(kv).await,
            // `redispool.Store` yields a nil value (=> valid) when the flag
            // cannot be read.
            Err(_) => true,
        };
        LicenseCheck::new(self.insights.clone(), move || {
            sg_licensing::check_code_insights(&license_key, valid)
        })
        .run()
        .await
        .map_err(HandlerError::new)
    }
}

/// `insightsJob`.
pub struct InsightsJob;

#[async_trait]
impl Job for InsightsJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !insights_enabled() {
            tracing::debug!("Code Insights disabled. Disabling background jobs.");
            return Ok(Vec::new());
        }
        tracing::debug!("Code Insights enabled. Enabling background jobs.");

        let dsn = deps
            .live_config
            .current()
            .service_connections
            .code_insights_dsn
            .clone();
        let insights = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| format!("failed to connect to codeinsights database: {err}"))?;

        let mut routines = build_base_routines(deps, insights.clone());
        if !historical_backfill_disabled(std::env::var(HISTORICAL_ENV).ok().as_deref()) {
            routines.extend(build_historical_monitor(deps, insights).routines());
        }
        Ok(routines)
    }
}

/// `GetBackgroundJobs`'s historical branch: `scheduler.NewBackgroundJobMonitor`
/// with its production dependency graph.
fn build_historical_monitor(
    deps: &JobDeps,
    insights: PgPool,
) -> crate::jobs::insights_scheduler::BackgroundJobMonitor {
    let unified = deps.live_config.current();

    // `internalGitserver.NewClient("insights")`: one client behind both the
    // commit client and the compression filter.
    let git = Arc::new(sg_git::GitserverClient::new(
        unified.service_connections.git_servers.clone(),
    ));
    let commit_client = Arc::new(GitCommitClient::new(git));
    let compression_plan: Arc<dyn DataFrameFilter> =
        Arc::new(GitserverFilter::new(commit_client.clone()));

    let provider = Arc::new(search::HttpStreamingSearch::from_env());
    let sub_repo = Arc::new(search::PgSubRepoChecker::new(
        deps.frontend.clone(),
        Arc::clone(&deps.live_config),
    ));
    let search_handlers = search::get_search_handlers(provider.clone(), sub_repo);
    let results_store = Arc::new(PgResultsStore::new(insights.clone()));

    let site = unified.site.clone();
    let backfiller = new_default_backfiller(BackfillerConfig {
        commit_client,
        compression_plan,
        search_handlers,
        insight_store: results_store,
        search_plan_worker_limit: 1,
        search_runner_worker_limit: 1,
        search_rate_limiter: Arc::new(limiter::SearchQueryLimiter::from_site(&site)),
        historic_rate_limiter: Arc::new(limiter::SearchQueryLimiter::historical_from_site(&site)),
    });

    let discovery_store: Arc<dyn DiscoveryRepoStore> =
        Arc::new(PgDiscoveryRepoStore::new(deps.frontend.clone()));
    let clock: Arc<dyn Clock> = Arc::new(OffsetDateTime::now_utc);
    let all_repos = Arc::new(AllReposIterator::new(
        Arc::clone(&discovery_store),
        clock,
        crate::jobs::insights_scheduler::ALL_REPOS_CACHE_TIME,
    ));
    let repo_query_executor = Arc::new(search::StreamingRepoQueryExecutor::new(provider));
    let series_repo_iterator: Arc<dyn sg_insights::discovery::SeriesRepoSelector> = Arc::new(
        SeriesRepoIterator::new(all_repos, discovery_store, repo_query_executor),
    );

    new_background_job_monitor(MonitorConfig {
        insights_pool: insights,
        series_repo_iterator,
        insight_repo_store: Arc::new(PgInsightRepoStore::new(deps.frontend.clone())),
        backfiller: Arc::new(PipelineBackfiller::new(backfiller)),
        clock: Arc::new(SystemClock),
        handler_config: HandlerConfig::default(),
    })
}

/// `GetBackgroundJobs`'s five always-on routines, in baseline order.
fn build_base_routines(deps: &JobDeps, insights: PgPool) -> Vec<BackgroundRoutine> {
    let frontend = deps.frontend.clone();

    let enqueuer = PeriodicRoutine::new(Arc::new(EnqueuerHandler {
        frontend: frontend.clone(),
        insights: insights.clone(),
    }))
    .with_name(ENQUEUER_NAME)
    .with_description(ENQUEUER_DESCRIPTION)
    .with_interval(ENQUEUER_INTERVAL);

    let retention = PeriodicRoutine::new(Arc::new(RetentionEnqueuerHandler {
        insights: insights.clone(),
    }))
    .with_name(RETENTION_ENQUEUER_NAME)
    .with_description(RETENTION_ENQUEUER_DESCRIPTION)
    .with_interval(RETENTION_ENQUEUER_INTERVAL);

    let pings = PeriodicRoutine::new(Arc::new(PingsHandler {
        frontend: frontend.clone(),
        insights: insights.clone(),
    }))
    .with_name(PINGS_EMITTER_NAME)
    .with_description(PINGS_EMITTER_DESCRIPTION)
    .with_interval(PINGS_EMITTER_INTERVAL);

    let pruner = PeriodicRoutine::new(Arc::new(DataPrunerHandler {
        frontend,
        insights: insights.clone(),
    }))
    .with_name(DATA_PRUNER_NAME)
    .with_description(DATA_PRUNER_DESCRIPTION)
    .with_interval(DATA_PRUNER_INTERVAL);

    let license_check = PeriodicRoutine::new(Arc::new(LicenseCheckHandler {
        insights,
        live_config: Arc::clone(&deps.live_config),
        store: Arc::new(SharedStore::new(sg_redis::store_endpoint())),
    }))
    .with_name(LICENSE_CHECK_NAME)
    .with_description(LICENSE_CHECK_DESCRIPTION)
    .with_interval(LICENSE_CHECK_INTERVAL);

    vec![
        enqueuer.into(),
        retention.into(),
        pings.into(),
        pruner.into(),
        license_check.into(),
    ]
}

/// The baseline routine metadata, for tests and the differential harness.
pub fn routine_metadata() -> [(&'static str, &'static str, Duration); 5] {
    [
        (ENQUEUER_NAME, ENQUEUER_DESCRIPTION, ENQUEUER_INTERVAL),
        (
            RETENTION_ENQUEUER_NAME,
            RETENTION_ENQUEUER_DESCRIPTION,
            RETENTION_ENQUEUER_INTERVAL,
        ),
        (
            PINGS_EMITTER_NAME,
            PINGS_EMITTER_DESCRIPTION,
            PINGS_EMITTER_INTERVAL,
        ),
        (
            DATA_PRUNER_NAME,
            DATA_PRUNER_DESCRIPTION,
            DATA_PRUNER_INTERVAL,
        ),
        (
            LICENSE_CHECK_NAME,
            LICENSE_CHECK_DESCRIPTION,
            LICENSE_CHECK_INTERVAL,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = InsightsJob;
        assert_eq!(job.description(), "");
        assert!(job.configs().is_empty());
    }

    #[test]
    fn historical_backfill_gate_matches_strconv() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert!(historical_backfill_disabled(Some(raw)), "{raw}");
        }
        for raw in ["0", "f", "F", "FALSE", "false", "False", ""] {
            assert!(!historical_backfill_disabled(Some(raw)), "{raw}");
        }
        assert!(!historical_backfill_disabled(Some("yes")));
        assert!(!historical_backfill_disabled(None));
    }
}
