#![forbid(unsafe_code)]

//! `insights-query-runner-job` (`cmd/worker/internal/insights`,
//! `internal/insights/background/queryrunner`).
//!
//! When code insights is enabled the job builds the process's query-runner
//! trio:
//!
//! * `insights_query_runner_worker` — a `workerutil.Worker` over
//!   `insights_query_runner_jobs` in the **frontend** database. Concurrency is
//!   `insights.query.worker.concurrency` (default 5), interval 5s, heartbeat
//!   15s. Each job runs one search/compute query through the frontend
//!   internal streaming API and records the per-repository counts into the
//!   **codeinsights** database.
//! * `insights_query_runner_worker_resetter` — `dbworker.Resetter` every
//!   minute (RetryAfter 30m, MaxNumRetries 10, MaxNumResets 10, StalledMaxAge
//!   60s).
//! * `insights.query_runner_cleaner` — hourly `PeriodicGoroutine` that
//!   deletes `completed` jobs older than a week.
//!
//! The queue table and all insight tables already exist; no schema changes.
//! Storage details are in [`store`] (frontend) and [`insights_store`]
//! (codeinsights); the streaming API is in [`streaming`]/[`search`]; the
//! handler is in [`handler`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use sg_config::LiveConfig;
use sg_db::repos::RepoStore;

use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbResetter, ResetterOptions, ResetterRoutine, WorkerOptions,
    WorkerRoutine,
};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::{Job, JobDeps};

pub mod handler;
pub mod insights_store;
pub mod limiter;
pub mod search;
pub mod store;
pub mod streaming;

/// `insightsQueryRunnerJob.Description` (empty in the baseline).
pub const JOB_DESCRIPTION: &str = "";

/// `NewWorker`'s `WorkerOptions.Name`.
pub const WORKER_NAME: &str = "insights_query_runner_worker";
/// `NewWorker`'s `WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str =
    "runs code insights queries for daily snapshots and new recordings";
/// `NewWorker`'s `WorkerOptions.Interval`.
pub const WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `NewWorker`'s `WorkerOptions.HeartbeatInterval`.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `NewWorker`'s default `NumHandlers` when the site config is `<= 0`.
pub const DEFAULT_CONCURRENCY: i64 = 5;

/// `NewResetter`'s `ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "insights_query_runner_worker_resetter";
/// `NewResetter`'s `ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

/// `NewCleaner`'s `goroutine.WithName`.
pub const CLEANER_NAME: &str = "insights.query_runner_cleaner";
/// `NewCleaner`'s `goroutine.WithDescription`.
pub const CLEANER_DESCRIPTION: &str = "removes completed or failed query runner jobs";
/// `NewCleaner`'s `goroutine.WithInterval`.
pub const CLEANER_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// `getSearchQueryWorkerConcurrency`: the configured value, or 5 when unset
/// or non-positive.
pub fn worker_concurrency(site: &sg_config::SiteConfiguration) -> usize {
    let configured = site.insights_query_worker_concurrency();
    if configured <= 0 {
        DEFAULT_CONCURRENCY as usize
    } else {
        configured as usize
    }
}

/// `insights.query_runner_cleaner`'s handler.
pub struct CleanerHandler {
    frontend: sqlx::PgPool,
}

impl CleanerHandler {
    pub fn new(frontend: sqlx::PgPool) -> Self {
        Self { frontend }
    }
}

#[async_trait]
impl Handler for CleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let before = sqlx::types::time::OffsetDateTime::now_utc()
            - time::Duration::try_from(store::CLEAN_MAX_AGE)
                .map_err(|_| HandlerError::new("query runner clean outside timestamp range"))?;
        store::clean_jobs(&self.frontend, before)
            .await
            .map(|_| ())
            .map_err(HandlerError::new)
    }
}

/// `insightsQueryRunnerJob`.
pub struct InsightsQueryRunnerJob;

#[async_trait]
impl Job for InsightsQueryRunnerJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !super::insights_data_retention::insights_enabled() {
            tracing::debug!("Code Insights disabled. Disabling query runner.");
            return Ok(Vec::new());
        }
        tracing::debug!("Code Insights enabled. Enabling query runner.");

        let dsn = deps
            .live_config
            .current()
            .service_connections
            .code_insights_dsn
            .clone();
        let insights_pool = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| format!("failed to connect to codeinsights database: {err}"))?;

        let site = deps.live_config.current().site.clone();
        let store = Arc::new(store::QueryRunnerStore::new(deps.frontend.clone()));
        let repo_store = RepoStore::new(deps.frontend.clone());
        let provider = Arc::new(search::HttpStreamingSearch::from_env());
        let live_config: Arc<LiveConfig> = Arc::clone(&deps.live_config);
        let sub_repo = Arc::new(search::PgSubRepoChecker::new(
            deps.frontend.clone(),
            live_config,
        ));
        let limiter = Arc::new(limiter::SearchQueryLimiter::from_site(&site));

        let handler = Arc::new(handler::QueryRunnerHandler::new(
            Arc::clone(&store),
            insights_pool.clone(),
            repo_store,
            provider,
            sub_repo,
            limiter,
        ));

        let worker = WorkerRoutine::new(
            Arc::clone(&store),
            handler,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: WORKER_INTERVAL,
                heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
                num_handlers: worker_concurrency(&site),
                worker_hostname: default_hostname(),
            },
        );

        let resetter = ResetterRoutine::new(
            store as Arc<dyn DbResetter>,
            ResetterOptions {
                name: RESETTER_NAME.to_owned(),
                interval: RESETTER_INTERVAL,
            },
        );

        let cleaner = PeriodicRoutine::new(Arc::new(CleanerHandler::new(deps.frontend.clone())))
            .with_name(CLEANER_NAME)
            .with_description(CLEANER_DESCRIPTION)
            .with_interval(CLEANER_INTERVAL);

        Ok(vec![
            BackgroundRoutine::DbBacked(Arc::new(worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(resetter) as Arc<dyn DbBackedRoutine>),
            cleaner.into(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = InsightsQueryRunnerJob;
        assert_eq!(job.description(), "");
        assert!(job.configs().is_empty());
    }

    #[test]
    fn routine_names_and_intervals_match_the_baseline() {
        assert_eq!(WORKER_NAME, "insights_query_runner_worker");
        assert_eq!(RESETTER_NAME, "insights_query_runner_worker_resetter");
        assert_eq!(CLEANER_NAME, "insights.query_runner_cleaner");
        assert_eq!(WORKER_INTERVAL, Duration::from_secs(5));
        assert_eq!(WORKER_HEARTBEAT_INTERVAL, Duration::from_secs(15));
        assert_eq!(RESETTER_INTERVAL, Duration::from_secs(60));
        assert_eq!(CLEANER_INTERVAL, Duration::from_secs(3600));
    }

    #[test]
    fn concurrency_defaults_to_five() {
        let site = sg_config::SiteConfiguration::parse("{}").unwrap();
        assert_eq!(worker_concurrency(&site), 5);

        let site =
            sg_config::SiteConfiguration::parse("{\"insights.query.worker.concurrency\": 8}")
                .unwrap();
        assert_eq!(worker_concurrency(&site), 8);

        let site =
            sg_config::SiteConfiguration::parse("{\"insights.query.worker.concurrency\": 0}")
                .unwrap();
        assert_eq!(worker_concurrency(&site), 5);

        let site =
            sg_config::SiteConfiguration::parse("{\"insights.query.worker.concurrency\": -3}")
                .unwrap();
        assert_eq!(worker_concurrency(&site), 5);
    }
}
