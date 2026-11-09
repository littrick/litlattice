#![forbid(unsafe_code)]

//! `internal/insights/scheduler`: the historical backfill monitor
//! (`NewBackgroundJobMonitor`).
//!
//! The monitor owns the two backfill queues of `insights_background_jobs`:
//!
//! * `backfill_in_progress_worker` (+ resetter) over the
//!   `insights_jobs_backfill_in_progress` view, ordered by
//!   `estimated_cost, backfill_id`;
//! * `backfill_new_backfill_worker` (+ resetter) over the
//!   `insights_jobs_backfill_new` view, ordered by `id` (oldest first).
//!
//! Both workers share the generic [`crate::framework::dbworker`] loop and the
//! [`BackfillJobStore`] from `insights_backfill_store`; the handlers are the
//! already ported `newBackfillHandler` and `inProgressHandler`, wrapped in
//! [`DbWorkerHandler`] adapters so the worker can dequeue a `BaseJob` and pass
//! the pair `(job id, backfill id)` to them.
//!
//! [`new_background_job_monitor`] mirrors `scheduler.NewBackgroundJobMonitor`;
//! [`BackgroundJobMonitor::routines`] returns the four routines in the
//! baseline order (in-progress worker, in-progress resetter, new worker, new
//! resetter).
//!
//! The remaining types in this module are the production dependency graph
//! that `background.GetBackgroundJobs` assembles for the monitor:
//!
//! * [`PgDiscoveryRepoStore`]: `database.Repos().List(LimitOffset)` for the
//!   all-repositories iterator (internal-actor visibility: soft-deleted and
//!   blocked repositories are excluded, no authz restriction);
//! * [`PgInsightRepoStore`]: `database.Repos().Get` by id for the in-progress
//!   handler;
//! * [`PgResultsStore`]: `store.RecordSeriesPoints` over the codeinsights
//!   database;
//! * [`PipelineBackfiller`]: adapts the ported
//!   [`crate::jobs::insights_pipeline::Backfiller`] to the handler's
//!   `Backfiller` trait, converting the series/repository views.
//!
//! Not ported: the Prometheus metrics of `workerutil.NewMetrics` /
//! `dbworker.NewResetterMetrics` and the `AllReposIterator` counter (the
//! iterator cache and pagination are ported in `sg_insights::discovery`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use sg_db::repos::RepoStore;
use sg_insights::discovery::{
    RepoStore as DiscoveryRepoStore, RepoSummary, ReposListOptions, SeriesRepoSelector,
};

use crate::framework::dbworker::{
    DbBackedRoutine, DbResetter, DbWorkerHandler, ResetterOptions, ResetterRoutine, WorkerOptions,
    WorkerRoutine,
};
use crate::framework::BackgroundRoutine;
use crate::jobs::insights_backfill_handler::{InsightSeriesRecord, NewBackfillHandler};
use crate::jobs::insights_backfill_inprogress::{
    BackfillError, Backfiller, HandlerClock, HandlerConfig, InProgressHandler, InsightRepo,
    InsightRepoStore, RepoGetError,
};
use crate::jobs::insights_backfill_store::{
    BackfillJob, BackfillJobStore, IN_PROGRESS_STORE, NEW_STORE,
};
use crate::jobs::insights_pipeline::{self, MinimalRepo as PipelineRepo};
use crate::jobs::insights_query_runner::insights_store::{self, RecordSeriesPoint, Series};

/// `makeInProgressWorker`'s `WorkerOptions.Name`.
pub const IN_PROGRESS_WORKER_NAME: &str = "backfill_in_progress_worker";
/// `makeInProgressWorker`'s `WorkerOptions.Description`.
pub const IN_PROGRESS_WORKER_DESCRIPTION: &str =
    "generates and runs searches to backfill a code insight";
/// `inProgressPollingInterval`.
pub const IN_PROGRESS_WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `makeInProgressWorker`'s `WorkerOptions.HeartbeatInterval`.
pub const IN_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `makeInProgressWorker`'s `ResetterOptions.Name`.
pub const IN_PROGRESS_RESETTER_NAME: &str = "backfill_in_progress_worker_resetter";
/// `makeInProgressWorker`'s `ResetterOptions.Interval`.
pub const IN_PROGRESS_RESETTER_INTERVAL: Duration = Duration::from_secs(20);

/// `makeNewBackfillWorker`'s `WorkerOptions.Name`.
pub const NEW_BACKFILL_WORKER_NAME: &str = "backfill_new_backfill_worker";
/// `makeNewBackfillWorker`'s `WorkerOptions.Description`.
pub const NEW_BACKFILL_WORKER_DESCRIPTION: &str =
    "determines the repos for a code insight and an approximate cost of the backfill";
/// `makeNewBackfillWorker`'s `WorkerOptions.Interval`.
pub const NEW_BACKFILL_WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `makeNewBackfillWorker`'s `WorkerOptions.HeartbeatInterval`.
pub const NEW_BACKFILL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `makeNewBackfillWorker`'s `ResetterOptions.Name`.
pub const NEW_BACKFILL_RESETTER_NAME: &str = "backfill_new_backfill_worker_resetter";
/// `makeNewBackfillWorker`'s `ResetterOptions.Interval`.
pub const NEW_BACKFILL_RESETTER_INTERVAL: Duration = Duration::from_secs(20);

/// `NewAllReposIterator`'s cache window (`15 * time.Minute`); the iterator
/// takes a `time::Duration`.
pub const ALL_REPOS_CACHE_TIME: time::Duration = time::Duration::minutes(15);

/// `pipeline.BackfillRequest` -> `workerutil.Handler` adapter for the
/// in-progress queue (`inProgressHandler.Handle`).
pub struct InProgressDbHandler {
    inner: InProgressHandler,
}

impl InProgressDbHandler {
    pub fn new(inner: InProgressHandler) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl DbWorkerHandler<BackfillJobStore> for InProgressDbHandler {
    async fn handle(&self, _ctx: CancellationToken, record: &BackfillJob) -> Result<(), String> {
        let backfill_id = record.backfill_id.unwrap_or(0);
        self.inner.handle(record.id, backfill_id).await
    }
}

/// `newBackfillHandler` adapter for the "new" queue.
pub struct NewBackfillDbHandler {
    inner: NewBackfillHandler,
}

impl NewBackfillDbHandler {
    pub fn new(inner: NewBackfillHandler) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl DbWorkerHandler<BackfillJobStore> for NewBackfillDbHandler {
    async fn handle(&self, _ctx: CancellationToken, record: &BackfillJob) -> Result<(), String> {
        let backfill_id = record.backfill_id.unwrap_or(0);
        self.inner.handle_new_backfill(record.id, backfill_id).await
    }
}

/// `scheduler.JobMonitorConfig`, reduced to the already-constructed handler
/// dependencies (the job assembles them in `insights_job`).
pub struct MonitorConfig {
    /// The codeinsights pool both worker stores and handlers use.
    pub insights_pool: PgPool,
    /// `discovery.NewSeriesRepoIterator(...)` for the "new" handler.
    pub series_repo_iterator: Arc<dyn SeriesRepoSelector>,
    /// `database.Repos()` for the in-progress handler (`repoStore.Get`).
    pub insight_repo_store: Arc<dyn InsightRepoStore>,
    /// `pipeline.NewDefaultBackfiller(...)` behind the handler trait.
    pub backfiller: Arc<dyn Backfiller>,
    /// `glock.NewRealClock()`.
    pub clock: Arc<dyn HandlerClock>,
    /// `newHandlerConfig()`.
    pub handler_config: HandlerConfig,
}

/// `scheduler.BackgroundJobMonitor`.
pub struct BackgroundJobMonitor {
    pub in_progress_worker: Arc<WorkerRoutine<BackfillJobStore, InProgressDbHandler>>,
    pub in_progress_resetter: Arc<ResetterRoutine>,
    pub in_progress_store: Arc<BackfillJobStore>,
    pub new_backfill_worker: Arc<WorkerRoutine<BackfillJobStore, NewBackfillDbHandler>>,
    pub new_backfill_resetter: Arc<ResetterRoutine>,
    pub new_backfill_store: Arc<BackfillJobStore>,
}

impl BackgroundJobMonitor {
    /// `BackgroundJobMonitor.Routines`, in baseline order.
    pub fn routines(&self) -> Vec<BackgroundRoutine> {
        vec![
            BackgroundRoutine::DbBacked(
                Arc::clone(&self.in_progress_worker) as Arc<dyn DbBackedRoutine>
            ),
            BackgroundRoutine::DbBacked(
                Arc::clone(&self.in_progress_resetter) as Arc<dyn DbBackedRoutine>
            ),
            BackgroundRoutine::DbBacked(
                Arc::clone(&self.new_backfill_worker) as Arc<dyn DbBackedRoutine>
            ),
            BackgroundRoutine::DbBacked(
                Arc::clone(&self.new_backfill_resetter) as Arc<dyn DbBackedRoutine>
            ),
        ]
    }
}

/// `scheduler.NewBackgroundJobMonitor`: builds the two workers, their two
/// resetters and their stores.
pub fn new_background_job_monitor(config: MonitorConfig) -> BackgroundJobMonitor {
    let in_progress_store = Arc::new(BackfillJobStore::new(
        config.insights_pool.clone(),
        IN_PROGRESS_STORE,
    ));
    let new_backfill_store = Arc::new(BackfillJobStore::new(
        config.insights_pool.clone(),
        NEW_STORE,
    ));

    let in_progress_handler = Arc::new(InProgressDbHandler::new(InProgressHandler::new(
        config.insights_pool.clone(),
        config.insight_repo_store,
        config.backfiller,
        config.clock,
        config.handler_config,
    )));
    let new_backfill_handler = Arc::new(NewBackfillDbHandler::new(NewBackfillHandler::new(
        config.insights_pool,
        config.series_repo_iterator,
    )));

    let in_progress_worker = Arc::new(WorkerRoutine::new(
        Arc::clone(&in_progress_store),
        in_progress_handler,
        WorkerOptions {
            name: IN_PROGRESS_WORKER_NAME.to_owned(),
            description: IN_PROGRESS_WORKER_DESCRIPTION.to_owned(),
            interval: IN_PROGRESS_WORKER_INTERVAL,
            heartbeat_interval: IN_PROGRESS_HEARTBEAT_INTERVAL,
            num_handlers: 1,
            worker_hostname: crate::framework::dbworker::default_hostname(),
        },
    ));
    let in_progress_resetter = Arc::new(ResetterRoutine::new(
        Arc::clone(&in_progress_store) as Arc<dyn DbResetter>,
        ResetterOptions {
            name: IN_PROGRESS_RESETTER_NAME.to_owned(),
            interval: IN_PROGRESS_RESETTER_INTERVAL,
        },
    ));

    let new_backfill_worker = Arc::new(WorkerRoutine::new(
        Arc::clone(&new_backfill_store),
        new_backfill_handler,
        WorkerOptions {
            name: NEW_BACKFILL_WORKER_NAME.to_owned(),
            description: NEW_BACKFILL_WORKER_DESCRIPTION.to_owned(),
            interval: NEW_BACKFILL_WORKER_INTERVAL,
            heartbeat_interval: NEW_BACKFILL_HEARTBEAT_INTERVAL,
            num_handlers: 1,
            worker_hostname: crate::framework::dbworker::default_hostname(),
        },
    ));
    let new_backfill_resetter = Arc::new(ResetterRoutine::new(
        Arc::clone(&new_backfill_store) as Arc<dyn DbResetter>,
        ResetterOptions {
            name: NEW_BACKFILL_RESETTER_NAME.to_owned(),
            interval: NEW_BACKFILL_RESETTER_INTERVAL,
        },
    ));

    BackgroundJobMonitor {
        in_progress_worker,
        in_progress_resetter,
        in_progress_store,
        new_backfill_worker,
        new_backfill_resetter,
        new_backfill_store,
    }
}

/// `database.Repos().List(opt)` for the subset discovery uses.
///
/// Only two filters are reachable from the iterator: the name list
/// (`ScopedRepoIterator`) and the id-ordered page (`AllReposIterator`). The
/// internal actor bypasses authz, so the default conditions remain
/// (`deleted_at IS NULL AND blocked IS NULL`). Names compare with
/// `lower(name::text) COLLATE "C" = ANY (...)` exactly like the baseline
/// (which lowercases the inputs first and relies on the `repo_name_idx`
/// btree index), and the sort is by `repo.id`.
pub struct PgDiscoveryRepoStore {
    pool: PgPool,
}

impl PgDiscoveryRepoStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DiscoveryRepoStore for PgDiscoveryRepoStore {
    async fn list(&self, opts: &ReposListOptions) -> Result<Vec<RepoSummary>, String> {
        let names: Vec<String> = opts
            .names
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|name| name.to_lowercase())
            .collect();
        let page = opts.limit_offset.clone();

        let mut query = String::from(
            "SELECT id, name FROM repo \
             WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL",
        );
        let mut next_index = 1;
        if !names.is_empty() {
            query.push_str(&format!(
                r#" AND lower(name::text) COLLATE "C" = ANY (${next_index}::text[])"#
            ));
            next_index += 1;
        }
        query.push_str(" ORDER BY repo.id");
        if page.is_some() {
            query.push_str(&format!(" LIMIT ${next_index} OFFSET ${}", next_index + 1));
        }

        let mut request = sqlx::query(&query);
        if !names.is_empty() {
            request = request.bind(names);
        }
        if let Some(page) = page {
            request = request.bind(page.limit).bind(page.offset);
        }
        let rows = request
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
        rows.iter()
            .map(|row| {
                Ok(RepoSummary {
                    id: i64::from(
                        row.try_get::<i32, _>("id")
                            .map_err(|err: sqlx::Error| err.to_string())?,
                    ),
                    name: row
                        .try_get("name")
                        .map_err(|err: sqlx::Error| err.to_string())?,
                })
            })
            .collect()
    }
}

/// `database.Repos().Get` for the in-progress handler.
pub struct PgInsightRepoStore {
    store: RepoStore,
}

impl PgInsightRepoStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            store: RepoStore::new(pool),
        }
    }
}

#[async_trait]
impl InsightRepoStore for PgInsightRepoStore {
    async fn get(&self, id: i32) -> Result<InsightRepo, RepoGetError> {
        let repo = self
            .store
            .get_by_id(id)
            .await
            .map_err(|err| RepoGetError::Other(err.to_string()))?;
        let Some(repo) = repo else {
            return Err(RepoGetError::NotFound);
        };
        if let Some(blocked) = &repo.blocked {
            let reason = blocked
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            return Err(RepoGetError::Other(format!(
                "repository {} has been blocked. reason: {reason}",
                repo.name
            )));
        }
        Ok(InsightRepo {
            id: repo.id,
            name: repo.name,
        })
    }
}

/// `store.Interface.RecordSeriesPoints` over the codeinsights database.
pub struct PgResultsStore {
    pool: PgPool,
}

impl PgResultsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl insights_pipeline::ResultsStore for PgResultsStore {
    async fn record_series_points(&self, points: &[RecordSeriesPoint]) -> Result<(), String> {
        let mut conn = self.pool.acquire().await.map_err(|err| err.to_string())?;
        insights_store::record_series_points(&mut conn, points).await
    }
}

/// Adapts the ctx-aware pipeline backfiller to the in-progress handler's
/// `Backfiller` trait, translating the two series/repository views.
pub struct PipelineBackfiller {
    inner: Box<dyn insights_pipeline::Backfiller>,
}

impl PipelineBackfiller {
    pub fn new(inner: Box<dyn insights_pipeline::Backfiller>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Backfiller for PipelineBackfiller {
    async fn run(
        &self,
        request: crate::jobs::insights_backfill_inprogress::BackfillRequest,
    ) -> Result<(), BackfillError> {
        let pipeline_request = insights_pipeline::BackfillRequest {
            series: series_from_record(&request.series),
            repo: PipelineRepo {
                id: request.repo.id,
                name: request.repo.name,
            },
            sample_times: request.sample_times,
        };
        // `Backfiller.Run` takes the worker context; the ported in-progress
        // handler does not thread one, so each run gets its own token.
        self.inner
            .run(&CancellationToken::new(), pipeline_request)
            .await
            .map_err(BackfillError::Message)
    }
}

/// `types.InsightSeries` (the record view) -> the pipeline's `Series`.
pub fn series_from_record(record: &InsightSeriesRecord) -> Series {
    Series {
        id: record.id,
        series_id: record.series_id.clone(),
        query: record.query.clone(),
        repositories: record.repositories.clone(),
        sample_interval_unit: record.sample_interval_unit.clone(),
        sample_interval_value: record.sample_interval_value,
        group_by: None,
        repository_criteria: record.repository_criteria.clone(),
        just_in_time: false,
        generation_method: record.generation_method.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_metadata_matches_the_baseline() {
        assert_eq!(IN_PROGRESS_WORKER_NAME, "backfill_in_progress_worker");
        assert_eq!(
            IN_PROGRESS_WORKER_DESCRIPTION,
            "generates and runs searches to backfill a code insight"
        );
        assert_eq!(IN_PROGRESS_WORKER_INTERVAL, Duration::from_secs(5));
        assert_eq!(IN_PROGRESS_HEARTBEAT_INTERVAL, Duration::from_secs(15));
        assert_eq!(
            IN_PROGRESS_RESETTER_NAME,
            "backfill_in_progress_worker_resetter"
        );
        assert_eq!(IN_PROGRESS_RESETTER_INTERVAL, Duration::from_secs(20));

        assert_eq!(NEW_BACKFILL_WORKER_NAME, "backfill_new_backfill_worker");
        assert_eq!(
            NEW_BACKFILL_WORKER_DESCRIPTION,
            "determines the repos for a code insight and an approximate cost of the backfill"
        );
        assert_eq!(NEW_BACKFILL_WORKER_INTERVAL, Duration::from_secs(5));
        assert_eq!(NEW_BACKFILL_HEARTBEAT_INTERVAL, Duration::from_secs(15));
        assert_eq!(
            NEW_BACKFILL_RESETTER_NAME,
            "backfill_new_backfill_worker_resetter"
        );
        assert_eq!(NEW_BACKFILL_RESETTER_INTERVAL, Duration::from_secs(20));
    }

    #[test]
    fn series_conversion_keeps_the_pipeline_fields() {
        let record = InsightSeriesRecord {
            id: 7,
            series_id: "series-7".to_owned(),
            query: "count()".to_owned(),
            created_at: sqlx::types::time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2023, time::Month::January, 2).unwrap(),
                time::Time::MIDNIGHT,
            ),
            sample_interval_unit: "WEEK".to_owned(),
            sample_interval_value: 2,
            generated_from_capture_groups: true,
            repositories: vec!["repo-a".to_owned()],
            repository_criteria: Some("repo:repo-a".to_owned()),
            generation_method: "search-compute".to_owned(),
        };
        let series = series_from_record(&record);
        assert_eq!(series.id, 7);
        assert_eq!(series.series_id, "series-7");
        assert_eq!(series.query, "count()");
        assert_eq!(series.sample_interval_unit, "WEEK");
        assert_eq!(series.sample_interval_value, 2);
        assert_eq!(series.repositories, vec!["repo-a".to_owned()]);
        assert_eq!(series.repository_criteria.as_deref(), Some("repo:repo-a"));
        assert_eq!(series.generation_method, "search-compute");
    }
}
