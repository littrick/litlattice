#![forbid(unsafe_code)]

//! Port of `inProgressHandler.Handle` from
//! `internal/insights/scheduler/backfill_state_inprogress_handler.go`.
//!
//! The handler consumes a job from the `insights_jobs_backfill_in_progress`
//! view, walks the persisted repository iterator page by page (running one
//! backfill per repository), retries errored repositories, marks repositories
//! that hit `MaxFailures` terminal (writing one incomplete datapoint per sample
//! time inside the finish transaction), and finally completes the backfill or
//! disables it when the error threshold is exceeded. If the configured
//! interrupt deadline passes, the job is requeued and the next execution
//! resumes from the persisted cursor.
//!
//! Transaction boundaries mirror the baseline:
//!
//! * `finish` opens one transaction for `iterator.MarkComplete` and
//!   `backfill.SetCompleted`, but `SetSeriesBackfillComplete` runs on the plain
//!   insight store (its own autocommit connection) between them — the same
//!   quirk as Go, where the stamp survives a later rollback;
//! * `disableBackfill` runs `SetFailed`, `MarkComplete` and every
//!   `AddIncompleteDatapoint` in one transaction;
//! * the terminal callback writes incomplete points through the finish
//!   transaction (`insightsStore.WithOther(tx)`).
//!
//! The backfiller, repository lookup and clock are injected so the worker can
//! supply the real implementations and the differential harness can script
//! them. Error prefixes reproduce the baseline (`loadBackfill`,
//! `GetDataSeriesByID`, `repoIterator`, `InProgressHandler.PrimaryLoop`,
//! `InProgressHandler.RetryLoop`, `disableBackfill`, `iterator.MarkComplete`,
//! `backfill.SetCompleted`, `SetFailed`, `itr.MarkComplete`,
//! `SetFailed.AddIncompleteDatapoint`, `IncompleteHandler.repoStore.Get`,
//! `AddIncompleteDatapoint`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

use sg_db::insights_scheduler::{
    self as is, IterationConfig, PersistentRepoIterator, SeriesBackfill,
};
use sg_insights::scheduler::{
    calculate_error_threshold, translate_incomplete_reasons, IncompleteReason, SearchTimeoutError,
};
use sg_insights::timeseries::{build_sample_times, TimeInterval};
use sg_insights::types::IntervalUnit;

use crate::jobs::insights_backfill_handler::{
    get_data_series_by_id, sql_error, truncate_to_minute, InsightSeriesRecord, SAMPLE_POINTS,
};
use crate::jobs::insights_backfill_store::{BackfillJobStore, IN_PROGRESS_STORE};
use crate::jobs::insights_query_runner::insights_store;

/// `defaultInterruptSeconds`.
pub const DEFAULT_INTERRUPT_SECONDS: u64 = 60;
/// `inProgressPollingInterval` (also the requeue delay and the retry loop's
/// page/concurrency).
pub const IN_PROGRESS_POLLING_INTERVAL: Duration = Duration::from_secs(5);
/// `defaultErrorThresholdFloor`.
pub const DEFAULT_ERROR_THRESHOLD_FLOOR: i64 = 50;
/// `iterator.IterationConfig{MaxFailures: 3}` in `doExecution`.
pub const MAX_FAILURES: i32 = 3;

/// `handlerConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerConfig {
    pub interrupt_after: Duration,
    pub error_threshold_floor: i64,
    pub page_size: usize,
    pub repo_concurrency: usize,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            interrupt_after: Duration::from_secs(DEFAULT_INTERRUPT_SECONDS),
            error_threshold_floor: DEFAULT_ERROR_THRESHOLD_FLOOR,
            page_size: 10,
            repo_concurrency: 3,
        }
    }
}

/// `glock.Clock` for the handler.
pub trait HandlerClock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

/// `glock.NewRealClock()`.
pub struct SystemClock;

impl HandlerClock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// `types.MinimalRepo` as returned by `repoStore.Get`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsightRepo {
    pub id: i32,
    pub name: String,
}

/// `repoStore.Get` errors: a missing (or soft-deleted) repository is
/// `database.RepoNotFoundErr` and is skipped by the handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoGetError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for RepoGetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoGetError::NotFound => f.write_str("repo not found"),
            RepoGetError::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RepoGetError {}

/// `database.RepoStore.Get` (only the lookup the handler needs).
#[async_trait]
pub trait InsightRepoStore: Send + Sync {
    async fn get(&self, id: i32) -> Result<InsightRepo, RepoGetError>;
}

/// `pipeline.BackfillRequest`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillRequest {
    pub series: InsightSeriesRecord,
    pub repo: InsightRepo,
    pub sample_times: Vec<OffsetDateTime>,
}

/// The failure of one repository run.
///
/// `Timeout` is a search timeout laid out exactly as the runner returns it
/// (`queryrunner.SearchTimeoutError` wrapped as `searchHandler: search
/// timeout`), so the stored `repo_iterator_errors` message and the `timeout`
/// incomplete reason both match the baseline. The payload keeps the original
/// runner text, since Go stores `maybeErr.Error()` verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillError {
    Timeout(String),
    Message(String),
}

static SEARCH_TIMEOUT: SearchTimeoutError = SearchTimeoutError;

impl std::fmt::Display for BackfillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillError::Timeout(message) | BackfillError::Message(message) => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for BackfillError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BackfillError::Timeout(_) => Some(&SEARCH_TIMEOUT),
            BackfillError::Message(_) => None,
        }
    }
}

/// `pipeline.Backfiller`.
#[async_trait]
pub trait Backfiller: Send + Sync {
    async fn run(&self, request: BackfillRequest) -> Result<(), BackfillError>;
}

/// The in-memory execution state (`backfillExecution`).
struct BackfillExecution {
    series: InsightSeriesRecord,
    backfill: SeriesBackfill,
    itr: PersistentRepoIterator,
    sample_times: Vec<OffsetDateTime>,
    config: HandlerConfig,
}

impl BackfillExecution {
    /// `backfillExecution.exceedsErrorThreshold`.
    fn exceeds_error_threshold(&self) -> bool {
        self.itr.total_errors()
            > calculate_error_threshold(
                0.05,
                self.config.error_threshold_floor,
                i64::from(self.itr.row.total_count),
            )
    }
}

/// `inProgressHandler`.
pub struct InProgressHandler {
    pool: PgPool,
    worker_store: BackfillJobStore,
    repo_store: Arc<dyn InsightRepoStore>,
    backfiller: Arc<dyn Backfiller>,
    clock: Arc<dyn HandlerClock>,
    config: HandlerConfig,
}

impl InProgressHandler {
    pub fn new(
        pool: PgPool,
        repo_store: Arc<dyn InsightRepoStore>,
        backfiller: Arc<dyn Backfiller>,
        clock: Arc<dyn HandlerClock>,
        config: HandlerConfig,
    ) -> Self {
        Self {
            worker_store: BackfillJobStore::new(pool.clone(), IN_PROGRESS_STORE),
            pool,
            repo_store,
            backfiller,
            clock,
            config,
        }
    }

    /// `inProgressHandler.Handle`.
    pub async fn handle(&self, job_id: i32, backfill_id: i32) -> Result<(), String> {
        let mut execution = self.load(backfill_id).await?;
        if self.do_execution(&mut execution).await? {
            self.do_interrupt(job_id).await
        } else {
            Ok(())
        }
    }

    /// `inProgressHandler.load`.
    async fn load(&self, backfill_id: i32) -> Result<BackfillExecution, String> {
        let backfill = is::load_backfill(&self.pool, backfill_id)
            .await
            .map_err(|err| format!("loadBackfill: {}", sql_error(&err)))?;
        let series = get_data_series_by_id(&self.pool, backfill.series_id)
            .await
            .map_err(|err| format!("GetDataSeriesByID: {err}"))?;
        let itr = self.repo_iterator(&backfill).await?;

        let interval = TimeInterval {
            unit: IntervalUnit::from_str_value(&series.sample_interval_unit),
            value: i64::from(series.sample_interval_value),
        };
        let created_at = truncate_to_minute(series.created_at.assume_utc());
        let sample_times = build_sample_times(SAMPLE_POINTS, &interval, created_at);

        Ok(BackfillExecution {
            series,
            backfill,
            itr,
            sample_times,
            config: self.config,
        })
    }

    /// `SeriesBackfill.repoIterator` (the load happens outside the handler's
    /// transactions, on the pool).
    async fn repo_iterator(
        &self,
        backfill: &SeriesBackfill,
    ) -> Result<PersistentRepoIterator, String> {
        let iterator_id = backfill.repo_iterator_id.unwrap_or(0);
        if iterator_id == 0 {
            return Err(format!(
                "repoIterator: invalid repo_iterator_id on backfill_id: {}",
                backfill.id
            ));
        }
        PersistentRepoIterator::load(&self.pool, iterator_id)
            .await
            .map_err(|err| format!("repoIterator: {}", raw_error_text(&err)))
    }

    /// `inProgressHandler.doExecution`.
    async fn do_execution(&self, execution: &mut BackfillExecution) -> Result<bool, String> {
        let deadline = self.clock.now() + self.config.interrupt_after;

        let interrupted = self
            .iteration_loop(
                execution,
                self.config.page_size,
                self.config.repo_concurrency,
                false,
                deadline,
            )
            .await
            .map_err(|err| format!("InProgressHandler.PrimaryLoop: {err}"))?;
        if interrupted {
            return Ok(true);
        }

        let interrupted = self
            .iteration_loop(execution, 1, 1, true, deadline)
            .await
            .map_err(|err| format!("InProgressHandler.RetryLoop: {err}"))?;
        if interrupted {
            return Ok(true);
        }

        if !execution.itr.has_more() && !execution.itr.has_errors() {
            self.finish(execution).await?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// `doExecution.itrLoop`: one page (or one retry when `retry` is set) per
    /// iteration. Returns `true` when the interrupt deadline expired.
    async fn iteration_loop(
        &self,
        execution: &mut BackfillExecution,
        page_size: usize,
        concurrency: usize,
        retry: bool,
        deadline: OffsetDateTime,
    ) -> Result<bool, String> {
        let itr_config = IterationConfig {
            max_failures: MAX_FAILURES,
        };
        loop {
            let next = if retry {
                execution
                    .itr
                    .next_retry_with_finish(itr_config, self.clock.now())
                    .map(|(repo, handle)| (vec![repo], handle))
            } else {
                execution
                    .itr
                    .next_page_with_finish(page_size, self.clock.now())
            };
            let Some((repo_ids, handle)) = next else {
                break;
            };
            if self.clock.now() >= deadline {
                return Ok(true);
            }

            let repo_errors = self.run_page(execution, &repo_ids, concurrency).await;
            let messages: HashMap<i32, String> = repo_errors
                .iter()
                .map(|(repo_id, error)| (*repo_id, error.to_string()))
                .collect();
            let mut terminals = IncompletePointWriter {
                series_id: execution.series.id,
                sample_times: &execution.sample_times,
                errors: &repo_errors,
            };
            execution
                .itr
                .finish_page(
                    &self.pool,
                    &handle,
                    &messages,
                    itr_config,
                    self.clock.now(),
                    &mut terminals,
                )
                .await?;

            if execution.exceeds_error_threshold() {
                self.disable_backfill(execution)
                    .await
                    .map_err(|err| format!("disableBackfill: {err}"))?;
            }
        }
        Ok(false)
    }

    /// Runs one backfill per repository. The Go pool caps concurrency at
    /// `repoConcurrency`; this port joins the page in chunks of that size.
    async fn run_page(
        &self,
        execution: &BackfillExecution,
        repo_ids: &[i32],
        concurrency: usize,
    ) -> HashMap<i32, BackfillError> {
        let mut repo_errors = HashMap::new();
        for chunk in repo_ids.chunks(concurrency.max(1)) {
            let mut set = tokio::task::JoinSet::new();
            for repo_id in chunk {
                let repo_store = Arc::clone(&self.repo_store);
                let backfiller = Arc::clone(&self.backfiller);
                let series = execution.series.clone();
                let sample_times = execution.sample_times.clone();
                let repo_id = *repo_id;
                set.spawn(async move {
                    run_repo(repo_store, backfiller, &series, &sample_times, repo_id).await
                });
            }
            while let Some(joined) = set.join_next().await {
                if let Ok(Some((repo_id, error))) = joined {
                    repo_errors.insert(repo_id, error);
                }
            }
        }
        repo_errors
    }

    /// `inProgressHandler.finish`.
    async fn finish(&self, execution: &mut BackfillExecution) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|err| sql_error(&err))?;
        let result = async {
            execution
                .itr
                .mark_complete_in(&mut tx, self.clock.now())
                .await
                .map_err(|err| format!("iterator.MarkComplete: {}", sql_error(&err)))?;
            // The Go handler calls SetSeriesBackfillComplete on the plain
            // insight store, not on this transaction.
            if let Some(completed_at) = execution.itr.row.completed_at {
                insights_store::set_series_backfill_complete(
                    &self.pool,
                    &execution.series.series_id,
                    completed_at,
                )
                .await?;
            }
            is::set_state_in(&mut tx, execution.backfill.id, "completed")
                .await
                .map_err(|err| format!("backfill.SetCompleted: {}", sql_error(&err)))?;
            Ok::<(), String>(())
        }
        .await;
        match result {
            Ok(()) => tx.commit().await.map_err(|err| sql_error(&err)),
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }

    /// `inProgressHandler.disableBackfill`.
    async fn disable_backfill(&self, execution: &mut BackfillExecution) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|err| sql_error(&err))?;
        let result = async {
            is::set_state_in(&mut tx, execution.backfill.id, "failed")
                .await
                .map_err(|err| format!("SetFailed: {}", sql_error(&err)))?;
            execution
                .itr
                .mark_complete_in(&mut tx, self.clock.now())
                .await
                .map_err(|err| format!("itr.MarkComplete: {}", sql_error(&err)))?;
            for frame in &execution.sample_times {
                insights_store::add_incomplete_datapoint_in(
                    &mut tx,
                    execution.series.id,
                    None,
                    insights_store::REASON_EXCEEDS_ERROR_LIMIT,
                    *frame,
                )
                .await
                .map_err(|err| format!("SetFailed.AddIncompleteDatapoint: {err}"))?;
            }
            Ok::<(), String>(())
        }
        .await;
        match result {
            Ok(()) => tx.commit().await.map_err(|err| sql_error(&err)),
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }

    /// `inProgressHandler.doInterrupt`: requeue the job one polling interval
    /// from the handler clock.
    async fn do_interrupt(&self, job_id: i32) -> Result<(), String> {
        self.worker_store
            .requeue(job_id, self.clock.now() + Duration::from_secs(5))
            .await
    }
}

/// The `OnTerminal` callback: writes one incomplete datapoint per sample time
/// for the terminal repository, inside the finish transaction.
struct IncompletePointWriter<'a> {
    series_id: i32,
    sample_times: &'a [OffsetDateTime],
    errors: &'a HashMap<i32, BackfillError>,
}

#[async_trait]
impl is::TerminalHandler for IncompletePointWriter<'_> {
    async fn on_terminal(
        &mut self,
        conn: &mut sqlx::PgConnection,
        repo_id: i32,
        _error: &str,
    ) -> Result<(), String> {
        let reason = self
            .errors
            .get(&repo_id)
            .map_or(IncompleteReason::Generic, |error| {
                translate_incomplete_reasons(error)
            });
        for frame in self.sample_times {
            insights_store::add_incomplete_datapoint_in(
                conn,
                self.series_id,
                Some(repo_id),
                reason.as_str(),
                *frame,
            )
            .await
            .map_err(|err| format!("AddIncompleteDatapoint: {err}"))?;
        }
        Ok(())
    }
}

/// One repository of a page: `repoStore.Get` then `backfiller.Run`. A
/// soft-deleted repository is skipped (no error, no run).
async fn run_repo(
    repo_store: Arc<dyn InsightRepoStore>,
    backfiller: Arc<dyn Backfiller>,
    series: &InsightSeriesRecord,
    sample_times: &[OffsetDateTime],
    repo_id: i32,
) -> Option<(i32, BackfillError)> {
    let repo = match repo_store.get(repo_id).await {
        Ok(repo) => repo,
        Err(RepoGetError::NotFound) => return None,
        Err(err) => {
            return Some((
                repo_id,
                BackfillError::Message(format!("InProgressHandler.repoStore.Get: {err}")),
            ))
        }
    };
    let request = BackfillRequest {
        series: series.clone(),
        repo,
        sample_times: sample_times.to_vec(),
    };
    match backfiller.run(request).await {
        Ok(()) => None,
        Err(err) => Some((repo_id, err)),
    }
}

/// Go driver errors the baseline surfaces verbatim (the protocol message has
/// no prefix in Go, so strip sqlx's).
fn raw_error_text(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Protocol(message) => message.clone(),
        sqlx::Error::RowNotFound => "sql: no rows in result set".to_owned(),
        other => other.to_string(),
    }
}
