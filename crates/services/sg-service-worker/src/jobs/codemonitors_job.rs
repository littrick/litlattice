#![forbid(unsafe_code)]

//! `codemonitors-job` assembly (`cmd/worker/internal/codemonitors` +
//! `internal/codemonitors/background`), M9.8ip.
//!
//! M9.16c production seam: the trigger searcher is the live frontend search
//! client and the action runner's e-mail sink is `super::code_monitor_email`
//! over `sg_txemail`; `run_worker_codemonitor_diff.sh` pins both the trigger
//! search and the action/e-mail path against the Go oracles.
//!
//! The two periodic routines (trigger enqueuer and trigger-job log deleter)
//! live in [`super::code_monitors`]; the dbworker stores and worker options in
//! [`super::code_monitor_workers`]; the action payloads and HTTP delivery in
//! [`super::code_monitor_actions`]; the e-mail transport in
//! `super::code_monitor_email`. This module adds the two handlers the
//! baseline's worker constructors close over ([`QueryTriggerHandler`],
//! [`ActionRunnerHandler`]), the metrics and resetter wiring, and the
//! six-routine assembly registered as `codemonitors-job`.
//!
//! Wiring reproduced from `background.NewBackgroundJobs`:
//!
//! 1. `code_monitors.trigger_query_enqueuer` (every minute);
//! 2. `code_monitors.trigger_jobs_log_deleter` (every hour);
//! 3. `code_monitors_trigger_jobs_worker` + its resetter;
//! 4. `code_monitors_action_jobs_worker` + its resetter.
//!
//! The database is `JobDeps.frontend` (the Rust equivalent of
//! `workerdb.InitDB`; the pool already exists at startup). Trigger search
//! execution (`codemonitors.Search`) runs through
//! [`sg_service_frontend::codemonitors_search::search`]; the production
//! [`TriggerSearcher`] is [`FrontendTriggerSearcher`], backed by the
//! commit-only search runtime assembled from the live configuration. The
//! trigger job's DB steps, error wrapping, `latestResultTime` and the
//! non-retryable classification are unchanged (M9.8ip + M9.9f).
//!
//! Seams (all injectable so the differential harness runs without a
//! database):
//!
//! * [`TriggerStore`] / [`ActionStore`] — the `database.CodeMonitorStore`
//!   reads and writes the handlers use;
//! * [`TriggerSearcher`] — `codemonitors.Search`;
//! * [`EmailSender`] — `SendEmailForNewSearchResult` (the M9.8cm seam, reused);
//! * [`WebhookSender`] / [`SlackSender`] — `sendWebhookNotification` /
//!   `sendSlackNotification`;
//! * [`CodeMonitorMetrics`] — the counter increments the baseline's
//!   `dbworker.Resetter` applies.
//!
//! Not ported (see the overview, §2.3): `codemonitors.Search` and its
//! actor/feature-flag/header machinery; `MaximumRuntimePerJob`;
//! `workerutil.NewMetrics` and Prometheus registry wiring; observation-log
//! scoping; the empty-`externalURL` behaviour of Go's `url.Parse` (Rust's
//! `url::Url` requires an absolute URL); and the `Transact` wrappers around
//! the action handlers (the reads are already single consistent statements).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use url::Url;

use sg_config::SiteConfiguration;
use sg_db::code_monitors::{
    self, ActionJob, ActionJobMetadata, EmailAction, Monitor, QueryTrigger, Recipient,
    SlackWebhookAction, TriggerJob, WebhookAction,
};
use sg_service_frontend::codemonitors_search::{
    self, TriggerSearchFailure, TriggerSearchStore,
};
use sg_service_frontend::search_stream::convert::CommitMatch as SearchCommitMatch;
use sg_service_frontend::search_stream::TreeRuntimeClients;

use super::code_monitor_action_runner::{commit_matches_from_json, EmailSender};
use super::code_monitor_actions::{
    new_template_data_for_new_search_results, send_slack_notification, send_webhook_notification,
    ActionArgs, CommitMatch, EmailActionInput, UTM_SOURCE_EMAIL,
};
use super::code_monitor_workers::{
    action_worker_options, code_monitors_concurrency, new_action_runner, new_trigger_query_runner,
    trigger_worker_options, ActionJobStore, TriggerJobStore, ACTION_RESETTER_NAME,
    RESETTER_INTERVAL, TRIGGER_RESETTER_NAME,
};
use super::code_monitors::{
    is_enabled, TriggerJobsLogDeleterHandler, TriggerQueryEnqueuerHandler, ENQUEUER_DESCRIPTION,
    ENQUEUER_INTERVAL, ENQUEUER_NAME, LOG_DELETER_DESCRIPTION, LOG_DELETER_INTERVAL,
    LOG_DELETER_NAME,
};
use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbResetter, DbWorkerHandler, ResetterOptions,
    ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::periodic::PeriodicRoutine;
use crate::framework::BackgroundRoutine;
use crate::jobs::{Job, JobDeps};

/// `handleWebhook`'s `UTMSource`.
pub const UTM_SOURCE_WEBHOOK: &str = "code-monitor-webhook";
/// `handleSlackWebhook`'s `UTMSource`.
pub const UTM_SOURCE_SLACK_WEBHOOK: &str = "code-monitor-slack-webhook";

/// `conf.CodeMonitors().PollInterval` default (5 minutes).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `newMetricsForTriggerQueries`'s counter names.
pub const TRIGGER_RESETS_METRIC: &str = "src_codemonitors_query_resets_total";
pub const TRIGGER_RESET_FAILURES_METRIC: &str = "src_codemonitors_query_reset_failures_total";
pub const TRIGGER_ERRORS_METRIC: &str = "src_codemonitors_query_errors_total";
/// `newMetricsForTriggerQueries`'s `workerutil.NewMetrics` root.
pub const TRIGGER_WORKER_METRICS_ROOT: &str = "code_monitors_trigger_queries";

/// `newActionMetrics`'s counter names.
pub const ACTION_RESETS_METRIC: &str = "src_codemonitors_action_resets_total";
pub const ACTION_RESET_FAILURES_METRIC: &str = "src_codemonitors_action_reset_failures_total";
pub const ACTION_ERRORS_METRIC: &str = "src_codemonitors_action_errors_total";
/// `newActionMetrics`'s `workerutil.NewMetrics` root.
pub const ACTION_WORKER_METRICS_ROOT: &str = "code_monitors_actions";

/// Which counter family a [`CodeMonitorMetrics`] belongs to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetricsKind {
    #[default]
    TriggerQueries,
    Actions,
}

impl MetricsKind {
    pub fn resets_metric(self) -> &'static str {
        match self {
            Self::TriggerQueries => TRIGGER_RESETS_METRIC,
            Self::Actions => ACTION_RESETS_METRIC,
        }
    }

    pub fn reset_failures_metric(self) -> &'static str {
        match self {
            Self::TriggerQueries => TRIGGER_RESET_FAILURES_METRIC,
            Self::Actions => ACTION_RESET_FAILURES_METRIC,
        }
    }

    pub fn errors_metric(self) -> &'static str {
        match self {
            Self::TriggerQueries => TRIGGER_ERRORS_METRIC,
            Self::Actions => ACTION_ERRORS_METRIC,
        }
    }

    pub fn worker_metrics_root(self) -> &'static str {
        match self {
            Self::TriggerQueries => TRIGGER_WORKER_METRICS_ROOT,
            Self::Actions => ACTION_WORKER_METRICS_ROOT,
        }
    }
}

/// The counter pair `newMetricsForTriggerQueries` / `newActionMetrics`
/// expose to the two resetters.
///
/// The baseline registers three Prometheus counters per family. The Rust port
/// keeps the same names and increment semantics as atomic counters; it is not
/// registered with `sg-metrics` (see the overview).
#[derive(Debug, Default)]
pub struct CodeMonitorMetrics {
    kind: MetricsKind,
    resets: AtomicU64,
    reset_failures: AtomicU64,
    errors: AtomicU64,
}

/// A point-in-time view of [`CodeMonitorMetrics`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    pub resets: u64,
    pub reset_failures: u64,
    pub errors: u64,
}

impl CodeMonitorMetrics {
    pub fn new(kind: MetricsKind) -> Self {
        Self {
            kind,
            resets: AtomicU64::new(0),
            reset_failures: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    pub fn kind(&self) -> MetricsKind {
        self.kind
    }

    /// `RecordResets.Add(float64(len(reset)))`.
    pub fn record_resets(&self, count: usize) {
        self.resets.fetch_add(count as u64, Ordering::SeqCst);
    }

    /// `RecordResetFailures.Add(float64(len(failed)))`.
    pub fn record_reset_failures(&self, count: usize) {
        self.reset_failures
            .fetch_add(count as u64, Ordering::SeqCst);
    }

    /// `Errors.Inc()`.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            resets: self.resets.load(Ordering::SeqCst),
            reset_failures: self.reset_failures.load(Ordering::SeqCst),
            errors: self.errors.load(Ordering::SeqCst),
        }
    }
}

/// `dbworker.Resetter`'s metrics wrapper: applies the baseline's counter
/// increments around a [`DbResetter`].
pub struct MetricsResetter {
    inner: Arc<dyn DbResetter>,
    metrics: Arc<CodeMonitorMetrics>,
}

impl MetricsResetter {
    pub fn new(inner: Arc<dyn DbResetter>, metrics: Arc<CodeMonitorMetrics>) -> Self {
        Self { inner, metrics }
    }

    pub fn metrics(&self) -> &Arc<CodeMonitorMetrics> {
        &self.metrics
    }
}

#[async_trait]
impl DbResetter for MetricsResetter {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let outcome = self.inner.reset_stalled().await;
        match outcome {
            Ok((reset, failed)) => {
                self.metrics.record_resets(reset.len());
                self.metrics.record_reset_failures(failed.len());
                Ok((reset, failed))
            }
            Err(err) => {
                self.metrics.record_error();
                Err(err)
            }
        }
    }
}

/// The trigger-job store operations `queryRunner.Handle` uses
/// (`database.CodeMonitorStore`).
#[async_trait]
pub trait TriggerStore: Send + Sync + 'static {
    async fn get_query_trigger_for_job(&self, trigger_job: i32) -> Result<QueryTrigger, String>;

    async fn get_monitor(&self, monitor_id: i64) -> Result<Monitor, String>;

    async fn set_query_trigger_next_run(
        &self,
        trigger_query_id: i64,
        next: OffsetDateTime,
        latest_result: OffsetDateTime,
    ) -> Result<(), String>;

    async fn update_trigger_job_with_results(
        &self,
        trigger_job_id: i32,
        query_string: &str,
        results: &serde_json::Value,
    ) -> Result<(), String>;

    async fn enqueue_action_jobs_for_monitor(
        &self,
        monitor_id: i64,
        trigger_job_id: i32,
    ) -> Result<(), String>;
}

/// `database.CodeMonitorStore` trigger reads/writes over the frontend pool.
#[derive(Clone)]
pub struct PgTriggerStore {
    pool: PgPool,
}

impl PgTriggerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TriggerStore for PgTriggerStore {
    async fn get_query_trigger_for_job(&self, trigger_job: i32) -> Result<QueryTrigger, String> {
        code_monitors::get_query_trigger_for_job(&self.pool, trigger_job)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_monitor(&self, monitor_id: i64) -> Result<Monitor, String> {
        code_monitors::get_monitor(&self.pool, monitor_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn set_query_trigger_next_run(
        &self,
        trigger_query_id: i64,
        next: OffsetDateTime,
        latest_result: OffsetDateTime,
    ) -> Result<(), String> {
        code_monitors::set_query_trigger_next_run(&self.pool, trigger_query_id, next, latest_result)
            .await
            .map_err(|err| err.to_string())
    }

    async fn update_trigger_job_with_results(
        &self,
        trigger_job_id: i32,
        query_string: &str,
        results: &serde_json::Value,
    ) -> Result<(), String> {
        code_monitors::update_trigger_job_with_results(
            &self.pool,
            trigger_job_id,
            query_string,
            results,
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn enqueue_action_jobs_for_monitor(
        &self,
        monitor_id: i64,
        trigger_job_id: i32,
    ) -> Result<(), String> {
        code_monitors::enqueue_action_jobs_for_monitor(&self.pool, monitor_id, trigger_job_id)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

/// The action-job reads `actionRunner` uses (`database.CodeMonitorStore`).
#[async_trait]
pub trait ActionStore: Send + Sync + 'static {
    async fn get_action_job_metadata(&self, job_id: i32) -> Result<ActionJobMetadata, String>;

    async fn get_email_action(&self, email_id: i64) -> Result<EmailAction, String>;

    async fn get_webhook_action(&self, webhook_id: i64) -> Result<WebhookAction, String>;

    async fn get_slack_webhook_action(
        &self,
        slack_webhook_id: i64,
    ) -> Result<SlackWebhookAction, String>;

    async fn list_recipients(&self, email_id: i64) -> Result<Vec<Recipient>, String>;
}

/// `database.CodeMonitorStore` action reads over the frontend pool.
#[derive(Clone)]
pub struct PgActionStore {
    pool: PgPool,
}

impl PgActionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ActionStore for PgActionStore {
    async fn get_action_job_metadata(&self, job_id: i32) -> Result<ActionJobMetadata, String> {
        code_monitors::get_action_job_metadata(&self.pool, job_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_email_action(&self, email_id: i64) -> Result<EmailAction, String> {
        code_monitors::get_email_action(&self.pool, email_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_webhook_action(&self, webhook_id: i64) -> Result<WebhookAction, String> {
        code_monitors::get_webhook_action(&self.pool, webhook_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_slack_webhook_action(
        &self,
        slack_webhook_id: i64,
    ) -> Result<SlackWebhookAction, String> {
        code_monitors::get_slack_webhook_action(&self.pool, slack_webhook_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_recipients(&self, email_id: i64) -> Result<Vec<Recipient>, String> {
        code_monitors::list_recipients(&self.pool, email_id)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `codemonitors.Search`'s result: the stored `[]*result.CommitMatch` JSON,
/// its length, and the first result's committer date (for
/// `latestResultTime`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TriggerSearchOutcome {
    /// `json.Marshal([]*result.CommitMatch)`; written to
    /// `cm_trigger_jobs.search_results`.
    pub stored_results: serde_json::Value,
    /// `len(results)`.
    pub count: usize,
    /// `results[0].Commit.Committer.Date` when the committer is set.
    pub first_committer_date: Option<OffsetDateTime>,
}

impl TriggerSearchOutcome {
    pub fn new(
        stored_results: serde_json::Value,
        count: usize,
        first_committer_date: Option<OffsetDateTime>,
    ) -> Self {
        Self {
            stored_results,
            count,
            first_committer_date,
        }
    }
}

/// A `codemonitors.Search` failure plus `errcode.MakeNonRetryable`'s
/// classification. `Display` is exactly the Go error text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerSearchError {
    pub message: String,
    pub non_retryable: bool,
}

impl From<String> for TriggerSearchError {
    fn from(message: String) -> Self {
        Self {
            message,
            non_retryable: false,
        }
    }
}

impl std::fmt::Display for TriggerSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TriggerSearchError {}

impl From<TriggerSearchFailure> for TriggerSearchError {
    fn from(failure: TriggerSearchFailure) -> Self {
        Self {
            message: failure.message,
            non_retryable: failure.non_retryable,
        }
    }
}

/// `codemonitors.Search` (`internal/codemonitors/search.go`).
#[async_trait]
pub trait TriggerSearcher: Send + Sync + 'static {
    async fn search(
        &self,
        query: &str,
        monitor_id: i64,
        trigger_job_id: i32,
    ) -> Result<TriggerSearchOutcome, TriggerSearchError>;
}

/// Kept for tests that exercise the explicit not-ported marker; production
/// assembly uses [`FrontendTriggerSearcher`].
pub struct NotPortedTriggerSearcher;

#[async_trait]
impl TriggerSearcher for NotPortedTriggerSearcher {
    async fn search(
        &self,
        _query: &str,
        _monitor_id: i64,
        _trigger_job_id: i32,
    ) -> Result<TriggerSearchOutcome, TriggerSearchError> {
        Err("codemonitors.Search is not ported to the Rust worker (M9.8ip)".to_owned().into())
    }
}

/// Production [`TriggerSearcher`]: runs [`codemonitors_search::search`] with
/// the runtime assembled from the live configuration.
pub struct FrontendTriggerSearcher {
    runtime: TreeRuntimeClients,
    store: Arc<dyn TriggerSearchStore + Send + Sync>,
}

impl FrontendTriggerSearcher {
    pub fn new(runtime: TreeRuntimeClients, store: Arc<dyn TriggerSearchStore + Send + Sync>) -> Self {
        Self { runtime, store }
    }
}

#[async_trait]
impl TriggerSearcher for FrontendTriggerSearcher {
    async fn search(
        &self,
        query: &str,
        monitor_id: i64,
        trigger_job_id: i32,
    ) -> Result<TriggerSearchOutcome, TriggerSearchError> {
        let results: Vec<SearchCommitMatch> = codemonitors_search::search(
            &self.runtime,
            Arc::clone(&self.store),
            query,
            monitor_id,
            trigger_job_id,
        )?;
        let first_committer_date = results
            .first()
            .map(|result| result.commit.committer.date);
        let stored_results = codemonitors_search::commit_matches_json(&results);
        Ok(TriggerSearchOutcome::new(
            stored_results,
            results.len(),
            first_committer_date,
        ))
    }
}

/// Production [`TriggerSearchStore`]: the three `codeMonitorStore` calls the
/// hook makes, bridging the sync hook to the async `sg-db` adapter.
pub struct PgTriggerSearchStore {
    pool: PgPool,
}

impl PgTriggerSearchStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl TriggerSearchStore for PgTriggerSearchStore {
    fn get_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
    ) -> Result<Option<Vec<String>>, sg_service_frontend::search_job::JobError> {
        block_on(code_monitors::get_last_searched(
            &self.pool,
            monitor_id,
            repo_id,
        ))
        .map_err(|error| sg_service_frontend::search_job::JobError(error.to_string()))
    }

    fn upsert_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
        commit_oids: &[String],
    ) -> Result<(), sg_service_frontend::search_job::JobError> {
        block_on(code_monitors::upsert_last_searched(
            &self.pool,
            monitor_id,
            repo_id,
            commit_oids,
        ))
        .map_err(|error| sg_service_frontend::search_job::JobError(error.to_string()))
    }

    fn update_trigger_job_with_logs(
        &self,
        trigger_id: i32,
        message: &str,
    ) -> Result<(), sg_service_frontend::search_job::JobError> {
        let entry = serde_json::json!({ "message": message });
        block_on(code_monitors::update_trigger_job_with_logs(
            &self.pool,
            trigger_id,
            &entry,
        ))
        .map_err(|error| sg_service_frontend::search_job::JobError(error.to_string()))
    }
}

/// Bridge a future to the synchronous `TriggerSearchStore` trait. The worker
/// runs on the multi-thread runtime (`block_in_place`); tests may use a
/// current-thread runtime, hence the scoped-thread fallback.
fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("building the trigger store blocking runtime cannot fail")
                        .block_on(future)
                })
                .join()
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
        }),
    }
}

/// `sendWebhookNotification`'s doer seam.
#[async_trait]
pub trait WebhookSender: Send + Sync + 'static {
    async fn send(&self, url: &str, args: &ActionArgs<'_>) -> Result<(), String>;
}

/// `sendSlackNotification`'s doer seam.
#[async_trait]
pub trait SlackSender: Send + Sync + 'static {
    async fn send(&self, url: &str, args: &ActionArgs<'_>) -> Result<(), String>;
}

/// Production webhook sender: `httpcli.ExternalDoer` is represented by the
/// caller-supplied `reqwest::Client`.
pub struct HttpWebhookSender {
    client: reqwest::Client,
}

impl HttpWebhookSender {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl WebhookSender for HttpWebhookSender {
    async fn send(&self, url: &str, args: &ActionArgs<'_>) -> Result<(), String> {
        send_webhook_notification(&self.client, url, args).await
    }
}

/// Production Slack sender.
pub struct HttpSlackSender {
    client: reqwest::Client,
}

impl HttpSlackSender {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SlackSender for HttpSlackSender {
    async fn send(&self, url: &str, args: &ActionArgs<'_>) -> Result<(), String> {
        send_slack_notification(&self.client, url, args).await
    }
}

/// `latestResultTime`.
pub fn latest_result_time(
    previous_last_result: Option<OffsetDateTime>,
    outcome: Option<&TriggerSearchOutcome>,
    search_failed: bool,
    now: OffsetDateTime,
) -> OffsetDateTime {
    let has_results = !search_failed && outcome.map(|value| value.count).unwrap_or(0) > 0;
    if !has_results {
        // Error performing the search, or there were no results. Assume the
        // previous info's result time.
        return previous_last_result.unwrap_or(now);
    }
    outcome
        .and_then(|value| value.first_committer_date)
        .unwrap_or(now)
}

/// A `run_query_trigger` failure with the searcher's non-retryable
/// classification (`errcode.IsNonRetryable`). `Display` is the exact message
/// stored on the record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerRunError {
    pub message: String,
    pub non_retryable: bool,
}

impl From<String> for TriggerRunError {
    fn from(message: String) -> Self {
        Self {
            message,
            non_retryable: false,
        }
    }
}

impl std::fmt::Display for TriggerRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TriggerRunError {}

/// `(*queryRunner).Handle`.
///
/// `now` stands in for the baseline's `time.Now()`/store clock, so the
/// differential harness can make `next_run` deterministic.
pub async fn run_query_trigger(
    store: &dyn TriggerStore,
    searcher: &dyn TriggerSearcher,
    trigger_job_id: i32,
    poll_interval: Duration,
    now: OffsetDateTime,
) -> Result<(), String> {
    run_query_trigger_classified(store, searcher, trigger_job_id, poll_interval, now)
        .await
        .map_err(|error| error.message)
}

/// [`run_query_trigger`] retaining the searcher's non-retryable bit so the
/// dbworker handler can mark the record failed instead of errored.
pub async fn run_query_trigger_classified(
    store: &dyn TriggerStore,
    searcher: &dyn TriggerSearcher,
    trigger_job_id: i32,
    poll_interval: Duration,
    now: OffsetDateTime,
) -> Result<(), TriggerRunError> {
    let query_trigger = store
        .get_query_trigger_for_job(trigger_job_id)
        .await
        .map_err(TriggerRunError::from)?;
    let monitor = store
        .get_monitor(query_trigger.monitor)
        .await
        .map_err(TriggerRunError::from)?;

    let searched = searcher
        .search(&query_trigger.query_string, monitor.id, trigger_job_id)
        .await;

    let new_latest_result = latest_result_time(
        query_trigger.latest_result,
        searched.as_ref().ok(),
        searched.is_err(),
        now,
    );
    store
        .set_query_trigger_next_run(query_trigger.id, now + poll_interval, new_latest_result)
        .await
        .map_err(TriggerRunError::from)?;

    let outcome = match searched {
        Ok(outcome) => outcome,
        Err(err) => {
            return Err(TriggerRunError {
                message: format!("execute search: {err}"),
                non_retryable: err.non_retryable,
            })
        }
    };

    store
        .update_trigger_job_with_results(
            trigger_job_id,
            &query_trigger.query_string,
            &outcome.stored_results,
        )
        .await
        .map_err(|err| TriggerRunError::from(format!("UpdateTriggerJobWithResults: {err}")))?;

    if outcome.count > 0 {
        store
            .enqueue_action_jobs_for_monitor(monitor.id, trigger_job_id)
            .await
            .map_err(|err| {
                TriggerRunError::from(format!("store.EnqueueActionJobsForQuery: {err}"))
            })?;
    }

    Ok(())
}

/// `(*queryRunner)`.
pub struct QueryTriggerHandler {
    store: Arc<dyn TriggerStore>,
    searcher: Arc<dyn TriggerSearcher>,
    poll_interval: Duration,
    /// Failures whose searcher error was `errcode.MakeNonRetryable`. The
    /// dbworker interface only passes the failure message to
    /// `non_retryable`, so the handler records the exact message produced by
    /// `handle` and consumes it there.
    non_retryable_messages: Mutex<HashSet<String>>,
}

impl QueryTriggerHandler {
    pub fn new(
        store: Arc<dyn TriggerStore>,
        searcher: Arc<dyn TriggerSearcher>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            store,
            searcher,
            poll_interval,
            non_retryable_messages: Mutex::new(HashSet::new()),
        }
    }
}

#[async_trait]
impl DbWorkerHandler<TriggerJobStore> for QueryTriggerHandler {
    async fn handle(&self, _ctx: CancellationToken, record: &TriggerJob) -> Result<(), String> {
        let result = run_query_trigger_classified(
            self.store.as_ref(),
            self.searcher.as_ref(),
            record.id,
            self.poll_interval,
            OffsetDateTime::now_utc(),
        )
        .await;
        if let Err(error) = &result {
            if error.non_retryable {
                self.non_retryable_messages
                    .lock()
                    .expect("non-retryable message set poisoned")
                    .insert(error.message.clone());
            }
        }
        result.map_err(|error| error.message)
    }

    /// `errcode.IsNonRetryable`: the Plan/NewPlanJob/addCodeMonitorHook errors
    /// of `codemonitors.Search` are marked failed instead of errored.
    fn non_retryable(&self, failure_message: &str) -> bool {
        self.non_retryable_messages
            .lock()
            .expect("non-retryable message set poisoned")
            .remove(failure_message)
    }
}

/// The fields of `database.ActionJobMetadata` the runner uses.
struct Metadata {
    description: String,
    query: String,
    monitor_id: i64,
    owner_name: String,
}

async fn metadata(
    store: &dyn ActionStore,
    job_id: i32,
) -> Result<(Metadata, Vec<CommitMatch>), String> {
    let metadata = store
        .get_action_job_metadata(job_id)
        .await
        .map_err(|err| format!("GetActionJobMetadata: {err}"))?;
    let results = commit_matches_from_json(&metadata.results)
        .map_err(|err| format!("GetActionJobMetadata: {err}"))?;
    Ok((
        Metadata {
            description: metadata.description,
            query: metadata.query.unwrap_or_default(),
            monitor_id: metadata.monitor_id,
            owner_name: metadata.owner_name,
        },
        results,
    ))
}

/// `(*actionRunner).Handle`. The dispatch order and error wrapping match the
/// baseline switch.
pub async fn handle_action(
    job: &ActionJob,
    store: &dyn ActionStore,
    external_url: &str,
    email_sender: &dyn EmailSender,
    webhook_sender: &dyn WebhookSender,
    slack_sender: &dyn SlackSender,
) -> Result<(), String> {
    if let Some(email) = job.email {
        return handle_email(job.id, email, store, external_url, email_sender)
            .await
            .map_err(|err| format!("Email: {err}"));
    }
    if let Some(webhook) = job.webhook {
        return handle_webhook(job.id, webhook, store, external_url, webhook_sender)
            .await
            .map_err(|err| format!("Webhook: {err}"));
    }
    if let Some(slack_webhook) = job.slack_webhook {
        return handle_slack_webhook(job.id, slack_webhook, store, external_url, slack_sender)
            .await
            .map_err(|err| format!("SlackWebhook: {err}"));
    }
    Err("job must be one of type email, webhook, or slack webhook".to_owned())
}

/// `(*actionRunner).handleEmail`.
pub async fn handle_email(
    job_id: i32,
    email_id: i64,
    store: &dyn ActionStore,
    external_url: &str,
    email_sender: &dyn EmailSender,
) -> Result<(), String> {
    let (metadata, results) = metadata(store, job_id).await?;

    let email = store
        .get_email_action(email_id)
        .await
        .map_err(|err| format!("GetEmailAction: {err}"))?;

    let recipients = store
        .list_recipients(email_id)
        .await
        .map_err(|err| format!("ListRecipients: {err}"))?;

    let external_url = Url::parse(external_url).map_err(|err| err.to_string())?;

    let args = ActionArgs {
        monitor_description: &metadata.description,
        external_url: &external_url,
        monitor_id: metadata.monitor_id,
        utm_source: UTM_SOURCE_EMAIL,
        monitor_owner_name: &metadata.owner_name,
        query: &metadata.query,
        results: &results,
        include_results: email.include_results,
    };
    let data = new_template_data_for_new_search_results(
        &args,
        &EmailActionInput {
            monitor: email.monitor,
            priority: email.priority,
        },
    );

    for recipient in recipients {
        if recipient.namespace_org_id.is_some() {
            // TODO (baseline): send emails to org members.
            continue;
        }
        let Some(user_id) = recipient.namespace_user_id else {
            return Err("nil recipient".to_owned());
        };
        email_sender.send(user_id, &data).await?;
    }
    Ok(())
}

/// `(*actionRunner).handleWebhook`.
pub async fn handle_webhook(
    job_id: i32,
    webhook_id: i64,
    store: &dyn ActionStore,
    external_url: &str,
    webhook_sender: &dyn WebhookSender,
) -> Result<(), String> {
    let (metadata, results) = metadata(store, job_id).await?;

    let webhook = store
        .get_webhook_action(webhook_id)
        .await
        .map_err(|err| format!("GetWebhookAction: {err}"))?;

    let external_url = Url::parse(external_url).map_err(|err| err.to_string())?;

    let args = ActionArgs {
        monitor_description: &metadata.description,
        external_url: &external_url,
        monitor_id: webhook.monitor,
        utm_source: UTM_SOURCE_WEBHOOK,
        monitor_owner_name: &metadata.owner_name,
        query: &metadata.query,
        results: &results,
        include_results: webhook.include_results,
    };
    webhook_sender.send(&webhook.url, &args).await
}

/// `(*actionRunner).handleSlackWebhook`.
pub async fn handle_slack_webhook(
    job_id: i32,
    slack_webhook_id: i64,
    store: &dyn ActionStore,
    external_url: &str,
    slack_sender: &dyn SlackSender,
) -> Result<(), String> {
    let (metadata, results) = metadata(store, job_id).await?;

    let webhook = store
        .get_slack_webhook_action(slack_webhook_id)
        .await
        .map_err(|err| format!("GetSlackWebhookAction: {err}"))?;

    let external_url = Url::parse(external_url).map_err(|err| err.to_string())?;

    let args = ActionArgs {
        monitor_description: &metadata.description,
        external_url: &external_url,
        monitor_id: webhook.monitor,
        utm_source: UTM_SOURCE_SLACK_WEBHOOK,
        monitor_owner_name: &metadata.owner_name,
        query: &metadata.query,
        results: &results,
        include_results: webhook.include_results,
    };
    slack_sender.send(&webhook.url, &args).await
}

/// `(*actionRunner)`.
pub struct ActionRunnerHandler {
    store: Arc<dyn ActionStore>,
    external_url: String,
    email_sender: Arc<dyn EmailSender>,
    webhook_sender: Arc<dyn WebhookSender>,
    slack_sender: Arc<dyn SlackSender>,
}

impl ActionRunnerHandler {
    pub fn new(
        store: Arc<dyn ActionStore>,
        external_url: String,
        email_sender: Arc<dyn EmailSender>,
        webhook_sender: Arc<dyn WebhookSender>,
        slack_sender: Arc<dyn SlackSender>,
    ) -> Self {
        Self {
            store,
            external_url,
            email_sender,
            webhook_sender,
            slack_sender,
        }
    }
}

#[async_trait]
impl DbWorkerHandler<ActionJobStore> for ActionRunnerHandler {
    async fn handle(&self, _ctx: CancellationToken, record: &ActionJob) -> Result<(), String> {
        handle_action(
            record,
            self.store.as_ref(),
            &self.external_url,
            self.email_sender.as_ref(),
            self.webhook_sender.as_ref(),
            self.slack_sender.as_ref(),
        )
        .await
    }
}

/// `conf.CodeMonitors().PollInterval`: `siteConfig.codeMonitors.pollInterval`
/// when non-empty (an unparsable value yields zero, exactly like the ignored
/// `time.ParseDuration` error), else 5 minutes.
pub fn code_monitors_poll_interval(site: &SiteConfiguration) -> Duration {
    let configured = site
        .raw()
        .get("codeMonitors")
        .and_then(|value| value.get("pollInterval"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if configured.is_empty() {
        DEFAULT_POLL_INTERVAL
    } else {
        sg_core::parse_go_duration(configured).unwrap_or(Duration::ZERO)
    }
}

/// `conf.Get().ExternalURL` as the action handlers read it.
pub fn external_url(site: &SiteConfiguration) -> String {
    site.raw()
        .get("externalURL")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The routines [`assemble_code_monitor_job`] produces, with the metadata the
/// recorder (and the differential harness) needs.
pub struct CodeMonitorJobRoutines {
    pub routines: Vec<BackgroundRoutine>,
    pub worker_options: Vec<WorkerOptions>,
    pub resetter_options: Vec<ResetterOptions>,
}

/// `background.NewBackgroundJobs`: the six routines in the baseline's order.
pub fn assemble_code_monitor_job(
    pool: PgPool,
    site: &SiteConfiguration,
    live_config: Arc<sg_config::LiveConfig>,
    worker_hostname: String,
) -> CodeMonitorJobRoutines {
    assemble_code_monitor_job_with_keyring(pool, site, live_config, worker_hostname, None)
}

/// [`assemble_code_monitor_job`] with the worker keyring's
/// `UserExternalAccountKey`. Go's `codemonitors.Search` builds
/// `client.New(logger, db, gitserver.NewClient(...))` over the worker
/// `database.DB`; the owners seam behind the commit job tree needs the same
/// key to decrypt `user_external_accounts.account_data` (M9.40d).
pub fn assemble_code_monitor_job_with_keyring(
    pool: PgPool,
    site: &SiteConfiguration,
    live_config: Arc<sg_config::LiveConfig>,
    worker_hostname: String,
    user_external_account_key: Option<Arc<dyn sg_encryption::Key>>,
) -> CodeMonitorJobRoutines {
    let concurrency = code_monitors_concurrency(site);
    let poll_interval = code_monitors_poll_interval(site);
    let external_url = external_url(site);

    let trigger_metrics = Arc::new(CodeMonitorMetrics::new(MetricsKind::TriggerQueries));
    let action_metrics = Arc::new(CodeMonitorMetrics::new(MetricsKind::Actions));

    // 1. `newTriggerQueryEnqueuer`.
    let enqueuer = PeriodicRoutine::new(Arc::new(TriggerQueryEnqueuerHandler::new(pool.clone())))
        .with_name(ENQUEUER_NAME)
        .with_description(ENQUEUER_DESCRIPTION)
        .with_interval(ENQUEUER_INTERVAL);

    // 2. `newTriggerJobsLogDeleter`.
    let deleter = PeriodicRoutine::new(Arc::new(TriggerJobsLogDeleterHandler::new(pool.clone())))
        .with_name(LOG_DELETER_NAME)
        .with_description(LOG_DELETER_DESCRIPTION)
        .with_interval(LOG_DELETER_INTERVAL);

    // 3. `newTriggerQueryRunner`.
    let trigger_store = Arc::new(TriggerJobStore::new(pool.clone()));
    let config = live_config.current();
    let search_runtime =
        sg_service_frontend::search_runtime::assemble_commit_search_with_encryption_key(
            &pool,
            &config.service_connections,
            site,
            user_external_account_key,
        );
    let searcher = Arc::new(FrontendTriggerSearcher::new(
        search_runtime,
        Arc::new(PgTriggerSearchStore::new(pool.clone())),
    ));
    let trigger_handler = Arc::new(QueryTriggerHandler::new(
        Arc::new(PgTriggerStore::new(pool.clone())),
        searcher,
        poll_interval,
    ));
    let trigger_worker: WorkerRoutine<TriggerJobStore, QueryTriggerHandler> =
        new_trigger_query_runner(
            Arc::clone(&trigger_store),
            trigger_handler,
            concurrency,
            worker_hostname.clone(),
        );

    // 4. `newTriggerQueryResetter`.
    let trigger_resetter = ResetterRoutine::new(
        Arc::new(MetricsResetter::new(
            Arc::clone(&trigger_store) as Arc<dyn DbResetter>,
            Arc::clone(&trigger_metrics),
        )),
        ResetterOptions {
            name: TRIGGER_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    );

    // 5. `newActionRunner`.
    let action_store = Arc::new(ActionJobStore::new(pool.clone()));
    let client = reqwest::Client::new();
    let action_handler = Arc::new(ActionRunnerHandler::new(
        Arc::new(PgActionStore::new(pool.clone())),
        external_url,
        Arc::new(super::code_monitor_email::TxEmailSender::new(
            pool,
            live_config,
        )),
        Arc::new(HttpWebhookSender::new(client.clone())),
        Arc::new(HttpSlackSender::new(client)),
    ));
    let action_worker: WorkerRoutine<ActionJobStore, ActionRunnerHandler> = new_action_runner(
        Arc::clone(&action_store),
        action_handler,
        worker_hostname.clone(),
    );

    // 6. `newActionJobResetter`.
    let action_resetter = ResetterRoutine::new(
        Arc::new(MetricsResetter::new(
            Arc::clone(&action_store) as Arc<dyn DbResetter>,
            action_metrics,
        )),
        ResetterOptions {
            name: ACTION_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    );

    let worker_options = vec![
        trigger_worker_options(concurrency, worker_hostname.clone()),
        action_worker_options(worker_hostname),
    ];
    let resetter_options = vec![
        ResetterOptions {
            name: TRIGGER_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
        ResetterOptions {
            name: ACTION_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    ];

    let routines = vec![
        enqueuer.into(),
        deleter.into(),
        BackgroundRoutine::DbBacked(Arc::new(trigger_worker) as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(Arc::new(trigger_resetter) as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(Arc::new(action_worker) as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(Arc::new(action_resetter) as Arc<dyn DbBackedRoutine>),
    ];

    CodeMonitorJobRoutines {
        routines,
        worker_options,
        resetter_options,
    }
}

/// `cmd/worker/internal/codemonitors.codeMonitorJob`.
pub struct CodeMonitorJob;

impl CodeMonitorJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CodeMonitorJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for CodeMonitorJob {
    fn description(&self) -> String {
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !is_enabled() {
            return Ok(Vec::new());
        }

        let site = deps.live_config.current().site.clone();
        Ok(assemble_code_monitor_job_with_keyring(
            deps.frontend.clone(),
            &site,
            Arc::clone(&deps.live_config),
            default_hostname(),
            deps.keyring.user_external_account_key(),
        )
        .routines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn metrics_follow_the_baseline_increments() {
        let metrics = CodeMonitorMetrics::new(MetricsKind::TriggerQueries);
        metrics.record_resets(2);
        metrics.record_reset_failures(1);
        metrics.record_error();
        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                resets: 2,
                reset_failures: 1,
                errors: 1,
            }
        );
        assert_eq!(metrics.kind().resets_metric(), TRIGGER_RESETS_METRIC);
        assert_eq!(
            metrics.kind().reset_failures_metric(),
            TRIGGER_RESET_FAILURES_METRIC
        );
        assert_eq!(metrics.kind().errors_metric(), TRIGGER_ERRORS_METRIC);
        assert_eq!(
            MetricsKind::Actions.worker_metrics_root(),
            ACTION_WORKER_METRICS_ROOT
        );
    }

    type ResetterOutcome = Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String>;

    struct StaticResetter {
        outcome: Mutex<Option<ResetterOutcome>>,
    }

    #[async_trait]
    impl DbResetter for StaticResetter {
        async fn reset_stalled(
            &self,
        ) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
            self.outcome
                .lock()
                .expect("outcome")
                .take()
                .expect("one call")
        }
    }

    #[tokio::test]
    async fn metrics_resetter_accumulates_like_the_go_resetter() {
        let metrics = Arc::new(CodeMonitorMetrics::new(MetricsKind::Actions));
        let resetter = MetricsResetter::new(
            Arc::new(StaticResetter {
                outcome: Mutex::new(Some(Ok((
                    vec![(1, Duration::from_secs(1)), (2, Duration::from_secs(2))],
                    vec![(3, Duration::from_secs(3))],
                )))),
            }),
            Arc::clone(&metrics),
        );
        resetter.reset_stalled().await.expect("reset");
        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                resets: 2,
                reset_failures: 1,
                errors: 0,
            }
        );

        let failing = MetricsResetter::new(
            Arc::new(StaticResetter {
                outcome: Mutex::new(Some(Err("boom".to_owned()))),
            }),
            Arc::clone(&metrics),
        );
        assert_eq!(failing.reset_stalled().await, Err("boom".to_owned()));
        assert_eq!(metrics.snapshot().errors, 1);
    }

    #[test]
    fn latest_result_time_matches_baseline_branches() {
        let previous = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(200).unwrap();
        let committer = OffsetDateTime::from_unix_timestamp(150).unwrap();

        // Search error: previous wins (or now when there is none).
        assert_eq!(
            latest_result_time(Some(previous), None, true, now),
            previous
        );
        assert_eq!(latest_result_time(None, None, true, now), now);

        // No results: previous wins even on success.
        let empty = TriggerSearchOutcome::new(serde_json::json!([]), 0, None);
        assert_eq!(
            latest_result_time(Some(previous), Some(&empty), false, now),
            previous
        );

        // Results: committer date when set, otherwise now.
        let with_committer = TriggerSearchOutcome::new(serde_json::json!([{}]), 1, Some(committer));
        assert_eq!(
            latest_result_time(Some(previous), Some(&with_committer), false, now),
            committer
        );
        let without_committer = TriggerSearchOutcome::new(serde_json::json!([{}]), 1, None);
        assert_eq!(
            latest_result_time(Some(previous), Some(&without_committer), false, now),
            now
        );
    }

    #[derive(Default)]
    struct FakeTriggerStore {
        calls: Mutex<Vec<String>>,
        query_trigger: Mutex<Option<QueryTrigger>>,
        search_error_at_set: Mutex<Option<OffsetDateTime>>,
        latest_at_set: Mutex<Option<OffsetDateTime>>,
        update_error: Mutex<Option<String>>,
    }

    #[async_trait]
    impl TriggerStore for FakeTriggerStore {
        async fn get_query_trigger_for_job(
            &self,
            _trigger_job: i32,
        ) -> Result<QueryTrigger, String> {
            self.calls
                .lock()
                .unwrap()
                .push("get_query_trigger".to_owned());
            Ok(self.query_trigger.lock().unwrap().clone().expect("query"))
        }

        async fn get_monitor(&self, _monitor_id: i64) -> Result<Monitor, String> {
            self.calls.lock().unwrap().push("get_monitor".to_owned());
            Ok(Monitor { id: 42 })
        }

        async fn set_query_trigger_next_run(
            &self,
            _trigger_query_id: i64,
            next: OffsetDateTime,
            latest_result: OffsetDateTime,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push("set_next_run".to_owned());
            *self.search_error_at_set.lock().unwrap() = Some(next);
            *self.latest_at_set.lock().unwrap() = Some(latest_result);
            Ok(())
        }

        async fn update_trigger_job_with_results(
            &self,
            _trigger_job_id: i32,
            _query_string: &str,
            _results: &serde_json::Value,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push("update_results".to_owned());
            match self.update_error.lock().unwrap().clone() {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }

        async fn enqueue_action_jobs_for_monitor(
            &self,
            _monitor_id: i64,
            _trigger_job_id: i32,
        ) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push("enqueue_actions".to_owned());
            Ok(())
        }
    }

    struct FakeSearcher {
        outcome: Result<TriggerSearchOutcome, TriggerSearchError>,
    }

    #[async_trait]
    impl TriggerSearcher for FakeSearcher {
        async fn search(
            &self,
            _query: &str,
            _monitor_id: i64,
            _trigger_job_id: i32,
        ) -> Result<TriggerSearchOutcome, TriggerSearchError> {
            self.outcome.clone()
        }
    }

    fn query_trigger(latest: Option<OffsetDateTime>) -> QueryTrigger {
        QueryTrigger {
            id: 7,
            monitor: 42,
            query_string: "type:commit after:2020-01-01".to_owned(),
            latest_result: latest,
        }
    }

    #[tokio::test]
    async fn query_trigger_search_error_still_sets_the_next_run() {
        let store = FakeTriggerStore {
            query_trigger: Mutex::new(Some(query_trigger(None))),
            ..Default::default()
        };
        let now = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let err = run_query_trigger(
            &store,
            &FakeSearcher {
                outcome: Err("boom".to_owned().into()),
            },
            1,
            Duration::from_secs(300),
            now,
        )
        .await
        .expect_err("search error");
        assert_eq!(err, "execute search: boom");
        assert_eq!(
            store.calls.lock().unwrap().as_slice(),
            &["get_query_trigger", "get_monitor", "set_next_run"]
        );
        assert_eq!(
            *store.search_error_at_set.lock().unwrap(),
            Some(now + Duration::from_secs(300))
        );
        assert_eq!(*store.latest_at_set.lock().unwrap(), Some(now));
    }

    #[tokio::test]
    async fn query_trigger_results_are_stored_and_actions_enqueued() {
        let previous = OffsetDateTime::from_unix_timestamp(500).unwrap();
        let store = FakeTriggerStore {
            query_trigger: Mutex::new(Some(query_trigger(Some(previous)))),
            ..Default::default()
        };
        let outcome =
            TriggerSearchOutcome::new(serde_json::json!([{"repoName": "r"}]), 1, Some(previous));
        run_query_trigger(
            &store,
            &FakeSearcher {
                outcome: Ok(outcome),
            },
            1,
            Duration::from_secs(300),
            OffsetDateTime::from_unix_timestamp(1000).unwrap(),
        )
        .await
        .expect("handle");
        assert_eq!(
            store.calls.lock().unwrap().as_slice(),
            &[
                "get_query_trigger",
                "get_monitor",
                "set_next_run",
                "update_results",
                "enqueue_actions",
            ]
        );
        assert_eq!(*store.latest_at_set.lock().unwrap(), Some(previous));
    }

    #[test]
    fn site_configuration_helpers_match_the_baseline_defaults() {
        let default = SiteConfiguration::default();
        assert_eq!(code_monitors_poll_interval(&default), DEFAULT_POLL_INTERVAL);
        assert_eq!(external_url(&default), "");

        let configured = SiteConfiguration::parse(
            r#"{"externalURL": "https://sg.example.com", "codeMonitors": {"pollInterval": "90s"}}"#,
        )
        .unwrap();
        assert_eq!(
            code_monitors_poll_interval(&configured),
            Duration::from_secs(90)
        );
        assert_eq!(external_url(&configured), "https://sg.example.com");
    }
}
