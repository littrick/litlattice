#![forbid(unsafe_code)]

//! `insights-data-retention-job` (`cmd/worker/internal/insights`,
//! `internal/insights/background/retention`).
//!
//! The job returns nothing unless code insights is enabled
//! (`insights.IsEnabled`: `DISABLE_CODE_INSIGHTS`, then the single-docker
//! deployment exception), in which case it connects to the **codeinsights**
//! database and builds three routines:
//!
//! * `insights_data_retention_worker` — a `workerutil.Worker` over
//!   `insights_data_retention_jobs` (NumHandlers 5, interval 30m, heartbeat
//!   15s). The handler archives the points of one series that fall beyond the
//!   configured maximum sample size (`insights.maximumSampleSize`, default 30,
//!   capped at 90) inside a single transaction: read the offset-N recording
//!   time, move older non-snapshot recording times into
//!   `archived_insight_series_recording_times`, then move series points older
//!   than that timestamp into `archived_series_points`.
//!   `experimentalFeatures.insightsDataRetention: false` disables the archive
//!   step (`null` keeps it enabled).
//! * `insights_data_retention_worker_resetter` — `dbworker.Resetter` every
//!   minute (RetryAfter 15m, MaxNumRetries 5, MaxNumResets 5, StalledMaxAge
//!   60s).
//! * `insights.data_retention_job_cleaner` — hourly `PeriodicGoroutine` that
//!   deletes `completed` jobs whose `started_at` is older than a week.
//!
//! Storage is entirely the existing codeinsights schema; no new tables or
//! columns are introduced. Queue SQL reproduces
//! `internal/workerutil/dbworker/store` for this table's options.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use sg_config::{LiveConfig, SiteConfiguration};

use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore,
    ResetterOptions, ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::{Job, JobDeps};

/// `insightsDataRetentionJob.Description`.
pub const JOB_DESCRIPTION: &str =
    "prunes insight data and moves prune data to separate table for retention";

/// `retention.NewWorker`'s `WorkerOptions.Name`.
pub const WORKER_NAME: &str = "insights_data_retention_worker";
/// `retention.NewWorker`'s `WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str =
    "archives code insights data points over the maximum sample size";
/// `retention.NewWorker`'s `WorkerOptions.Interval`.
pub const WORKER_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// `retention.NewWorker`'s `WorkerOptions.HeartbeatInterval`.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `retention.NewWorker`'s `WorkerOptions.NumHandlers`.
pub const WORKER_NUM_HANDLERS: usize = 5;

/// `retention.NewResetter`'s `ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "insights_data_retention_worker_resetter";
/// `retention.NewResetter`'s `ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

/// `retention.NewCleaner`'s `goroutine.WithName`.
pub const CLEANER_NAME: &str = "insights.data_retention_job_cleaner";
/// `retention.NewCleaner`'s `goroutine.WithDescription`.
pub const CLEANER_DESCRIPTION: &str = "removes completed data retention jobs";
/// `retention.NewCleaner`'s `goroutine.WithInterval`.
pub const CLEANER_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// `cleanJobsFmtStr`'s `time.Now().Add(-168 * time.Hour)`.
pub const CLEANER_MAX_AGE: Duration = Duration::from_secs(168 * 60 * 60);

/// `CreateDBWorkerStore`'s `Options.TableName`.
pub const TABLE_NAME: &str = "insights_data_retention_jobs";
/// `CreateDBWorkerStore`'s `Options.RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(15 * 60);
/// `CreateDBWorkerStore`'s `Options.MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = 5;
/// `CreateDBWorkerStore`'s `Options.MaxNumResets`.
pub const MAX_NUM_RESETS: i32 = 5;
/// `CreateDBWorkerStore`'s `Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(60);
/// `defaultResetFailureMessage`.
pub const RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// `deploy.SingleDocker` (`internal/conf/deploy/deploytype.go`).
pub const SINGLE_DOCKER_DEPLOY_TYPE: &str = "docker-container";
/// `deploy.Type()`'s default.
pub const DEFAULT_DEPLOY_TYPE: &str = "kubernetes";

/// `getMaximumSampleSize`'s default.
pub const DEFAULT_MAXIMUM_SAMPLE_SIZE: i64 = 30;
/// `getMaximumSampleSize`'s cap.
pub const MAXIMUM_SAMPLE_SIZE_CAP: i64 = 90;

/// `strconv.ParseBool`: the accepted spellings and `(false, ErrSyntax)`
/// otherwise.
pub fn parse_go_bool(raw: &str) -> (bool, bool) {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => (true, true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => (false, true),
        _ => (false, false),
    }
}

/// `deploy.Type()`: `DEPLOY_TYPE`, defaulting to `kubernetes`.
pub fn deploy_type_from(raw: Option<&str>) -> String {
    match raw {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => DEFAULT_DEPLOY_TYPE.to_owned(),
    }
}

/// `insights.IsEnabled`, with the three environment inputs passed explicitly.
pub fn insights_enabled_from(
    disable_code_insights: Option<&str>,
    deploy_type: Option<&str>,
    allow_single_docker_code_insights: Option<&str>,
) -> bool {
    if let Some(raw) = disable_code_insights {
        if parse_go_bool(raw).0 {
            return false;
        }
    }
    if deploy_type_from(deploy_type) == SINGLE_DOCKER_DEPLOY_TYPE {
        return match allow_single_docker_code_insights {
            Some(raw) => parse_go_bool(raw).0,
            None => false,
        };
    }
    true
}

/// `insights.IsEnabled` (`internal/insights/insights.go`).
pub fn insights_enabled() -> bool {
    let disable = std::env::var("DISABLE_CODE_INSIGHTS").ok();
    let deploy = std::env::var("DEPLOY_TYPE").ok();
    let allow = std::env::var("ALLOW_SINGLE_DOCKER_CODE_INSIGHTS").ok();
    insights_enabled_from(disable.as_deref(), deploy.as_deref(), allow.as_deref())
}

/// `getMaximumSampleSize`'s config half: the configured value or the default.
/// Values above the cap are clamped by [`maximum_sample_size`].
pub fn configured_maximum_sample_size(site: &SiteConfiguration) -> i64 {
    let configured = site.insights_maximum_sample_size();
    if configured > 0 {
        configured
    } else {
        DEFAULT_MAXIMUM_SAMPLE_SIZE
    }
}

/// `getMaximumSampleSize`: default 30, capped at 90 (the baseline logs an info
/// line when clamping; the caller does that).
pub fn maximum_sample_size(site: &SiteConfiguration) -> (i64, bool) {
    let configured = configured_maximum_sample_size(site);
    if configured > MAXIMUM_SAMPLE_SIZE_CAP {
        (MAXIMUM_SAMPLE_SIZE_CAP, true)
    } else {
        (configured, false)
    }
}

/// `retention.DataRetentionJob` — the fields the worker loop and handler use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRetentionJob {
    pub id: i32,
    pub series_id: i32,
    pub series_id_string: String,
}

impl DbRecord for DataRetentionJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

/// `CreateDBWorkerStore` over `insights_data_retention_jobs` in the
/// codeinsights database.
#[derive(Clone)]
pub struct RetentionWorkerStore {
    pool: PgPool,
}

impl RetentionWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `workerutil/dbworker/store.(*store[T]).Dequeue` with the retention
    /// options (`OrderByExpression: queued_at, id`, `RetryAfter: 15m`).
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<DataRetentionJob>, String> {
        let row = sqlx::query(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY queued_at, id) AS candidate_order
	FROM insights_data_retention_jobs
	WHERE
		(
			(
				state = 'queued' AND
				(process_after IS NULL OR process_after <= $1)
			) OR (
				$2 > 0 AND
				state = 'errored' AND
				$1 - finished_at > ($2 * '1 second'::interval)
			)
		)
	ORDER BY queued_at, id
	LIMIT 50
),
candidate AS (
	SELECT id FROM insights_data_retention_jobs
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF insights_data_retention_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE insights_data_retention_jobs
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $3
	WHERE id IN (SELECT id FROM candidate)
	RETURNING id, series_id, series_id_string
)
SELECT id, series_id, series_id_string FROM updated_record
"#,
        )
        .bind(now)
        .bind(RETRY_AFTER.as_secs() as i64)
        .bind(worker_hostname)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        row.map(|row| {
            Ok(DataRetentionJob {
                id: row
                    .try_get("id")
                    .map_err(|err: sqlx::Error| err.to_string())?,
                series_id: row
                    .try_get("series_id")
                    .map_err(|err: sqlx::Error| err.to_string())?,
                series_id_string: row
                    .try_get("series_id_string")
                    .map_err(|err: sqlx::Error| err.to_string())?,
            })
        })
        .transpose()
    }

    /// `Store.Heartbeat`.
    pub async fn heartbeat(
        &self,
        ids: &[i32],
        now: OffsetDateTime,
    ) -> Result<(Vec<i32>, Vec<i32>), String> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let rows = sqlx::query(
            r#"
WITH alive_candidates AS (
	SELECT id
	FROM insights_data_retention_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE insights_data_retention_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
        )
        .bind(ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        let mut known = Vec::with_capacity(rows.len());
        let mut canceled = Vec::new();
        for row in rows {
            let id: i32 = row
                .try_get("id")
                .map_err(|err: sqlx::Error| err.to_string())?;
            let cancel: bool = row
                .try_get("cancel")
                .map_err(|err: sqlx::Error| err.to_string())?;
            known.push(id);
            if cancel {
                canceled.push(id);
            }
        }
        Ok((known, canceled))
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_data_retention_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkErrored` with `MaxNumRetries = 5`.
    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_data_retention_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= $3 THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .bind(failure_message)
        .bind(MAX_NUM_RETRIES)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkFailed` (non-retryable handler errors).
    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_data_retention_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .bind(failure_message)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.ResetStalled` (`StalledMaxAge = 60s`, `MaxNumResets = 5`).
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM insights_data_retention_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_data_retention_jobs
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(STALLED_MAX_AGE.as_secs() as i64)
        .bind(MAX_NUM_RESETS)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        let failed_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM insights_data_retention_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_data_retention_jobs
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(STALLED_MAX_AGE.as_secs() as i64)
        .bind(MAX_NUM_RESETS)
        .bind(RESET_FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        Ok((
            heartbeat_ages(now, reset_rows)?,
            heartbeat_ages(now, failed_rows)?,
        ))
    }
}

fn heartbeat_ages(
    now: OffsetDateTime,
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<(i32, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?;
        let last_heartbeat: Option<OffsetDateTime> = row
            .try_get("last_heartbeat_at")
            .map_err(|err: sqlx::Error| err.to_string())?;
        let age = match last_heartbeat {
            Some(last) => {
                let delta = now - last;
                Duration::try_from(delta).unwrap_or(Duration::ZERO)
            }
            None => Duration::ZERO,
        };
        ages.push((id, age));
    }
    Ok(ages)
}

#[async_trait]
impl DbWorkerStore for RetentionWorkerStore {
    type Record = DataRetentionJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        RetentionWorkerStore::dequeue(self, worker_hostname, OffsetDateTime::now_utc()).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let mut parsed = Vec::with_capacity(ids.len());
        for id in ids {
            parsed.push(
                id.parse::<i32>()
                    .map_err(|err| format!("invalid record id {id:?}: {err}"))?,
            );
        }
        let (known, canceled) =
            RetentionWorkerStore::heartbeat(self, &parsed, OffsetDateTime::now_utc()).await?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        RetentionWorkerStore::mark_complete(self, record.id).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        RetentionWorkerStore::mark_errored(self, record.id, failure_message).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        RetentionWorkerStore::mark_failed(self, record.id, failure_message).await
    }
}

#[async_trait]
impl DbResetter for RetentionWorkerStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) =
            RetentionWorkerStore::reset_stalled_at(self, OffsetDateTime::now_utc()).await?;
        Ok((
            reset
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
            failed
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
        ))
    }
}

/// The retention handler (`dataRetentionHandler.Handle`) plus the transaction
/// body as a standalone function.
pub struct RetentionHandler {
    pool: PgPool,
    live_config: std::sync::Arc<LiveConfig>,
}

impl RetentionHandler {
    pub fn new(pool: PgPool, live_config: std::sync::Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

/// `(dataRetentionHandler.Handle)`'s archive body, after the config gate.
pub async fn archive_series(
    pool: &PgPool,
    insight_series_id: i32,
    series_id: &str,
    maximum_sample_size: i64,
) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    // We remove 1 off the maximum sample size so that we get the last
    // timestamp that we want to keep data for. Snapshots are ignored.
    let oldest_row = sqlx::query(
        "select recording_time from insight_series_recording_times \
         where insight_series_id = $1 and snapshot is false \
         order by recording_time desc offset $2 limit 1",
    )
    .bind(insight_series_id)
    .bind(maximum_sample_size - 1)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| format!("GetOffsetNRecordingTime: {err}"))?;

    let Some(row) = oldest_row else {
        // This series does not have any data beyond the max sample size.
        tx.commit().await.map_err(|err| err.to_string())?;
        return Ok(());
    };
    let oldest_recording_time: OffsetDateTime = row
        .try_get("recording_time")
        .map_err(|err: sqlx::Error| format!("GetOffsetNRecordingTime: {err}"))?;

    sqlx::query(
        r#"
WITH moved_rows AS (
	DELETE FROM insight_series_recording_times
	WHERE insight_series_id = $1 AND snapshot IS FALSE AND recording_time < $2
	RETURNING *
)
INSERT INTO archived_insight_series_recording_times
SELECT * FROM moved_rows
ON CONFLICT DO NOTHING
"#,
    )
    .bind(insight_series_id)
    .bind(oldest_recording_time)
    .execute(&mut *tx)
    .await
    .map_err(|err| format!("archiveOldRecordingTimes: {err}"))?;

    sqlx::query(
        r#"
with moved_rows as (
	DELETE FROM series_points
	WHERE series_id = $1 AND time < $2
	RETURNING *
)
INSERT INTO archived_series_points (series_id, time, value, repo_id, repo_name_id, original_repo_name_id, capture)
SELECT series_id, time, value, repo_id, repo_name_id, original_repo_name_id, capture from moved_rows
ON CONFLICT DO NOTHING
"#,
    )
    .bind(series_id)
    .bind(oldest_recording_time)
    .execute(&mut *tx)
    .await
    .map_err(|err| format!("archiveOldSeriesPoints: {err}"))?;

    tx.commit().await.map_err(|err| err.to_string())
}

#[async_trait]
impl DbWorkerHandler<RetentionWorkerStore> for RetentionHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &DataRetentionJob,
    ) -> Result<(), String> {
        let site = self.live_config.current().site.clone();
        // If the setting is not set we run retention by default.
        if let Some(false) = site.insights_data_retention() {
            return Ok(());
        }

        let (maximum_sample_size, clamped) = maximum_sample_size(&site);
        if clamped {
            tracing::info!(
                disallowed_maximum_value = configured_maximum_sample_size(&site),
                "code insights maximum sample size was set over allowed maximum, setting to 90"
            );
        }

        archive_series(
            &self.pool,
            record.series_id,
            &record.series_id_string,
            maximum_sample_size,
        )
        .await
    }
}

/// `cleanJobs`: deletes `completed` jobs whose `started_at` is older than a
/// week.
pub async fn clean_jobs(pool: &PgPool, before: OffsetDateTime) -> Result<u64, String> {
    let result = sqlx::query(
        "DELETE FROM insights_data_retention_jobs WHERE state='completed' AND started_at <= $1",
    )
    .bind(before)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(result.rows_affected())
}

/// `retention.EnqueueJob`: queue one series for the retention worker.
///
/// `insights_data_retention_jobs.series_id` is the `insight_series.id` and
/// `series_id_string` is the string `series_id` (the column names are
/// inverted in the baseline schema).
pub async fn enqueue_job(
    pool: &PgPool,
    insight_series_id: i32,
    series_id: &str,
) -> Result<i32, String> {
    let row = sqlx::query(
        "INSERT INTO insights_data_retention_jobs (series_id, series_id_string) VALUES ($1, $2) RETURNING id",
    )
    .bind(insight_series_id)
    .bind(series_id)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())?;
    row.try_get("id")
        .map_err(|err: sqlx::Error| err.to_string())
}

/// `retention.NewCleaner`'s handler.
pub struct CleanerHandler {
    pool: PgPool,
}

impl CleanerHandler {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Handler for CleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let before = OffsetDateTime::now_utc()
            - time::Duration::try_from(CLEANER_MAX_AGE)
                .map_err(|_| HandlerError::new("retention outside the timestamp range"))?;
        clean_jobs(&self.pool, before)
            .await
            .map(|_| ())
            .map_err(HandlerError::new)
    }
}

/// `insightsDataRetentionJob`.
pub struct InsightsDataRetentionJob;

#[async_trait]
impl Job for InsightsDataRetentionJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !insights_enabled() {
            tracing::debug!("Code Insights disabled. Disabling insights data retention job.");
            return Ok(Vec::new());
        }
        tracing::debug!("Code Insights enabled. Enabling insights data retention job.");

        let dsn = deps
            .live_config
            .current()
            .service_connections
            .code_insights_dsn
            .clone();
        let pool = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| format!("failed to connect to codeinsights database: {err}"))?;

        let store = std::sync::Arc::new(RetentionWorkerStore::new(pool.clone()));
        let handler = std::sync::Arc::new(RetentionHandler::new(
            pool.clone(),
            std::sync::Arc::clone(&deps.live_config),
        ));

        let worker = WorkerRoutine::new(
            std::sync::Arc::clone(&store),
            handler,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: WORKER_INTERVAL,
                heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
                num_handlers: WORKER_NUM_HANDLERS,
                worker_hostname: default_hostname(),
            },
        );

        let resetter = ResetterRoutine::new(
            store as std::sync::Arc<dyn DbResetter>,
            ResetterOptions {
                name: RESETTER_NAME.to_owned(),
                interval: RESETTER_INTERVAL,
            },
        );

        let cleaner = PeriodicRoutine::new(std::sync::Arc::new(CleanerHandler::new(pool)))
            .with_name(CLEANER_NAME)
            .with_description(CLEANER_DESCRIPTION)
            .with_interval(CLEANER_INTERVAL);

        Ok(vec![
            BackgroundRoutine::DbBacked(
                std::sync::Arc::new(worker) as std::sync::Arc<dyn DbBackedRoutine>
            ),
            BackgroundRoutine::DbBacked(
                std::sync::Arc::new(resetter) as std::sync::Arc<dyn DbBackedRoutine>
            ),
            cleaner.into(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_go_bool_matches_strconv() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert_eq!(parse_go_bool(raw), (true, true), "{raw}");
        }
        for raw in ["0", "f", "F", "FALSE", "false", "False"] {
            assert_eq!(parse_go_bool(raw), (false, true), "{raw}");
        }
        assert_eq!(parse_go_bool("yes"), (false, false));
        assert_eq!(parse_go_bool(""), (false, false));
    }

    #[test]
    fn insights_enabled_reproduces_the_baseline() {
        assert!(insights_enabled_from(None, None, None));
        assert!(!insights_enabled_from(Some("true"), None, None));
        assert!(!insights_enabled_from(Some("1"), None, None));
        assert!(insights_enabled_from(Some("false"), None, None));
        // Single docker disables unless explicitly allowed.
        assert!(!insights_enabled_from(None, Some("docker-container"), None));
        assert!(insights_enabled_from(
            None,
            Some("docker-container"),
            Some("true")
        ));
        assert!(!insights_enabled_from(
            None,
            Some("docker-container"),
            Some("false")
        ));
        // A non-single-docker deployment ignores the allow flag.
        assert!(insights_enabled_from(
            None,
            Some("kubernetes"),
            Some("false")
        ));
    }

    #[test]
    fn maximum_sample_size_defaults_and_caps() {
        let site = SiteConfiguration::parse("{}").unwrap();
        assert_eq!(maximum_sample_size(&site), (30, false));

        let site = SiteConfiguration::parse(r#"{"insights.maximumSampleSize": 45}"#).unwrap();
        assert_eq!(maximum_sample_size(&site), (45, false));

        let site = SiteConfiguration::parse(r#"{"insights.maximumSampleSize": 0}"#).unwrap();
        assert_eq!(maximum_sample_size(&site), (30, false));

        let site = SiteConfiguration::parse(r#"{"insights.maximumSampleSize": 120}"#).unwrap();
        assert_eq!(maximum_sample_size(&site), (90, true));
    }

    #[test]
    fn insights_data_retention_is_tri_state() {
        let site = SiteConfiguration::parse("{}").unwrap();
        assert_eq!(site.insights_data_retention(), None);

        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures": {"insightsDataRetention": false}}"#,
        )
        .unwrap();
        assert_eq!(site.insights_data_retention(), Some(false));

        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures": {"insightsDataRetention": true}}"#,
        )
        .unwrap();
        assert_eq!(site.insights_data_retention(), Some(true));
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = InsightsDataRetentionJob;
        assert_eq!(
            job.description(),
            "prunes insight data and moves prune data to separate table for retention"
        );
        assert!(job.configs().is_empty());
    }

    #[test]
    fn routine_names_and_intervals_match_the_baseline() {
        assert_eq!(WORKER_NAME, "insights_data_retention_worker");
        assert_eq!(RESETTER_NAME, "insights_data_retention_worker_resetter");
        assert_eq!(CLEANER_NAME, "insights.data_retention_job_cleaner");
        assert_eq!(WORKER_INTERVAL, Duration::from_secs(1800));
        assert_eq!(RESETTER_INTERVAL, Duration::from_secs(60));
        assert_eq!(CLEANER_INTERVAL, Duration::from_secs(3600));
    }
}
