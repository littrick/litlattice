#![forbid(unsafe_code)]

//! `internal/insights/background/queryrunner` worker store: the
//! `insights_query_runner_jobs` queue lives in the **frontend** database
//! (`GetBackgroundQueryRunnerJob` builds the base store from `mainAppDB`).
//!
//! SQL is a transcription of `internal/workerutil/dbworker/store` for the
//! options `queryrunner.CreateDBWorkerStore` sets (`OrderByExpression:
//! priority, id`, `StalledMaxAge 60s`, `RetryAfter 30m`, `MaxNumRetries 10`,
//! `MaxNumResets 10`) plus the queryrunner-specific reads (`dequeueJob` and
//! its dependency table). The generic store machinery itself is in
//! [`crate::framework::dbworker`]; this module only supplies the statements.
//!
//! `execution_logs` (a `json[]`) is not read: the handler never touches it.
//! Dequeue still clears it, exactly like `columnsUpdatedByDequeue`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::Row;

use crate::framework::dbworker::{DbRecord, DbResetter, DbWorkerStore};

/// `CreateDBWorkerStore`'s `Options.Name`.
pub const STORE_NAME: &str = "insights_query_runner_jobs_store";
/// `CreateDBWorkerStore`'s `Options.TableName`.
pub const TABLE_NAME: &str = "insights_query_runner_jobs";
/// `CreateDBWorkerStore`'s `Options.OrderByExpression`.
pub const ORDER_BY: &str = "priority, id";
/// `CreateDBWorkerStore`'s `Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(60);
/// `CreateDBWorkerStore`'s `Options.RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(30 * 60);
/// `CreateDBWorkerStore`'s `Options.MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = 10;
/// `CreateDBWorkerStore`'s `Options.MaxNumResets`.
pub const MAX_NUM_RESETS: i32 = 10;
/// `defaultResetFailureMessage` in `internal/workerutil/dbworker/store`.
pub const RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";
/// `NewCleaner`'s `cleanJobsFmtStr`: `time.Now().Add(-168 * time.Hour)`.
pub const CLEAN_MAX_AGE: Duration = Duration::from_secs(168 * 60 * 60);

/// `queryrunner.Job` (the `SearchJob` fields plus the dbworker columns the
/// handler reads).
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    // SearchJob.
    pub series_id: String,
    pub search_query: String,
    pub record_time: Option<OffsetDateTime>,
    pub persist_mode: String,
    pub dependent_frames: Vec<PrimitiveDateTime>,

    // Query runner fields.
    pub cost: i32,
    pub priority: i32,

    // Standard/required dbworker fields.
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
}

impl DbRecord for Job {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

/// `store.RecordMode` / `store.SnapshotMode`.
pub const RECORD_MODE: &str = "record";
pub const SNAPSHOT_MODE: &str = "snapshot";

/// The `queryrunner.Job` fields `EnqueueJob` writes; the remaining dbworker
/// columns are left at their table defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    pub series_id: String,
    pub search_query: String,
    pub record_time: Option<OffsetDateTime>,
    pub persist_mode: String,
    pub dependent_frames: Vec<PrimitiveDateTime>,
    pub state: String,
    pub process_after: Option<OffsetDateTime>,
    pub cost: i32,
    pub priority: i32,
}

/// `queryrunner.EnqueueJob`: insert the job row, then its dependent frames,
/// in one transaction, returning the new id.
pub async fn enqueue_job(pool: &PgPool, job: &NewJob) -> Result<i32, String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
    let record_time = job
        .record_time
        .map(|value| PrimitiveDateTime::new(value.date(), value.time()));
    let process_after = job
        .process_after
        .map(|value| PrimitiveDateTime::new(value.date(), value.time()));
    let row = sqlx::query(
        r#"
INSERT INTO insights_query_runner_jobs (
	series_id,
	search_query,
	record_time,
	state,
	process_after,
	cost,
	priority,
	persist_mode
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::persistmode)
RETURNING id
"#,
    )
    .bind(&job.series_id)
    .bind(&job.search_query)
    .bind(record_time)
    .bind(&job.state)
    .bind(process_after)
    .bind(job.cost)
    .bind(job.priority)
    .bind(&job.persist_mode)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| err.to_string())?;
    let id: i32 = row
        .try_get("id")
        .map_err(|err: sqlx::Error| err.to_string())?;
    if !job.dependent_frames.is_empty() {
        let mut values: Vec<String> = Vec::with_capacity(job.dependent_frames.len());
        for index in 0..job.dependent_frames.len() {
            values.push(format!("($1, ${})", index + 2));
        }
        let sql = format!(
            "INSERT INTO insights_query_runner_jobs_dependencies (job_id, recording_time) VALUES {}",
            values.join(",")
        );
        let mut query = sqlx::query(&sql).bind(id);
        for frame in &job.dependent_frames {
            query = query.bind(*frame);
        }
        if query.execute(&mut *tx).await.is_err() {
            // Baseline quirk: `EnqueueJob` returns `(0, nil)` when the
            // dependency insert fails, so the already-inserted job row is
            // committed and the caller sees success with id 0.
            tx.commit().await.map_err(|err| err.to_string())?;
            return Ok(0);
        }
    }
    tx.commit().await.map_err(|err| err.to_string())?;
    Ok(id)
}

/// `workerStoreExtra.WillRetry`: `int(NumFailures)+1 < MaxNumRetries`.
pub fn will_retry(num_failures: i32) -> bool {
    num_failures + 1 < MAX_NUM_RETRIES
}

/// A handle on `insights_query_runner_jobs` in the frontend database.
#[derive(Clone)]
pub struct QueryRunnerStore {
    pool: PgPool,
}

/// The 14 scanned columns in `scanJob` order, with the columns updated by
/// dequeue replaced by their new values (the data-modifying CTE is not
/// visible to the final SELECT in the same statement).
const DEQUEUE_COLUMNS: &str = r#"
	series_id,
	search_query,
	record_time,
	cost,
	priority,
	persist_mode::text AS persist_mode,
	id,
	'processing' AS state,
	NULL::text AS failure_message,
	$1 AS started_at,
	NULL::timestamptz AS finished_at,
	process_after,
	num_resets,
	num_failures
"#;

/// `scanJob`'s column list for a plain `SELECT ... WHERE id = $1`
/// (`dequeueJobFmtStr`).
const JOB_COLUMNS: &str = r#"
	series_id,
	search_query,
	record_time,
	cost,
	priority,
	persist_mode::text AS persist_mode,
	id,
	state,
	failure_message,
	started_at,
	finished_at,
	process_after,
	num_resets,
	num_failures
"#;

impl QueryRunnerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `workerutil/dbworker/store.(*store[T]).Dequeue` with the queryrunner
    /// options. The returned job has no dependency frames; use
    /// [`QueryRunnerStore::dequeue_job`] for that, like the baseline handler.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<Job>, String> {
        let query = format!(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY {order_by}) AS candidate_order
	FROM {table}
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
	ORDER BY {order_by}
	LIMIT 50
),
candidate AS (
	SELECT id FROM {table}
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF {table} SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE {table}
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $3
	WHERE id IN (SELECT id FROM candidate)
)
SELECT {columns}
FROM {table}
WHERE id IN (SELECT id FROM candidate)
"#,
            order_by = ORDER_BY,
            table = TABLE_NAME,
            columns = DEQUEUE_COLUMNS,
        );

        let row = sqlx::query(&query)
            .bind(now)
            .bind(RETRY_AFTER.as_secs() as i64)
            .bind(worker_hostname)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| err.to_string())?;

        row.map(scan_job).transpose()
    }

    /// `dequeueJob`: re-read the record (and its dependency frames) inside the
    /// handler so the worker has the authoritative row.
    pub async fn dequeue_job(&self, record_id: i32) -> Result<Job, String> {
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;

        let query = format!("SELECT {} FROM {} WHERE id = $1", JOB_COLUMNS, TABLE_NAME);
        let rows = sqlx::query(&query)
            .bind(record_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;
        if rows.len() != 1 {
            return Err(format!("expected 1 job to dequeue, found {}", rows.len()));
        }
        let mut job = scan_job(rows.into_iter().next().expect("one row"))?;

        let dependencies = sqlx::query(
            "select recording_time from insights_query_runner_jobs_dependencies where job_id = $1",
        )
        .bind(record_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
        for row in dependencies {
            let recording_time: PrimitiveDateTime = row
                .try_get("recording_time")
                .map_err(|err: sqlx::Error| err.to_string())?;
            job.dependent_frames.push(recording_time);
        }

        tx.commit().await.map_err(|err| err.to_string())?;
        Ok(job)
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
	FROM insights_query_runner_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE insights_query_runner_jobs
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
UPDATE insights_query_runner_jobs
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

    /// `Store.MarkErrored` with `MaxNumRetries = 10`.
    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_query_runner_jobs
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
UPDATE insights_query_runner_jobs
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

    /// `Store.ResetStalled` (`StalledMaxAge = 60s`, `MaxNumResets = 10`).
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM insights_query_runner_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_query_runner_jobs
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
	SELECT id FROM insights_query_runner_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_query_runner_jobs
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

/// `cleanJobs`: deletes `completed` jobs whose `started_at` is older than a
/// week and returns the number of removed rows.
pub async fn clean_jobs(pool: &PgPool, before: OffsetDateTime) -> Result<u64, String> {
    let row = sqlx::query(
        r#"
WITH deleted AS (
	DELETE FROM insights_query_runner_jobs WHERE state='completed' AND started_at <= $1 RETURNING *
) SELECT count(*) FROM deleted
"#,
    )
    .bind(before)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())?;
    let count: i64 = row
        .try_get("count")
        .map_err(|err: sqlx::Error| err.to_string())?;
    Ok(count as u64)
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

fn scan_job(row: sqlx::postgres::PgRow) -> Result<Job, String> {
    Ok(Job {
        series_id: row
            .try_get("series_id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        search_query: row
            .try_get("search_query")
            .map_err(|err: sqlx::Error| err.to_string())?,
        record_time: row
            .try_get("record_time")
            .map_err(|err: sqlx::Error| err.to_string())?,
        persist_mode: row
            .try_get("persist_mode")
            .map_err(|err: sqlx::Error| err.to_string())?,
        cost: row
            .try_get("cost")
            .map_err(|err: sqlx::Error| err.to_string())?,
        priority: row
            .try_get("priority")
            .map_err(|err: sqlx::Error| err.to_string())?,
        id: row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        state: row
            .try_get("state")
            .map_err(|err: sqlx::Error| err.to_string())?,
        failure_message: row
            .try_get("failure_message")
            .map_err(|err: sqlx::Error| err.to_string())?,
        started_at: row
            .try_get("started_at")
            .map_err(|err: sqlx::Error| err.to_string())?,
        finished_at: row
            .try_get("finished_at")
            .map_err(|err: sqlx::Error| err.to_string())?,
        process_after: row
            .try_get("process_after")
            .map_err(|err: sqlx::Error| err.to_string())?,
        num_resets: row
            .try_get("num_resets")
            .map_err(|err: sqlx::Error| err.to_string())?,
        num_failures: row
            .try_get("num_failures")
            .map_err(|err: sqlx::Error| err.to_string())?,
        dependent_frames: Vec::new(),
    })
}

#[async_trait]
impl DbWorkerStore for QueryRunnerStore {
    type Record = Job;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        QueryRunnerStore::dequeue(self, worker_hostname, OffsetDateTime::now_utc()).await
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
            QueryRunnerStore::heartbeat(self, &parsed, OffsetDateTime::now_utc()).await?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        QueryRunnerStore::mark_complete(self, record.id).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        QueryRunnerStore::mark_errored(self, record.id, failure_message).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        QueryRunnerStore::mark_failed(self, record.id, failure_message).await
    }
}

#[async_trait]
impl DbResetter for QueryRunnerStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) =
            QueryRunnerStore::reset_stalled_at(self, OffsetDateTime::now_utc()).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn will_retry_matches_the_baseline_boundary() {
        for failures in 0..8 {
            assert!(will_retry(failures), "{failures}");
        }
        // `NumFailures+1 < 10`: the 10th attempt is the last.
        assert!(!will_retry(9));
        assert!(!will_retry(10));
    }

    #[test]
    fn store_options_match_the_baseline() {
        assert_eq!(STORE_NAME, "insights_query_runner_jobs_store");
        assert_eq!(TABLE_NAME, "insights_query_runner_jobs");
        assert_eq!(ORDER_BY, "priority, id");
        assert_eq!(STALLED_MAX_AGE, Duration::from_secs(60));
        assert_eq!(RETRY_AFTER, Duration::from_secs(1800));
        assert_eq!(MAX_NUM_RETRIES, 10);
        assert_eq!(MAX_NUM_RESETS, 10);
        assert_eq!(CLEAN_MAX_AGE, Duration::from_secs(168 * 3600));
    }
}
