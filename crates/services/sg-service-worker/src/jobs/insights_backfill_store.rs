#![forbid(unsafe_code)]

//! `insights-job` backfill worker store: the `dbworkerstore.Store[*BaseJob]`
//! for the two backfill queues of `internal/insights/scheduler`.
//!
//! `makeNewBackfillWorker` and `makeInProgressWorker` both configure the
//! generic `internal/workerutil/dbworker/store` with
//!
//! * `TableName: insights_background_jobs`;
//! * `ViewName: insights_jobs_backfill_new` / `insights_jobs_backfill_in_progress`
//!   (views over the table joined to `insight_series_backfill`, filtered by the
//!   backfill state; the in-progress view also exposes `estimated_cost` and
//!   `cost_bucket`);
//! * `ColumnExpressions: baseJobColumns` (`scheduler.baseJobColumns`);
//! * `OrderByExpression: id` (new) / `estimated_cost, backfill_id` (in
//!   progress);
//! * `MaxNumResets: 100`, `StalledMaxAge: 30s`, `RetryAfter: 30s`,
//!   `MaxNumRetries: 3`.
//!
//! This module transcribes the generic store's SQL for exactly those options,
//! following the same approach as
//! [`crate::jobs::insights_query_runner::store`]. The worker/resetter loops
//! live in [`crate::framework::dbworker`].
//!
//! The queue table is created by the `codeinsights` migration
//! `1664984848_insights_scheduler_table`; the backfill columns and views by
//! `1665616961_backfill_table` (and `1667309737_backfill_priority_groups` for
//! `cost_bucket`). No new tables or columns are introduced.
//!
//! `execution_logs` (`json[]`) is not read (the handlers never touch it);
//! dequeue still clears it, exactly like `columnsUpdatedByDequeue`.
//!
//! The `insights-job` handlers (`newBackfillHandler`, `inProgressHandler`) are
//! not ported yet, and the job therefore stays out of the registry. Landing
//! the store first keeps the queue SQL independently verifiable.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use crate::framework::dbworker::{DbRecord, DbResetter, DbWorkerStore};

/// `dbworkerstore.Options.TableName` for both backfill stores.
pub const TABLE_NAME: &str = "insights_background_jobs";
/// `makeNewBackfillWorker`'s store view.
pub const NEW_VIEW: &str = "insights_jobs_backfill_new";
/// `makeInProgressWorker`'s store view.
pub const IN_PROGRESS_VIEW: &str = "insights_jobs_backfill_in_progress";
/// `makeNewBackfillWorker`'s `OrderByExpression`.
pub const NEW_ORDER_BY: &str = "id";
/// `makeInProgressWorker`'s `OrderByExpression`.
pub const IN_PROGRESS_ORDER_BY: &str = "estimated_cost, backfill_id";
/// `Options.MaxNumResets` in both backfill workers.
pub const MAX_NUM_RESETS: i32 = 100;
/// `Options.StalledMaxAge` in both backfill workers.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(30);
/// `Options.RetryAfter` in both backfill workers.
pub const RETRY_AFTER: Duration = Duration::from_secs(30);
/// `Options.MaxNumRetries` in both backfill workers.
pub const MAX_NUM_RETRIES: i32 = 3;
/// `defaultResetFailureMessage` in `internal/workerutil/dbworker/store`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// `scheduler.BaseJob` (the worker-side view of an `insights_background_jobs`
/// row). `execution_logs` is deliberately omitted.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillJob {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: Option<OffsetDateTime>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub cancel: bool,
    pub backfill_id: Option<i32>,
}

impl DbRecord for BackfillJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

/// One of the two backfill worker store configurations.
#[derive(Debug, Clone, Copy)]
pub struct BackfillStoreConfig {
    /// `Options.Name` (`<worker>_store`).
    pub name: &'static str,
    /// `Options.ViewName`.
    pub view: &'static str,
    /// `Options.OrderByExpression`.
    pub order_by: &'static str,
}

/// `fmt.Sprintf("%s_store", "backfill_new_backfill_worker")`.
pub const NEW_STORE: BackfillStoreConfig = BackfillStoreConfig {
    name: "backfill_new_backfill_worker_store",
    view: NEW_VIEW,
    order_by: NEW_ORDER_BY,
};

/// `fmt.Sprintf("%s_store", "backfill_in_progress_worker")`.
pub const IN_PROGRESS_STORE: BackfillStoreConfig = BackfillStoreConfig {
    name: "backfill_in_progress_worker_store",
    view: IN_PROGRESS_VIEW,
    order_by: IN_PROGRESS_ORDER_BY,
};

/// Dequeue's selected columns, with the columns updated by the data-modifying
/// CTE replaced by their new values (the CTE update is not visible to the
/// final `SELECT` in the same statement).
const DEQUEUE_COLUMNS: &str = r#"
	id,
	'processing' AS state,
	NULL::text AS failure_message,
	queued_at,
	$1 AS started_at,
	NULL::timestamptz AS finished_at,
	process_after,
	num_resets,
	num_failures,
	$1 AS last_heartbeat_at,
	$3 AS worker_hostname,
	cancel,
	backfill_id
"#;

/// A handle on the `insights_background_jobs` queue through one of the
/// backfill views.
#[derive(Clone)]
pub struct BackfillJobStore {
    pool: PgPool,
    config: BackfillStoreConfig,
}

impl BackfillJobStore {
    pub fn new(pool: PgPool, config: BackfillStoreConfig) -> Self {
        Self { pool, config }
    }

    pub fn config(&self) -> BackfillStoreConfig {
        self.config
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `Store.CountByState`.
    pub async fn count_by_state(&self, states: &[&str]) -> Result<i64, String> {
        if states.is_empty() {
            return Err(
                "pre-condition failure: statesBitset should contain at least one state".to_owned(),
            );
        }
        let placeholders: Vec<String> = (1..=states.len()).map(|i| format!("${i}")).collect();
        let query = format!(
            "SELECT COUNT(*) FROM {} WHERE state IN ({})",
            self.config.view,
            placeholders.join(", ")
        );
        let mut query = sqlx::query_scalar::<_, i64>(&query);
        for state in states {
            query = query.bind(*state);
        }
        query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| err.to_string())
    }

    /// `Store.Exists`.
    pub async fn exists(&self, states: &[&str]) -> Result<bool, String> {
        if states.is_empty() {
            return Err(
                "pre-condition failure: statesBitset should contain at least one state".to_owned(),
            );
        }
        let placeholders: Vec<String> = (1..=states.len()).map(|i| format!("${i}")).collect();
        let query = format!(
            "SELECT EXISTS (SELECT * FROM {} WHERE state IN ({}))",
            self.config.view,
            placeholders.join(", ")
        );
        let mut query = sqlx::query_scalar::<_, bool>(&query);
        for state in states {
            query = query.bind(*state);
        }
        query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| err.to_string())
    }

    /// `Store.Dequeue` with no additional conditions.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<BackfillJob>, String> {
        let query = format!(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY {order_by}) AS candidate_order
	FROM {view}
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
	SELECT
		id FROM {table}
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF {table} SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE
		{table}
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
SELECT
	{columns}
FROM
	{view}
WHERE
	id IN (SELECT id FROM candidate)
"#,
            order_by = self.config.order_by,
            view = self.config.view,
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

    /// `Store.Heartbeat` with the given ids (and an optional
    /// `WorkerHostname` condition).
    pub async fn heartbeat(
        &self,
        ids: &[i32],
        now: OffsetDateTime,
        worker_hostname: Option<&str>,
    ) -> Result<(Vec<i32>, Vec<i32>), String> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let hostname_cond = match worker_hostname {
            Some(_) => "AND worker_hostname = $3",
            None => "",
        };
        let query = format!(
            r#"
WITH alive_candidates AS (
	SELECT
		id
	FROM
		{table}
	WHERE
		id = ANY($1) AND state = 'processing' {hostname_cond}
	ORDER BY
		id ASC
	FOR UPDATE
)
UPDATE
	{table}
SET
	last_heartbeat_at = $2
WHERE
	id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
            table = TABLE_NAME,
            hostname_cond = hostname_cond,
        );

        let mut query = sqlx::query(&query).bind(ids).bind(now);
        if let Some(hostname) = worker_hostname {
            query = query.bind(hostname);
        }
        let rows = query
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

    /// `Store.Requeue`.
    pub async fn requeue(&self, id: i32, after: OffsetDateTime) -> Result<(), String> {
        sqlx::query(
            r#"
UPDATE insights_background_jobs
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	process_after = $2,
	cancel = false
WHERE id = $1
"#,
        )
        .bind(id)
        .bind(after)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(())
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_background_jobs
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

    /// `Store.MarkErrored` with `MaxNumRetries = 3`.
    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_background_jobs
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

    /// `Store.MarkFailed`.
    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE insights_background_jobs
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
}

/// `Store.ResetStalled` (`StalledMaxAge = 30s`, `MaxNumResets = 100`).
#[async_trait]
impl DbResetter for BackfillJobStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let now = OffsetDateTime::now_utc();
        let (reset, failed) = self.reset_stalled_at(now).await?;
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

impl BackfillJobStore {
    /// `Store.ResetStalled` with an explicit clock (the Go store's
    /// `options.clock` is not settable from the scheduler package, so the
    /// differential harness drives the same SQL with a wall clock).
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM insights_background_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_background_jobs
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
	SELECT id FROM insights_background_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE insights_background_jobs
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
        .bind(DEFAULT_RESET_FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        Ok((
            scan_last_heartbeats(reset_rows, now)?,
            scan_last_heartbeats(failed_rows, now)?,
        ))
    }
}

fn scan_last_heartbeats(
    rows: Vec<sqlx::postgres::PgRow>,
    now: OffsetDateTime,
) -> Result<Vec<(i32, Duration)>, String> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?;
        let last_heartbeat: Option<OffsetDateTime> = row
            .try_get("last_heartbeat_at")
            .map_err(|err: sqlx::Error| err.to_string())?;
        let age = match last_heartbeat {
            Some(last) => Duration::try_from(now - last).unwrap_or(Duration::ZERO),
            None => Duration::ZERO,
        };
        out.push((id, age));
    }
    Ok(out)
}

/// `scanBaseJob` minus `execution_logs`.
fn scan_job(row: sqlx::postgres::PgRow) -> Result<BackfillJob, String> {
    Ok(BackfillJob {
        id: row.try_get(0).map_err(|err: sqlx::Error| err.to_string())?,
        state: row.try_get(1).map_err(|err: sqlx::Error| err.to_string())?,
        failure_message: row.try_get(2).map_err(|err: sqlx::Error| err.to_string())?,
        queued_at: row.try_get(3).map_err(|err: sqlx::Error| err.to_string())?,
        started_at: row.try_get(4).map_err(|err: sqlx::Error| err.to_string())?,
        finished_at: row.try_get(5).map_err(|err: sqlx::Error| err.to_string())?,
        process_after: row.try_get(6).map_err(|err: sqlx::Error| err.to_string())?,
        num_resets: row.try_get(7).map_err(|err: sqlx::Error| err.to_string())?,
        num_failures: row.try_get(8).map_err(|err: sqlx::Error| err.to_string())?,
        last_heartbeat_at: row.try_get(9).map_err(|err: sqlx::Error| err.to_string())?,
        worker_hostname: row
            .try_get(10)
            .map_err(|err: sqlx::Error| err.to_string())?,
        cancel: row
            .try_get(11)
            .map_err(|err: sqlx::Error| err.to_string())?,
        backfill_id: row
            .try_get(12)
            .map_err(|err: sqlx::Error| err.to_string())?,
    })
}

#[async_trait]
impl DbWorkerStore for BackfillJobStore {
    type Record = BackfillJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        BackfillJobStore::dequeue(self, worker_hostname, OffsetDateTime::now_utc()).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed: Vec<i32> = ids
            .iter()
            .map(|id| id.parse::<i32>().map_err(|err| err.to_string()))
            .collect::<Result<_, _>>()?;
        let (known, canceled) =
            BackfillJobStore::heartbeat(self, &parsed, OffsetDateTime::now_utc(), None).await?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        BackfillJobStore::mark_complete(self, record.id).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        BackfillJobStore::mark_errored(self, record.id, failure_message).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        BackfillJobStore::mark_failed(self, record.id, failure_message).await
    }
}
