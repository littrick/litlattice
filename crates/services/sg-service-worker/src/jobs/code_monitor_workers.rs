#![forbid(unsafe_code)]

//! Code monitor trigger/action dbworkers
//! (`internal/codemonitors/background/workers.go`).
//!
//! The baseline builds two generic `dbworkerstore.New` instances over
//! `cm_trigger_jobs` / `cm_action_jobs` and wraps them in a
//! `workerutil.Worker` plus a `dbworker.Resetter`:
//!
//! * `code_monitors_trigger_jobs_worker` — description "runs trigger queries
//!   for code monitors", `NumHandlers = conf.CodeMonitors().Concurrency`
//!   (default 4), interval 5s, heartbeat 15s, `MaximumRuntimePerJob` 1m;
//! * `code_monitors_action_jobs_worker` — description "runs actions for code
//!   monitors", 1 handler, interval 5s, heartbeat 15s;
//! * the matching resetters, both every minute;
//! * both stores use `StalledMaxAge` 60s, `RetryAfter` 10s,
//!   `MaxNumRetries` 3 and `ORDER BY id`. `MaxNumResets` is left at its zero
//!   value, so `ResetStalled` never requeues a stalled record: every stalled
//!   `processing` record is moved straight to `failed` (the reset query's
//!   `num_resets < 0` matches nothing and the failure query's
//!   `num_resets >= 0` matches everything). This is baseline behaviour and is
//!   reproduced verbatim; see docs §106.3.
//!
//! The handlers themselves are not part of this module: `queryRunner` needs
//! the unported trigger search execution and `actionRunner` needs the
//! unported e-mail transport. [`new_trigger_query_runner`] and
//! [`new_action_runner`] therefore take the handler as a parameter, exactly
//! like the Go constructors take the store. The differential harness
//! (`tests/code_monitor_workers.rs`) drives the real stores and prints the
//! same canonical lines as `cm_workers_harness_test.go`.
//!
//! Not ported: `MaximumRuntimePerJob` (the shared framework does not enforce
//! it; M9.8cm documents the gap), Prometheus metrics, and the observation
//! scoping of the store names.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use sg_config::SiteConfiguration;
use sg_db::code_monitors::{ActionJob, TriggerJob};

use crate::framework::dbworker::{
    DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore, ResetterOptions, ResetterRoutine,
    WorkerOptions, WorkerRoutine,
};

/// `newTriggerQueryRunner`'s `WorkerOptions.Name`.
pub const TRIGGER_WORKER_NAME: &str = "code_monitors_trigger_jobs_worker";
/// `newTriggerQueryRunner`'s `WorkerOptions.Description`.
pub const TRIGGER_WORKER_DESCRIPTION: &str = "runs trigger queries for code monitors";
/// `newTriggerQueryRunner`'s `WorkerOptions.Interval`.
pub const TRIGGER_WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `newTriggerQueryRunner`'s `WorkerOptions.HeartbeatInterval`.
pub const TRIGGER_WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `newTriggerQueryRunner`'s `WorkerOptions.MaximumRuntimePerJob`.
pub const TRIGGER_MAXIMUM_RUNTIME_PER_JOB: Duration = Duration::from_secs(60);
/// `newTriggerQueryResetter`'s `ResetterOptions.Name`.
pub const TRIGGER_RESETTER_NAME: &str = "code_monitors_trigger_jobs_worker_resetter";

/// `newActionRunner`'s `WorkerOptions.Name`.
pub const ACTION_WORKER_NAME: &str = "code_monitors_action_jobs_worker";
/// `newActionRunner`'s `WorkerOptions.Description`.
pub const ACTION_WORKER_DESCRIPTION: &str = "runs actions for code monitors";
/// `newActionRunner`'s `WorkerOptions.Interval`.
pub const ACTION_WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `newActionRunner`'s `WorkerOptions.HeartbeatInterval`.
pub const ACTION_WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `newActionRunner`'s `WorkerOptions.NumHandlers`.
pub const ACTION_NUM_HANDLERS: usize = 1;
/// `newActionJobResetter`'s `ResetterOptions.Name`.
pub const ACTION_RESETTER_NAME: &str = "code_monitors_action_jobs_worker_resetter";

/// Both resetters' `ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

/// `createDBWorkerStoreForTriggerJobs`'s `Options.Name`.
pub const TRIGGER_STORE_NAME: &str = "code_monitors_trigger_jobs_worker_store";
/// `createDBWorkerStoreForActionJobs`'s `Options.Name`.
pub const ACTION_STORE_NAME: &str = "code_monitors_action_jobs_worker_store";

/// `createDBWorkerStoreForTriggerJobs`'s `Options.TableName`.
pub const TRIGGER_TABLE_NAME: &str = "cm_trigger_jobs";
/// `createDBWorkerStoreForActionJobs`'s `Options.TableName`.
pub const ACTION_TABLE_NAME: &str = "cm_action_jobs";

/// Both stores' `Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(60);
/// Both stores' `Options.RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(10);
/// Both stores' `Options.MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = 3;
/// Both stores leave `Options.MaxNumResets` unset (Go zero value).
pub const MAX_NUM_RESETS: i32 = 0;
/// `defaultResetFailureMessage`.
pub const RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// `conf.CodeMonitors()`'s default `Concurrency`.
pub const DEFAULT_CONCURRENCY: i64 = 4;
/// `conf.CodeMonitors()`'s default `PollInterval` (not used by the worker).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `conf.CodeMonitors().Concurrency`: `siteConfig.codeMonitors.concurrency`
/// when non-zero, else 4. The baseline validates negative values away at the
/// configuration layer; the value is kept signed here so the caller can
/// reproduce that.
pub fn code_monitors_concurrency(site: &SiteConfiguration) -> i64 {
    let configured = site
        .raw()
        .get("codeMonitors")
        .and_then(|value| value.get("concurrency"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if configured != 0 {
        configured
    } else {
        DEFAULT_CONCURRENCY
    }
}

/// `newTriggerQueryRunner`'s `WorkerOptions`, with the concurrency the caller
/// resolved from `conf.CodeMonitors()`.
pub fn trigger_worker_options(concurrency: i64, worker_hostname: String) -> WorkerOptions {
    WorkerOptions {
        name: TRIGGER_WORKER_NAME.to_owned(),
        description: TRIGGER_WORKER_DESCRIPTION.to_owned(),
        interval: TRIGGER_WORKER_INTERVAL,
        heartbeat_interval: TRIGGER_WORKER_HEARTBEAT_INTERVAL,
        num_handlers: usize::try_from(concurrency).unwrap_or(0),
        worker_hostname,
    }
}

/// `newActionRunner`'s `WorkerOptions`.
pub fn action_worker_options(worker_hostname: String) -> WorkerOptions {
    WorkerOptions {
        name: ACTION_WORKER_NAME.to_owned(),
        description: ACTION_WORKER_DESCRIPTION.to_owned(),
        interval: ACTION_WORKER_INTERVAL,
        heartbeat_interval: ACTION_WORKER_HEARTBEAT_INTERVAL,
        num_handlers: ACTION_NUM_HANDLERS,
        worker_hostname,
    }
}

/// `newTriggerQueryRunner`: the routine around the real trigger store.
pub fn new_trigger_query_runner<H: DbWorkerHandler<TriggerJobStore>>(
    store: Arc<TriggerJobStore>,
    handler: Arc<H>,
    concurrency: i64,
    worker_hostname: String,
) -> WorkerRoutine<TriggerJobStore, H> {
    WorkerRoutine::new(
        store,
        handler,
        trigger_worker_options(concurrency, worker_hostname),
    )
}

/// `newActionRunner`: the routine around the real action store.
pub fn new_action_runner<H: DbWorkerHandler<ActionJobStore>>(
    store: Arc<ActionJobStore>,
    handler: Arc<H>,
    worker_hostname: String,
) -> WorkerRoutine<ActionJobStore, H> {
    WorkerRoutine::new(store, handler, action_worker_options(worker_hostname))
}

/// `newTriggerQueryResetter`.
pub fn new_trigger_query_resetter(store: Arc<TriggerJobStore>) -> ResetterRoutine {
    ResetterRoutine::new(
        store as Arc<dyn DbResetter>,
        ResetterOptions {
            name: TRIGGER_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    )
}

/// `newActionJobResetter`.
pub fn new_action_job_resetter(store: Arc<ActionJobStore>) -> ResetterRoutine {
    ResetterRoutine::new(
        store as Arc<dyn DbResetter>,
        ResetterOptions {
            name: ACTION_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    )
}

impl DbRecord for TriggerJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

impl DbRecord for ActionJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

/// `createDBWorkerStoreForTriggerJobs` over `cm_trigger_jobs`.
#[derive(Clone)]
pub struct TriggerJobStore {
    pool: PgPool,
}

impl TriggerJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `workerutil/dbworker/store.(*store[T]).Dequeue` for the trigger store
    /// (`ORDER BY id`, `RetryAfter` 10s). The column expressions are
    /// `database.TriggerJobsColumns`; the columns updated by dequeue (`state`,
    /// `started_at`, `last_heartbeat_at`, `finished_at`, `failure_message`,
    /// `execution_logs`, `worker_hostname`) are projected as the dequeue
    /// values, exactly like the generic store's select substitution.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<TriggerJob>, String> {
        let row = sqlx::query(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY id) AS candidate_order
	FROM cm_trigger_jobs
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
	ORDER BY id
	LIMIT 50
),
candidate AS (
	SELECT id FROM cm_trigger_jobs
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF cm_trigger_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE cm_trigger_jobs
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
	id,
	query,
	query_string,
	search_results,
	'processing' AS state,
	NULL AS failure_message,
	$1 AS started_at,
	NULL AS finished_at,
	process_after,
	num_resets,
	num_failures
FROM cm_trigger_jobs
WHERE id IN (SELECT id FROM candidate)
"#,
        )
        .bind(now)
        .bind(RETRY_AFTER.as_secs() as i64)
        .bind(worker_hostname)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        row.map(scan_trigger_job).transpose()
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
	FROM cm_trigger_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE cm_trigger_jobs
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

        heartbeat_result(rows)
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE cm_trigger_jobs
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
UPDATE cm_trigger_jobs
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
UPDATE cm_trigger_jobs
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

    /// `Store.ResetStalled` (`StalledMaxAge` 60s, `MaxNumResets` 0).
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM cm_trigger_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE cm_trigger_jobs
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
	SELECT id FROM cm_trigger_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE cm_trigger_jobs
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

/// `createDBWorkerStoreForActionJobs` over `cm_action_jobs`.
#[derive(Clone)]
pub struct ActionJobStore {
    pool: PgPool,
}

impl ActionJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The action counterpart of [`TriggerJobStore::dequeue`] with
    /// `database.ActionJobColumns`.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ActionJob>, String> {
        let row = sqlx::query(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY id) AS candidate_order
	FROM cm_action_jobs
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
	ORDER BY id
	LIMIT 50
),
candidate AS (
	SELECT id FROM cm_action_jobs
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF cm_action_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE cm_action_jobs
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
	id,
	email,
	webhook,
	slack_webhook,
	trigger_event,
	'processing' AS state,
	NULL AS failure_message,
	$1 AS started_at,
	NULL AS finished_at,
	process_after,
	num_resets,
	num_failures,
	log_contents
FROM cm_action_jobs
WHERE id IN (SELECT id FROM candidate)
"#,
        )
        .bind(now)
        .bind(RETRY_AFTER.as_secs() as i64)
        .bind(worker_hostname)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        row.map(scan_action_job).transpose()
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
	FROM cm_action_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE cm_action_jobs
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

        heartbeat_result(rows)
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        let result = sqlx::query(
            r#"
UPDATE cm_action_jobs
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
UPDATE cm_action_jobs
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
UPDATE cm_action_jobs
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

    /// `Store.ResetStalled` (`StalledMaxAge` 60s, `MaxNumResets` 0).
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM cm_action_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE cm_action_jobs
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
	SELECT id FROM cm_action_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE cm_action_jobs
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

fn scan_trigger_job(row: sqlx::postgres::PgRow) -> Result<TriggerJob, String> {
    Ok(TriggerJob {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        query: row.try_get("query").map_err(|err| err.to_string())?,
        query_string: row.try_get("query_string").map_err(|err| err.to_string())?,
        search_results: row
            .try_get("search_results")
            .map_err(|err| err.to_string())?,
        state: row.try_get("state").map_err(|err| err.to_string())?,
        failure_message: row
            .try_get("failure_message")
            .map_err(|err| err.to_string())?,
        started_at: row.try_get("started_at").map_err(|err| err.to_string())?,
        finished_at: row.try_get("finished_at").map_err(|err| err.to_string())?,
        process_after: row
            .try_get("process_after")
            .map_err(|err| err.to_string())?,
        num_resets: row.try_get("num_resets").map_err(|err| err.to_string())?,
        num_failures: row.try_get("num_failures").map_err(|err| err.to_string())?,
    })
}

fn scan_action_job(row: sqlx::postgres::PgRow) -> Result<ActionJob, String> {
    Ok(ActionJob {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        email: row.try_get("email").map_err(|err| err.to_string())?,
        webhook: row.try_get("webhook").map_err(|err| err.to_string())?,
        slack_webhook: row
            .try_get("slack_webhook")
            .map_err(|err| err.to_string())?,
        trigger_event: row
            .try_get("trigger_event")
            .map_err(|err| err.to_string())?,
        state: row.try_get("state").map_err(|err| err.to_string())?,
        failure_message: row
            .try_get("failure_message")
            .map_err(|err| err.to_string())?,
        started_at: row.try_get("started_at").map_err(|err| err.to_string())?,
        finished_at: row.try_get("finished_at").map_err(|err| err.to_string())?,
        process_after: row
            .try_get("process_after")
            .map_err(|err| err.to_string())?,
        num_resets: row.try_get("num_resets").map_err(|err| err.to_string())?,
        num_failures: row.try_get("num_failures").map_err(|err| err.to_string())?,
        log_contents: row.try_get("log_contents").map_err(|err| err.to_string())?,
    })
}

fn heartbeat_result(rows: Vec<sqlx::postgres::PgRow>) -> Result<(Vec<i32>, Vec<i32>), String> {
    let mut known = Vec::with_capacity(rows.len());
    let mut canceled = Vec::new();
    for row in rows {
        let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
        let cancel: bool = row.try_get("cancel").map_err(|err| err.to_string())?;
        known.push(id);
        if cancel {
            canceled.push(id);
        }
    }
    Ok((known, canceled))
}

fn heartbeat_ages(
    now: OffsetDateTime,
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<(i32, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
        let last_heartbeat: Option<OffsetDateTime> = row
            .try_get("last_heartbeat_at")
            .map_err(|err| err.to_string())?;
        let age = match last_heartbeat {
            Some(last) => Duration::try_from(now - last).unwrap_or(Duration::ZERO),
            None => Duration::ZERO,
        };
        ages.push((id, age));
    }
    Ok(ages)
}

fn parse_ids(ids: &[String]) -> Result<Vec<i32>, String> {
    let mut parsed = Vec::with_capacity(ids.len());
    for id in ids {
        parsed.push(
            id.parse::<i32>()
                .map_err(|err| format!("invalid record id {id:?}: {err}"))?,
        );
    }
    Ok(parsed)
}

fn string_ids(ids: Vec<i32>) -> Vec<String> {
    ids.into_iter().map(|id| id.to_string()).collect()
}

#[async_trait]
impl DbWorkerStore for TriggerJobStore {
    type Record = TriggerJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        TriggerJobStore::dequeue(self, worker_hostname, OffsetDateTime::now_utc()).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed = parse_ids(ids)?;
        let (known, canceled) =
            TriggerJobStore::heartbeat(self, &parsed, OffsetDateTime::now_utc()).await?;
        Ok((string_ids(known), string_ids(canceled)))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        TriggerJobStore::mark_complete(self, record.id).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        TriggerJobStore::mark_errored(self, record.id, failure_message).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        TriggerJobStore::mark_failed(self, record.id, failure_message).await
    }
}

#[async_trait]
impl DbWorkerStore for ActionJobStore {
    type Record = ActionJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        ActionJobStore::dequeue(self, worker_hostname, OffsetDateTime::now_utc()).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed = parse_ids(ids)?;
        let (known, canceled) =
            ActionJobStore::heartbeat(self, &parsed, OffsetDateTime::now_utc()).await?;
        Ok((string_ids(known), string_ids(canceled)))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        ActionJobStore::mark_complete(self, record.id).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        ActionJobStore::mark_errored(self, record.id, failure_message).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        ActionJobStore::mark_failed(self, record.id, failure_message).await
    }
}

#[async_trait]
impl DbResetter for TriggerJobStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) =
            TriggerJobStore::reset_stalled_at(self, OffsetDateTime::now_utc()).await?;
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

#[async_trait]
impl DbResetter for ActionJobStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) =
            ActionJobStore::reset_stalled_at(self, OffsetDateTime::now_utc()).await?;
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
    fn worker_options_match_baseline() {
        assert_eq!(TRIGGER_WORKER_NAME, "code_monitors_trigger_jobs_worker");
        assert_eq!(TRIGGER_WORKER_INTERVAL, Duration::from_secs(5));
        assert_eq!(TRIGGER_WORKER_HEARTBEAT_INTERVAL, Duration::from_secs(15));
        assert_eq!(TRIGGER_MAXIMUM_RUNTIME_PER_JOB, Duration::from_secs(60));
        assert_eq!(ACTION_WORKER_NAME, "code_monitors_action_jobs_worker");
        assert_eq!(ACTION_NUM_HANDLERS, 1);
        assert_eq!(RESETTER_INTERVAL, Duration::from_secs(60));
        assert_eq!(STALLED_MAX_AGE, Duration::from_secs(60));
        assert_eq!(RETRY_AFTER, Duration::from_secs(10));
        assert_eq!(MAX_NUM_RETRIES, 3);
        assert_eq!(MAX_NUM_RESETS, 0);
    }

    #[test]
    fn trigger_options_use_the_resolved_concurrency() {
        let options = trigger_worker_options(7, "h".to_owned());
        assert_eq!(options.num_handlers, 7);
        assert_eq!(options.worker_hostname, "h");
        assert_eq!(options.description, TRIGGER_WORKER_DESCRIPTION);
    }

    #[test]
    fn concurrency_falls_back_to_four() {
        let default = SiteConfiguration::default();
        assert_eq!(code_monitors_concurrency(&default), DEFAULT_CONCURRENCY);

        let configured =
            SiteConfiguration::parse(r#"{"codeMonitors": {"concurrency": 7}}"#).unwrap();
        assert_eq!(code_monitors_concurrency(&configured), 7);

        let zero = SiteConfiguration::parse(r#"{"codeMonitors": {"concurrency": 0}}"#).unwrap();
        assert_eq!(code_monitors_concurrency(&zero), DEFAULT_CONCURRENCY);
    }
}
