#![forbid(unsafe_code)]

//! The dbworker-store half of the `codeintel-autoindexing-dependency-scheduler`
//! job: `internal/workerutil/dbworker/store` (`Dequeue`, `Heartbeat`,
//! `Requeue`, `MarkComplete`, `MarkErrored`, `MarkFailed`, `ResetStalled`)
//! applied to the two dependency queues.
//!
//! The two queue stores are configured in
//! `internal/codeintel/autoindexing/internal/background/dependencies/utils.go`:
//!
//! | store | table | order by | select columns |
//! |---|---|---|---|
//! | `DependencySyncingJobWorkerStoreOptions` | `lsif_dependency_syncing_jobs` | `queued_at, upload_id` | 9 |
//! | `DependencyIndexingJobWorkerStoreOptions` | `lsif_dependency_indexing_jobs` | `queued_at, upload_id` | 11 |
//!
//! Neither store sets `ViewName`, `RetryAfter` or `MaxNumRetries`, so:
//!
//! * `Dequeue` reads from the table itself, only picks `state = 'queued'` rows
//!   whose `process_after` is null or past, and keeps the (always false)
//!   errored-retry branch of the baseline query;
//! * `MarkErrored`'s `num_failures + 1 >= MaxNumRetries` is `… >= 0`, so an
//!   errored record becomes `failed` unless it is flagged `cancel`;
//! * the `lsif_indexes` resetter (`precise_code_intel_index_worker_resetter`)
//!   only needs `ResetStalled`, which always updates the base table.
//!
//! Observation spans and per-worker metrics are omitted; migrations stay owned
//! by the Go `migrator`.
//!
//! Deviations from the baseline are documented in `docs/modules/06_worker.md`
//! §129: timestamps are returned as `NULL`/non-`NULL` by the differential
//! harness, and the nullable `upload_id` / `external_service_sync` columns are
//! modelled as `Option` (the baseline scan errors on `NULL`).

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `DependencySyncingJobWorkerStoreOptions.TableName`.
pub const SYNCING_JOBS_TABLE: &str = "lsif_dependency_syncing_jobs";
/// `DependencyIndexingJobWorkerStoreOptions.TableName`.
pub const INDEXING_JOBS_TABLE: &str = "lsif_dependency_indexing_jobs";
/// `IndexWorkerStoreOptions.TableName` (used by the index resetter only).
pub const LSIF_INDEXES_TABLE: &str = "lsif_indexes";

/// `stalledDependencySyncingJobMaxAge` / `stalledDependencyIndexingJobMaxAge`
/// / `stalledIndexMaxAge`: all three are 25 seconds.
pub const STALLED_MAX_AGE_SECONDS: i32 = 25;
/// `dependencySyncingJobMaxNumResets` / `dependencyIndexingJobMaxNumResets` /
/// `indexMaxNumResets`: all three are 3.
pub const MAX_NUM_RESETS: i32 = 3;
/// `Options.MaxNumRetries` is unset (0) for both dependency queues.
pub const MAX_NUM_RETRIES: i32 = 0;
/// `defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// A `lsif_dependency_syncing_jobs` row as `scanDependencySyncingJob` reads it
/// after the dequeue rewrites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencySyncingJobRow {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub upload_id: Option<i32>,
}

/// A `lsif_dependency_indexing_jobs` row as `scanDependencyIndexingJob` reads
/// it after the dequeue rewrites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyIndexingJobRow {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub upload_id: Option<i32>,
    pub external_service_kind: String,
    pub external_service_sync: Option<OffsetDateTime>,
}

/// The shared `dequeueQuery` body. `select` is the column expression list with
/// the dequeue-updated columns already substituted (the baseline's
/// `makeDequeueSelectExpressions`), `order_by` is the store's
/// `OrderByExpression`, and `table` is both `ViewName` and `TableName`.
const DEQUEUE_QUERY: &str = "
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY {order_by}) AS order
	FROM {table}
	WHERE
		(
			(
				state = 'queued' AND
				(process_after IS NULL OR process_after <= $1)
			) OR (
				0 > 0 AND
				state = 'errored' AND
				$1 - finished_at > (0 * '1 second'::interval)
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
		-- Recheck state.
		state IN ('queued', 'errored')
	ORDER BY pc.order
	FOR UPDATE OF {table} SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE
		{table}
	SET
		state='processing', started_at=$1::timestamp, last_heartbeat_at=$1::timestamp, finished_at=NULL, failure_message=NULL, execution_logs=NULL, worker_hostname=$2
	WHERE
		id IN (SELECT id FROM candidate)
)
SELECT
	{select}
FROM
	{table}
WHERE
	id IN (SELECT id FROM candidate)
";

/// `dependencySyncingJobColumns` with the dequeue-updated expressions
/// substituted (state/started_at/finished_at/failure_message). The baseline
/// returns these positionally; the Rust layer reads rows by name, so the
/// substituted expressions carry the column's name as an alias and `NULL`s
/// carry the column type (same values, decodable by `sqlx`).
const SYNCING_JOB_SELECT: &str = "lsif_dependency_syncing_jobs.id, 'processing' AS state, NULL::text AS failure_message, $1 AS started_at, NULL::timestamptz AS finished_at, lsif_dependency_syncing_jobs.process_after, lsif_dependency_syncing_jobs.num_resets, lsif_dependency_syncing_jobs.num_failures, lsif_dependency_syncing_jobs.upload_id";

/// `dependencyIndexingJobColumns` with the same substitutions.
const INDEXING_JOB_SELECT: &str = "lsif_dependency_indexing_jobs.id, 'processing' AS state, NULL::text AS failure_message, $1 AS started_at, NULL::timestamptz AS finished_at, lsif_dependency_indexing_jobs.process_after, lsif_dependency_indexing_jobs.num_resets, lsif_dependency_indexing_jobs.num_failures, lsif_dependency_indexing_jobs.upload_id, lsif_dependency_indexing_jobs.external_service_kind, lsif_dependency_indexing_jobs.external_service_sync";

const HEARTBEAT_QUERY: &str = "
WITH alive_candidates AS (
	SELECT
		id
	FROM
		{table}
	WHERE
		id = ANY ($1::text[]::int[]) AND state = 'processing'
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
";

const REQUEUE_QUERY: &str = "
UPDATE {table}
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	process_after = $2,
	cancel = false
WHERE id = $1
";

const MARK_COMPLETE_QUERY: &str = "
UPDATE {table}
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
RETURNING id
";

/// `markErroredQuery`; `{max_num_retries}` is the baseline's `%d` literal.
const MARK_ERRORED_QUERY: &str = "
UPDATE {table}
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= {max_num_retries} THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
";

const MARK_FAILED_QUERY: &str = "
UPDATE {table}
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
";

const RESET_STALLED_QUERY: &str = "
WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id::bigint AS id, last_heartbeat_at
";

const RESET_STALLED_MAX_RESETS_QUERY: &str = "
WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id::bigint AS id, last_heartbeat_at
";

fn render(query: &str, table: &str, select: Option<&str>) -> String {
    let mut sql = query.replace("{table}", table);
    if let Some(select) = select {
        sql = sql.replace("{order_by}", "queued_at, upload_id");
        sql = sql.replace("{select}", select);
    }
    sql
}

/// `(*store[DependencySyncingJob]).Dequeue`: returns `None` when the queue has
/// no eligible record.
pub async fn dequeue_syncing_job(
    pool: &PgPool,
    now: OffsetDateTime,
    worker_hostname: &str,
) -> Result<Option<DependencySyncingJobRow>, sqlx::Error> {
    let sql = render(DEQUEUE_QUERY, SYNCING_JOBS_TABLE, Some(SYNCING_JOB_SELECT));
    let rows = sqlx::query(&sql)
        .bind(now)
        .bind(worker_hostname)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().next().map(|row| DependencySyncingJobRow {
        id: row.get("id"),
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        process_after: row.get("process_after"),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
        upload_id: row.get("upload_id"),
    }))
}

/// `(*store[DependencyIndexingJob]).Dequeue`.
pub async fn dequeue_indexing_job(
    pool: &PgPool,
    now: OffsetDateTime,
    worker_hostname: &str,
) -> Result<Option<DependencyIndexingJobRow>, sqlx::Error> {
    let sql = render(
        DEQUEUE_QUERY,
        INDEXING_JOBS_TABLE,
        Some(INDEXING_JOB_SELECT),
    );
    let rows = sqlx::query(&sql)
        .bind(now)
        .bind(worker_hostname)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().next().map(|row| DependencyIndexingJobRow {
        id: row.get("id"),
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        process_after: row.get("process_after"),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
        upload_id: row.get("upload_id"),
        external_service_kind: row.get("external_service_kind"),
        external_service_sync: row.get("external_service_sync"),
    }))
}

/// `(*store[T]).Heartbeat`: `(known, canceled)` record ids.
pub async fn heartbeat(
    pool: &PgPool,
    table: &str,
    now: OffsetDateTime,
    ids: &[String],
) -> Result<(Vec<i32>, Vec<i32>), sqlx::Error> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let sql = render(HEARTBEAT_QUERY, table, None);
    let rows = sqlx::query(&sql)
        .bind(ids)
        .bind(now)
        .fetch_all(pool)
        .await?;
    let mut known = Vec::with_capacity(rows.len());
    let mut canceled = Vec::new();
    for row in rows {
        let id: i32 = row.get("id");
        known.push(id);
        if row.get::<bool, _>("cancel") {
            canceled.push(id);
        }
    }
    Ok((known, canceled))
}

/// `(*store[T]).Requeue`.
pub async fn requeue(
    pool: &PgPool,
    table: &str,
    id: i32,
    after: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let sql = render(REQUEUE_QUERY, table, None);
    sqlx::query(&sql).bind(id).bind(after).execute(pool).await?;
    Ok(())
}

/// `(*store[T]).MarkComplete`: `true` when the row was still processing.
pub async fn mark_complete(pool: &PgPool, table: &str, id: i32) -> Result<bool, sqlx::Error> {
    let sql = render(MARK_COMPLETE_QUERY, table, None);
    let row = sqlx::query(&sql).bind(id).fetch_optional(pool).await?;
    Ok(row.is_some())
}

/// `(*store[T]).MarkErrored`.
pub async fn mark_errored(
    pool: &PgPool,
    table: &str,
    id: i32,
    failure_message: &str,
    max_num_retries: i32,
) -> Result<bool, sqlx::Error> {
    let sql = render(MARK_ERRORED_QUERY, table, None)
        .replace("{max_num_retries}", &max_num_retries.to_string());
    let row = sqlx::query(&sql)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[T]).MarkFailed`.
pub async fn mark_failed(
    pool: &PgPool,
    table: &str,
    id: i32,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let sql = render(MARK_FAILED_QUERY, table, None);
    let row = sqlx::query(&sql)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[T]).ResetStalled`: returns, for each group, the record id and the
/// age of its last heartbeat computed from `now`.
pub async fn reset_stalled(
    pool: &PgPool,
    table: &str,
    now: OffsetDateTime,
    stalled_max_age_seconds: i32,
    max_num_resets: i32,
    reset_failure_message: &str,
) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), sqlx::Error> {
    let reset_sql = render(RESET_STALLED_QUERY, table, None);
    let reset_rows = sqlx::query(&reset_sql)
        .bind(now)
        .bind(stalled_max_age_seconds)
        .bind(max_num_resets)
        .fetch_all(pool)
        .await?;

    let failed_sql = render(RESET_STALLED_MAX_RESETS_QUERY, table, None);
    let failed_rows = sqlx::query(&failed_sql)
        .bind(now)
        .bind(stalled_max_age_seconds)
        .bind(max_num_resets)
        .bind(reset_failure_message)
        .fetch_all(pool)
        .await?;

    Ok((
        rows_to_ages(reset_rows, now),
        rows_to_ages(failed_rows, now),
    ))
}

fn rows_to_ages(rows: Vec<sqlx::postgres::PgRow>, now: OffsetDateTime) -> Vec<(i64, Duration)> {
    rows.into_iter()
        .map(|row| {
            // The reset queries RETURNING `id::bigint`: `lsif_indexes.id` is
            // BIGINT while the dependency queues use INTEGER, and `sqlx`
            // will not widen INT4 into `i64` on its own.
            let id: i64 = row.get("id");
            let last_heartbeat: Option<OffsetDateTime> = row.get("last_heartbeat_at");
            let age = last_heartbeat
                .map(|last| Duration::from_secs_f64((now - last).as_seconds_f64()))
                .unwrap_or_default();
            (id, age)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dequeue_queries_target_the_baseline_tables_and_columns() {
        let syncing = render(DEQUEUE_QUERY, SYNCING_JOBS_TABLE, Some(SYNCING_JOB_SELECT));
        assert!(syncing.contains("FROM lsif_dependency_syncing_jobs"));
        assert!(syncing.contains("FOR UPDATE OF lsif_dependency_syncing_jobs SKIP LOCKED"));
        assert!(syncing.contains("ORDER BY queued_at, upload_id"));
        assert!(syncing.contains("worker_hostname=$2"));
        assert!(syncing.contains("lsif_dependency_syncing_jobs.upload_id"));

        let indexing = render(
            DEQUEUE_QUERY,
            INDEXING_JOBS_TABLE,
            Some(INDEXING_JOB_SELECT),
        );
        assert!(indexing.contains("FROM lsif_dependency_indexing_jobs"));
        assert!(indexing.contains("lsif_dependency_indexing_jobs.external_service_kind"));
        assert!(indexing.contains("lsif_dependency_indexing_jobs.external_service_sync"));
    }

    #[test]
    fn mark_errored_uses_the_baseline_max_num_retries_literal() {
        let sql = render(MARK_ERRORED_QUERY, SYNCING_JOBS_TABLE, None)
            .replace("{max_num_retries}", &MAX_NUM_RETRIES.to_string());
        assert!(sql.contains("num_failures + 1 >= 0"));
        assert!(sql.contains("THEN 'failed'"));
    }

    #[test]
    fn reset_queries_match_the_baseline_thresholds() {
        let reset = render(RESET_STALLED_QUERY, LSIF_INDEXES_TABLE, None);
        assert!(reset.contains("num_resets < $3"));
        assert!(reset.contains("num_resets = num_resets + 1"));
        let failed = render(RESET_STALLED_MAX_RESETS_QUERY, LSIF_INDEXES_TABLE, None);
        assert!(failed.contains("num_resets >= $3"));
        assert!(failed.contains("state = 'failed'"));
    }

    #[test]
    fn heartbeat_parses_text_ids_against_the_integer_column() {
        let sql = render(HEARTBEAT_QUERY, INDEXING_JOBS_TABLE, None);
        assert!(sql.contains("id = ANY ($1::text[]::int[])"));
        assert!(sql.contains("RETURNING id, cancel"));
    }
}
