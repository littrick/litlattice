#![forbid(unsafe_code)]

//! Store operations used by the `batches-janitor` worker job
//! (`cmd/worker/internal/batches`):
//!
//! * the four dbworker `ResetStalled` queries (reconciler, bulk operation,
//!   workspace execution and workspace resolution queues), each with its own
//!   table, state column, `StalledMaxAge` and `MaxNumResets`;
//! * `DeleteUnattachedExpiredChangesetSpecs`, `DeleteExpiredChangesetSpecs`,
//!   `DeleteExpiredBatchSpecs` (`internal/batches/store`);
//! * `CleanBatchSpecExecutionCacheEntries` and `CleanDetachedChangesets`.
//!
//! Migrations stay owned by the Go `migrator`; these statements only delete
//! rows and reset queue state, and every SQL string is copied from the
//! baseline store.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `batches/types.BatchSpecTTL` (7 days).
pub const BATCH_SPEC_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// `batches/types.ChangesetSpecTTL` (2 days).
pub const CHANGESET_SPEC_TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);

/// `batches/types.CurrentCacheVersion`.
pub const CURRENT_CACHE_VERSION: i32 = 2;

/// `store.defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// The subset of `dbworkerstore.Options` that `ResetStalled` reads.
#[derive(Debug, Clone, Copy)]
pub struct ResetWorkerOptions {
    /// `Options.TableName`. `ResetStalled` uses the table, never `ViewName`.
    pub table: &'static str,
    /// `{state}`, after `AlternateColumnNames` substitution.
    pub state_column: &'static str,
    /// `Options.StalledMaxAge`.
    pub stalled_max_age: Duration,
    /// `Options.MaxNumResets`.
    pub max_num_resets: i32,
}

/// `reconcilerWorkerStoreOpts`.
pub const RECONCILER_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "changesets",
    state_column: "reconciler_state",
    stalled_max_age: Duration::from_secs(60),
    max_num_resets: 10,
};

/// `bulkOperationWorkerStoreOpts`.
pub const BULK_OPERATION_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "changeset_jobs",
    state_column: "state",
    stalled_max_age: Duration::from_secs(60),
    max_num_resets: 60,
};

/// `batchSpecWorkspaceExecutionWorkerStoreOptions`.
pub const WORKSPACE_EXECUTION_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "batch_spec_workspace_execution_jobs",
    state_column: "state",
    stalled_max_age: Duration::from_secs(25),
    max_num_resets: 3,
};

/// `batchSpecResolutionWorkerOpts`.
pub const BATCH_SPEC_RESOLUTION_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "batch_spec_resolution_jobs",
    state_column: "state",
    stalled_max_age: Duration::from_secs(60),
    max_num_resets: 60,
};

/// `(*store[T]).ResetStalled`.
///
/// Returns `(reset, failed)` as `(id, age-since-last-heartbeat)` pairs, sorted
/// by id. The two statements run in sequence, exactly like the baseline: the
/// first re-queues records below `MaxNumResets`, the second marks records at
/// or above it as `failed` with the default reset failure message.
pub async fn reset_stalled(
    pool: &PgPool,
    options: ResetWorkerOptions,
    now: OffsetDateTime,
) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
    let stalled_secs = options.stalled_max_age.as_secs() as i32;
    let state = options.state_column;
    let table = options.table;

    let reset_query = format!(
        r#"
WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		{state} = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	{state} = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#
    );
    let failed_query = format!(
        r#"
WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		{state} = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	{state} = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#
    );

    let reset = sqlx::query(&reset_query)
        .bind(now)
        .bind(stalled_secs)
        .bind(options.max_num_resets)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    let failed = sqlx::query(&failed_query)
        .bind(now)
        .bind(stalled_secs)
        .bind(options.max_num_resets)
        .bind(DEFAULT_RESET_FAILURE_MESSAGE)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    Ok((scan_ages(reset, now)?, scan_ages(failed, now)?))
}

fn scan_ages(
    rows: Vec<sqlx::postgres::PgRow>,
    now: OffsetDateTime,
) -> Result<Vec<(i64, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        // `dbworkerstore.Store.ResetStalled` decodes the id as Go `int`; the
        // queue tables disagree on the SQL type (`changesets.id` is BIGINT,
        // `repo_embedding_jobs.id` is INT4), so accept either.
        let id = match row.try_get::<i64, _>("id") {
            Ok(id) => id,
            Err(_) => i64::from(row.try_get::<i32, _>("id").map_err(|err| err.to_string())?),
        };
        let last_heartbeat: OffsetDateTime = row
            .try_get("last_heartbeat_at")
            .map_err(|err| err.to_string())?;
        let age = now - last_heartbeat;
        ages.push((id, age.try_into().unwrap_or(Duration::ZERO)));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

/// `Store.DeleteUnattachedExpiredChangesetSpecs`.
pub async fn delete_unattached_expired_changeset_specs(
    pool: &PgPool,
    now: OffsetDateTime,
) -> Result<(), String> {
    let expiration = now - CHANGESET_SPEC_TTL;
    sqlx::query(
        r#"
DELETE FROM
  changeset_specs
WHERE
  -- The spec is older than the ChangesetSpecTTL
  created_at < $1
  AND
  -- and it was never attached to a batch_spec
  batch_spec_id IS NULL
"#,
    )
    .bind(expiration)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `Store.DeleteExpiredChangesetSpecs`.
pub async fn delete_expired_changeset_specs(
    pool: &PgPool,
    now: OffsetDateTime,
) -> Result<(), String> {
    let expiration = now - BATCH_SPEC_TTL;
    sqlx::query(
        r#"
WITH candidates AS (
	SELECT cs.id
	FROM changeset_specs cs
	JOIN batch_specs bs ON bs.id = cs.batch_spec_id
	LEFT JOIN batch_changes bc ON bs.id = bc.batch_spec_id
	LEFT JOIN changesets c ON (c.current_spec_id = cs.id OR c.previous_spec_id = cs.id)
	WHERE
		-- The spec is older than the BatchSpecTTL
		cs.created_at < $1
		-- and it is not created by SSBC
		AND NOT bs.created_from_raw
		-- and the batch spec it is attached to is not applied to a batch change
		AND bc.id IS NULL
		-- and it is not attached to a changeset
		AND c.id IS NULL
	FOR UPDATE OF cs
)
DELETE FROM changeset_specs
WHERE
	id IN (SELECT id FROM candidates)
"#,
    )
    .bind(expiration)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `Store.DeleteExpiredBatchSpecs`.
pub async fn delete_expired_batch_specs(pool: &PgPool, now: OffsetDateTime) -> Result<(), String> {
    let expiration = now - BATCH_SPEC_TTL;
    sqlx::query(
        r#"
DELETE FROM
  batch_specs
WHERE
  created_at < $1
AND NOT EXISTS (
  SELECT 1 FROM batch_changes WHERE batch_spec_id = batch_specs.id
)
-- Only delete expired batch specs that have been created by src-cli
AND NOT created_from_raw
AND NOT EXISTS (
  SELECT 1 FROM changeset_specs WHERE batch_spec_id = batch_specs.id
)
"#,
    )
    .bind(expiration)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `Store.CleanBatchSpecExecutionCacheEntries`.
pub async fn clean_batch_spec_execution_cache_entries(
    pool: &PgPool,
    max_cache_size: i64,
) -> Result<(), String> {
    sqlx::query(
        r#"
WITH total_size AS (
  SELECT sum(octet_length(value)) AS total FROM batch_spec_execution_cache_entries
),
candidates AS (
  SELECT
    id
  FROM (
    SELECT
      entries.id,
      entries.created_at,
      entries.last_used_at,
      SUM(octet_length(entries.value)) OVER (ORDER BY COALESCE(entries.last_used_at, entries.created_at) ASC, entries.id ASC) AS running_size
    FROM batch_spec_execution_cache_entries entries
  ) t
  WHERE
    ((SELECT total FROM total_size) - t.running_size) >= $1
),
outdated AS (
	SELECT
		id
	FROM batch_spec_execution_cache_entries
	WHERE
		version < $2
),
ids AS (
	SELECT id FROM outdated
	UNION ALL
	SELECT id FROM candidates
)
DELETE FROM batch_spec_execution_cache_entries WHERE id IN (SELECT id FROM ids)
"#,
    )
    .bind(max_cache_size)
    .bind(CURRENT_CACHE_VERSION)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `Store.CleanDetachedChangesets`.
///
/// The baseline divides the retention by `time.Second` before sending it, so
/// sub-second precision is dropped; the caller passes the same integer.
pub async fn clean_detached_changesets(
    pool: &PgPool,
    retention_seconds: i64,
) -> Result<(), String> {
    sqlx::query("DELETE FROM changesets WHERE detached_at < (NOW() - ($1 * interval '1 second'));")
        .bind(retention_seconds)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_options_match_the_baseline() {
        assert_eq!(RECONCILER_WORKER.table, "changesets");
        assert_eq!(RECONCILER_WORKER.state_column, "reconciler_state");
        assert_eq!(RECONCILER_WORKER.stalled_max_age, Duration::from_secs(60));
        assert_eq!(RECONCILER_WORKER.max_num_resets, 10);

        assert_eq!(BULK_OPERATION_WORKER.table, "changeset_jobs");
        assert_eq!(BULK_OPERATION_WORKER.max_num_resets, 60);

        assert_eq!(
            WORKSPACE_EXECUTION_WORKER.stalled_max_age,
            Duration::from_secs(25)
        );
        assert_eq!(WORKSPACE_EXECUTION_WORKER.max_num_resets, 3);

        assert_eq!(BATCH_SPEC_RESOLUTION_WORKER.max_num_resets, 60);
        assert_eq!(BATCH_SPEC_TTL, Duration::from_secs(7 * 24 * 3600));
        assert_eq!(CHANGESET_SPEC_TTL, Duration::from_secs(2 * 24 * 3600));
        assert_eq!(CURRENT_CACHE_VERSION, 2);
    }
}
