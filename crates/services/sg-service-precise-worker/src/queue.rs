//! `lsif_uploads` worker queue store.
//!
//! This is the `dbworker/store` implementation for the precise-code-intel
//! upload processor, configured exactly like
//! `internal/codeintel/uploads/internal/store/uploads.go`'s
//! `UploadWorkerStoreOptions`:
//!
//! - table `lsif_uploads`, view `lsif_uploads_with_repository_name u`
//! - `StalledMaxAge = 25s`, `MaxNumResets = 3`
//! - `RetryAfter = 0`, `MaxNumRetries = 0` (errored records are never
//!   dequeued again; a handler error always lands in `failed`)
//! - `ORDER BY u.associated_index_id IS NULL DESC,
//!   COALESCE(u.process_after, u.uploaded_at), u.id`
//!
//! The queue lives in the **frontend** database (`db.Handle()` in the
//! baseline), not the code-intel database.
//!
//! Later slices add the worker loop and the SCIP handler; this module exposes
//! the same operations `dbworker/store.Store` does so both can share it.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `stalledUploadMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(25);
/// `uploadMaxNumResets`.
pub const MAX_NUM_RESETS: i32 = 3;
/// `store.defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// The columns selected by `uploadColumnsWithNullRank` (in order), scanned
/// into [`Upload`] the way `scanCompleteUpload` does.
#[derive(Debug, Clone, PartialEq)]
pub struct Upload {
    pub id: i32,
    pub commit: String,
    pub root: String,
    pub visible_at_tip: bool,
    pub uploaded_at: OffsetDateTime,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub repository_id: i32,
    pub repository_name: String,
    pub indexer: String,
    /// `dbutil.NullString`: empty when the column is NULL.
    pub indexer_version: String,
    pub num_parts: i32,
    pub uploaded_parts: Vec<i32>,
    pub upload_size: Option<i64>,
    /// `*int` in the baseline; the column is `bigint`.
    pub associated_index_id: Option<i64>,
    pub content_type: String,
    pub should_reindex: bool,
    /// Always NULL (`uploadColumnsWithNullRank` appends a literal `NULL`).
    pub rank: Option<i32>,
    pub uncompressed_size: Option<i64>,
}

impl Upload {
    pub fn record_id(&self) -> i32 {
        self.id
    }
}

/// Result of [`UploadQueueStore::heartbeat`]: ids still known to the database
/// and the subset flagged for cancellation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeartbeatResult {
    pub known_ids: Vec<i32>,
    pub cancel_ids: Vec<i32>,
}

/// Result of [`UploadQueueStore::reset_stalled`]. The baseline returns maps
/// from record id to the age of the last heartbeat; this keeps the same pairs
/// sorted by id so callers and tests are deterministic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetStalledResult {
    /// Records moved back to `queued`.
    pub reset: Vec<(i32, Duration)>,
    /// Records that reached `MaxNumResets` and were marked `failed`.
    pub failed: Vec<(i32, Duration)>,
}

#[derive(Clone)]
pub struct UploadQueueStore {
    pool: PgPool,
}

impl UploadQueueStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `dbworker/store.Store.Dequeue` with `UploadWorkerStoreOptions`.
    ///
    /// Returns `Ok(None)` when there is no dequeueable record. `RetryAfter`
    /// is zero, so only `queued` records are candidates; the `errored` branch
    /// is kept for parity with the generic store (it is disabled by the
    /// `$2 > 0` guard).
    pub async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Upload>, sqlx::Error> {
        let now = OffsetDateTime::now_utc();
        let retry_after_seconds: i64 = 0;

        let row = sqlx::query(
            r#"
WITH potential_candidates AS (
	SELECT
		u.id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY
		u.associated_index_id IS NULL DESC,
		COALESCE(u.process_after, u.uploaded_at),
		u.id
	) AS ordering
	FROM lsif_uploads_with_repository_name u
	WHERE
		(
			(
				u.state = 'queued' AND
				(u.process_after IS NULL OR u.process_after <= $1)
			) OR (
				$2 > 0 AND
				u.state = 'errored' AND
				$1 - u.finished_at > ($2 * '1 second'::interval)
			)
		)
	ORDER BY
		u.associated_index_id IS NULL DESC,
		COALESCE(u.process_after, u.uploaded_at),
		u.id
	LIMIT 50
),
candidate AS (
	SELECT
		u.id AS id
	FROM lsif_uploads u
	JOIN potential_candidates pc ON pc.candidate_id = u.id
	WHERE
		-- Recheck state.
		u.state IN ('queued', 'errored')
	ORDER BY pc.ordering
	FOR UPDATE OF u SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE
		lsif_uploads
	SET
		state = 'processing',
		execution_logs = NULL,
		failure_message = NULL,
		finished_at = NULL,
		last_heartbeat_at = $1,
		started_at = $1,
		worker_hostname = $3
	WHERE id IN (SELECT id FROM candidate)
)
SELECT
	u.id,
	u.commit,
	u.root,
	EXISTS (
		SELECT 1
		FROM lsif_uploads_visible_at_tip uvt
		WHERE
			uvt.repository_id = u.repository_id AND
			uvt.upload_id = u.id AND
			uvt.is_default_branch
	) AS visible_at_tip,
	u.uploaded_at,
	'processing' AS state,
	NULL::text AS failure_message,
	$1 AS started_at,
	NULL::timestamptz AS finished_at,
	u.process_after,
	u.num_resets,
	u.num_failures,
	u.repository_id,
	u.repository_name::text AS repository_name,
	u.indexer,
	u.indexer_version,
	u.num_parts,
	u.uploaded_parts,
	u.upload_size,
	u.associated_index_id,
	u.content_type,
	u.should_reindex,
	NULL::integer AS rank,
	u.uncompressed_size
FROM
	lsif_uploads_with_repository_name u
WHERE
	u.id IN (SELECT id FROM candidate)
"#,
        )
        .bind(now)
        .bind(retry_after_seconds)
        .bind(worker_hostname)
        .fetch_optional(&self.pool)
        .await?;

        row.map(scan_upload).transpose()
    }

    /// `dbworker/store.Store.Heartbeat`. Refreshes `last_heartbeat_at` for
    /// processing records and reports which ids are still known and which are
    /// flagged for cancellation.
    pub async fn heartbeat(
        &self,
        ids: &[i32],
        worker_hostname: Option<&str>,
    ) -> Result<HeartbeatResult, sqlx::Error> {
        if ids.is_empty() {
            return Ok(HeartbeatResult::default());
        }

        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query(
            r#"
WITH alive_candidates AS (
	SELECT
		id
	FROM
		lsif_uploads
	WHERE
		id = ANY ($1) AND
		state = 'processing' AND
		($3::text IS NULL OR worker_hostname = $3)
	ORDER BY
		id ASC
	FOR UPDATE
)
UPDATE
	lsif_uploads
SET
	last_heartbeat_at = $2
WHERE
	id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
        )
        .bind(ids)
        .bind(now)
        .bind(worker_hostname)
        .fetch_all(&self.pool)
        .await?;

        let mut result = HeartbeatResult::default();
        for row in rows {
            let id: i32 = row.try_get("id")?;
            let cancel: bool = row.try_get("cancel")?;
            result.known_ids.push(id);
            if cancel {
                result.cancel_ids.push(id);
            }
        }
        result.known_ids.sort_unstable();
        result.cancel_ids.sort_unstable();
        Ok(result)
    }

    /// `dbworker/store.Store.Requeue`: back to `queued` with a processing
    /// delay (`process_after`).
    pub async fn requeue(&self, id: i32, after: OffsetDateTime) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
UPDATE lsif_uploads
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
        .await?;
        Ok(())
    }

    /// `dbworker/store.Store.MarkComplete`. No effect unless the record is
    /// still `processing`.
    pub async fn mark_complete(
        &self,
        id: i32,
        worker_hostname: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE lsif_uploads
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing' AND ($2::text IS NULL OR worker_hostname = $2)
"#,
        )
        .bind(id)
        .bind(worker_hostname)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `dbworker/store.Store.MarkErrored` with `MaxNumRetries = 0`: an errored
    /// handler always lands in `failed` (or `canceled` when `cancel` is set).
    pub async fn mark_errored(
        &self,
        id: i32,
        failure_message: &str,
        worker_hostname: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE lsif_uploads
SET
	state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= 0 THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing' AND ($3::text IS NULL OR worker_hostname = $3)
"#,
        )
        .bind(id)
        .bind(failure_message)
        .bind(worker_hostname)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `dbworker/store.Store.MarkFailed` (non-retryable handler error).
    pub async fn mark_failed(
        &self,
        id: i32,
        failure_message: &str,
        worker_hostname: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE lsif_uploads
SET
	state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing' AND ($3::text IS NULL OR worker_hostname = $3)
"#,
        )
        .bind(id)
        .bind(failure_message)
        .bind(worker_hostname)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `dbworker/store.Store.ResetStalled` with `StalledMaxAge = 25s` and
    /// `MaxNumResets = 3`.
    pub async fn reset_stalled(&self) -> Result<ResetStalledResult, sqlx::Error> {
        let now = OffsetDateTime::now_utc();
        let stalled_max_age_seconds: i64 = STALLED_MAX_AGE.as_secs() as i64;

        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM lsif_uploads
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE lsif_uploads
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
        .bind(stalled_max_age_seconds)
        .bind(MAX_NUM_RESETS)
        .fetch_all(&self.pool)
        .await?;

        let failed_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM lsif_uploads
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE lsif_uploads
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(stalled_max_age_seconds)
        .bind(MAX_NUM_RESETS)
        .bind(DEFAULT_RESET_FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await?;

        Ok(ResetStalledResult {
            reset: collect_heartbeat_ages(reset_rows, now)?,
            failed: collect_heartbeat_ages(failed_rows, now)?,
        })
    }
}

fn collect_heartbeat_ages(
    rows: Vec<sqlx::postgres::PgRow>,
    now: OffsetDateTime,
) -> Result<Vec<(i32, Duration)>, sqlx::Error> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let last_heartbeat_at: OffsetDateTime = row.try_get("last_heartbeat_at")?;
        let age = (now - last_heartbeat_at)
            .try_into()
            .unwrap_or(Duration::ZERO);
        ages.push((id, age));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

fn scan_upload(row: sqlx::postgres::PgRow) -> Result<Upload, sqlx::Error> {
    // `pq.Array` scans SQL NULL into an empty slice in the baseline.
    let uploaded_parts: Option<Vec<i32>> = row.try_get("uploaded_parts")?;
    Ok(Upload {
        id: row.try_get("id")?,
        commit: row.try_get("commit")?,
        root: row.try_get("root")?,
        visible_at_tip: row.try_get("visible_at_tip")?,
        uploaded_at: row.try_get("uploaded_at")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        process_after: row.try_get("process_after")?,
        num_resets: row.try_get("num_resets")?,
        num_failures: row.try_get("num_failures")?,
        repository_id: row.try_get("repository_id")?,
        repository_name: row.try_get("repository_name")?,
        indexer: row.try_get("indexer")?,
        indexer_version: row
            .try_get::<Option<String>, _>("indexer_version")?
            .unwrap_or_default(),
        num_parts: row.try_get("num_parts")?,
        uploaded_parts: uploaded_parts.unwrap_or_default(),
        upload_size: row.try_get("upload_size")?,
        associated_index_id: row.try_get("associated_index_id")?,
        content_type: row.try_get("content_type")?,
        should_reindex: row.try_get("should_reindex")?,
        rank: row.try_get("rank")?,
        uncompressed_size: row.try_get("uncompressed_size")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stalled_constants_match_the_baseline() {
        assert_eq!(STALLED_MAX_AGE, Duration::from_secs(25));
        assert_eq!(MAX_NUM_RESETS, 3);
        assert_eq!(
            DEFAULT_RESET_FAILURE_MESSAGE,
            "job processor died while handling this message too many times"
        );
    }

    #[test]
    fn reset_result_defaults_are_empty() {
        let result = ResetStalledResult::default();
        assert!(result.reset.is_empty());
        assert!(result.failed.is_empty());
    }
}
