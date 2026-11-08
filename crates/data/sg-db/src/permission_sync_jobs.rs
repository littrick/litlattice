#![forbid(unsafe_code)]

//! `internal/database/permission_sync_jobs.go` + the queries used by
//! `cmd/worker/internal/permissions/perms_syncer_cleaner.go` and
//! `perms_syncer_scheduler.go`.
//!
//! Ported statements:
//!
//! * `cleanJobs` (the cleaner job);
//! * `UserIDsWithNoPerms` / `RepoIDsWithNoPerms`;
//! * `UserIDsWithOldestPerms` / `ReposIDsWithOldestPerms`, with the
//!   `getCutoffClause` behaviour (`age == 0` → `TRUE`, otherwise
//!   `finished_at < cutoff OR finished_at IS NULL`); the caller computes the
//!   cutoff;
//! * `CreateUserSyncJob` / `CreateRepoSyncJob`, including
//!   `checkDuplicateAndCreateSyncJob` (cancel a queued lower-priority job,
//!   skip when one with equal or higher priority exists).
//!
//! The SQL is copied verbatim apart from binding the formatted arguments.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

const CLEAN_JOBS: &str = r#"
-- CTE for fetching queued/processing jobs per repository_id/user_id and their row numbers

WITH job_history AS (
	SELECT id, repository_id, user_id, ROW_NUMBER() OVER (
		PARTITION BY repository_id, user_id
		ORDER BY finished_at DESC NULLS LAST
	) FROM permission_sync_jobs
	WHERE state NOT IN ('queued', 'processing')
)

-- Removing those jobs which count per repo/user exceeds a certain number

DELETE FROM permission_sync_jobs
WHERE id IN (
	SELECT id
	FROM job_history
	WHERE row_number > $1
)
"#;

/// `cleanJobs`: delete finished jobs beyond the newest `jobs_to_keep` per
/// `(repository_id, user_id)`; returns the number of deleted rows.
pub async fn clean_jobs(pool: &PgPool, jobs_to_keep: i32) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(CLEAN_JOBS)
        .bind(jobs_to_keep)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// `userIDsWithNoPermsQuery`: users that have neither stored permissions nor
/// any permission sync job. `enforce_for_site_admins`
/// (`authz.enforceForSiteAdmins`) decides whether site admins are filtered
/// out (the baseline's default) or kept.
const USER_IDS_WITH_NO_PERMS: &str = r#"
WITH rp AS (
	-- Filter out users with permissions
	SELECT DISTINCT user_id FROM user_repo_permissions
	UNION
	-- Filter out users with sync jobs
	SELECT DISTINCT user_id FROM permission_sync_jobs WHERE user_id IS NOT NULL
)
SELECT users.id
FROM users
LEFT OUTER JOIN rp ON rp.user_id = users.id
WHERE
	users.deleted_at IS NULL
AND ($1 OR users.site_admin = FALSE)
AND rp.user_id IS NULL
ORDER BY users.id
"#;

/// `PermsStore.UserIDsWithNoPerms`.
pub async fn user_ids_with_no_perms(
    pool: &PgPool,
    enforce_for_site_admins: bool,
) -> Result<Vec<i32>, sqlx::Error> {
    let rows = sqlx::query(USER_IDS_WITH_NO_PERMS)
        .bind(enforce_for_site_admins)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(|row| row.try_get("id")).collect()
}

/// `repoIDsWithNoPermsQuery`: private, non-deleted repositories that have
/// neither stored permissions nor any permission sync job.
const REPO_IDS_WITH_NO_PERMS: &str = r#"
WITH rp AS (
	-- Filter out repos with permissions
	SELECT DISTINCT perms.repo_id FROM user_repo_permissions AS perms
	UNION
	-- Filter out repos with sync jobs
	SELECT DISTINCT syncs.repository_id AS repo_id FROM permission_sync_jobs AS syncs
		WHERE syncs.repository_id IS NOT NULL
)
SELECT r.id
FROM repo AS r
LEFT OUTER JOIN rp ON rp.repo_id = r.id
WHERE r.deleted_at IS NULL
AND r.private = TRUE
AND rp.repo_id IS NULL
ORDER BY r.id
"#;

/// `PermsStore.RepoIDsWithNoPerms`.
pub async fn repo_ids_with_no_perms(pool: &PgPool) -> Result<Vec<i32>, sqlx::Error> {
    let rows = sqlx::query(REPO_IDS_WITH_NO_PERMS).fetch_all(pool).await?;
    rows.into_iter().map(|row| row.try_get("id")).collect()
}

/// `usersWithOldestPermsQuery`, with the `getCutoffClause` argument inlined as
/// `$1` and the limit as `$2`. The baseline returns a `map[int32]time.Time`;
/// the scheduler only iterates it, so the caller sorts the IDs to stay
/// deterministic.
const USER_IDS_WITH_OLDEST_PERMS: &str = r#"
SELECT u.id as user_id, MAX(p.finished_at) as finished_at
FROM users u
LEFT JOIN permission_sync_jobs p ON u.id = p.user_id AND p.user_id IS NOT NULL
WHERE u.deleted_at IS NULL AND ($1)
AND NOT EXISTS (
	SELECT 1 FROM permission_sync_jobs p2
	WHERE p2.user_id = u.id AND (p2.state = 'queued' OR p2.state = 'processing')
)
GROUP BY u.id
ORDER BY finished_at ASC NULLS FIRST, user_id ASC
LIMIT $2
"#;

/// The `getCutoffClause` half: `TRUE` for a zero age, otherwise
/// `finished_at < $cutoff OR finished_at IS NULL`. A zero age removes the
/// cutoff parameter entirely, so the `LIMIT $2` placeholder is renumbered to
/// `$1` to keep the bound arguments contiguous.
fn applicable_query(base: &str, cutoff: Option<OffsetDateTime>) -> String {
    match cutoff {
        Some(_) => base.replace("($1)", "(finished_at < $1 OR finished_at IS NULL)"),
        None => base
            .replace("($1)", "(TRUE)")
            .replace("LIMIT $2", "LIMIT $1"),
    }
}

/// `PermsStore.UserIDsWithOldestPerms`. `cutoff` is `now - age`; `None` means
/// `age == 0`, so every user with finished-or-missing jobs qualifies.
pub async fn user_ids_with_oldest_perms(
    pool: &PgPool,
    limit: i64,
    cutoff: Option<OffsetDateTime>,
) -> Result<Vec<i32>, sqlx::Error> {
    let query = applicable_query(USER_IDS_WITH_OLDEST_PERMS, cutoff);
    let mut q = sqlx::query(&query);
    if let Some(cutoff) = cutoff {
        q = q.bind(cutoff);
    }
    let rows = q.bind(limit).fetch_all(pool).await?;
    rows.into_iter().map(|row| row.try_get("user_id")).collect()
}

/// `reposWithOldestPermsQuery`.
const REPO_IDS_WITH_OLDEST_PERMS: &str = r#"
SELECT r.id as repo_id, MAX(p.finished_at) as finished_at
FROM repo r
LEFT JOIN permission_sync_jobs p ON r.id = p.repository_id AND p.repository_id IS NOT NULL
WHERE r.private AND r.deleted_at IS NULL AND ($1)
AND NOT EXISTS (
	SELECT 1 FROM permission_sync_jobs p2
	WHERE p2.repository_id = r.id AND (p2.state = 'queued' OR p2.state = 'processing')
)
GROUP BY r.id
ORDER BY finished_at ASC NULLS FIRST, repo_id ASC
LIMIT $2
"#;

/// `PermsStore.ReposIDsWithOldestPerms`.
pub async fn repo_ids_with_oldest_perms(
    pool: &PgPool,
    limit: i64,
    cutoff: Option<OffsetDateTime>,
) -> Result<Vec<i32>, sqlx::Error> {
    let query = applicable_query(REPO_IDS_WITH_OLDEST_PERMS, cutoff);
    let mut q = sqlx::query(&query);
    if let Some(cutoff) = cutoff {
        q = q.bind(cutoff);
    }
    let rows = q.bind(limit).fetch_all(pool).await?;
    rows.into_iter().map(|row| row.try_get("repo_id")).collect()
}

/// `CancellationReasonHigherPriority`.
pub const CANCELLATION_REASON_HIGHER_PRIORITY: &str = "A job with higher priority was added.";

/// `listPermissionSyncJobQueryFmtstr` narrowed to the duplicate check:
/// `ListPermissionSyncJobOpts{UserID|RepoID, State: queued, NotCanceled,
/// NullProcessAfter}` (which also adds `is_partial_success = FALSE`).
const QUEUED_DUPLICATES_FOR_USER: &str = r#"
SELECT id, priority FROM permission_sync_jobs
WHERE user_id = $1
  AND state = lower('queued')
  AND is_partial_success = FALSE
  AND process_after IS NULL
  AND cancel = false
ORDER BY permission_sync_jobs.id
LIMIT 1
"#;

const QUEUED_DUPLICATES_FOR_REPO: &str = r#"
SELECT id, priority FROM permission_sync_jobs
WHERE repository_id = $1
  AND state = lower('queued')
  AND is_partial_success = FALSE
  AND process_after IS NULL
  AND cancel = false
ORDER BY permission_sync_jobs.id
LIMIT 1
"#;

/// `CancelQueuedJob`.
const CANCEL_QUEUED_JOB: &str = r#"
UPDATE permission_sync_jobs
SET cancel = TRUE, state = 'canceled', finished_at = $2, cancellation_reason = $3
WHERE id = $1 AND state = 'queued' AND cancel IS FALSE
"#;

/// `permissionSyncJobCreateQueryFmtstr` with the NULL columns bound as NULL.
/// `RETURNING id` stands in for the baseline's full `RETURNING columns`;
/// `ON CONFLICT DO NOTHING` yielding no row is an error there
/// (`sql.ErrNoRows` from the scan), which is preserved here as
/// [`sqlx::Error::RowNotFound`].
const CREATE_USER_SYNC_JOB: &str = r#"
INSERT INTO permission_sync_jobs (
	reason,
	triggered_by_user_id,
	process_after,
	repository_id,
	user_id,
	priority,
	invalidate_caches,
	no_perms
)
VALUES (
	$1,
	NULL,
	NULL,
	NULL,
	$2,
	$3,
	FALSE,
	$4
)
ON CONFLICT DO NOTHING
RETURNING id
"#;

const CREATE_REPO_SYNC_JOB: &str = r#"
INSERT INTO permission_sync_jobs (
	reason,
	triggered_by_user_id,
	process_after,
	repository_id,
	user_id,
	priority,
	invalidate_caches,
	no_perms
)
VALUES (
	$1,
	NULL,
	NULL,
	$2,
	NULL,
	$3,
	FALSE,
	$4
)
ON CONFLICT DO NOTHING
RETURNING id
"#;

/// `CreateUserSyncJob` + `checkDuplicateAndCreateSyncJob` for the scheduler's
/// zero-`process_after` jobs. `now` is used as `finished_at` when an existing
/// lower-priority job is canceled.
pub async fn create_user_sync_job(
    pool: &PgPool,
    user_id: i32,
    reason: &str,
    priority: i32,
    no_perms: bool,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    create_sync_job(
        pool,
        QUEUED_DUPLICATES_FOR_USER,
        CREATE_USER_SYNC_JOB,
        user_id,
        reason,
        priority,
        no_perms,
        now,
    )
    .await
}

/// `CreateRepoSyncJob` + `checkDuplicateAndCreateSyncJob`.
pub async fn create_repo_sync_job(
    pool: &PgPool,
    repo_id: i32,
    reason: &str,
    priority: i32,
    no_perms: bool,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    create_sync_job(
        pool,
        QUEUED_DUPLICATES_FOR_REPO,
        CREATE_REPO_SYNC_JOB,
        repo_id,
        reason,
        priority,
        no_perms,
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_sync_job(
    pool: &PgPool,
    duplicates_query: &str,
    insert_query: &str,
    id: i32,
    reason: &str,
    priority: i32,
    no_perms: bool,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let duplicate = sqlx::query(duplicates_query)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(row) = duplicate {
        let existing_priority: i32 = row.try_get("priority")?;
        // Equal or higher priority: the baseline leaves the existing job and
        // returns nil (`Debug` log only).
        if existing_priority >= priority {
            tx.commit().await?;
            return Ok(());
        }
        let existing_id: i32 = row.try_get("id")?;
        // `CancelQueuedJob`: a missing row (`errcode.IsNotFound`) is ignored
        // and the insert still runs.
        sqlx::query(CANCEL_QUEUED_JOB)
            .bind(existing_id)
            .bind(now)
            .bind(CANCELLATION_REASON_HIGHER_PRIORITY)
            .execute(&mut *tx)
            .await?;
    }

    let inserted = sqlx::query(insert_query)
        .bind(reason)
        .bind(id)
        .bind(priority)
        .bind(no_perms)
        .fetch_optional(&mut *tx)
        .await?;
    if inserted.is_none() {
        tx.rollback().await?;
        return Err(sqlx::Error::RowNotFound);
    }

    tx.commit().await
}

/// `database.PermissionSyncJob`, reduced to the fields the worker handler and
/// the store operations read. `RecordID` is the `i64` the worker framework
/// uses; the column is a Postgres `integer`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionSyncJob {
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
    pub repository_id: Option<i32>,
    pub user_id: Option<i32>,
    pub priority: i32,
    pub no_perms: bool,
    pub invalidate_caches: bool,
}

/// Which half of the permission-sync queue a worker store serves:
/// `syncTypeRepo` / `syncTypeUser` in `perms_syncer_worker.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionSyncJobType {
    Repo,
    User,
}

impl PermissionSyncJobType {
    /// The `PreDequeue` predicate appended to the dequeue query.
    fn dequeue_condition(self) -> &'static str {
        match self {
            Self::Repo => "repository_id IS NOT NULL",
            Self::User => "user_id IS NOT NULL",
        }
    }
}

/// Ids reset by [`PermissionSyncJobsWorkerStore::reset_stalled`], with the age
/// of their last heartbeat when the reset ran.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionSyncJobResetResult {
    pub reset: Vec<(i32, Duration)>,
    pub failed: Vec<(i32, Duration)>,
}

/// `dbworkerstore.New` configured by `makeStore` in
/// `cmd/worker/internal/authz/perms_syncer_worker.go`: table
/// `permission_sync_jobs`, order
/// `priority DESC, process_after ASC NULLS FIRST, id ASC`,
/// `MaxNumResets: 5`, `StalledMaxAge: 30s`, no retries (`MaxNumRetries: 0`,
/// `RetryAfter: 0`).
#[derive(Debug, Clone)]
pub struct PermissionSyncJobsWorkerStore {
    pool: PgPool,
    sync_type: PermissionSyncJobType,
}

impl PermissionSyncJobsWorkerStore {
    pub fn new(pool: PgPool, sync_type: PermissionSyncJobType) -> Self {
        Self { pool, sync_type }
    }

    pub fn sync_type(&self) -> PermissionSyncJobType {
        self.sync_type
    }

    /// `Store.Dequeue` with the `PreDequeue` predicate for this worker type.
    /// Only `queued` rows are eligible: with `RetryAfter = 0` the baseline's
    /// `errored` branch is never enabled.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
    ) -> Result<Option<PermissionSyncJob>, sqlx::Error> {
        let now = OffsetDateTime::now_utc();
        let condition = self.sync_type.dequeue_condition();
        let query = format!(
            r#"
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY priority DESC, process_after ASC NULLS FIRST, id ASC) AS order
	FROM permission_sync_jobs
	WHERE
		state = 'queued' AND
		(process_after IS NULL OR process_after <= $1) AND
		({condition})
	ORDER BY priority DESC, process_after ASC NULLS FIRST, id ASC
	LIMIT 50
),
candidate AS (
	SELECT id FROM permission_sync_jobs
	JOIN potential_candidates pc ON pc.candidate_id = permission_sync_jobs.id
	WHERE state IN ('queued', 'errored')
	ORDER BY pc.order
	FOR UPDATE OF permission_sync_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE permission_sync_jobs
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $2
	WHERE id IN (SELECT id FROM candidate)
	RETURNING id, state, failure_message, queued_at, started_at, finished_at,
	          process_after, num_resets, num_failures, last_heartbeat_at,
	          worker_hostname, cancel, repository_id, user_id, priority,
	          no_perms, invalidate_caches
)
SELECT * FROM updated_record
"#
        );
        let row = sqlx::query(&query)
            .bind(now)
            .bind(worker_hostname)
            .fetch_optional(&self.pool)
            .await?;
        row.map(scan_job).transpose()
    }

    /// `Store.Heartbeat`: refresh `last_heartbeat_at` for processing rows and
    /// report which ids are known and which are flagged `cancel`.
    pub async fn heartbeat(&self, ids: &[i32]) -> Result<(Vec<i32>, Vec<i32>), sqlx::Error> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query(
            r#"
WITH alive_candidates AS (
	SELECT id
	FROM permission_sync_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE permission_sync_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
        )
        .bind(ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        let mut known = Vec::with_capacity(rows.len());
        let mut canceled = Vec::new();
        for row in rows {
            let id: i32 = row.try_get("id")?;
            let cancel: bool = row.try_get("cancel")?;
            known.push(id);
            if cancel {
                canceled.push(id);
            }
        }
        Ok((known, canceled))
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE permission_sync_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkErrored` with `MaxNumRetries = 0`: an errored handler always
    /// lands in `failed` (or `canceled` when the cancel flag is set).
    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE permission_sync_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= 0 THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .bind(failure_message)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkFailed` (non-retryable handler errors).
    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE permission_sync_jobs
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
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.ResetStalled` (`StalledMaxAge = 30s`, `MaxNumResets = 5`).
    pub async fn reset_stalled(&self) -> Result<PermissionSyncJobResetResult, sqlx::Error> {
        const STALLED_MAX_AGE_SECONDS: i64 = 30;
        const MAX_NUM_RESETS: i32 = 5;
        const FAILURE_MESSAGE: &str =
            "job processor died while handling this message too many times";

        let now = OffsetDateTime::now_utc();
        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM permission_sync_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE permission_sync_jobs
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
        .bind(STALLED_MAX_AGE_SECONDS)
        .bind(MAX_NUM_RESETS)
        .fetch_all(&self.pool)
        .await?;

        let failed_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM permission_sync_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE permission_sync_jobs
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(STALLED_MAX_AGE_SECONDS)
        .bind(MAX_NUM_RESETS)
        .bind(FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await?;

        Ok(PermissionSyncJobResetResult {
            reset: scan_reset_rows(now, &reset_rows)?,
            failed: scan_reset_rows(now, &failed_rows)?,
        })
    }
}

fn scan_job(row: sqlx::postgres::PgRow) -> Result<PermissionSyncJob, sqlx::Error> {
    Ok(PermissionSyncJob {
        id: row.try_get("id")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        queued_at: row.try_get("queued_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        process_after: row.try_get("process_after")?,
        num_resets: row.try_get("num_resets")?,
        num_failures: row.try_get("num_failures")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        worker_hostname: row.try_get("worker_hostname")?,
        cancel: row.try_get("cancel")?,
        repository_id: row.try_get("repository_id")?,
        user_id: row.try_get("user_id")?,
        priority: row.try_get("priority")?,
        no_perms: row.try_get("no_perms")?,
        invalidate_caches: row.try_get("invalidate_caches")?,
    })
}

fn scan_reset_rows(
    now: OffsetDateTime,
    rows: &[sqlx::postgres::PgRow],
) -> Result<Vec<(i32, Duration)>, sqlx::Error> {
    rows.iter()
        .map(|row| {
            let id: i32 = row.try_get("id")?;
            let last_heartbeat: Option<OffsetDateTime> = row.try_get("last_heartbeat_at")?;
            let age = last_heartbeat
                .and_then(|heartbeat| Duration::try_from(now - heartbeat).ok())
                .unwrap_or_default();
            Ok((id, age))
        })
        .collect()
}
