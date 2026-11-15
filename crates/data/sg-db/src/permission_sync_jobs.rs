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

// ---------------------------------------------------------------------------
// Read side used by the authz GraphQL surface (M9.48a)
// ---------------------------------------------------------------------------

/// One `permission_sync_jobs` row with every column the authz GraphQL
/// `PermissionsSyncJob` type exposes. `place_in_queue` is `Some` only when
/// `with_place_in_queue` was requested (`ROW_NUMBER()` over queued rows).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PermissionSyncJobListRow {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub reason: String,
    pub cancellation_reason: Option<String>,
    pub triggered_by_user_id: Option<i32>,
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
    pub permissions_added: i32,
    pub permissions_removed: i32,
    pub permissions_found: i32,
    pub code_host_states: Vec<serde_json::Value>,
    pub is_partial_success: bool,
    pub place_in_queue: Option<i32>,
}

/// `database.ListPermissionSyncJobOpts` narrowed to the fields the GraphQL
/// resolvers set, plus the pagination arguments (`database.PaginationArgs`).
#[derive(Debug, Clone, Default)]
pub struct PermissionSyncJobListOpts {
    pub id: i32,
    pub user_id: i32,
    pub repo_id: i32,
    pub reason: String,
    pub reason_group: String,
    pub state: String,
    pub null_process_after: bool,
    pub not_null_process_after: bool,
    pub not_canceled: bool,
    pub partial_success: bool,
    pub with_place_in_queue: bool,
    pub search_type: Option<String>,
    pub query: String,
    /// `database.PaginationArgs.First` (+1 probe), applied with `ORDER BY id ASC`.
    pub first: Option<i64>,
    /// `database.PaginationArgs.Last` (+1 probe), applied with `ORDER BY id DESC`.
    pub last: Option<i64>,
    /// Cursor values (job IDs).
    pub after: Option<i32>,
    pub before: Option<i32>,
}

/// `ReasonGroupToReasons` (`internal/database/permission_sync_jobs.go:77-119`).
fn reason_group_reasons(group: &str) -> Option<&'static [&'static str]> {
    match group {
        "MANUAL" => Some(&["REASON_MANUAL_REPO_SYNC", "REASON_MANUAL_USER_SYNC"]),
        "WEBHOOK" => Some(&[
            "REASON_GITHUB_USER_EVENT",
            "REASON_GITHUB_USER_ADDED_EVENT",
            "REASON_GITHUB_USER_REMOVED_EVENT",
            "REASON_GITHUB_USER_MEMBERSHIP_ADDED_EVENT",
            "REASON_GITHUB_USER_MEMBERSHIP_REMOVED_EVENT",
            "REASON_GITHUB_TEAM_ADDED_TO_REPO_EVENT",
            "REASON_GITHUB_TEAM_REMOVED_FROM_REPO_EVENT",
            "REASON_GITHUB_ORG_MEMBER_ADDED_EVENT",
            "REASON_GITHUB_ORG_MEMBER_REMOVED_EVENT",
            "REASON_GITHUB_REPO_EVENT",
            "REASON_GITHUB_REPO_MADE_PRIVATE_EVENT",
        ]),
        "SCHEDULE" => Some(&[
            "REASON_USER_OUTDATED_PERMS",
            "REASON_USER_NO_PERMS",
            "REASON_REPO_OUTDATED_PERMS",
            "REASON_REPO_NO_PERMS",
            "REASON_REPO_UPDATED_FROM_CODE_HOST",
        ]),
        "SOURCEGRAPH" => Some(&[
            "REASON_USER_EMAIL_REMOVED",
            "REASON_USER_EMAIL_VERIFIED",
            "REASON_USER_ADDED",
            "REASON_USER_ADDED_TO_ORG",
            "REASON_USER_REMOVED_FROM_ORG",
            "REASON_USER_ACCEPTED_ORG_INVITE",
            "REASON_EXTERNAL_ACCOUNT_ADDED",
            "REASON_EXTERNAL_ACCOUNT_DELETED",
        ]),
        // `UNKNOWN` has no reasons; the baseline adds no condition.
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum JobParam {
    Int(i32),
    Text(String),
    Bool(bool),
}

struct JobQuery {
    sql: String,
    params: Vec<JobParam>,
}

impl JobQuery {
    fn new(base: &str) -> Self {
        Self {
            sql: base.to_owned(),
            params: Vec::new(),
        }
    }

    fn push(&mut self, param: JobParam) -> String {
        self.params.push(param);
        format!("${}", self.params.len())
    }

    fn push_int(&mut self, value: i32) -> String {
        self.push(JobParam::Int(value))
    }

    fn push_text(&mut self, value: impl Into<String>) -> String {
        self.push(JobParam::Text(value.into()))
    }

    fn bind<'q>(
        &self,
        mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        for param in &self.params {
            query = match param {
                JobParam::Int(value) => query.bind(*value),
                JobParam::Text(value) => query.bind(value.clone()),
                JobParam::Bool(value) => query.bind(*value),
            };
        }
        query
    }
}

/// Conditions shared by `List` and `Count` (`opts.sqlConds()`). Returns
/// whether a top-level `WHERE` was emitted.
fn job_conditions(query: &mut JobQuery, opts: &PermissionSyncJobListOpts) -> bool {
    let mut conditions = Vec::new();
    if opts.id != 0 {
        conditions.push(format!("permission_sync_jobs.id = {}", query.push_int(opts.id)));
    }
    if opts.user_id != 0 {
        conditions.push(format!("user_id = {}", query.push_int(opts.user_id)));
    }
    if opts.repo_id != 0 {
        conditions.push(format!("repository_id = {}", query.push_int(opts.repo_id)));
    }
    if !opts.reason_group.is_empty() && opts.reason.is_empty() {
        if let Some(reasons) = reason_group_reasons(&opts.reason_group) {
            let placeholders: Vec<String> = reasons
                .iter()
                .map(|reason| query.push_text(*reason))
                .collect();
            conditions.push(format!("reason IN ({})", placeholders.join(", ")));
        }
    }
    if !opts.reason.is_empty() {
        conditions.push(format!("reason = {}", query.push_text(opts.reason.clone())));
    }
    if opts.partial_success {
        conditions.push("is_partial_success = TRUE".to_owned());
        conditions.push("state = lower('completed')".to_owned());
    } else if !opts.state.is_empty() {
        conditions.push(format!("state = lower({})", query.push_text(opts.state.clone())));
        conditions.push("is_partial_success = FALSE".to_owned());
    }
    if opts.null_process_after {
        conditions.push("process_after IS NULL".to_owned());
    }
    if opts.not_null_process_after {
        conditions.push("process_after IS NOT NULL".to_owned());
    }
    if opts.not_canceled {
        conditions.push("cancel = false".to_owned());
    }
    match opts.search_type.as_deref() {
        Some("REPOSITORY") => {
            conditions.push("permission_sync_jobs.repository_id IS NOT NULL".to_owned());
            if !opts.query.is_empty() {
                let pattern = format!("%{}%", opts.query);
                conditions.push(format!("repo.name ILIKE {}", query.push_text(pattern)));
            }
        }
        Some("USER") => {
            conditions.push("permission_sync_jobs.user_id IS NOT NULL".to_owned());
            if !opts.query.is_empty() {
                let pattern = format!("%{}%", opts.query);
                let first = query.push_text(pattern.clone());
                let second = query.push_text(pattern);
                conditions.push(format!(
                    "(users.username ILIKE {first} OR users.display_name ILIKE {second})"
                ));
            }
        }
        _ => {}
    }
    if !conditions.is_empty() {
        query.sql.push_str(" WHERE ");
        query.sql.push_str(&conditions.join(" AND "));
        true
    } else {
        false
    }
}

fn job_search_join(opts: &PermissionSyncJobListOpts) -> String {
    if opts.query.is_empty() {
        return String::new();
    }
    match opts.search_type.as_deref() {
        Some("REPOSITORY") => " JOIN repo ON permission_sync_jobs.repository_id = repo.id".to_owned(),
        Some("USER") => " JOIN users ON permission_sync_jobs.user_id = users.id".to_owned(),
        _ => String::new(),
    }
}

const JOB_COLUMNS: &str = "permission_sync_jobs.id, permission_sync_jobs.state, \
    permission_sync_jobs.reason, permission_sync_jobs.cancellation_reason, \
    permission_sync_jobs.triggered_by_user_id, permission_sync_jobs.failure_message, \
    permission_sync_jobs.queued_at, permission_sync_jobs.started_at, \
    permission_sync_jobs.finished_at, permission_sync_jobs.process_after, \
    permission_sync_jobs.num_resets, permission_sync_jobs.num_failures, \
    permission_sync_jobs.last_heartbeat_at, permission_sync_jobs.worker_hostname, \
    permission_sync_jobs.cancel, permission_sync_jobs.repository_id, \
    permission_sync_jobs.user_id, permission_sync_jobs.priority, \
    permission_sync_jobs.no_perms, permission_sync_jobs.invalidate_caches, \
    permission_sync_jobs.permissions_added, permission_sync_jobs.permissions_removed, \
    permission_sync_jobs.permissions_found, permission_sync_jobs.code_host_states, \
    permission_sync_jobs.is_partial_success";

fn scan_list_row(row: &sqlx::postgres::PgRow) -> Result<PermissionSyncJobListRow, sqlx::Error> {
    let code_host_states: Option<Vec<serde_json::Value>> = row.try_get("code_host_states")?;
    // `ROW_NUMBER()` is `int8`; Go scans it into `*int32`.
    let place_in_queue: Option<i32> = match row.try_get::<Option<i64>, _>("queue_rank") {
        Ok(value) => value.map(|value| value as i32),
        Err(_) => None,
    };
    Ok(PermissionSyncJobListRow {
        id: row.try_get("id")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        reason: row.try_get("reason")?,
        cancellation_reason: row.try_get("cancellation_reason")?,
        triggered_by_user_id: row.try_get("triggered_by_user_id")?,
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
        permissions_added: row.try_get("permissions_added")?,
        permissions_removed: row.try_get("permissions_removed")?,
        permissions_found: row.try_get("permissions_found")?,
        code_host_states: code_host_states.unwrap_or_default(),
        is_partial_success: row.try_get::<Option<bool>, _>("is_partial_success")?.unwrap_or(false),
        place_in_queue,
    })
}

/// `List` with the authz resolver's `PaginationArgs` semantics. The query is
/// always ordered by `permission_sync_jobs.id` (the caller's only order key).
pub async fn list_permission_sync_jobs(
    pool: &PgPool,
    opts: &PermissionSyncJobListOpts,
) -> Result<Vec<PermissionSyncJobListRow>, sqlx::Error> {
    let mut query = JobQuery::new(&format!(
        "SELECT {JOB_COLUMNS}{} FROM permission_sync_jobs",
        if opts.with_place_in_queue {
            ", queue_ranks.rank AS queue_rank"
        } else {
            ""
        }
    ));
    query.sql.push_str(&job_search_join(opts));
    if opts.with_place_in_queue {
        query.sql.push_str(
            " LEFT JOIN (SELECT id, ROW_NUMBER() OVER (ORDER BY permission_sync_jobs.priority DESC, \
             permission_sync_jobs.process_after ASC NULLS FIRST, permission_sync_jobs.id ASC) AS rank \
             FROM permission_sync_jobs WHERE state = 'queued') AS queue_ranks \
             ON queue_ranks.id = permission_sync_jobs.id",
        );
    }
    let mut has_where = job_conditions(&mut query, opts);

    // `database.PaginationArgs.SQL()` where clause. The jobs connection keeps
    // the default `ConnectionResolverOptions.Ascending == false`, so `first`
    // pages descend and `last` pages ascend.
    let ascending = opts.last.is_some();
    let mut pagination_conditions = Vec::new();
    if let Some(after) = opts.after {
        let op = if ascending { ">" } else { "<" };
        pagination_conditions.push(format!(
            "(permission_sync_jobs.id) {op} ({})",
            query.push_int(after)
        ));
    }
    if let Some(before) = opts.before {
        let op = if ascending { "<" } else { ">" };
        pagination_conditions.push(format!(
            "(permission_sync_jobs.id) {op} ({})",
            query.push_int(before)
        ));
    }
    if !pagination_conditions.is_empty() {
        if has_where {
            query.sql.push_str(" AND ");
        } else {
            query.sql.push_str(" WHERE ");
            has_where = true;
        }
        query.sql.push_str(&pagination_conditions.join(" AND "));
    }
    // `AppendOrderToQuery` + `AppendLimitToQuery`.
    let direction = if ascending { "ASC" } else { "DESC" };
    query
        .sql
        .push_str(&format!(" ORDER BY permission_sync_jobs.id {direction}"));
    if let Some(limit) = opts.first.or(opts.last) {
        query.sql.push_str(&format!(" LIMIT {limit}"));
    }

    let rows = query
        .bind(sqlx::query(&query.sql))
        .fetch_all(pool)
        .await?;
    rows.iter().map(scan_list_row).collect()
}

/// `Count` with the same conditions (no pagination).
pub async fn count_permission_sync_jobs(
    pool: &PgPool,
    opts: &PermissionSyncJobListOpts,
) -> Result<i64, sqlx::Error> {
    let mut query = JobQuery::new(&format!(
        "SELECT COUNT(*) FROM permission_sync_jobs{}",
        job_search_join(opts)
    ));
    let _ = job_conditions(&mut query, opts);
    query.bind(sqlx::query(&query.sql)).fetch_one(pool).await.map(|row| {
        use sqlx::Row as _;
        row.get::<i64, _>(0)
    })
}

const COUNT_USERS_WITH_FAILING: &str = r#"
WITH latest_user_sync_jobs AS (
  SELECT DISTINCT ON (user_id) user_id, id, state
  FROM permission_sync_jobs
  WHERE user_id IS NOT NULL AND state IN ('completed', 'failed')
  ORDER BY user_id, finished_at DESC
)
SELECT COUNT(*)
FROM latest_user_sync_jobs
JOIN users ON users.id = latest_user_sync_jobs.user_id
WHERE latest_user_sync_jobs.state = 'failed' AND users.deleted_at IS NULL
"#;

/// `CountUsersWithFailingSyncJob`.
pub async fn count_users_with_failing_sync_job(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(COUNT_USERS_WITH_FAILING).fetch_one(pool).await
}

const COUNT_REPOS_WITH_FAILING: &str = r#"
WITH latest_repo_sync_jobs AS (
  SELECT DISTINCT ON (repository_id) repository_id, id, state
  FROM permission_sync_jobs
  WHERE repository_id IS NOT NULL AND state IN ('completed', 'failed')
  ORDER BY repository_id, finished_at DESC
)
SELECT COUNT(*)
FROM latest_repo_sync_jobs
JOIN repo ON repo.id = latest_repo_sync_jobs.repository_id
WHERE latest_repo_sync_jobs.state = 'failed' AND repo.deleted_at IS NULL
"#;

/// `CountReposWithFailingSyncJob`.
pub async fn count_repos_with_failing_sync_job(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(COUNT_REPOS_WITH_FAILING).fetch_one(pool).await
}

/// `CancelQueuedJob`: `true` when exactly one queued row was canceled; `false`
/// reproduces the baseline's `notFoundError` when no row matched.
pub async fn cancel_queued_job(
    pool: &PgPool,
    id: i32,
    reason: &str,
    now: OffsetDateTime,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(CANCEL_QUEUED_JOB)
        .bind(id)
        .bind(now)
        .bind(reason)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// `database.PermissionSyncJobOpts`, restricted to the fields the authz
/// mutations pass to `Create{User,Repo}SyncJob`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreatePermissionSyncJobOpts {
    pub reason: String,
    pub priority: i32,
    pub invalidate_caches: bool,
    pub triggered_by_user_id: i32,
    pub process_after: Option<OffsetDateTime>,
}

const CREATE_USER_SYNC_JOB_WITH_OPTS: &str = r#"
INSERT INTO permission_sync_jobs (
	reason, triggered_by_user_id, process_after, repository_id, user_id, priority, invalidate_caches, no_perms
) VALUES ($1, $2, $3, NULL, $4, $5, $6, FALSE)
ON CONFLICT DO NOTHING
RETURNING id
"#;

const CREATE_REPO_SYNC_JOB_WITH_OPTS: &str = r#"
INSERT INTO permission_sync_jobs (
	reason, triggered_by_user_id, process_after, repository_id, user_id, priority, invalidate_caches, no_perms
) VALUES ($1, $2, $3, $4, NULL, $5, $6, FALSE)
ON CONFLICT DO NOTHING
RETURNING id
"#;

async fn create_with_opts(
    pool: &PgPool,
    target_id: i32,
    user: bool,
    opts: &CreatePermissionSyncJobOpts,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    // `createSyncJob`: a postponed job skips the duplicate check.
    if opts.process_after.is_none() {
        let duplicates_query = if user {
            QUEUED_DUPLICATES_FOR_USER
        } else {
            QUEUED_DUPLICATES_FOR_REPO
        };
        if let Some(row) = sqlx::query(duplicates_query)
            .bind(target_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            let existing_priority: i32 = sqlx::Row::try_get(&row, "priority")?;
            if existing_priority >= opts.priority {
                tx.commit().await?;
                return Ok(());
            }
            let existing_id: i32 = sqlx::Row::try_get(&row, "id")?;
            sqlx::query(CANCEL_QUEUED_JOB)
                .bind(existing_id)
                .bind(now)
                .bind(CANCELLATION_REASON_HIGHER_PRIORITY)
                .execute(&mut *tx)
                .await?;
        }
    }
    let triggered_by = if opts.triggered_by_user_id == 0 {
        None
    } else {
        Some(opts.triggered_by_user_id)
    };
    let insert = if user {
        CREATE_USER_SYNC_JOB_WITH_OPTS
    } else {
        CREATE_REPO_SYNC_JOB_WITH_OPTS
    };
    let mut query = sqlx::query(insert)
        .bind(&opts.reason)
        .bind(triggered_by)
        .bind(opts.process_after);
    if user {
        query = query.bind(target_id);
    } else {
        query = query.bind(target_id);
    }
    let query = if user {
        query.bind(opts.priority).bind(opts.invalidate_caches)
    } else {
        query.bind(opts.priority).bind(opts.invalidate_caches)
    };
    let inserted = query.fetch_optional(&mut *tx).await?;
    if inserted.is_none() {
        tx.rollback().await?;
        return Err(sqlx::Error::RowNotFound);
    }
    tx.commit().await
}

/// `CreateUserSyncJob` with the full `PermissionSyncJobOpts` (M9.48a).
pub async fn create_user_sync_job_with_opts(
    pool: &PgPool,
    user_id: i32,
    opts: &CreatePermissionSyncJobOpts,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    create_with_opts(pool, user_id, true, opts, now).await
}

/// `CreateRepoSyncJob` with the full `PermissionSyncJobOpts` (M9.48a).
pub async fn create_repo_sync_job_with_opts(
    pool: &PgPool,
    repo_id: i32,
    opts: &CreatePermissionSyncJobOpts,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    create_with_opts(pool, repo_id, false, opts, now).await
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
