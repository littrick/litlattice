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
