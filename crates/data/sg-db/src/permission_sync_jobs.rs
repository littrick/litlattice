#![forbid(unsafe_code)]

//! `internal/database/permission_sync_jobs.go` + the cleaner query in
//! `cmd/worker/internal/permissions/perms_syncer_cleaner.go`.
//!
//! Only the cleanup statement is ported; the SQL is copied verbatim apart
//! from binding `jobsToKeep` as `$1` instead of formatting it with `%d`.

use sqlx::postgres::PgPool;

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
