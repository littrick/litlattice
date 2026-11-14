#![forbid(unsafe_code)]

//! Store operations used by the `codeintel-upload-janitor` worker job
//! (`internal/codeintel/uploads/internal/store`):
//!
//! * `DeleteUploadsWithoutRepository`, `DeleteUploadsStuckUploading`,
//!   `SoftDeleteExpiredUploads`, `SoftDeleteExpiredUploadsViaTraversal`;
//! * `DeleteOldAuditLogs`, `ReconcileCandidates`,
//!   `GetUploadsByIDsAllowDeleted` (ids only), `HardDeleteUploadsByIDs`;
//! * `DeleteAutoIndexJobsWithoutRepository`, `ExpireFailedRecords`;
//! * `GetStaleSourcedCommits` / `DeleteSourcedCommits` /
//!   `UpdateSourcedCommits` and `ProcessSourcedCommits` /
//!   `ProcessStaleSourcedCommits`;
//! * `SourcedCommitsWithoutCommittedAt` / `UpdateCommittedAt` /
//!   `SetRepositoryAsDirty` (the `codeintel-upload-backfiller` job);
//! * `ReferencesForUpload` and a `GetUploadByID` subset (`id`, `indexer`) for
//!   the `codeintel-autoindexing-dependency-scheduler` handlers.
//!
//! Every SQL string is copied from the baseline store; migrations stay owned
//! by the Go `migrator`. Two deliberate equivalences:
//!
//! * The janitor runs with an internal actor, so `database.AuthzQueryConds`
//!   contributes `TRUE` and is omitted.
//! * `GetUploads(state="deleted", AllowExpired, AllowDeletedRepo)` is
//!   specialised to [`get_deleted_upload_ids`]: the janitor only reads the
//!   identifiers, and the fixed options collapse to
//!   `u.state = 'deleted' AND repo.blocked IS NULL` (no `NOT u.expired`, no
//!   `repo.deleted_at` condition).
//!
//! `basestore.SetLocal("codeintel.lsif_uploads_audit.reason", ...)` returns
//! `ErrNotInTransaction` unless the handle is in a transaction; the baseline
//! ignores that error. The audit reason is therefore only set by the
//! functions that run inside a transaction (deleted repository, soft deletes,
//! stale index commits), which is what this module reproduces.

use std::time::Duration;

use sqlx::postgres::{PgConnection, PgPool, PgTransaction};
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `deletedRepositoryGracePeriod` (30 minutes).
pub const DELETED_REPOSITORY_GRACE_PERIOD: Duration = Duration::from_secs(30 * 60);

/// `codeintel.lsif_uploads_audit.reason` (read by the audit trigger).
const AUDIT_REASON_GUC: &str = "codeintel.lsif_uploads_audit.reason";

/// `SourcedCommits`: a repository and the commits sourced from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcedCommits {
    pub repository_id: i64,
    pub repository_name: String,
    pub commits: Vec<String>,
}

/// The `shouldDelete` callback of `ProcessSourcedCommits` and
/// `ProcessStaleSourcedCommits` (a gitserver `ResolveRevision` check in the
/// job).
#[async_trait::async_trait]
pub trait CommitDeleter: Send + Sync {
    async fn should_delete(
        &self,
        repository_id: i64,
        repository_name: &str,
        commit: &str,
    ) -> Result<bool, String>;
}

/// `store.DeleteUploadsWithoutRepository`. Returns `(totalCount, count)`.
pub async fn delete_uploads_without_repository(
    pool: &PgPool,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_audit_reason(
        &mut tx,
        "upload associated with repository not known to this instance",
    )
    .await?;

    let rows = sqlx::query(DELETE_UPLOADS_WITHOUT_REPOSITORY_QUERY)
        .bind(now)
        .bind(DELETED_REPOSITORY_GRACE_PERIOD.as_secs() as i64)
        .fetch_all(&mut *tx)
        .await?;

    let (total, count) = scan_counts_with_total_count(&rows);
    tx.commit().await?;
    Ok((total, count))
}

/// `store.DeleteUploadsStuckUploading`.
pub async fn delete_uploads_stuck_uploading(
    pool: &PgPool,
    uploaded_before: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(DELETE_UPLOADS_STUCK_UPLOADING_QUERY)
        .bind(uploaded_before)
        .fetch_one(pool)
        .await?;
    let count: i64 = row.try_get(0)?;
    Ok((count, count))
}

/// `store.SoftDeleteExpiredUploads`. Returns `(scannedCount, alteredCount)`.
pub async fn soft_delete_expired_uploads(
    pool: &PgPool,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    // `DEBUG_PRECISE_CODE_INTEL_SOFT_DELETE_BAIL_OUT`
    if !std::env::var("DEBUG_PRECISE_CODE_INTEL_SOFT_DELETE_BAIL_OUT")
        .unwrap_or_default()
        .is_empty()
    {
        return Ok((0, 0));
    }

    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "soft-deleting expired uploads").await?;

    let rows = sqlx::query(SOFT_DELETE_EXPIRED_UPLOADS_QUERY)
        .bind(batch_size)
        .fetch_all(&mut *tx)
        .await?;
    let (scanned, count, repositories) = scan_counts_and_repositories(&rows);
    mark_repositories_dirty(&mut tx, &repositories).await?;
    tx.commit().await?;
    Ok((scanned, count))
}

/// `store.SoftDeleteExpiredUploadsViaTraversal`.
pub async fn soft_delete_expired_uploads_via_traversal(
    pool: &PgPool,
    traversal_limit: i64,
) -> Result<(i64, i64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_audit_reason(
        &mut tx,
        "soft-deleting expired uploads (via graph traversal)",
    )
    .await?;

    let rows = sqlx::query(SOFT_DELETE_EXPIRED_UPLOADS_VIA_TRAVERSAL_QUERY)
        .bind(traversal_limit)
        .bind(traversal_limit)
        .fetch_all(&mut *tx)
        .await?;
    let (scanned, count, repositories) = scan_counts_and_repositories(&rows);
    mark_repositories_dirty(&mut tx, &repositories).await?;
    tx.commit().await?;
    Ok((scanned, count))
}

/// `store.DeleteOldAuditLogs`.
pub async fn delete_old_audit_logs(
    pool: &PgPool,
    max_age: Duration,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(DELETE_OLD_AUDIT_LOGS_QUERY)
        .bind(now)
        .bind(max_age.as_secs() as i64)
        .fetch_one(pool)
        .await?;
    let count: i64 = row.try_get(0)?;
    Ok((count, count))
}

/// `store.ReconcileCandidates` (frontend schema).
pub async fn reconcile_candidates(pool: &PgPool, batch_size: i64) -> Result<Vec<i64>, sqlx::Error> {
    let rows = sqlx::query(RECONCILE_CANDIDATES_QUERY)
        .bind(batch_size)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| i64::from(row.get::<i32, _>(0)))
        .collect())
}

/// `store.GetUploadsByIDsAllowDeleted` reduced to the identifiers the
/// reconciler needs.
pub async fn existing_upload_ids(pool: &PgPool, ids: &[i64]) -> Result<Vec<i64>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(EXISTING_UPLOAD_IDS_QUERY)
        .bind(ids)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| i64::from(row.get::<i32, _>(0)))
        .collect())
}

/// `store.HardDeleteUploadsByIDs`.
pub async fn hard_delete_uploads_by_ids(pool: &PgPool, ids: &[i64]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(HARD_DELETE_UPLOADS_BY_IDS_QUERY)
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

/// `store.GetUploads` specialised to `state = 'deleted'` (see the module
/// documentation). Returns `(ids, totalCount)`.
pub async fn get_deleted_upload_ids(
    pool: &PgPool,
    limit: i64,
) -> Result<(Vec<i64>, i64), sqlx::Error> {
    let rows = sqlx::query(GET_DELETED_UPLOAD_IDS_QUERY)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    let ids = rows
        .iter()
        .map(|row| i64::from(row.get::<i32, _>(0)))
        .collect();
    let row = sqlx::query(GET_DELETED_UPLOADS_COUNT_QUERY)
        .fetch_one(pool)
        .await?;
    let total: i64 = row.try_get(0)?;
    Ok((ids, total))
}

/// One upload returned by [`get_uploads_for_expiration`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpirationUpload {
    pub id: i32,
    pub uploaded_at: OffsetDateTime,
}

/// `store.SetRepositoriesForRetentionScan`: claims a batch of repositories
/// that have completed uploads and a fresh commit graph, skipping any that
/// were claimed within `process_delay`. Returns the claimed repository
/// identifiers.
pub async fn set_repositories_for_retention_scan(
    pool: &PgPool,
    process_delay: std::time::Duration,
    limit: i64,
    now: OffsetDateTime,
) -> Result<Vec<i32>, sqlx::Error> {
    // `sqlf.Sprintf(repositoryIDsForRetentionScanQuery, now,
    // int(processDelay/time.Second), limit, now, now)`: the delay (seconds)
    // and the limit are inlined by sqlf; the three timestamps are values.
    let query = REPOSITORY_IDS_FOR_RETENTION_SCAN_QUERY
        .replace("{delay_seconds}", &process_delay.as_secs().to_string())
        .replace("{limit}", &limit.to_string());
    let rows = sqlx::query(&query)
        .bind(now)
        .bind(now)
        .bind(now)
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(|row| row.get::<i32, _>(0)).collect())
}

/// `store.GetUploads` specialised to the expirer's options: completed,
/// non-expired uploads of one repository that are present in the commit graph
/// (`finished_at < lsif_dirty_repositories.updated_at`) and were not scanned
/// after `last_retention_scan_before`. The projection is narrowed to the two
/// fields `isUploadProtectedByPolicy` reads; the `left join (uploadRankQuery)`
/// and the `visible_at_tip` projection of the baseline select cannot change
/// the row set (the rank subquery only covers `state = 'queued'` rows) and are
/// therefore omitted.
pub async fn get_uploads_for_expiration(
    pool: &PgPool,
    repository_id: i32,
    limit: i64,
    last_retention_scan_before: OffsetDateTime,
) -> Result<Vec<ExpirationUpload>, sqlx::Error> {
    let rows = sqlx::query(GET_UPLOADS_FOR_EXPIRATION_QUERY)
        .bind(repository_id)
        .bind(last_retention_scan_before)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| ExpirationUpload {
            id: row.get(0),
            uploaded_at: row.get(1),
        })
        .collect())
}

/// `store.GetCommitsVisibleToUpload`: returns `(commits, nextToken)` where
/// `nextToken` is the last commit of the page (when the page is non-empty).
pub async fn get_commits_visible_to_upload(
    pool: &PgPool,
    upload_id: i64,
    limit: i64,
    token: Option<&str>,
) -> Result<(Vec<String>, Option<String>), sqlx::Error> {
    let after = token.unwrap_or("");
    let query = COMMITS_VISIBLE_TO_UPLOAD_QUERY.replace("{limit}", &limit.to_string());
    let rows = sqlx::query(&query)
        .bind(upload_id.to_string())
        .bind(after)
        .fetch_all(pool)
        .await?;
    let commits: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    let next_token = commits.last().cloned();
    Ok((commits, next_token))
}

/// `store.UpdateUploadRetention`: stamps `last_retention_scan_at` on the
/// protected uploads and sets `expired = TRUE` on the expired ones, in a
/// single transaction. The identifiers are sorted first so the two `UPDATE`s
/// take row locks in a deterministic order (the baseline does the same).
pub async fn update_upload_retention(
    pool: &PgPool,
    protected_ids: &mut [i32],
    expired_ids: &mut [i32],
) -> Result<(), sqlx::Error> {
    protected_ids.sort_unstable();
    expired_ids.sort_unstable();

    let mut tx = pool.begin().await?;
    if !protected_ids.is_empty() {
        sqlx::query("UPDATE lsif_uploads SET last_retention_scan_at = $1 WHERE id = ANY($2)")
            .bind(sqlx::types::time::OffsetDateTime::now_utc())
            .bind(&*protected_ids)
            .execute(&mut *tx)
            .await?;
    }
    if !expired_ids.is_empty() {
        sqlx::query("UPDATE lsif_uploads SET expired = TRUE WHERE id = ANY($1)")
            .bind(&*expired_ids)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// `store.DeleteAutoIndexJobsWithoutRepository`.
pub async fn delete_auto_index_jobs_without_repository(
    pool: &PgPool,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(DELETE_INDEXES_WITHOUT_REPOSITORY_QUERY)
        .bind(now)
        .bind(DELETED_REPOSITORY_GRACE_PERIOD.as_secs() as i64)
        .fetch_all(&mut *tx)
        .await?;
    let (total, count) = scan_counts_with_total_count(&rows);
    tx.commit().await?;
    Ok((total, count))
}

/// `store.ExpireFailedRecords`. Returns `(scanned, deleted)`.
pub async fn expire_failed_records(
    pool: &PgPool,
    batch_size: i64,
    failed_index_max_age: Duration,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(EXPIRE_FAILED_RECORDS_QUERY)
        .bind(now)
        .bind(failed_index_max_age.as_secs() as i64)
        .bind(batch_size)
        .fetch_one(pool)
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

/// `store.GetStaleSourcedCommits`: upload-sourced commits ordered by
/// repository id, then commit.
pub async fn get_stale_sourced_commits(
    pool: &PgPool,
    minimum_time_since_last_check: Duration,
    limit: i64,
    now: OffsetDateTime,
) -> Result<Vec<SourcedCommits>, sqlx::Error> {
    let rows = sqlx::query(GET_STALE_SOURCED_COMMITS_QUERY)
        .bind(now)
        .bind(minimum_time_since_last_check.as_secs() as i64)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(scan_sourced_commits(&rows))
}

/// `store.UpdateSourcedCommits`: bumps `commit_last_checked_at` for every
/// upload of the given repository and commit.
pub async fn update_sourced_commits(
    pool: &PgPool,
    repository_id: i64,
    commit: &str,
    now: OffsetDateTime,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(UPDATE_SOURCED_COMMITS_QUERY)
        .bind(repository_id)
        .bind(commit)
        .bind(now)
        .fetch_one(pool)
        .await?;
    row.try_get(0)
}

/// `store.SourcedCommitsWithoutCommittedAt`: completed uploads whose
/// `codeintel_commit_dates` row is missing, grouped by repository. The
/// backfiller reads it inside its transaction (`store.WithTransaction`).
pub async fn sourced_commits_without_committed_at(
    conn: &mut PgConnection,
    batch_size: i64,
) -> Result<Vec<SourcedCommits>, sqlx::Error> {
    let rows = sqlx::query(SOURCED_COMMITS_WITHOUT_COMMITTED_AT_QUERY)
        .bind(batch_size)
        .fetch_all(&mut *conn)
        .await?;
    Ok(scan_sourced_commits(&rows))
}

/// `store.UpdateCommittedAt`: records the commit date for all uploads
/// attached to the repository and commit (`ON CONFLICT DO NOTHING`).
pub async fn update_committed_at(
    conn: &mut PgConnection,
    repository_id: i64,
    commit: &str,
    commit_date_string: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(UPDATE_COMMITTED_AT_QUERY)
        .bind(repository_id)
        .bind(commit)
        .bind(commit_date_string)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// `store.SetRepositoryAsDirty` (`setRepositoryAsDirtyWithTx`): bumps the
/// dirty token inside the caller's transaction.
pub async fn set_repository_as_dirty(
    conn: &mut PgConnection,
    repository_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(SET_REPOSITORY_AS_DIRTY_QUERY)
        .bind(repository_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// `store.DeleteSourcedCommits`. Returns `(uploadsUpdated, uploadsDeleted)`.
pub async fn delete_sourced_commits(
    pool: &PgPool,
    repository_id: i64,
    commit: &str,
    maximum_commit_lag: Duration,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(DELETE_SOURCED_COMMITS_QUERY)
        .bind(repository_id)
        .bind(commit)
        .bind(now)
        .bind(maximum_commit_lag.as_secs() as i64)
        .bind(now)
        .fetch_one(pool)
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

/// `store.ProcessSourcedCommits` (uploads). Returns `(numCommits,
/// numUploadsDeleted)`.
pub async fn process_sourced_commits(
    pool: &PgPool,
    minimum_time_since_last_check: Duration,
    commit_resolver_maximum_commit_lag: Duration,
    limit: i64,
    deleter: &dyn CommitDeleter,
    now: OffsetDateTime,
) -> Result<(i64, i64), String> {
    let sourced_uploads =
        get_stale_sourced_commits(pool, minimum_time_since_last_check, limit, now)
            .await
            .map_err(|err| err.to_string())?;

    let mut num_deleted = 0;
    let mut num_commits = 0;
    for sourced in &sourced_uploads {
        for commit in &sourced.commits {
            num_commits += 1;

            let should_delete = deleter
                .should_delete(sourced.repository_id, &sourced.repository_name, commit)
                .await?;

            if should_delete {
                let (_, uploads_deleted) = delete_sourced_commits(
                    pool,
                    sourced.repository_id,
                    commit,
                    commit_resolver_maximum_commit_lag,
                    now,
                )
                .await
                .map_err(|err| err.to_string())?;
                num_deleted += uploads_deleted;
            }

            update_sourced_commits(pool, sourced.repository_id, commit, now)
                .await
                .map_err(|err| err.to_string())?;
        }
    }

    Ok((num_commits, num_deleted))
}

/// `store.ProcessStaleSourcedCommits` (indexes). The whole scan runs in one
/// transaction, exactly like the baseline, so the per-repository index
/// updates are serialised with the candidate read.
pub async fn process_stale_sourced_commits(
    pool: &PgPool,
    minimum_time_since_last_check: Duration,
    commit_resolver_batch_size: i64,
    deleter: &dyn CommitDeleter,
    now: OffsetDateTime,
) -> Result<(i64, i64), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let rows = sqlx::query(STALE_INDEX_SOURCED_COMMITS_QUERY)
        .bind(now)
        .bind(minimum_time_since_last_check.as_secs() as i64)
        .bind(commit_resolver_batch_size)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let stale_indexes = scan_sourced_commits(&rows);

    let mut total_deleted = 0;
    for sourced in &stale_indexes {
        let mut keep = Vec::new();
        let mut remove = Vec::new();
        for commit in &sourced.commits {
            if deleter
                .should_delete(sourced.repository_id, &sourced.repository_name, commit)
                .await?
            {
                remove.push(commit.clone());
            } else {
                keep.push(commit.clone());
            }
        }

        set_audit_reason_tx(&mut tx, "upload associated with unknown commit")
            .await
            .map_err(|err| err.to_string())?;

        let row = sqlx::query(UPDATE_SOURCED_COMMITS_QUERY2)
            .bind(sourced.repository_id)
            .bind(&keep)
            .bind(&remove)
            .bind(now)
            .bind(&keep)
            .bind(&remove)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;
        total_deleted += row.try_get::<i64, _>(0).map_err(|err| err.to_string())?;
    }

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok((stale_indexes.len() as i64, total_deleted))
}

//
//

async fn set_audit_reason(tx: &mut PgTransaction<'_>, reason: &str) -> Result<(), sqlx::Error> {
    set_audit_reason_tx(tx, reason).await
}

async fn set_audit_reason_tx(tx: &mut PgTransaction<'_>, reason: &str) -> Result<(), sqlx::Error> {
    // `SET LOCAL "key" TO "value"`; the parameterised form is equivalent and
    // avoids quoting issues with the reason text.
    sqlx::query("SELECT set_config($1, $2, true)")
        .bind(AUDIT_REASON_GUC)
        .bind(reason)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `SELECT setRepositoryAsDirtyWithTx` for every affected repository (the
/// baseline iterates over the map, so the order does not matter).
async fn mark_repositories_dirty(
    tx: &mut PgTransaction<'_>,
    repositories: &[i64],
) -> Result<(), sqlx::Error> {
    for repository_id in repositories {
        sqlx::query(SET_REPOSITORY_AS_DIRTY_QUERY)
            .bind(repository_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// `scanCountsWithTotalCount` / `scanCountsAndTotalCount`: `(total, id,
/// count)` rows. The total column repeats on every row.
fn scan_counts_with_total_count(rows: &[sqlx::postgres::PgRow]) -> (i64, i64) {
    let mut total = 0;
    let mut count = 0;
    for row in rows {
        total = row.get::<i64, _>(0);
        count += row.get::<i64, _>(2);
    }
    (total, count)
}

/// Same plus the distinct repository ids (for `setRepositoryAsDirty`).
fn scan_counts_and_repositories(rows: &[sqlx::postgres::PgRow]) -> (i64, i64, Vec<i64>) {
    let mut total = 0;
    let mut count = 0;
    let mut repositories = Vec::new();
    for row in rows {
        total = row.get::<i64, _>(0);
        let repository_id = i64::from(row.get::<i32, _>(1));
        if !repositories.contains(&repository_id) {
            repositories.push(repository_id);
        }
        count += row.get::<i64, _>(2);
    }
    (total, count, repositories)
}

/// `scanSourcedCommits`: group by repository id, commits sorted, repositories
/// sorted by id.
fn scan_sourced_commits(rows: &[sqlx::postgres::PgRow]) -> Vec<SourcedCommits> {
    let mut result: Vec<SourcedCommits> = Vec::new();
    for row in rows {
        let repository_id: i64 = i64::from(row.get::<i32, _>(0));
        let repository_name: String = row.get(1);
        let commit: String = row.get(2);

        match result
            .iter_mut()
            .find(|entry| entry.repository_id == repository_id)
        {
            Some(entry) => entry.commits.push(commit),
            None => result.push(SourcedCommits {
                repository_id,
                repository_name,
                commits: vec![commit],
            }),
        }
    }
    for entry in &mut result {
        entry.commits.sort();
    }
    result.sort_by_key(|entry| entry.repository_id);
    result
}

//
// The SQL below is transcribed from
// `internal/codeintel/uploads/internal/store/{cleanup,expiration,uploads}.go`.
//

const DELETE_UPLOADS_WITHOUT_REPOSITORY_QUERY: &str = r#"
WITH
candidates AS (
	SELECT u.id
	FROM repo r
	JOIN lsif_uploads u ON u.repository_id = r.id
	WHERE
		$1 - r.deleted_at >= $2 * interval '1 second' OR
		r.blocked IS NOT NULL

	ORDER BY u.id FOR UPDATE
),
deleted AS (
	UPDATE lsif_uploads u
	SET state = 'deleted'
	WHERE u.id IN (SELECT id FROM candidates)
	RETURNING u.id, u.repository_id
)
SELECT (SELECT COUNT(*) FROM candidates), d.repository_id, COUNT(*) FROM deleted d GROUP BY d.repository_id
"#;

const DELETE_UPLOADS_STUCK_UPLOADING_QUERY: &str = r#"
WITH
candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.state = 'uploading' AND u.uploaded_at < $1

	ORDER BY u.id FOR UPDATE
),
deleted AS (
	UPDATE lsif_uploads u
	SET state = 'deleted'
	WHERE id IN (SELECT id FROM candidates)
	RETURNING u.repository_id
)
SELECT COUNT(*) FROM deleted
"#;

const SOFT_DELETE_EXPIRED_UPLOADS_QUERY: &str = r#"
WITH

expired_uploads AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.state = 'completed' AND u.expired
	ORDER BY u.last_referenced_scan_at NULLS FIRST, u.finished_at, u.id
	LIMIT $1
),

packages_defined_by_target_uploads AS (
	SELECT p.scheme, p.manager, p.name, p.version
	FROM lsif_packages p
	WHERE p.dump_id IN (SELECT id FROM expired_uploads)
),

ranked_uploads_providing_packages AS (
	SELECT
		u.id,
		p.scheme,
		p.manager,
		p.name,
		p.version,
		rank() OVER (
			PARTITION BY
				p.scheme, p.manager, p.name, p.version,
				u.repository_id, u.indexer, u.root
			ORDER BY
				(SELECT cd.committed_at FROM codeintel_commit_dates cd WHERE cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')) NULLS LAST,
				u.id
		) AS rank
	FROM lsif_uploads u
	LEFT JOIN lsif_packages p ON p.dump_id = u.id
	WHERE
		(
			u.id = ANY (SELECT id FROM expired_uploads) OR

			(p.scheme, p.manager, p.name, p.version) IN (
				SELECT p.scheme, p.manager, p.name, p.version
				FROM packages_defined_by_target_uploads p
			)
		) AND

		u.state = 'completed'
),

referenced_uploads_providing_package_canonically AS (
	SELECT ru.id
	FROM ranked_uploads_providing_packages ru
	WHERE
		ru.id IN (SELECT id FROM expired_uploads) AND

		ru.rank = 1 AND

		EXISTS (
			SELECT 1
			FROM lsif_references r
			WHERE
				r.scheme = ru.scheme AND
				r.manager = ru.manager AND
				r.name = ru.name AND
				r.version = ru.version AND
				r.dump_id != ru.id
		)
),

candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE
		u.id IN (SELECT id FROM expired_uploads) AND
		NOT EXISTS (
			SELECT 1
			FROM referenced_uploads_providing_package_canonically pkg_refcount
			WHERE pkg_refcount.id = u.id
		)
),
locked_uploads AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.id IN (SELECT id FROM expired_uploads)
	ORDER BY u.id FOR UPDATE
),
updated AS (
	UPDATE lsif_uploads u

	SET
		last_referenced_scan_at = NOW(),

		state = CASE WHEN u.id IN (SELECT id FROM candidates) THEN 'deleting' ELSE 'completed' END
	WHERE u.id IN (SELECT id FROM locked_uploads)
	RETURNING u.id, u.repository_id, u.state
)

SELECT (SELECT COUNT(*) FROM expired_uploads), u.repository_id, COUNT(*) FROM updated u WHERE u.state = 'deleting' GROUP BY u.repository_id
"#;

const SOFT_DELETE_EXPIRED_UPLOADS_VIA_TRAVERSAL_QUERY: &str = r#"
WITH RECURSIVE

root_upload_and_packages AS (
	SELECT * FROM (
		SELECT
			u.id,
			u.expired,
			u.last_traversal_scan_at,
			u.finished_at,
			p.scheme,
			p.manager,
			p.name,
			p.version,
			rank() OVER (
				PARTITION BY
					p.scheme, p.manager, p.name, p.version,
					u.repository_id, u.indexer, u.root
				ORDER BY
					(SELECT cd.committed_at FROM codeintel_commit_dates cd WHERE cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')) NULLS LAST,
					u.id
			) AS rank
		FROM lsif_uploads u
		LEFT JOIN lsif_packages p ON p.dump_id = u.id
		WHERE u.state = 'completed' AND u.expired
	) s

	WHERE s.rank = 1 AND EXISTS (
		SELECT 1
		FROM lsif_references r
		WHERE
			r.scheme = s.scheme AND
			r.manager = s.manager AND
			r.name = s.name AND
			r.version = s.version AND
			r.dump_id != s.id
		)
	ORDER BY s.last_traversal_scan_at NULLS FIRST, s.finished_at, s.id
	LIMIT 1
),

transitive_dependents(id, expired, scheme, manager, name, version) AS MATERIALIZED (
	(
		SELECT up.id, up.expired, up.scheme, up.manager, up.name, up.version FROM root_upload_and_packages up
	) UNION (
		SELECT s.id, s.expired, s.scheme, s.manager, s.name, s.version
		FROM (
			SELECT
				u.id,
				u.expired,
				p.scheme,
				p.manager,
				p.name,
				p.version,
				rank() OVER (
					PARTITION BY
						p.scheme, p.manager, p.name, p.version,
						u.repository_id, u.indexer, u.root
					ORDER BY
						(SELECT cd.committed_at FROM codeintel_commit_dates cd WHERE cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')) NULLS LAST,
						u.id
				) AS rank
			FROM transitive_dependents d
			JOIN lsif_references r ON
				r.scheme = d.scheme AND
				r.manager = d.manager AND
				r.name = d.name AND
				r.version = d.version AND
				r.dump_id != d.id
			JOIN lsif_uploads u ON u.id = r.dump_id
			JOIN lsif_packages p ON p.dump_id = u.id
			WHERE
				u.state = 'completed' AND
				d.expired
		) s

		WHERE s.rank = 1
	)
),

candidates AS (
	SELECT * FROM transitive_dependents d
	LIMIT ($1 + 1)
),
locked_uploads AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.id IN (SELECT id FROM candidates)
	ORDER BY u.id FOR UPDATE
),
updated AS (
	UPDATE lsif_uploads u

	SET
		last_traversal_scan_at = NOW(),

		state = CASE
			WHEN (SELECT bool_and(d.expired) AND COUNT(*) <= $2 FROM candidates d) THEN 'deleting'
			ELSE 'completed'
		END
	WHERE u.id IN (SELECT id FROM locked_uploads)
	RETURNING u.id, u.repository_id, u.state
)

SELECT (SELECT COUNT(*) FROM candidates), u.repository_id, COUNT(*) FROM updated u WHERE u.state = 'deleting' GROUP BY u.repository_id
"#;

const SET_REPOSITORY_AS_DIRTY_QUERY: &str = r#"
INSERT INTO lsif_dirty_repositories (repository_id, dirty_token, update_token)
VALUES ($1, 1, 0)
ON CONFLICT (repository_id) DO UPDATE SET
    dirty_token = lsif_dirty_repositories.dirty_token + 1,
    set_dirty_at = CASE
        WHEN lsif_dirty_repositories.update_token = lsif_dirty_repositories.dirty_token THEN NOW()
        ELSE lsif_dirty_repositories.set_dirty_at
    END
"#;

const SOURCED_COMMITS_WITHOUT_COMMITTED_AT_QUERY: &str = r#"
SELECT u.repository_id, r.name, u.commit
FROM lsif_uploads u
JOIN repo r ON r.id = u.repository_id
LEFT JOIN codeintel_commit_dates cd ON cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')
WHERE u.state = 'completed' AND cd.committed_at IS NULL
GROUP BY u.repository_id, r.name, u.commit
ORDER BY repository_id, commit
LIMIT $1
"#;

const UPDATE_COMMITTED_AT_QUERY: &str = r#"
INSERT INTO codeintel_commit_dates(repository_id, commit_bytea, committed_at) VALUES ($1, decode($2, 'hex'), $3::timestamptz) ON CONFLICT DO NOTHING
"#;

const DELETE_OLD_AUDIT_LOGS_QUERY: &str = r#"
WITH deleted AS (
	DELETE FROM lsif_uploads_audit_logs
	WHERE $1 - log_timestamp > ($2 * '1 second'::interval)
	RETURNING upload_id
)
SELECT count(*) FROM deleted
"#;

const RECONCILE_CANDIDATES_QUERY: &str = r#"
WITH
candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.state = 'completed'
	ORDER BY u.last_reconcile_at DESC NULLS FIRST, u.id
	LIMIT $1
),
locked_candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE id = ANY(SELECT id FROM candidates)
	ORDER BY u.id
	FOR UPDATE
)
UPDATE lsif_uploads
SET last_reconcile_at = NOW()
WHERE id = ANY(SELECT id FROM locked_candidates)
RETURNING id
"#;

const EXISTING_UPLOAD_IDS_QUERY: &str = r#"
SELECT u.id
FROM lsif_dumps_with_repository_name u
WHERE u.state != 'deleted' AND u.id = ANY($1)
"#;

const HARD_DELETE_UPLOADS_BY_IDS_QUERY: &str = r#"
WITH
locked_uploads AS (
	SELECT u.id, u.associated_index_id
	FROM lsif_uploads u
	WHERE u.id = ANY($1)
	ORDER BY u.id FOR UPDATE
),
delete_uploads AS (
	DELETE FROM lsif_uploads WHERE id IN (SELECT id FROM locked_uploads)
),
locked_indexes AS (
	SELECT u.id
	FROM lsif_indexes U
	WHERE u.id IN (SELECT associated_index_id FROM locked_uploads)
	ORDER BY u.id FOR UPDATE
)
DELETE FROM lsif_indexes WHERE id IN (SELECT id FROM locked_indexes)
"#;

const GET_DELETED_UPLOAD_IDS_QUERY: &str = r#"
SELECT u.id
FROM lsif_uploads u
JOIN repo ON repo.id = u.repository_id
WHERE u.state = 'deleted' AND repo.blocked IS NULL
ORDER BY u.uploaded_at DESC, u.id
LIMIT $1
"#;

const GET_DELETED_UPLOADS_COUNT_QUERY: &str = r#"
SELECT COUNT(*) AS count
FROM lsif_uploads u
JOIN repo ON repo.id = u.repository_id
WHERE u.state = 'deleted' AND repo.blocked IS NULL
"#;

const DELETE_INDEXES_WITHOUT_REPOSITORY_QUERY: &str = r#"
WITH
candidates AS (
	SELECT u.id
	FROM repo r
	JOIN lsif_indexes u ON u.repository_id = r.id
	WHERE
		$1 - r.deleted_at >= $2 * interval '1 second' OR
		r.blocked IS NOT NULL

	ORDER BY u.id FOR UPDATE
),
deleted AS (
	DELETE FROM lsif_indexes u
	WHERE id IN (SELECT id FROM candidates)
	RETURNING u.id, u.repository_id
)
SELECT (SELECT COUNT(*) FROM candidates), d.repository_id, COUNT(*) FROM deleted d GROUP BY d.repository_id
"#;

const EXPIRE_FAILED_RECORDS_QUERY: &str = r#"
WITH
ranked_indexes AS (
	SELECT
		u.*,
		RANK() OVER (
			PARTITION BY
				repository_id,
				root,
				indexer
			ORDER BY
				finished_at DESC
		) AS rank
	FROM lsif_indexes u
	WHERE
		u.state = 'failed' AND
		$1 - u.finished_at >= $2 * interval '1 second'
),
locked_indexes AS (
	SELECT i.id
	FROM lsif_indexes i
	JOIN ranked_indexes ri ON ri.id = i.id

	WHERE ri.rank != 1 OR EXISTS (
		SELECT 1
		FROM lsif_indexes i2
		WHERE
			i2.state = 'completed' AND
			i2.finished_at > i.finished_at AND
			i2.repository_id = i.repository_id AND
			i2.root = i.root AND
			i2.indexer = i.indexer
	)
	ORDER BY i.id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
del AS (
	DELETE FROM lsif_indexes
	WHERE id IN (SELECT id FROM locked_indexes)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM ranked_indexes),
	(SELECT COUNT(*) FROM del)
"#;

const GET_STALE_SOURCED_COMMITS_QUERY: &str = r#"
WITH
	candidates AS (
SELECT
	repository_id,
	commit,
	MAX(commit_last_checked_at) as max_last_checked_at
FROM lsif_uploads
WHERE
	state NOT IN ('deleted', 'deleting') AND
	($1 - commit_last_checked_at > ($2 * '1 second'::interval)) IS DISTINCT FROM FALSE
GROUP BY repository_id, commit
)
SELECT r.id, r.name, c.commit
FROM candidates c
JOIN repo r ON r.id = c.repository_id
ORDER BY MIN(c.max_last_checked_at) OVER (PARTITION BY c.repository_id), c.commit
LIMIT $3
"#;

const UPDATE_SOURCED_COMMITS_QUERY: &str = r#"
WITH
candidate_uploads AS (
SELECT u.id, u.state, u.uploaded_at
FROM lsif_uploads u
WHERE u.repository_id = $1 AND u.commit = $2

ORDER BY u.id FOR UPDATE
),
update_uploads AS (
	UPDATE lsif_uploads u
	SET commit_last_checked_at = $3
	WHERE id IN (SELECT id FROM candidate_uploads)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM update_uploads) AS num_uploads
"#;

const DELETE_SOURCED_COMMITS_QUERY: &str = r#"
WITH
candidate_uploads AS (
SELECT u.id, u.state, u.uploaded_at
FROM lsif_uploads u
WHERE u.repository_id = $1 AND u.commit = $2

ORDER BY u.id FOR UPDATE
),
tagged_candidate_uploads AS (
SELECT
	u.*,
	(u.state IN ('uploading', 'queued', 'processing') AND $3 - u.uploaded_at <= ($4 * '1 second'::interval)) AS protected
FROM candidate_uploads u
),
update_uploads AS (
	UPDATE lsif_uploads u
	SET commit_last_checked_at = $5
	WHERE EXISTS (SELECT 1 FROM tagged_candidate_uploads tu WHERE tu.id = u.id AND tu.protected)
	RETURNING 1
),
delete_uploads AS (
	UPDATE lsif_uploads u
	SET state = CASE WHEN u.state = 'completed' THEN 'deleting' ELSE 'deleted' END
	WHERE EXISTS (SELECT 1 FROM tagged_candidate_uploads tu WHERE tu.id = u.id AND NOT tu.protected)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM update_uploads) AS num_uploads_updated,
	(SELECT COUNT(*) FROM delete_uploads) AS num_uploads_deleted
"#;

const STALE_INDEX_SOURCED_COMMITS_QUERY: &str = r#"
WITH candidates AS (
	SELECT
		repository_id,
		commit,
		MAX(commit_last_checked_at) as max_last_checked_at
	FROM lsif_indexes
	WHERE
		state NOT IN ('deleted', 'deleting') AND
		($1 - commit_last_checked_at > ($2 * '1 second'::interval)) IS DISTINCT FROM FALSE
	GROUP BY repository_id, commit
)
SELECT r.id, r.name, c.commit
FROM candidates c
JOIN repo r ON r.id = c.repository_id
ORDER BY MIN(c.max_last_checked_at) OVER (PARTITION BY c.repository_id), c.commit
LIMIT $3
"#;

const UPDATE_SOURCED_COMMITS_QUERY2: &str = r#"
WITH
candidate_indexes AS (
	SELECT u.id
	FROM lsif_indexes u
	WHERE
		u.repository_id = $1 AND
		(
			u.commit = ANY($2) OR
			u.commit = ANY($3)
		)

	ORDER BY u.id FOR UPDATE
),
update_indexes AS (
	UPDATE lsif_indexes
	SET commit_last_checked_at = $4
	WHERE id IN (SELECT id FROM candidate_indexes WHERE commit = ANY($5))
	RETURNING 1
),
delete_indexes AS (
	DELETE FROM lsif_indexes
	WHERE id IN (SELECT id FROM candidate_indexes WHERE commit = ANY($6))
	RETURNING 1
)
SELECT COUNT(*) FROM delete_indexes
"#;

/// `repositoryIDsForRetentionScanQuery` (the expirer's
/// `SetRepositoriesForRetentionScan`). `{delay_seconds}` and `{limit}` are
/// inlined like sqlf's integer parameters; the three timestamps are bound.
const REPOSITORY_IDS_FOR_RETENTION_SCAN_QUERY: &str = r#"
WITH candidate_repositories AS (
	SELECT DISTINCT u.repository_id AS id
	FROM lsif_uploads u
	WHERE u.state = 'completed'
),
repositories AS (
	SELECT cr.id
	FROM candidate_repositories cr
	LEFT JOIN lsif_last_retention_scan lrs ON lrs.repository_id = cr.id
	JOIN lsif_dirty_repositories dr ON dr.repository_id = cr.id

	-- Ignore records that have been checked recently. Note this condition is
	-- true for a null last_retention_scan_at (which has never been checked).
	WHERE ($1 - lrs.last_retention_scan_at > ({delay_seconds} * '1 second'::interval)) IS DISTINCT FROM FALSE
	AND dr.update_token = dr.dirty_token
	ORDER BY
		lrs.last_retention_scan_at NULLS FIRST,
		cr.id -- tie breaker
	LIMIT {limit}
)
INSERT INTO lsif_last_retention_scan (repository_id, last_retention_scan_at)
SELECT r.id, $2::timestamp FROM repositories r
ON CONFLICT (repository_id) DO UPDATE
SET last_retention_scan_at = $3
RETURNING repository_id
"#;

/// `store.GetUploads`' conditions for the expirer's option set (see
/// [`get_uploads_for_expiration`]).
const GET_UPLOADS_FOR_EXPIRATION_QUERY: &str = r#"
SELECT u.id, u.uploaded_at
FROM lsif_uploads u
JOIN repo ON repo.id = u.repository_id
WHERE
	u.repository_id = $1 AND
	u.state = 'completed' AND
	u.finished_at < (SELECT updated_at FROM lsif_dirty_repositories ldr WHERE ldr.repository_id = u.repository_id) AND
	(u.last_retention_scan_at IS NULL OR u.last_retention_scan_at < $2) AND
	NOT u.expired AND
	repo.deleted_at IS NULL AND
	repo.blocked IS NULL
ORDER BY uploaded_at, id DESC
LIMIT $3 OFFSET 0
"#;

/// `commitsVisibleToUploadQuery`. The upload identifier is compared against
/// the JSONB key (`nu.uploads ? %s`), hence the text bind.
const COMMITS_VISIBLE_TO_UPLOAD_QUERY: &str = r#"
WITH
direct_commits AS (
	SELECT nu.repository_id, nu.commit_bytea
	FROM lsif_nearest_uploads nu
	WHERE nu.uploads ? $1
),
linked_commits AS (
	SELECT ul.commit_bytea
	FROM direct_commits dc
	JOIN lsif_nearest_uploads_links ul
	ON
		ul.repository_id = dc.repository_id AND
		ul.ancestor_commit_bytea = dc.commit_bytea
),
combined_commits AS (
	SELECT dc.commit_bytea FROM direct_commits dc
	UNION ALL
	SELECT lc.commit_bytea FROM linked_commits lc
)
SELECT encode(c.commit_bytea, 'hex') as commit
FROM combined_commits c
WHERE decode($2, 'hex') < c.commit_bytea
ORDER BY c.commit_bytea
LIMIT {limit}
"#;

/// `store.ReferencesForUpload` scans a `PackageReferenceScanner`: one row per
/// `lsif_references` entry, ordered by `(scheme, manager, name, version)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadReference {
    pub dump_id: i32,
    pub scheme: String,
    pub manager: String,
    pub name: String,
    pub version: String,
}

const REFERENCES_FOR_UPLOAD_QUERY: &str = "
SELECT r.dump_id, r.scheme, r.manager, r.name, r.version
FROM lsif_references r
WHERE dump_id = $1
ORDER BY r.scheme, r.manager, r.name, r.version
";

/// `store.ReferencesForUpload`: the upload's package references in scan
/// order. The baseline streams the rows; the handlers consume the whole scan,
/// so the rows are returned as a vector.
pub async fn references_for_upload(
    pool: &PgPool,
    upload_id: i32,
) -> Result<Vec<UploadReference>, sqlx::Error> {
    let rows = sqlx::query(REFERENCES_FOR_UPLOAD_QUERY)
        .bind(upload_id)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(UploadReference {
                dump_id: row.try_get("dump_id")?,
                scheme: row.try_get("scheme")?,
                manager: row.try_get("manager")?,
                name: row.try_get("name")?,
                version: row.try_get("version")?,
            })
        })
        .collect()
}

/// The columns of `shared.Upload` the dependency-scheduler handlers read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadHeader {
    pub id: i32,
    pub indexer: String,
}

/// `getUploadByIDQuery` reduced to `id` + `indexer`, with the baseline's
/// `repo.deleted_at IS NULL AND u.state != 'deleted'` conditions. The handlers
/// run with an internal actor, so `AuthzQueryConds` is `TRUE` and omitted
/// (`scanFirstUpload` returns `exists == false` when no row matches).
const GET_UPLOAD_BY_ID_QUERY: &str = "
SELECT u.id, u.indexer
FROM lsif_uploads u
JOIN repo ON repo.id = u.repository_id
WHERE repo.deleted_at IS NULL AND u.state != 'deleted' AND u.id = $1
";

/// `store.GetUploadByID` subset used by `shouldIndexDependencies`.
pub async fn get_upload_by_id(pool: &PgPool, id: i32) -> Result<Option<UploadHeader>, sqlx::Error> {
    let row = sqlx::query(GET_UPLOAD_BY_ID_QUERY)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    row.map(|row| {
        Ok(UploadHeader {
            id: row.try_get("id")?,
            indexer: row.try_get("indexer")?,
        })
    })
    .transpose()
}

// ---------------------------------------------------------------------------
// Precise-index GraphQL mutations (M9.28a)
//
// Ports `store.DeleteUploadByID`, `store.DeleteUploads`,
// `store.DeleteAutoIndexJobByID`, `store.DeleteAutoIndexJobs`,
// `store.ReindexUploadByID`, `store.ReindexUploads`,
// `store.SetRerunAutoIndexJobByID`, `store.SetRerunAutoIndexJobs` and
// `store.GetIndexers` (`internal/codeintel/uploads/internal/store`). The
// internal-actor `AuthzQueryConds` is `TRUE` and therefore omitted, exactly
// like the read helpers above. No table/column is added.
// ---------------------------------------------------------------------------

/// `makeStateCondition`/`makeIndexStateCondition`: a requested `errored` or
/// `failed` state expands to both database values.
fn mutation_state_condition(states: &[String], params: &mut Vec<MutationParam>) -> Option<String> {
    if states.is_empty() {
        return None;
    }
    let mut expanded: Vec<String> = Vec::new();
    for state in states {
        if state == "errored" || state == "failed" {
            for value in ["errored", "failed"] {
                if !expanded.iter().any(|existing| existing == value) {
                    expanded.push(value.to_owned());
                }
            }
        } else if !expanded.iter().any(|existing| existing == state) {
            expanded.push(state.clone());
        }
    }
    expanded.sort();
    if expanded.len() == 1 {
        let value = expanded.remove(0);
        params.push(MutationParam::Text(value));
        Some(format!("u.state = ${}", params.len()))
    } else {
        params.push(MutationParam::TextArray(expanded));
        Some(format!("u.state = ANY(${})", params.len()))
    }
}

fn mutation_search_condition(
    columns: &[&str],
    term: &str,
    params: &mut Vec<MutationParam>,
) -> String {
    let mut parts = Vec::new();
    for column in columns {
        params.push(MutationParam::Text(format!("%{term}%")));
        parts.push(format!("{column} ILIKE ${}", params.len()));
    }
    format!("({})", parts.join(" OR "))
}

fn mutation_indexer_condition(
    names: &[String],
    params: &mut Vec<MutationParam>,
) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for name in names {
        params.push(MutationParam::Text(format!("%{name}%")));
        parts.push(format!("u.indexer ILIKE ${}", params.len()));
    }
    Some(format!("({})", parts.join(" OR ")))
}

enum MutationParam {
    Text(String),
    Int(i64),
    TextArray(Vec<String>),
}

fn bind_mutation_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [MutationParam],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for param in params {
        query = match param {
            MutationParam::Text(value) => query.bind(value.clone()),
            MutationParam::Int(value) => query.bind(*value),
            MutationParam::TextArray(values) => query.bind(values.clone()),
        };
    }
    query
}

/// `store.DeleteUploads` (`uploads.go:689`): soft-delete matching uploads and
/// mark every affected repository dirty.
pub async fn delete_uploads_by_filter(
    pool: &PgPool,
    repository_id: i32,
    states: &[String],
    indexer_names: &[String],
    term: &str,
    visible_at_tip: bool,
) -> Result<(), sqlx::Error> {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<MutationParam> = Vec::new();
    if repository_id != 0 {
        params.push(MutationParam::Int(repository_id as i64));
        conditions.push(format!("u.repository_id = ${}", params.len()));
    }
    conditions.push("repo.deleted_at IS NULL".to_owned());
    conditions.push("u.state != 'deleted'".to_owned());
    if !term.is_empty() {
        conditions.push(mutation_search_condition(
            &[
                "u.commit",
                "u.root",
                "(u.state)::text",
                "u.failure_message",
                "repo.name",
                "u.indexer",
                "u.indexer_version",
            ],
            term,
            &mut params,
        ));
    }
    if let Some(condition) = mutation_state_condition(states, &mut params) {
        conditions.push(condition);
    }
    if visible_at_tip {
        conditions.push(
            "EXISTS (SELECT 1 FROM lsif_uploads_visible_at_tip uvt \
             WHERE uvt.repository_id = u.repository_id AND uvt.upload_id = u.id \
             AND uvt.is_default_branch)"
                .to_owned(),
        );
    }
    if let Some(condition) = mutation_indexer_condition(indexer_names, &mut params) {
        conditions.push(condition);
    }

    let sql = format!(
        "UPDATE lsif_uploads u SET state = CASE WHEN u.state = 'completed' THEN 'deleting' ELSE 'deleted' END \
         FROM repo WHERE repo.id = u.repository_id AND {} RETURNING u.repository_id",
        conditions.join(" AND ")
    );

    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "direct delete by filter criteria request").await?;
    let rows = bind_mutation_params(sqlx::query(&sql), &params)
        .fetch_all(&mut *tx)
        .await?;
    for row in rows {
        let repo_id: i32 = row.try_get("repository_id")?;
        sqlx::query(SET_REPOSITORY_AS_DIRTY_QUERY)
            .bind(repo_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

/// `store.DeleteUploadByID` (`uploads.go:738`).
pub async fn delete_upload_by_id(pool: &PgPool, id: i64) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "direct delete by ID request").await?;
    let repo_id: Option<i32> = sqlx::query_scalar(
        "UPDATE lsif_uploads u SET state = CASE WHEN u.state = 'completed' THEN 'deleting' ELSE 'deleted' END \
         WHERE id = $1 RETURNING repository_id",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(repo_id) = repo_id {
        sqlx::query(SET_REPOSITORY_AS_DIRTY_QUERY)
            .bind(repo_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(repo_id.is_some())
}

/// `store.DeleteAutoIndexJobByID` (`indexes.go:294`).
pub async fn delete_auto_index_job_by_id(pool: &PgPool, id: i64) -> Result<bool, sqlx::Error> {
    let row = sqlx::query_scalar::<_, i32>(
        "DELETE FROM lsif_indexes WHERE id = $1 RETURNING repository_id",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

fn index_mutation_conditions(
    repository_id: i32,
    states: &[String],
    indexer_names: &[String],
    term: &str,
    without_upload: bool,
    params: &mut Vec<MutationParam>,
) -> Vec<String> {
    let mut conditions: Vec<String> = Vec::new();
    if repository_id != 0 {
        params.push(MutationParam::Int(repository_id as i64));
        conditions.push(format!("u.repository_id = ${}", params.len()));
    }
    if !term.is_empty() {
        conditions.push(mutation_search_condition(
            &[
                "u.commit",
                "(u.state)::text",
                "u.failure_message",
                "repo.name",
                "u.root",
                "u.indexer",
            ],
            term,
            params,
        ));
    }
    if let Some(condition) = mutation_state_condition(states, params) {
        conditions.push(condition);
    }
    if without_upload {
        conditions.push(
            "NOT EXISTS (SELECT 1 FROM lsif_uploads u2 WHERE u2.associated_index_id = u.id)"
                .to_owned(),
        );
    }
    if let Some(condition) = mutation_indexer_condition(indexer_names, params) {
        conditions.push(condition);
    }
    conditions
}

/// `store.DeleteAutoIndexJobs` (`indexes.go:303`).
pub async fn delete_auto_index_jobs_by_filter(
    pool: &PgPool,
    repository_id: i32,
    states: &[String],
    indexer_names: &[String],
    term: &str,
    without_upload: bool,
) -> Result<(), sqlx::Error> {
    let mut params = Vec::new();
    let conditions = index_mutation_conditions(
        repository_id,
        states,
        indexer_names,
        term,
        without_upload,
        &mut params,
    );
    let sql = if conditions.is_empty() {
        "DELETE FROM lsif_indexes u USING repo WHERE u.repository_id = repo.id".to_owned()
    } else {
        format!(
            "DELETE FROM lsif_indexes u USING repo WHERE u.repository_id = repo.id AND {}",
            conditions.join(" AND ")
        )
    };
    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "direct delete by filter criteria request").await?;
    bind_mutation_params(sqlx::query(&sql), &params)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

/// `store.ReindexUploadByID` (`uploads.go:852`).
pub async fn reindex_upload_by_id(pool: &PgPool, id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "WITH update_uploads AS (UPDATE lsif_uploads u SET should_reindex = true WHERE id = $1) \
         UPDATE lsif_indexes u SET should_reindex = true \
         WHERE id IN (SELECT associated_index_id FROM lsif_uploads WHERE id = $2)",
    )
    .bind(id)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `store.ReindexUploads` (`uploads.go:778`).
pub async fn reindex_uploads_by_filter(
    pool: &PgPool,
    repository_id: i32,
    states: &[String],
    indexer_names: &[String],
    term: &str,
    visible_at_tip: bool,
) -> Result<(), sqlx::Error> {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<MutationParam> = Vec::new();
    if repository_id != 0 {
        params.push(MutationParam::Int(repository_id as i64));
        conditions.push(format!("u.repository_id = ${}", params.len()));
    }
    if !term.is_empty() {
        conditions.push(mutation_search_condition(
            &[
                "u.commit",
                "u.root",
                "(u.state)::text",
                "u.failure_message",
                "repo.name",
                "u.indexer",
                "u.indexer_version",
            ],
            term,
            &mut params,
        ));
    }
    if let Some(condition) = mutation_state_condition(states, &mut params) {
        conditions.push(condition);
    }
    if visible_at_tip {
        conditions.push(
            "EXISTS (SELECT 1 FROM lsif_uploads_visible_at_tip uvt \
             WHERE uvt.repository_id = u.repository_id AND uvt.upload_id = u.id \
             AND uvt.is_default_branch)"
                .to_owned(),
        );
    }
    if let Some(condition) = mutation_indexer_condition(indexer_names, &mut params) {
        conditions.push(condition);
    }
    let where_clause = if conditions.is_empty() {
        "TRUE".to_owned()
    } else {
        conditions.join(" AND ")
    };
    let sql = format!(
        "WITH upload_candidates AS ( \
             SELECT u.id, u.associated_index_id FROM lsif_uploads u \
             JOIN repo ON repo.id = u.repository_id WHERE {where_clause} \
             ORDER BY u.id FOR UPDATE \
         ), update_uploads AS ( \
             UPDATE lsif_uploads u SET should_reindex = true \
             WHERE u.id IN (SELECT id FROM upload_candidates) \
         ), index_candidates AS ( \
             SELECT u.id FROM lsif_indexes u \
             WHERE u.id IN (SELECT associated_index_id FROM upload_candidates) \
             ORDER BY u.id FOR UPDATE \
         ) \
         UPDATE lsif_indexes u SET should_reindex = true \
         WHERE u.id IN (SELECT id FROM index_candidates)"
    );
    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "direct reindex by filter criteria request").await?;
    bind_mutation_params(sqlx::query(&sql), &params)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

/// `store.SetRerunAutoIndexJobByID` (`indexes.go:361`).
pub async fn set_rerun_auto_index_job_by_id(pool: &PgPool, id: i64) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE lsif_indexes u SET should_reindex = true WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `store.SetRerunAutoIndexJobs` (`indexes.go:377`).
pub async fn set_rerun_auto_index_jobs_by_filter(
    pool: &PgPool,
    repository_id: i32,
    states: &[String],
    indexer_names: &[String],
    term: &str,
    without_upload: bool,
) -> Result<(), sqlx::Error> {
    let mut params = Vec::new();
    let conditions = index_mutation_conditions(
        repository_id,
        states,
        indexer_names,
        term,
        without_upload,
        &mut params,
    );
    let where_clause = if conditions.is_empty() {
        "TRUE".to_owned()
    } else {
        conditions.join(" AND ")
    };
    let sql = format!(
        "WITH candidates AS ( \
             SELECT u.id FROM lsif_indexes u JOIN repo ON repo.id = u.repository_id \
             WHERE {where_clause} ORDER BY u.id FOR UPDATE \
         ) \
         UPDATE lsif_indexes u SET should_reindex = true \
         WHERE u.id IN (SELECT id FROM candidates)"
    );
    let mut tx = pool.begin().await?;
    set_audit_reason(&mut tx, "direct reindex by filter criteria request").await?;
    bind_mutation_params(sqlx::query(&sql), &params)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

/// `store.GetIndexers` (`summary.go:17`). Returns the distinct `indexer`
/// filenames across uploads and indexes, ordered ascending. The
/// repository-scoped branch applies `database.AuthzQueryConds` (passed in as
/// `authz`); the global branch does not reference `repo`.
pub async fn get_indexer_names(
    pool: &PgPool,
    repository_id: i32,
    authz: &str,
) -> Result<Vec<String>, sqlx::Error> {
    if repository_id == 0 {
        return sqlx::query_scalar(
            "WITH combined_indexers AS ( \
                 SELECT DISTINCT u.indexer FROM lsif_uploads u \
                 UNION ALL \
                 SELECT DISTINCT u.indexer FROM lsif_indexes u \
             ) \
             SELECT DISTINCT u.indexer FROM combined_indexers u ORDER BY u.indexer",
        )
        .fetch_all(pool)
        .await;
    }

    sqlx::query_scalar(&format!(
        "WITH repo_candidate AS ( \
             SELECT repo.id FROM repo \
             WHERE repo.id = $1 AND {authz} AND repo.deleted_at IS NULL AND repo.blocked IS NULL \
         ), combined_indexers AS ( \
             SELECT DISTINCT u.indexer FROM lsif_uploads u \
             JOIN repo_candidate r ON r.id = u.repository_id \
             UNION ALL \
             SELECT DISTINCT u.indexer FROM lsif_indexes u \
             JOIN repo_candidate r ON r.id = u.repository_id \
         ) \
         SELECT DISTINCT u.indexer FROM combined_indexers u ORDER BY u.indexer"
    ))
    .bind(repository_id)
    .fetch_all(pool)
    .await
}

// ---------------------------------------------------------------------------
// Precise-index GraphQL remaining reads (M9.29d)
//
// Ports the summary/dependency/audit SQL of
// `internal/codeintel/uploads/internal/store` and
// `internal/codeintel/autoindexing/internal/store/coverage.go`. The
// internal-actor `AuthzQueryConds` is `TRUE` and therefore omitted. No table,
// column or index is added.
// ---------------------------------------------------------------------------

/// `packageRankingQueryFragment` (`store/util.go:40`).
pub const PACKAGE_RANKING_FRAGMENT: &str = "rank() OVER ( \
    PARTITION BY p.scheme, p.manager, p.name, p.version, u.repository_id, u.indexer, u.root \
    ORDER BY (SELECT cd.committed_at FROM codeintel_commit_dates cd \
        WHERE cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')) NULLS LAST, \
        u.id \
)";

/// `rankedDependencyCandidateCTEQuery` (`store/uploads.go:1089`) with the
/// `r.dump_id = %s` parameter rendered as `{dependency_of}`.
pub const RANKED_DEPENDENCIES_CTE_TEMPLATE: &str = "ranked_dependencies AS ( \
    SELECT p.dump_id AS pkg_id, r.dump_id AS ref_id, \
        PACKAGE_RANKING AS rank \
    FROM lsif_uploads u \
    JOIN lsif_packages p ON p.dump_id = u.id \
    JOIN lsif_references r ON \
        r.scheme = p.scheme AND r.manager = p.manager AND \
        r.name = p.name AND r.version = p.version AND r.dump_id != p.dump_id \
    WHERE u.state = 'completed' AND r.dump_id = {dependency_of} \
)";

/// `rankedDependentCandidateCTEQuery` (`store/uploads.go:1112`).
pub const RANKED_DEPENDENTS_CTE_TEMPLATE: &str = "ranked_dependents AS ( \
    SELECT p.dump_id AS pkg_id, p.scheme AS scheme, p.manager AS manager, \
        p.name AS name, p.version AS version, \
        PACKAGE_RANKING AS rank \
    FROM lsif_uploads u \
    JOIN lsif_packages p ON p.dump_id = u.id \
    WHERE u.state = 'completed' AND \
        (p.scheme, p.manager, p.name, p.version) IN ( \
            SELECT p.scheme, p.manager, p.name, p.version \
            FROM lsif_packages p WHERE p.dump_id = {dependent_of} \
        ) \
)";

/// Render a dependency CTE template with a bind placeholder.
pub fn dependency_cte(template: &str, placeholder: &str) -> String {
    template
        .replace("PACKAGE_RANKING", PACKAGE_RANKING_FRAGMENT)
        .replace("{dependency_of}", placeholder)
        .replace("{dependent_of}", placeholder)
}

/// `deletedUploadsFromAuditLogsCTEQuery` (`store/uploads.go:1132`).
pub const DELETED_UPLOADS_CTE: &str = "deleted_uploads AS ( \
    SELECT DISTINCT ON (s.upload_id) s.upload_id AS id, au.commit, au.root, \
        au.uploaded_at, 'deleted' AS state, \
        s.snapshot->'failure_message' AS failure_message, \
        (s.snapshot->'started_at')::timestamptz AS started_at, \
        (s.snapshot->'finished_at')::timestamptz AS finished_at, \
        (s.snapshot->'process_after')::timestamptz AS process_after, \
        COALESCE((s.snapshot->'num_resets')::integer, -1) AS num_resets, \
        COALESCE((s.snapshot->'num_failures')::integer, -1) AS num_failures, \
        au.repository_id, au.indexer, au.indexer_version, \
        COALESCE((s.snapshot->'num_parts')::integer, -1) AS num_parts, \
        NULL::integer[] AS uploaded_parts, \
        au.upload_size, au.associated_index_id, au.content_type, \
        false AS should_reindex, \
        COALESCE((s.snapshot->'expired')::boolean, false) AS expired, \
        NULL::bigint AS uncompressed_size \
    FROM ( \
        SELECT upload_id, snapshot_transition_columns(transition_columns ORDER BY sequence ASC) AS snapshot \
        FROM lsif_uploads_audit_logs \
        WHERE record_deleted_at IS NOT NULL \
        GROUP BY upload_id \
    ) AS s \
    JOIN lsif_uploads_audit_logs au ON au.upload_id = s.upload_id \
)";

/// The union source table that appends audit-log snapshots of deleted uploads.
pub const DELETED_UPLOADS_SOURCE: &str = "( \
    SELECT id, commit, root, uploaded_at, state, failure_message, started_at, \
        finished_at, process_after, num_resets, num_failures, repository_id, \
        indexer, indexer_version, num_parts, uploaded_parts, upload_size, \
        associated_index_id, content_type, should_reindex, expired, uncompressed_size \
    FROM lsif_uploads \
    UNION ALL \
    SELECT * FROM deleted_uploads \
) AS u";

/// `shared.UploadLog` reduced to the GraphQL `LSIFUploadAuditLog` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadAuditLogRow {
    pub log_timestamp: OffsetDateTime,
    pub record_deleted_at: Option<OffsetDateTime>,
    pub upload_id: i32,
    pub commit: String,
    pub root: String,
    pub uploaded_at: OffsetDateTime,
    pub indexer: String,
    /// One hstore per transition, rendered as a JSON object
    /// `{"column": ..., "old": ..., "new": ...}`.
    pub transition_columns: Vec<serde_json::Value>,
    pub reason: Option<String>,
    pub operation: String,
}

/// `getAuditLogsForUploadQuery` (`store/uploads.go:633`), with the
/// `hstore[]` transitions rendered as `json` text so sqlx can decode them.
const GET_AUDIT_LOGS_FOR_UPLOAD_QUERY: &str = " \
SELECT \
    u.log_timestamp, u.record_deleted_at, u.upload_id, u.commit, u.root, \
    u.uploaded_at, u.indexer, \
    ARRAY(SELECT hstore_to_json(e)::text FROM unnest(u.transition_columns) e) AS transitions, \
    u.reason, u.operation::text AS operation \
FROM lsif_uploads_audit_logs u \
JOIN repo ON repo.id = u.repository_id \
WHERE u.upload_id = $1 \
ORDER BY u.sequence";

/// `store.GetAuditLogsForUpload` (`store/uploads.go:624`).
pub async fn get_audit_logs_for_upload(
    pool: &PgPool,
    upload_id: i32,
) -> Result<Vec<UploadAuditLogRow>, sqlx::Error> {
    let rows = sqlx::query(GET_AUDIT_LOGS_FOR_UPLOAD_QUERY)
    .bind(upload_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let transitions: Vec<String> = row.try_get("transitions")?;
            let transition_columns = transitions
                .iter()
                .map(|text| {
                    serde_json::from_str(text).map_err(|error| {
                        sqlx::Error::Decode(Box::new(error))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(UploadAuditLogRow {
                log_timestamp: row.try_get("log_timestamp")?,
                record_deleted_at: row.try_get("record_deleted_at")?,
                upload_id: row.try_get("upload_id")?,
                commit: row.try_get("commit")?,
                root: row.try_get("root")?,
                uploaded_at: row.try_get("uploaded_at")?,
                indexer: row.try_get("indexer")?,
                transition_columns,
                reason: row.try_get("reason")?,
                operation: row.try_get("operation")?,
            })
        })
        .collect()
}

/// `countRepositoriesQuery` (`store/summary.go:387`).
pub async fn num_repositories_with_code_intelligence(
    pool: &PgPool,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "WITH candidate_repositories AS ( \
             SELECT DISTINCT uvt.repository_id AS id \
             FROM lsif_uploads_visible_at_tip uvt \
             WHERE is_default_branch \
         ) \
         SELECT COUNT(*) FROM candidate_repositories s \
         JOIN repo r ON r.id = s.id \
         WHERE r.deleted_at IS NULL AND r.blocked IS NULL",
    )
    .fetch_one(pool)
    .await
}

/// `shared.RepositoryWithCount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWithCount {
    pub repository_id: i32,
    pub repository_name: String,
    pub count: i64,
}

/// `repositoriesWithErrorsQuery` (`store/summary.go:310`), joining the repo
/// name so the GraphQL `CodeIntelRepository` fields can be built inline.
pub async fn repository_ids_with_errors(
    pool: &PgPool,
    offset: i64,
    limit: i64,
) -> Result<(Vec<RepositoryWithCount>, i64), sqlx::Error> {
    let rows = sqlx::query(
        "WITH candidates_from_uploads AS ( \
             SELECT u.repository_id \
             FROM lsif_uploads_with_repository_name u \
             WHERE u.state = 'failed' AND NOT EXISTS ( \
                 SELECT 1 FROM lsif_uploads u2 \
                 WHERE u2.state = 'completed' AND u2.repository_id = u.repository_id \
                   AND u2.root = u.root AND u2.indexer = u.indexer \
                   AND u2.finished_at > u.finished_at \
             ) \
             GROUP BY u.repository_id, u.root, u.indexer \
         ), candidates_from_indexes AS ( \
             SELECT u.repository_id FROM lsif_indexes u \
             WHERE u.state = 'failed' AND NOT EXISTS ( \
                 SELECT 1 FROM lsif_indexes u2 \
                 WHERE u2.state = 'completed' AND u2.repository_id = u.repository_id \
                   AND u2.root = u.root AND u2.indexer = u.indexer \
                   AND u2.finished_at > u.finished_at \
             ) \
             GROUP BY u.repository_id, u.root, u.indexer \
         ), candidates AS ( \
             SELECT * FROM candidates_from_uploads UNION ALL \
             SELECT * FROM candidates_from_indexes \
         ), grouped_candidates AS ( \
             SELECT r.repository_id, COUNT(*) AS num_failures \
             FROM candidates r GROUP BY r.repository_id \
         ) \
         SELECT g.repository_id, repo.name::text AS name, g.num_failures, COUNT(*) OVER() AS count \
         FROM grouped_candidates g JOIN repo ON repo.id = g.repository_id \
         ORDER BY g.num_failures DESC, g.repository_id \
         LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    let total = rows
        .first()
        .map(|row| row.try_get::<i64, _>("count"))
        .transpose()?
        .unwrap_or(0);
    let items = rows
        .into_iter()
        .map(|row| {
            Ok(RepositoryWithCount {
                repository_id: row.try_get("repository_id")?,
                repository_name: row.try_get("name")?,
                count: row.try_get("num_failures")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok((items, total))
}

/// `shared.RepositoryWithAvailableIndexers` (`cached_available_indexers`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWithConfiguration {
    pub repository_id: i32,
    pub repository_name: String,
    /// Raw `available_indexers` jsonb text (`map[string]AvailableIndexer`).
    pub available_indexers: String,
}

/// `repositoriesWithConfigurationQuery` (`autoindexing/internal/store/coverage.go:73`).
pub async fn repository_ids_with_configuration(
    pool: &PgPool,
    offset: i64,
    limit: i64,
) -> Result<(Vec<RepositoryWithConfiguration>, i64), sqlx::Error> {
    let rows = sqlx::query(
        "SELECT r.id, r.name::text AS name, cai.available_indexers::text AS available_indexers, \
                COUNT(*) OVER() AS count \
         FROM cached_available_indexers cai \
         JOIN repo r ON r.id = cai.repository_id \
         WHERE available_indexers != '{}'::jsonb AND \
             r.deleted_at IS NULL AND r.blocked IS NULL \
         ORDER BY num_events DESC \
         LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    let total = rows
        .first()
        .map(|row| row.try_get::<i64, _>("count"))
        .transpose()?
        .unwrap_or(0);
    let items = rows
        .into_iter()
        .map(|row| {
            Ok(RepositoryWithConfiguration {
                repository_id: row.try_get("id")?,
                repository_name: row.try_get("name")?,
                available_indexers: row.try_get("available_indexers")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok((items, total))
}
