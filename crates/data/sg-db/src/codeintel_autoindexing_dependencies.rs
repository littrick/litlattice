#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/store/dependencies.go`:
//! `InsertDependencyIndexingJob` and `QueueRepoRev`.
//!
//! * [`insert_dependency_indexing_job`] inserts a
//!   `lsif_dependency_indexing_jobs` row and returns its id;
//! * [`queue_repo_rev`] runs in one transaction: it no-ops when
//!   [`is_queued_in_tx`] reports the repo/rev pair already covered, otherwise
//!   it inserts a `codeintel_autoindex_queue` row (`ON CONFLICT DO NOTHING`,
//!   though the baseline's earlier `IsQueued` check is the effective guard).
//!
//! Observation spans are omitted; migrations stay owned by the Go `migrator`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use crate::codeintel_autoindexing_enqueuer::is_queued_in_tx;

const INSERT_DEPENDENCY_INDEXING_JOB_SQL: &str = "
INSERT INTO lsif_dependency_indexing_jobs (upload_id, external_service_kind, external_service_sync)
VALUES ($1, $2, $3)
RETURNING id
";

/// `store.InsertDependencyIndexingJob`.
pub async fn insert_dependency_indexing_job(
    pool: &PgPool,
    upload_id: i32,
    external_service_kind: &str,
    sync_time: OffsetDateTime,
) -> Result<i32, sqlx::Error> {
    let row = sqlx::query(INSERT_DEPENDENCY_INDEXING_JOB_SQL)
        .bind(upload_id)
        .bind(external_service_kind)
        .bind(sync_time)
        .fetch_one(pool)
        .await?;
    row.try_get(0)
}

const QUEUE_REPO_REV_SQL: &str = "
INSERT INTO codeintel_autoindex_queue (repository_id, rev)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
";

/// `store.QueueRepoRev`.
pub async fn queue_repo_rev(
    pool: &PgPool,
    repository_id: i32,
    rev: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    if is_queued_in_tx(&mut tx, repository_id, rev).await? {
        tx.commit().await?;
        return Ok(());
    }
    sqlx::query(QUEUE_REPO_REV_SQL)
        .bind(repository_id)
        .bind(rev)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_target_the_baseline_tables() {
        assert!(INSERT_DEPENDENCY_INDEXING_JOB_SQL.contains("lsif_dependency_indexing_jobs"));
        assert!(INSERT_DEPENDENCY_INDEXING_JOB_SQL.contains("RETURNING id"));
        assert!(QUEUE_REPO_REV_SQL.contains("codeintel_autoindex_queue"));
        // The baseline relies on the in-transaction `IsQueued` check; the
        // conflict clause only protects against a concurrent insert.
        assert!(QUEUE_REPO_REV_SQL.contains("ON CONFLICT DO NOTHING"));
    }
}
