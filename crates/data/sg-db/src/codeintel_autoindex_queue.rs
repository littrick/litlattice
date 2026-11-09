#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/store` (the
//! `codeintel_autoindex_queue` half) used by the on-demand half of the
//! `codeintel-autoindexing-scheduler` worker job.
//!
//! Ported 1:1 from `store/scheduler.go`:
//!
//! * [`get_queued_repo_rev`] — `GetQueuedRepoRev`: the oldest unprocessed rows
//!   (`processed_at IS NULL ORDER BY queued_at ASC`), locked with
//!   `FOR UPDATE SKIP LOCKED` so concurrent scheduler instances never claim the
//!   same row;
//! * [`mark_repo_revs_as_processed`] — `MarkRepoRevsAsProcessed`: sets
//!   `processed_at = NOW()` for the claimed ids.
//!
//! Both run inside the caller's transaction: the baseline calls them through
//! `store.WithTransaction`, so the claim, the enqueue side effects and the
//! `processed_at` update commit or roll back together.
//!
//! The baseline's `operations` observation spans (`getQueuedRepoRev`,
//! `markRepoRevsAsProcessed`) have no observable effect beyond tracing; they
//! are not reproduced. Migrations stay owned by the Go `migrator`.

use sqlx::{Postgres, Row, Transaction};

/// `store.RepoRev` — one `codeintel_autoindex_queue` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRev {
    pub id: i32,
    pub repository_id: i32,
    pub rev: String,
}

const GET_QUEUED_REPO_REV_SQL: &str = "
SELECT
	id,
	repository_id,
	rev
FROM codeintel_autoindex_queue
WHERE processed_at IS NULL
ORDER BY queued_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $1
";

/// `store.GetQueuedRepoRev`.
pub async fn get_queued_repo_rev(
    tx: &mut Transaction<'_, Postgres>,
    batch_size: i64,
) -> Result<Vec<RepoRev>, sqlx::Error> {
    let rows = sqlx::query(GET_QUEUED_REPO_REV_SQL)
        .bind(batch_size)
        .fetch_all(&mut **tx)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| RepoRev {
            id: row.get("id"),
            repository_id: row.get("repository_id"),
            rev: row.get("rev"),
        })
        .collect())
}

const MARK_REPO_REVS_AS_PROCESSED_SQL: &str = "
UPDATE codeintel_autoindex_queue
SET processed_at = NOW()
WHERE id = ANY($1)
";

/// `store.MarkRepoRevsAsProcessed`.
pub async fn mark_repo_revs_as_processed(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[i32],
) -> Result<(), sqlx::Error> {
    sqlx::query(MARK_REPO_REVS_AS_PROCESSED_SQL)
        .bind(ids)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
