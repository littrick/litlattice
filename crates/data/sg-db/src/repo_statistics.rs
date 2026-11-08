#![forbid(unsafe_code)]

//! `internal/database/repo_statistics.go`: the append-only
//! `repo_statistics` / `gitserver_repos_statistics` tables.
//!
//! The worker's compactor and weekly resetter are the only writers; both SQL
//! statements are copied verbatim from the baseline, including the
//! `COALESCE(SUM(...), 0)` in the compactor (a compacted table always has
//! exactly one row) and the `GROUP BY shard_id` in the gitserver variant.

use sqlx::postgres::PgPool;

/// One row of `repo_statistics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RepoStatistics {
    pub total: i64,
    pub soft_deleted: i64,
    pub not_cloned: i64,
    pub cloning: i64,
    pub cloned: i64,
    pub failed_fetch: i64,
    pub corrupted: i64,
}

/// One row of `gitserver_repos_statistics`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GitserverReposStatistic {
    pub shard_id: String,
    pub total: i64,
    pub not_cloned: i64,
    pub cloning: i64,
    pub cloned: i64,
    pub failed_fetch: i64,
    pub corrupted: i64,
}

// The baseline scan (`rows.Scan(&d.Total)`) relies on lib/pq converting the
// NUMERIC result of `SUM(bigint)` into an int64. sqlx is stricter, so the
// Rust copy casts the sums back to `bigint`; the values are identical.
const GET_REPO_STATISTICS: &str = r#"
SELECT
	SUM(total)::bigint,
	SUM(soft_deleted)::bigint,
	SUM(not_cloned)::bigint,
	SUM(cloning)::bigint,
	SUM(cloned)::bigint,
	SUM(failed_fetch)::bigint,
	SUM(corrupted)::bigint
FROM repo_statistics
"#;

const COMPACT_REPO_STATISTICS: &str = r#"
WITH deleted AS (
	DELETE FROM repo_statistics
	RETURNING
		total,
		soft_deleted,
		not_cloned,
		cloning,
		cloned,
		failed_fetch,
		corrupted
)
INSERT INTO repo_statistics (total, soft_deleted, not_cloned, cloning, cloned, failed_fetch, corrupted)
SELECT
	COALESCE(SUM(total), 0),
	COALESCE(SUM(soft_deleted), 0),
	COALESCE(SUM(not_cloned), 0),
	COALESCE(SUM(cloning), 0),
	COALESCE(SUM(cloned), 0),
	COALESCE(SUM(failed_fetch), 0),
	COALESCE(SUM(corrupted), 0)
FROM deleted;
"#;

const COMPACT_GITSERVER_REPOS_STATISTICS: &str = r#"
WITH deleted AS (
	DELETE FROM gitserver_repos_statistics
	RETURNING
		shard_id,
		total,
		not_cloned,
		cloning,
		cloned,
		failed_fetch,
		corrupted
)
INSERT INTO gitserver_repos_statistics (shard_id, total, not_cloned, cloning, cloned, failed_fetch, corrupted)
SELECT
	shard_id,
	SUM(total),
	SUM(not_cloned),
	SUM(cloning),
	SUM(cloned),
	SUM(failed_fetch),
	SUM(corrupted)
FROM deleted
GROUP BY shard_id
"#;

const GET_GITSERVER_REPOS_STATISTICS: &str = r#"
SELECT
	shard_id,
	SUM(total)::bigint AS total,
	SUM(not_cloned)::bigint AS not_cloned,
	SUM(cloning)::bigint AS cloning,
	SUM(cloned)::bigint AS cloned,
	SUM(failed_fetch)::bigint AS failed_fetch,
	SUM(corrupted)::bigint AS corrupted
FROM gitserver_repos_statistics
GROUP BY shard_id
"#;

const RECREATE_REPO_STATISTICS: &str = r#"
INSERT INTO repo_statistics (total, soft_deleted, not_cloned, cloning, cloned, failed_fetch, corrupted)
SELECT
  COUNT(*) AS total,
  (SELECT COUNT(*) FROM repo WHERE deleted_at is NOT NULL AND blocked IS NULL) AS soft_deleted,
  COUNT(*) FILTER(WHERE gitserver_repos.clone_status = 'not_cloned') AS not_cloned,
  COUNT(*) FILTER(WHERE gitserver_repos.clone_status = 'cloning') AS cloning,
  COUNT(*) FILTER(WHERE gitserver_repos.clone_status = 'cloned') AS cloned,
  COUNT(*) FILTER(WHERE gitserver_repos.last_error IS NOT NULL) AS failed_fetch,
  COUNT(*) FILTER(WHERE gitserver_repos.corrupted_at IS NOT NULL) AS corrupted
FROM repo
JOIN gitserver_repos ON gitserver_repos.repo_id = repo.id
WHERE
  repo.deleted_at is NULL AND repo.blocked IS NULL
"#;

const RECREATE_GITSERVER_REPOS_STATISTICS: &str = r#"
INSERT INTO
  gitserver_repos_statistics (shard_id, total, not_cloned, cloning, cloned, failed_fetch, corrupted)
SELECT
  shard_id,
  COUNT(*) AS total,
  COUNT(*) FILTER(WHERE clone_status = 'not_cloned') AS not_cloned,
  COUNT(*) FILTER(WHERE clone_status = 'cloning') AS cloning,
  COUNT(*) FILTER(WHERE clone_status = 'cloned') AS cloned,
  COUNT(*) FILTER(WHERE last_error IS NOT NULL) AS failed_fetch,
  COUNT(*) FILTER(WHERE corrupted_at IS NOT NULL) AS corrupted
FROM
  gitserver_repos
GROUP BY shard_id
"#;

/// `RepoStatisticsStore.GetRepoStatistics`.
pub async fn get_repo_statistics(pool: &PgPool) -> Result<RepoStatistics, sqlx::Error> {
    sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(GET_REPO_STATISTICS)
        .fetch_one(pool)
        .await
        .map(
            |(total, soft_deleted, not_cloned, cloning, cloned, failed_fetch, corrupted)| {
                RepoStatistics {
                    total,
                    soft_deleted,
                    not_cloned,
                    cloning,
                    cloned,
                    failed_fetch,
                    corrupted,
                }
            },
        )
}

/// `RepoStatisticsStore.CompactRepoStatistics`.
pub async fn compact_repo_statistics(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(COMPACT_REPO_STATISTICS).execute(pool).await?;
    Ok(())
}

/// `RepoStatisticsStore.CompactGitserverReposStatistics`.
pub async fn compact_gitserver_repos_statistics(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(COMPACT_GITSERVER_REPOS_STATISTICS)
        .execute(pool)
        .await?;
    Ok(())
}

/// `RepoStatisticsStore.GetGitserverReposStatistics`.
pub async fn get_gitserver_repos_statistics(
    pool: &PgPool,
) -> Result<Vec<GitserverReposStatistic>, sqlx::Error> {
    let rows: Vec<(String, i64, i64, i64, i64, i64, i64)> =
        sqlx::query_as(GET_GITSERVER_REPOS_STATISTICS)
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(
            |(shard_id, total, not_cloned, cloning, cloned, failed_fetch, corrupted)| {
                GitserverReposStatistic {
                    shard_id,
                    total,
                    not_cloned,
                    cloning,
                    cloned,
                    failed_fetch,
                    corrupted,
                }
            },
        )
        .collect())
}

/// `RepoStatisticsStore.DeleteAndRecreateStatistics`: lock the source tables,
/// drop the append-only statistic rows and rebuild them.
pub async fn delete_and_recreate_statistics(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Lock `repo` first: triggers write `repo` and update `gitserver_repos`,
    // never the other way around.
    sqlx::query("LOCK repo IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    sqlx::query("LOCK gitserver_repos IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM repo_statistics")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM gitserver_repos_statistics")
        .execute(&mut *tx)
        .await?;

    sqlx::query(RECREATE_REPO_STATISTICS)
        .execute(&mut *tx)
        .await?;
    sqlx::query(RECREATE_GITSERVER_REPOS_STATISTICS)
        .execute(&mut *tx)
        .await?;

    tx.commit().await
}
