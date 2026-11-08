//! Port of `internal/database/gitserver_repos.go`
//! (`gitserverRepoStore`) for the operations the gitserver write path needs.
//!
//! The `repo` table owns repository names; `gitserver_repos` is joined through
//! `repo_id`. Writes keep the baseline's `IS DISTINCT FROM` / `EXCLUDED`
//! clauses, so repeating an update does not touch `updated_at`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `types.CloneStatus` (`internal/types/types.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneStatus {
    /// Any value the baseline does not recognize; never written back.
    Unknown,
    NotCloned,
    Cloning,
    Cloned,
}

impl CloneStatus {
    pub fn parse(value: &str) -> Self {
        match value {
            "not_cloned" => Self::NotCloned,
            "cloning" => Self::Cloning,
            "cloned" => Self::Cloned,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "",
            Self::NotCloned => "not_cloned",
            Self::Cloning => "cloning",
            Self::Cloned => "cloned",
        }
    }
}

/// `types.GitserverRepo`. `name` is empty for lookups by ID, like the
/// baseline's `'' as name` column.
#[derive(Debug, Clone)]
pub struct GitserverRepo {
    pub repo_id: i32,
    pub name: String,
    pub clone_status: CloneStatus,
    pub shard_id: String,
    pub last_error: Option<String>,
    pub last_fetched: OffsetDateTime,
    pub last_changed: OffsetDateTime,
    pub repo_size_bytes: Option<i64>,
    pub updated_at: OffsetDateTime,
    pub corrupted_at: Option<OffsetDateTime>,
    pub corruption_logs: serde_json::Value,
}

/// `ErrGitserverRepoNotFound`.
#[derive(Debug)]
pub struct GitserverRepoNotFound;

impl std::fmt::Display for GitserverRepoNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("gitserver repo not found")
    }
}

impl std::error::Error for GitserverRepoNotFound {}

#[derive(Clone)]
pub struct GitserverReposStore {
    pool: PgPool,
}

impl GitserverReposStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// `SetCloneStatus`: clears `corrupted_at`, stores the status and shard,
    /// and only writes when the status actually changes.
    pub async fn set_clone_status(
        &self,
        repo: &str,
        status: CloneStatus,
        shard_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
UPDATE gitserver_repos
SET
	corrupted_at = NULL,
	clone_status = $1,
	shard_id = $2,
	updated_at = NOW()
WHERE
	repo_id = (SELECT id FROM repo WHERE name = $3)
	AND
	clone_status IS DISTINCT FROM $1
"#,
        )
        .bind(status.as_str())
        .bind(shard_id)
        .bind(repo)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// `SetLastError`: an empty error is stored as `NULL` (`NewNullString`).
    pub async fn set_last_error(
        &self,
        repo: &str,
        error: &str,
        shard_id: &str,
    ) -> Result<(), sqlx::Error> {
        let error = if error.is_empty() { None } else { Some(error) };
        sqlx::query(
            r#"
UPDATE gitserver_repos
SET
	last_error = $1,
	shard_id = $2,
	updated_at = NOW()
WHERE
	repo_id = (SELECT id FROM repo WHERE name = $3)
	AND
	last_error IS DISTINCT FROM $1
"#,
        )
        .bind(error)
        .bind(shard_id)
        .bind(repo)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// `SetLastOutput`: upsert into `gitserver_repos_sync_output`.
    pub async fn set_last_output(&self, repo: &str, output: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
INSERT INTO gitserver_repos_sync_output(repo_id, last_output)
SELECT id, $1 FROM repo WHERE name = $2
ON CONFLICT(repo_id)
DO UPDATE SET last_output = EXCLUDED.last_output, updated_at = NOW()
"#,
        )
        .bind(output)
        .bind(repo)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// `GetLastSyncOutput`. The baseline never stores `NULL`, so an empty
    /// string is reported as "no output".
    pub async fn get_last_sync_output(&self, repo: &str) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query(
            r#"
SELECT last_output
FROM gitserver_repos_sync_output
WHERE repo_id = (SELECT id FROM repo WHERE name = $1)
"#,
        )
        .bind(repo)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let output: String = row.try_get("last_output")?;
        Ok((!output.is_empty()).then_some(output))
    }

    /// `SetRepoSize`: also marks the repository cloned and records the shard.
    pub async fn set_repo_size(
        &self,
        repo: &str,
        size: i64,
        shard_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
UPDATE gitserver_repos
SET
	repo_size_bytes = $1,
	shard_id = $2,
	clone_status = $3,
	updated_at = NOW()
WHERE
	repo_id = (SELECT id FROM repo WHERE name = $4)
	AND
	repo_size_bytes IS DISTINCT FROM $1
"#,
        )
        .bind(size)
        .bind(shard_id)
        .bind(CloneStatus::Cloned.as_str())
        .bind(repo)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// `GetByName` (the baseline selects `'' as name` for this lookup).
    pub async fn get_by_name(&self, repo: &str) -> Result<Option<GitserverRepo>, sqlx::Error> {
        let row = sqlx::query(
            r#"
SELECT
	gr.repo_id,
	'' as name,
	gr.clone_status,
	gr.shard_id,
	gr.last_error,
	gr.last_fetched,
	gr.last_changed,
	gr.repo_size_bytes,
	gr.updated_at,
	gr.corrupted_at,
	gr.corruption_logs
FROM gitserver_repos gr
JOIN repo r ON r.id = gr.repo_id
WHERE r.name = $1
"#,
        )
        .bind(repo)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(GitserverRepo {
            repo_id: row.try_get("repo_id")?,
            name: row.try_get("name")?,
            clone_status: CloneStatus::parse(row.try_get::<String, _>("clone_status")?.as_str()),
            shard_id: row.try_get("shard_id")?,
            last_error: row.try_get("last_error")?,
            last_fetched: row.try_get("last_fetched")?,
            last_changed: row.try_get("last_changed")?,
            repo_size_bytes: row.try_get("repo_size_bytes")?,
            updated_at: row.try_get("updated_at")?,
            corrupted_at: row.try_get("corrupted_at")?,
            corruption_logs: row.try_get("corruption_logs")?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs against the local baseline database when `SG_TEST_DATABASE_DSN`
    /// is set; the test cleans up the repository row it creates (the trigger
    /// creates the matching `gitserver_repos` row).
    #[tokio::test]
    async fn writes_gitserver_repo_columns_like_the_baseline() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = GitserverReposStore::new(pool.clone());

        let name = format!(
            "gitserver-test-{}.example.com/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        sqlx::query("INSERT INTO repo(name) VALUES ($1)")
            .bind(&name)
            .execute(&pool)
            .await
            .expect("insert repo");

        let result = async {
            // The trigger creates the row with clone_status not_cloned.
            let repo = store
                .get_by_name(&name)
                .await
                .expect("get")
                .expect("row exists");
            assert_eq!(repo.clone_status, CloneStatus::NotCloned);
            assert_eq!(repo.last_error, None);

            store
                .set_clone_status(&name, CloneStatus::Cloning, "shard-1")
                .await
                .expect("clone status");
            let repo = store.get_by_name(&name).await.expect("get").expect("row");
            assert_eq!(repo.clone_status, CloneStatus::Cloning);
            assert_eq!(repo.shard_id, "shard-1");

            store
                .set_last_error(&name, "boom", "shard-1")
                .await
                .expect("last error");
            let repo = store.get_by_name(&name).await.expect("get").expect("row");
            assert_eq!(repo.last_error.as_deref(), Some("boom"));

            // An empty error clears the column.
            store
                .set_last_error(&name, "", "shard-1")
                .await
                .expect("clear error");
            let repo = store.get_by_name(&name).await.expect("get").expect("row");
            assert_eq!(repo.last_error, None);

            store
                .set_last_output(&name, "fetch log")
                .await
                .expect("output");
            assert_eq!(
                store.get_last_sync_output(&name).await.expect("read"),
                Some("fetch log".to_owned())
            );

            store
                .set_repo_size(&name, 1234, "shard-1")
                .await
                .expect("size");
            let repo = store.get_by_name(&name).await.expect("get").expect("row");
            assert_eq!(repo.repo_size_bytes, Some(1234));
            assert_eq!(repo.clone_status, CloneStatus::Cloned);
        }
        .await;

        sqlx::query("DELETE FROM repo WHERE name = $1")
            .bind(&name)
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }
}
