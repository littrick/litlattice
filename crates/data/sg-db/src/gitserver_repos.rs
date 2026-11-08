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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloneStatus {
    NotCloned,
    Cloning,
    Cloned,
    /// Any value the baseline does not recognize. Kept verbatim so
    /// round-tripping a row (e.g. `Update` in the state syncer) does not
    /// rewrite an unknown status.
    Other(String),
}

impl CloneStatus {
    pub fn parse(value: &str) -> Self {
        match value {
            "not_cloned" => Self::NotCloned,
            "cloning" => Self::Cloning,
            "cloned" => Self::Cloned,
            _ => Self::Other(value.to_owned()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::NotCloned => "not_cloned",
            Self::Cloning => "cloning",
            Self::Cloned => "cloned",
            Self::Other(value) => value,
        }
    }
}

/// `LogCorruption` failure modes.
#[derive(Debug, thiserror::Error)]
pub enum LogCorruptionError {
    #[error("repo not found or already corrupt")]
    RepoNotFoundOrAlreadyCorrupt,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Go `time.Time` JSON encoding (`time.RFC3339Nano`, UTC `Z` suffix,
/// fractional seconds with trailing zeros removed).
fn json_time(now: OffsetDateTime) -> String {
    let mut fraction = String::new();
    let nanos = now.nanosecond();
    if nanos > 0 {
        fraction = format!(".{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{fraction}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
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

/// `SetLastFetched` failures: the baseline returns `errors.New("repo not
/// found")` when the update touches no row and wraps execution errors with
/// "setting last fetched".
#[derive(Debug)]
pub enum SetLastFetchedError {
    RepoNotFound,
    Database(sqlx::Error),
}

impl std::fmt::Display for SetLastFetchedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RepoNotFound => f.write_str("repo not found"),
            Self::Database(err) => write!(f, "setting last fetched: {err}"),
        }
    }
}

impl std::error::Error for SetLastFetchedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RepoNotFound => None,
            Self::Database(err) => Some(err),
        }
    }
}

impl From<sqlx::Error> for SetLastFetchedError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
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

/// `IterateRepoGitserverStatusOptions`.
#[derive(Debug, Clone, Default)]
pub struct IterateRepoGitserverStatusOptions {
    /// Only iterate over repos that have not been assigned to a shard.
    pub only_without_shard: bool,
    /// Also include soft-deleted repos (their names start with `DELETED-`).
    pub include_deleted: bool,
    pub batch_size: i64,
    /// Last `repo_id` of the previous page.
    pub next_cursor: i32,
}

/// `types.RepoGitserverStatus`.
#[derive(Debug, Clone)]
pub struct RepoGitserverStatus {
    pub id: i32,
    pub name: String,
    pub gitserver_repo: GitserverRepo,
}

fn scan_gitserver_repo(row: &sqlx::postgres::PgRow) -> Result<GitserverRepo, sqlx::Error> {
    Ok(GitserverRepo {
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
    })
}

#[derive(Clone)]
pub struct GitserverReposStore {
    pool: PgPool,
}

impl GitserverReposStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool, for stores and tests that need the same
    /// connection (e.g. creating a `repo` row before exercising the store).
    pub fn pool(&self) -> &PgPool {
        &self.pool
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
	repo_id = (SELECT id FROM repo WHERE name = $3::citext)
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
	repo_id = (SELECT id FROM repo WHERE name = $3::citext)
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
SELECT id, $1 FROM repo WHERE name = $2::citext
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
WHERE repo_id = (SELECT id FROM repo WHERE name = $1::citext)
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
	repo_id = (SELECT id FROM repo WHERE name = $4::citext)
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

    /// `SetLastFetched`: records the fetch timestamps, clears `corrupted_at`
    /// and marks the repository cloned. Unlike the other setters there is no
    /// `IS DISTINCT FROM` guard, so `updated_at` always moves.
    pub async fn set_last_fetched(
        &self,
        repo: &str,
        last_fetched: OffsetDateTime,
        last_changed: OffsetDateTime,
    ) -> Result<(), SetLastFetchedError> {
        let result = sqlx::query(
            r#"
UPDATE gitserver_repos
SET
	corrupted_at = NULL,
	last_fetched = $1,
	last_changed = $2,
	clone_status = $3,
	updated_at = NOW()
WHERE repo_id = (SELECT id FROM repo WHERE name = $4::citext)
"#,
        )
        .bind(last_fetched)
        .bind(last_changed)
        .bind(CloneStatus::Cloned.as_str())
        .bind(repo)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(SetLastFetchedError::RepoNotFound);
        }
        Ok(())
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
WHERE r.name = $1::citext
"#,
        )
        .bind(repo)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(scan_gitserver_repo(&row)?))
    }

    /// `LogCorruption`: prepend `{"time": <now>, "reason": <reason>}` to
    /// `corruption_logs`, keeping at most 10 entries, and set `corrupted_at`.
    /// The reason is truncated to 1 MiB like the baseline.
    ///
    /// The update only matches rows on this shard (or with an empty
    /// `shard_id`) that are not already corrupt; zero or several matched rows
    /// are an error, matching `repo not found or already corrupt`.
    pub async fn log_corruption(
        &self,
        repo: &str,
        reason: &str,
        shard_id: &str,
    ) -> Result<(), LogCorruptionError> {
        const MAX_REASON_SIZE: usize = 1 << 20;
        let reason = if reason.len() > MAX_REASON_SIZE {
            String::from_utf8_lossy(&reason.as_bytes()[..MAX_REASON_SIZE]).into_owned()
        } else {
            reason.to_owned()
        };
        let log = serde_json::json!({
            "time": json_time(OffsetDateTime::now_utc()),
            "reason": reason,
        });
        let result = sqlx::query(
            r#"
UPDATE gitserver_repos AS gtr
SET
	corrupted_at = NOW(),
	corruption_logs = (SELECT jsonb_path_query_array($1::jsonb || gtr.corruption_logs, '$[0 to 9]')),
	updated_at = NOW()
WHERE
	repo_id = (SELECT id FROM repo WHERE name = $2::citext)
AND
	(shard_id = $3 OR shard_id = '')
AND
	corrupted_at IS NULL
"#,
        )
        .bind(log)
        .bind(repo)
        .bind(shard_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(LogCorruptionError::RepoNotFoundOrAlreadyCorrupt);
        }
        Ok(())
    }

    /// `UpdateRepoSizes`: batch-update `repo_size_bytes` for at most 1000
    /// repositories per statement, leaving rows whose size is unchanged
    /// untouched (`IS DISTINCT FROM`). Returns the number of updated rows.
    pub async fn update_repo_sizes(&self, repos: &[(String, i64)]) -> Result<u64, sqlx::Error> {
        const BATCH_SIZE: usize = 1000;
        let mut updated = 0u64;
        for chunk in repos.chunks(BATCH_SIZE) {
            let mut query = String::from(
                r#"
WITH update_data AS (
	SELECT * FROM (VALUES
"#,
            );
            for index in 0..chunk.len() {
                if index > 0 {
                    query.push(',');
                }
                let name = index * 2 + 1;
                query.push_str(&format!("(${name}::citext, ${}::bigint)", name + 1));
            }
            query.push_str(
                r#"
	) AS tmp(repo_name, repo_size_bytes)
)
UPDATE gitserver_repos AS gr
SET
	repo_size_bytes = update_data.repo_size_bytes,
	updated_at = NOW()
FROM repo r
INNER JOIN update_data on update_data.repo_name = r.name
WHERE
	r.id = gr.repo_id
AND
	update_data.repo_size_bytes IS DISTINCT FROM gr.repo_size_bytes
"#,
            );
            let mut statement = sqlx::query(&query);
            for (name, size) in chunk {
                statement = statement.bind(name).bind(size);
            }
            updated += statement.execute(&self.pool).await?.rows_affected();
        }
        Ok(updated)
    }

    /// `IterateRepoGitserverStatus`: page through `gitserver_repos` joined
    /// with `repo`, ordered by `repo_id`. The returned cursor is the last
    /// `repo_id` seen (0 when the page was empty).
    pub async fn iterate_repo_gitserver_status(
        &self,
        options: &IterateRepoGitserverStatusOptions,
    ) -> Result<(Vec<RepoGitserverStatus>, i32), sqlx::Error> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT \
	gr.repo_id, \
	repo.name, \
	gr.clone_status, \
	gr.shard_id, \
	gr.last_error, \
	gr.last_fetched, \
	gr.last_changed, \
	gr.repo_size_bytes, \
	gr.updated_at, \
	gr.corrupted_at, \
	gr.corruption_logs \
FROM gitserver_repos gr \
JOIN repo ON gr.repo_id = repo.id \
WHERE TRUE",
        );
        if !options.include_deleted {
            query.push(" AND repo.deleted_at IS NULL");
        }
        if options.only_without_shard {
            query.push(" AND gr.shard_id = ''");
        }
        if options.next_cursor > 0 {
            query.push(" AND gr.repo_id > ");
            query.push_bind(options.next_cursor);
            query.push(" AND repo.id > ");
            query.push_bind(options.next_cursor);
        }
        query.push(" ORDER BY gr.repo_id ASC");
        if options.batch_size > 0 {
            query.push(" LIMIT ");
            query.push_bind(options.batch_size);
        }

        let rows = query.build().fetch_all(&self.pool).await?;
        let mut next_cursor = 0;
        let mut statuses = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut gitserver_repo = scan_gitserver_repo(row)?;
            let name = row.try_get::<String, _>("name")?;
            // The baseline scans `repo.name` into the status only; the
            // embedded `GitserverRepo.Name` stays empty.
            gitserver_repo.name = String::new();
            next_cursor = gitserver_repo.repo_id;
            statuses.push(RepoGitserverStatus {
                id: gitserver_repo.repo_id,
                name,
                gitserver_repo,
            });
        }
        Ok((statuses, next_cursor))
    }

    /// `Update`: upsert every column of the given rows in one statement. The
    /// baseline locks the matching rows with `FOR UPDATE` and writes
    /// `updated_at = NOW()`.
    pub async fn update(&self, repos: &[GitserverRepo]) -> Result<(), sqlx::Error> {
        if repos.is_empty() {
            return Ok(());
        }
        let mut query = sqlx::QueryBuilder::new(
            "WITH update_data AS (\
	SELECT * FROM (VALUES ",
        );
        for (index, repo) in repos.iter().enumerate() {
            if index > 0 {
                query.push(", ");
            }
            query.push("(");
            // Eight columns per row, matching the baseline's VALUES list.
            query.push_bind(repo.repo_id).push(", ");
            query
                .push_bind(repo.clone_status.as_str().to_owned())
                .push(", ");
            query.push_bind(repo.shard_id.clone()).push(", ");
            query.push_bind(repo.last_error.clone()).push(", ");
            query.push_bind(repo.last_fetched).push(", ");
            query.push_bind(repo.last_changed).push(", ");
            query.push_bind(repo.corrupted_at).push(", ");
            query.push_bind(repo.repo_size_bytes);
            query.push(")");
        }
        query.push(
            " ) AS tmp(repo_id, clone_status, shard_id, last_error, last_fetched, last_changed, corrupted_at, repo_size_bytes)), \
locked_data AS (\
	SELECT update_data.* \
	FROM update_data \
	JOIN gitserver_repos gr ON gr.repo_id = update_data.repo_id \
	ORDER BY update_data.repo_id ASC \
	FOR UPDATE OF gr\
)\
UPDATE gitserver_repos AS gr \
SET \
	clone_status = locked_data.clone_status, \
	shard_id = locked_data.shard_id, \
	last_error = locked_data.last_error, \
	last_fetched = locked_data.last_fetched, \
	last_changed = locked_data.last_changed, \
	corrupted_at = locked_data.corrupted_at, \
	repo_size_bytes = locked_data.repo_size_bytes, \
	updated_at = NOW() \
FROM locked_data \
WHERE locked_data.repo_id = gr.repo_id",
        );
        query.build().execute(&self.pool).await.map(|_| ())
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

            // SetLastFetched has no IS DISTINCT FROM guard: it always clears
            // corrupted_at and forces clone_status = cloned.
            sqlx::query(
                "UPDATE gitserver_repos SET corrupted_at = NOW(), clone_status = 'not_cloned' \
                 WHERE repo_id = (SELECT id FROM repo WHERE name = $1::citext)",
            )
            .bind(&name)
            .execute(&pool)
            .await
            .expect("mark corrupt");
            let last_fetched = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("time");
            let last_changed = OffsetDateTime::from_unix_timestamp(1_700_000_500).expect("time");
            store
                .set_last_fetched(&name, last_fetched, last_changed)
                .await
                .expect("last fetched");
            let repo = store.get_by_name(&name).await.expect("get").expect("row");
            assert_eq!(repo.last_fetched, last_fetched);
            assert_eq!(repo.last_changed, last_changed);
            assert_eq!(repo.clone_status, CloneStatus::Cloned);
            assert_eq!(repo.corrupted_at, None);

            // A repository without a gitserver_repos row is "repo not found".
            let err = store
                .set_last_fetched("missing.example.com/repo", last_fetched, last_changed)
                .await
                .expect_err("missing");
            assert!(matches!(err, SetLastFetchedError::RepoNotFound));
            assert_eq!(err.to_string(), "repo not found");
        }
        .await;

        sqlx::query("DELETE FROM repo WHERE name = $1::citext")
            .bind(&name)
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }

    /// `IterateRepoGitserverStatus` + `Update`, the state syncer's storage
    /// contract: paging by `repo_id`, the deleted/shard filters, and the
    /// batched CTE upsert.
    #[tokio::test]
    async fn iterates_and_updates_gitserver_repos() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = GitserverReposStore::new(pool.clone());

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let names = [
            format!("iterate-test-{suffix}.example.com/one"),
            format!("iterate-test-{suffix}.example.com/two"),
        ];
        for name in &names {
            sqlx::query("INSERT INTO repo(name) VALUES ($1)")
                .bind(name)
                .execute(&pool)
                .await
                .expect("insert repo");
        }
        // Only the first row is assigned to another shard, so
        // `only_without_shard` keeps just the second one.
        store
            .set_clone_status(&names[0], CloneStatus::Cloned, "other-shard")
            .await
            .expect("assign shard");

        let result = async {
            let mut seen = Vec::new();
            let mut options = IterateRepoGitserverStatusOptions {
                include_deleted: true,
                only_without_shard: true,
                batch_size: 2,
                next_cursor: 0,
            };
            loop {
                let (repos, next_cursor) = store
                    .iterate_repo_gitserver_status(&options)
                    .await
                    .expect("iterate");
                seen.extend(repos);
                if next_cursor == 0 {
                    break;
                }
                options.next_cursor = next_cursor;
            }
            let status = seen
                .iter()
                .find(|status| status.name == names[1])
                .expect("second repo is unassigned");
            assert_eq!(status.gitserver_repo.repo_id, status.id);
            // The embedded repo never carries the name, like the baseline.
            assert_eq!(status.gitserver_repo.name, "");
            assert_eq!(status.gitserver_repo.clone_status, CloneStatus::NotCloned);
            assert_eq!(status.gitserver_repo.shard_id, "");
            assert!(!seen.iter().any(|status| status.name == names[0]));

            // Mark the row as corrupt and then let the syncer-style update
            // move it to this shard with a cloned status.
            sqlx::query(
                "UPDATE gitserver_repos SET corrupted_at = NOW() \
                 WHERE repo_id = (SELECT id FROM repo WHERE name = $1::citext)",
            )
            .bind(&names[1])
            .execute(&pool)
            .await
            .expect("mark corrupt");
            let mut updated = store
                .get_by_name(&names[1])
                .await
                .expect("get")
                .expect("row");
            assert!(updated.corrupted_at.is_some());
            updated.clone_status = CloneStatus::Cloned;
            updated.shard_id = "this-shard".to_owned();
            updated.corrupted_at = None;
            store.update(&[updated]).await.expect("update");

            let stored = store
                .get_by_name(&names[1])
                .await
                .expect("get")
                .expect("row");
            assert_eq!(stored.clone_status, CloneStatus::Cloned);
            assert_eq!(stored.shard_id, "this-shard");
            assert_eq!(stored.corrupted_at, None);
        }
        .await;

        for name in &names {
            sqlx::query("DELETE FROM repo WHERE name = $1::citext")
                .bind(name)
                .execute(&pool)
                .await
                .expect("cleanup");
        }
        result
    }
}
