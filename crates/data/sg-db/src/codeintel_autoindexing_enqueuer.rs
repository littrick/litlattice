#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/store/enqueuer.go` — the
//! `lsif_indexes` half of the autoindexing store used by the production
//! `IndexEnqueuer` (`queueJobsForRepoAndCommit`).
//!
//! Ported 1:1:
//!
//! * [`is_queued`] — `IsQueued`: true when a usable (non-deleted, not
//!   reindex-flagged, not already-associated) upload exists for the
//!   repo/commit, or when every most-recent `(root, indexer)` index record is
//!   not flagged for reindexing;
//! * [`is_queued_root_indexer`] — `IsQueuedRootIndexer`: the latest
//!   `(repo, commit, root, indexer)` record is not flagged for reindexing;
//! * [`insert_jobs`] — `InsertJobs`: bulk-inserts `AutoIndexJob`s (nil docker
//!   steps / local steps / indexer args normalized to empty arrays) and
//!   re-reads them, joined with the repository name, in id order.
//!
//! Representation notes:
//!
//! * [`AutoIndexJob`] mirrors `shared.AutoIndexJob`; nullable columns become
//!   `Option`, `execution_logs` (`json[]`) becomes
//!   `Option<Vec<serde_json::Value>>` (NULL stays `None`, so the baseline's
//!   nil-vs-empty distinction is preserved), and `requested_envvars`
//!   (`text[]`) becomes `Option<Vec<String>>`.
//! * `database.AuthzQueryConds` is omitted (`TRUE`): the worker calls this
//!   store with an internal actor, for which the baseline collapses the
//!   conditions to `TRUE`. The unported GraphQL callers are user-scoped and
//!   will need the real conditions when they are ported.
//! * The `scanJob` conversion of `execution_logs` JSON into
//!   `executor.ExecutionLogEntry` is not modelled; the raw JSON values are
//!   returned.
//!
//! Migrations stay owned by the Go `migrator`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::types::Json;
use sqlx::Row;

/// `shared.DockerStep`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DockerStep {
    pub root: String,
    pub image: String,
    pub commands: Vec<String>,
}

/// `shared.AutoIndexJob` (see the module docs for the representation notes).
#[derive(Debug, Clone, PartialEq)]
pub struct AutoIndexJob {
    pub id: i64,
    pub commit: String,
    pub queued_at: OffsetDateTime,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub repository_id: i32,
    pub local_steps: Vec<String>,
    pub repository_name: String,
    pub docker_steps: Vec<DockerStep>,
    pub root: String,
    pub indexer: String,
    pub indexer_args: Vec<String>,
    pub outfile: String,
    pub execution_logs: Option<Vec<serde_json::Value>>,
    pub rank: Option<i64>,
    pub associated_upload_id: Option<i64>,
    pub should_reindex: bool,
    pub requested_env_vars: Option<Vec<String>>,
    pub enqueuer_user_id: i32,
}

impl Default for AutoIndexJob {
    /// A zero-valued job. Only the columns `InsertJobs` reads are meaningful
    /// for an input value; the rest are filled by the re-read query.
    fn default() -> Self {
        Self {
            id: 0,
            commit: String::new(),
            queued_at: OffsetDateTime::UNIX_EPOCH,
            state: String::new(),
            failure_message: None,
            started_at: None,
            finished_at: None,
            process_after: None,
            num_resets: 0,
            num_failures: 0,
            repository_id: 0,
            local_steps: Vec::new(),
            repository_name: String::new(),
            docker_steps: Vec::new(),
            root: String::new(),
            indexer: String::new(),
            indexer_args: Vec::new(),
            outfile: String::new(),
            execution_logs: None,
            rank: None,
            associated_upload_id: None,
            should_reindex: false,
            requested_env_vars: None,
            enqueuer_user_id: 0,
        }
    }
}

const IS_QUEUED_SQL: &str = "
-- The query has two parts, 'A' UNION 'B', where 'A' is true if there's a manual and
-- reachable upload for a repo/commit pair. This signifies that the user has configured
-- manual indexing on a repo and we shouldn't clobber it with autoindexing. The other
-- query 'B' is true if there's an auto-index record already enqueued for this repo. This
-- signifies that we've already infered jobs for this repo/commit pair so we can skip it
-- (we should infer the same jobs).

-- We added a way to say \"you might infer different jobs\" for part 'B' by adding the
-- check on u.should_reindex. We're now adding a way to say \"the indexer might result
-- in a different output_ for part A, allowing auto-indexing to clobber records that
-- have undergone some possibly lossy transformation (like LSIF -> SCIP conversion in-db).
SELECT
	EXISTS (
		SELECT 1
		FROM lsif_uploads u
		WHERE
			repository_id = $1 AND
			commit = $2 AND
			state NOT IN ('deleting', 'deleted') AND
			associated_index_id IS NULL AND
			NOT u.should_reindex
	)

	OR

	-- We want IsQueued to return true when there exists auto-indexing job records
	-- and none of them are marked for reindexing. If we have one or more rows and
	-- ALL of them are not marked for re-indexing, we'll block additional indexing
	-- attempts.
	(
		SELECT COALESCE(bool_and(NOT should_reindex), false)
		FROM (
			-- For each distinct (root, indexer) pair, use the most recently queued
			-- index as the authoritative attempt.
			SELECT DISTINCT ON (root, indexer) should_reindex
			FROM lsif_indexes
			WHERE repository_id = $3 AND commit = $4
			ORDER BY root, indexer, queued_at DESC
		) _
	)
";

/// `store.IsQueued`.
pub async fn is_queued(
    pool: &PgPool,
    repository_id: i32,
    commit: &str,
) -> Result<bool, sqlx::Error> {
    fetch_is_queued(pool, repository_id, commit).await
}

/// [`is_queued`] inside the caller's transaction (`QueueRepoRev`).
pub async fn is_queued_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
    commit: &str,
) -> Result<bool, sqlx::Error> {
    fetch_is_queued(&mut **tx, repository_id, commit).await
}

async fn fetch_is_queued<'e, E>(
    executor: E,
    repository_id: i32,
    commit: &str,
) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let row = sqlx::query(IS_QUEUED_SQL)
        .bind(repository_id)
        .bind(commit)
        .bind(repository_id)
        .bind(commit)
        .fetch_one(executor)
        .await?;
    row.try_get(0)
}

const IS_QUEUED_ROOT_INDEXER_SQL: &str = "
SELECT NOT should_reindex
FROM lsif_indexes
WHERE
	repository_id  = $1 AND
	commit = $2 AND
	root = $3 AND
	indexer = $4
ORDER BY queued_at DESC
LIMIT 1
";

/// `store.IsQueuedRootIndexer`.
pub async fn is_queued_root_indexer(
    pool: &PgPool,
    repository_id: i32,
    commit: &str,
    root: &str,
    indexer: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(IS_QUEUED_ROOT_INDEXER_SQL)
        .bind(repository_id)
        .bind(commit)
        .bind(root)
        .bind(indexer)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(row) => row.try_get(0),
        None => Ok(false),
    }
}

const GET_AUTO_INDEX_JOBS_BY_IDS_SQL: &str = "
SELECT
	u.id,
	u.commit,
	u.queued_at,
	u.state,
	u.failure_message,
	u.started_at,
	u.finished_at,
	u.process_after,
	u.num_resets,
	u.num_failures,
	u.repository_id,
	repo.name,
	u.docker_steps,
	u.root,
	u.indexer,
	u.indexer_args,
	u.outfile,
	u.execution_logs,
	s.rank,
	u.local_steps,
	(SELECT MAX(id) FROM lsif_uploads WHERE associated_index_id = u.id) AS associated_upload_id,
	u.should_reindex,
	u.requested_envvars,
	u.enqueuer_user_id
FROM lsif_indexes u
LEFT JOIN (
	SELECT
		r.id,
		ROW_NUMBER() OVER (ORDER BY COALESCE(r.process_after, r.queued_at), r.id) as rank
	FROM lsif_indexes_with_repository_name r
	WHERE r.state = 'queued'
) s
ON u.id = s.id
JOIN repo ON repo.id = u.repository_id
WHERE repo.deleted_at IS NULL AND u.id IN (%s) AND TRUE
ORDER BY u.id
";

/// `store.InsertJobs`.
///
/// `enqueuer_user_id` is the actor's UID (the baseline reads it from the
/// context; the worker uses the internal actor, `0`). Returns `None` — the
/// baseline's nil slice — when `jobs` is empty.
pub async fn insert_jobs(
    pool: &PgPool,
    jobs: &[AutoIndexJob],
    enqueuer_user_id: i32,
) -> Result<Option<Vec<AutoIndexJob>>, sqlx::Error> {
    if jobs.is_empty() {
        return Ok(None);
    }

    let mut tx = pool.begin().await?;

    let mut values = String::new();
    let mut index = 1;
    for _ in 0..jobs.len() {
        if index > 1 {
            values.push(',');
        }
        values.push('(');
        for column in 0..12 {
            if column > 0 {
                values.push(',');
            }
            values.push_str(&format!("${index}"));
            index += 1;
        }
        values.push(')');
    }
    let insert_sql = format!(
        "INSERT INTO lsif_indexes (\n\tstate,\n\tcommit,\n\trepository_id,\n\tdocker_steps,\n\tlocal_steps,\n\troot,\n\tindexer,\n\tindexer_args,\n\toutfile,\n\texecution_logs,\n\trequested_envvars,\n\tenqueuer_user_id\n)\nVALUES {values}\nRETURNING id"
    );

    let mut query = sqlx::query(&insert_sql);
    for job in jobs {
        let docker_steps: Vec<Json<DockerStep>> =
            job.docker_steps.iter().cloned().map(Json).collect();
        query = query
            .bind(&job.state)
            .bind(&job.commit)
            .bind(job.repository_id)
            .bind(docker_steps)
            .bind(&job.local_steps)
            .bind(&job.root)
            .bind(&job.indexer)
            .bind(&job.indexer_args)
            .bind(&job.outfile)
            .bind(job.execution_logs.as_deref())
            .bind(job.requested_env_vars.as_deref())
            .bind(enqueuer_user_id);
    }

    let ids: Vec<i64> = query
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(|row| row.get::<i64, _>(0))
        .collect();

    let mut id_list = String::new();
    let mut index = 1;
    for _ in 0..ids.len() {
        if index > 1 {
            id_list.push(',');
        }
        id_list.push_str(&format!("${index}"));
        index += 1;
    }
    let select_sql = GET_AUTO_INDEX_JOBS_BY_IDS_SQL.replace("%s", &id_list);
    let mut query = sqlx::query(&select_sql);
    for id in &ids {
        query = query.bind(id);
    }
    let rows = query.fetch_all(&mut *tx).await?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        result.push(scan_job(&row)?);
    }

    tx.commit().await?;
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_job_matches_the_baseline_zero_value() {
        let job = AutoIndexJob::default();
        assert_eq!(job.id, 0);
        assert_eq!(job.state, "");
        assert_eq!(job.num_resets, 0);
        assert_eq!(job.enqueuer_user_id, 0);
        assert!(job.docker_steps.is_empty());
        assert!(job.local_steps.is_empty());
        assert!(job.indexer_args.is_empty());
        assert!(job.execution_logs.is_none());
        assert!(job.requested_env_vars.is_none());
        assert!(job.rank.is_none());
        assert!(job.associated_upload_id.is_none());
        assert!(!job.should_reindex);
    }

    #[test]
    fn docker_step_marshals_like_the_baseline() {
        let step = DockerStep {
            root: String::new(),
            image: "alpine".to_owned(),
            commands: vec!["echo".to_owned()],
        };
        assert_eq!(
            serde_json::to_string(&step).expect("json"),
            r#"{"root":"","image":"alpine","commands":["echo"]}"#
        );
    }
}

fn scan_job(row: &sqlx::postgres::PgRow) -> Result<AutoIndexJob, sqlx::Error> {
    let docker_steps: Vec<Json<DockerStep>> = row.try_get("docker_steps")?;
    Ok(AutoIndexJob {
        id: row.try_get("id")?,
        commit: row.try_get("commit")?,
        queued_at: row.try_get("queued_at")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        process_after: row.try_get("process_after")?,
        num_resets: row.try_get("num_resets")?,
        num_failures: row.try_get("num_failures")?,
        repository_id: row.try_get("repository_id")?,
        repository_name: row.try_get("name")?,
        docker_steps: docker_steps.into_iter().map(|step| step.0).collect(),
        root: row.try_get("root")?,
        indexer: row.try_get("indexer")?,
        indexer_args: row.try_get("indexer_args")?,
        outfile: row.try_get("outfile")?,
        execution_logs: row.try_get("execution_logs")?,
        rank: row.try_get("rank")?,
        associated_upload_id: row.try_get("associated_upload_id")?,
        should_reindex: row.try_get("should_reindex")?,
        requested_env_vars: row.try_get("requested_envvars")?,
        local_steps: row.try_get("local_steps")?,
        enqueuer_user_id: row.try_get("enqueuer_user_id")?,
    })
}
