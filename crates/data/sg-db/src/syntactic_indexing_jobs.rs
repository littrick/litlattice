//! `internal/codeintel/syntactic_indexing/jobstore`: the queries the
//! syntactic indexing scheduler's enqueuer performs.
//!
//! Only the scheduler-facing paths are ported:
//!
//! * `IsQueued` — whether any job already exists for `(repository_id, commit)`
//!   (any state, newest first; `EXISTS` makes the ordering irrelevant);
//! * `InsertIndexingJobs` — inserts `(state, commit, repository_id,
//!   enqueuer_user_id)` rows and re-selects them through
//!   `syntactic_scip_indexing_jobs_with_repository_name`, ordered by id.
//!
//! The scheduler runs as the internal actor, so `database.AuthzQueryConds`
//! renders `TRUE`; the repository join (`repo.deleted_at IS NULL AND
//! repo.blocked IS NULL`) is kept from the baseline query. The worker's
//! `dbworkerstore` reads/writes (`DBWorkerStore`, dequeue, heartbeat) belong to
//! the syntactic indexing worker and are not part of this slice.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `jobstore.SyntacticIndexingJob`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntacticIndexingJob {
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
    pub repository_name: String,
    pub should_reindex: bool,
    pub enqueuer_user_id: i32,
}

/// `jobstore.Queued`.
pub const QUEUED: &str = "queued";

/// `Store.IsQueued`.
pub async fn is_queued(pool: &PgPool, repository_id: i32, commit: &str) -> Result<bool, String> {
    let row = sqlx::query(IS_QUEUED_QUERY)
        .bind(repository_id)
        .bind(commit)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;
    row.try_get::<bool, _>(0).map_err(|err| err.to_string())
}

/// One row of the `InsertIndexingJobs` values list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSyntacticIndexingJob {
    pub state: String,
    pub commit: String,
    pub repository_id: i32,
    pub enqueuer_user_id: i32,
}

/// `Store.InsertIndexingJobs`: returns the inserted jobs (through the view,
/// ordered by id), or an empty list when `jobs` is empty.
pub async fn insert_indexing_jobs(
    pool: &PgPool,
    jobs: &[NewSyntacticIndexingJob],
) -> Result<Vec<SyntacticIndexingJob>, String> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }

    let query = INSERT_INDEX_QUERY.replace("{values}", &values_list(jobs.len()));

    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let mut insert = sqlx::query(&query);
    for job in jobs {
        insert = insert
            .bind(&job.state)
            .bind(&job.commit)
            .bind(job.repository_id)
            .bind(job.enqueuer_user_id);
    }
    let rows = insert
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let ids: Vec<i64> = rows
        .iter()
        .map(|row| row.try_get::<i64, _>(0).map_err(|err| err.to_string()))
        .collect::<Result<_, String>>()?;

    // `authzConds` for the internal actor is `TRUE`; the ids are database
    // values, inlined like `sqlf`'s `%d` would (as binds, but the SQL result
    // is identical).
    let id_list = ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let lookup = GET_INDEXES_BY_IDS_QUERY.replace("{ids}", &id_list);
    let rows = sqlx::query(&lookup)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;

    let jobs = rows
        .iter()
        .map(scan_job)
        .collect::<Result<Vec<_>, String>>()?;

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok(jobs)
}

/// The `(%s, %s, %s, %s)` values list of `insertIndexQuery`, one tuple per
/// job.
fn values_list(jobs: usize) -> String {
    let mut values = Vec::with_capacity(jobs);
    let mut placeholder = 1;
    for _ in 0..jobs {
        values.push(format!(
            "(${placeholder}, ${}, ${}, ${})",
            placeholder + 1,
            placeholder + 2,
            placeholder + 3
        ));
        placeholder += 4;
    }
    values.join(",")
}

fn scan_job(row: &sqlx::postgres::PgRow) -> Result<SyntacticIndexingJob, String> {
    fn column<'r, T>(row: &'r sqlx::postgres::PgRow, name: &str) -> Result<T, String>
    where
        T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    {
        row.try_get(name).map_err(|err| err.to_string())
    }

    Ok(SyntacticIndexingJob {
        id: column(row, "id")?,
        commit: column(row, "commit")?,
        queued_at: column(row, "queued_at")?,
        state: column(row, "state")?,
        failure_message: column(row, "failure_message")?,
        started_at: column(row, "started_at")?,
        finished_at: column(row, "finished_at")?,
        process_after: column(row, "process_after")?,
        num_resets: column(row, "num_resets")?,
        num_failures: column(row, "num_failures")?,
        repository_id: column(row, "repository_id")?,
        repository_name: column(row, "repository_name")?,
        should_reindex: column(row, "should_reindex")?,
        enqueuer_user_id: column(row, "enqueuer_user_id")?,
    })
}

const INSERT_INDEX_QUERY: &str = r#"
INSERT INTO syntactic_scip_indexing_jobs (
	state,
	commit,
	repository_id,
	enqueuer_user_id
)
VALUES {values}
RETURNING id
"#;

const IS_QUEUED_QUERY: &str = r#"
SELECT EXISTS(
	SELECT queued_at
	FROM syntactic_scip_indexing_jobs
	WHERE
		repository_id  = $1 AND
		commit = $2
	ORDER BY queued_at DESC
	LIMIT 1
)
"#;

const GET_INDEXES_BY_IDS_QUERY: &str = r#"
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
	u.repository_name,
	u.should_reindex,
	u.enqueuer_user_id
FROM syntactic_scip_indexing_jobs_with_repository_name u
JOIN repo ON repo.id = u.repository_id AND repo.deleted_at IS NULL AND repo.blocked IS NULL
WHERE u.id IN ({ids}) and TRUE
ORDER BY u.id
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_placeholders_are_per_job() {
        assert_eq!(values_list(0), "");
        assert_eq!(values_list(1), "($1, $2, $3, $4)");
        assert_eq!(values_list(2), "($1, $2, $3, $4),($5, $6, $7, $8)");
    }
}
