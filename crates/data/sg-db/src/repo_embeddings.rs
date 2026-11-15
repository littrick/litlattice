#![forbid(unsafe_code)]

//! `internal/embeddings/background/repo`'s `repo_embedding_jobs` worker store
//! options, plus the `ResetStalled` operation they drive.
//!
//! `NewRepoEmbeddingJobWorkerStore` (`internal/embeddings/background/repo/
//! store.go`) builds a `dbworkerstore.Store` with
//!
//! | option | value |
//! |---|---|
//! | `TableName` | `repo_embedding_jobs` |
//! | `OrderByExpression` | `repo_embedding_jobs.queued_at, repo_embedding_jobs.id` |
//! | `StalledMaxAge` | 60s |
//! | `MaxNumResets` | 5 |
//! | `MaxNumRetries` | 1 |
//!
//! and no `AlternateColumnNames`, so `ResetStalled` uses the default `id`,
//! `state`, `num_resets`, `queued_at`, `started_at`, `finished_at`,
//! `failure_message` and `last_heartbeat_at` columns. The reset statements are
//! the generic `internal/workerutil/dbworker/store` queries; the shared
//! implementation lives in [`crate::batches_janitor`] and is re-exported here so
//! embedding jobs do not have to reach into the batches module.
//!
//! Migrations stay owned by the Go `migrator`; this module only resets queue
//! state.
//!
//! It also carries the scheduling path used by the `repo-embedding-scheduler`
//! job: [`get_embeddable_repos`], [`list_minimal_repos`],
//! [`get_last_repo_embedding_job_for_revision`] and
//! [`create_repo_embedding_job`] (`internal/embeddings/background/repo` and
//! `internal/database`'s `ListMinimalRepos`).

use std::time::Duration;

use sqlx::postgres::Postgres;
use sqlx::Row;

use crate::OffsetDateTime;

pub use crate::batches_janitor::{reset_stalled, ResetWorkerOptions};

/// `NewRepoEmbeddingJobWorkerStore`'s `dbworkerstore.Options` (the subset
/// `ResetStalled` reads).
pub const REPO_EMBEDDING_JOB_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "repo_embedding_jobs",
    state_column: "state",
    stalled_max_age: Duration::from_secs(60),
    max_num_resets: 5,
};

/// `repo.EmbeddableRepo`: a repo matched by an embeddings policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddableRepo {
    pub id: i32,
    pub last_changed: OffsetDateTime,
}

/// `repo.EmbeddableRepoOpts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddableRepoOpts {
    /// `MinimumInterval`: the minimum time since the last queued job.
    pub minimum_interval: Duration,
    /// `PolicyRepositoryMatchLimit`: `None` or a negative value means
    /// unlimited.
    pub policy_repository_match_limit: Option<i64>,
}

/// `repoEmbeddingJobsStore.GetEmbeddableRepos`: repos matched by a global,
/// per-repository or pattern policy that have not been queued within
/// `minimum_interval`.
///
/// The limit only applies to the global-policy arm, exactly like the
/// baseline's `%s -- limit clause`.
pub async fn get_embeddable_repos(
    pool: &sqlx::PgPool,
    opts: EmbeddableRepoOpts,
) -> Result<Vec<EmbeddableRepo>, sqlx::Error> {
    let limit_clause = match opts.policy_repository_match_limit {
        Some(limit) if limit >= 0 => format!("LIMIT {limit}"),
        _ => String::new(),
    };
    let seconds = opts.minimum_interval.as_secs_f64();

    let rows = sqlx::query(&format!(
        r#"
WITH
global_policy_descriptor AS MATERIALIZED (
	SELECT 1
	FROM codeintel_configuration_policies p
	WHERE
		p.embeddings_enabled AND
		p.repository_id IS NULL AND
		p.repository_patterns IS NULL
	LIMIT 1
),
last_queued_jobs AS (
	SELECT DISTINCT ON (repo_id) repo_id, queued_at
	FROM repo_embedding_jobs
	ORDER BY repo_id, queued_at DESC
),
repositories_matching_policy AS (
    (
        SELECT r.id, gr.last_changed
        FROM repo r
        JOIN gitserver_repos gr ON gr.repo_id = r.id
        JOIN global_policy_descriptor gpd ON TRUE
        WHERE
            r.deleted_at IS NULL AND
            r.blocked IS NULL AND
            gr.clone_status = 'cloned'
        ORDER BY stars DESC NULLS LAST, id
        {limit_clause}
    ) UNION ALL (
        SELECT r.id, gr.last_changed
        FROM repo r
        JOIN gitserver_repos gr ON gr.repo_id = r.id
        JOIN codeintel_configuration_policies p ON p.repository_id = r.id
        WHERE
            r.deleted_at IS NULL AND
            r.blocked IS NULL AND
            p.embeddings_enabled AND
            gr.clone_status = 'cloned'
    ) UNION ALL (
        SELECT r.id, gr.last_changed
        FROM repo r
        JOIN gitserver_repos gr ON gr.repo_id = r.id
        JOIN codeintel_configuration_policies_repository_pattern_lookup rpl ON rpl.repo_id = r.id
        JOIN codeintel_configuration_policies p ON p.id = rpl.policy_id
        WHERE
            r.deleted_at IS NULL AND
            r.blocked IS NULL AND
            p.embeddings_enabled AND
            gr.clone_status = 'cloned'
    )
)
SELECT DISTINCT ON (rmp.id) rmp.id, rmp.last_changed
FROM repositories_matching_policy rmp
LEFT JOIN last_queued_jobs lqj ON lqj.repo_id = rmp.id
WHERE lqj.queued_at IS NULL OR lqj.queued_at < current_timestamp - ($1 * '1 second'::interval);
"#
    ))
    .bind(seconds)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| EmbeddableRepo {
            id: row.get("id"),
            last_changed: row.get("last_changed"),
        })
        .collect())
}

/// `types.MinimalRepo`, reduced to the columns the scheduler reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinimalRepo {
    pub id: i32,
    pub name: String,
}

/// `repoStore.ListMinimalRepos` with only `IDs` set and the internal actor
/// (authz bypassed): `deleted_at`/`blocked` filters, ordered by id.
pub async fn list_minimal_repos<'e, E>(
    executor: E,
    ids: &[i32],
) -> Result<Vec<MinimalRepo>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
SELECT repo.id, repo.name
FROM repo
WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL AND repo.id = ANY($1)
ORDER BY repo.id ASC"#,
    )
    .bind(ids.to_vec())
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| MinimalRepo {
            id: row.get("id"),
            name: row.get("name"),
        })
        .collect())
}

/// `repoEmbeddingJobsStore.GetLastRepoEmbeddingJobForRevision`: the id of the
/// most recently queued job for `(repo_id, revision)`, if any.
pub async fn get_last_repo_embedding_job_for_revision<'e, E>(
    executor: E,
    repo_id: i32,
    revision: &str,
) -> Result<Option<i32>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
SELECT repo_embedding_jobs.id
FROM repo_embedding_jobs
WHERE repo_id = $1 AND revision = $2
ORDER BY queued_at DESC
LIMIT 1"#,
    )
    .bind(repo_id)
    .bind(revision)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|row| row.get("id")))
}

/// `repoEmbeddingJobsStore.CreateRepoEmbeddingJob`.
pub async fn create_repo_embedding_job<'e, E>(
    executor: E,
    repo_id: i32,
    revision: &str,
) -> Result<i32, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        "INSERT INTO repo_embedding_jobs (repo_id, revision) VALUES ($1, $2) RETURNING id",
    )
    .bind(repo_id)
    .bind(revision)
    .fetch_one(executor)
    .await?;
    Ok(row.get("id"))
}

/// `repoStore.RepoEmbeddingExists` (`internal/database/repos.go`):
/// `SELECT EXISTS(SELECT 1 FROM repo_embedding_jobs WHERE repo_id = $1 AND
/// state = 'completed')`.
pub async fn repo_embedding_exists<'e, E>(
    executor: E,
    repo_id: i32,
) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
SELECT EXISTS(SELECT 1 FROM repo_embedding_jobs WHERE repo_id = $1 AND state = 'completed')"#,
    )
    .bind(repo_id)
    .fetch_one(executor)
    .await?;
    Ok(row.get("exists"))
}

/// `repoEmbeddingJobsStore.CountRepoEmbeddings`
/// (`internal/embeddings/background/repo/store.go`): the number of distinct
/// repos with a completed embedding job.
pub async fn count_repo_embeddings<'e, E>(executor: E) -> Result<i64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        "SELECT COUNT(DISTINCT repo_id) AS count FROM repo_embedding_jobs WHERE state = 'completed'",
    )
    .fetch_one(executor)
    .await?;
    Ok(row.get("count"))
}

// ---------------------------------------------------------------------------
// GraphQL read surface (`internal/embeddings/background/repo/store.go`)
// ---------------------------------------------------------------------------

/// A `repo_embedding_jobs` row as the `RepoEmbeddingJob` GraphQL resolver
/// reads it (`repoEmbeddingJobsColumns` minus `execution_logs`, which the
/// GraphQL type does not expose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoEmbeddingJobRow {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub cancel: bool,
    pub repo_id: i32,
    pub revision: String,
}

/// `repo.ListOpts` minus pagination (`Query`, `State`, `Repo`). `state` is
/// compared case-insensitively (`strings.ToLower` in the baseline).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepoEmbeddingJobsFilter<'a> {
    pub query: Option<&'a str>,
    pub state: Option<&'a str>,
    pub repo_id: Option<i32>,
}

/// The `database.PaginationArgs` shape the connection resolver passes through
/// (`first`/`last` plus `after`/`before` primary-key values). The baseline
/// orders by `repo_embedding_jobs.id` descending (`Ascending: false`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoEmbeddingJobsPage {
    First {
        limit: i64,
        after: Option<i32>,
        before: Option<i32>,
    },
    Last {
        limit: i64,
        after: Option<i32>,
        before: Option<i32>,
    },
}

enum Bind {
    Text(String),
    Integer(i32),
}

fn push_filter(
    sql: &mut String,
    binds: &mut Vec<Bind>,
    filter: &RepoEmbeddingJobsFilter<'_>,
    join: &mut bool,
) {
    let mut conditions: Vec<String> = Vec::new();
    if let Some(query) = filter.query.filter(|query| !query.is_empty()) {
        *join = true;
        conditions.push(format!("repo.name LIKE ${}", binds.len() + 1));
        binds.push(Bind::Text(format!("%{query}%")));
    }
    if let Some(state) = filter.state.filter(|state| !state.is_empty()) {
        conditions.push(format!("repo_embedding_jobs.state = ${}", binds.len() + 1));
        binds.push(Bind::Text(state.to_lowercase()));
    }
    if let Some(repo_id) = filter.repo_id {
        conditions.push(format!(
            "repo_embedding_jobs.repo_id = ${}",
            binds.len() + 1
        ));
        binds.push(Bind::Integer(repo_id));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
}

const JOB_COLUMNS: &str = "repo_embedding_jobs.id, repo_embedding_jobs.state, \
     repo_embedding_jobs.failure_message, repo_embedding_jobs.queued_at, \
     repo_embedding_jobs.started_at, repo_embedding_jobs.finished_at, \
     repo_embedding_jobs.process_after, repo_embedding_jobs.num_resets, \
     repo_embedding_jobs.num_failures, repo_embedding_jobs.last_heartbeat_at, \
     repo_embedding_jobs.worker_hostname, repo_embedding_jobs.cancel, \
     repo_embedding_jobs.repo_id, repo_embedding_jobs.revision";

fn scan_job(row: &sqlx::postgres::PgRow) -> RepoEmbeddingJobRow {
    RepoEmbeddingJobRow {
        id: row.get("id"),
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        queued_at: row.get("queued_at"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        process_after: row.get("process_after"),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
        last_heartbeat_at: row.get("last_heartbeat_at"),
        worker_hostname: row.get("worker_hostname"),
        cancel: row.get("cancel"),
        repo_id: row.get("repo_id"),
        revision: row.get("revision"),
    }
}

/// `repoEmbeddingJobsStore.CountRepoEmbeddingJobs`.
pub async fn count_repo_embedding_jobs(
    pool: &sqlx::PgPool,
    filter: &RepoEmbeddingJobsFilter<'_>,
) -> Result<i64, sqlx::Error> {
    let mut sql = String::from("SELECT COUNT(*) AS count FROM repo_embedding_jobs");
    let mut binds = Vec::new();
    let mut join = false;
    // The join must precede the WHERE clause; build it in two passes.
    let mut where_sql = String::new();
    push_filter(&mut where_sql, &mut binds, filter, &mut join);
    if join {
        sql.push_str(" JOIN repo ON repo.id = repo_embedding_jobs.repo_id");
    }
    sql.push_str(&where_sql);

    let mut query = sqlx::query(&sql);
    for bind in binds {
        query = match bind {
            Bind::Text(value) => query.bind(value),
            Bind::Integer(value) => query.bind(value),
        };
    }
    let row = query.fetch_one(pool).await?;
    Ok(row.get("count"))
}

/// `repoEmbeddingJobsStore.ListRepoEmbeddingJobs` with the connection
/// resolver's `limit + 1` probe and `id` ordering.
pub async fn list_repo_embedding_jobs(
    pool: &sqlx::PgPool,
    filter: &RepoEmbeddingJobsFilter<'_>,
    page: &RepoEmbeddingJobsPage,
) -> Result<Vec<RepoEmbeddingJobRow>, sqlx::Error> {
    let mut sql = format!("SELECT {JOB_COLUMNS} FROM repo_embedding_jobs");
    let mut binds = Vec::new();
    let mut join = false;

    let (limit, order, after, before) = match page {
        RepoEmbeddingJobsPage::First {
            limit,
            after,
            before,
        } => (*limit, "DESC", *after, *before),
        // `AppendOrderToQuery` flips the order for `last`; the connection
        // resolver reverses the fetched page afterwards.
        RepoEmbeddingJobsPage::Last {
            limit,
            after,
            before,
        } => (*limit, "ASC", *after, *before),
    };

    let mut where_sql = String::new();
    push_filter(&mut where_sql, &mut binds, filter, &mut join);
    let mut conditions: Vec<String> = Vec::new();
    // `Ascending == false`: `after` is `id < cursor`, `before` is `id > cursor`.
    if let Some(after) = after {
        conditions.push(format!(
            "repo_embedding_jobs.id < ${}",
            binds.len() + 1
        ));
        binds.push(Bind::Integer(after));
    }
    if let Some(before) = before {
        conditions.push(format!(
            "repo_embedding_jobs.id > ${}",
            binds.len() + 1
        ));
        binds.push(Bind::Integer(before));
    }
    if !conditions.is_empty() {
        if where_sql.is_empty() {
            where_sql.push_str(" WHERE ");
            where_sql.push_str(&conditions.join(" AND "));
        } else {
            where_sql.push_str(" AND ");
            where_sql.push_str(&conditions.join(" AND "));
        }
    }
    if join {
        sql.push_str(" JOIN repo ON repo.id = repo_embedding_jobs.repo_id");
    }
    sql.push_str(&where_sql);
    sql.push_str(&format!(
        " ORDER BY repo_embedding_jobs.id {order} LIMIT ${}",
        binds.len() + 1
    ));
    binds.push(Bind::Integer(i32::try_from(limit).unwrap_or(i32::MAX)));

    let mut query = sqlx::query(&sql);
    for bind in binds {
        query = match bind {
            Bind::Text(value) => query.bind(value),
            Bind::Integer(value) => query.bind(value),
        };
    }
    Ok(query
        .fetch_all(pool)
        .await?
        .iter()
        .map(scan_job)
        .collect())
}

/// `repoEmbeddingJobStatsResolver`'s source row: the code/text counters plus
/// the `map[string]int` skip reasons.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoEmbeddingStatsRow {
    pub code_files_scheduled: i64,
    pub code_files_embedded: i64,
    pub code_files_skipped: std::collections::BTreeMap<String, i64>,
    pub text_files_scheduled: i64,
    pub text_files_embedded: i64,
    pub text_files_skipped: std::collections::BTreeMap<String, i64>,
}

/// `repoEmbeddingJobsStore.GetRepoEmbeddingJobStats`: a missing stats row
/// yields the zero value (the baseline loops over no rows without error).
pub async fn get_repo_embedding_job_stats(
    pool: &sqlx::PgPool,
    job_id: i32,
) -> Result<RepoEmbeddingStatsRow, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT
  code_files_total,
  code_files_embedded,
  code_files_skipped,
  text_files_total,
  text_files_embedded,
  text_files_skipped
FROM repo_embedding_job_stats
WHERE job_id = $1"#,
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(RepoEmbeddingStatsRow::default());
    };
    let code_skipped: sqlx::types::Json<std::collections::BTreeMap<String, i64>> =
        row.get("code_files_skipped");
    let text_skipped: sqlx::types::Json<std::collections::BTreeMap<String, i64>> =
        row.get("text_files_skipped");
    Ok(RepoEmbeddingStatsRow {
        code_files_scheduled: row.get("code_files_total"),
        code_files_embedded: row.get("code_files_embedded"),
        code_files_skipped: code_skipped.0,
        text_files_scheduled: row.get("text_files_total"),
        text_files_embedded: row.get("text_files_embedded"),
        text_files_skipped: text_skipped.0,
    })
}

/// `repoEmbeddingJobsStore.CancelRepoEmbeddingJob`, including the
/// `could not find cancellable embedding job: jobID=%d` no-row error.
pub async fn cancel_repo_embedding_job(
    pool: &sqlx::PgPool,
    job_id: i32,
) -> Result<(), String> {
    let result = sqlx::query(
        r#"
UPDATE repo_embedding_jobs
SET
    cancel = TRUE,
    state = CASE WHEN repo_embedding_jobs.state = 'processing' THEN repo_embedding_jobs.state ELSE 'canceled' END,
    finished_at = CASE WHEN repo_embedding_jobs.state = 'processing' THEN repo_embedding_jobs.finished_at ELSE NOW() END
WHERE
    id = $1 AND state IN ('queued', 'processing')"#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| error.to_string())?;
    if result.rows_affected() == 0 {
        return Err(format!(
            "could not find cancellable embedding job: jobID={job_id}"
        ));
    }
    Ok(())
}
