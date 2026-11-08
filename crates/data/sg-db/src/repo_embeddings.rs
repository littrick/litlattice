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
