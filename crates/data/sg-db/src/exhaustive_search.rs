#![forbid(unsafe_code)]

//! `internal/search/exhaustive/{types,store}` — the exhaustive search job
//! storage used by the `exhaustive-search-job` worker job and the search-jobs
//! GraphQL API.
//!
//! Ported 1:1 from `internal/search/exhaustive/store`:
//!
//! * `CreateExhaustiveSearchJob` / `CancelSearchJob` /
//!   `GetExhaustiveSearchJob` / `UserHasAccess` / `ListExhaustiveSearchJobs` /
//!   `DeleteExhaustiveSearchJob`;
//! * `CreateExhaustiveSearchRepoJob` / `CreateExhaustiveSearchRepoRevisionJob`
//!   / `GetQueryRepoRev`;
//! * `GetAggregateRepoRevState` / `GetJobLogs`.
//!
//! The three worker stores (`dbworkerstore.New` over `exhaustive_search_jobs`,
//! `exhaustive_search_repo_jobs` and `exhaustive_search_repo_revision_jobs`)
//! are not part of this module yet; their dequeue/reset SQL is shared with the
//! generic `workerutil/dbworker/store` and lands with the worker routines.
//!
//! Representation notes (see docs §113.3):
//!
//! * NULL timestamps become `None`; the baseline scans them into
//!   `time.Time` and leaves the zero value. Every reader treats the zero value
//!   as "unset" (`formatOrNULL`, `gqlutil.FromTime`), so the two are
//!   equivalent; `failure_message` NULL becomes `""` exactly like
//!   `dbutil.NullString`.
//! * `execution_logs` (`json[]`) and `last_heartbeat_at` are never read by the
//!   baseline scan targets; the projections omit them. `last_heartbeat_at` is
//!   a column the generic dequeue store maintains, not one of
//!   `exhaustiveSearchJobColumns`.
//! * `actor.FromContext` is reduced to [`Actor`] (`uid`, `internal`); the
//!   store methods perform `auth.CheckSiteAdminOrSameUser` /
//!   `auth.CheckUserIsSiteAdmin` against the `users` table exactly like the
//!   baseline, including swallowing non-nil errors into
//!   `ErrMustBeSiteAdminOrSameUser`.
//! * Pagination (`database.PaginationArgs`) is not wired yet: its only caller
//!   is the unported search-jobs GraphQL connection, which supplies
//!   `First`/`Last`/`After`/`Before`. [`list_exhaustive_search_jobs`] therefore
//!   takes no pagination and returns the baseline order for `nil`
//!   `PaginationArgs` (no `ORDER BY`, as in the baseline).
//!
//! Migrations stay owned by the Go `migrator`; every SQL string below is
//! copied from the baseline store.

use std::time::Duration;

use sqlx::postgres::{PgPool, PgRow};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;

/// `types.JobState` (a Go string type with no validation on scan): the raw
/// database value plus the helpers the callers use.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobState(String);

impl JobState {
    pub const QUEUED: &'static str = "queued";
    pub const PROCESSING: &'static str = "processing";
    pub const ERRORED: &'static str = "errored";
    pub const FAILED: &'static str = "failed";
    pub const COMPLETED: &'static str = "completed";
    pub const CANCELED: &'static str = "canceled";

    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `types.JobState.ToGraphQL` (`strings.ToUpper`).
    pub fn to_graphql(&self) -> String {
        self.0.to_ascii_uppercase()
    }

    /// `types.JobState.IsTerminal`.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.0.as_str(),
            Self::COMPLETED | Self::FAILED | Self::CANCELED
        )
    }
}

impl std::fmt::Display for JobState {
    /// `types.JobState.String` (`strings.ToLower`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_ascii_lowercase())
    }
}

/// `types.WorkerJob` minus `LastHeartbeatAt` (never scanned; see module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerJob {
    pub state: JobState,
    pub failure_message: String,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i64,
    pub num_failures: i64,
    pub worker_hostname: String,
    pub cancel: bool,
}

/// `types.ExhaustiveSearchJob` (`exhaustive_search_jobs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExhaustiveSearchJob {
    pub worker: WorkerJob,
    pub id: i64,
    pub initiator_id: i32,
    pub query: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// `AggState`: only set by the list/get queries' aggregate subquery.
    pub agg_state: JobState,
    pub is_aggregated: bool,
}

/// `types.ExhaustiveSearchRepoJob` (`exhaustive_search_repo_jobs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExhaustiveSearchRepoJob {
    pub worker: WorkerJob,
    pub id: i64,
    pub repo_id: i32,
    pub ref_spec: String,
    pub search_job_id: i64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `types.ExhaustiveSearchRepoRevisionJob`
/// (`exhaustive_search_repo_revision_jobs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExhaustiveSearchRepoRevisionJob {
    pub worker: WorkerJob,
    pub id: i64,
    pub search_repo_job_id: i64,
    pub revision: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `cmd/worker/internal/search/janitor.go`'s `job`: a search job that has not
/// been aggregated yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnaggregatedSearchJob {
    pub id: i64,
    pub initiator_id: i32,
}

/// `types.SearchJobLog`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchJobLog {
    pub id: i64,
    pub repo_name: String,
    pub revision: String,
    pub state: JobState,
    pub failure_message: String,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
}

/// `types.RepositoryRevSpecs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRevSpecs {
    /// `api.RepoID`.
    pub repository: i32,
    /// `types.RevisionSpecifiers`: the serialized `[]query.RevisionSpecifier`.
    pub revision_specifiers: String,
}

/// `types.RepositoryRevision`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRevision {
    pub repository_rev_specs: RepositoryRevSpecs,
    /// The resolved revision (`HEAD`, a branch name, a commit hash, …).
    pub revision: String,
}

/// `types.RepoRevJobStats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepoRevJobStats {
    pub total: i32,
    pub completed: i32,
    pub failed: i32,
    pub in_progress: i32,
}

/// `types.RevisionSpecifiers.Get`: `:` is the separator used by the query
/// language.
pub fn revision_specifiers_get(value: &str) -> Vec<String> {
    value.split(':').map(str::to_owned).collect()
}

/// `types.RevisionSpecifierJoin`: the inverse of [`revision_specifiers_get`].
pub fn revision_specifiers_join(specifiers: &[String]) -> String {
    specifiers.join(":")
}

/// `actor.Actor` reduced to the fields these store methods read.
///
/// `uid == 0` is unauthenticated; `internal` maps `WithInternalActor`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Actor {
    pub uid: i32,
    pub internal: bool,
}

impl Actor {
    /// `actor.Internal()`.
    pub fn internal() -> Self {
        Self {
            uid: 0,
            internal: true,
        }
    }

    /// `actor.FromUser(uid)`.
    pub fn user(uid: i32) -> Self {
        Self {
            uid,
            internal: false,
        }
    }

    /// `Actor.IsAuthenticated`.
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }
}

/// The baseline's errors, with the `errors.New`/`errors.Errorf` texts.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sqlx(#[from] sqlx::Error),

    /// `store.ErrNoResults`.
    #[error("no results")]
    NoResults,

    /// `errors.Wrapf(ErrNoResults, "failed to scan job with id %d: %s", …)`.
    #[error("failed to scan job with id {id}: {detail}: no results")]
    ScanJob { id: i64, detail: String },

    #[error("missing query")]
    MissingQuery,

    #[error("missing initiator ID")]
    MissingInitiatorId,

    #[error("missing search job ID")]
    MissingSearchJobId,

    #[error("missing repo ID")]
    MissingRepoId,

    #[error("missing ref spec")]
    MissingRefSpec,

    #[error("missing search repo job ID")]
    MissingSearchRepoJobId,

    #[error("missing revision")]
    MissingRevision,

    #[error("not authenticated")]
    NotAuthenticated,

    /// `auth.ErrMustBeSiteAdminOrSameUser`.
    #[error("must be authenticated as the authorized user or site admin")]
    MustBeSiteAdminOrSameUser,

    #[error("can only list jobs for an authenticated user")]
    ListRequiresAuthenticatedUser,

    #[error("cannot filter by user id if not a site admin")]
    CannotFilterByUserIds,

    /// `database/sql`'s `sql.ErrNoRows` text, used where the baseline returns
    /// the raw scanner error (e.g. `GetQueryRepoRev`).
    #[error("sql: no rows in result set")]
    NoRows,
}

/// `exhaustiveSearchJobColumns` minus `execution_logs`; see the module docs
/// for the omissions (`execution_logs`, `last_heartbeat_at`).
pub const EXHAUSTIVE_SEARCH_JOB_COLUMNS: &str = "id, initiator_id, state, query, \
     failure_message, started_at, finished_at, process_after, num_resets, \
     num_failures, worker_hostname, cancel, created_at, updated_at, is_aggregated";

pub const REPO_SEARCH_JOB_COLUMNS: &str = "id, state, repo_id, ref_spec, \
     search_job_id, failure_message, started_at, finished_at, process_after, \
     num_resets, num_failures, worker_hostname, cancel, created_at, updated_at";

pub const REV_SEARCH_JOB_COLUMNS: &str = "id, state, search_repo_job_id, \
     revision, failure_message, started_at, finished_at, process_after, \
     num_resets, num_failures, worker_hostname, cancel, created_at, updated_at";

/// `createExhaustiveSearchJobQueryFmtr`.
pub const CREATE_EXHAUSTIVE_SEARCH_JOB_SQL: &str =
    "INSERT INTO exhaustive_search_jobs (query, initiator_id)\nVALUES ($1, $2)\nRETURNING id";

/// `cancelJobFmtStr`.
pub const CANCEL_SEARCH_JOB_SQL: &str = "
WITH updated_jobs AS (
    UPDATE exhaustive_search_jobs
    SET CANCEL = TRUE,
    state = CASE WHEN exhaustive_search_jobs.state = 'processing' THEN exhaustive_search_jobs.state ELSE 'canceled' END,
    finished_at = CASE WHEN exhaustive_search_jobs.state = 'processing' THEN exhaustive_search_jobs.finished_at ELSE $1 END
    WHERE id = $2
    RETURNING id
),
updated_repo_jobs AS (
    UPDATE exhaustive_search_repo_jobs
    SET CANCEL = TRUE,
    state = CASE WHEN exhaustive_search_repo_jobs.state = 'processing' THEN exhaustive_search_repo_jobs.state ELSE 'canceled' END,
    finished_at = CASE WHEN exhaustive_search_repo_jobs.state = 'processing' THEN exhaustive_search_repo_jobs.finished_at ELSE $3 END
    WHERE search_job_id IN (SELECT id FROM updated_jobs)
    RETURNING id
),
updated_repo_revision_jobs AS (
    UPDATE exhaustive_search_repo_revision_jobs
    SET CANCEL = TRUE,
    state = CASE WHEN exhaustive_search_repo_revision_jobs.state = 'processing' THEN exhaustive_search_repo_revision_jobs.state ELSE 'canceled' END,
    finished_at = CASE WHEN exhaustive_search_repo_revision_jobs.state = 'processing' THEN exhaustive_search_repo_revision_jobs.finished_at ELSE $4 END
    WHERE search_repo_job_id IN (SELECT id FROM updated_repo_jobs)
    RETURNING id
)
SELECT (SELECT count(*) FROM updated_jobs) + (SELECT count(*) FROM updated_repo_jobs) + (SELECT count(*) FROM updated_repo_revision_jobs) as total_canceled
";

/// `deleteExhaustiveSearchJobQueryFmtStr`.
pub const DELETE_EXHAUSTIVE_SEARCH_JOB_SQL: &str =
    "DELETE FROM exhaustive_search_jobs\nWHERE id = $1";

/// `createExhaustiveSearchRepoJobQueryFmtr`.
pub const CREATE_REPO_SEARCH_JOB_SQL: &str = "\
INSERT INTO exhaustive_search_repo_jobs (repo_id, search_job_id, ref_spec)
VALUES ($1, $2, $3)
RETURNING id";

/// `createExhaustiveSearchRepoRevisionJobQueryFmtr`.
pub const CREATE_REV_SEARCH_JOB_SQL: &str = "\
INSERT INTO exhaustive_search_repo_revision_jobs (revision, search_repo_job_id)
VALUES ($1, $2)
RETURNING id";

/// `getQueryRepoRevFmtStr`.
pub const GET_QUERY_REPO_REV_SQL: &str = "\
SELECT sj.id, sj.initiator_id, sj.query, srj.repo_id, srj.ref_spec
FROM exhaustive_search_repo_jobs srj
JOIN exhaustive_search_jobs sj ON srj.search_job_id = sj.id
WHERE srj.id = $1";

/// `getJobLogsFmtStr` before the `WHERE`/`LIMIT` substitution.
pub const GET_JOB_LOGS_SQL: &str = "\
SELECT
rjj.id,
r.name,
rjj.revision,
rjj.state,
rjj.failure_message,
rjj.started_at,
rjj.finished_at
FROM exhaustive_search_repo_revision_jobs rjj
JOIN exhaustive_search_repo_jobs rj ON rjj.search_repo_job_id = rj.id
JOIN repo r ON r.id = rj.repo_id
";

/// `listSearchJobs`'s query.
pub const LIST_UNAGGREGATED_SEARCH_JOBS_SQL: &str =
    "SELECT id, initiator_id FROM exhaustive_search_jobs WHERE is_aggregated = false";

/// `updateSearchJobStatus`'s query.
pub const UPDATE_SEARCH_JOB_STATE_SQL: &str =
    "UPDATE exhaustive_search_jobs SET state = $1 WHERE id = $2";

/// `deleteRepoJobs`'s query.
pub const DELETE_SEARCH_JOB_REPO_JOBS_SQL: &str =
    "DELETE FROM exhaustive_search_repo_jobs WHERE search_job_id = $1";

/// `setJobAsAggregated`'s query.
pub const SET_SEARCH_JOB_AGGREGATED_SQL: &str =
    "UPDATE exhaustive_search_jobs SET is_aggregated = true WHERE id = $1";

/// `getAggregateStateTable` with the three references to the job id bound as
/// `$1`. The correlated form used by the list/get projections is
/// [`aggregate_state_correlated`].
pub const AGGREGATE_STATE_TABLE_SQL: &str = "
SELECT state, COUNT(*) as count
FROM
  (
		(SELECT state
		 FROM exhaustive_search_jobs sj
		 WHERE sj.id = $1)
    UNION ALL
		(SELECT state
		 FROM exhaustive_search_repo_jobs rj
		 WHERE rj.search_job_id = $1)
    UNION ALL
		(SELECT rrj.state
		 FROM exhaustive_search_repo_revision_jobs rrj
		JOIN exhaustive_search_repo_jobs rj ON rrj.search_repo_job_id = rj.id
		WHERE rj.search_job_id = $1)
  ) AS sub
GROUP BY state
";

/// `aggStateSubQuery` around `getAggregateStateTable`, with the outer job row
/// referenced as `exhaustive_search_jobs.id` (the form the list query uses;
/// the correlated subquery is valid in the get projection as well).
pub const AGGREGATE_STATE_CORRELATED: &str = "
		SELECT
			CASE
				WHEN canceled > 0 THEN 'canceled'
				WHEN processing > 0 THEN 'processing'
				WHEN errored > 0 THEN 'processing'
				WHEN queued > 0 AND completed is null THEN 'queued'
				WHEN queued > 0 THEN 'processing'
				WHEN failed > 0 THEN 'failed'
				WHEN completed > 0 THEN 'completed'
				ELSE 'queued'
			END
		FROM (
			SELECT
				max( CASE WHEN state = 'failed' THEN count END) AS failed,
				max( CASE WHEN state = 'processing' THEN count END) AS processing,
				max( CASE WHEN state = 'completed' THEN count END) AS completed,
				max( CASE WHEN state = 'queued' THEN count END) AS queued,
				max( CASE WHEN state = 'canceled' THEN count END) AS canceled,
				max( CASE WHEN state = 'errored' THEN count END) AS errored
			FROM (
				SELECT state, COUNT(*) as count
				FROM
				  (
						(SELECT state
						 FROM exhaustive_search_jobs sj
						 WHERE sj.id = exhaustive_search_jobs.id)
				    UNION ALL
						(SELECT state
						 FROM exhaustive_search_repo_jobs rj
						 WHERE rj.search_job_id = exhaustive_search_jobs.id)
				    UNION ALL
						(SELECT rrj.state
						 FROM exhaustive_search_repo_revision_jobs rrj
						JOIN exhaustive_search_repo_jobs rj ON rrj.search_repo_job_id = rj.id
						WHERE rj.search_job_id = exhaustive_search_jobs.id)
				  ) AS sub
				GROUP BY state
			 ) AS state_histogram) AS transposed_state_histogram
";

/// `listSearchJobQuery`: the aggregate-state projection over the given
/// `WHERE` clause (`""` for no filter).
pub fn list_search_job_query(where_clause: &str) -> String {
    format!(
        "SELECT * FROM (SELECT {EXHAUSTIVE_SEARCH_JOB_COLUMNS}, ({AGGREGATE_STATE_CORRELATED}) as agg_state FROM exhaustive_search_jobs) as outer_query\n{where_clause}"
    )
}

fn scan_job(row: &PgRow, with_agg_state: bool) -> Result<ExhaustiveSearchJob, sqlx::Error> {
    Ok(ExhaustiveSearchJob {
        worker: scan_worker_job(row)?,
        id: i64::from(row.try_get::<i32, _>("id")?),
        initiator_id: row.try_get("initiator_id")?,
        query: row.try_get("query")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        agg_state: if with_agg_state {
            JobState::new(row.try_get::<String, _>("agg_state")?)
        } else {
            JobState::new("")
        },
        is_aggregated: row.try_get("is_aggregated")?,
    })
}

fn scan_worker_job(row: &PgRow) -> Result<WorkerJob, sqlx::Error> {
    Ok(WorkerJob {
        state: JobState::new(row.try_get::<String, _>("state")?),
        failure_message: row
            .try_get::<Option<String>, _>("failure_message")?
            .unwrap_or_default(),
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        process_after: row.try_get("process_after")?,
        num_resets: i64::from(row.try_get::<i32, _>("num_resets")?),
        num_failures: i64::from(row.try_get::<i32, _>("num_failures")?),
        worker_hostname: row.try_get("worker_hostname")?,
        cancel: row.try_get("cancel")?,
    })
}

fn scan_repo_job(row: &PgRow) -> Result<ExhaustiveSearchRepoJob, sqlx::Error> {
    Ok(ExhaustiveSearchRepoJob {
        worker: scan_worker_job(row)?,
        id: i64::from(row.try_get::<i32, _>("id")?),
        repo_id: row.try_get("repo_id")?,
        ref_spec: row.try_get("ref_spec")?,
        search_job_id: i64::from(row.try_get::<i32, _>("search_job_id")?),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn scan_rev_job(row: &PgRow) -> Result<ExhaustiveSearchRepoRevisionJob, sqlx::Error> {
    Ok(ExhaustiveSearchRepoRevisionJob {
        worker: scan_worker_job(row)?,
        id: i64::from(row.try_get::<i32, _>("id")?),
        search_repo_job_id: i64::from(row.try_get::<i32, _>("search_repo_job_id")?),
        revision: row.try_get("revision")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// `auth.CheckUserIsSiteAdmin` as a bool: internal actors are site admins,
/// missing/deleted users and any lookup error are not (the baseline's
/// `CheckSiteAdminOrSameUser` maps the error to
/// `ErrMustBeSiteAdminOrSameUser`; `List` maps it to "not an admin").
async fn actor_is_site_admin(pool: &PgPool, actor: Actor) -> bool {
    if actor.internal {
        return true;
    }
    if actor.uid == 0 {
        return false;
    }
    sqlx::query_scalar::<_, bool>(
        "SELECT site_admin FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(actor.uid)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(false)
}

/// `auth.CheckSiteAdminOrSameUser`: internal or same user pass; otherwise the
/// actor must be a site admin, and every other outcome (including a lookup
/// error) becomes `ErrMustBeSiteAdminOrSameUser`.
async fn check_site_admin_or_same_user(
    pool: &PgPool,
    actor: Actor,
    subject_user_id: i32,
) -> Result<(), Error> {
    if actor.internal || (actor.is_authenticated() && actor.uid == subject_user_id) {
        return Ok(());
    }
    if actor_is_site_admin(pool, actor).await {
        return Ok(());
    }
    Err(Error::MustBeSiteAdminOrSameUser)
}

/// `Store.CreateExhaustiveSearchJob`.
pub async fn create_exhaustive_search_job(
    pool: &PgPool,
    actor: Actor,
    query: &str,
    initiator_id: i32,
) -> Result<i64, Error> {
    if query.is_empty() {
        return Err(Error::MissingQuery);
    }
    if initiator_id <= 0 {
        return Err(Error::MissingInitiatorId);
    }
    // SECURITY: InitiatorID has to match the actor or can be overridden by
    // SiteAdmin.
    check_site_admin_or_same_user(pool, actor, initiator_id).await?;

    let id = sqlx::query_scalar::<_, i32>(CREATE_EXHAUSTIVE_SEARCH_JOB_SQL)
        .bind(query)
        .bind(initiator_id)
        .fetch_one(pool)
        .await?;
    Ok(i64::from(id))
}

/// `Store.UserHasAccess`: `Ok(None)` maps a missing job to
/// `failed to scan job with id …`; a zero initiator id is `no results`.
pub async fn user_has_access(pool: &PgPool, actor: Actor, id: i64) -> Result<(), Error> {
    let initiator_id = sqlx::query_scalar::<_, i32>(
        "SELECT initiator_id FROM exhaustive_search_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    let Some(initiator_id) = initiator_id else {
        return Err(Error::ScanJob {
            id,
            detail: "sql: no rows in result set".to_owned(),
        });
    };
    if initiator_id == 0 {
        return Err(Error::NoResults);
    }
    // SECURITY: only the initiator, internal or site admins may access a job.
    check_site_admin_or_same_user(pool, actor, initiator_id).await
}

/// `Store.GetExhaustiveSearchJob`.
pub async fn get_exhaustive_search_job(
    pool: &PgPool,
    actor: Actor,
    id: i64,
) -> Result<ExhaustiveSearchJob, Error> {
    let sql = list_search_job_query("WHERE id = $1");
    let row = sqlx::query(&sql).bind(id).fetch_optional(pool).await?;

    let Some(row) = row else {
        return Err(Error::ScanJob {
            id,
            detail: "sql: no rows in result set".to_owned(),
        });
    };
    let job = scan_job(&row, true)?;
    if job.id == 0 {
        return Err(Error::NoResults);
    }
    // SECURITY: only the initiator, internal or site admins may view a job.
    check_site_admin_or_same_user(pool, actor, job.initiator_id).await?;
    Ok(job)
}

/// `Store.CancelSearchJob`.
pub async fn cancel_search_job(
    pool: &PgPool,
    actor: Actor,
    id: i64,
    now: OffsetDateTime,
) -> Result<i64, Error> {
    // SECURITY: only someone with access to the job may cancel the job.
    user_has_access(pool, actor, id).await?;

    let total_canceled = sqlx::query_scalar::<_, i64>(CANCEL_SEARCH_JOB_SQL)
        .bind(now)
        .bind(id)
        .bind(now)
        .bind(now)
        .fetch_one(pool)
        .await?;
    Ok(total_canceled)
}

/// `store.ListArgs` without pagination (see the module docs).
#[derive(Debug, Clone, Default)]
pub struct ListArgs {
    pub query: String,
    pub states: Vec<String>,
    pub user_ids: Vec<i32>,
}

enum Bind {
    Text(String),
    Int(i32),
    Long(i64),
}

/// `Store.ListExhaustiveSearchJobs`.
pub async fn list_exhaustive_search_jobs(
    pool: &PgPool,
    actor: Actor,
    args: &ListArgs,
) -> Result<Vec<ExhaustiveSearchJob>, Error> {
    // SECURITY: Only authenticated users can list search jobs.
    if !actor.is_authenticated() {
        return Err(Error::ListRequiresAuthenticatedUser);
    }

    let mut conditions: Vec<String> = Vec::new();
    let mut binds: Vec<Bind> = Vec::new();

    if !args.query.is_empty() {
        binds.push(Bind::Text(format!("%{}%", args.query)));
        conditions.push(format!("query LIKE ${}", binds.len()));
    }

    if !args.states.is_empty() {
        let placeholders: Vec<String> = args
            .states
            .iter()
            .map(|state| {
                binds.push(Bind::Text(state.to_ascii_lowercase()));
                format!("${}", binds.len())
            })
            .collect();
        conditions.push(format!("agg_state in ({})", placeholders.join(",")));
    }

    // SECURITY: Site admins see any job and may filter based on args.UserIDs.
    // Other users only see their own jobs.
    if actor_is_site_admin(pool, actor).await {
        if !args.user_ids.is_empty() {
            let placeholders: Vec<String> = args
                .user_ids
                .iter()
                .map(|id| {
                    binds.push(Bind::Int(*id));
                    format!("${}", binds.len())
                })
                .collect();
            conditions.push(format!("initiator_id in ({})", placeholders.join(",")));
        }
    } else {
        if !args.user_ids.is_empty() {
            return Err(Error::CannotFilterByUserIds);
        }
        binds.push(Bind::Int(actor.uid));
        conditions.push(format!("initiator_id = ${}", binds.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join("\n AND "))
    };
    let sql = list_search_job_query(&where_clause);

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            Bind::Text(value) => query.bind(value),
            Bind::Int(value) => query.bind(value),
            Bind::Long(value) => query.bind(value),
        };
    }

    let rows = query.fetch_all(pool).await?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        jobs.push(scan_job(row, true)?);
    }
    Ok(jobs)
}

/// `Store.DeleteExhaustiveSearchJob`.
pub async fn delete_exhaustive_search_job(
    pool: &PgPool,
    actor: Actor,
    id: i64,
) -> Result<(), Error> {
    // SECURITY: only someone with access to the job may delete the job.
    user_has_access(pool, actor, id).await?;
    sqlx::query(DELETE_EXHAUSTIVE_SEARCH_JOB_SQL)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `Store.GetAggregateRepoRevState`: `state -> count` for the job cascade.
pub async fn get_aggregate_repo_rev_state(
    pool: &PgPool,
    actor: Actor,
    id: i64,
) -> Result<std::collections::BTreeMap<String, i64>, Error> {
    // SECURITY: only someone with access to the job may read its state.
    user_has_access(pool, actor, id).await?;

    let rows = sqlx::query(AGGREGATE_STATE_TABLE_SQL)
        .bind(id)
        .fetch_all(pool)
        .await?;
    let mut state = std::collections::BTreeMap::new();
    for row in &rows {
        state.insert(
            row.try_get::<String, _>("state")?,
            row.try_get::<i64, _>("count")?,
        );
    }
    Ok(state)
}

/// `store.GetJobLogsOpts`.
#[derive(Debug, Clone, Copy, Default)]
pub struct GetJobLogsOptions {
    pub from: i64,
    pub limit: i32,
}

/// `Store.GetJobLogs`.
pub async fn get_job_logs(
    pool: &PgPool,
    actor: Actor,
    id: i64,
    options: Option<GetJobLogsOptions>,
) -> Result<Vec<SearchJobLog>, Error> {
    // SECURITY: only someone with access to the job may access the logs.
    user_has_access(pool, actor, id).await?;

    let mut conditions = vec!["rj.search_job_id = $1".to_owned()];
    let mut binds: Vec<Bind> = vec![Bind::Long(id)];
    let mut limit = String::new();
    if let Some(options) = options {
        if options.from != 0 {
            binds.push(Bind::Long(options.from));
            conditions.push(format!("rjj.id >= ${}", binds.len()));
        }
        if options.limit != 0 {
            binds.push(Bind::Long(i64::from(options.limit)));
            limit = format!("\nLIMIT ${}", binds.len());
        }
    }
    let sql = format!(
        "{GET_JOB_LOGS_SQL}\nWHERE {}\nORDER BY id ASC{limit}",
        conditions.join(" AND ")
    );

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            Bind::Text(value) => query.bind(value),
            Bind::Int(value) => query.bind(value),
            Bind::Long(value) => query.bind(value),
        };
    }
    let rows = query.fetch_all(pool).await?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        jobs.push(SearchJobLog {
            id: i64::from(row.try_get::<i32, _>("id")?),
            repo_name: row.try_get("name")?,
            revision: row.try_get("revision")?,
            state: JobState::new(row.try_get::<String, _>("state")?),
            failure_message: row
                .try_get::<Option<String>, _>("failure_message")?
                .unwrap_or_default(),
            started_at: row.try_get("started_at")?,
            finished_at: row.try_get("finished_at")?,
        });
    }
    Ok(jobs)
}

/// `listSearchJobs`: search jobs that haven't been aggregated yet.
///
/// Deliberately unordered, like the baseline query.
pub async fn list_unaggregated_search_jobs(
    pool: &PgPool,
) -> Result<Vec<UnaggregatedSearchJob>, Error> {
    let rows = sqlx::query(LIST_UNAGGREGATED_SEARCH_JOBS_SQL)
        .fetch_all(pool)
        .await?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        jobs.push(UnaggregatedSearchJob {
            id: i64::from(row.try_get::<i32, _>("id")?),
            initiator_id: row.try_get("initiator_id")?,
        });
    }
    Ok(jobs)
}

/// `updateSearchJobStatus` inside the janitor's transaction.
pub async fn update_search_job_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
    state: &str,
) -> Result<(), Error> {
    sqlx::query(UPDATE_SEARCH_JOB_STATE_SQL)
        .bind(state)
        .bind(id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `deleteRepoJobs` inside the janitor's transaction.
pub async fn delete_repo_jobs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    search_job_id: i64,
) -> Result<(), Error> {
    sqlx::query(DELETE_SEARCH_JOB_REPO_JOBS_SQL)
        .bind(search_job_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `setJobAsAggregated` inside the janitor's transaction.
pub async fn set_search_job_aggregated_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
) -> Result<(), Error> {
    sqlx::query(SET_SEARCH_JOB_AGGREGATED_SQL)
        .bind(id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn validate_repo_job(repo_id: i32, search_job_id: i64, ref_spec: &str) -> Result<(), Error> {
    if search_job_id <= 0 {
        return Err(Error::MissingSearchJobId);
    }
    if repo_id <= 0 {
        return Err(Error::MissingRepoId);
    }
    if ref_spec.is_empty() {
        return Err(Error::MissingRefSpec);
    }
    Ok(())
}

fn validate_revision_job(search_repo_job_id: i64, revision: &str) -> Result<(), Error> {
    if search_repo_job_id <= 0 {
        return Err(Error::MissingSearchRepoJobId);
    }
    if revision.is_empty() {
        return Err(Error::MissingRevision);
    }
    Ok(())
}

/// `Store.CreateExhaustiveSearchRepoJob`.
pub async fn create_exhaustive_search_repo_job(
    pool: &PgPool,
    repo_id: i32,
    search_job_id: i64,
    ref_spec: &str,
) -> Result<i64, Error> {
    validate_repo_job(repo_id, search_job_id, ref_spec)?;
    let id = sqlx::query_scalar::<_, i32>(CREATE_REPO_SEARCH_JOB_SQL)
        .bind(repo_id)
        .bind(search_job_id)
        .bind(ref_spec)
        .fetch_one(pool)
        .await?;
    Ok(i64::from(id))
}

/// `Store.CreateExhaustiveSearchRepoJob` inside a caller-owned transaction
/// (`Store.Transact` in the worker handler).
pub async fn create_exhaustive_search_repo_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    repo_id: i32,
    search_job_id: i64,
    ref_spec: &str,
) -> Result<i64, Error> {
    validate_repo_job(repo_id, search_job_id, ref_spec)?;
    let id = sqlx::query_scalar::<_, i32>(CREATE_REPO_SEARCH_JOB_SQL)
        .bind(repo_id)
        .bind(search_job_id)
        .bind(ref_spec)
        .fetch_one(&mut **tx)
        .await?;
    Ok(i64::from(id))
}

/// `Store.CreateExhaustiveSearchRepoRevisionJob`.
pub async fn create_exhaustive_search_repo_revision_job(
    pool: &PgPool,
    search_repo_job_id: i64,
    revision: &str,
) -> Result<i64, Error> {
    validate_revision_job(search_repo_job_id, revision)?;
    let id = sqlx::query_scalar::<_, i32>(CREATE_REV_SEARCH_JOB_SQL)
        .bind(revision)
        .bind(search_repo_job_id)
        .fetch_one(pool)
        .await?;
    Ok(i64::from(id))
}

/// `Store.CreateExhaustiveSearchRepoRevisionJob` inside a caller-owned
/// transaction.
pub async fn create_exhaustive_search_repo_revision_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    search_repo_job_id: i64,
    revision: &str,
) -> Result<i64, Error> {
    validate_revision_job(search_repo_job_id, revision)?;
    let id = sqlx::query_scalar::<_, i32>(CREATE_REV_SEARCH_JOB_SQL)
        .bind(revision)
        .bind(search_repo_job_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(i64::from(id))
}

/// `Store.GetQueryRepoRev`.
pub struct QueryRepoRev {
    pub id: i64,
    pub query: String,
    pub repo_rev: RepositoryRevision,
    pub initiator_id: i32,
}

pub async fn get_query_repo_rev(
    pool: &PgPool,
    search_repo_job_id: i64,
    revision: &str,
) -> Result<QueryRepoRev, Error> {
    let row = sqlx::query(GET_QUERY_REPO_REV_SQL)
        .bind(search_repo_job_id)
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else {
        return Err(Error::NoRows);
    };
    Ok(QueryRepoRev {
        id: i64::from(row.try_get::<i32, _>("id")?),
        query: row.try_get("query")?,
        repo_rev: RepositoryRevision {
            repository_rev_specs: RepositoryRevSpecs {
                repository: row.try_get("repo_id")?,
                revision_specifiers: row.try_get("ref_spec")?,
            },
            revision: revision.to_owned(),
        },
        initiator_id: row.try_get("initiator_id")?,
    })
}

// ---------------------------------------------------------------------------
// Worker stores (`NewExhaustiveSearchJobWorkerStore` and friends)
// ---------------------------------------------------------------------------

/// `maxNumResets` shared by the three exhaustive worker store options.
pub const WORKER_MAX_NUM_RESETS: i32 = 5;
/// `maxNumRetries` shared by the three exhaustive worker store options.
pub const WORKER_MAX_NUM_RETRIES: i32 = 3;
/// `StalledMaxAge` (seconds) shared by the three options.
pub const WORKER_STALLED_MAX_AGE_SECS: i64 = 60;
/// `RetryAfter` (seconds) shared by the three options.
pub const WORKER_RETRY_AFTER_SECS: i64 = 5;
/// `defaultResetFailureMessage`.
pub const WORKER_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

const EXHAUSTIVE_SEARCH_JOBS_TABLE: &str = "exhaustive_search_jobs";
const EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE: &str = "exhaustive_search_repo_jobs";
const EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE: &str = "exhaustive_search_repo_revision_jobs";

/// The dequeue projection of `exhaustive_search_jobs`: the generic store
/// substitutes the columns updated by dequeue with the new values.
const JOB_DEQUEUE_COLUMNS: &str = "id, initiator_id, 'processing' AS state, query, \
     NULL AS failure_message, $1 AS started_at, NULL AS finished_at, process_after, \
     num_resets, num_failures, $3 AS worker_hostname, cancel, created_at, \
     updated_at, is_aggregated";

/// The dequeue projection of `exhaustive_search_repo_jobs`.
const REPO_DEQUEUE_COLUMNS: &str = "id, 'processing' AS state, repo_id, ref_spec, \
     search_job_id, NULL AS failure_message, $1 AS started_at, NULL AS finished_at, \
     process_after, num_resets, num_failures, $3 AS worker_hostname, cancel, \
     created_at, updated_at";

/// The dequeue projection of `exhaustive_search_repo_revision_jobs`.
const REV_DEQUEUE_COLUMNS: &str = "id, 'processing' AS state, search_repo_job_id, \
     revision, NULL AS failure_message, $1 AS started_at, NULL AS finished_at, \
     process_after, num_resets, num_failures, $3 AS worker_hostname, cancel, \
     created_at, updated_at";

/// `dbworkerstore.Dequeue` for the exhaustive tables. The order expression is
/// the baseline's `state = 'errored', updated_at DESC`, qualified with the
/// table name (the baseline uses `exhaustive_search_<table>.state` in the
/// order expression).
fn dequeue_query(table: &str, select_columns: &str) -> String {
    format!(
        r#"WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY {table}.state = 'errored', {table}.updated_at DESC) AS candidate_order
	FROM {table}
	WHERE
		(
			(
				state = 'queued' AND
				(process_after IS NULL OR process_after <= $1)
			) OR (
				$2 > 0 AND
				state = 'errored' AND
				$1 - finished_at > ($2 * '1 second'::interval)
			)
		)
	ORDER BY {table}.state = 'errored', {table}.updated_at DESC
	LIMIT 50
),
candidate AS (
	SELECT id FROM {table}
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF {table} SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE {table}
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $3
	WHERE id IN (SELECT id FROM candidate)
)
SELECT {select_columns}
FROM {table}
WHERE id IN (SELECT id FROM candidate)"#
    )
}

async fn worker_dequeue<R>(
    pool: &PgPool,
    table: &str,
    select_columns: &str,
    scan: fn(&PgRow) -> Result<R, sqlx::Error>,
    worker_hostname: &str,
    now: OffsetDateTime,
) -> Result<Option<R>, String> {
    let sql = dequeue_query(table, select_columns);
    let row = sqlx::query(&sql)
        .bind(now)
        .bind(WORKER_RETRY_AFTER_SECS)
        .bind(worker_hostname)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    match row {
        Some(row) => scan(&row).map(Some).map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

async fn worker_heartbeat(
    pool: &PgPool,
    table: &str,
    ids: &[i32],
    now: OffsetDateTime,
) -> Result<(Vec<i32>, Vec<i32>), String> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let sql = format!(
        r#"WITH alive_candidates AS (
	SELECT id
	FROM {table}
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE {table}
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel"#
    );
    let rows = sqlx::query(&sql)
        .bind(ids)
        .bind(now)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    let mut known = Vec::with_capacity(rows.len());
    let mut canceled = Vec::new();
    for row in &rows {
        let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
        let cancel: bool = row.try_get("cancel").map_err(|err| err.to_string())?;
        known.push(id);
        if cancel {
            canceled.push(id);
        }
    }
    known.sort_unstable();
    canceled.sort_unstable();
    Ok((known, canceled))
}

async fn worker_mark_complete(pool: &PgPool, table: &str, id: i32) -> Result<bool, String> {
    let sql = format!(
        "UPDATE {table}\nSET state = 'completed', finished_at = clock_timestamp()\n\
         WHERE id = $1 AND state = 'processing'"
    );
    let result = sqlx::query(&sql)
        .bind(id)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(result.rows_affected() > 0)
}

async fn worker_mark_errored(
    pool: &PgPool,
    table: &str,
    id: i32,
    failure_message: &str,
) -> Result<bool, String> {
    let sql = format!(
        "UPDATE {table}\n\
         SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= $3 THEN 'failed' ELSE 'errored' END,\n\
         \tfinished_at = clock_timestamp(),\n\
         \tfailure_message = $2,\n\
         \tnum_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END\n\
         WHERE id = $1 AND state = 'processing'"
    );
    let result = sqlx::query(&sql)
        .bind(id)
        .bind(failure_message)
        .bind(WORKER_MAX_NUM_RETRIES)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(result.rows_affected() > 0)
}

async fn worker_mark_failed(
    pool: &PgPool,
    table: &str,
    id: i32,
    failure_message: &str,
) -> Result<bool, String> {
    let sql = format!(
        "UPDATE {table}\n\
         SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,\n\
         \tfinished_at = clock_timestamp(),\n\
         \tfailure_message = $2,\n\
         \tnum_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END\n\
         WHERE id = $1 AND state = 'processing'"
    );
    let result = sqlx::query(&sql)
        .bind(id)
        .bind(failure_message)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(result.rows_affected() > 0)
}

fn worker_heartbeat_ages(
    now: OffsetDateTime,
    rows: Vec<PgRow>,
) -> Result<Vec<(i32, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
        let last_heartbeat: Option<OffsetDateTime> = row
            .try_get("last_heartbeat_at")
            .map_err(|err| err.to_string())?;
        let age = match last_heartbeat {
            Some(last) => Duration::try_from(now - last).unwrap_or(Duration::ZERO),
            None => Duration::ZERO,
        };
        ages.push((id, age));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

async fn worker_reset_stalled(
    pool: &PgPool,
    table: &str,
    now: OffsetDateTime,
) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
    let reset_sql = format!(
        r#"WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at"#
    );
    let reset_rows = sqlx::query(&reset_sql)
        .bind(now)
        .bind(WORKER_STALLED_MAX_AGE_SECS)
        .bind(WORKER_MAX_NUM_RESETS)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    let failed_sql = format!(
        r#"WITH stalled AS (
	SELECT id FROM {table}
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE {table}
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at"#
    );
    let failed_rows = sqlx::query(&failed_sql)
        .bind(now)
        .bind(WORKER_STALLED_MAX_AGE_SECS)
        .bind(WORKER_MAX_NUM_RESETS)
        .bind(WORKER_RESET_FAILURE_MESSAGE)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    Ok((
        worker_heartbeat_ages(now, reset_rows)?,
        worker_heartbeat_ages(now, failed_rows)?,
    ))
}

/// `NewExhaustiveSearchJobWorkerStore` over `exhaustive_search_jobs`.
#[derive(Clone)]
pub struct ExhaustiveSearchJobWorkerStore {
    pool: PgPool,
}

impl ExhaustiveSearchJobWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ExhaustiveSearchJob>, String> {
        worker_dequeue(
            &self.pool,
            EXHAUSTIVE_SEARCH_JOBS_TABLE,
            JOB_DEQUEUE_COLUMNS,
            scan_job_without_agg_state,
            worker_hostname,
            now,
        )
        .await
    }

    pub async fn heartbeat(
        &self,
        ids: &[i32],
        now: OffsetDateTime,
    ) -> Result<(Vec<i32>, Vec<i32>), String> {
        worker_heartbeat(&self.pool, EXHAUSTIVE_SEARCH_JOBS_TABLE, ids, now).await
    }

    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        worker_mark_complete(&self.pool, EXHAUSTIVE_SEARCH_JOBS_TABLE, id).await
    }

    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_errored(
            &self.pool,
            EXHAUSTIVE_SEARCH_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_failed(
            &self.pool,
            EXHAUSTIVE_SEARCH_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    /// `ResetStalled`: `(reset-to-queued, reset-to-failed)` with the age of
    /// the last heartbeat.
    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        worker_reset_stalled(&self.pool, EXHAUSTIVE_SEARCH_JOBS_TABLE, now).await
    }
}

fn scan_job_without_agg_state(row: &PgRow) -> Result<ExhaustiveSearchJob, sqlx::Error> {
    scan_job(row, false)
}

/// `NewRepoSearchJobWorkerStore` over `exhaustive_search_repo_jobs`.
#[derive(Clone)]
pub struct ExhaustiveSearchRepoJobWorkerStore {
    pool: PgPool,
}

impl ExhaustiveSearchRepoJobWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ExhaustiveSearchRepoJob>, String> {
        worker_dequeue(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE,
            REPO_DEQUEUE_COLUMNS,
            scan_repo_job,
            worker_hostname,
            now,
        )
        .await
    }

    pub async fn heartbeat(
        &self,
        ids: &[i32],
        now: OffsetDateTime,
    ) -> Result<(Vec<i32>, Vec<i32>), String> {
        worker_heartbeat(&self.pool, EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE, ids, now).await
    }

    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        worker_mark_complete(&self.pool, EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE, id).await
    }

    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_errored(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_failed(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        worker_reset_stalled(&self.pool, EXHAUSTIVE_SEARCH_REPO_JOBS_TABLE, now).await
    }
}

/// `NewRevSearchJobWorkerStore` over
/// `exhaustive_search_repo_revision_jobs`.
#[derive(Clone)]
pub struct ExhaustiveSearchRepoRevisionJobWorkerStore {
    pool: PgPool,
}

impl ExhaustiveSearchRepoRevisionJobWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ExhaustiveSearchRepoRevisionJob>, String> {
        worker_dequeue(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE,
            REV_DEQUEUE_COLUMNS,
            scan_rev_job,
            worker_hostname,
            now,
        )
        .await
    }

    pub async fn heartbeat(
        &self,
        ids: &[i32],
        now: OffsetDateTime,
    ) -> Result<(Vec<i32>, Vec<i32>), String> {
        worker_heartbeat(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE,
            ids,
            now,
        )
        .await
    }

    pub async fn mark_complete(&self, id: i32) -> Result<bool, String> {
        worker_mark_complete(&self.pool, EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE, id).await
    }

    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_errored(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, String> {
        worker_mark_failed(
            &self.pool,
            EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE,
            id,
            failure_message,
        )
        .await
    }

    pub async fn reset_stalled_at(
        &self,
        now: OffsetDateTime,
    ) -> Result<(Vec<(i32, Duration)>, Vec<(i32, Duration)>), String> {
        worker_reset_stalled(&self.pool, EXHAUSTIVE_SEARCH_REPO_REVISION_JOBS_TABLE, now).await
    }
}
