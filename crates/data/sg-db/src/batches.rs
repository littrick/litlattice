//! Read-only queries for the Batch Changes changeset syncer
//! (`internal/batches/store`):
//!
//! - `ListCodeHosts` (`store/codehost.go`), used to decide which code hosts
//!   get a `changesetSyncer`;
//! - `ListChangesetSyncData` (`store/changesets.go`), used both to compute a
//!   code host's schedule and to route high-priority enqueues;
//! - `ListChangesets` with only `RepoIDs` set (`store/changesets.go`), used by
//!   `EnqueueChangesetSyncsForRepos`;
//! - `GetChangeset` with the syncer's preconditions (`store/changesets.go`),
//!   used by `SyncChangeset`;
//! - `EnqueueNextScheduledChangeset` (`store/changesets.go`), the
//!   `batches-scheduler` job's `scheduled` → `queued` transition.
//!
//! Migrations stay owned by the Go `migrator`; these queries never change the
//! schema. The only write is the changeset state transition above, which is an
//! existing M6 storage contract (`changesets.reconciler_state`).

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// The external service types Batch Changes supports
/// (`batches/types.GetSupportedExternalServices`). The values are
/// `extsvc.TypeToKind`→`AsType()` spellings, as stored in
/// `repo.external_service_type`.
pub const SUPPORTED_EXTERNAL_SERVICE_TYPES: [&str; 6] = [
    "github",
    "bitbucketServer",
    "gitlab",
    "bitbucketCloud",
    "azuredevops",
    "gerrit",
];

/// `extsvc.TypePerforce`, only supported when
/// `experimentalFeatures.batchChanges.enablePerforce` is set.
pub const PERFORCE_EXTERNAL_SERVICE_TYPE: &str = "perforce";

/// `batches/types.CodeHost`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeHost {
    pub external_service_type: String,
    pub external_service_id: String,
    pub requires_ssh: bool,
    pub has_webhooks: bool,
}

impl CodeHost {
    /// `(*CodeHost).IsSupported`: the store already filters, but the registry
    /// checks again before creating a syncer.
    pub fn is_supported(&self, include_perforce: bool) -> bool {
        SUPPORTED_EXTERNAL_SERVICE_TYPES.contains(&self.external_service_type.as_str())
            || (include_perforce && self.external_service_type == PERFORCE_EXTERNAL_SERVICE_TYPE)
    }
}

/// `batches/types.ChangesetSyncData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetSyncData {
    pub changeset_id: i64,
    pub updated_at: OffsetDateTime,
    pub latest_event: Option<OffsetDateTime>,
    pub external_updated_at: Option<OffsetDateTime>,
    pub repo_external_service_id: String,
}

/// The subset of `batches/store.Store` the syncer uses.
#[derive(Clone)]
pub struct BatchesStore {
    pool: PgPool,
}

impl BatchesStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `ListCodeHosts`. `repo_ids` and `only_without_webhooks` correspond to
    /// `ListCodeHostsOpts`; the syncer always passes an empty list and
    /// `false`.
    pub async fn list_code_hosts(
        &self,
        repo_ids: &[i32],
        only_without_webhooks: bool,
        include_perforce: bool,
    ) -> Result<Vec<CodeHost>, sqlx::Error> {
        let mut types: Vec<&str> = SUPPORTED_EXTERNAL_SERVICE_TYPES.to_vec();
        if include_perforce {
            types.push(PERFORCE_EXTERNAL_SERVICE_TYPE);
        }

        // `listCodeHostsQueryFmtstr`, with the supported-type list bound as a
        // `text[]` instead of the baseline's literal `IN (...)` list.
        let mut query = String::from(
            r#"
WITH
    esr_with_ssh AS (
        SELECT
            *
        FROM
            external_service_repos
        WHERE
            clone_url ILIKE $1 OR
            clone_url !~* '[[:alpha:]]+://'
    ),
    esr_with_webhooks AS (
        SELECT
            *
        FROM
            external_service_repos
        WHERE
            external_service_id IN (
                SELECT
                    id
                FROM
                    external_services
                WHERE
                    has_webhooks IS NULL OR
                    has_webhooks = TRUE
            )
    ),
    aggregated_repos AS (
        SELECT
            repo.external_service_type,
            repo.external_service_id,
            COUNT(esr_with_ssh.external_service_id) AS ssh_required_count,
            COUNT(esr_with_webhooks.external_service_id) AS has_webhooks_count
        FROM
            repo
        LEFT JOIN
            esr_with_ssh
        ON
            repo.id = esr_with_ssh.repo_id
        LEFT JOIN
            esr_with_webhooks
        ON
            repo.id = esr_with_webhooks.repo_id
        WHERE
            repo.deleted_at IS NULL
            AND repo.external_service_type = ANY($2)
"#,
        );
        let next = 3;
        if !repo_ids.is_empty() {
            query.push_str(&format!("            AND repo.id = ANY(${next})\n"));
        }
        query.push_str(
            r#"        GROUP BY
            repo.external_service_type, repo.external_service_id
        ORDER BY
            repo.external_service_type ASC, repo.external_service_id ASC
    )
SELECT
    external_service_type,
    external_service_id,
    ssh_required_count > 0 AS ssh_required,
    has_webhooks_count > 0 AS has_webhooks
FROM
    aggregated_repos
WHERE
"#,
        );
        if only_without_webhooks {
            query.push_str(
                "has_webhooks_count = 0 AND external_service_id NOT IN \
                 (SELECT DISTINCT(code_host_urn) FROM webhooks)",
            );
        } else {
            query.push_str("TRUE");
        }

        let mut statement = sqlx::query(&query).bind("ssh://%").bind(types);
        if !repo_ids.is_empty() {
            statement = statement.bind(repo_ids.to_vec());
        }

        let rows = statement.fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                Ok(CodeHost {
                    external_service_type: row.try_get("external_service_type")?,
                    external_service_id: row.try_get("external_service_id")?,
                    requires_ssh: row.try_get("ssh_required")?,
                    has_webhooks: row.try_get("has_webhooks")?,
                })
            })
            .collect()
    }

    /// `ListChangesetSyncData`. An empty `changeset_ids` means "all".
    pub async fn list_changeset_sync_data(
        &self,
        changeset_ids: &[i64],
        external_service_id: &str,
    ) -> Result<Vec<ChangesetSyncData>, sqlx::Error> {
        let mut query = String::from(
            r#"
SELECT changesets.id,
	changesets.updated_at,
	max(ce.updated_at) AS latest_event,
	changesets.external_updated_at,
	r.external_service_id
FROM changesets
LEFT JOIN changeset_events ce ON changesets.id = ce.changeset_id
JOIN batch_changes ON changesets.batch_change_ids ? batch_changes.id::TEXT
JOIN repo r ON changesets.repo_id = r.id
WHERE batch_changes.closed_at IS NULL
 AND r.deleted_at IS NULL
 AND changesets.publication_state = $1
 AND changesets.reconciler_state = $2
"#,
        );
        let mut next = 3;
        if !changeset_ids.is_empty() {
            query.push_str(&format!(" AND changesets.id = ANY(${next})\n"));
            next += 1;
        }
        if !external_service_id.is_empty() {
            query.push_str(&format!(" AND r.external_service_id = ${next}\n"));
        }
        query.push_str("GROUP BY changesets.id, r.id\nORDER BY changesets.id ASC\n");

        let mut statement = sqlx::query(&query).bind("PUBLISHED").bind("completed");
        if !changeset_ids.is_empty() {
            statement = statement.bind(changeset_ids.to_vec());
        }
        if !external_service_id.is_empty() {
            statement = statement.bind(external_service_id);
        }

        let rows = statement.fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                Ok(ChangesetSyncData {
                    changeset_id: row.try_get("id")?,
                    updated_at: row.try_get("updated_at")?,
                    latest_event: row.try_get("latest_event")?,
                    external_updated_at: row.try_get("external_updated_at")?,
                    repo_external_service_id: row.try_get("external_service_id")?,
                })
            })
            .collect()
    }

    /// `ListChangesets` reduced to `IDs` for `RepoIDs`: the registry runs
    /// under an internal actor, so no authz predicates apply, and the
    /// baseline passes no limit.
    pub async fn list_changeset_ids_for_repos(
        &self,
        repo_ids: &[i32],
    ) -> Result<Vec<i64>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
SELECT changesets.id FROM changesets
INNER JOIN repo ON repo.id = changesets.repo_id
WHERE changesets.id >= 0
 AND repo.deleted_at IS NULL
 AND repo.id = ANY($1)
ORDER BY id ASC
"#,
        )
        .bind(repo_ids.to_vec())
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(|row| row.try_get("id")).collect()
    }

    /// `GetChangeset` with the syncer's preconditions
    /// (`ReconcilerState: completed`, `PublicationState: PUBLISHED`). Returns
    /// the changeset's repo id, or `None` for `store.ErrNoResults`.
    pub async fn get_changeset_for_sync(&self, id: i64) -> Result<Option<i32>, sqlx::Error> {
        let row = sqlx::query(
            r#"
SELECT changesets.repo_id FROM changesets
INNER JOIN repo ON repo.id = changesets.repo_id
WHERE repo.deleted_at IS NULL
 AND changesets.id = $1
 AND changesets.reconciler_state = $2
 AND changesets.publication_state = $3
LIMIT 1
"#,
        )
        .bind(id)
        .bind("completed")
        .bind("PUBLISHED")
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| row.try_get("repo_id")).transpose()
    }

    /// `Store.EnqueueNextScheduledChangeset`
    /// (`store/changesets.go`): move the oldest `scheduled` changeset to
    /// `queued` and return its id, or `None` for `store.ErrNoResults`.
    ///
    /// The baseline CTE selects by `updated_at ASC LIMIT 1` and updates the
    /// matching row in a single statement; this is the same statement with
    /// `RETURNING changesets.id` (the baseline scans the full row and treats
    /// `ID == 0` as no results).
    pub async fn enqueue_next_scheduled_changeset(&self) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar(
            r#"
WITH c AS (
    SELECT *
    FROM changesets
    WHERE reconciler_state = $1
    ORDER BY updated_at ASC
    LIMIT 1
)
UPDATE changesets
SET reconciler_state = $2
FROM c
WHERE c.id = changesets.id
RETURNING changesets.id
"#,
        )
        .bind("scheduled")
        .bind("queued")
        .fetch_optional(&self.pool)
        .await
    }
}

// ---------------------------------------------------------------------------
// M9.9i: `batches-workspace-resolver` worker store
// (`internal/batches/store/worker_batch_spec_resolution.go`).
//
// Delimited from the changeset-syncer queries above to keep sibling slices'
// edits independent. Only the existing `batch_spec_resolution_jobs` columns
// are touched; migrations stay owned by the Go `migrator`.
// ---------------------------------------------------------------------------

/// `batchSpecResolutionWorkerOpts` knobs.
pub const BATCH_SPEC_RESOLUTION_TABLE: &str = "batch_spec_resolution_jobs";
/// `Options.RetryAfter`.
pub const BATCH_SPEC_RESOLUTION_RETRY_AFTER_SECONDS: i32 = 5;
/// `Options.MaxNumRetries` (0: the first error is terminal).
pub const BATCH_SPEC_RESOLUTION_MAX_NUM_RETRIES: i32 = 0;

/// A `batch_spec_resolution_jobs` row as the worker store returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecResolutionJobRow {
    pub id: i64,
    pub batch_spec_id: i32,
    pub initiator_id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub worker_hostname: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// The generic `dbworker/store` dequeue statement with this store's
/// `OrderByExpression` (`state = 'errored', updated_at DESC`) and
/// `RetryAfter`/`MaxNumRetries` literals. `{select}` is replaced by
/// [`DEQUEUE_SELECT`].
const DEQUEUE_QUERY: &str = "
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY state = 'errored', updated_at DESC) AS order
	FROM batch_spec_resolution_jobs
	WHERE
		(
			(
				state = 'queued' AND
				(process_after IS NULL OR process_after <= $1)
			) OR (
				0 > 0 AND
				state = 'errored' AND
				$1 - finished_at > (5 * '1 second'::interval)
			)
		)

	ORDER BY state = 'errored', updated_at DESC
	LIMIT 50
),
candidate AS (
	SELECT
		id FROM batch_spec_resolution_jobs
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE
		-- Recheck state.
		state IN ('queued', 'errored')
	ORDER BY pc.order
	FOR UPDATE OF batch_spec_resolution_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE
		batch_spec_resolution_jobs
	SET
		state='processing', started_at=$1::timestamp, last_heartbeat_at=$1::timestamp, finished_at=NULL, failure_message=NULL, execution_logs=NULL, worker_hostname=$2
	WHERE
		id IN (SELECT id FROM candidate)
)
SELECT
	{select}
FROM
	batch_spec_resolution_jobs
WHERE
	id IN (SELECT id FROM candidate)
";

/// `batchSpecResolutionJobColums` with the dequeue-updated expressions
/// substituted; aliases keep the row decodable by name.
const DEQUEUE_SELECT: &str = "batch_spec_resolution_jobs.id, batch_spec_resolution_jobs.batch_spec_id, batch_spec_resolution_jobs.initiator_id, 'processing' AS state, NULL::text AS failure_message, $1 AS started_at, NULL::timestamptz AS finished_at, batch_spec_resolution_jobs.process_after, batch_spec_resolution_jobs.num_resets, batch_spec_resolution_jobs.num_failures, $2 AS worker_hostname, batch_spec_resolution_jobs.created_at, batch_spec_resolution_jobs.updated_at";

const HEARTBEAT_QUERY: &str = "
WITH alive_candidates AS (
	SELECT
		id
	FROM
		batch_spec_resolution_jobs
	WHERE
		id = ANY ($1::text[]::int[]) AND state = 'processing'
	ORDER BY
		id ASC
	FOR UPDATE
)
UPDATE
	batch_spec_resolution_jobs
SET
	last_heartbeat_at = $2
WHERE
	id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
";

const MARK_COMPLETE_QUERY: &str = "
UPDATE batch_spec_resolution_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
RETURNING id
";

const MARK_ERRORED_QUERY: &str = "
UPDATE batch_spec_resolution_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= 0 THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
";

const MARK_FAILED_QUERY: &str = "
UPDATE batch_spec_resolution_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
";

fn nullable<'r, T>(row: &'r sqlx::postgres::PgRow, column: &str) -> Option<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<Option<T>, _>(column).ok().flatten()
}

fn scan_resolution_job(row: sqlx::postgres::PgRow) -> BatchSpecResolutionJobRow {
    BatchSpecResolutionJobRow {
        id: row.get("id"),
        batch_spec_id: row.get("batch_spec_id"),
        initiator_id: row.get("initiator_id"),
        state: row.get("state"),
        failure_message: nullable(&row, "failure_message"),
        started_at: nullable(&row, "started_at"),
        finished_at: nullable(&row, "finished_at"),
        process_after: nullable(&row, "process_after"),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
        worker_hostname: row.get("worker_hostname"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// `(*store[BatchSpecResolutionJob]).Dequeue`.
pub async fn dequeue_batch_spec_resolution_job(
    pool: &PgPool,
    now: OffsetDateTime,
    worker_hostname: &str,
) -> Result<Option<BatchSpecResolutionJobRow>, sqlx::Error> {
    let sql = DEQUEUE_QUERY.replace("{select}", DEQUEUE_SELECT);
    let rows = sqlx::query(&sql)
        .bind(now)
        .bind(worker_hostname)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().next().map(scan_resolution_job))
}

/// `(*store[BatchSpecResolutionJob]).Heartbeat`: `(known, canceled)` ids.
pub async fn heartbeat_batch_spec_resolution_jobs(
    pool: &PgPool,
    now: OffsetDateTime,
    ids: &[String],
) -> Result<(Vec<i64>, Vec<i64>), sqlx::Error> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let rows = sqlx::query(HEARTBEAT_QUERY)
        .bind(ids)
        .bind(now)
        .fetch_all(pool)
        .await?;
    let mut known = Vec::with_capacity(rows.len());
    let mut canceled = Vec::new();
    for row in rows {
        let id: i64 = row.get("id");
        known.push(id);
        if row.get::<bool, _>("cancel") {
            canceled.push(id);
        }
    }
    Ok((known, canceled))
}

/// `(*store[BatchSpecResolutionJob]).MarkComplete`.
pub async fn mark_batch_spec_resolution_job_complete(
    pool: &PgPool,
    id: i64,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(MARK_COMPLETE_QUERY)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[BatchSpecResolutionJob]).MarkErrored`.
pub async fn mark_batch_spec_resolution_job_errored(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(MARK_ERRORED_QUERY)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[BatchSpecResolutionJob]).MarkFailed`.
pub async fn mark_batch_spec_resolution_job_failed(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(MARK_FAILED_QUERY)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

// ---------------------------------------------------------------------------
// M9.9i: batch spec read + resolution persistence (handler side).
// ---------------------------------------------------------------------------

/// The subset of `batch_specs` the resolution handler reads.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecRow {
    pub id: i64,
    pub rand_id: String,
    pub raw_spec: String,
    pub spec: serde_json::Value,
    pub namespace_user_id: Option<i32>,
    pub namespace_org_id: Option<i32>,
    pub user_id: Option<i32>,
    pub allow_unsupported: bool,
    pub allow_ignored: bool,
    pub no_cache: bool,
}

/// `(*store.Store).GetBatchSpec` with `GetBatchSpecOpts{ID}`.
pub async fn get_batch_spec(pool: &PgPool, id: i64) -> Result<Option<BatchSpecRow>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT id, rand_id, raw_spec, spec, namespace_user_id, namespace_org_id, user_id,
       allow_unsupported, allow_ignored, no_cache
FROM batch_specs
WHERE id = $1
"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| BatchSpecRow {
        id: row.get("id"),
        rand_id: row.get("rand_id"),
        raw_spec: row.get("raw_spec"),
        spec: row.get("spec"),
        namespace_user_id: row.get("namespace_user_id"),
        namespace_org_id: row.get("namespace_org_id"),
        user_id: row.get("user_id"),
        allow_unsupported: row.get("allow_unsupported"),
        allow_ignored: row.get("allow_ignored"),
        no_cache: row.get("no_cache"),
    }))
}

/// One `batch_spec_workspaces` insert row (`btypes.BatchSpecWorkspace`).
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecWorkspaceInsert {
    pub batch_spec_id: i64,
    pub changeset_spec_ids: Vec<i64>,
    pub repo_id: i32,
    pub branch: String,
    pub commit: String,
    pub path: String,
    pub file_matches: Vec<String>,
    pub only_fetch_workspace: bool,
    pub ignored: bool,
    pub unsupported: bool,
    pub skipped: bool,
    pub cached_result_found: bool,
    pub step_cache_results: serde_json::Value,
}

/// `(*store.Store).CreateBatchSpecWorkspace`.
pub async fn create_batch_spec_workspaces(
    pool: &PgPool,
    workspaces: &[BatchSpecWorkspaceInsert],
) -> Result<(), sqlx::Error> {
    for workspace in workspaces {
        sqlx::query(
            r#"
INSERT INTO batch_spec_workspaces
    (batch_spec_id, changeset_spec_ids, repo_id, branch, commit, path, file_matches,
     only_fetch_workspace, ignored, unsupported, skipped, cached_result_found,
     step_cache_results)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
"#,
        )
        .bind(workspace.batch_spec_id as i32)
        .bind(sqlx::types::Json(&workspace.changeset_spec_ids))
        .bind(workspace.repo_id)
        .bind(&workspace.branch)
        .bind(&workspace.commit)
        .bind(&workspace.path)
        .bind(&workspace.file_matches)
        .bind(workspace.only_fetch_workspace)
        .bind(workspace.ignored)
        .bind(workspace.unsupported)
        .bind(workspace.skipped)
        .bind(workspace.cached_result_found)
        .bind(sqlx::types::Json(&workspace.step_cache_results))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// One imported `changeset_specs` insert row (`ChangesetSpecTypeExisting`).
#[derive(Debug, Clone, PartialEq)]
pub struct ImportChangesetSpecInsert {
    pub rand_id: String,
    pub batch_spec_id: i64,
    pub repo_id: i32,
    pub user_id: Option<i32>,
    pub external_id: String,
}

/// `(*store.Store).CreateChangesetSpec` for `importChangesets` entries
/// (`type = 'existing'`).
pub async fn create_import_changeset_specs(
    pool: &PgPool,
    specs: &[ImportChangesetSpecInsert],
) -> Result<(), sqlx::Error> {
    for spec in specs {
        sqlx::query(
            r#"
INSERT INTO changeset_specs
    (rand_id, batch_spec_id, repo_id, user_id, external_id, type)
VALUES ($1, $2, $3, $4, $5, 'existing')
"#,
        )
        .bind(&spec.rand_id)
        .bind(spec.batch_spec_id)
        .bind(spec.repo_id)
        .bind(spec.user_id)
        .bind(&spec.external_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}
