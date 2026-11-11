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

// ===========================================================================
// M9.9h — `changeset_jobs` bulk processor store
//
// Ported from `internal/batches/store/worker_bulk_operations.go` (the
// `dbworkerstore` configuration over `changeset_jobs`) and the processor's
// reads/writes in `internal/batches/store/{changesets,changeset_specs}.go`.
// Delimited so sibling Batch Changes slices can append their own sections
// without conflicts. Only existing tables and columns are touched.
// ===========================================================================

use sqlx::PgConnection;

/// `bulkOperationWorkerStoreOpts.TableName`.
pub const CHANGESET_JOBS_TABLE: &str = "changeset_jobs";
/// `bulkOperationWorkerStoreOpts.Name`.
pub const BULK_WORKER_STORE_NAME: &str = "batches_bulk_worker_store";
/// `bulkProcessorMaxNumRetries`.
pub const BULK_PROCESSOR_MAX_NUM_RETRIES: i32 = 10;
/// `bulkProcessorMaxNumResets`.
pub const BULK_PROCESSOR_MAX_NUM_RESETS: i32 = 60;
/// `bulkOperationWorkerStoreOpts.StalledMaxAge`.
pub const BULK_STALLED_MAX_AGE_SECONDS: i32 = 60;
/// `bulkOperationWorkerStoreOpts.RetryAfter`.
pub const BULK_RETRY_AFTER_SECONDS: i32 = 5;
/// `defaultResetFailureMessage` (`dbworker/store`).
pub const RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// A `changeset_jobs` row after the dequeue rewrites, as `scanChangesetJob`
/// reads it. `payload` is the raw JSONB; the job-type-specific binding happens
/// in the worker crate.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetJobRow {
    pub id: i64,
    pub bulk_group: String,
    pub user_id: i32,
    pub batch_change_id: i64,
    pub changeset_id: i64,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub state: String,
    pub failure_message: Option<String>,
    pub num_resets: i32,
    pub num_failures: i32,
}

/// A `changesets` row reduced to the processor's fields.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkChangesetRow {
    pub id: i64,
    pub repo_id: i32,
    pub current_spec_id: Option<i64>,
    pub reconciler_state: String,
    pub ui_publication_state: Option<String>,
    pub batch_change_ids: serde_json::Value,
}

/// A `repo` row reduced to what `sourcer.ForUser` / `GetRemoteRepo` need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkRepoRow {
    pub id: i32,
    pub name: String,
    pub external_service_type: String,
    pub external_service_id: String,
}

/// `changesetJobColumns` with the dequeue-updated expressions substituted
/// (`state`, `started_at`, `finished_at`, `failure_message`).
const CHANGESET_JOB_DEQUEUE_SELECT: &str = "\
changeset_jobs.id, changeset_jobs.bulk_group, changeset_jobs.user_id, \
changeset_jobs.batch_change_id, changeset_jobs.changeset_id, changeset_jobs.job_type, \
changeset_jobs.payload, 'processing' AS state, NULL::text AS failure_message, \
$1::timestamp AS started_at, NULL::timestamptz AS finished_at, \
changeset_jobs.process_after, changeset_jobs.num_resets, changeset_jobs.num_failures, \
changeset_jobs.created_at, changeset_jobs.updated_at";

/// `dequeueQuery` specialised for `changeset_jobs` (bigint ids, `RetryAfter`
/// 5s, `OrderByExpression` = errored last, most recently updated first).
const CHANGESET_JOB_DEQUEUE_QUERY: &str = "
WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY changeset_jobs.state = 'errored', changeset_jobs.updated_at DESC) AS order
	FROM changeset_jobs
	WHERE
		(
			(
				state = 'queued' AND
				(process_after IS NULL OR process_after <= $1)
			) OR (
				5 > 0 AND
				state = 'errored' AND
				$1 - finished_at > (5 * '1 second'::interval)
			)
		)
	ORDER BY changeset_jobs.state = 'errored', changeset_jobs.updated_at DESC
	LIMIT 50
),
candidate AS (
	SELECT
		changeset_jobs.id FROM changeset_jobs
	JOIN potential_candidates pc ON pc.candidate_id = changeset_jobs.id
	WHERE
		-- Recheck state.
		changeset_jobs.state IN ('queued', 'errored')
	ORDER BY pc.order
	FOR UPDATE OF changeset_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE
		changeset_jobs
	SET
		state = 'processing',
		started_at = $1::timestamp,
		last_heartbeat_at = $1::timestamp,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $2
	WHERE
		changeset_jobs.id IN (SELECT id FROM candidate)
)
SELECT
	{select}
FROM
	changeset_jobs
WHERE
	changeset_jobs.id IN (SELECT id FROM candidate)
";

/// `(*store[ChangesetJob]).Dequeue`.
pub async fn dequeue_changeset_job(
    pool: &PgPool,
    now: OffsetDateTime,
    worker_hostname: &str,
) -> Result<Option<ChangesetJobRow>, sqlx::Error> {
    let sql = CHANGESET_JOB_DEQUEUE_QUERY.replace("{select}", CHANGESET_JOB_DEQUEUE_SELECT);
    let row = sqlx::query(&sql)
        .bind(now)
        .bind(worker_hostname)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| ChangesetJobRow {
        id: row.get("id"),
        bulk_group: row.get("bulk_group"),
        user_id: row.get("user_id"),
        batch_change_id: row.get("batch_change_id"),
        changeset_id: row.get("changeset_id"),
        job_type: row.get("job_type"),
        payload: row.get("payload"),
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        num_resets: row.get("num_resets"),
        num_failures: row.get("num_failures"),
    }))
}

/// `(*store[ChangesetJob]).Heartbeat`: returns `(known, canceled)` record UIDs.
pub async fn heartbeat_changeset_jobs(
    pool: &PgPool,
    now: OffsetDateTime,
    ids: &[String],
) -> Result<(Vec<String>, Vec<String>), sqlx::Error> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let rows = sqlx::query(
        "
WITH alive_candidates AS (
	SELECT id FROM changeset_jobs
	WHERE id = ANY ($1::text[]::bigint[]) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE changeset_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
",
    )
    .bind(ids)
    .bind(now)
    .fetch_all(pool)
    .await?;

    let mut known = Vec::with_capacity(rows.len());
    let mut canceled = Vec::new();
    for row in rows {
        let id: i64 = row.get("id");
        let id = id.to_string();
        if row.get::<bool, _>("cancel") {
            canceled.push(id.clone());
        }
        known.push(id);
    }
    Ok((known, canceled))
}

/// `(*store[ChangesetJob]).MarkComplete`.
pub async fn mark_changeset_job_complete(pool: &PgPool, id: i64) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE changeset_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
RETURNING id",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// `(*store[ChangesetJob]).MarkErrored` with `MaxNumRetries = 10`.
pub async fn mark_changeset_job_errored(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let sql = format!(
        "UPDATE changeset_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= {BULK_PROCESSOR_MAX_NUM_RETRIES} THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id"
    );
    let row = sqlx::query(&sql)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[ChangesetJob]).MarkFailed`.
pub async fn mark_changeset_job_failed(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE changeset_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id",
    )
    .bind(id)
    .bind(failure_message)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// `(*store[ChangesetJob]).ResetStalled` with `StalledMaxAge = 60s` and
/// `MaxNumResets = 60`; returns `(reset-to-queued, reset-to-failed)` as
/// `(id, last_heartbeat_at)`.
pub async fn reset_stalled_changeset_jobs(
    pool: &PgPool,
    now: OffsetDateTime,
) -> Result<(Vec<(i64, OffsetDateTime)>, Vec<(i64, OffsetDateTime)>), sqlx::Error> {
    let reset_rows = sqlx::query(
        "WITH stalled AS (
	SELECT id FROM changeset_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets < $3
	FOR UPDATE SKIP LOCKED
)
UPDATE changeset_jobs
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id::bigint AS id, last_heartbeat_at",
    )
    .bind(now)
    .bind(BULK_STALLED_MAX_AGE_SECONDS)
    .bind(BULK_PROCESSOR_MAX_NUM_RESETS)
    .fetch_all(pool)
    .await?;

    let failed_rows = sqlx::query(
        "WITH stalled AS (
	SELECT id FROM changeset_jobs
	WHERE
		state = 'processing' AND
		$1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
		num_resets >= $3
	FOR UPDATE SKIP LOCKED
)
UPDATE changeset_jobs
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id::bigint AS id, last_heartbeat_at",
    )
    .bind(now)
    .bind(BULK_STALLED_MAX_AGE_SECONDS)
    .bind(BULK_PROCESSOR_MAX_NUM_RESETS)
    .bind(RESET_FAILURE_MESSAGE)
    .fetch_all(pool)
    .await?;

    let pairs = |rows: Vec<sqlx::postgres::PgRow>| {
        rows.into_iter()
            .map(|row| {
                let id: i64 = row.get("id");
                let heartbeat: Option<OffsetDateTime> = row.get("last_heartbeat_at");
                (id, heartbeat.unwrap_or(now))
            })
            .collect::<Vec<_>>()
    };
    Ok((pairs(reset_rows), pairs(failed_rows)))
}

/// `(*store).GetChangeset(opts{ID})` (joins `repo` so a deleted repo yields no
/// row, exactly like `repo.deleted_at IS NULL` in the baseline query).
pub async fn get_changeset_for_bulk_job(
    conn: &mut PgConnection,
    id: i64,
) -> Result<Option<BulkChangesetRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT
	changesets.id,
	changesets.repo_id,
	changesets.current_spec_id,
	changesets.reconciler_state,
	changesets.ui_publication_state::text AS ui_publication_state,
	changesets.batch_change_ids
FROM changesets
INNER JOIN repo ON repo.id = changesets.repo_id
WHERE repo.deleted_at IS NULL AND changesets.id = $1
LIMIT 1",
    )
    .bind(id)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| BulkChangesetRow {
        id: row.get("id"),
        repo_id: row.get("repo_id"),
        current_spec_id: row.get("current_spec_id"),
        reconciler_state: row.get("reconciler_state"),
        ui_publication_state: row.get("ui_publication_state"),
        batch_change_ids: row.get("batch_change_ids"),
    }))
}

/// `(*store).Repos().Get(repoID)` redux: `deleted_at`/`blocked` filtering.
pub async fn get_repo_for_bulk_job(
    conn: &mut PgConnection,
    repo_id: i32,
) -> Result<Option<BulkRepoRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id, name, external_service_type, external_service_id
FROM repo
WHERE id = $1 AND deleted_at IS NULL AND blocked IS NULL
LIMIT 1",
    )
    .bind(repo_id)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| BulkRepoRow {
        id: row.get("id"),
        name: row.get("name"),
        external_service_type: row.get("external_service_type"),
        external_service_id: row.get("external_service_id"),
    }))
}

/// `(*store).GetChangesetSpecByID` only for the `published` column:
/// `Ok(Some(None))` means the spec exists with `Published == nil`.
pub async fn get_changeset_spec_published(
    conn: &mut PgConnection,
    id: i64,
) -> Result<Option<Option<bool>>, sqlx::Error> {
    let row = sqlx::query("SELECT published FROM changeset_specs WHERE id = $1")
        .bind(id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| {
        let published: Option<String> = row.get("published");
        published.map(|_| true)
    }))
}

/// `(*store).UpdateChangesetBatchChanges`: only `batch_change_ids` and
/// `updated_at`.
pub async fn update_changeset_batch_changes(
    conn: &mut PgConnection,
    id: i64,
    batch_change_ids: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE changesets
SET updated_at = clock_timestamp(), batch_change_ids = $2
WHERE id = $1",
    )
    .bind(id)
    .bind(batch_change_ids)
    .execute(conn)
    .await?;
    Ok(())
}

/// `(*store).UpdateChangesetUiPublicationState`: only `ui_publication_state`
/// and `updated_at`.
pub async fn update_changeset_ui_publication_state(
    conn: &mut PgConnection,
    id: i64,
    state: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE changesets
SET updated_at = clock_timestamp(), ui_publication_state = $2::batch_changes_changeset_ui_publication_state
WHERE id = $1",
    )
    .bind(id)
    .bind(state)
    .execute(conn)
    .await?;
    Ok(())
}

/// `(*store).EnqueueChangeset(cs, resetState, currentState)`: returns `false`
/// when no row matched (the caller turns that into
/// `cannot re-enqueue changeset not in failed state`).
pub async fn enqueue_changeset(
    conn: &mut PgConnection,
    id: i64,
    reset_state: &str,
    current_state: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let sql = match current_state {
        Some(_) => {
            "UPDATE changesets
SET
	reconciler_state = $2,
	num_resets = 0,
	num_failures = 0,
	previous_failure_message = changesets.failure_message,
	failure_message = NULL,
	syncer_error = NULL,
	updated_at = clock_timestamp()
WHERE id = $1 AND reconciler_state = $3
RETURNING changesets.id"
        }
        None => {
            "UPDATE changesets
SET
	reconciler_state = $2,
	num_resets = 0,
	num_failures = 0,
	previous_failure_message = changesets.failure_message,
	failure_message = NULL,
	syncer_error = NULL,
	updated_at = clock_timestamp()
WHERE id = $1
RETURNING changesets.id"
        }
    };
    let mut query = sqlx::query(sql).bind(id).bind(reset_state);
    if let Some(state) = current_state {
        query = query.bind(state);
    }
    Ok(query.fetch_optional(conn).await?.is_some())
}
