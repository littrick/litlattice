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

// ===========================================================================
// M9.9g: `batches-reconciler` dbworker store + reconciler reads/writes.
//
// Everything below is new for the `batches-reconciler` job
// (`internal/batches/store/worker_reconciler.go`,
// `internal/workerutil/dbworker/store/store.go` and
// `internal/batches/reconciler/*`). It stays in its own delimited block so the
// sibling M9.9h/M9.9i slices can merge without touching the syncer code above.
// ===========================================================================

/// `reconcilerWorkerStoreOpts.Name`.
pub const RECONCILER_STORE_NAME: &str = "batches_reconciler_worker_store";
/// `reconcilerWorkerStoreOpts.TableName`.
pub const RECONCILER_TABLE: &str = "changesets";
/// `reconcilerWorkerStoreOpts.ViewName`.
pub const RECONCILER_VIEW: &str = "reconciler_changesets changesets";
/// `reconcilerWorkerStoreOpts.AlternateColumnNames["state"]`.
pub const RECONCILER_STATE_COLUMN: &str = "reconciler_state";
/// `reconcilerWorkerStoreOpts.StalledMaxAge`.
pub const RECONCILER_STALLED_MAX_AGE_SECONDS: i32 = 60;
/// `reconcilerMaxNumResets`.
pub const RECONCILER_MAX_NUM_RESETS: i32 = 10;
/// `reconcilerWorkerStoreOpts.RetryAfter`.
pub const RECONCILER_RETRY_AFTER_SECONDS: i32 = 5;
/// `reconcilerMaxNumRetries`.
pub const RECONCILER_MAX_NUM_RETRIES: i32 = 10;
/// `reconcilerWorkerStoreOpts.OrderByExpression` as SQL.
pub const RECONCILER_ORDER_BY: &str =
    "changesets.reconciler_state = 'errored', changesets.updated_at DESC";

/// `workerutil.WorkerOptions.Name` (`workers.NewReconcilerWorker`).
pub const RECONCILER_WORKER_NAME: &str = "batches_reconciler_worker";
/// `workerutil.WorkerOptions.Description`.
pub const RECONCILER_WORKER_DESCRIPTION: &str =
    "changeset reconciler that publishes, modifies and closes changesets on the code host";

/// A `changesets` row selected for the reconciler after the dequeue update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerChangesetRow {
    pub id: i64,
    pub repo_id: i32,
    pub external_service_type: String,
    pub external_branch: Option<String>,
    pub external_state: Option<String>,
    pub publication_state: String,
    pub ui_publication_state: Option<String>,
    pub owned_by_batch_change_id: Option<i64>,
    pub current_spec_id: Option<i64>,
    pub previous_spec_id: Option<i64>,
    pub closing: bool,
    pub detached_at: Option<OffsetDateTime>,
    pub batch_change_ids: serde_json::Value,
    pub failure_message: Option<String>,
    pub previous_failure_message: Option<String>,
}

/// A `changeset_specs` row as `GetChangesetSpecByID` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetSpecRow {
    pub id: i64,
    pub external_id: String,
    pub base_rev: String,
    pub base_ref: String,
    pub head_ref: String,
    pub title: String,
    pub body: String,
    /// Raw `changeset_specs.published` (`"true"`, `"false"`, `"\"draft\""`, NULL).
    pub published: Option<String>,
    pub diff: Vec<u8>,
    pub commit_message: String,
    pub commit_author_name: String,
    pub commit_author_email: String,
}

const RECONCILER_DEQUEUE_QUERY: &str = "
WITH potential_candidates AS (
    SELECT
        changesets.id AS candidate_id,
        ROW_NUMBER() OVER (ORDER BY changesets.reconciler_state = 'errored', changesets.updated_at DESC) AS order
    FROM changesets
    WHERE
        (
            (
                changesets.reconciler_state = 'queued' AND
                (changesets.process_after IS NULL OR changesets.process_after <= $1)
            ) OR (
                5 > 0 AND
                changesets.reconciler_state = 'errored' AND
                $1 - changesets.finished_at > (5 * '1 second'::interval)
            )
        )
    ORDER BY changesets.reconciler_state = 'errored', changesets.updated_at DESC
    LIMIT 50
),
candidate AS (
    SELECT changesets.id FROM changesets
    JOIN potential_candidates pc ON pc.candidate_id = changesets.id
    WHERE changesets.reconciler_state IN ('queued', 'errored')
    ORDER BY pc.order
    FOR UPDATE OF changesets SKIP LOCKED
    LIMIT 1
),
updated_record AS (
    UPDATE changesets
    SET
        reconciler_state = 'processing',
        started_at = $1::timestamp,
        last_heartbeat_at = $1::timestamp,
        finished_at = NULL,
        failure_message = NULL,
        worker_hostname = $2
    WHERE changesets.id IN (SELECT id FROM candidate)
)
SELECT
    changesets.id,
    changesets.repo_id,
    changesets.external_service_type,
    changesets.external_branch,
    changesets.external_state,
    changesets.publication_state,
    changesets.ui_publication_state,
    changesets.owned_by_batch_change_id,
    changesets.current_spec_id,
    changesets.previous_spec_id,
    changesets.closing,
    changesets.detached_at,
    changesets.batch_change_ids,
    changesets.failure_message,
    changesets.previous_failure_message
FROM changesets
WHERE changesets.id IN (SELECT id FROM candidate)
";

/// `(*store[changeset]).Dequeue`: returns `None` when no row is eligible.
pub async fn dequeue_reconciler_changeset(
    pool: &PgPool,
    now: OffsetDateTime,
    worker_hostname: &str,
) -> Result<Option<ReconcilerChangesetRow>, sqlx::Error> {
    let row = sqlx::query(RECONCILER_DEQUEUE_QUERY)
        .bind(now)
        .bind(worker_hostname)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| ReconcilerChangesetRow {
        id: row.get("id"),
        repo_id: row.get("repo_id"),
        external_service_type: row.get("external_service_type"),
        external_branch: row.get("external_branch"),
        external_state: row.get("external_state"),
        publication_state: row.get("publication_state"),
        ui_publication_state: row.get("ui_publication_state"),
        owned_by_batch_change_id: row.get("owned_by_batch_change_id"),
        current_spec_id: row.get("current_spec_id"),
        previous_spec_id: row.get("previous_spec_id"),
        closing: row.get("closing"),
        detached_at: row.get("detached_at"),
        batch_change_ids: row.get("batch_change_ids"),
        failure_message: row.get("failure_message"),
        previous_failure_message: row.get("previous_failure_message"),
    }))
}

const RECONCILER_HEARTBEAT_QUERY: &str = "
WITH alive_candidates AS (
    SELECT id FROM changesets
    WHERE id = ANY ($1::text[]::bigint[]) AND reconciler_state = 'processing'
    ORDER BY id ASC
    FOR UPDATE
)
UPDATE changesets
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
";

/// `(*store[changeset]).Heartbeat`: `(known, canceled)` ids.
pub async fn heartbeat_reconciler_changesets(
    pool: &PgPool,
    now: OffsetDateTime,
    ids: &[String],
) -> Result<(Vec<i64>, Vec<i64>), sqlx::Error> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let rows = sqlx::query(RECONCILER_HEARTBEAT_QUERY)
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

const RECONCILER_MARK_COMPLETE_QUERY: &str = "
UPDATE changesets
SET reconciler_state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND reconciler_state = 'processing'
RETURNING id
";

const RECONCILER_MARK_ERRORED_QUERY: &str = "
UPDATE changesets
SET reconciler_state = CASE
        WHEN cancel THEN 'canceled'
        WHEN num_failures + 1 >= 10 THEN 'failed'
        ELSE 'errored'
    END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND reconciler_state = 'processing'
RETURNING id
";

const RECONCILER_MARK_FAILED_QUERY: &str = "
UPDATE changesets
SET reconciler_state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND reconciler_state = 'processing'
RETURNING id
";

/// `(*store[changeset]).MarkComplete`.
pub async fn mark_reconciler_complete(pool: &PgPool, id: i64) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(RECONCILER_MARK_COMPLETE_QUERY)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[changeset]).MarkErrored` (`MaxNumRetries=10`).
pub async fn mark_reconciler_errored(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(RECONCILER_MARK_ERRORED_QUERY)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `(*store[changeset]).MarkFailed`.
pub async fn mark_reconciler_failed(
    pool: &PgPool,
    id: i64,
    failure_message: &str,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(RECONCILER_MARK_FAILED_QUERY)
        .bind(id)
        .bind(failure_message)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

const GET_CHANGESET_SPEC_QUERY: &str = "
SELECT
    changeset_specs.id,
    COALESCE(changeset_specs.external_id, '') AS external_id,
    COALESCE(changeset_specs.base_rev, '') AS base_rev,
    COALESCE(changeset_specs.base_ref, '') AS base_ref,
    COALESCE(changeset_specs.head_ref, '') AS head_ref,
    COALESCE(changeset_specs.title, '') AS title,
    COALESCE(changeset_specs.body, '') AS body,
    changeset_specs.published,
    COALESCE(changeset_specs.diff, ''::bytea) AS diff,
    COALESCE(changeset_specs.commit_message, '') AS commit_message,
    COALESCE(changeset_specs.commit_author_name, '') AS commit_author_name,
    COALESCE(changeset_specs.commit_author_email, '') AS commit_author_email
FROM changeset_specs
INNER JOIN repo ON repo.id = changeset_specs.repo_id
WHERE repo.deleted_at IS NULL AND changeset_specs.id = $1
LIMIT 1
";

/// `Store.GetChangesetSpecByID`.
pub async fn get_changeset_spec(
    conn: &mut sqlx::PgConnection,
    id: i64,
) -> Result<Option<ChangesetSpecRow>, sqlx::Error> {
    let row = sqlx::query(GET_CHANGESET_SPEC_QUERY)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|row| ChangesetSpecRow {
        id: row.get("id"),
        external_id: row.get("external_id"),
        base_rev: row.get("base_rev"),
        base_ref: row.get("base_ref"),
        head_ref: row.get("head_ref"),
        title: row.get("title"),
        body: row.get("body"),
        published: row.get("published"),
        diff: row.get("diff"),
        commit_message: row.get("commit_message"),
        commit_author_name: row.get("commit_author_name"),
        commit_author_email: row.get("commit_author_email"),
    }))
}

const GET_CHANGESET_BY_EXTERNAL_BRANCH_QUERY: &str = "
SELECT changesets.id
FROM changesets
INNER JOIN repo ON repo.id = changesets.repo_id
WHERE repo.deleted_at IS NULL
  AND changesets.external_service_type = $1
  AND changesets.repo_id = $2
  AND changesets.external_branch = $3
ORDER BY changesets.id ASC
LIMIT 1
";

/// `Store.GetChangeset` with `{ExternalServiceType, RepoID, ExternalBranch}`.
pub async fn get_changeset_by_external_branch(
    conn: &mut sqlx::PgConnection,
    external_service_type: &str,
    repo_id: i32,
    external_branch: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(GET_CHANGESET_BY_EXTERNAL_BRANCH_QUERY)
        .bind(external_service_type)
        .bind(repo_id)
        .bind(external_branch)
        .fetch_optional(&mut *conn)
        .await
}

/// A repository as the reconciler needs it (`Repos().Get` + remote repo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerRepo {
    pub id: i32,
    pub name: String,
    pub archived: bool,
}

/// `Repos().Get` (the reconciler only reads id/name/archived).
pub async fn get_reconciler_repo(
    conn: &mut sqlx::PgConnection,
    id: i32,
) -> Result<Option<ReconcilerRepo>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id, name, archived FROM repo WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|row| ReconcilerRepo {
        id: row.get("id"),
        name: row.get("name"),
        archived: row.get("archived"),
    }))
}

/// Mark a repository archived (`handleArchivedRepo`'s `UpdateRepo`).
pub async fn mark_reconciler_repo_archived(
    conn: &mut sqlx::PgConnection,
    id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE repo SET archived = TRUE WHERE id = $1")
        .bind(id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The fields the reconciler writes back after executing a plan. The baseline
/// `UpdateChangeset` writes every column; the production adapter only reaches
/// this path for source-independent operations (detach/archive/reattach/close
/// no-op) because code-host sources are not ported (see overview §4).
pub struct ChangesetUpdate<'a> {
    pub id: i64,
    pub batch_change_ids: serde_json::Value,
    pub detached_at: Option<OffsetDateTime>,
    pub closing: bool,
    pub publication_state: &'a str,
    pub ui_publication_state: Option<&'a str>,
    pub external_id: Option<&'a str>,
    pub external_branch: Option<&'a str>,
    pub external_state: Option<&'a str>,
    pub failure_message: Option<&'a str>,
    pub previous_failure_message: Option<&'a str>,
}

/// `Store.UpdateChangeset` (targeted columns, see [`ChangesetUpdate`]).
pub async fn update_reconciler_changeset(
    conn: &mut sqlx::PgConnection,
    update: &ChangesetUpdate<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
UPDATE changesets
SET batch_change_ids = $2,
    detached_at = $3,
    closing = $4,
    publication_state = $5,
    ui_publication_state = $6::batch_changes_changeset_ui_publication_state,
    external_id = $7,
    external_branch = $8,
    external_state = $9,
    failure_message = $10,
    previous_failure_message = $11,
    updated_at = clock_timestamp()
WHERE id = $1
"#,
    )
    .bind(update.id)
    .bind(&update.batch_change_ids)
    .bind(update.detached_at)
    .bind(update.closing)
    .bind(update.publication_state)
    .bind(update.ui_publication_state)
    .bind(update.external_id)
    .bind(update.external_branch)
    .bind(update.external_state)
    .bind(update.failure_message)
    .bind(update.previous_failure_message)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `Store.UpdateChangesetCommitVerification` (`changesets.go:1088-1116`): the
/// baseline writes only `updated_at` and `commit_verification` in one
/// statement. `jsonbColumn(nil)` is `{}`, so callers pass `{}` for an
/// unverified commit rather than SQL `NULL`.
pub async fn update_reconciler_changeset_commit_verification(
    conn: &mut sqlx::PgConnection,
    id: i64,
    verification: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
UPDATE changesets
SET updated_at = clock_timestamp(),
    commit_verification = $2::jsonb
WHERE id = $1
"#,
    )
    .bind(id)
    .bind(verification)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `BatchChange.URL(ctx, namespaceName)` (the `[link](url)` markdown wrapper is
/// applied by the executor). Errors mirror `loadBatchChange`/`GetByID`/`URL`.
pub async fn batch_change_link(
    conn: &mut sqlx::PgConnection,
    external_url: &str,
    batch_change_id: i64,
) -> Result<String, String> {
    if batch_change_id == 0 {
        return Err("changeset has no owning batch change".to_owned());
    }
    let row = sqlx::query(
        "SELECT name, namespace_org_id, namespace_user_id FROM batch_changes WHERE id = $1 LIMIT 1",
    )
    .bind(batch_change_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| err.to_string())?
    .ok_or_else(|| format!("batch change not found: {batch_change_id}"))?;
    let name: String = row.get("name");
    // Exactly one namespace column is set (`batch_changes_has_1_namespace`).
    let org_id: Option<i32> = row.get("namespace_org_id");
    let user_id: Option<i32> = row.get("namespace_user_id");

    let (prefix, namespace_name): (&str, String) = if let Some(org_id) = org_id.filter(|id| *id != 0)
    {
        let name = sqlx::query_scalar::<_, String>("SELECT name FROM orgs WHERE id = $1 LIMIT 1")
            .bind(org_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("org not found: {org_id}"))?;
        ("organizations", name)
    } else {
        let user_id = user_id
            .filter(|id| *id != 0)
            .ok_or_else(|| "batch change has no namespace".to_owned())?;
        let name =
            sqlx::query_scalar::<_, String>("SELECT username FROM users WHERE id = $1 LIMIT 1")
                .bind(user_id)
                .fetch_optional(&mut *conn)
                .await
                .map_err(|err| err.to_string())?
                .ok_or_else(|| format!("namespace not found: {user_id}"))?;
        ("users", name)
    };

    let base = external_url.trim_end_matches('/');
    Ok(format!(
        "[_Created by Sourcegraph batch change `{namespace_name}/{name}`._]({base}/{prefix}/{namespace_name}/batch-changes/{name})"
    ))
}

#[cfg(test)]
mod reconciler_tests {
    use super::*;

    #[test]
    fn reconciler_store_options_match_the_baseline() {
        assert_eq!(RECONCILER_STORE_NAME, "batches_reconciler_worker_store");
        assert_eq!(RECONCILER_TABLE, "changesets");
        assert_eq!(RECONCILER_VIEW, "reconciler_changesets changesets");
        assert_eq!(RECONCILER_STATE_COLUMN, "reconciler_state");
        assert_eq!(RECONCILER_STALLED_MAX_AGE_SECONDS, 60);
        assert_eq!(RECONCILER_MAX_NUM_RESETS, 10);
        assert_eq!(RECONCILER_RETRY_AFTER_SECONDS, 5);
        assert_eq!(RECONCILER_MAX_NUM_RETRIES, 10);
        assert_eq!(
            RECONCILER_ORDER_BY,
            "changesets.reconciler_state = 'errored', changesets.updated_at DESC"
        );
        assert_eq!(RECONCILER_WORKER_NAME, "batches_reconciler_worker");
    }

    #[test]
    fn dequeue_and_mark_queries_target_the_reconciler_state() {
        assert!(RECONCILER_DEQUEUE_QUERY.contains("changesets.reconciler_state = 'queued'"));
        assert!(RECONCILER_DEQUEUE_QUERY.contains("FOR UPDATE OF changesets SKIP LOCKED"));
        assert!(RECONCILER_DEQUEUE_QUERY.contains("reconciler_state = 'processing'"));
        assert!(RECONCILER_MARK_COMPLETE_QUERY.contains("reconciler_state = 'completed'"));
        assert!(RECONCILER_MARK_ERRORED_QUERY.contains("num_failures + 1 >= 10"));
        assert!(RECONCILER_MARK_ERRORED_QUERY.contains("THEN 'failed'"));
        assert!(RECONCILER_MARK_FAILED_QUERY.contains("THEN 'failed'"));
        assert!(RECONCILER_HEARTBEAT_QUERY.contains("::text[]::bigint[]"));
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
