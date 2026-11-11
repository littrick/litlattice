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
    ui_publication_state = $6,
    external_id = $7,
    external_state = $8,
    failure_message = $9,
    previous_failure_message = $10,
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
    .bind(update.external_state)
    .bind(update.failure_message)
    .bind(update.previous_failure_message)
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
    let org_id: i32 = row.get("namespace_org_id");
    let user_id: i32 = row.get("namespace_user_id");

    let (prefix, namespace_name): (&str, String) = if org_id != 0 {
        let name = sqlx::query_scalar::<_, String>("SELECT name FROM orgs WHERE id = $1 LIMIT 1")
            .bind(org_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("org not found: {org_id}"))?;
        ("organizations", name)
    } else {
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
