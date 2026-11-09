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
