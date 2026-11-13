#![forbid(unsafe_code)]

//! Postgres adapter for [`ChangesetReadDatabase`] (M9.26c).
//!
//! Direct ports of the store queries the changeset deep-read surface needs:
//! `changeset_events.go` (`ListChangesetEvents`, `CountChangesetEvents`),
//! `changesets.go` (`ScanChangeset` extras, `GetChangesetsStats`,
//! `GetGlobalChangesetsStats`), `changeset_specs.go`
//! (`GetChangesetSpecByID`), `batch_changes.go` (`GetBatchChangeDiffStat`),
//! `bulk_operations.go` (`ListBulkOperations`, `CountBulkOperations`,
//! `ListBulkOperationErrors`) and `codehost.go` (`ListCodeHosts`).
//!
//! No table, column or index is added.

use serde_json::Value as JsonValue;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{Postgres, Row};
use time::OffsetDateTime;

use crate::batches_changeset_read::{
    BulkOperationErrorRow, BulkOperationListRow, ChangesetEventRow, ChangesetReadDatabase,
    ChangesetReadExtra, ChangesetSpecDiffRow, ChangesetsStats, CodeHostRow,
    GlobalChangesetsStats,
};
use crate::batches_graphql::BulkOperationRow;
use crate::batches_graphql_store::{
    authz_conds_async, block_on, error, PgBatchesGraphqlStore, SUPPORTED_CODE_HOSTS,
};
use sg_extsvc::batches_state::ChangesetSyncState;
use sg_graphql::exec::Actor;

/// `bulkOperationColumns` (`bulk_operations.go:15-49`).
const BULK_OPERATION_COLUMNS: &str = "changeset_jobs.bulk_group AS id, \
     MIN(changeset_jobs.id) AS db_id, changeset_jobs.job_type AS type, \
     CASE WHEN COUNT(*) FILTER (WHERE changeset_jobs.state IN ('processing', 'queued', 'errored')) > 0 \
         THEN 'PROCESSING' \
         WHEN COUNT(*) FILTER (WHERE changeset_jobs.state = 'failed') > 0 THEN 'FAILED' \
         ELSE 'COMPLETED' END AS state, \
     CAST(COUNT(*) FILTER (WHERE changeset_jobs.state IN ('completed', 'failed')) AS float) \
         / CAST(COUNT(*) AS float) AS progress, \
     MIN(changeset_jobs.user_id) AS user_id, COUNT(changeset_jobs.id) AS changeset_count, \
     MIN(changeset_jobs.created_at) AS created_at, \
     CASE WHEN (COUNT(*) FILTER (WHERE changeset_jobs.state IN ('completed', 'failed')) \
         / COUNT(*)) = 1.0 THEN MAX(changeset_jobs.finished_at) ELSE null END AS finished_at";

/// `archivedInBatchChange` for a text placeholder (e.g. `$1`).
fn archived_in_batch_change(placeholder: &str) -> String {
    format!(
        "(COALESCE((changesets.batch_change_ids->{placeholder}->>'isArchived')::bool, false) \
         OR COALESCE((changesets.batch_change_ids->{placeholder}->>'archive')::bool, false))"
    )
}

enum Param {
    I64(i64),
    I64Array(Vec<i64>),
    TextArray(Vec<String>),
    Time(OffsetDateTime),
}

fn bind_all<'q>(
    mut query: Query<'q, Postgres, PgArguments>,
    params: Vec<Param>,
) -> Query<'q, Postgres, PgArguments> {
    for param in params {
        query = match param {
            Param::I64(value) => query.bind(value),
            Param::I64Array(value) => query.bind(value),
            Param::TextArray(value) => query.bind(value),
            Param::Time(value) => query.bind(value),
        };
    }
    query
}

fn parse_sync_state(value: &JsonValue) -> ChangesetSyncState {
    let text = |key: &str| {
        value
            .get(key)
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    ChangesetSyncState {
        base_ref_oid: text("BaseRefOid"),
        head_ref_oid: text("HeadRefOid"),
        is_complete: value
            .get("IsComplete")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    }
}

#[allow(unused_assignments)]
fn scan_event(row: &sqlx::postgres::PgRow) -> Result<ChangesetEventRow, sqlx::Error> {
    let mut index = 0usize;
    macro_rules! next {
        ($ty:ty) => {{
            let value: $ty = row.try_get(index)?;
            index += 1;
            value
        }};
    }
    Ok(ChangesetEventRow {
        id: next!(i64),
        changeset_id: next!(i64),
        kind: next!(String),
        created_at: next!(OffsetDateTime),
        metadata: next!(JsonValue),
    })
}

impl ChangesetReadDatabase for PgBatchesGraphqlStore {
    fn changeset_events(
        &self,
        changeset_ids: &[i64],
        kinds: &[&str],
        limit: i64,
        cursor: i64,
    ) -> Result<(Vec<ChangesetEventRow>, i64), String> {
        let mut params = vec![Param::I64(cursor)];
        let mut sql = String::from(
            "SELECT id, changeset_id, kind, created_at, COALESCE(metadata, '{}'::jsonb) \
             FROM changeset_events WHERE id >= $1",
        );
        if !changeset_ids.is_empty() {
            params.push(Param::I64Array(changeset_ids.to_vec()));
            sql.push_str(&format!(" AND changeset_id = ANY(${})", params.len()));
        }
        if !kinds.is_empty() {
            params.push(Param::TextArray(
                kinds.iter().map(|kind| (*kind).to_owned()).collect(),
            ));
            sql.push_str(&format!(" AND kind = ANY(${})", params.len()));
        }
        sql.push_str(" ORDER BY id ASC");
        if limit != 0 {
            sql.push_str(&format!(" LIMIT {}", limit + 1));
        }
        let rows = block_on(bind_all(sqlx::query(&sql), params).fetch_all(&self.pool))
            .map_err(error)?;
        let mut events = rows
            .iter()
            .map(scan_event)
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        let mut next = 0i64;
        if limit != 0 && events.len() as i64 == limit + 1 {
            next = events[events.len() - 1].id;
            events.truncate(limit as usize);
        }
        Ok((events, next))
    }

    fn count_changeset_events(&self, changeset_id: i64) -> Result<i64, String> {
        block_on(
            sqlx::query_scalar("SELECT COUNT(id) FROM changeset_events WHERE changeset_id = $1")
                .bind(changeset_id)
                .fetch_one(&self.pool),
        )
        .map_err(error)
    }

    fn changeset_read_extra(
        &self,
        changeset_id: i64,
    ) -> Result<Option<ChangesetReadExtra>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT COALESCE(commit_verification, '{}'::jsonb), \
                 COALESCE(sync_state, '{}'::jsonb) FROM changesets WHERE id = $1",
            )
            .bind(changeset_id)
            .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let commit_verification: JsonValue = row.try_get(0).map_err(error)?;
        let sync_state: JsonValue = row.try_get(1).map_err(error)?;
        Ok(Some(ChangesetReadExtra {
            commit_verification,
            sync_state: parse_sync_state(&sync_state),
        }))
    }

    fn changeset_spec_for_diff(
        &self,
        spec_id: i64,
    ) -> Result<Option<ChangesetSpecDiffRow>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT changeset_specs.type, COALESCE(changeset_specs.base_rev, ''), \
                 COALESCE(changeset_specs.diff, ''::bytea) \
                 FROM changeset_specs INNER JOIN repo ON repo.id = changeset_specs.repo_id \
                 WHERE repo.deleted_at IS NULL AND changeset_specs.id = $1 LIMIT 1",
            )
            .bind(spec_id)
            .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ChangesetSpecDiffRow {
            spec_type: row.try_get(0).map_err(error)?,
            base_rev: row.try_get(1).map_err(error)?,
            diff: row.try_get(2).map_err(error)?,
        }))
    }

    fn changesets_stats(&self, batch_change_id: i64) -> Result<ChangesetsStats, String> {
        let archived = archived_in_batch_change("$1");
        let sql = format!(
            "SELECT COUNT(*) AS total, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'RETRYING') AS retrying, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'FAILED') AS failed, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'SCHEDULED') AS scheduled, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'PROCESSING') AS processing, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'UNPUBLISHED') AS unpublished, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'CLOSED') AS closed, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'DRAFT') AS draft, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'MERGED') AS merged, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'OPEN') AS open, \
             COUNT(*) FILTER (WHERE NOT {archived} AND changesets.computed_state = 'DELETED') AS deleted, \
             COUNT(*) FILTER (WHERE {archived}) AS archived \
             FROM changesets INNER JOIN repo ON repo.id = changesets.repo_id \
             WHERE repo.deleted_at IS NULL AND changesets.batch_change_ids ? $1"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(batch_change_id.to_string())
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        macro_rules! count {
            ($index:expr) => {{
                let value: i64 = row.try_get($index).map_err(error)?;
                value as i32
            }};
        }
        Ok(ChangesetsStats {
            total: count!(0),
            retrying: count!(1),
            failed: count!(2),
            scheduled: count!(3),
            processing: count!(4),
            unpublished: count!(5),
            closed: count!(6),
            draft: count!(7),
            merged: count!(8),
            open: count!(9),
            deleted: count!(10),
            archived: count!(11),
        })
    }

    fn global_changesets_stats(&self) -> Result<GlobalChangesetsStats, String> {
        const SQL: &str = "SELECT \
             COUNT(*) AS total, \
             COUNT(*) FILTER (WHERE computed_state = 'UNPUBLISHED') AS unpublished, \
             COUNT(*) FILTER (WHERE computed_state = 'DRAFT') AS draft, \
             COUNT(*) FILTER (WHERE computed_state = 'CLOSED') AS closed, \
             COUNT(*) FILTER (WHERE computed_state = 'MERGED') AS merged, \
             COUNT(*) FILTER (WHERE computed_state = 'OPEN') AS open \
             FROM ( \
                 SELECT changesets.id, changesets.computed_state \
                 FROM changesets INNER JOIN repo ON repo.id = changesets.repo_id \
                 WHERE jsonb_path_exists(changesets.batch_change_ids, \
                     '$.* ? ((!exists(@.isArchived) || @.isArchived == false) && \
                     (!exists(@.archive) || @.archive == false))') \
                 AND repo.deleted_at IS NULL AND repo.blocked IS NULL \
             ) AS fcs";
        let row = block_on(sqlx::query(SQL).fetch_one(&self.pool)).map_err(error)?;
        macro_rules! count {
            ($index:expr) => {{
                let value: i64 = row.try_get($index).map_err(error)?;
                value as i32
            }};
        }
        Ok(GlobalChangesetsStats {
            total: count!(0),
            unpublished: count!(1),
            draft: count!(2),
            closed: count!(3),
            merged: count!(4),
            open: count!(5),
        })
    }

    fn batch_change_diff_stat(
        &self,
        batch_change_id: i64,
        actor: Actor,
    ) -> Result<(i64, i64), String> {
        let authz = block_on(authz_conds_async(&self.pool, actor))?;
        let sql = format!(
            "SELECT COALESCE(SUM(changesets.diff_stat_added), 0), \
             COALESCE(SUM(changesets.diff_stat_deleted), 0) \
             FROM changesets INNER JOIN repo ON changesets.repo_id = repo.id \
             WHERE changesets.batch_change_ids ? $1 AND repo.deleted_at IS NULL AND {authz}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(batch_change_id.to_string())
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        Ok((
            row.try_get(0).map_err(error)?,
            row.try_get(1).map_err(error)?,
        ))
    }

    fn bulk_operations(
        &self,
        batch_change_id: i64,
        limit: i64,
        cursor: i64,
        created_after: Option<OffsetDateTime>,
    ) -> Result<(Vec<BulkOperationListRow>, i64), String> {
        let mut params = vec![Param::I64(batch_change_id)];
        let mut sql = format!(
            "SELECT {BULK_OPERATION_COLUMNS} \
             FROM changeset_jobs \
             INNER JOIN changesets ON changesets.id = changeset_jobs.changeset_id \
             INNER JOIN repo ON repo.id = changesets.repo_id \
             WHERE repo.deleted_at IS NULL AND changeset_jobs.batch_change_id = $1"
        );
        if cursor > 0 {
            params.push(Param::I64(cursor));
            sql.push_str(&format!(" AND changeset_jobs.id <= ${}", params.len()));
        }
        sql.push_str(" GROUP BY changeset_jobs.bulk_group, changeset_jobs.job_type");
        if let Some(created_after) = created_after {
            params.push(Param::Time(created_after));
            sql.push_str(&format!(
                " HAVING MIN(changeset_jobs.created_at) >= ${}",
                params.len()
            ));
        }
        sql.push_str(" ORDER BY MIN(changeset_jobs.id) DESC");
        if limit != 0 {
            sql.push_str(&format!(" LIMIT {}", limit + 1));
        }
        let rows = block_on(bind_all(sqlx::query(&sql), params).fetch_all(&self.pool))
            .map_err(error)?;
        macro_rules! scan {
            ($row:expr) => {{
                let row = $row;
                Ok::<BulkOperationListRow, sqlx::Error>(BulkOperationListRow {
                    db_id: row.try_get(1)?,
                    operation: BulkOperationRow {
                        id: row.try_get(0)?,
                        job_type: row.try_get(2)?,
                        state: row.try_get(3)?,
                        progress: row.try_get(4)?,
                        user_id: row.try_get(5)?,
                        changeset_count: row.try_get(6)?,
                        created_at: row.try_get(7)?,
                        finished_at: row.try_get(8)?,
                    },
                })
            }};
        }
        let mut operations = rows
            .iter()
            .map(|row| scan!(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        let mut next = 0i64;
        if limit != 0 && operations.len() as i64 == limit + 1 {
            next = operations[operations.len() - 1].db_id;
            operations.truncate(limit as usize);
        }
        Ok((operations, next))
    }

    fn count_bulk_operations(
        &self,
        batch_change_id: i64,
        created_after: Option<OffsetDateTime>,
    ) -> Result<i64, String> {
        let mut params = vec![Param::I64(batch_change_id)];
        let mut sql = String::from(
            "SELECT COUNT(DISTINCT(changeset_jobs.bulk_group)) FROM changeset_jobs \
             INNER JOIN changesets ON changesets.id = changeset_jobs.changeset_id \
             INNER JOIN repo ON repo.id = changesets.repo_id \
             WHERE repo.deleted_at IS NULL AND changeset_jobs.batch_change_id = $1",
        );
        if let Some(created_after) = created_after {
            params.push(Param::Time(created_after));
            sql.push_str(&format!(" AND changeset_jobs.created_at >= ${}", params.len()));
        }
        let row = block_on(bind_all(sqlx::query(&sql), params).fetch_one(&self.pool))
            .map_err(error)?;
        row.try_get(0).map_err(error)
    }

    fn bulk_operation_errors(
        &self,
        bulk_group: &str,
    ) -> Result<Vec<BulkOperationErrorRow>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT changeset_jobs.changeset_id, changeset_jobs.failure_message \
                 FROM changeset_jobs \
                 INNER JOIN changesets ON changesets.id = changeset_jobs.changeset_id \
                 INNER JOIN repo ON repo.id = changesets.repo_id \
                 WHERE repo.deleted_at IS NULL AND changeset_jobs.failure_message IS NOT NULL \
                 AND changeset_jobs.bulk_group = $1",
            )
            .bind(bulk_group)
            .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter()
            .map(|row| {
                Ok(BulkOperationErrorRow {
                    changeset_id: row.try_get(0)?,
                    error: row.try_get(1)?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(error)
    }

    fn code_hosts(&self) -> Result<Vec<CodeHostRow>, String> {
        let supported = SUPPORTED_CODE_HOSTS
            .iter()
            .map(|kind| format!("'{kind}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH esr_with_ssh AS ( \
                 SELECT * FROM external_service_repos \
                 WHERE clone_url ILIKE $1 OR clone_url !~* '[[:alpha:]]+://'), \
             esr_with_webhooks AS ( \
                 SELECT * FROM external_service_repos \
                 WHERE external_service_id IN ( \
                     SELECT id FROM external_services \
                     WHERE has_webhooks IS NULL OR has_webhooks = TRUE)), \
             aggregated_repos AS ( \
                 SELECT repo.external_service_type, repo.external_service_id, \
                     COUNT(esr_with_ssh.external_service_id) AS ssh_required_count, \
                     COUNT(esr_with_webhooks.external_service_id) AS has_webhooks_count \
                 FROM repo \
                 LEFT JOIN esr_with_ssh ON repo.id = esr_with_ssh.repo_id \
                 LEFT JOIN esr_with_webhooks ON repo.id = esr_with_webhooks.repo_id \
                 WHERE repo.deleted_at IS NULL \
                 AND repo.external_service_type IN ({supported}) \
                 GROUP BY repo.external_service_type, repo.external_service_id \
                 ORDER BY repo.external_service_type ASC, repo.external_service_id ASC) \
             SELECT external_service_type, external_service_id, \
                 ssh_required_count > 0 AS ssh_required, \
                 has_webhooks_count > 0 AS has_webhooks \
             FROM aggregated_repos WHERE TRUE"
        );
        let rows = block_on(
            sqlx::query(&sql)
                .bind("ssh://%")
                .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter()
            .map(|row| {
                Ok(CodeHostRow {
                    external_service_type: row.try_get(0)?,
                    external_service_id: row.try_get(1)?,
                    requires_ssh: row.try_get(2)?,
                    has_webhooks: row.try_get(3)?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(error)
    }

    fn changeset_diff_source(
        &self,
    ) -> Option<&dyn crate::batches_changeset_read::ChangesetDiffSource> {
        self.changeset_diff_source.as_deref()
    }
}
