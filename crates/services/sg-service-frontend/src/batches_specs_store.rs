#![forbid(unsafe_code)]

//! Postgres adapter for the M9.26a Batch Changes spec store seam.
//!
//! Ports the SQL of `internal/batches/store/{batch_specs.go,
//! changeset_specs.go,batch_spec_workspaces.go}` (the subset the GraphQL spec
//! surface reads) plus the `service.CreateBatchSpec` transaction. All queries
//! use existing tables only; no new tables, columns or Redis keys.

use sg_graphql::exec::Actor;
use sqlx::{PgPool, Postgres, Row};
use time::OffsetDateTime;

use crate::batches_graphql_store::{block_on, PgBatchesGraphqlStore};
use crate::batches_specs_graphql::{
    canonical_batch_spec, BatchSpecFilter, BatchSpecPage, BatchSpecResolutionJobRow, BatchSpecRow,
    BatchSpecStats, BatchSpecWorkspaceExecutionRow, BatchSpecWorkspaceFilter,
    BatchSpecWorkspacePage, BatchSpecWorkspaceRow, BatchesSpecsDatabase, ChangesetSpecFilter,
    ChangesetSpecPage, ChangesetSpecRow, NewBatchSpec, NewChangesetSpec,
};

const BATCH_SPEC_COLUMNS: &str = "batch_specs.id, batch_specs.rand_id, batch_specs.raw_spec, \
     batch_specs.spec::text, batch_specs.namespace_user_id, batch_specs.namespace_org_id, \
     batch_specs.user_id, batch_specs.created_from_raw, batch_specs.allow_unsupported, \
     batch_specs.allow_ignored, batch_specs.no_cache, batch_specs.batch_change_id, \
     batch_specs.created_at, batch_specs.updated_at";

const CHANGESET_SPEC_COLUMNS: &str = "changeset_specs.id, changeset_specs.rand_id, \
     changeset_specs.batch_spec_id, changeset_specs.repo_id, changeset_specs.user_id, \
     changeset_specs.diff_stat_added, changeset_specs.diff_stat_deleted, \
     changeset_specs.created_at, changeset_specs.updated_at, \
     changeset_specs.fork_namespace, changeset_specs.external_id, changeset_specs.head_ref, \
     changeset_specs.title, changeset_specs.base_rev, changeset_specs.base_ref, \
     changeset_specs.body, changeset_specs.published, changeset_specs.diff, \
     changeset_specs.commit_message, changeset_specs.commit_author_name, \
     changeset_specs.commit_author_email, changeset_specs.type";

fn error(error: sqlx::Error) -> String {
    error.to_string()
}

// ---------------------------------------------------------------------------
// Bind list
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Bind {
    I32(i32),
    I64(i64),
    Bool(bool),
    Text(String),
    TextArray(Vec<String>),
}

fn bind_query<'q>(
    text: &'q str,
    binds: &[Bind],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    let mut query = sqlx::query(text);
    for bind in binds {
        query = match bind {
            Bind::I32(value) => query.bind(*value),
            Bind::I64(value) => query.bind(*value),
            Bind::Bool(value) => query.bind(*value),
            Bind::Text(value) => query.bind(value.clone()),
            Bind::TextArray(value) => query.bind(value.clone()),
        };
    }
    query
}

fn push_bind(sql: &mut String, binds: &mut Vec<Bind>, value: Bind) {
    binds.push(value);
    sql.push('$');
    sql.push_str(&binds.len().to_string());
}

// ---------------------------------------------------------------------------
// Scanning
// ---------------------------------------------------------------------------

fn scan_batch_spec(row: &sqlx::postgres::PgRow) -> Result<BatchSpecRow, String> {
    let spec_text: String = row.try_get(3).map_err(error)?;
    let spec_value: serde_json::Value = serde_json::from_str(&spec_text)
        .map_err(|error| format!("scanBatchSpec: failed to unmarshal spec: {error}"))?;
    let spec = canonical_batch_spec(&spec_value)
        .map_err(|error| format!("scanBatchSpec: failed to unmarshal spec: {error}"))?;
    // `batchSpecResolver.Description` reads `Spec.Name` / `Spec.Description`.
    let name = spec_value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let description = spec_value
        .get("description")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Ok(BatchSpecRow {
        id: row.try_get(0).map_err(error)?,
        rand_id: row.try_get(1).map_err(error)?,
        raw_spec: row.try_get(2).map_err(error)?,
        spec,
        name,
        description,
        namespace_user_id: row
            .try_get::<Option<i32>, _>(4)
            .map_err(error)?
            .unwrap_or(0),
        namespace_org_id: row
            .try_get::<Option<i32>, _>(5)
            .map_err(error)?
            .unwrap_or(0),
        user_id: row
            .try_get::<Option<i32>, _>(6)
            .map_err(error)?
            .unwrap_or(0),
        created_from_raw: row.try_get(7).map_err(error)?,
        allow_unsupported: row.try_get(8).map_err(error)?,
        allow_ignored: row.try_get(9).map_err(error)?,
        no_cache: row.try_get(10).map_err(error)?,
        batch_change_id: row
            .try_get::<Option<i64>, _>(11)
            .map_err(error)?
            .unwrap_or(0),
        created_at: row.try_get(12).map_err(error)?,
        updated_at: row.try_get(13).map_err(error)?,
    })
}

fn scan_changeset_spec(row: &sqlx::postgres::PgRow) -> Result<ChangesetSpecRow, String> {
    Ok(ChangesetSpecRow {
        id: row.try_get(0).map_err(error)?,
        rand_id: row.try_get(1).map_err(error)?,
        batch_spec_id: row.try_get::<Option<i64>, _>(2).map_err(error)?.unwrap_or(0),
        repo_id: row.try_get(3).map_err(error)?,
        user_id: row.try_get::<Option<i32>, _>(4).map_err(error)?.unwrap_or(0),
        diff_stat_added: row
            .try_get::<Option<i32>, _>(5)
            .map_err(error)?
            .unwrap_or(0),
        diff_stat_deleted: row
            .try_get::<Option<i32>, _>(6)
            .map_err(error)?
            .unwrap_or(0),
        created_at: row.try_get(7).map_err(error)?,
        updated_at: row.try_get(8).map_err(error)?,
        fork_namespace: row.try_get(9).map_err(error)?,
        external_id: row.try_get(10).map_err(error)?,
        head_ref: row.try_get(11).map_err(error)?,
        title: row.try_get(12).map_err(error)?,
        base_rev: row.try_get(13).map_err(error)?,
        base_ref: row.try_get(14).map_err(error)?,
        body: row.try_get(15).map_err(error)?,
        // `published` is a text column holding the JSON encoding of
        // `batcheslib.PublishedValue` (`true` / `false` / `"draft"` / NULL).
        published: row
            .try_get::<Option<String>, _>(16)
            .map_err(error)?
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| {
                    format!("scanChangesetSpec: failed to unmarshal published: {error}")
                })
            })
            .transpose()?,
        diff: row
            .try_get::<Option<Vec<u8>>, _>(17)
            .map_err(error)?
            .unwrap_or_default(),
        commit_message: row.try_get(18).map_err(error)?,
        commit_author_name: row.try_get(19).map_err(error)?,
        commit_author_email: row.try_get(20).map_err(error)?,
        kind: row.try_get(21).map_err(error)?,
    })
}

// ---------------------------------------------------------------------------
// Authz
// ---------------------------------------------------------------------------

async fn site_admin_async(pool: &PgPool, actor: Actor) -> Result<bool, String> {
    match actor {
        Actor::Internal => Ok(true),
        Actor::Anonymous => Ok(false),
        Actor::User { uid } => sg_db::users::get_site_admin(pool, uid)
            .await
            .map_err(error)
            .map(|value| value == Some(true)),
    }
}

/// `database.AuthzQueryConds` for the actor (fragment references `repo`).
async fn repo_authz_conds(pool: &PgPool, actor: Actor) -> Result<String, String> {
    if site_admin_async(pool, actor).await? {
        return Ok("TRUE".to_owned());
    }
    let uid = actor.uid().unwrap_or(0);
    Ok(format!(
        "(EXISTS (SELECT FROM user_repo_permissions \
            WHERE repo_id = repo.id AND user_id IS NULL) \
         OR (NOT repo.private OR EXISTS (SELECT FROM external_services AS es \
            JOIN external_service_repos AS esr ON (esr.external_service_id = es.id \
                AND esr.repo_id = repo.id AND es.unrestricted = TRUE \
                AND es.deleted_at IS NULL))) \
         OR EXISTS (SELECT repo_id FROM user_repo_permissions \
            WHERE repo_id = repo.id AND user_id = {uid}))"
    ))
}

// ---------------------------------------------------------------------------
// Async helpers
// ---------------------------------------------------------------------------

async fn get_batch_spec_async(
    pool: &PgPool,
    id: i64,
    rand_id: &str,
    exclude_created_from_raw_not_owned_by_user: i32,
) -> Result<Option<BatchSpecRow>, String> {
    let mut sql = format!(
        "SELECT {BATCH_SPEC_COLUMNS} FROM batch_specs WHERE "
    );
    let mut binds: Vec<Bind> = Vec::new();
    let mut preds: Vec<String> = Vec::new();
    if id != 0 {
        let mut pred = String::from("id = ");
        push_bind(&mut pred, &mut binds, Bind::I64(id));
        preds.push(pred);
    }
    if !rand_id.is_empty() {
        let mut pred = String::from("rand_id = ");
        push_bind(&mut pred, &mut binds, Bind::Text(rand_id.to_owned()));
        preds.push(pred);
    }
    if exclude_created_from_raw_not_owned_by_user != 0 {
        let mut pred = String::from("(user_id = ");
        push_bind(
            &mut pred,
            &mut binds,
            Bind::I32(exclude_created_from_raw_not_owned_by_user),
        );
        pred.push_str(" OR created_from_raw IS FALSE)");
        preds.push(pred);
    }
    if preds.is_empty() {
        preds.push("TRUE".to_owned());
    }
    sql.push_str(&preds.join("\n AND "));
    sql.push_str(" LIMIT 1");
    let row = bind_query(&sql, &binds)
        .fetch_optional(pool)
        .await
        .map_err(error)?;
    row.as_ref().map(scan_batch_spec).transpose()
}

async fn list_batch_specs_async(
    pool: &PgPool,
    filter: &BatchSpecFilter,
) -> Result<BatchSpecPage, String> {
    let mut sql = format!("SELECT {BATCH_SPEC_COLUMNS} FROM batch_specs");
    let mut binds: Vec<Bind> = Vec::new();
    let mut joins: Vec<&str> = Vec::new();
    let mut preds: Vec<String> = Vec::new();

    if filter.batch_change_id != 0 {
        joins.push(
            "INNER JOIN batch_changes ON batch_changes.name = batch_specs.spec->>'name' \
             AND batch_changes.namespace_user_id IS NOT DISTINCT FROM batch_specs.namespace_user_id \
             AND batch_changes.namespace_org_id IS NOT DISTINCT FROM batch_specs.namespace_org_id",
        );
        let mut pred = String::from("batch_changes.id = ");
        push_bind(&mut pred, &mut binds, Bind::I64(filter.batch_change_id));
        preds.push(pred);
    }
    if filter.exclude_created_from_raw_not_owned_by_user != 0 {
        let mut pred = String::from("(batch_specs.user_id = ");
        push_bind(
            &mut pred,
            &mut binds,
            Bind::I32(filter.exclude_created_from_raw_not_owned_by_user),
        );
        pred.push_str(" OR batch_specs.created_from_raw IS FALSE)");
        preds.push(pred);
    }
    if !filter.include_locally_executed_specs {
        preds.push("batch_specs.created_from_raw IS TRUE".to_owned());
    }
    if filter.exclude_empty_specs {
        preds.push(
            "(EXISTS (SELECT * FROM jsonb_object_keys(batch_specs.spec) AS t (k) \
             WHERE t.k NOT LIKE 'name'))"
                .to_owned(),
        );
    }
    let order;
    if filter.newest_first {
        order = "batch_specs.id DESC";
        if filter.cursor != 0 {
            let mut pred = String::from("batch_specs.id <= ");
            push_bind(&mut pred, &mut binds, Bind::I64(filter.cursor));
            preds.push(pred);
        }
    } else {
        order = "batch_specs.id ASC";
        let mut pred = String::from("batch_specs.id >= ");
        push_bind(&mut pred, &mut binds, Bind::I64(filter.cursor));
        preds.push(pred);
    }
    if preds.is_empty() {
        preds.push("TRUE".to_owned());
    }
    for join in joins {
        sql.push('\n');
        sql.push_str(join);
    }
    sql.push_str("\nWHERE ");
    sql.push_str(&preds.join("\n AND "));
    sql.push_str("\nORDER BY ");
    sql.push_str(order);
    if filter.limit > 0 {
        sql.push_str(&format!("\nLIMIT {}", filter.limit + 1));
    }
    let rows = bind_query(&sql, &binds)
        .fetch_all(pool)
        .await
        .map_err(error)?;
    let mut page = BatchSpecPage {
        rows: rows.iter().map(scan_batch_spec).collect::<Result<_, _>>()?,
        next: 0,
    };
    if filter.limit != 0 && page.rows.len() as i64 == filter.limit + 1 {
        page.next = page
            .rows
            .last()
            .map(|row| row.id)
            .expect("non-empty when len == limit + 1");
        page.rows.pop();
    }
    Ok(page)
}

async fn count_batch_specs_async(
    pool: &PgPool,
    filter: &BatchSpecFilter,
) -> Result<i64, String> {
    let mut sql = String::from("SELECT COUNT(batch_specs.id) FROM batch_specs");
    let mut binds: Vec<Bind> = Vec::new();
    let mut joins: Vec<&str> = Vec::new();
    let mut preds: Vec<String> = Vec::new();
    if filter.batch_change_id != 0 {
        joins.push(
            "INNER JOIN batch_changes ON batch_changes.name = batch_specs.spec->>'name' \
             AND batch_changes.namespace_user_id IS NOT DISTINCT FROM batch_specs.namespace_user_id \
             AND batch_changes.namespace_org_id IS NOT DISTINCT FROM batch_specs.namespace_org_id",
        );
        let mut pred = String::from("batch_changes.id = ");
        push_bind(&mut pred, &mut binds, Bind::I64(filter.batch_change_id));
        preds.push(pred);
    }
    if filter.exclude_created_from_raw_not_owned_by_user != 0 {
        let mut pred = String::from("(batch_specs.user_id = ");
        push_bind(
            &mut pred,
            &mut binds,
            Bind::I32(filter.exclude_created_from_raw_not_owned_by_user),
        );
        pred.push_str(" OR batch_specs.created_from_raw IS FALSE)");
        preds.push(pred);
    }
    if !filter.include_locally_executed_specs {
        preds.push("batch_specs.created_from_raw IS TRUE".to_owned());
    }
    if filter.exclude_empty_specs {
        preds.push(
            "(EXISTS (SELECT * FROM jsonb_object_keys(batch_specs.spec) AS t (k) \
             WHERE t.k NOT LIKE 'name'))"
                .to_owned(),
        );
    }
    if preds.is_empty() {
        preds.push("TRUE".to_owned());
    }
    for join in joins {
        sql.push('\n');
        sql.push_str(join);
    }
    sql.push_str("\nWHERE ");
    sql.push_str(&preds.join("\n AND "));
    let count: i64 = bind_query(&sql, &binds)
        .fetch_one(pool)
        .await
        .map_err(error)?
        .try_get(0)
        .map_err(error)?;
    Ok(count)
}

async fn batch_spec_stats_async(
    pool: &PgPool,
    id: i64,
) -> Result<Option<BatchSpecStats>, String> {
    let row = sqlx::query(
        "SELECT \
             COALESCE(res_job.state IN ('completed', 'failed'), FALSE) AS resolution_done, \
             COUNT(ws.id) AS workspaces, \
             COUNT(ws.id) FILTER (WHERE ws.skipped) AS skipped_workspaces, \
             COUNT(ws.id) FILTER (WHERE ws.cached_result_found) AS cached_workspaces, \
             MIN(jobs.started_at) AS started_at, \
             MAX(jobs.finished_at) AS finished_at, \
             COUNT(jobs.id) AS executions, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'completed') AS completed, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'processing' AND jobs.cancel = FALSE) AS processing, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'queued') AS queued, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'failed') AS failed, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'canceled') AS canceled, \
             COUNT(jobs.id) FILTER (WHERE jobs.state = 'processing' AND jobs.cancel = TRUE) AS canceling \
         FROM batch_specs \
         LEFT JOIN batch_spec_resolution_jobs res_job ON res_job.batch_spec_id = batch_specs.id \
         LEFT JOIN batch_spec_workspaces ws ON ws.batch_spec_id = batch_specs.id \
         LEFT JOIN batch_spec_workspace_execution_jobs jobs ON jobs.batch_spec_workspace_id = ws.id \
         WHERE batch_specs.id = $1 \
         GROUP BY batch_specs.id, res_job.state",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(BatchSpecStats {
        resolution_done: row.try_get(0).map_err(error)?,
        workspaces: row.try_get(1).map_err(error)?,
        skipped_workspaces: row.try_get(2).map_err(error)?,
        cached_workspaces: row.try_get(3).map_err(error)?,
        started_at: row.try_get(4).map_err(error)?,
        finished_at: row.try_get(5).map_err(error)?,
        executions: row.try_get(6).map_err(error)?,
        completed: row.try_get(7).map_err(error)?,
        processing: row.try_get(8).map_err(error)?,
        queued: row.try_get(9).map_err(error)?,
        failed: row.try_get(10).map_err(error)?,
        canceled: row.try_get(11).map_err(error)?,
        canceling: row.try_get(12).map_err(error)?,
    }))
}

async fn batch_spec_diff_stat_async(
    pool: &PgPool,
    id: i64,
    actor: Actor,
) -> Result<(i64, i64), String> {
    let authz = repo_authz_conds(pool, actor).await?;
    let sql = format!(
        "SELECT COALESCE(SUM(diff_stat_added), 0) AS added, \
                COALESCE(SUM(diff_stat_deleted), 0) AS deleted \
         FROM changeset_specs \
         INNER JOIN repo ON repo.id = changeset_specs.repo_id \
         WHERE repo.deleted_at IS NULL AND batch_spec_id = $1 AND ({authz})"
    );
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(error)?;
    Ok((
        row.try_get(0).map_err(error)?,
        row.try_get(1).map_err(error)?,
    ))
}

async fn list_changeset_specs_async(
    pool: &PgPool,
    filter: &ChangesetSpecFilter,
) -> Result<ChangesetSpecPage, String> {
    let mut sql = format!(
        "SELECT {CHANGESET_SPEC_COLUMNS} FROM changeset_specs \
         INNER JOIN repo ON repo.id = changeset_specs.repo_id WHERE "
    );
    let mut binds: Vec<Bind> = Vec::new();
    let mut preds: Vec<String> = Vec::new();
    {
        let mut pred = String::from("changeset_specs.id >= ");
        push_bind(&mut pred, &mut binds, Bind::I64(filter.cursor));
        preds.push(pred);
    }
    preds.push("repo.deleted_at IS NULL".to_owned());
    if filter.batch_spec_id != 0 {
        let mut pred = String::from("changeset_specs.batch_spec_id = ");
        push_bind(&mut pred, &mut binds, Bind::I64(filter.batch_spec_id));
        preds.push(pred);
    }
    if !filter.rand_ids.is_empty() {
        let mut pred = String::from("changeset_specs.rand_id = ANY(");
        push_bind(
            &mut pred,
            &mut binds,
            Bind::TextArray(filter.rand_ids.clone()),
        );
        pred.push(')');
        preds.push(pred);
    }
    if !filter.ids.is_empty() {
        // `id = ANY(int8[])`; bind as text array and cast, avoiding a new bind
        // variant for i64 arrays.
        let mut pred = String::from("changeset_specs.id = ANY(");
        push_bind(
            &mut pred,
            &mut binds,
            Bind::TextArray(filter.ids.iter().map(i64::to_string).collect()),
        );
        pred.push_str("::int8[])");
        preds.push(pred);
    }
    match filter.kind.as_deref() {
        Some("EXISTING") => preds.push("changeset_specs.external_id IS NOT NULL".to_owned()),
        Some("BRANCH") => preds.push("changeset_specs.external_id IS NULL".to_owned()),
        Some("") | None => {}
        Some(_) => {}
    }
    sql.push_str(&preds.join("\n AND "));
    sql.push_str("\nORDER BY changeset_specs.id ASC");
    if filter.limit > 0 {
        sql.push_str(&format!("\nLIMIT {}", filter.limit + 1));
    }
    let rows = bind_query(&sql, &binds)
        .fetch_all(pool)
        .await
        .map_err(error)?;
    let mut page = ChangesetSpecPage {
        rows: rows
            .iter()
            .map(scan_changeset_spec)
            .collect::<Result<_, _>>()?,
        next: 0,
    };
    if filter.limit != 0 && page.rows.len() as i64 == filter.limit + 1 {
        page.next = page
            .rows
            .last()
            .map(|row| row.id)
            .expect("non-empty when len == limit + 1");
        page.rows.pop();
    }
    Ok(page)
}

async fn count_changeset_specs_async(
    pool: &PgPool,
    filter: &ChangesetSpecFilter,
) -> Result<i64, String> {
    // `changesetSpecConnectionResolver.TotalCount` only forwards BatchSpecID
    // and Type; limit/cursor/rand IDs/IDs are ignored.
    let mut sql = String::from(
        "SELECT COUNT(changeset_specs.id) FROM changeset_specs \
         INNER JOIN repo ON repo.id = changeset_specs.repo_id \
         WHERE repo.deleted_at IS NULL",
    );
    if filter.batch_spec_id != 0 {
        sql.push_str(&format!(
            " AND changeset_specs.batch_spec_id = {}",
            filter.batch_spec_id
        ));
    }
    match filter.kind.as_deref() {
        Some("EXISTING") => sql.push_str(" AND changeset_specs.external_id IS NOT NULL"),
        Some("BRANCH") => sql.push_str(" AND changeset_specs.external_id IS NULL"),
        _ => {}
    }
    let count: i64 = sqlx::query(&sql)
        .fetch_one(pool)
        .await
        .map_err(error)?
        .try_get(0)
        .map_err(error)?;
    Ok(count)
}

async fn get_changeset_spec_async(
    pool: &PgPool,
    id: i64,
    rand_id: &str,
) -> Result<Option<ChangesetSpecRow>, String> {
    let mut sql = format!(
        "SELECT {CHANGESET_SPEC_COLUMNS} FROM changeset_specs \
         INNER JOIN repo ON repo.id = changeset_specs.repo_id \
         WHERE repo.deleted_at IS NULL"
    );
    if id != 0 {
        sql.push_str(&format!(" AND changeset_specs.id = {id}"));
    }
    let mut binds: Vec<Bind> = Vec::new();
    if !rand_id.is_empty() {
        sql.push_str(" AND changeset_specs.rand_id = $1");
        binds.push(Bind::Text(rand_id.to_owned()));
    }
    sql.push_str(" LIMIT 1");
    let row = bind_query(&sql, &binds)
        .fetch_optional(pool)
        .await
        .map_err(error)?;
    row.as_ref().map(scan_changeset_spec).transpose()
}

/// Column list for `batch_spec_workspaces` + latest execution job. Kept in
/// one place so `get` and `list` scan identically.
const WORKSPACE_COLUMNS: &str = "batch_spec_workspaces.id, \
     batch_spec_workspaces.batch_spec_id::bigint, \
     batch_spec_workspaces.changeset_spec_ids::text, \
     batch_spec_workspaces.repo_id, batch_spec_workspaces.branch, \
     batch_spec_workspaces.commit, batch_spec_workspaces.path, \
     batch_spec_workspaces.file_matches, batch_spec_workspaces.only_fetch_workspace, \
     batch_spec_workspaces.unsupported, batch_spec_workspaces.ignored, \
     batch_spec_workspaces.skipped, batch_spec_workspaces.cached_result_found, \
     batch_spec_workspaces.step_cache_results::text, \
     batch_spec_workspaces.created_at, batch_spec_workspaces.updated_at, \
     ex.id, ex.state, ex.cancel, ex.failure_message, ex.started_at, ex.finished_at, \
     ex.created_at, ex.place_in_user_queue, ex.place_in_global_queue, \
     ex.execution_logs::text, ex.worker_hostname, ex.version";

const WORKSPACE_EXECUTION_JOIN: &str = "LEFT JOIN LATERAL ( \
     SELECT id, state, cancel, failure_message, started_at, finished_at, created_at, \
            place_in_user_queue, place_in_global_queue, execution_logs, worker_hostname, version \
     FROM batch_spec_workspace_execution_jobs_with_rank job \
     WHERE job.batch_spec_workspace_id = batch_spec_workspaces.id \
     ORDER BY job.id DESC LIMIT 1 \
 ) AS ex ON TRUE";

fn scan_workspace(row: &sqlx::postgres::PgRow) -> Result<BatchSpecWorkspaceRow, String> {
    let changeset_spec_ids_text: Option<String> = row.try_get(2).map_err(error)?;
    let changeset_spec_ids = changeset_spec_ids_text
        .as_deref()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        .and_then(|value| {
            value.as_object().map(|map| {
                map.keys()
                    .filter_map(|key| key.parse::<i64>().ok())
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default();
    let step_cache_results_text: Option<String> = row.try_get(13).map_err(error)?;
    let step_cache_results = step_cache_results_text
        .as_deref()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        .unwrap_or(serde_json::Value::Null);
    let execution_id: Option<i64> = row.try_get(16).map_err(error)?;
    let execution = match execution_id {
        Some(id) => Some(BatchSpecWorkspaceExecutionRow {
            id,
            state: row.try_get(17).map_err(error)?,
            cancel: row.try_get(18).map_err(error)?,
            failure_message: row.try_get(19).map_err(error)?,
            started_at: row.try_get(20).map_err(error)?,
            finished_at: row.try_get(21).map_err(error)?,
            created_at: row.try_get(22).map_err(error)?,
            place_in_user_queue: row.try_get(23).map_err(error)?,
            place_in_global_queue: row.try_get(24).map_err(error)?,
            execution_logs: row
                .try_get::<Option<String>, _>(25)
                .map_err(error)?
                .as_deref()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
                .unwrap_or(serde_json::Value::Null),
            worker_hostname: row.try_get::<Option<String>, _>(26).map_err(error)?.unwrap_or_default(),
            version: row.try_get(27).map_err(error)?,
        }),
        None => None,
    };
    Ok(BatchSpecWorkspaceRow {
        id: row.try_get(0).map_err(error)?,
        batch_spec_id: row.try_get(1).map_err(error)?,
        changeset_spec_ids,
        repo_id: row.try_get(3).map_err(error)?,
        branch: row.try_get(4).map_err(error)?,
        commit: row.try_get(5).map_err(error)?,
        path: row.try_get(6).map_err(error)?,
        file_matches: row
            .try_get::<Option<Vec<String>>, _>(7)
            .map_err(error)?
            .unwrap_or_default(),
        only_fetch_workspace: row.try_get(8).map_err(error)?,
        unsupported: row.try_get(9).map_err(error)?,
        ignored: row.try_get(10).map_err(error)?,
        skipped: row.try_get(11).map_err(error)?,
        cached_result_found: row.try_get(12).map_err(error)?,
        step_cache_results,
        execution,
        created_at: row.try_get(14).map_err(error)?,
        updated_at: row.try_get(15).map_err(error)?,
    })
}

async fn get_batch_spec_workspace_async(
    pool: &PgPool,
    id: i64,
) -> Result<Option<BatchSpecWorkspaceRow>, String> {
    let sql = format!(
        "SELECT {WORKSPACE_COLUMNS} FROM batch_spec_workspaces \
         INNER JOIN repo ON repo.id = batch_spec_workspaces.repo_id \
         {WORKSPACE_EXECUTION_JOIN} \
         WHERE repo.deleted_at IS NULL AND batch_spec_workspaces.id = $1 LIMIT 1"
    );
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(error)?;
    row.as_ref().map(scan_workspace).transpose()
}

fn workspace_predicates(
    filter: &BatchSpecWorkspaceFilter,
    binds: &mut Vec<Bind>,
    for_count: bool,
) -> Vec<String> {
    let mut preds = vec!["repo.deleted_at IS NULL".to_owned()];
    if filter.batch_spec_id != 0 {
        let mut pred = String::from("batch_spec_workspaces.batch_spec_id = ");
        push_bind(&mut pred, binds, Bind::I64(filter.batch_spec_id));
        preds.push(pred);
    }
    if !for_count && filter.cursor > 0 {
        let mut pred = String::from("batch_spec_workspaces.id >= ");
        push_bind(&mut pred, binds, Bind::I64(filter.cursor));
        preds.push(pred);
    }
    if let Some(state) = filter.state.as_deref() {
        if !state.is_empty() {
            let mut pred = String::from("batch_spec_workspace_execution_jobs.state = ");
            push_bind(&mut pred, binds, Bind::Text(state.to_owned()));
            preds.push(pred);
        }
    }
    if filter.only_without_execution_and_not_cached {
        preds.push(
            "batch_spec_workspace_execution_jobs.id IS NULL \
             AND NOT batch_spec_workspaces.cached_result_found"
                .to_owned(),
        );
    }
    if filter.only_cached_or_completed {
        preds.push(
            "(batch_spec_workspaces.cached_result_found OR \
             batch_spec_workspace_execution_jobs.state = 'completed')"
                .to_owned(),
        );
    }
    if let Some(cancel) = filter.cancel {
        let mut pred = String::from("batch_spec_workspace_execution_jobs.cancel = ");
        push_bind(&mut pred, binds, Bind::Bool(cancel));
        preds.push(pred);
    }
    if let Some(skipped) = filter.skipped {
        let mut pred = String::from("batch_spec_workspaces.skipped = ");
        push_bind(&mut pred, binds, Bind::Bool(skipped));
        preds.push(pred);
    }
    for term in &filter.search {
        let mut pred = String::from(if term.negated {
            "repo.name NOT ILIKE "
        } else {
            "repo.name ILIKE "
        });
        push_bind(
            &mut pred,
            binds,
            Bind::Text(format!("%{}%", term.value)),
        );
        preds.push(pred);
    }
    preds
}

fn workspace_join(filter: &BatchSpecWorkspaceFilter) -> &'static str {
    if filter.state.is_some()
        || filter.only_cached_or_completed
        || filter.only_without_execution_and_not_cached
        || filter.cancel.is_some()
    {
        "LEFT JOIN batch_spec_workspace_execution_jobs \
         ON batch_spec_workspace_execution_jobs.batch_spec_workspace_id = batch_spec_workspaces.id"
    } else {
        ""
    }
}

async fn list_batch_spec_workspaces_async(
    pool: &PgPool,
    filter: &BatchSpecWorkspaceFilter,
) -> Result<BatchSpecWorkspacePage, String> {
    let mut binds: Vec<Bind> = Vec::new();
    let preds = workspace_predicates(filter, &mut binds, false);
    let mut sql = format!(
        "SELECT {WORKSPACE_COLUMNS} FROM batch_spec_workspaces \
         INNER JOIN repo ON repo.id = batch_spec_workspaces.repo_id \
         {WORKSPACE_EXECUTION_JOIN} {} \
         WHERE {} ORDER BY batch_spec_workspaces.id ASC",
        workspace_join(filter),
        preds.join("\n AND ")
    );
    if filter.limit > 0 {
        sql.push_str(&format!("\nLIMIT {}", filter.limit + 1));
    }
    let rows = bind_query(&sql, &binds)
        .fetch_all(pool)
        .await
        .map_err(error)?;
    let mut page = BatchSpecWorkspacePage {
        rows: rows.iter().map(scan_workspace).collect::<Result<_, _>>()?,
        next: 0,
    };
    if filter.limit != 0 && page.rows.len() as i64 == filter.limit + 1 {
        page.next = page.rows.last().map(|row| row.id).unwrap_or(0);
        page.rows.pop();
    }
    Ok(page)
}

async fn count_batch_spec_workspaces_async(
    pool: &PgPool,
    filter: &BatchSpecWorkspaceFilter,
) -> Result<i64, String> {
    let mut binds: Vec<Bind> = Vec::new();
    let preds = workspace_predicates(filter, &mut binds, true);
    let sql = format!(
        "SELECT COUNT(1)::bigint FROM batch_spec_workspaces \
         INNER JOIN repo ON repo.id = batch_spec_workspaces.repo_id {} WHERE {}",
        workspace_join(filter),
        preds.join("\n AND ")
    );
    let row = bind_query(&sql, &binds)
        .fetch_one(pool)
        .await
        .map_err(error)?;
    row.try_get(0).map_err(error)
}

async fn get_batch_spec_resolution_job_async(
    pool: &PgPool,
    batch_spec_id: i64,
) -> Result<Option<BatchSpecResolutionJobRow>, String> {
    let row = sqlx::query(
        "SELECT id, batch_spec_id::bigint, state, failure_message, started_at, finished_at, \
                created_at, updated_at \
         FROM batch_spec_resolution_jobs WHERE batch_spec_id = $1 LIMIT 1",
    )
    .bind(batch_spec_id)
    .fetch_optional(pool)
    .await
    .map_err(error)?;
    Ok(row.map(|row| BatchSpecResolutionJobRow {
        id: row.try_get(0).map_err(error).unwrap_or_default(),
        batch_spec_id: row.try_get(1).map_err(error).unwrap_or_default(),
        state: row.try_get(2).map_err(error).unwrap_or_default(),
        failure_message: row.try_get(3).map_err(error).unwrap_or_default(),
        started_at: row.try_get(4).map_err(error).unwrap_or_default(),
        finished_at: row.try_get(5).map_err(error).unwrap_or_default(),
        created_at: row
            .try_get(6)
            .map_err(error)
            .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH),
        updated_at: row
            .try_get(7)
            .map_err(error)
            .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH),
    }))
}

async fn get_newest_batch_spec_async(
    pool: &PgPool,
    user_id: i32,
    namespace_user_id: i32,
    namespace_org_id: i32,
    name: &str,
) -> Result<Option<BatchSpecRow>, String> {
    let mut sql = format!(
        "SELECT {BATCH_SPEC_COLUMNS} FROM batch_specs \
         WHERE batch_specs.user_id = $1 AND batch_specs.spec->>'name' = $2"
    );
    let mut binds: Vec<Bind> = vec![Bind::I32(user_id), Bind::Text(name.to_owned())];
    if namespace_user_id != 0 {
        sql.push_str(" AND batch_specs.namespace_user_id = $3");
        binds.push(Bind::I32(namespace_user_id));
    } else {
        sql.push_str(" AND batch_specs.namespace_org_id = $3");
        binds.push(Bind::I32(namespace_org_id));
    }
    sql.push_str(" ORDER BY batch_specs.id DESC LIMIT 1");
    let row = bind_query(&sql, &binds)
        .fetch_optional(pool)
        .await
        .map_err(error)?;
    row.as_ref().map(scan_batch_spec).transpose()
}

async fn conflicting_head_refs_async(
    pool: &PgPool,
    batch_spec_id: i64,
) -> Result<Vec<(i32, String, i64)>, String> {
    let rows = sqlx::query(
        "SELECT repo_id, head_ref, COUNT(*) AS count FROM changeset_specs \
         WHERE batch_spec_id = $1 AND head_ref IS NOT NULL \
         GROUP BY repo_id, head_ref HAVING COUNT(*) > 1 \
         ORDER BY repo_id ASC, head_ref ASC",
    )
    .bind(batch_spec_id)
    .fetch_all(pool)
    .await
    .map_err(error)?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get(0).map_err(error)?,
                row.try_get(1).map_err(error)?,
                row.try_get(2).map_err(error)?,
            ))
        })
        .collect()
}

async fn batch_spec_execution_failure_messages_async(
    pool: &PgPool,
    batch_spec_id: i64,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query(
        "SELECT jobs.failure_message \
         FROM batch_spec_workspace_execution_jobs jobs \
         JOIN batch_spec_workspaces ws ON jobs.batch_spec_workspace_id = ws.id \
         WHERE ws.batch_spec_id = $1 \
           AND jobs.state IN ('errored', 'failed') \
           AND jobs.failure_message IS NOT NULL \
           AND jobs.cancel = FALSE \
         ORDER BY jobs.id ASC",
    )
    .bind(batch_spec_id)
    .fetch_all(pool)
    .await
    .map_err(error)?;
    rows.iter()
        .map(|row| row.try_get(0).map_err(error))
        .collect()
}

async fn create_batch_spec_async(
    pool: &PgPool,
    spec: &NewBatchSpec,
) -> Result<BatchSpecRow, String> {
    let spec_value: serde_json::Value = serde_json::from_str(&spec.spec)
        .map_err(|error| format!("invalid batch spec JSON: {error}"))?;
    let mut tx = pool.begin().await.map_err(error)?;
    let now = OffsetDateTime::now_utc();
    let row = sqlx::query(&format!(
        "INSERT INTO batch_specs \
             (rand_id, raw_spec, spec, namespace_user_id, namespace_org_id, user_id, \
              created_from_raw, allow_unsupported, allow_ignored, no_cache, batch_change_id, \
              created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, FALSE, FALSE, NULL, $7, $7) \
         RETURNING {BATCH_SPEC_COLUMNS}"
    ))
    .bind(&spec.rand_id)
    .bind(&spec.raw_spec)
    .bind(spec_value)
    .bind((spec.namespace_user_id != 0).then_some(spec.namespace_user_id))
    .bind((spec.namespace_org_id != 0).then_some(spec.namespace_org_id))
    .bind((spec.user_id != 0).then_some(spec.user_id))
    .bind(now)
    .fetch_one(&mut *tx)
    .await
    .map_err(error)?;
    let created = scan_batch_spec(&row)?;
    if !spec.changeset_spec_rand_ids.is_empty() {
        sqlx::query(
            "UPDATE changeset_specs SET batch_spec_id = $1 WHERE rand_id = ANY($2)",
        )
        .bind(created.id)
        .bind(spec.changeset_spec_rand_ids.clone())
        .execute(&mut *tx)
        .await
        .map_err(error)?;
    }
    tx.commit().await.map_err(error)?;
    Ok(created)
}

async fn create_changeset_specs_async(
    pool: &PgPool,
    specs: &[NewChangesetSpec],
) -> Result<Vec<ChangesetSpecRow>, String> {
    let mut tx = pool.begin().await.map_err(error)?;
    let now = OffsetDateTime::now_utc();
    let mut rows = Vec::with_capacity(specs.len());
    for spec in specs {
        // The column is `text`; the Go store writes the JSON encoding of
        // `batcheslib.PublishedValue` (`true` / `false` / `"draft"`).
        let published = spec
            .published
            .as_ref()
            .map(|value| serde_json::to_string(value))
            .transpose()
            .map_err(|error| format!("invalid PublishedValue: {error}"))?;
        let row = sqlx::query(&format!(
            "INSERT INTO changeset_specs \
                 (rand_id, batch_spec_id, repo_id, user_id, diff_stat_added, \
                  diff_stat_deleted, created_at, updated_at, fork_namespace, external_id, \
                  head_ref, title, base_rev, base_ref, body, published, diff, commit_message, \
                  commit_author_name, commit_author_email, type) \
             VALUES ($1, NULL, $2, $3, $4, $5, $6, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                     $15, $16, $17, $18, $19) \
             RETURNING {CHANGESET_SPEC_COLUMNS}"
        ))
        .bind(&spec.rand_id)
        .bind(spec.repo_id)
        .bind((spec.user_id != 0).then_some(spec.user_id))
        .bind(spec.diff_stat_added)
        .bind(spec.diff_stat_deleted)
        .bind(now)
        .bind(&spec.fork_namespace)
        .bind(&spec.external_id)
        .bind(&spec.head_ref)
        .bind(&spec.title)
        .bind(&spec.base_rev)
        .bind(&spec.base_ref)
        .bind(&spec.body)
        .bind(published)
        .bind(&spec.diff)
        .bind(&spec.commit_message)
        .bind(&spec.commit_author_name)
        .bind(&spec.commit_author_email)
        .bind(&spec.kind)
        .fetch_one(&mut *tx)
        .await
        .map_err(error)?;
        rows.push(scan_changeset_spec(&row)?);
    }
    tx.commit().await.map_err(error)?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Trait impl
// ---------------------------------------------------------------------------

impl BatchesSpecsDatabase for PgBatchesGraphqlStore {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    fn create_batch_spec(&self, spec: &NewBatchSpec) -> Result<BatchSpecRow, String> {
        block_on(create_batch_spec_async(self.pool(), spec))
    }

    fn get_batch_spec(
        &self,
        id: i64,
        rand_id: &str,
        exclude_created_from_raw_not_owned_by_user: i32,
    ) -> Result<Option<BatchSpecRow>, String> {
        block_on(get_batch_spec_async(
            self.pool(),
            id,
            rand_id,
            exclude_created_from_raw_not_owned_by_user,
        ))
    }

    fn list_batch_specs(&self, filter: &BatchSpecFilter) -> Result<BatchSpecPage, String> {
        block_on(list_batch_specs_async(self.pool(), filter))
    }

    fn count_batch_specs(&self, filter: &BatchSpecFilter) -> Result<i64, String> {
        block_on(count_batch_specs_async(self.pool(), filter))
    }

    fn batch_spec_stats(&self, id: i64) -> Result<Option<BatchSpecStats>, String> {
        block_on(batch_spec_stats_async(self.pool(), id))
    }

    fn batch_spec_diff_stat(&self, id: i64, actor: Actor) -> Result<(i64, i64), String> {
        block_on(batch_spec_diff_stat_async(self.pool(), id, actor))
    }

    fn batch_spec_resolution_failure(&self, id: i64) -> Result<Option<String>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT failure_message FROM batch_spec_resolution_jobs \
                 WHERE batch_spec_id = $1 LIMIT 1",
            )
            .bind(id)
            .fetch_optional(self.pool()),
        )
        .map_err(error)?;
        match row {
            Some(row) => row.try_get(0).map_err(error),
            None => Ok(None),
        }
    }

    fn batch_spec_execution_failure_messages(&self, id: i64) -> Result<Vec<String>, String> {
        block_on(batch_spec_execution_failure_messages_async(self.pool(), id))
    }

    fn list_changeset_specs(
        &self,
        filter: &ChangesetSpecFilter,
    ) -> Result<ChangesetSpecPage, String> {
        block_on(list_changeset_specs_async(self.pool(), filter))
    }

    fn count_changeset_specs(&self, filter: &ChangesetSpecFilter) -> Result<i64, String> {
        block_on(count_changeset_specs_async(self.pool(), filter))
    }

    fn get_changeset_spec(
        &self,
        id: i64,
        rand_id: &str,
    ) -> Result<Option<ChangesetSpecRow>, String> {
        block_on(get_changeset_spec_async(self.pool(), id, rand_id))
    }

    fn create_changeset_specs(
        &self,
        specs: &[NewChangesetSpec],
    ) -> Result<Vec<ChangesetSpecRow>, String> {
        block_on(create_changeset_specs_async(self.pool(), specs))
    }

    fn get_batch_spec_workspace(
        &self,
        id: i64,
    ) -> Result<Option<BatchSpecWorkspaceRow>, String> {
        block_on(get_batch_spec_workspace_async(self.pool(), id))
    }

    fn list_batch_spec_workspaces(
        &self,
        filter: &BatchSpecWorkspaceFilter,
    ) -> Result<BatchSpecWorkspacePage, String> {
        block_on(list_batch_spec_workspaces_async(self.pool(), filter))
    }

    fn count_batch_spec_workspaces(
        &self,
        filter: &BatchSpecWorkspaceFilter,
    ) -> Result<i64, String> {
        block_on(count_batch_spec_workspaces_async(self.pool(), filter))
    }

    fn get_batch_spec_resolution_job(
        &self,
        batch_spec_id: i64,
    ) -> Result<Option<BatchSpecResolutionJobRow>, String> {
        block_on(get_batch_spec_resolution_job_async(
            self.pool(),
            batch_spec_id,
        ))
    }

    fn get_newest_batch_spec(
        &self,
        user_id: i32,
        namespace_user_id: i32,
        namespace_org_id: i32,
        name: &str,
    ) -> Result<Option<BatchSpecRow>, String> {
        block_on(get_newest_batch_spec_async(
            self.pool(),
            user_id,
            namespace_user_id,
            namespace_org_id,
            name,
        ))
    }

    fn conflicting_head_refs(&self, batch_spec_id: i64) -> Result<Vec<(i32, String, i64)>, String> {
        block_on(conflicting_head_refs_async(self.pool(), batch_spec_id))
    }
}
