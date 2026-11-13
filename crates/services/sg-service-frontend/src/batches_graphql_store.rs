#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::batches_graphql::BatchesGraphqlDatabase`] (M9.24c).
//!
//! Ports the read queries of `internal/batches/store/{batch_changes,changesets}.go`
//! plus the repository-permission predicate from
//! `internal/database/repos_perm.go` (`AuthzQueryConds`) and the
//! `users`/`orgs`/`org_members` lookups the resolvers need.
//!
//! No table, column or index is added. The synchronous trait is bridged to
//! `sqlx` with the same `block_in_place` helper as the other store adapters.

use std::collections::HashMap;
use std::future::Future;

use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::{PgPool, Postgres, Row};
use time::OffsetDateTime;

use crate::batches_graphql::{
    BatchChangeFilter, BatchChangeLookup, BatchChangePage, BatchChangeRow, BatchesGraphqlDatabase,
    ChangesetFilter, ChangesetPage, ChangesetRow, ChangesetSyncDataRow, TextSearchTerm,
    text_search_pattern,
};
use crate::graphql_resolvers::{OrgData, RepositoryData, UserData};
use sg_graphql::exec::Actor;

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("batches blocking runtime")
            .block_on(future),
    }
}

/// `batchChangeColumns` (`batch_changes.go:42`), rendered with `COALESCE`
/// for the nullable columns the GraphQL surface reads.
const BATCH_CHANGE_COLUMNS: &str = "batch_changes.id, batch_changes.name, \
     COALESCE(batch_changes.description, ''), COALESCE(batch_changes.creator_id, 0), \
     COALESCE(batch_changes.last_applier_id, 0), batch_changes.last_applied_at, \
     COALESCE(batch_changes.namespace_user_id, 0), COALESCE(batch_changes.namespace_org_id, 0), \
     batch_changes.created_at, batch_changes.updated_at, batch_changes.closed_at, \
     batch_changes.batch_spec_id";

/// `ChangesetColumns` (`changesets.go`), nullable columns coalesced.
const CHANGESET_COLUMNS: &str = "changesets.id, changesets.repo_id, changesets.created_at, \
     changesets.updated_at, COALESCE(changesets.metadata, '{}'::jsonb), \
     COALESCE(changesets.external_id, ''), changesets.external_service_type, \
     changesets.external_deleted_at, changesets.external_updated_at, \
     COALESCE(changesets.external_state, ''), COALESCE(changesets.external_review_state, ''), \
     COALESCE(changesets.external_check_state, ''), changesets.diff_stat_added, \
     changesets.diff_stat_deleted, COALESCE(changesets.owned_by_batch_change_id, 0), \
     COALESCE(changesets.current_spec_id, 0), COALESCE(changesets.publication_state, ''), \
     changesets.computed_state, changesets.failure_message, \
     changesets.previous_failure_message, changesets.syncer_error, \
     COALESCE(changesets.external_fork_name::text, ''), \
     COALESCE(changesets.external_fork_namespace::text, '')";

#[allow(unused_assignments)]
fn scan_batch_change(row: &PgRow) -> Result<BatchChangeRow, sqlx::Error> {
    let mut index = 0usize;
    macro_rules! next {
        ($ty:ty) => {{
            let value: $ty = row.try_get(index)?;
            index += 1;
            value
        }};
    }
    Ok(BatchChangeRow {
        id: next!(i64),
        name: next!(String),
        description: next!(String),
        creator_id: next!(i32),
        // `batch_changes.last_applier_id` is BIGINT (Go scans it through
        // database/sql into int32); the COALESCE keeps the bigint type.
        last_applier_id: next!(i64) as i32,
        last_applied_at: next!(Option<OffsetDateTime>),
        namespace_user_id: next!(i32),
        namespace_org_id: next!(i32),
        created_at: next!(OffsetDateTime),
        updated_at: next!(OffsetDateTime),
        closed_at: next!(Option<OffsetDateTime>),
        batch_spec_id: next!(i64),
    })
}

#[allow(unused_assignments)]
fn scan_changeset(row: &PgRow) -> Result<ChangesetRow, sqlx::Error> {
    let mut index = 0usize;
    macro_rules! next {
        ($ty:ty) => {{
            let value: $ty = row.try_get(index)?;
            index += 1;
            value
        }};
    }
    Ok(ChangesetRow {
        id: next!(i64),
        repo_id: next!(i32),
        created_at: next!(OffsetDateTime),
        updated_at: next!(OffsetDateTime),
        metadata: next!(serde_json::Value),
        external_id: next!(String),
        external_service_type: next!(String),
        external_deleted_at: next!(Option<OffsetDateTime>),
        external_updated_at: next!(Option<OffsetDateTime>),
        external_state: next!(String),
        external_review_state: next!(String),
        external_check_state: next!(String),
        diff_stat_added: next!(Option<i32>),
        diff_stat_deleted: next!(Option<i32>),
        owned_by_batch_change_id: next!(i64),
        current_spec_id: next!(i64),
        publication_state: next!(String),
        computed_state: next!(String),
        failure_message: next!(Option<String>),
        previous_failure_message: next!(Option<String>),
        syncer_error: next!(Option<String>),
        external_fork_name: next!(String),
        external_fork_namespace: next!(String),
    })
}

#[allow(unused_assignments)]
fn scan_repo(row: &PgRow) -> Result<RepositoryData, sqlx::Error> {
    let mut index = 0usize;
    macro_rules! next {
        ($ty:ty) => {{
            let value: $ty = row.try_get(index)?;
            index += 1;
            value
        }};
    }
    Ok(RepositoryData {
        id: next!(i32),
        name: next!(String),
        uri: next!(String),
        description: next!(String),
        fork: next!(bool),
        archived: next!(bool),
        private: next!(bool),
        stars: next!(i32),
        external_id: next!(String),
        external_service_type: next!(String),
        external_service_id: next!(String),
    })
}

fn error(error: sqlx::Error) -> String {
    error.to_string()
}

// ---------------------------------------------------------------------------
// Bind list
// ---------------------------------------------------------------------------

enum Bind {
    I32(i32),
    I64(i64),
    Text(String),
    TextArray(Vec<String>),
    I32Array(Vec<i32>),
}

#[derive(Default)]
struct Binds {
    values: Vec<Bind>,
}

impl Binds {
    /// Push a value and return its positional placeholder.
    fn push(&mut self, value: Bind) -> String {
        self.values.push(value);
        format!("${}", self.values.len())
    }

    fn i32(&mut self, value: i32) -> String {
        self.push(Bind::I32(value))
    }

    fn i64(&mut self, value: i64) -> String {
        self.push(Bind::I64(value))
    }

    fn text(&mut self, value: impl Into<String>) -> String {
        self.push(Bind::Text(value.into()))
    }

    fn text_array(&mut self, value: Vec<String>) -> String {
        self.push(Bind::TextArray(value))
    }

    fn i32_array(&mut self, value: Vec<i32>) -> String {
        self.push(Bind::I32Array(value))
    }

    fn apply<'q>(self, mut query: Query<'q, Postgres, sqlx::postgres::PgArguments>) -> Query<'q, Postgres, sqlx::postgres::PgArguments> {
        for value in self.values {
            query = match value {
                Bind::I32(value) => query.bind(value),
                Bind::I64(value) => query.bind(value),
                Bind::Text(value) => query.bind(value),
                Bind::TextArray(value) => query.bind(value),
                Bind::I32Array(value) => query.bind(value),
            };
        }
        query
    }
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

pub struct PgBatchesGraphqlStore {
    pool: PgPool,
    /// `conf.BatchChangesEnabled`. Only the differential harness disables it
    /// to exercise `ErrBatchChangesDisabled`; production keeps the default.
    batch_changes_enabled: bool,
}

impl PgBatchesGraphqlStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            batch_changes_enabled: true,
        }
    }

    /// Test seam for the `batchChanges.enabled: false` site configuration.
    pub fn with_batch_changes_enabled(mut self, enabled: bool) -> Self {
        self.batch_changes_enabled = enabled;
        self
    }

    /// `AuthzQueryConds` for the actor (`repos_perm.go:25-133`). The returned
    /// fragment references the `repo` table; `TRUE` bypasses authz for
    /// internal actors and site admins (`AuthzEnforceForSiteAdmins` defaults
    /// to false).
    fn authz_conds(&self, binds: &mut Binds, actor: Actor) -> Result<String, String> {
        let bypass = actor.is_internal()
            || block_on(sg_db::users::get_site_admin(
                &self.pool,
                actor.uid().unwrap_or(0),
            ))
            .map_err(error)?
            == Some(true);
        if bypass {
            return Ok("TRUE".to_owned());
        }
        let uid = binds.i32(actor.uid().unwrap_or(0));
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

    fn is_site_admin(&self, actor: Actor) -> Result<Option<bool>, sqlx::Error> {
        match actor {
            Actor::Internal => Ok(Some(true)),
            Actor::Anonymous => Ok(None),
            Actor::User { uid } => block_on(sg_db::users::get_site_admin(&self.pool, uid)),
        }
    }
}

fn batch_change_predicates(
    filter: &BatchChangeFilter,
    binds: &mut Binds,
    authz: &str,
) -> Vec<String> {
    let mut preds = vec![
        "namespace_user.deleted_at IS NULL".to_owned(),
        "namespace_org.deleted_at IS NULL".to_owned(),
    ];

    if filter.id != 0 {
        let id = binds.i64(filter.id);
        preds.push(format!("batch_changes.id = {id}"));
    }
    if let Some(name) = &filter.name {
        let name = binds.text(name.clone());
        preds.push(format!("batch_changes.name = {name}"));
    }
    if filter.cursor != 0 {
        let cursor = binds.i64(filter.cursor);
        preds.push(format!("batch_changes.id <= {cursor}"));
    }
    if filter.changeset_id != 0 {
        let changeset_id = binds.i64(filter.changeset_id);
        preds.push(format!("changesets.id = {changeset_id}"));
    }
    if !filter.states.is_empty() {
        let mut conditions = Vec::new();
        for state in &filter.states {
            match state.as_str() {
                "OPEN" => conditions.push(
                    "batch_changes.closed_at IS NULL AND batch_changes.last_applied_at IS NOT NULL"
                        .to_owned(),
                ),
                "CLOSED" => conditions.push("batch_changes.closed_at IS NOT NULL".to_owned()),
                "DRAFT" => conditions.push(
                    "batch_changes.last_applied_at IS NULL AND batch_changes.closed_at IS NULL"
                        .to_owned(),
                ),
                _ => {}
            }
        }
        if !conditions.is_empty() {
            preds.push(format!("({})", conditions.join(" OR ")));
        }
    }
    if filter.only_administered_by_user_id != 0 {
        let uid = binds.i32(filter.only_administered_by_user_id);
        preds.push(format!(
            "(batch_changes.namespace_user_id = {uid} OR (EXISTS (SELECT 1 FROM org_members \
                WHERE org_id = batch_changes.namespace_org_id AND user_id = {uid} \
                AND org_id <> 0)))"
        ));
    }
    if filter.namespace_user_id != 0 {
        let user_id = binds.i32(filter.namespace_user_id);
        preds.push(format!("batch_changes.namespace_user_id = {user_id}"));
        if filter.exclude_drafts_not_owned_by_user_id != 0
            && filter.exclude_drafts_not_owned_by_user_id != filter.namespace_user_id
        {
            preds.push("batch_changes.last_applied_at IS NOT NULL".to_owned());
        }
    } else if filter.exclude_drafts_not_owned_by_user_id != 0 {
        let uid = binds.i32(filter.exclude_drafts_not_owned_by_user_id);
        preds.push(format!(
            "(batch_changes.last_applied_at IS NOT NULL OR EXISTS (SELECT 1 FROM batch_specs \
                WHERE batch_specs.id = batch_changes.batch_spec_id AND batch_specs.user_id = {uid}))"
        ));
    }
    if filter.namespace_org_id != 0 {
        let org_id = binds.i32(filter.namespace_org_id);
        preds.push(format!("batch_changes.namespace_org_id = {org_id}"));
    }
    if filter.repo_id != 0 {
        let repo_id = binds.i32(filter.repo_id);
        preds.push(format!(
            "EXISTS(SELECT * FROM changesets INNER JOIN repo ON changesets.repo_id = repo.id \
                WHERE changesets.batch_change_ids ? batch_changes.id::TEXT \
                AND changesets.repo_id = {repo_id} AND repo.deleted_at IS NULL AND {authz})"
        ));
    }
    preds
}

fn batch_change_joins(filter: &BatchChangeFilter) -> String {
    let mut joins = String::from(
        "LEFT JOIN users namespace_user ON batch_changes.namespace_user_id = namespace_user.id \
         LEFT JOIN orgs namespace_org ON batch_changes.namespace_org_id = namespace_org.id",
    );
    if filter.changeset_id != 0 {
        joins.push_str(
            " INNER JOIN changesets ON changesets.batch_change_ids ? batch_changes.id::TEXT",
        );
    }
    joins
}

impl BatchesGraphqlDatabase for PgBatchesGraphqlStore {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    fn batch_changes_enabled(&self) -> bool {
        self.batch_changes_enabled
    }

    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String> {
        self.is_site_admin(actor).map_err(error)
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let record = block_on(sg_db::users::get_session_user(&self.pool, id)).map_err(error)?;
        Ok(record.map(crate::user_store::to_user_data))
    }

    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String> {
        let record = block_on(sg_db::orgs::find_org_by_id(&self.pool, id)).map_err(error)?;
        Ok(record.map(|record| OrgData {
            id: record.id,
            name: record.name,
            display_name: record.display_name,
            created_at: record.created_at,
            viewer_can_administer: false,
            viewer_is_member: false,
        }))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let row = block_on(
            sqlx::query("SELECT 1 FROM org_members WHERE org_id = $1 AND user_id = $2 LIMIT 1")
                .bind(org_id)
                .bind(user_id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        Ok(row.is_some())
    }

    fn list_batch_changes(
        &self,
        filter: &BatchChangeFilter,
        actor: Actor,
    ) -> Result<BatchChangePage, String> {
        let mut binds = Binds::default();
        let authz = self.authz_conds(&mut binds, actor)?;
        let joins = batch_change_joins(filter);
        let preds = batch_change_predicates(filter, &mut binds, &authz);
        let mut sql = format!(
            "SELECT {BATCH_CHANGE_COLUMNS} FROM batch_changes {joins} WHERE {} \
             ORDER BY batch_changes.id DESC",
            preds.join(" AND ")
        );
        let db_limit = if filter.limit > 0 {
            filter.limit + 1
        } else {
            0
        };
        if db_limit > 0 {
            sql.push_str(&format!(" LIMIT {db_limit}"));
        }
        let rows = block_on(binds.apply(sqlx::query(&sql)).fetch_all(&self.pool)).map_err(error)?;
        let mut page = BatchChangePage {
            rows: rows
                .iter()
                .map(scan_batch_change)
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)?,
            next: 0,
        };
        if filter.limit != 0 && page.rows.len() as i64 == db_limit {
            page.next = page.rows.last().map(|row| row.id).unwrap_or(0);
            page.rows.pop();
        }
        Ok(page)
    }

    fn count_batch_changes(
        &self,
        filter: &BatchChangeFilter,
        actor: Actor,
    ) -> Result<i64, String> {
        // `CountBatchChangesOpts` has no cursor: TotalCount counts the whole
        // filtered set, independent of the page being requested.
        let mut filter = filter.clone();
        filter.cursor = 0;
        let filter = &filter;
        let mut binds = Binds::default();
        let authz = self.authz_conds(&mut binds, actor)?;
        let joins = batch_change_joins(filter);
        let preds = batch_change_predicates(filter, &mut binds, &authz);
        let sql = format!(
            "SELECT COUNT(batch_changes.id) FROM batch_changes {joins} WHERE {}",
            preds.join(" AND ")
        );
        block_on(
            binds
                .apply(sqlx::query(&sql))
                .fetch_one(&self.pool),
        )
        .map_err(error)?
        .try_get::<i64, _>(0)
        .map_err(error)
    }

    fn get_batch_change(&self, filter: &BatchChangeFilter) -> Result<BatchChangeLookup, String> {
        let mut binds = Binds::default();
        let joins = batch_change_joins(filter);
        // `getBatchChangeQuery` (`batch_changes.go:434-467`) applies only the
        // ID/batch-spec/namespace/name predicates; it deliberately keeps rows
        // whose namespace was deleted so the caller can report
        // `ErrDeletedNamespace`.
        let mut preds = Vec::new();
        if filter.id != 0 {
            let id = binds.i64(filter.id);
            preds.push(format!("batch_changes.id = {id}"));
        }
        if let Some(name) = filter.name.as_deref().filter(|name| !name.is_empty()) {
            let name = binds.text(name.to_owned());
            preds.push(format!("batch_changes.name = {name}"));
        }
        if filter.namespace_user_id != 0 {
            let user_id = binds.i32(filter.namespace_user_id);
            preds.push(format!("batch_changes.namespace_user_id = {user_id}"));
        }
        if filter.namespace_org_id != 0 {
            let org_id = binds.i32(filter.namespace_org_id);
            preds.push(format!("batch_changes.namespace_org_id = {org_id}"));
        }
        if preds.is_empty() {
            preds.push("TRUE".to_owned());
        }
        let sql = format!(
            "SELECT {BATCH_CHANGE_COLUMNS}, namespace_user.deleted_at AS user_deleted_at, \
             namespace_org.deleted_at AS org_deleted_at FROM batch_changes {joins} WHERE {} \
             ORDER BY batch_changes.id DESC LIMIT 1",
            preds.join(" AND ")
        );
        let row = block_on(binds.apply(sqlx::query(&sql)).fetch_optional(&self.pool))
            .map_err(error)?;
        let Some(row) = row else {
            return Ok(BatchChangeLookup::NotFound);
        };
        let user_deleted_at: Option<OffsetDateTime> =
            row.try_get("user_deleted_at").map_err(error)?;
        let org_deleted_at: Option<OffsetDateTime> =
            row.try_get("org_deleted_at").map_err(error)?;
        if user_deleted_at.is_some() || org_deleted_at.is_some() {
            return Ok(BatchChangeLookup::DeletedNamespace);
        }
        Ok(BatchChangeLookup::Found(scan_batch_change(&row).map_err(error)?))
    }

    fn list_changesets(
        &self,
        filter: &ChangesetFilter,
        actor: Actor,
    ) -> Result<ChangesetPage, String> {
        let mut binds = Binds::default();
        let mut sql = String::from("SELECT ");
        sql.push_str(CHANGESET_COLUMNS);
        sql.push_str(" FROM changesets INNER JOIN repo ON repo.id = changesets.repo_id");
        let mut preds = vec![
            "changesets.id >= 0".to_owned(),
            "repo.deleted_at IS NULL".to_owned(),
        ];
        if !filter.text_search.is_empty() {
            sql.push_str(
                " LEFT JOIN changeset_specs ON changesets.current_spec_id = changeset_specs.id",
            );
        }
        let cursor = binds.i64(filter.cursor);
        preds[0] = format!("changesets.id >= {cursor}");

        if filter.batch_change_id != 0 {
            let batch_change_id = binds.text(filter.batch_change_id.to_string());
            preds.push(format!("changesets.batch_change_ids ? {batch_change_id}"));
            if filter.only_archived {
                preds.push(archived_in_batch_change(&batch_change_id));
            } else if !filter.include_archived {
                preds.push(format!(
                    "NOT ({})",
                    archived_in_batch_change(&batch_change_id)
                ));
            }
        }
        if let Some(state) = &filter.publication_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.publication_state = {state}"));
        }
        if !filter.states.is_empty() {
            let states = binds.text_array(filter.states.clone());
            preds.push(format!("changesets.computed_state = ANY({states})"));
        }
        if let Some(state) = &filter.external_review_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.external_review_state = {state}"));
        }
        if let Some(state) = &filter.external_check_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.external_check_state = {state}"));
        }
        if filter.owned_by_batch_change_id != 0 {
            let id = binds.i64(filter.owned_by_batch_change_id);
            preds.push(format!("changesets.owned_by_batch_change_id = {id}"));
        }
        if filter.enforce_authz {
            let authz = self.authz_conds(&mut binds, actor)?;
            preds.push(authz);
        }
        if !filter.repo_ids.is_empty() {
            let ids = binds.i32_array(filter.repo_ids.clone());
            preds.push(format!("repo.id = ANY({ids})"));
        }
        for term in &filter.text_search {
            preds.push(text_search_clause(term, &mut binds));
        }
        sql.push_str(" WHERE ");
        sql.push_str(&preds.join(" AND "));
        sql.push_str(" ORDER BY changesets.id ASC");
        let db_limit = if filter.limit > 0 {
            filter.limit + 1
        } else {
            0
        };
        if db_limit > 0 {
            sql.push_str(&format!(" LIMIT {db_limit}"));
        }
        let rows = block_on(binds.apply(sqlx::query(&sql)).fetch_all(&self.pool)).map_err(error)?;
        let mut page = ChangesetPage {
            rows: rows
                .iter()
                .map(scan_changeset)
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)?,
            next: 0,
        };
        if filter.limit != 0 && page.rows.len() as i64 == db_limit {
            page.next = page.rows.last().map(|row| row.id).unwrap_or(0);
            page.rows.pop();
        }
        Ok(page)
    }

    fn count_changesets(
        &self,
        filter: &ChangesetFilter,
        actor: Actor,
    ) -> Result<i64, String> {
        let mut binds = Binds::default();
        let mut sql =
            String::from("SELECT COUNT(changesets.id) FROM changesets INNER JOIN repo ON repo.id = changesets.repo_id");
        let mut preds = vec!["repo.deleted_at IS NULL".to_owned()];
        if !filter.text_search.is_empty() {
            sql.push_str(
                " LEFT JOIN changeset_specs ON changesets.current_spec_id = changeset_specs.id",
            );
        }
        if filter.batch_change_id != 0 {
            let batch_change_id = binds.text(filter.batch_change_id.to_string());
            preds.push(format!("changesets.batch_change_ids ? {batch_change_id}"));
            if filter.only_archived {
                preds.push(archived_in_batch_change(&batch_change_id));
            } else if !filter.include_archived {
                preds.push(format!(
                    "NOT ({})",
                    archived_in_batch_change(&batch_change_id)
                ));
            }
        }
        if let Some(state) = &filter.publication_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.publication_state = {state}"));
        }
        if !filter.states.is_empty() {
            let states = binds.text_array(filter.states.clone());
            preds.push(format!("changesets.computed_state = ANY({states})"));
        }
        if let Some(state) = &filter.external_review_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.external_review_state = {state}"));
        }
        if let Some(state) = &filter.external_check_state {
            let state = binds.text(state.clone());
            preds.push(format!("changesets.external_check_state = {state}"));
        }
        if filter.owned_by_batch_change_id != 0 {
            let id = binds.i64(filter.owned_by_batch_change_id);
            preds.push(format!("changesets.owned_by_batch_change_id = {id}"));
        }
        if filter.enforce_authz {
            let authz = self.authz_conds(&mut binds, actor)?;
            preds.push(authz);
        }
        if !filter.repo_ids.is_empty() {
            let ids = binds.i32_array(filter.repo_ids.clone());
            preds.push(format!("repo.id = ANY({ids})"));
        }
        for term in &filter.text_search {
            preds.push(text_search_clause(term, &mut binds));
        }
        sql.push_str(" WHERE ");
        sql.push_str(&preds.join(" AND "));
        block_on(binds.apply(sqlx::query(&sql)).fetch_one(&self.pool))
            .map_err(error)?
            .try_get::<i64, _>(0)
            .map_err(error)
    }

    fn get_changeset(&self, id: i64) -> Result<Option<ChangesetRow>, String> {
        let sql = format!(
            "SELECT {CHANGESET_COLUMNS} FROM changesets \
             INNER JOIN repo ON repo.id = changesets.repo_id \
             WHERE repo.deleted_at IS NULL AND changesets.id = $1 LIMIT 1"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_changeset).transpose().map_err(error)
    }

    fn changeset_sync_data(
        &self,
        changeset_ids: &[i64],
    ) -> Result<Vec<ChangesetSyncDataRow>, String> {
        if changeset_ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = "SELECT changesets.id, changesets.updated_at, \
             max(ce.updated_at) AS latest_event, changesets.external_updated_at \
             FROM changesets \
             LEFT JOIN changeset_events ce ON changesets.id = ce.changeset_id \
             JOIN batch_changes ON changesets.batch_change_ids ? batch_changes.id::TEXT \
             JOIN repo r ON changesets.repo_id = r.id \
             WHERE batch_changes.closed_at IS NULL AND r.deleted_at IS NULL \
             AND changesets.publication_state = 'PUBLISHED' \
             AND changesets.reconciler_state = 'completed' \
             AND changesets.id = ANY($1) \
             GROUP BY changesets.id, r.id \
             ORDER BY changesets.id ASC";
        let rows = block_on(
            sqlx::query(sql)
                .bind(changeset_ids.to_vec())
                .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter()
            .map(|row| {
                Ok(ChangesetSyncDataRow {
                    changeset_id: row.try_get("id")?,
                    updated_at: row.try_get("updated_at")?,
                    latest_event: row.try_get("latest_event")?,
                    external_updated_at: row.try_get("external_updated_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(error)
    }

    fn repos_by_ids(
        &self,
        repo_ids: &[i32],
        actor: Actor,
    ) -> Result<HashMap<i32, RepositoryData>, String> {
        if repo_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut binds = Binds::default();
        let authz = self.authz_conds(&mut binds, actor)?;
        let ids = binds.i32_array(repo_ids.to_vec());
        let sql = format!(
            "SELECT repo.id, repo.name::text, COALESCE(repo.uri::text, ''), \
             COALESCE(repo.description, ''), repo.fork, repo.archived, repo.private, repo.stars, \
             COALESCE(repo.external_id, ''), COALESCE(repo.external_service_type, ''), \
             COALESCE(repo.external_service_id, '') \
             FROM repo WHERE repo.id = ANY({ids}) AND repo.deleted_at IS NULL AND {authz}"
        );
        let rows = block_on(binds.apply(sqlx::query(&sql)).fetch_all(&self.pool)).map_err(error)?;
        let mut repos = HashMap::with_capacity(rows.len());
        for row in &rows {
            let repo = scan_repo(row).map_err(error)?;
            repos.insert(repo.id, repo);
        }
        Ok(repos)
    }
}

/// `archivedInBatchChange` (`changesets.go:...`).
fn archived_in_batch_change(batch_change_id: &str) -> String {
    format!(
        "(COALESCE((changesets.batch_change_ids->{batch_change_id}->>'isArchived')::bool, false) \
         OR COALESCE((changesets.batch_change_ids->{batch_change_id}->>'archive')::bool, false))"
    )
}

/// `textSearchTermToClause` (`text_search.go:22-55`).
fn text_search_clause(term: &TextSearchTerm, binds: &mut Binds) -> String {
    let pattern = text_search_pattern(term);
    let first = binds.text(pattern.clone());
    let second = binds.text(pattern);
    let operator = if term.not { "!~*" } else { "~*" };
    let boolean = if term.not { "AND" } else { "OR" };
    format!(
        "(COALESCE(changesets.external_title, changeset_specs.title) {operator} {first} \
         {boolean} repo.name::text {operator} {second})"
    )
}
