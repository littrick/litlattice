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
    /// `conf.BatchChangesRestrictToAdmins`. Test seam only; production keeps
    /// the default (`false`).
    batch_changes_restricted_to_admins: bool,
}

impl PgBatchesGraphqlStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            batch_changes_enabled: true,
            batch_changes_restricted_to_admins: false,
        }
    }

    /// Test seam for the `batchChanges.enabled: false` site configuration.
    pub fn with_batch_changes_enabled(mut self, enabled: bool) -> Self {
        self.batch_changes_enabled = enabled;
        self
    }

    /// Test seam for `batchChanges.restrictToAdmins: true`.
    pub fn with_batch_changes_restricted_to_admins(mut self, restricted: bool) -> Self {
        self.batch_changes_restricted_to_admins = restricted;
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

// ---------------------------------------------------------------------------
// M9.25b async write helpers
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::time::Duration;

use crate::batches_graphql::{
    ApplyBatchChangeRequest, BatchSpecRow, BulkOperationRow, NamespaceIds, PublishedValue,
};
use crate::batches_mutations::{
    DELETED_NAMESPACE, INVALID_BATCH_CHANGE_NAME, MATCHING_BATCH_CHANGE_EXISTS,
    MISSING_BATCH_CHANGES_WRITE, MUST_BE_SITE_ADMIN_OR_SAME_USER, NAME_NOT_UNIQUE, NO_NAMESPACE,
    NO_RESULTS, NOT_AN_ORG_MEMBER, NOT_AUTHENTICATED,
};

/// `btypes.GetSupportedExternalServices` (`types/util.go:24-38`), without the
/// experimental perforce toggle (no site configuration seam in this slice).
const SUPPORTED_CODE_HOSTS: &[&str] = &[
    "github",
    "bitbucketServer",
    "gitlab",
    "bitbucketCloud",
    "azuredevops",
    "gerrit",
];

/// `segmentio/fasthash/fnv1.HashString32` (FNV-1: multiply, then XOR).
fn fnv1_32(input: &str) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    for byte in input.as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u32::from(*byte);
    }
    hash
}

/// `locker.StringKey("batches_apply")` (`locker.go:16-18`).
fn batches_apply_lock_namespace() -> i32 {
    (fnv1_32("batches_apply") % i32::MAX as u32) as i32
}

async fn site_admin_async(pool: &PgPool, actor: Actor) -> Result<Option<bool>, String> {
    match actor {
        Actor::Internal => Ok(Some(true)),
        Actor::Anonymous => Ok(None),
        Actor::User { uid } => sg_db::users::get_site_admin(pool, uid)
            .await
            .map_err(error),
    }
}

/// `database.AuthzQueryConds` for a raw `repo` table reference.
async fn authz_conds_async(pool: &PgPool, actor: Actor) -> Result<String, String> {
    if actor.is_internal() || site_admin_async(pool, actor).await? == Some(true) {
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

/// `rbac.CheckCurrentUserHasPermission(BATCH_CHANGES#WRITE)`.
async fn check_write_async(pool: &PgPool, actor: Actor) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    let Some(uid) = actor.uid() else {
        return Err(NOT_AUTHENTICATED.to_owned());
    };
    let user = sg_db::users::get_session_user(pool, uid)
        .await
        .map_err(error)?;
    if user.is_none() {
        return Err(NOT_AUTHENTICATED.to_owned());
    }
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT permissions.id FROM permissions \
         INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
         INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id \
         WHERE permissions.action = 'WRITE' AND permissions.namespace = 'BATCH_CHANGES' \
         AND user_roles.user_id = $1 LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await
    .map_err(error)?;
    if found.is_none() {
        return Err(MISSING_BATCH_CHANGES_WRITE.to_owned());
    }
    Ok(())
}

/// `auth.CheckSiteAdminOrSameUser` (`site_admin.go:68-73`).
async fn check_site_admin_or_same_user(
    pool: &PgPool,
    actor: Actor,
    subject_user_id: i32,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    if let Some(uid) = actor.uid() {
        if uid == subject_user_id {
            return Ok(());
        }
    }
    if site_admin_async(pool, actor).await? == Some(true) {
        return Ok(());
    }
    Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
}

/// `auth.CheckOrgAccessOrSiteAdmin` (`auth/orgs.go:19-57`).
async fn check_org_access_or_site_admin(
    pool: &PgPool,
    actor: Actor,
    org_id: i32,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    let Some(uid) = actor.uid() else {
        return Err(NOT_AUTHENTICATED.to_owned());
    };
    let user = sg_db::users::get_session_user(pool, uid)
        .await
        .map_err(error)?;
    let Some(user) = user else {
        return Err(NOT_AUTHENTICATED.to_owned());
    };
    if user.site_admin {
        return Ok(());
    }
    let member: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM org_members WHERE org_id = $1 AND user_id = $2 LIMIT 1")
            .bind(org_id)
            .bind(uid)
            .fetch_optional(pool)
            .await
            .map_err(error)?;
    if member.is_some() {
        Ok(())
    } else {
        Err(NOT_AN_ORG_MEMBER.to_owned())
    }
}

/// `service.checkViewerCanAdminister(orgID, creatorID, allowOrgMemberAccess)`.
async fn viewer_can_administer_async(
    pool: &PgPool,
    actor: Actor,
    org_id: i32,
    creator_id: i32,
    allow_org_member_access: bool,
) -> Result<(), String> {
    if org_id != 0 {
        let all_members_admin = org_all_members_admin_async(pool, org_id).await?;
        if all_members_admin || allow_org_member_access {
            return check_org_access_or_site_admin(pool, actor, org_id).await;
        }
    }
    check_site_admin_or_same_user(pool, actor, creator_id).await
}

/// `orgs.allMembersBatchChangesAdmin` from the org's latest settings.
async fn org_all_members_admin_async(pool: &PgPool, org_id: i32) -> Result<bool, String> {
    let Some(record) = sg_db::settings::latest_for_org(pool, org_id)
        .await
        .map_err(error)?
    else {
        return Ok(false);
    };
    let stripped = sg_config::strip_jsonc(&record.contents);
    let value: serde_json::Value = serde_json::from_str(&stripped).map_err(|error| error.to_string())?;
    Ok(match value.get("orgs.allMembersBatchChangesAdmin") {
        Some(serde_json::Value::Bool(value)) => *value,
        _ => false,
    })
}

/// `service.CheckNamespaceAccess`.
async fn check_namespace_access_async(
    pool: &PgPool,
    actor: Actor,
    namespace: NamespaceIds,
) -> Result<(), String> {
    if namespace.org != 0 {
        check_org_access_or_site_admin(pool, actor, namespace.org).await
    } else if namespace.user != 0 {
        check_site_admin_or_same_user(pool, actor, namespace.user).await
    } else {
        Err(NO_NAMESPACE.to_owned())
    }
}

/// The batch-changes name pattern `^[\w.-]+$` (ASCII `\w`, like Go's regexp).
fn valid_batch_change_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-')
}

async fn load_batch_spec_async(
    pool: &PgPool,
    rand_id: &str,
) -> Result<Option<BatchSpecRow>, String> {
    let row = sqlx::query(
        "SELECT batch_specs.id, batch_specs.rand_id, batch_specs.raw_spec, \
         COALESCE(batch_specs.spec->>'name', ''), \
         COALESCE(batch_specs.spec->>'description', ''), \
         COALESCE(batch_specs.namespace_user_id, 0), \
         COALESCE(batch_specs.namespace_org_id, 0), \
         COALESCE(batch_specs.user_id, 0), COALESCE(batch_specs.batch_change_id, 0) \
         FROM batch_specs WHERE batch_specs.rand_id = $1 LIMIT 1",
    )
    .bind(rand_id)
    .fetch_optional(pool)
    .await
    .map_err(error)?;
    row.map(|row| {
        Ok(BatchSpecRow {
            id: row.try_get(0)?,
            rand_id: row.try_get(1)?,
            raw_spec: row.try_get(2)?,
            name: row.try_get(3)?,
            description: row.try_get(4)?,
            namespace_user_id: row.try_get(5)?,
            namespace_org_id: row.try_get(6)?,
            user_id: row.try_get(7)?,
            batch_change_id: row.try_get(8)?,
        })
    })
    .transpose()
    .map_err(error)
}

/// `store.GetBatchChange` including the `ErrDeletedNamespace` marker.
async fn get_batch_change_row_async(
    pool: &PgPool,
    id: i64,
    name: Option<(&str, i32, i32)>,
) -> Result<Option<BatchChangeRow>, String> {
    let mut sql = format!(
        "SELECT {BATCH_CHANGE_COLUMNS}, namespace_user.deleted_at AS user_deleted_at, \
         namespace_org.deleted_at AS org_deleted_at FROM batch_changes \
         LEFT JOIN users namespace_user ON batch_changes.namespace_user_id = namespace_user.id \
         LEFT JOIN orgs namespace_org ON batch_changes.namespace_org_id = namespace_org.id WHERE "
    );
    let mut binds = Binds::default();
    let mut preds = Vec::new();
    if id != 0 {
        let id = binds.i64(id);
        preds.push(format!("batch_changes.id = {id}"));
    }
    if let Some((name, namespace_user_id, namespace_org_id)) = name {
        if !name.is_empty() {
            let name = binds.text(name.to_owned());
            preds.push(format!("batch_changes.name = {name}"));
        }
        if namespace_user_id != 0 {
            let user = binds.i32(namespace_user_id);
            preds.push(format!("batch_changes.namespace_user_id = {user}"));
        }
        if namespace_org_id != 0 {
            let org = binds.i32(namespace_org_id);
            preds.push(format!("batch_changes.namespace_org_id = {org}"));
        }
    }
    if preds.is_empty() {
        preds.push("TRUE".to_owned());
    }
    sql.push_str(&preds.join(" AND "));
    sql.push_str(" ORDER BY batch_changes.id DESC LIMIT 1");
    let row = binds
        .apply(sqlx::query(&sql))
        .fetch_optional(pool)
        .await
        .map_err(error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let user_deleted_at: Option<OffsetDateTime> =
        row.try_get("user_deleted_at").map_err(error)?;
    let org_deleted_at: Option<OffsetDateTime> = row.try_get("org_deleted_at").map_err(error)?;
    if user_deleted_at.is_some() || org_deleted_at.is_some() {
        return Err(DELETED_NAMESPACE.to_owned());
    }
    Ok(Some(scan_batch_change(&row).map_err(error)?))
}

async fn get_batch_change_by_id_async(
    pool: &PgPool,
    id: i64,
) -> Result<BatchChangeRow, String> {
    get_batch_change_row_async(pool, id, None)
        .await?
        .ok_or_else(|| NO_RESULTS.to_owned())
}

/// `service.GetBatchChangeMatchingBatchSpec` + `ReconcileBatchChange`'s
/// lookup half.
async fn matching_batch_change_async(
    pool: &PgPool,
    spec: &BatchSpecRow,
) -> Result<Option<BatchChangeRow>, String> {
    if spec.batch_change_id != 0 {
        get_batch_change_row_async(pool, spec.batch_change_id, None).await
    } else {
        get_batch_change_row_async(
            pool,
            id_unset(),
            Some((&spec.name, spec.namespace_user_id, spec.namespace_org_id)),
        )
        .await
    }
}

fn id_unset() -> i64 {
    0
}

/// `service.ValidateChangesetSpecs`.
async fn validate_changeset_specs_async(
    pool: &PgPool,
    batch_spec_id: i64,
    actor: Actor,
) -> Result<(), String> {
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
    if rows.is_empty() {
        return Ok(());
    }
    let mut repo_ids = Vec::with_capacity(rows.len());
    let mut conflicts = Vec::with_capacity(rows.len());
    for row in &rows {
        let repo_id: i32 = row.try_get("repo_id").map_err(error)?;
        let head_ref: String = row.try_get("head_ref").map_err(error)?;
        let count: i64 = row.try_get("count").map_err(error)?;
        repo_ids.push(repo_id);
        conflicts.push((repo_id, count, head_ref));
    }
    let repos = repos_for_rewire_async(pool, &repo_ids, actor).await?;
    let mut errors = Vec::with_capacity(conflicts.len());
    for (repo_id, count, head_ref) in conflicts {
        errors.push(match repos.get(&repo_id) {
            Some(repo) => format!(
                "{count} changeset specs in {} use the same branch: {head_ref}",
                repo.name
            ),
            None => format!(
                "{count} changeset specs in the same repository use the same branch: {head_ref}"
            ),
        });
    }
    if errors.len() == 1 {
        return Err(format!(
            "Validating changeset specs resulted in an error:\n* {}\n",
            errors[0]
        ));
    }
    let points: Vec<String> = errors.iter().map(|error| format!("* {error}")).collect();
    Err(format!(
        "{} errors when validating changeset specs:\n{}\n",
        errors.len(),
        points.join("\n")
    ))
}

struct RewireSpec {
    id: i64,
    diff_added: Option<i32>,
    diff_deleted: Option<i32>,
    external_id: String,
    kind: String,
}

struct RewireRepo {
    id: i32,
    name: String,
    external_service_type: String,
}

async fn repos_for_rewire_async(
    pool: &PgPool,
    repo_ids: &[i32],
    actor: Actor,
) -> Result<HashMap<i32, RewireRepo>, String> {
    if repo_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let authz = authz_conds_async(pool, actor).await?;
    let sql = format!(
        "SELECT repo.id, repo.name::text, COALESCE(repo.external_service_type, '') \
         FROM repo WHERE repo.id = ANY($1) AND repo.deleted_at IS NULL AND {authz}"
    );
    let rows = sqlx::query(&sql)
        .bind(repo_ids.to_vec())
        .fetch_all(pool)
        .await
        .map_err(error)?;
    let mut repos = HashMap::with_capacity(rows.len());
    for row in &rows {
        let repo = RewireRepo {
            id: row.try_get(0).map_err(error)?,
            name: row.try_get(1).map_err(error)?,
            external_service_type: row.try_get(2).map_err(error)?,
        };
        repos.insert(repo.id, repo);
    }
    Ok(repos)
}

#[derive(Debug, Clone, Default)]
struct Assoc {
    detach: bool,
    archive: bool,
    is_archived: bool,
}

#[derive(Debug, Clone)]
struct RewireChangeset {
    id: i64,
    repo_id: i32,
    external_service_type: String,
    assocs: BTreeMap<i64, Assoc>,
    external_id: Option<String>,
    owned_by: i64,
    current_spec_id: i64,
    previous_spec_id: i64,
    diff_added: Option<i32>,
    diff_deleted: Option<i32>,
    publication_state: String,
    reconciler_state: String,
    external_state: String,
    failure_message: Option<String>,
    syncer_error: Option<String>,
    detached_at: Option<OffsetDateTime>,
    num_resets: i32,
    num_failures: i32,
    closing: bool,
    ui_publication_state: Option<String>,
}

impl RewireChangeset {
    fn attached_to(&self, batch_change_id: i64) -> bool {
        self.assocs.contains_key(&batch_change_id)
    }

    fn archived_in(&self, batch_change_id: i64) -> bool {
        self.assocs
            .get(&batch_change_id)
            .map(|assoc| assoc.is_archived)
            .unwrap_or(false)
    }

    fn attach(&mut self, batch_change_id: i64) {
        let assoc = self.assocs.entry(batch_change_id).or_default();
        assoc.detach = false;
        assoc.archive = false;
        assoc.is_archived = false;
        self.detached_at = None;
    }

    fn detach(&mut self, batch_change_id: i64) -> bool {
        match self.assocs.get_mut(&batch_change_id) {
            Some(assoc) => {
                assoc.detach = true;
                true
            }
            None => false,
        }
    }

    fn archive(&mut self, batch_change_id: i64) -> bool {
        match self.assocs.get_mut(&batch_change_id) {
            Some(assoc) if !assoc.is_archived => {
                assoc.archive = true;
                true
            }
            _ => false,
        }
    }

    fn published(&self) -> bool {
        self.publication_state == "PUBLISHED"
    }

    fn closeable(&self) -> bool {
        self.external_state != "CLOSED"
            && self.external_state != "MERGED"
            && self.external_state != "READONLY"
    }

    fn reset_reconciler_state(&mut self) {
        self.reconciler_state = "queued".to_owned();
        self.num_resets = 0;
        self.num_failures = 0;
        // `ResetReconcilerState` also copies `FailureMessage` into
        // `PreviousFailureMessage`, but `UpdateChangesetsForApply` never
        // persists that column (its temp table leaves it NULL), so the value
        // is not tracked here.
        self.failure_message = None;
        self.syncer_error = None;
    }

    fn set_current_spec(&mut self, spec: &RewireSpec) {
        self.current_spec_id = spec.id;
        self.diff_added = spec.diff_added;
        self.diff_deleted = spec.diff_deleted;
    }

    fn assocs_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (id, assoc) in &self.assocs {
            let mut object = serde_json::Map::new();
            if assoc.detach {
                object.insert("detach".to_owned(), serde_json::Value::Bool(true));
            }
            if assoc.archive {
                object.insert("archive".to_owned(), serde_json::Value::Bool(true));
            }
            if assoc.is_archived {
                object.insert("isArchived".to_owned(), serde_json::Value::Bool(true));
            }
            map.insert(id.to_string(), serde_json::Value::Object(object));
        }
        serde_json::Value::Object(map)
    }
}

fn parse_assocs(value: &serde_json::Value) -> BTreeMap<i64, Assoc> {
    let mut assocs = BTreeMap::new();
    let serde_json::Value::Object(map) = value else {
        return assocs;
    };
    for (key, value) in map {
        let Ok(id) = key.parse::<i64>() else {
            continue;
        };
        let object = value.as_object();
        let flag = |name: &str| {
            object
                .and_then(|object| object.get(name))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };
        assocs.insert(
            id,
            Assoc {
                detach: flag("detach"),
                archive: flag("archive"),
                is_archived: flag("isArchived"),
            },
        );
    }
    assocs
}

fn optional_string(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn optional_id(value: i64) -> Option<i64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn optional_uid(value: i32) -> Option<i32> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

impl PgBatchesGraphqlStore {
    async fn move_batch_change_async(
        &self,
        batch_change_id: i64,
        new_name: Option<&str>,
        new_namespace: Option<NamespaceIds>,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        let mut batch_change = get_batch_change_by_id_async(&self.pool, batch_change_id).await?;
        viewer_can_administer_async(
            &self.pool,
            actor,
            batch_change.namespace_org_id,
            batch_change.creator_id,
            false,
        )
        .await?;
        if let Some(namespace) = new_namespace {
            if namespace.user != 0 || namespace.org != 0 {
                check_namespace_access_async(&self.pool, actor, namespace).await?;
            }
            if namespace.org != 0 {
                batch_change.namespace_org_id = namespace.org;
                batch_change.namespace_user_id = 0;
            } else if namespace.user != 0 {
                batch_change.namespace_user_id = namespace.user;
                batch_change.namespace_org_id = 0;
            }
        }
        if let Some(name) = new_name {
            if !name.is_empty() {
                batch_change.name = name.to_owned();
            }
        }
        sqlx::query(
            "UPDATE batch_changes SET name = $1, description = $2, creator_id = $3, \
             last_applier_id = $4, last_applied_at = $5, namespace_user_id = $6, \
             namespace_org_id = $7, updated_at = now(), closed_at = $8, batch_spec_id = $9 \
             WHERE id = $10",
        )
        .bind(&batch_change.name)
        .bind(&batch_change.description)
        .bind(optional_uid(batch_change.creator_id))
        .bind(optional_uid(batch_change.last_applier_id))
        .bind(batch_change.last_applied_at)
        .bind(optional_uid(batch_change.namespace_user_id))
        .bind(optional_uid(batch_change.namespace_org_id))
        .bind(batch_change.closed_at)
        .bind(batch_change.batch_spec_id)
        .bind(batch_change_id)
        .execute(&self.pool)
        .await
        .map_err(error)?;
        get_batch_change_by_id_async(&self.pool, batch_change_id).await
    }

    async fn close_batch_change_async(
        &self,
        batch_change_id: i64,
        close_changesets: bool,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        let batch_change = get_batch_change_by_id_async(&self.pool, batch_change_id)
            .await
            .map_err(|error| format!("getting batch change: {error}"))?;
        if batch_change.closed() {
            return Ok(batch_change);
        }
        viewer_can_administer_async(
            &self.pool,
            actor,
            batch_change.namespace_org_id,
            batch_change.creator_id,
            false,
        )
        .await?;
        let mut tx = self.pool.begin().await.map_err(error)?;
        sqlx::query(
            "UPDATE batch_changes SET name = $1, description = $2, creator_id = $3, \
             last_applier_id = $4, last_applied_at = $5, namespace_user_id = $6, \
             namespace_org_id = $7, updated_at = now(), closed_at = now(), batch_spec_id = $8 \
             WHERE id = $9",
        )
        .bind(&batch_change.name)
        .bind(&batch_change.description)
        .bind(optional_uid(batch_change.creator_id))
        .bind(optional_uid(batch_change.last_applier_id))
        .bind(batch_change.last_applied_at)
        .bind(optional_uid(batch_change.namespace_user_id))
        .bind(optional_uid(batch_change.namespace_org_id))
        .bind(batch_change.batch_spec_id)
        .bind(batch_change_id)
        .execute(&mut *tx)
        .await
        .map_err(error)?;
        if close_changesets {
            enqueue_changesets_to_close_async(&mut tx, batch_change_id).await?;
        }
        tx.commit().await.map_err(error)?;
        // Go's `CloseBatchChange` only enqueues the webhook on the error path
        // (an inverted condition); the successful path sends nothing. The
        // seam call is kept for the error path only, mirroring the bug.
        get_batch_change_by_id_async(&self.pool, batch_change_id).await
    }

    async fn delete_batch_change_async(
        &self,
        batch_change_id: i64,
        actor: Actor,
    ) -> Result<(), String> {
        let batch_change = get_batch_change_by_id_async(&self.pool, batch_change_id).await?;
        viewer_can_administer_async(
            &self.pool,
            actor,
            batch_change.namespace_org_id,
            batch_change.creator_id,
            false,
        )
        .await?;
        // `enqueueBatchChangeWebhook(batch_change:delete, ...)` runs *before*
        // the hard delete in the baseline; the seam is wired by the runtime.
        sqlx::query("DELETE FROM batch_changes WHERE id = $1")
            .bind(batch_change_id)
            .execute(&self.pool)
            .await
            .map_err(error)?;
        Ok(())
    }

    async fn create_empty_batch_change_async(
        &self,
        namespace: NamespaceIds,
        name: &str,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        check_namespace_access_async(&self.pool, actor, namespace).await?;
        if !valid_batch_change_name(name) {
            return Err(NAME_PATTERN.to_owned());
        }
        let existing = get_batch_change_row_async(
            &self.pool,
            id_unset(),
            Some((name, namespace.user, namespace.org)),
        )
        .await?;
        if existing.is_some() {
            return Err(NAME_NOT_UNIQUE.to_owned());
        }
        let uid = actor.uid().unwrap_or(0);
        let rand_id = uuid::Uuid::new_v4().to_string();
        let raw_spec = format!("name: {name}\n");
        let spec = serde_json::json!({ "name": name });
        let mut tx = self.pool.begin().await.map_err(error)?;
        let batch_spec_id: i64 = sqlx::query_scalar(
            "INSERT INTO batch_specs (rand_id, raw_spec, spec, namespace_user_id, \
             namespace_org_id, user_id, created_from_raw, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, TRUE, now(), now()) RETURNING id",
        )
        .bind(&rand_id)
        .bind(&raw_spec)
        .bind(&spec)
        .bind(optional_uid(namespace.user))
        .bind(optional_uid(namespace.org))
        .bind(optional_uid(uid))
        .fetch_one(&mut *tx)
        .await
        .map_err(error)?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO batch_changes (name, description, creator_id, last_applier_id, \
             last_applied_at, namespace_user_id, namespace_org_id, created_at, updated_at, \
             closed_at, batch_spec_id) \
             VALUES ($1, '', $2, NULL, NULL, $3, $4, now(), now(), NULL, $5) RETURNING id",
        )
        .bind(name)
        .bind(optional_uid(uid))
        .bind(optional_uid(namespace.user))
        .bind(optional_uid(namespace.org))
        .bind(batch_spec_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(error)?;
        tx.commit().await.map_err(error)?;
        get_batch_change_by_id_async(&self.pool, id).await
    }

    async fn apply_batch_change_async(
        &self,
        request: &ApplyBatchChangeRequest,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        let spec = load_batch_spec_async(&self.pool, &request.batch_spec_rand_id)
            .await?
            .ok_or_else(|| NO_RESULTS.to_owned())?;
        viewer_can_administer_async(
            &self.pool,
            actor,
            spec.namespace_org_id,
            spec.user_id,
            false,
        )
        .await?;
        validate_changeset_specs_async(&self.pool, spec.id, actor).await?;

        let existing = matching_batch_change_async(&self.pool, &spec).await?;
        let previous_spec_id = existing
            .as_ref()
            .map(|batch_change| batch_change.batch_spec_id)
            .unwrap_or(0);
        let mut batch_change = existing.unwrap_or(BatchChangeRow {
            id: 0,
            name: String::new(),
            description: String::new(),
            creator_id: 0,
            last_applier_id: 0,
            last_applied_at: None,
            namespace_user_id: 0,
            namespace_org_id: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            closed_at: None,
            batch_spec_id: 0,
        });
        batch_change.batch_spec_id = spec.id;
        batch_change.namespace_user_id = spec.namespace_user_id;
        batch_change.namespace_org_id = spec.namespace_org_id;
        batch_change.name = spec.name.clone();
        batch_change.description = spec.description.clone();
        let uid = actor.uid().unwrap_or(0);
        if batch_change.creator_id == 0 {
            batch_change.creator_id = uid;
        }
        batch_change.last_applier_id = uid;
        batch_change.last_applied_at = Some(OffsetDateTime::now_utc());

        if batch_change.id != 0 && request.fail_if_batch_change_exists {
            return Err(MATCHING_BATCH_CHANGE_EXISTS.to_owned());
        }
        if request.ensure_batch_change_id != 0
            && batch_change.id != request.ensure_batch_change_id
        {
            return Err(
                "a batch change in the given namespace and with the given name exists but does not \
                 match the given ID"
                    .to_owned(),
            );
        }
        if batch_change.closed() {
            return Err("existing batch change matched by batch spec is closed".to_owned());
        }
        if previous_spec_id == spec.id {
            // The baseline returns its in-memory struct here (with the fresh
            // `lastAppliedAt`/`lastApplierID`), without writing to the
            // database. Mirror that overlay so the resolver sees the same
            // state.
            return Ok(batch_change);
        }

        let mut tx = self.pool.begin().await.map_err(error)?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1, $2)")
            .bind(batches_apply_lock_namespace())
            .bind(batch_change.id as i32)
            .fetch_one(&mut *tx)
            .await
            .map_err(error)?;
        if !locked {
            return Err("batch change locked by other user applying batch spec".to_owned());
        }

        if cancel_queued_changesets_async(&mut tx, batch_change.id)
            .await
            .is_err()
        {
            // `ApplyBatchChange` swallows this error and returns the
            // in-memory batch change (`return batchChange, nil`); the
            // deferred `tx.Done(nil)` commits whatever ran before the failure.
            tx.commit().await.map_err(error)?;
            return Ok(batch_change);
        }

        if batch_change.id == 0 {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO batch_changes (name, description, creator_id, last_applier_id, \
                 last_applied_at, namespace_user_id, namespace_org_id, created_at, updated_at, \
                 closed_at, batch_spec_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now(), NULL, $8) RETURNING id",
            )
            .bind(&batch_change.name)
            .bind(&batch_change.description)
            .bind(optional_uid(batch_change.creator_id))
            .bind(optional_uid(batch_change.last_applier_id))
            .bind(batch_change.last_applied_at)
            .bind(optional_uid(batch_change.namespace_user_id))
            .bind(optional_uid(batch_change.namespace_org_id))
            .bind(batch_change.batch_spec_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| {
                if is_name_constraint(&error) {
                    INVALID_BATCH_CHANGE_NAME.to_owned()
                } else {
                    error.to_string()
                }
            })?;
            batch_change.id = id;
        } else {
            sqlx::query(
                "UPDATE batch_changes SET name = $1, description = $2, creator_id = $3, \
                 last_applier_id = $4, last_applied_at = $5, namespace_user_id = $6, \
                 namespace_org_id = $7, updated_at = now(), closed_at = $8, batch_spec_id = $9 \
                 WHERE id = $10",
            )
            .bind(&batch_change.name)
            .bind(&batch_change.description)
            .bind(optional_uid(batch_change.creator_id))
            .bind(optional_uid(batch_change.last_applier_id))
            .bind(batch_change.last_applied_at)
            .bind(optional_uid(batch_change.namespace_user_id))
            .bind(optional_uid(batch_change.namespace_org_id))
            .bind(batch_change.closed_at)
            .bind(batch_change.batch_spec_id)
            .bind(batch_change.id)
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                if is_name_constraint(&error) {
                    INVALID_BATCH_CHANGE_NAME.to_owned()
                } else {
                    error.to_string()
                }
            })?;
        }

        let (inserts, updates) =
            rewire_async(&mut tx, &self.pool, spec.id, batch_change.id, actor).await?;
        let mut inserts = inserts;
        let mut updates = updates;
        let publication_states = prepare_publication_states(
            &request.publication_states,
            &inserts,
            &updates,
            &mut tx,
        )
        .await?;
        for changeset in inserts.iter_mut().chain(updates.iter_mut()) {
            if let Some(state) = publication_states.get(&changeset.current_spec_id) {
                changeset.ui_publication_state = state.clone();
            }
        }
        for changeset in &inserts {
            insert_changeset_async(&mut tx, changeset).await?;
        }
        for changeset in &updates {
            update_changeset_async(&mut tx, changeset).await?;
        }
        tx.commit().await.map_err(error)?;
        // `enqueueBatchChangeWebhook(batch_change:apply, ...)` runs after a
        // successful commit in the baseline.
        get_batch_change_by_id_async(&self.pool, batch_change.id).await
    }

    async fn reenqueue_changesets_async(
        &self,
        batch_change_id: i64,
        changeset_ids: &[i64],
        actor: Actor,
    ) -> Result<Option<BulkOperationRow>, String> {
        let batch_change = get_batch_change_by_id_async(&self.pool, batch_change_id)
            .await
            .map_err(|error| format!("loading batch change: {error}"))?;
        viewer_can_administer_async(
            &self.pool,
            actor,
            batch_change.namespace_org_id,
            batch_change.creator_id,
            false,
        )
        .await?;

        let authz = authz_conds_async(&self.pool, actor).await?;
        let sql = format!(
            "SELECT changesets.id FROM changesets INNER JOIN repo ON repo.id = changesets.repo_id \
             WHERE repo.deleted_at IS NULL AND changesets.id = ANY($1) \
             AND changesets.batch_change_ids ? $2 \
             AND NOT (COALESCE((changesets.batch_change_ids->$2->>'isArchived')::bool, false) \
                 OR COALESCE((changesets.batch_change_ids->$2->>'archive')::bool, false)) \
             AND changesets.reconciler_state = ANY($3) AND {authz}"
        );
        let rows = sqlx::query(&sql)
            .bind(changeset_ids.to_vec())
            .bind(batch_change_id.to_string())
            .bind(vec!["failed".to_owned()])
            .fetch_all(&self.pool)
            .await
            .map_err(|error| format!("listing changesets: {error}"))?;
        if rows.len() != changeset_ids.len() {
            return Err("some changesets could not be found".to_owned());
        }
        let found: Vec<i64> = rows
            .iter()
            .map(|row| row.try_get(0).map_err(error))
            .collect::<Result<_, _>>()?;

        let bulk_group = uuid::Uuid::new_v4().simple().to_string();
        let uid = actor.uid().unwrap_or(0);
        let mut tx = self.pool.begin().await.map_err(error)?;
        for changeset_id in found {
            sqlx::query(
                "INSERT INTO changeset_jobs (bulk_group, user_id, batch_change_id, changeset_id, \
                 job_type, payload, state, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, 'reenqueue', '{}'::jsonb, 'queued', now(), now())",
            )
            .bind(&bulk_group)
            .bind(uid)
            .bind(batch_change_id as i32)
            .bind(changeset_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| format!("creating changeset jobs: {error}"))?;
        }
        tx.commit().await.map_err(error)?;
        get_bulk_operation_async(&self.pool, &bulk_group).await
    }
}

const NAME_PATTERN: &str = "The batch change name can only contain word characters, dots and \
     dashes. No whitespace or newlines allowed.";

fn is_name_constraint(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database) => database
            .constraint()
            .map(|name| name.starts_with("batch_change_name") || name.starts_with("batch_changes_name"))
            .unwrap_or(false),
        _ => false,
    }
}

async fn cancel_queued_changesets_async(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    batch_change_id: i64,
) -> Result<(), String> {
    loop {
        let remaining: i64 = sqlx::query_scalar(
            "WITH changeset_ids AS (\
                 SELECT id FROM changesets WHERE owned_by_batch_change_id = $1 \
                 AND reconciler_state IN ('scheduled', 'queued', 'errored')), \
             updated_records AS (\
                 UPDATE changesets SET reconciler_state = 'failed', failure_message = $2 \
                 WHERE id IN (SELECT id FROM changeset_ids)) \
             SELECT COUNT(id) FROM changesets WHERE owned_by_batch_change_id = $1 \
             AND reconciler_state = 'processing'",
        )
        .bind(batch_change_id)
        .bind("Canceled")
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| format!("canceling queued batch change changesets failed: {error}"))?;
        if remaining == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn enqueue_changesets_to_close_async(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    batch_change_id: i64,
) -> Result<(), String> {
    loop {
        let remaining: i64 = sqlx::query_scalar(
            "WITH all_matching AS (\
                 SELECT id, reconciler_state FROM changesets \
                 WHERE owned_by_batch_change_id = $1 AND publication_state = 'PUBLISHED' \
                 AND NOT (reconciler_state = 'completed' \
                     AND (external_state = 'CLOSED' OR external_state = 'MERGED'))), \
             updated_records AS (\
                 UPDATE changesets SET reconciler_state = 'queued', failure_message = NULL, \
                 num_resets = 0, num_failures = 0, closing = TRUE \
                 WHERE changesets.id IN (SELECT id FROM all_matching \
                     WHERE NOT all_matching.reconciler_state = 'processing')) \
             SELECT COUNT(id) FROM all_matching WHERE all_matching.reconciler_state = 'processing'",
        )
        .bind(batch_change_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(error)?;
        if remaining == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn get_bulk_operation_async(
    pool: &PgPool,
    bulk_group: &str,
) -> Result<Option<BulkOperationRow>, String> {
    let row = sqlx::query(
        "SELECT changeset_jobs.bulk_group AS id, changeset_jobs.job_type AS type, \
         CASE WHEN COUNT(*) FILTER (WHERE changeset_jobs.state IN \
                 ('processing', 'queued', 'errored')) > 0 THEN 'PROCESSING' \
              WHEN COUNT(*) FILTER (WHERE changeset_jobs.state = 'failed') > 0 THEN 'FAILED' \
              ELSE 'COMPLETED' END AS state, \
         CAST(COUNT(*) FILTER (WHERE changeset_jobs.state IN ('completed', 'failed')) AS float) \
             / CAST(COUNT(*) AS float) AS progress, \
         MIN(changeset_jobs.user_id) AS user_id, \
         COUNT(changeset_jobs.id) AS changeset_count, \
         MIN(changeset_jobs.created_at) AS created_at, \
         CASE WHEN (COUNT(*) FILTER (WHERE changeset_jobs.state IN ('completed', 'failed')) \
                 / COUNT(*)) = 1.0 THEN MAX(changeset_jobs.finished_at) ELSE NULL END AS finished_at \
         FROM changeset_jobs \
         INNER JOIN changesets ON changesets.id = changeset_jobs.changeset_id \
         INNER JOIN repo ON repo.id = changesets.repo_id \
         WHERE repo.deleted_at IS NULL AND changeset_jobs.bulk_group = $1 \
         GROUP BY changeset_jobs.bulk_group, changeset_jobs.job_type LIMIT 1",
    )
    .bind(bulk_group)
    .fetch_optional(pool)
    .await
    .map_err(error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(BulkOperationRow {
        id: row.try_get("id").map_err(error)?,
        job_type: row.try_get("type").map_err(error)?,
        state: row.try_get("state").map_err(error)?,
        progress: row.try_get("progress").map_err(error)?,
        user_id: row.try_get("user_id").map_err(error)?,
        changeset_count: row.try_get("changeset_count").map_err(error)?,
        created_at: row.try_get("created_at").map_err(error)?,
        finished_at: row.try_get("finished_at").map_err(error)?,
    }))
}

/// `store.GetRewirerMappings` + `rewirer.Rewire`.
async fn rewire_async(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    pool: &PgPool,
    batch_spec_id: i64,
    batch_change_id: i64,
    actor: Actor,
) -> Result<(Vec<RewireChangeset>, Vec<RewireChangeset>), String> {
    let mappings = sqlx::query(
        "SELECT mappings.changeset_spec_id, mappings.changeset_id, mappings.repo_id FROM (\
             SELECT changeset_spec_id, changeset_id, repo_id \
             FROM tracking_changeset_specs_and_changesets WHERE batch_spec_id = $1 \
             UNION ALL \
             SELECT changeset_spec_id, \
                 MAX(CASE WHEN owner_batch_change_id = $2 THEN changeset_id ELSE 0 END), repo_id \
             FROM branch_changeset_specs_and_changesets WHERE batch_spec_id = $1 \
             GROUP BY changeset_spec_id, repo_id \
             UNION ALL \
             SELECT 0 AS changeset_spec_id, changesets.id AS changeset_id, \
                 changesets.repo_id AS repo_id \
             FROM changesets INNER JOIN repo ON changesets.repo_id = repo.id \
             WHERE repo.deleted_at IS NULL AND changesets.id NOT IN (\
                 SELECT changeset_id FROM tracking_changeset_specs_and_changesets \
                     WHERE batch_spec_id = $1 \
                 UNION \
                 SELECT MAX(CASE WHEN owner_batch_change_id = $2 THEN changeset_id ELSE 0 END) \
                 FROM branch_changeset_specs_and_changesets WHERE batch_spec_id = $1 \
                 GROUP BY changeset_spec_id, repo_id) \
             AND changesets.batch_change_ids ? $3 \
             AND NOT COALESCE((changesets.batch_change_ids->$4->>'isArchived')::bool, false) \
         ) AS mappings ORDER BY mappings.changeset_spec_id ASC, mappings.changeset_id ASC",
    )
    .bind(batch_spec_id)
    .bind(batch_change_id)
    .bind(batch_change_id.to_string())
    .bind(batch_change_id.to_string())
    .fetch_all(&mut **tx)
    .await
    .map_err(error)?;

    let mut spec_ids = Vec::new();
    let mut changeset_ids = Vec::new();
    let mut repo_ids = Vec::new();
    let mut tuples = Vec::with_capacity(mappings.len());
    for row in &mappings {
        let spec_id: i64 = row.try_get(0).map_err(error)?;
        let changeset_id: i64 = row.try_get(1).map_err(error)?;
        let repo_id: i32 = row.try_get(2).map_err(error)?;
        if spec_id != 0 {
            spec_ids.push(spec_id);
        }
        if changeset_id != 0 {
            changeset_ids.push(changeset_id);
        }
        repo_ids.push(repo_id);
        tuples.push((spec_id, changeset_id, repo_id));
    }

    let mut specs = HashMap::new();
    if !spec_ids.is_empty() {
        spec_ids.sort_unstable();
        spec_ids.dedup();
        let rows = sqlx::query(
            "SELECT id, diff_stat_added, diff_stat_deleted, \
             COALESCE(external_id, ''), type FROM changeset_specs \
             WHERE id = ANY($1)",
        )
        .bind(spec_ids)
        .fetch_all(&mut **tx)
        .await
        .map_err(error)?;
        for row in &rows {
            let spec = RewireSpec {
                id: row.try_get(0).map_err(error)?,
                diff_added: row.try_get(1).map_err(error)?,
                diff_deleted: row.try_get(2).map_err(error)?,
                external_id: row.try_get(3).map_err(error)?,
                kind: row.try_get(4).map_err(error)?,
            };
            specs.insert(spec.id, spec);
        }
    }

    let mut changesets = HashMap::new();
    if !changeset_ids.is_empty() {
        changeset_ids.sort_unstable();
        changeset_ids.dedup();
        let rows = sqlx::query(
            "SELECT id, repo_id, external_service_type, batch_change_ids, \
             COALESCE(external_id, ''), COALESCE(owned_by_batch_change_id, 0), \
             COALESCE(current_spec_id, 0), COALESCE(previous_spec_id, 0), \
             diff_stat_added, diff_stat_deleted, COALESCE(publication_state, ''), \
             COALESCE(reconciler_state, ''), COALESCE(external_state, ''), failure_message, \
             previous_failure_message, syncer_error, detached_at, num_resets, num_failures, \
             closing, ui_publication_state::text \
             FROM changesets WHERE id = ANY($1)",
        )
        .bind(changeset_ids)
        .fetch_all(&mut **tx)
        .await
        .map_err(error)?;
        for row in &rows {
            let batch_change_ids: serde_json::Value = row.try_get(3).map_err(error)?;
            let changeset = RewireChangeset {
                id: row.try_get(0).map_err(error)?,
                repo_id: row.try_get(1).map_err(error)?,
                external_service_type: row.try_get(2).map_err(error)?,
                assocs: parse_assocs(&batch_change_ids),
                external_id: optional_string(row.try_get(4).map_err(error)?),
                owned_by: row.try_get(5).map_err(error)?,
                current_spec_id: row.try_get(6).map_err(error)?,
                previous_spec_id: row.try_get(7).map_err(error)?,
                diff_added: row.try_get(8).map_err(error)?,
                diff_deleted: row.try_get(9).map_err(error)?,
                publication_state: row.try_get(10).map_err(error)?,
                reconciler_state: row.try_get(11).map_err(error)?,
                external_state: row.try_get(12).map_err(error)?,
                failure_message: row.try_get(13).map_err(error)?,
                syncer_error: row.try_get(15).map_err(error)?,
                detached_at: row.try_get(16).map_err(error)?,
                num_resets: row.try_get(17).map_err(error)?,
                num_failures: row.try_get(18).map_err(error)?,
                closing: row.try_get(19).map_err(error)?,
                ui_publication_state: row.try_get(20).map_err(error)?,
            };
            changesets.insert(changeset.id, changeset);
        }
    }

    repo_ids.sort_unstable();
    repo_ids.dedup();
    let repos = repos_for_rewire_async(pool, &repo_ids, actor).await?;

    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    for (spec_id, changeset_id, repo_id) in tuples {
        if spec_id == 0 {
            let Some(changeset) = changesets.get_mut(&changeset_id) else {
                continue;
            };
            if !repos.contains_key(&repo_id) {
                continue;
            }
            if !changeset.attached_to(batch_change_id) {
                continue;
            }
            close_changeset(changeset, batch_change_id);
            updates.push(changeset.clone());
            continue;
        }
        let Some(spec) = specs.get(&spec_id) else {
            continue;
        };
        let Some(repo) = repos.get(&repo_id) else {
            return Err(format!("repo not found: id={repo_id}"));
        };
        if !SUPPORTED_CODE_HOSTS.contains(&repo.external_service_type.as_str()) {
            return Err(format!(
                "Code host type {} of repository {:?} is currently not supported for use with \
                 Batch Changes",
                repo.external_service_type, repo.name
            ));
        }
        if let Some(changeset) = changesets.get_mut(&changeset_id) {
            if spec.kind == "existing" {
                attach_tracking_changeset(changeset, batch_change_id);
            } else {
                update_changeset_to_new_spec(changeset, spec, batch_change_id);
            }
            updates.push(changeset.clone());
        } else {
            let changeset = if spec.kind == "existing" {
                create_tracking_changeset(repo, spec, batch_change_id)
            } else {
                create_changeset_for_spec(repo, spec, batch_change_id)
            };
            inserts.push(changeset);
        }
    }
    Ok((inserts, updates))
}

fn close_changeset(changeset: &mut RewireChangeset, batch_change_id: i64) {
    let mut reset = false;
    if changeset.current_spec_id != 0
        && changeset.owned_by == batch_change_id
        && changeset.published()
    {
        if changeset.reconciler_state == "completed" {
            changeset.previous_spec_id = changeset.current_spec_id;
        }
        if !changeset.archived_in(batch_change_id) {
            changeset.archive(batch_change_id);
            reset = true;
            if changeset.closeable() {
                changeset.closing = true;
            }
        }
    } else if changeset.detach(batch_change_id) {
        reset = true;
    }
    if reset {
        changeset.reset_reconciler_state();
    }
}

fn attach_tracking_changeset(changeset: &mut RewireChangeset, batch_change_id: i64) {
    changeset.attach(batch_change_id);
    if changeset.owned_by == 0
        && (changeset.reconciler_state == "errored" || changeset.reconciler_state == "failed")
    {
        changeset.reset_reconciler_state();
    }
}

fn update_changeset_to_new_spec(
    changeset: &mut RewireChangeset,
    spec: &RewireSpec,
    batch_change_id: i64,
) {
    if changeset.reconciler_state == "completed" {
        changeset.previous_spec_id = changeset.current_spec_id;
    }
    changeset.set_current_spec(spec);
    changeset.attach(batch_change_id);
    changeset.reset_reconciler_state();
}

fn create_changeset_for_spec(
    repo: &RewireRepo,
    spec: &RewireSpec,
    batch_change_id: i64,
) -> RewireChangeset {
    let mut changeset = RewireChangeset {
        id: 0,
        repo_id: repo.id,
        external_service_type: repo.external_service_type.clone(),
        assocs: BTreeMap::new(),
        external_id: None,
        owned_by: batch_change_id,
        current_spec_id: 0,
        previous_spec_id: 0,
        diff_added: None,
        diff_deleted: None,
        publication_state: "UNPUBLISHED".to_owned(),
        reconciler_state: "queued".to_owned(),
        external_state: String::new(),
        failure_message: None,
        syncer_error: None,
        detached_at: None,
        num_resets: 0,
        num_failures: 0,
        closing: false,
        ui_publication_state: None,
    };
    changeset.assocs.insert(batch_change_id, Assoc::default());
    changeset.set_current_spec(spec);
    changeset
}

fn create_tracking_changeset(
    repo: &RewireRepo,
    spec: &RewireSpec,
    batch_change_id: i64,
) -> RewireChangeset {
    let mut assocs = BTreeMap::new();
    assocs.insert(batch_change_id, Assoc::default());
    RewireChangeset {
        id: 0,
        repo_id: repo.id,
        external_service_type: repo.external_service_type.clone(),
        assocs,
        external_id: optional_string(spec.external_id.clone()),
        owned_by: 0,
        current_spec_id: 0,
        previous_spec_id: 0,
        diff_added: None,
        diff_deleted: None,
        publication_state: "UNPUBLISHED".to_owned(),
        reconciler_state: "queued".to_owned(),
        external_state: String::new(),
        failure_message: None,
        syncer_error: None,
        detached_at: None,
        num_resets: 0,
        num_failures: 0,
        closing: false,
        ui_publication_state: None,
    }
}

/// `UiPublicationStates.prepareAndValidate`, keyed by changeset spec ID.
async fn prepare_publication_states(
    states: &[(String, PublishedValue)],
    inserts: &[RewireChangeset],
    updates: &[RewireChangeset],
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<HashMap<i64, Option<String>>, String> {
    let mut prepared = HashMap::new();
    if states.is_empty() {
        return Ok(prepared);
    }
    let spec_ids: Vec<i64> = inserts
        .iter()
        .chain(updates.iter())
        .map(|changeset| changeset.current_spec_id)
        .filter(|id| *id != 0)
        .collect();
    let mut specs: HashMap<String, (i64, Option<String>)> = HashMap::new();
    if !spec_ids.is_empty() {
        let mut spec_ids = spec_ids;
        spec_ids.sort_unstable();
        spec_ids.dedup();
        let rows = sqlx::query(
            "SELECT id, rand_id, published FROM changeset_specs WHERE id = ANY($1)",
        )
        .bind(spec_ids)
        .fetch_all(&mut **tx)
        .await
        .map_err(error)?;
        for row in &rows {
            let id: i64 = row.try_get(0).map_err(error)?;
            let rand_id: String = row.try_get(1).map_err(error)?;
            let published: Option<String> = row.try_get(2).map_err(error)?;
            specs.insert(rand_id, (id, published));
        }
    }
    let mut remaining: Vec<String> = Vec::new();
    let mut errors = Vec::new();
    for (rand_id, value) in states {
        match specs.get(rand_id) {
            Some((id, published)) => {
                if published.is_some() {
                    errors.push(format!(
                        "changeset spec {rand_id:?} has the published field set in its spec"
                    ));
                    // `prepareAndValidate` does not drain the entry when the
                    // spec has an explicit `published` field, so the same rand
                    // ID is also reported as not found.
                    remaining.push(rand_id.clone());
                } else if let Some(state) = ui_publication_state(value) {
                    // `UiPublicationStates.get` returns a nil pointer for a
                    // `null` publication state, so the changeset's UI state is
                    // left untouched instead of being cleared.
                    prepared.insert(*id, Some(state.to_owned()));
                }
            }
            None => remaining.push(rand_id.clone()),
        }
    }
    for rand_id in remaining {
        errors.push(format!("changeset spec {rand_id:?} not found"));
    }
    if !errors.is_empty() {
        return Err(crate::batches_mutations::join_multi_errors(&errors));
    }
    Ok(prepared)
}

fn ui_publication_state(value: &PublishedValue) -> Option<&'static str> {
    match value {
        PublishedValue::Null => None,
        PublishedValue::Bool(true) => Some("PUBLISHED"),
        PublishedValue::Bool(false) => Some("UNPUBLISHED"),
        PublishedValue::Draft => Some("DRAFT"),
    }
}

async fn insert_changeset_async(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    changeset: &RewireChangeset,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO changesets (repo_id, external_service_type, batch_change_ids, external_id, \
         owned_by_batch_change_id, current_spec_id, diff_stat_added, diff_stat_deleted, \
         publication_state, reconciler_state, num_resets, num_failures, closing, \
         ui_publication_state, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, 0, FALSE, \
         $11::batch_changes_changeset_ui_publication_state, now(), now())",
    )
    .bind(changeset.repo_id)
    .bind(&changeset.external_service_type)
    .bind(changeset.assocs_json())
    .bind(&changeset.external_id)
    .bind(optional_id(changeset.owned_by))
    .bind(optional_id(changeset.current_spec_id))
    .bind(changeset.diff_added)
    .bind(changeset.diff_deleted)
    .bind(&changeset.publication_state)
    .bind(&changeset.reconciler_state)
    .bind(&changeset.ui_publication_state)
    .execute(&mut **tx)
    .await
    .map_err(error)?;
    Ok(())
}

async fn update_changeset_async(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    changeset: &RewireChangeset,
) -> Result<(), String> {
    sqlx::query(
        // `UpdateChangesetsForApply` leaves `previous_failure_message` out of
        // its temporary table, so the column is always reset to NULL.
        "UPDATE changesets SET batch_change_ids = $2, updated_at = now(), detached_at = $3, \
         diff_stat_added = $4, diff_stat_deleted = $5, current_spec_id = $6, \
         previous_spec_id = $7, \
         ui_publication_state = $8::batch_changes_changeset_ui_publication_state, \
         reconciler_state = $9, failure_message = $10, previous_failure_message = NULL, \
         num_resets = $11, num_failures = $12, closing = $13, syncer_error = $14 WHERE id = $1",
    )
    .bind(changeset.id)
    .bind(changeset.assocs_json())
    .bind(changeset.detached_at)
    .bind(changeset.diff_added)
    .bind(changeset.diff_deleted)
    .bind(optional_id(changeset.current_spec_id))
    .bind(optional_id(changeset.previous_spec_id))
    .bind(&changeset.ui_publication_state)
    .bind(&changeset.reconciler_state)
    .bind(&changeset.failure_message)
    .bind(changeset.num_resets)
    .bind(changeset.num_failures)
    .bind(changeset.closing)
    .bind(&changeset.syncer_error)
    .execute(&mut **tx)
    .await
    .map_err(error)?;
    Ok(())
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

    fn batch_changes_restricted_to_admins(&self) -> bool {
        self.batch_changes_restricted_to_admins
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

    // -------------------------------------------------------------------
    // M9.25b write surface
    // -------------------------------------------------------------------

    fn current_user_has_batch_changes_write(&self, actor: Actor) -> Result<(), String> {
        block_on(check_write_async(&self.pool, actor))
    }

    fn check_namespace_access(
        &self,
        actor: Actor,
        namespace: NamespaceIds,
    ) -> Result<(), String> {
        block_on(check_namespace_access_async(&self.pool, actor, namespace))
    }

    fn viewer_can_administer_batch_change(
        &self,
        actor: Actor,
        namespace_org_id: i32,
        creator_id: i32,
    ) -> Result<(), String> {
        block_on(viewer_can_administer_async(
            &self.pool,
            actor,
            namespace_org_id,
            creator_id,
            false,
        ))
    }

    fn org_all_members_batch_changes_admin(&self, org_id: i32) -> Result<bool, String> {
        block_on(org_all_members_admin_async(&self.pool, org_id))
    }

    fn get_batch_spec_by_rand_id(&self, rand_id: &str) -> Result<Option<BatchSpecRow>, String> {
        block_on(load_batch_spec_async(&self.pool, rand_id))
    }

    fn apply_batch_change(
        &self,
        request: &ApplyBatchChangeRequest,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        block_on(self.apply_batch_change_async(request, actor))
    }

    fn create_empty_batch_change(
        &self,
        namespace: NamespaceIds,
        name: &str,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        block_on(self.create_empty_batch_change_async(namespace, name, actor))
    }

    fn move_batch_change(
        &self,
        batch_change_id: i64,
        new_name: Option<&str>,
        new_namespace: Option<NamespaceIds>,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        block_on(self.move_batch_change_async(batch_change_id, new_name, new_namespace, actor))
    }

    fn close_batch_change(
        &self,
        batch_change_id: i64,
        close_changesets: bool,
        actor: Actor,
    ) -> Result<BatchChangeRow, String> {
        block_on(self.close_batch_change_async(batch_change_id, close_changesets, actor))
    }

    fn delete_batch_change(&self, batch_change_id: i64, actor: Actor) -> Result<(), String> {
        block_on(self.delete_batch_change_async(batch_change_id, actor))
    }

    fn reenqueue_changesets(
        &self,
        batch_change_id: i64,
        changeset_ids: &[i64],
        actor: Actor,
    ) -> Result<Option<BulkOperationRow>, String> {
        block_on(self.reenqueue_changesets_async(batch_change_id, changeset_ids, actor))
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
