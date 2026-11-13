#![forbid(unsafe_code)]

//! Batch Changes GraphQL read-only surface (M9.24c, phase 1).
//!
//! Ports the read paths of `cmd/frontend/internal/batches/resolvers`:
//!
//! - `Query.batchChanges` (`resolver.go:760-873`) with the global /
//!   `User` / `Org` / `Repository` scopes, `state`/`states`,
//!   `viewerCanAdminister`, integer `after` cursors and the `first`
//!   validation (`ErrInvalidFirstParameter`, max 10000);
//! - `Query.batchChange(namespace, name)` (`resolver.go:193-215`) and the
//!   `BatchChange` node (`batch_change.go`);
//! - `BatchChange.changesets` (`batch_change.go:194-210`) with
//!   `listChangesetOptsFromArgs` (`resolver.go:900-990`) and the read-only
//!   `Changeset` / `HiddenExternalChangeset` fields (`changeset.go`);
//! - `Query.node` kinds `BatchChange` and `Changeset`;
//! - `User.batchChanges` / `Org.batchChanges` / `Repository.batchChanges`.
//!
//! Unported sub-surfaces return `Ok(None)` (the executor then reports the
//! field as unported): batch specs/workspaces, bulk operations, changeset
//! events, changeset diff/currentSpec, changeset stats, label computation
//! from changeset events, user resolution of `Person`, and every mutation.
//! See `m9_24c_batches_graphql_overview.md` for the full list.
//!
//! The database is injected through [`BatchesGraphqlDatabase`], a synchronous
//! seam in the same style as the saved-searches resolver: the production
//! adapter ([`crate::batches_graphql_store`]) bridges to sqlx with
//! `tokio::task::block_in_place` + `Handle::block_on`. No table, column or
//! Redis key is added.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use serde_json::Value as JsonValue;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::{Duration, OffsetDateTime};

use crate::graphql_inventory_d::{ExternalLinkData, ExternalLinkObject};
use crate::graphql_resolvers::{
    datetime_rfc3339, PageInfoObject, RepositoryData, RepositoryObject,
};

/// `resolvers.batchChangeIDKind` (`batch_change.go:48`).
pub const BATCH_CHANGE_KIND: &str = "BatchChange";
/// `resolvers.changesetIDKind` (`changeset.go:87`).
pub const CHANGESET_KIND: &str = "Changeset";
/// `resolvers.defaultMaxFirstParam` (`resolver.go:2093`).
pub const DEFAULT_MAX_FIRST_PARAM: i64 = 10000;
/// SDL default for `first` (`batches.graphql`, `= 50`).
pub const DEFAULT_FIRST: i64 = 50;
/// `enterprise.ErrBatchChangesDisabled.Error()`.
pub const BATCH_CHANGES_DISABLED: &str = "batch changes are disabled. Ask a site admin to set \
     'batchChanges.enabled' in the site configuration to enable the feature.";
/// `auth.ErrMustBeSiteAdminOrSameUser` (`internal/auth/site_admin.go:15`).
pub const MUST_BE_SITE_ADMIN_OR_SAME_USER: &str =
    "must be authenticated as the authorized user or site admin";

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

pub type BatchesHandle = Arc<dyn BatchesGraphqlDatabase + Send + Sync>;

/// Wrapper so [`crate::graphql_resolvers::NestedPrefetch`] can keep its
/// `PartialEq`/`Eq` derive (handles compare by pointer).
#[derive(Clone)]
pub struct BatchesHandleWrapper(pub BatchesHandle);

impl std::fmt::Debug for BatchesHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchesHandle")
    }
}

impl PartialEq for BatchesHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for BatchesHandleWrapper {}

// ---------------------------------------------------------------------------
// Rows and filters
// ---------------------------------------------------------------------------

/// `types.Namespace`: at most one of `user`/`org` is non-zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NamespaceIds {
    pub user: i32,
    pub org: i32,
}

impl NamespaceIds {
    pub fn is_empty(&self) -> bool {
        self.user == 0 && self.org == 0
    }
}

/// One `batch_changes` row (`batchChangeColumns` + `ScanBatchChange`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchChangeRow {
    pub id: i64,
    pub name: String,
    pub description: String,
    pub creator_id: i32,
    pub last_applier_id: i32,
    pub last_applied_at: Option<OffsetDateTime>,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,
    pub batch_spec_id: i64,
}

impl BatchChangeRow {
    pub fn namespace(&self) -> NamespaceIds {
        NamespaceIds {
            user: self.namespace_user_id,
            org: self.namespace_org_id,
        }
    }

    /// `types.BatchChange.Closed`.
    pub fn closed(&self) -> bool {
        self.closed_at.is_some()
    }

    /// `types.BatchChange.IsDraft`: applied from a draft batch spec.
    pub fn is_draft(&self) -> bool {
        self.last_applied_at.is_none()
    }

    /// `types.BatchChangeState` (`batch_change.go:75-86`).
    pub fn state(&self) -> &'static str {
        if self.closed() {
            "CLOSED"
        } else if self.is_draft() {
            "DRAFT"
        } else {
            "OPEN"
        }
    }
}

/// `store.ListBatchChangesOpts`/`CountBatchChangesOpts` reduced to the read
/// predicates, plus `GetBatchChangeOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchChangeFilter {
    pub limit: i64,
    pub cursor: i64,
    pub states: Vec<String>,
    pub changeset_id: i64,
    pub only_administered_by_user_id: i32,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub repo_id: i32,
    pub exclude_drafts_not_owned_by_user_id: i32,
    /// `GetBatchChangeOpts.ID` / `.Name`.
    pub id: i64,
    pub name: Option<String>,
}

/// `store.ListBatchChanges` result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchChangePage {
    pub rows: Vec<BatchChangeRow>,
    /// `next` from `ListBatchChanges` (0 when the page was not full).
    pub next: i64,
}

/// `GetBatchChange` outcome (`store.ErrNoResults` vs `store.ErrDeletedNamespace`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchChangeLookup {
    Found(BatchChangeRow),
    NotFound,
    DeletedNamespace,
}

/// One `changesets` row (`ChangesetColumns`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetRow {
    pub id: i64,
    pub repo_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub metadata: JsonValue,
    pub external_id: String,
    pub external_service_type: String,
    pub external_deleted_at: Option<OffsetDateTime>,
    pub external_updated_at: Option<OffsetDateTime>,
    pub external_state: String,
    pub external_review_state: String,
    pub external_check_state: String,
    pub diff_stat_added: Option<i32>,
    pub diff_stat_deleted: Option<i32>,
    pub owned_by_batch_change_id: i64,
    pub current_spec_id: i64,
    pub publication_state: String,
    pub computed_state: String,
    pub failure_message: Option<String>,
    pub previous_failure_message: Option<String>,
    pub syncer_error: Option<String>,
    pub external_fork_name: String,
    pub external_fork_namespace: String,
}

impl ChangesetRow {
    /// `types.Changeset.Published`.
    pub fn published(&self) -> bool {
        self.publication_state == "PUBLISHED"
    }

    /// `types.Changeset.IsImporting`: unpublished and no current spec.
    pub fn is_importing(&self) -> bool {
        !self.published() && self.current_spec_id == 0
    }

    /// `types.Changeset.DiffStat`: nil unless both columns are set.
    pub fn diff_stat(&self) -> Option<(i32, i32)> {
        match (self.diff_stat_added, self.diff_stat_deleted) {
            (Some(added), Some(deleted)) => Some((added, deleted)),
            _ => None,
        }
    }
}

/// A single text-search term (`batches/search.TextSearchTerm`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchTerm {
    pub term: String,
    pub not: bool,
}

/// `store.ListChangesetsOpts` reduced to the read predicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangesetFilter {
    pub limit: i64,
    pub cursor: i64,
    pub batch_change_id: i64,
    pub states: Vec<String>,
    pub publication_state: Option<String>,
    pub external_review_state: Option<String>,
    pub external_check_state: Option<String>,
    pub owned_by_batch_change_id: i64,
    pub only_archived: bool,
    pub include_archived: bool,
    pub repo_ids: Vec<i32>,
    pub text_search: Vec<TextSearchTerm>,
    pub enforce_authz: bool,
}

/// `store.ListChangesets` result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangesetPage {
    pub rows: Vec<ChangesetRow>,
    pub next: i64,
}

/// `types.ChangesetSyncData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetSyncDataRow {
    pub changeset_id: i64,
    pub updated_at: Option<OffsetDateTime>,
    pub latest_event: Option<OffsetDateTime>,
    pub external_updated_at: Option<OffsetDateTime>,
}

/// One `batch_specs` row as the mutation resolvers need it (M9.25b).
///
/// `name`/`description` are read from the `spec` JSONB column, matching
/// `btypes.BatchSpec.Spec.Name`/`.Description` in the Go baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSpecRow {
    pub id: i64,
    pub rand_id: String,
    pub raw_spec: String,
    pub name: String,
    pub description: String,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub user_id: i32,
    pub batch_change_id: i64,
}

impl BatchSpecRow {
    pub fn namespace(&self) -> NamespaceIds {
        NamespaceIds {
            user: self.namespace_user_id,
            org: self.namespace_org_id,
        }
    }
}

/// `batches.PublishedValue`: the quadruple `nil`, `true`, `false`, `"draft"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PublishedValue {
    #[default]
    Null,
    Bool(bool),
    Draft,
}

impl PublishedValue {
    /// `ChangesetUiPublicationStateFromPublishedValue`: `nil` maps to `None`.
    pub fn ui_publication_state(&self) -> Option<&'static str> {
        match self {
            PublishedValue::Null => None,
            PublishedValue::Bool(true) => Some("PUBLISHED"),
            PublishedValue::Bool(false) => Some("UNPUBLISHED"),
            PublishedValue::Draft => Some("DRAFT"),
        }
    }
}

/// `service.ApplyBatchChangeOpts` as the GraphQL resolver builds it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApplyBatchChangeRequest {
    pub batch_spec_rand_id: String,
    pub ensure_batch_change_id: i64,
    pub fail_if_batch_change_exists: bool,
    pub publication_states: Vec<(String, PublishedValue)>,
}

/// `types.BulkOperation` aggregated over `changeset_jobs`.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkOperationRow {
    /// `changeset_jobs.bulk_group`, the GraphQL ID payload.
    pub id: String,
    /// `changeset_jobs.job_type`.
    pub job_type: String,
    pub state: String,
    pub progress: f64,
    pub user_id: i32,
    pub changeset_count: i64,
    pub created_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Synchronous database seam for the batches read surface.
pub trait BatchesGraphqlDatabase: Send + Sync {
    /// The store clock (`store.Clock`, defaults to `time.Now`).
    fn now(&self) -> OffsetDateTime;

    /// `conf.Get().DisableAutoCodeHostSyncs`.
    fn disable_auto_code_host_syncs(&self) -> bool {
        false
    }

    /// `enterprise.BatchChangesEnabledForSite` (default true).
    fn batch_changes_enabled(&self) -> bool {
        true
    }

    /// `users.site_admin` of the actor: `None` for anonymous or a missing
    /// user, `Some(true)` for internal actors.
    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String>;

    fn user_by_id(&self, id: i32) -> Result<Option<crate::graphql_resolvers::UserData>, String>;

    fn org_by_id(&self, id: i32) -> Result<Option<crate::graphql_resolvers::OrgData>, String>;

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;

    fn list_batch_changes(
        &self,
        filter: &BatchChangeFilter,
        actor: Actor,
    ) -> Result<BatchChangePage, String>;

    fn count_batch_changes(
        &self,
        filter: &BatchChangeFilter,
        actor: Actor,
    ) -> Result<i64, String>;

    fn get_batch_change(&self, filter: &BatchChangeFilter) -> Result<BatchChangeLookup, String>;

    fn list_changesets(
        &self,
        filter: &ChangesetFilter,
        actor: Actor,
    ) -> Result<ChangesetPage, String>;

    fn count_changesets(
        &self,
        filter: &ChangesetFilter,
        actor: Actor,
    ) -> Result<i64, String>;

    fn get_changeset(&self, id: i64) -> Result<Option<ChangesetRow>, String>;

    fn changeset_sync_data(
        &self,
        changeset_ids: &[i64],
    ) -> Result<Vec<ChangesetSyncDataRow>, String>;

    /// `database.Repos().GetReposSetByIDs`: only repos the actor can access.
    fn repos_by_ids(
        &self,
        repo_ids: &[i32],
        actor: Actor,
    ) -> Result<HashMap<i32, RepositoryData>, String>;

    // -------------------------------------------------------------------
    // M9.25b write surface
    // -------------------------------------------------------------------

    /// `conf.BatchChangesRestrictedToAdmins`.
    fn batch_changes_restricted_to_admins(&self) -> bool {
        false
    }

    /// M9.26c changeset deep-read surface; `None` leaves the fields unported.
    fn changeset_read(
        &self,
    ) -> Option<&dyn crate::batches_changeset_read::ChangesetReadDatabase> {
        None
    }

    /// `rbac.CheckCurrentUserHasPermission(..., BatchChangesWritePermission)`.
    fn current_user_has_batch_changes_write(&self, actor: Actor) -> Result<(), String>;

    /// `service.CheckNamespaceAccess`.
    fn check_namespace_access(&self, actor: Actor, namespace: NamespaceIds) -> Result<(), String>;

    /// `service.checkViewerCanAdminister` for an existing batch change
    /// (`allowOrgMemberAccess == false`).
    fn viewer_can_administer_batch_change(
        &self,
        actor: Actor,
        namespace_org_id: i32,
        creator_id: i32,
    ) -> Result<(), String>;

    /// The org setting `orgs.allMembersBatchChangesAdmin` (default false).
    fn org_all_members_batch_changes_admin(&self, org_id: i32) -> Result<bool, String>;

    /// `store.GetBatchSpec(RandID)`; `None` corresponds to `ErrNoResults`.
    fn get_batch_spec_by_rand_id(&self, rand_id: &str) -> Result<Option<BatchSpecRow>, String>;

    /// `service.ApplyBatchChange` (including `Rewire`), returning the batch
    /// change to render.
    fn apply_batch_change(
        &self,
        request: &ApplyBatchChangeRequest,
        actor: Actor,
    ) -> Result<BatchChangeRow, String>;

    /// `service.CreateEmptyBatchChange`.
    fn create_empty_batch_change(
        &self,
        namespace: NamespaceIds,
        name: &str,
        actor: Actor,
    ) -> Result<BatchChangeRow, String>;

    /// `service.MoveBatchChange`.
    fn move_batch_change(
        &self,
        batch_change_id: i64,
        new_name: Option<&str>,
        new_namespace: Option<NamespaceIds>,
        actor: Actor,
    ) -> Result<BatchChangeRow, String>;

    /// `service.CloseBatchChange`.
    fn close_batch_change(
        &self,
        batch_change_id: i64,
        close_changesets: bool,
        actor: Actor,
    ) -> Result<BatchChangeRow, String>;

    /// `service.DeleteBatchChange`.
    fn delete_batch_change(&self, batch_change_id: i64, actor: Actor) -> Result<(), String>;

    /// `service.CreateChangesetJobs(ChangesetJobTypeReenqueue)`: `None` is
    /// `store.ErrNoResults` from `GetBulkOperation`.
    fn reenqueue_changesets(
        &self,
        batch_change_id: i64,
        changeset_ids: &[i64],
        actor: Actor,
    ) -> Result<Option<BulkOperationRow>, String>;

    /// `service.CreateChangesetJobs` for the bulk-operation mutations
    /// (M9.27c). `None` is `store.ErrNoResults` from `GetBulkOperation`.
    fn create_changeset_jobs(
        &self,
        request: &crate::batches_bulk_mutations::CreateChangesetJobsRequest,
        actor: Actor,
    ) -> Result<Option<BulkOperationRow>, String> {
        let _ = (request, actor);
        Err("createChangesetJobs is not wired".to_owned())
    }

    /// `service.GetAvailableBulkOperations` (M9.27c).
    fn available_bulk_operations(
        &self,
        batch_change_id: i64,
        changeset_ids: &[i64],
        actor: Actor,
    ) -> Result<Vec<String>, String> {
        let _ = (batch_change_id, changeset_ids, actor);
        Err("availableBulkOperations is not wired".to_owned())
    }

    /// `store.GetBulkOperation` (M9.27c); `None` is `store.ErrNoResults`.
    fn get_bulk_operation(
        &self,
        bulk_group: &str,
    ) -> Result<Option<BulkOperationRow>, String> {
        let _ = bulk_group;
        Err("bulk operation node is not wired".to_owned())
    }

    /// `service.enqueueBatchChangeWebhook`. Fire-and-forget in the baseline
    /// (errors only logged); the production adapter wires
    /// `sg_extsvc::batches_webhooks` here.
    fn enqueue_batch_change_webhook(&self, event_type: &str, graphql_id: &str) {
        let _ = (event_type, graphql_id);
    }

    // -------------------------------------------------------------------
    // M9.26a spec surface
    // -------------------------------------------------------------------

    /// The batch-spec / changeset-spec store seam. Implemented by
    /// `PgBatchesGraphqlStore` in `batches_specs_store.rs`; `None` leaves the
    /// spec resolvers unregistered (e.g. stub stores in tests).
    fn specs_database(&self) -> Option<&dyn crate::batches_specs_graphql::BatchesSpecsDatabase> {
        None
    }

    // -------------------------------------------------------------------
    // M9.30c credentials surface
    // -------------------------------------------------------------------

    /// The `user_credentials` / `batch_changes_site_credentials` seam.
    /// Implemented by `PgBatchesGraphqlStore` in `batches_credentials_store.rs`;
    /// `None` leaves credentials unresolved (the fields render `null`).
    fn credentials_database(
        &self,
    ) -> Option<&dyn crate::batches_credentials::BatchesCredentialsDatabase> {
        None
    }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// `auth.CheckCurrentUserIsSiteAdmin` (`site_admin.go:16-32`).
pub fn check_current_user_is_site_admin(
    db: &dyn BatchesGraphqlDatabase,
    actor: Actor,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    match db.current_user_site_admin(actor)? {
        None => Err("not authenticated".to_owned()),
        Some(false) => Err("must be site admin".to_owned()),
        Some(true) => Ok(()),
    }
}

/// `service.CheckViewerCanAdminister` (`service.go:175-225`): site admins, the
/// namespace user, or an org member.
///
/// The baseline turns "valid but unauthorized" errors into `false`: the
/// `Unauthorized` `ErrMustBeSiteAdminOrSameUser` for user namespaces, and
/// `ErrNotAnOrgMember` for org namespaces. It propagates `ErrNotAuthenticated`
/// for org namespaces.
pub fn viewer_can_administer(
    db: &dyn BatchesGraphqlDatabase,
    actor: Actor,
    namespace: NamespaceIds,
) -> Result<bool, String> {
    if actor.is_internal() {
        return Ok(true);
    }
    let Some(uid) = actor.uid() else {
        // `CheckOrgAccessOrSiteAdmin` reports `ErrNotAuthenticated`, which
        // `CheckViewerCanAdminister` does not swallow; for a user namespace
        // `CheckSiteAdminOrSameUser` reports the `Unauthorized`
        // `ErrMustBeSiteAdminOrSameUser`, which it turns into `false`.
        if namespace.org != 0 {
            return Err("not authenticated".to_owned());
        }
        return Ok(false);
    };
    let site_admin = db.current_user_site_admin(actor)?;
    if site_admin == Some(true) {
        return Ok(true);
    }
    if namespace.org != 0 {
        if site_admin.is_none() {
            // `CurrentUser` is nil (deleted/missing row): `CheckOrgAccessOrSiteAdmin`
            // returns `ErrNotAuthenticated`, which the baseline propagates.
            return Err("not authenticated".to_owned());
        }
        // `ErrNotAnOrgMember` is swallowed by `CheckViewerCanAdminister`.
        return db.is_org_member(namespace.org, uid);
    }
    if namespace.user != 0 && uid == namespace.user {
        return Ok(true);
    }
    // `ErrMustBeSiteAdminOrSameUser` is an `Unauthorized` error, so
    // `CheckViewerCanAdminister` converts it to `false`.
    Ok(false)
}

fn actor_uid(actor: Actor) -> i32 {
    actor.uid().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/// `resolvers.ErrInvalidFirstParameter` (`errors.go:5-15`).
pub fn validate_first_param(first: i64) -> Result<(), String> {
    if first < 0 || first > DEFAULT_MAX_FIRST_PARAM {
        return Err(format!(
            "first param {first} is out of range (min=0, max={DEFAULT_MAX_FIRST_PARAM})"
        ));
    }
    Ok(())
}

/// `parseBatchChangeState` (`resolver.go:2070-2085`).
pub fn parse_batch_change_state(state: Option<&str>) -> Result<Option<&'static str>, String> {
    let Some(state) = state else {
        return Ok(None);
    };
    match state {
        "OPEN" => Ok(Some("OPEN")),
        "CLOSED" => Ok(Some("CLOSED")),
        "DRAFT" => Ok(Some("DRAFT")),
        other => Err(format!("unknown state {other:?}")),
    }
}

/// `parseBatchChangeStates` (`resolver.go:2053-2068`).
pub fn parse_batch_change_states(states: &[&str]) -> Result<Vec<&'static str>, String> {
    let mut parsed = Vec::with_capacity(states.len());
    for state in states {
        if let Some(state) = parse_batch_change_state(Some(state))? {
            parsed.push(state);
        }
    }
    Ok(parsed)
}

/// `ChangesetState` values from `batches.graphql`.
pub const CHANGESET_STATES: &[&str] = &[
    "UNPUBLISHED",
    "FAILED",
    "RETRYING",
    "SCHEDULED",
    "PROCESSING",
    "OPEN",
    "DRAFT",
    "CLOSED",
    "MERGED",
    "READONLY",
    "DELETED",
];

/// `ChangesetReviewState` values from `batches.graphql`.
pub const CHANGESET_REVIEW_STATES: &[&str] =
    &["APPROVED", "CHANGES_REQUESTED", "PENDING", "COMMENTED", "DISMISSED"];

/// `ChangesetCheckState` values from `batches.graphql`.
pub const CHANGESET_CHECK_STATES: &[&str] = &["PENDING", "PASSED", "FAILED"];

fn list_argument(arguments: &Arguments, name: &str) -> Option<Vec<String>> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
        ),
        _ => None,
    }
}

/// The namespace/repo/changeset scope carried by the extension fields.
#[derive(Debug, Clone, Copy, Default)]
struct BatchChangeScope {
    namespace: Option<NamespaceIds>,
    repo_id: Option<i32>,
    changeset_id: i64,
}

/// `Resolver.BatchChanges` argument handling (`resolver.go:760-873`).
fn parse_batch_change_filter(
    db: &dyn BatchesGraphqlDatabase,
    arguments: &Arguments,
    actor: Actor,
    scope: BatchChangeScope,
) -> Result<BatchChangeFilter, ExecError> {
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }

    let mut states: Vec<String> = Vec::new();
    if let Some(state) =
        parse_batch_change_state(arguments.string("state")).map_err(ExecError::new)?
    {
        states.push(state.to_owned());
    }
    // If multiple `states` are provided, prefer them over `state` (even when
    // the list is empty).
    if let Some(list) = list_argument(arguments, "states") {
        let refs: Vec<&str> = list.iter().map(String::as_str).collect();
        states = parse_batch_change_states(&refs)
            .map_err(ExecError::new)?
            .into_iter()
            .map(str::to_owned)
            .collect();
    }

    let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
    validate_first_param(first).map_err(ExecError::new)?;

    let cursor = match arguments.string("after") {
        Some(after) => after
            .parse::<i32>()
            .map(i64::from)
            .map_err(|error| ExecError::new(error.to_string()))?,
        None => 0,
    };

    let mut filter = BatchChangeFilter {
        limit: first,
        cursor,
        states,
        changeset_id: scope.changeset_id,
        repo_id: scope.repo_id.unwrap_or(0),
        ..BatchChangeFilter::default()
    };
    if let Some(namespace) = scope.namespace {
        filter.namespace_user_id = namespace.user;
        filter.namespace_org_id = namespace.org;
    }

    // `auth.CheckCurrentUserIsSiteAdmin`: only `ErrMustBeSiteAdmin` is
    // tolerated (the viewer is simply not an admin); every other error is
    // fatal.
    let is_site_admin = match check_current_user_is_site_admin(db, actor) {
        Ok(()) => true,
        Err(message) if message == "must be site admin" => false,
        Err(message) => return Err(ExecError::new(message)),
    };
    if !is_site_admin {
        if arguments.bool("viewerCanAdminister").unwrap_or(false) {
            filter.only_administered_by_user_id = actor_uid(actor);
        }
        // 🚨 SECURITY: exclude unapplied (draft) batch changes the user does
        // not own.
        filter.exclude_drafts_not_owned_by_user_id = actor_uid(actor);
    }

    Ok(filter)
}

/// `listChangesetOptsFromArgs` (`resolver.go:900-990`). The second value is
/// `optsSafe`: when false, hidden changesets are filtered by authz.
fn parse_changeset_filter(
    arguments: &Arguments,
    batch_change_id: i64,
) -> Result<(ChangesetFilter, bool), String> {
    let mut safe = true;
    let mut filter = ChangesetFilter {
        batch_change_id,
        ..ChangesetFilter::default()
    };

    let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
    validate_first_param(first)?;
    filter.limit = first;

    if let Some(after) = arguments.string("after") {
        filter.cursor = after
            .parse::<i32>()
            .map(i64::from)
            .map_err(|error| format!("parsing after cursor: {error}"))?;
    }

    let only_closable = arguments.bool("onlyClosable").unwrap_or(false);
    if only_closable {
        if arguments.string("state").is_some() {
            return Err("invalid combination of state and onlyClosable".to_owned());
        }
        filter.states = vec!["OPEN".to_owned(), "DRAFT".to_owned()];
    }

    if let Some(state) = arguments.string("state") {
        if !CHANGESET_STATES.contains(&state) {
            return Err("changeset state not valid".to_owned());
        }
        filter.states = vec![state.to_owned()];
    }

    if let Some(state) = arguments.string("reviewState") {
        if !CHANGESET_REVIEW_STATES.contains(&state) {
            return Err("changeset review state not valid".to_owned());
        }
        filter.external_review_state = Some(state.to_owned());
        safe = false;
    }
    if let Some(state) = arguments.string("checkState") {
        if !CHANGESET_CHECK_STATES.contains(&state) {
            return Err("changeset check state not valid".to_owned());
        }
        filter.external_check_state = Some(state.to_owned());
        safe = false;
    }

    if arguments.get("onlyPublishedByThisBatchChange").is_some() {
        filter.owned_by_batch_change_id = batch_change_id;
        filter.publication_state = Some("PUBLISHED".to_owned());
    }

    if let Some(search) = arguments.string("search") {
        filter.text_search =
            parse_text_search(search).map_err(|error| format!("parsing search: {error}"))?;
        // Searching by repository name may leak hidden repositories.
        safe = false;
    }

    filter.only_archived = arguments.bool("onlyArchived").unwrap_or(false);

    if let Some(repo) = arguments.string("repo") {
        let repo_id =
            unmarshal_relay_int(repo).map_err(|error| format!("unmarshalling repo id: {error}"))?;
        filter.repo_ids = vec![repo_id as i32];
    }

    Ok((filter, safe))
}

/// The payload after `relay.MarshalID`'s colon.
fn unmarshal_relay_spec(id: &str) -> Result<JsonValue, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `relay.UnmarshalSpec` into an integer.
pub fn unmarshal_relay_int(id: &str) -> Result<i64, String> {
    match unmarshal_relay_spec(id)? {
        JsonValue::Number(number) => number
            .as_i64()
            .ok_or_else(|| "invalid graphql.ID".to_owned()),
        _ => Err("invalid graphql.ID".to_owned()),
    }
}

/// `UnmarshalNamespaceID` + `UnmarshalNamespaceToIDs` (`namespaces.go:32-87`).
pub fn unmarshal_namespace(id: &str) -> Result<NamespaceIds, String> {
    let invalid = || format!("invalid ID \"{id}\" for namespace");
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|_| invalid())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((kind, spec)) = text.split_once(':') else {
        return Err(invalid());
    };
    let value: JsonValue = serde_json::from_str(spec).map_err(|error| error.to_string())?;
    let namespace = match kind {
        "User" => NamespaceIds {
            user: value.as_i64().unwrap_or(0) as i32,
            org: 0,
        },
        "Org" => NamespaceIds {
            user: 0,
            org: value.as_i64().unwrap_or(0) as i32,
        },
        _ => return Err(invalid()),
    };
    if namespace.is_empty() {
        return Err(invalid());
    }
    Ok(namespace)
}

// ---------------------------------------------------------------------------
// Text search (`internal/batches/search`)
// ---------------------------------------------------------------------------

fn regexp_quote_meta(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for character in value.chars() {
        if "\\.+*?()|[]{}^$".contains(character) {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted
}

/// A best-effort port of `search.ParseTextSearch`: literal and quoted terms
/// with `-` negations. Fields and regex patterns are rejected with the
/// baseline's wrapping text (character positions are approximate).
pub fn parse_text_search(search: &str) -> Result<Vec<TextSearchTerm>, String> {
    let bytes = search.as_bytes();
    let mut terms = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let start = index;
        let mut not = false;
        if bytes[index] == b'-' {
            not = true;
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        if bytes[index] == b'/' {
            return Err(format!(
                "regular expressions are unsupported. The error started at character {}: <code>{}</code>",
                start + 1,
                &search[start..]
            ));
        }
        let term;
        if bytes[index] == b'"' {
            index += 1;
            let content_start = index;
            while index < bytes.len() && bytes[index] != b'"' {
                index += 1;
            }
            term = search[content_start..index].to_owned();
            if index < bytes.len() {
                index += 1;
            }
        } else {
            let content_start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            let token = &search[content_start..index];
            if let Some((field, _value)) = token.split_once(':') {
                return Err(format!(
                    "fields of type `{field}` are unsupported. The error started at character {}: <code>{}</code>",
                    start + 1,
                    &search[start..]
                ));
            }
            term = token.to_owned();
        }
        if !term.is_empty() {
            terms.push(TextSearchTerm { term, not });
        }
    }
    Ok(terms)
}

/// `textSearchTermToClause`'s bound regex for one field.
pub fn text_search_pattern(term: &TextSearchTerm) -> String {
    format!(r"\m{}\M", regexp_quote_meta(&term.term))
}

// ---------------------------------------------------------------------------
// Changeset metadata extraction (`types/changeset.go`)
// ---------------------------------------------------------------------------

fn string_at(value: &JsonValue, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .unwrap_or("")
        .to_owned()
}

fn millis_at(value: &JsonValue, pointer: &str) -> Option<OffsetDateTime> {
    let millis = value.pointer(pointer).and_then(JsonValue::as_i64)?;
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).ok()
}

fn time_at(value: &JsonValue, pointer: &str) -> Option<OffsetDateTime> {
    let text = value.pointer(pointer).and_then(JsonValue::as_str)?;
    OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
}

/// The `ChangesetLabel` GraphQL object data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetLabelData {
    pub name: String,
    pub color: String,
    pub description: String,
}

/// The fields the GraphQL surface needs from a code-host metadata payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataView {
    pub title: String,
    pub body: String,
    pub url: String,
    pub author_name: String,
    pub author_email: String,
    pub external_created_at: Option<OffsetDateTime>,
    pub labels: Vec<ChangesetLabelData>,
}

/// `types.Changeset.Title` / `.Body` / `.URL` / `.AuthorName` /
/// `.AuthorEmail` / `.ExternalCreatedAt` / `.Labels` per code host
/// (`types/changeset.go:494-700,1080-1130`).
pub fn metadata_view(external_service_type: &str, metadata: &JsonValue) -> MetadataView {
    match external_service_type {
        "github" => MetadataView {
            title: string_at(metadata, "/Title"),
            body: string_at(metadata, "/Body"),
            url: string_at(metadata, "/URL"),
            author_name: string_at(metadata, "/Author/Login"),
            author_email: String::new(),
            external_created_at: time_at(metadata, "/CreatedAt"),
            labels: metadata
                .pointer("/Labels/Nodes")
                .and_then(JsonValue::as_array)
                .map(|nodes| {
                    nodes
                        .iter()
                        .map(|node| ChangesetLabelData {
                            name: string_at(node, "/Name"),
                            color: string_at(node, "/Color"),
                            description: string_at(node, "/Description"),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        "gitlab" => MetadataView {
            title: string_at(metadata, "/title"),
            body: string_at(metadata, "/description"),
            url: string_at(metadata, "/web_url"),
            author_name: string_at(metadata, "/author/username"),
            author_email: string_at(metadata, "/author/email"),
            external_created_at: time_at(metadata, "/created_at"),
            labels: metadata
                .pointer("/labels")
                .and_then(JsonValue::as_array)
                .map(|labels| {
                    labels
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(|name| ChangesetLabelData {
                            name: name.to_owned(),
                            color: "000000".to_owned(),
                            description: String::new(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        "bitbucketServer" => MetadataView {
            title: string_at(metadata, "/title"),
            body: string_at(metadata, "/description"),
            url: string_at(metadata, "/links/self/0/href"),
            author_name: string_at(metadata, "/author/user/name"),
            author_email: string_at(metadata, "/author/user/emailAddress"),
            external_created_at: millis_at(metadata, "/createdDate"),
            labels: Vec::new(),
        },
        "bitbucketCloud" => MetadataView {
            title: string_at(metadata, "/title"),
            body: string_at(metadata, "/rendered/description/raw"),
            url: string_at(metadata, "/links/html/href"),
            author_name: string_at(metadata, "/author/username"),
            author_email: String::new(),
            external_created_at: time_at(metadata, "/created_on"),
            labels: Vec::new(),
        },
        "azuredevops" => MetadataView {
            title: string_at(metadata, "/title"),
            body: string_at(metadata, "/description"),
            url: azure_devops_url(metadata),
            author_name: string_at(metadata, "/createdBy/uniqueName"),
            author_email: string_at(metadata, "/createdBy/uniqueName"),
            external_created_at: time_at(metadata, "/creationDate"),
            labels: Vec::new(),
        },
        "gerrit" => {
            let subject = string_at(metadata, "/change/subject");
            let title = subject.split('\n').next().unwrap_or("").trim_matches('"');
            MetadataView {
                title: title.to_owned(),
                body: subject,
                url: String::new(),
                author_name: string_at(metadata, "/change/owner/name"),
                author_email: string_at(metadata, "/change/owner/email"),
                external_created_at: time_at(metadata, "/change/created"),
                labels: metadata
                    .pointer("/change/hashtags")
                    .and_then(JsonValue::as_array)
                    .map(|tags| {
                        tags.iter()
                            .filter_map(JsonValue::as_str)
                            .map(|name| ChangesetLabelData {
                                name: name.to_owned(),
                                color: "000000".to_owned(),
                                description: String::new(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        }
        "perforce" => MetadataView {
            title: string_at(metadata, "/Title"),
            body: String::new(),
            url: String::new(),
            author_name: string_at(metadata, "/Author"),
            author_email: String::new(),
            external_created_at: time_at(metadata, "/CreationDate"),
            labels: Vec::new(),
        },
        _ => MetadataView::default(),
    }
}

fn azure_devops_url(metadata: &JsonValue) -> String {
    let Some(link) = metadata.get("url").and_then(JsonValue::as_str) else {
        return String::new();
    };
    let project = string_at(metadata, "/repository/project/name");
    let name = string_at(metadata, "/repository/name");
    let id = metadata.get("id").and_then(JsonValue::as_i64).unwrap_or(0);
    if project.is_empty() || name.is_empty() {
        return String::new();
    }
    let Ok(mut parsed) = url::Url::parse(link) else {
        return String::new();
    };
    let org = parsed
        .path_segments()
        .and_then(|mut segments| segments.next())
        .unwrap_or("")
        .to_owned();
    parsed.set_path(&format!(
        "/{org}/{project}/_git/{name}/pullrequest/{id}"
    ));
    parsed.to_string()
}

/// `extsvc.TypeToKind` for the batches code hosts.
pub fn type_to_kind(external_service_type: &str) -> String {
    match external_service_type {
        "github" => "GITHUB",
        "gitlab" => "GITLAB",
        "bitbucketServer" => "BITBUCKETSERVER",
        "bitbucketCloud" => "BITBUCKETCLOUD",
        "azuredevops" => "AZUREDEVOPS",
        "gerrit" => "GERRIT",
        "perforce" => "PERFORCE",
        _ => "",
    }
    .to_owned()
}

/// `types.Changeset.SupportsLabels` (`util.go:26-60`): GitHub and GitLab only.
fn supports_labels(external_service_type: &str) -> bool {
    matches!(external_service_type, "github" | "gitlab")
}

// ---------------------------------------------------------------------------
// nextSyncAt (`internal/batches/syncer/sync.go:15-52`)
// ---------------------------------------------------------------------------

const MIN_SYNC_DELAY_SECONDS: i64 = 2 * 60;
const MAX_SYNC_DELAY_SECONDS: i64 = 8 * 60 * 60;

/// `syncer.NextSync(clock, data)`.
pub fn next_sync(
    now: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    latest_event: Option<OffsetDateTime>,
    external_updated_at: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    let Some(last_sync) = updated_at else {
        // Edge case where we've never synced.
        return Some(now);
    };
    let latest_event = latest_event.unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let external_updated_at = external_updated_at.unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let last_change = if latest_event != OffsetDateTime::UNIX_EPOCH
        && (latest_event - last_sync).whole_seconds().abs() < MIN_SYNC_DELAY_SECONDS
    {
        external_updated_at
    } else {
        external_updated_at.max(latest_event)
    };
    let diff = last_sync - last_change;
    if diff.is_negative() {
        return Some(last_change + Duration::seconds(MIN_SYNC_DELAY_SECONDS));
    }
    let diff = diff
        .whole_seconds()
        .clamp(MIN_SYNC_DELAY_SECONDS, MAX_SYNC_DELAY_SECONDS);
    Some(last_sync + Duration::seconds(diff))
}

// ---------------------------------------------------------------------------
// Relay IDs / shared helpers
// ---------------------------------------------------------------------------

/// `relay.MarshalID("BatchChange", id)`.
pub fn marshal_batch_change_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{BATCH_CHANGE_KIND}:{id}"))
}

/// `relay.MarshalID("Changeset", id)`.
pub fn marshal_changeset_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{CHANGESET_KIND}:{id}"))
}

/// `resolvers.marshalBulkOperationID` (`bulk_operation.go:24`): the payload is
/// a JSON string, so the colon suffix is quoted.
pub fn marshal_bulk_operation_id(id: &str) -> String {
    base64::engine::general_purpose::URL_SAFE
        .encode(format!("BulkOperation:{}", serde_json::json!(id)))
}

/// `relay.UnmarshalSpec` into a string (batch spec random IDs).
pub fn unmarshal_relay_string(id: &str) -> Result<String, String> {
    match unmarshal_relay_spec(id)? {
        JsonValue::String(value) => Ok(value),
        _ => Err("invalid graphql.ID".to_owned()),
    }
}

fn json_str(value: impl Into<String>) -> FieldValue {
    FieldValue::Json(Json::Str(value.into()))
}

fn optional_json_str(value: Option<String>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value)),
        None => FieldValue::Null,
    }
}

fn namespace_url(db: &dyn BatchesGraphqlDatabase, namespace: NamespaceIds) -> Result<String, String> {
    if namespace.user != 0 {
        let user = db
            .user_by_id(namespace.user)?
            .ok_or_else(|| "namespace of batch change has been deleted".to_owned())?;
        Ok(format!("/users/{}", user.username))
    } else if namespace.org != 0 {
        let org = db
            .org_by_id(namespace.org)?
            .ok_or_else(|| "namespace of batch change has been deleted".to_owned())?;
        Ok(format!("/organizations/{}", org.name))
    } else {
        Err("namespace of batch change has been deleted".to_owned())
    }
}

fn namespace_object(
    db: &dyn BatchesGraphqlDatabase,
    actor: Actor,
    namespace: NamespaceIds,
) -> Result<FieldValue, ExecError> {
    let admin = db.current_user_site_admin(actor).map_err(ExecError::new)? == Some(true);
    if namespace.user != 0 {
        let user = db
            .user_by_id(namespace.user)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new("namespace of batch change has been deleted"))?;
        return Ok(FieldValue::Object(Arc::new(
            crate::graphql_resolvers::UserObject::from_data_as_viewer(user, admin),
        )));
    }
    if namespace.org != 0 {
        let member = match actor.uid() {
            Some(uid) if !admin => db.is_org_member(namespace.org, uid).unwrap_or(false),
            _ => false,
        };
        let mut org = db
            .org_by_id(namespace.org)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new("namespace of batch change has been deleted"))?;
        org.viewer_can_administer = admin || member;
        org.viewer_is_member = member;
        return Ok(FieldValue::Object(Arc::new(
            crate::graphql_resolvers::OrgObject::from_data_as_viewer(org, admin),
        )));
    }
    Err(ExecError::new(
        "namespace of batch change has been deleted",
    ))
}

// ---------------------------------------------------------------------------
// BatchChange object
// ---------------------------------------------------------------------------

pub struct BatchChangeObject {
    pub row: BatchChangeRow,
    pub db: BatchesHandle,
}

impl BatchChangeObject {
    pub fn new(row: BatchChangeRow, db: BatchesHandle) -> Self {
        Self { row, db }
    }

    fn user_field(&self, id: i32, actor: Actor) -> Result<FieldValue, ExecError> {
        if id == 0 {
            return Ok(FieldValue::Null);
        }
        let admin = self.db.current_user_site_admin(actor).map_err(ExecError::new)? == Some(true);
        match self.db.user_by_id(id).map_err(ExecError::new)? {
            Some(user) => Ok(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::UserObject::from_data_as_viewer(user, admin),
            ))),
            None => Ok(FieldValue::Null),
        }
    }
}

impl ObjectValue for BatchChangeObject {
    fn type_name(&self) -> &'static str {
        "BatchChange"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_str(marshal_batch_change_id(self.row.id)))),
            "namespace" => Ok(Some(namespace_object(
                self.db.as_ref(),
                actor,
                self.row.namespace(),
            )?)),
            "name" => Ok(Some(json_str(self.row.name.clone()))),
            "description" => Ok(Some(optional_json_str(
                (!self.row.description.is_empty()).then(|| self.row.description.clone()),
            ))),
            "state" => Ok(Some(json_str(self.row.state()))),
            "creator" => self.user_field(self.row.creator_id, actor).map(Some),
            "lastApplier" => self.user_field(self.row.last_applier_id, actor).map(Some),
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(
                viewer_can_administer(self.db.as_ref(), actor, self.row.namespace())
                    .map_err(ExecError::new)?,
            )))),
            "url" => {
                let base = namespace_url(self.db.as_ref(), self.row.namespace())
                    .map_err(ExecError::new)?;
                Ok(Some(json_str(format!(
                    "{base}/batch-changes/{}",
                    self.row.name
                ))))
            }
            "createdAt" => Ok(Some(json_str(datetime_rfc3339(self.row.created_at)))),
            "updatedAt" => Ok(Some(json_str(datetime_rfc3339(self.row.updated_at)))),
            "lastAppliedAt" => Ok(Some(optional_json_str(
                self.row.last_applied_at.map(datetime_rfc3339),
            ))),
            "closedAt" => Ok(Some(optional_json_str(
                if self.row.closed() {
                    self.row.closed_at.map(datetime_rfc3339)
                } else {
                    None
                },
            ))),
            "changesets" => {
                let (filter, safe) =
                    parse_changeset_filter(arguments, self.row.id).map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    ChangesetConnectionObject::new(self.db.clone(), filter, safe, actor),
                ))))
            }
            // M9.26a: `BatchChange.currentSpec` / `batchSpecs`.
            "currentSpec" | "batchSpecs" => {
                crate::batches_specs_graphql::batch_change_field(
                    &self.db,
                    &self.row,
                    field,
                    arguments,
                    actor,
                )
            }
            // M9.26c: changeset counts/stats/diffstat/bulk operations.
            _ => crate::batches_changeset_read::batch_change_field(self, field, arguments, actor),
        }
    }
}

// ---------------------------------------------------------------------------
// BatchChanges connection
// ---------------------------------------------------------------------------

pub struct BatchChangeConnectionObject {
    db: BatchesHandle,
    filter: BatchChangeFilter,
    actor: Actor,
    page: OnceLock<Result<BatchChangePage, String>>,
    total: OnceLock<Result<i64, String>>,
}

impl BatchChangeConnectionObject {
    pub fn new(db: BatchesHandle, filter: BatchChangeFilter, actor: Actor) -> Self {
        Self {
            db,
            filter,
            actor,
            page: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn page(&self) -> Result<&BatchChangePage, String> {
        self.page
            .get_or_init(|| self.db.list_batch_changes(&self.filter, self.actor))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn total(&self) -> Result<i64, String> {
        self.total
            .get_or_init(|| self.db.count_batch_changes(&self.filter, self.actor))
            .clone()
    }
}

impl ObjectValue for BatchChangeConnectionObject {
    fn type_name(&self) -> &'static str {
        "BatchChangeConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let page = self.page().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: page
                        .rows
                        .iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(BatchChangeObject::new(
                                row.clone(),
                                self.db.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("BatchChange"),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total().map_err(ExecError::new)?,
            )))),
            "pageInfo" => {
                let next = self.page().map_err(ExecError::new)?.next;
                let (has_next, cursor) = if next != 0 {
                    (true, Some(next.to_string()))
                } else {
                    (false, None)
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Changeset object
// ---------------------------------------------------------------------------

pub struct ChangesetObject {
    pub row: ChangesetRow,
    pub db: BatchesHandle,
    /// A `None` repository means the actor cannot access it → the resolver
    /// resolves to `HiddenExternalChangeset`.
    pub repo: Option<RepositoryData>,
}

impl ChangesetObject {
    pub fn new(row: ChangesetRow, db: BatchesHandle, repo: Option<RepositoryData>) -> Self {
        Self { row, db, repo }
    }

    fn metadata(&self) -> MetadataView {
        metadata_view(&self.row.external_service_type, &self.row.metadata)
    }

    fn next_sync_at(&self) -> Result<Option<OffsetDateTime>, ExecError> {
        if self.db.disable_auto_code_host_syncs() {
            return Ok(None);
        }
        let data = self
            .db
            .changeset_sync_data(&[self.row.id])
            .map_err(ExecError::new)?;
        let Some(data) = data.iter().find(|data| data.changeset_id == self.row.id) else {
            return Ok(None);
        };
        Ok(next_sync(
            self.db.now(),
            data.updated_at,
            data.latest_event,
            data.external_updated_at,
        ))
    }

    fn external_url(&self) -> Option<String> {
        if !self.row.published() {
            return None;
        }
        if self.row.external_state == "DELETED" {
            return None;
        }
        let url = self.metadata().url;
        (!url.is_empty()).then_some(url)
    }

    fn author(&self) -> FieldValue {
        if self.row.is_importing() || !self.row.published() {
            return FieldValue::Null;
        }
        let metadata = self.metadata();
        if metadata.author_name.is_empty() && metadata.author_email.is_empty() {
            return FieldValue::Null;
        }
        FieldValue::Object(Arc::new(PersonObject {
            name: metadata.author_name,
            email: metadata.author_email,
        }))
    }
}

impl ObjectValue for ChangesetObject {
    fn type_name(&self) -> &'static str {
        if self.repo.is_some() {
            "ExternalChangeset"
        } else {
            "HiddenExternalChangeset"
        }
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // Interface fields (`batches.graphql`, `HiddenExternalChangeset`).
            "id" => Ok(Some(json_str(marshal_changeset_id(self.row.id)))),
            // `changesetResolver.BatchChanges` does not exclude drafts (see
            // `batches_changeset_read::changeset_field`).
            "batchChanges" => {
                crate::batches_changeset_read::changeset_field(self, field, arguments, actor)
            }
            "state" => Ok(Some(json_str(self.row.computed_state.clone()))),
            "createdAt" => Ok(Some(json_str(datetime_rfc3339(self.row.created_at)))),
            "updatedAt" => Ok(Some(json_str(datetime_rfc3339(self.row.updated_at)))),
            "nextSyncAt" => Ok(Some(optional_json_str(
                self.next_sync_at()?.map(datetime_rfc3339),
            ))),
            _ => self.external_field(field, arguments, actor),
        }
    }
}

impl ChangesetObject {
    fn external_field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        // Hidden changesets only expose the interface fields.
        let Some(repo) = &self.repo else {
            return Ok(None);
        };
        match field {
            "externalID" => Ok(Some(optional_json_str(
                (!self.row.external_id.is_empty()).then(|| self.row.external_id.clone()),
            ))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                RepositoryObject::from_data(repo.clone()).with_batches(self.db.clone()),
            )))),
            "ownedByBatchChange" => Ok(Some(optional_json_str(
                (self.row.owned_by_batch_change_id != 0)
                    .then(|| marshal_batch_change_id(self.row.owned_by_batch_change_id)),
            ))),
            "events" => crate::batches_changeset_read::changeset_field(
                self,
                field,
                arguments,
                actor,
            ),
            "title" => {
                if self.row.is_importing() {
                    return Ok(Some(FieldValue::Null));
                }
                if self.row.published() {
                    return Ok(Some(json_str(self.metadata().title)));
                }
                // Draft changesets read the title from `changeset_specs`
                // (`getBranchSpecDescription`), which is not ported.
                Ok(Some(FieldValue::Null))
            }
            "body" => {
                if self.row.is_importing() {
                    return Ok(Some(FieldValue::Null));
                }
                if self.row.published() {
                    return Ok(Some(json_str(self.metadata().body)));
                }
                Ok(Some(FieldValue::Null))
            }
            "author" => Ok(Some(self.author())),
            // Labels are computed from `changeset_events` in the baseline
            // (`state.ComputeLabels`); the read surface returns an empty list
            // (the field is non-null and accepts `[]`).
            "labels" => {
                let _ = supports_labels(&self.row.external_service_type);
                Ok(Some(FieldValue::List {
                    items: Vec::new(),
                    element_type: Some("ChangesetLabel"),
                }))
            }
            "externalURL" => Ok(Some(match self.external_url() {
                Some(url) => FieldValue::Object(Arc::new(ExternalLinkObject {
                    data: ExternalLinkData {
                        url,
                        service_kind: type_to_kind(&self.row.external_service_type),
                        service_type: self.row.external_service_type.clone(),
                    },
                })),
                None => FieldValue::Null,
            })),
            "forkNamespace" => Ok(Some(optional_json_str(
                (!self.row.external_fork_namespace.is_empty())
                    .then(|| self.row.external_fork_namespace.clone()),
            ))),
            "forkName" => Ok(Some(optional_json_str(
                (!self.row.external_fork_name.is_empty())
                    .then(|| self.row.external_fork_name.clone()),
            ))),
            "commitVerification" => crate::batches_changeset_read::changeset_field(
                self,
                field,
                arguments,
                actor,
            ),
            "reviewState" => Ok(Some(optional_json_str(
                self.row
                    .published()
                    .then(|| self.row.external_review_state.clone()),
            ))),
            "diff" => crate::batches_changeset_read::changeset_field(
                self,
                field,
                arguments,
                actor,
            ),
            "diffStat" => Ok(Some(match self.row.diff_stat() {
                Some((added, deleted)) => FieldValue::Object(Arc::new(DiffStatObject {
                    added,
                    deleted,
                })),
                None => FieldValue::Null,
            })),
            "checkState" => Ok(Some(optional_json_str(
                if self.row.published() && self.row.external_check_state != "UNKNOWN" {
                    Some(self.row.external_check_state.clone())
                } else {
                    None
                },
            ))),
            "error" => Ok(Some(optional_json_str(
                self.row
                    .failure_message
                    .clone()
                    .or_else(|| self.row.previous_failure_message.clone()),
            ))),
            "syncerError" => Ok(Some(optional_json_str(self.row.syncer_error.clone()))),
            "scheduleEstimateAt" => Ok(Some(FieldValue::Null)),
            "currentSpec" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `graphqlbackend.PersonResolver` narrowed to the metadata-derived fields.
/// Resolving the matching Sourcegraph user by verified email and the dotcom
/// email rewrite are not ported (phase 1).
pub struct PersonObject {
    pub name: String,
    pub email: String,
}

impl ObjectValue for PersonObject {
    fn type_name(&self) -> &'static str {
        "Person"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(json_str(self.name.clone()))),
            "email" => Ok(Some(json_str(self.email.clone()))),
            "displayName" => {
                let name = self.name.trim();
                let display = if !name.is_empty() {
                    name.to_owned()
                } else if !self.email.is_empty() {
                    self.email.clone()
                } else {
                    "unknown".to_owned()
                };
                Ok(Some(json_str(display)))
            }
            "avatarURL" => Ok(Some(FieldValue::Null)),
            "user" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `batches.graphql` `ChangesetLabel`.
pub struct ChangesetLabelObject {
    pub label: ChangesetLabelData,
}

impl ObjectValue for ChangesetLabelObject {
    fn type_name(&self) -> &'static str {
        "ChangesetLabel"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "text" => Ok(Some(json_str(self.label.name.clone()))),
            "color" => Ok(Some(json_str(self.label.color.clone()))),
            "description" => Ok(Some(optional_json_str(
                (!self.label.description.is_empty()).then(|| self.label.description.clone()),
            ))),
            _ => Ok(None),
        }
    }
}

/// `graphqlbackend.DiffStat` (`repository_comparison.go:670-683`).
pub struct DiffStatObject {
    pub added: i32,
    pub deleted: i32,
}

impl ObjectValue for DiffStatObject {
    fn type_name(&self) -> &'static str {
        "DiffStat"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "added" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.added))))),
            "deleted" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.deleted,
            ))))),
            _ => Ok(None),
        }
    }
}

/// Empty `xConnection` (changeset events are out of scope for phase 1).
pub struct EmptyConnectionObject {
    pub type_name: &'static str,
}

impl ObjectValue for EmptyConnectionObject {
    fn type_name(&self) -> &'static str {
        self.type_name
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: None,
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                false, None,
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Changeset connection
// ---------------------------------------------------------------------------

pub struct ChangesetConnectionObject {
    db: BatchesHandle,
    filter: ChangesetFilter,
    safe: bool,
    actor: Actor,
    page: OnceLock<Result<ChangesetPage, String>>,
    total: OnceLock<Result<i64, String>>,
}

impl ChangesetConnectionObject {
    pub fn new(db: BatchesHandle, filter: ChangesetFilter, safe: bool, actor: Actor) -> Self {
        Self {
            db,
            filter,
            safe,
            actor,
            page: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn page(&self) -> Result<&ChangesetPage, String> {
        self.page
            .get_or_init(|| {
                let mut filter = self.filter.clone();
                if !self.safe {
                    filter.enforce_authz = true;
                }
                self.db.list_changesets(&filter, self.actor)
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn total(&self) -> Result<i64, String> {
        self.total
            .get_or_init(|| {
                let mut filter = self.filter.clone();
                filter.enforce_authz = !self.safe;
                self.db.count_changesets(&filter, self.actor)
            })
            .clone()
    }
}

impl ObjectValue for ChangesetConnectionObject {
    fn type_name(&self) -> &'static str {
        "ChangesetConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let page = self.page().map_err(ExecError::new)?;
                let repo_ids: Vec<i32> = page.rows.iter().map(|row| row.repo_id).collect();
                let repos = self
                    .db
                    .repos_by_ids(&repo_ids, self.actor)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: page
                        .rows
                        .iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(ChangesetObject::new(
                                row.clone(),
                                self.db.clone(),
                                repos.get(&row.repo_id).cloned(),
                            )))
                        })
                        .collect(),
                    element_type: Some("Changeset"),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total().map_err(ExecError::new)?,
            )))),
            "pageInfo" => {
                let next = self.page().map_err(ExecError::new)?.next;
                let (has_next, cursor) = if next > 0 {
                    (true, Some(next.to_string()))
                } else {
                    (false, None)
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root / nested dispatch
// ---------------------------------------------------------------------------

fn batch_changes(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
    scope: BatchChangeScope,
) -> Result<FieldValue, ExecError> {
    let filter = parse_batch_change_filter(db.as_ref(), arguments, actor, scope)?;
    Ok(FieldValue::Object(Arc::new(
        BatchChangeConnectionObject::new(db.clone(), filter, actor),
    )))
}

fn batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let _ = actor;
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }
    let Some(namespace) = arguments.string("namespace") else {
        return Err(ExecError::new("invalid ID \"\" for namespace"));
    };
    let namespace = unmarshal_namespace(namespace).map_err(ExecError::new)?;
    let name = arguments.string("name").unwrap_or_default().to_owned();
    let filter = BatchChangeFilter {
        namespace_user_id: namespace.user,
        namespace_org_id: namespace.org,
        name: Some(name),
        ..BatchChangeFilter::default()
    };
    match db.get_batch_change(&filter).map_err(ExecError::new)? {
        BatchChangeLookup::Found(row) => Ok(FieldValue::Object(Arc::new(
            BatchChangeObject::new(row, db.clone()),
        ))),
        BatchChangeLookup::NotFound => Ok(FieldValue::Null),
        BatchChangeLookup::DeletedNamespace => Err(ExecError::new("namespace has been deleted")),
    }
}

/// `Query.batchChanges` / `Query.batchChange`.
pub fn root_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "batchChanges" => batch_changes(db, arguments, actor, BatchChangeScope::default())?,
        "batchChange" => batch_change(db, arguments, actor)?,
        _ if crate::batches_bulk_mutations::is_root_field(field) => {
            return crate::batches_bulk_mutations::root_field(db, field, arguments, actor)
        }
        _ if crate::batches_credentials::is_root_field(field) => {
            return crate::batches_credentials::root_field(db, field, arguments, actor)
        }
        _ => return crate::batches_changeset_read::root_field(db, field, arguments, actor),
    };
    Ok(Some(value))
}

/// Whether `field` is owned by this surface.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "batchChanges" | "batchChange")
        || crate::batches_bulk_mutations::is_root_field(field)
        || crate::batches_credentials::is_root_field(field)
        || crate::batches_changeset_read::is_root_field(field)
}

/// `User.batchChanges` / `Org.batchChanges`.
pub fn namespace_batch_changes(
    db: &BatchesHandle,
    namespace: NamespaceIds,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes(
        db,
        arguments,
        actor,
        BatchChangeScope {
            namespace: Some(namespace),
            ..BatchChangeScope::default()
        },
    )
}

/// `Repository.batchChanges`.
pub fn repository_batch_changes(
    db: &BatchesHandle,
    repo_id: i32,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes(
        db,
        arguments,
        actor,
        BatchChangeScope {
            repo_id: Some(repo_id),
            ..BatchChangeScope::default()
        },
    )
}

fn node_batch_change(
    db: &BatchesHandle,
    id: &str,
    _actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }
    let id = unmarshal_relay_int(id).map_err(ExecError::new)?;
    if id == 0 {
        return Err(ExecError::new("invalid node id"));
    }
    let filter = BatchChangeFilter {
        id,
        ..BatchChangeFilter::default()
    };
    match db.get_batch_change(&filter).map_err(ExecError::new)? {
        BatchChangeLookup::Found(row) => Ok(Some(FieldValue::Object(Arc::new(
            BatchChangeObject::new(row, db.clone()),
        )))),
        BatchChangeLookup::NotFound => Ok(Some(FieldValue::Null)),
        BatchChangeLookup::DeletedNamespace => Err(ExecError::new("namespace has been deleted")),
    }
}

fn node_changeset(
    db: &BatchesHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }
    let id = unmarshal_relay_int(id).map_err(ExecError::new)?;
    if id == 0 {
        return Err(ExecError::new("invalid node id"));
    }
    let Some(row) = db.get_changeset(id).map_err(ExecError::new)? else {
        return Ok(Some(FieldValue::Null));
    };
    let repos = db
        .repos_by_ids(&[row.repo_id], actor)
        .map_err(ExecError::new)?;
    let repo = repos.get(&row.repo_id).cloned();
    Ok(Some(FieldValue::Object(Arc::new(ChangesetObject::new(
        row,
        db.clone(),
        repo,
    )))))
}

/// `Query.node` kinds `BatchChange` / `Changeset`.
pub fn node_field(
    db: &BatchesHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match kind {
        BATCH_CHANGE_KIND => node_batch_change(db, id, actor),
        CHANGESET_KIND => node_changeset(db, id, actor),
        _ if crate::batches_bulk_mutations::is_node_kind(kind) => {
            crate::batches_bulk_mutations::node_field(db, kind, id, actor)
        }
        _ => Ok(None),
    }
}

/// Whether `kind` is one of the two node kinds (plus `BulkOperation`).
pub fn is_node_kind(kind: &str) -> bool {
    matches!(kind, BATCH_CHANGE_KIND | CHANGESET_KIND)
        || crate::batches_bulk_mutations::is_node_kind(kind)
}
