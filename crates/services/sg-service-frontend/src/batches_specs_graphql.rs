#![forbid(unsafe_code)]

//! Batch Changes GraphQL **spec surface** (M9.26a).
//!
//! Ports the spec-related read paths of
//! `cmd/frontend/internal/batches/resolvers`:
//!
//! - `Query.batchSpecs` (`resolver.go:1449-1498`) and `Query.node` kinds
//!   `BatchSpec`, `ChangesetSpec`, `BatchSpecWorkspace`;
//! - `Mutation.createBatchSpec`, `createChangesetSpec` and
//!   `createChangesetSpecs` (`resolver.go:557-700`);
//! - `BatchChange.currentSpec` / `BatchChange.batchSpecs`
//!   (`batch_change.go:161-169,324-361`);
//! - the read-only `BatchSpec`, `Visible|HiddenChangesetSpec`,
//!   `ExistingChangesetReference`, `GitBranchChangesetDescription` and the
//!   minimal `BatchSpecWorkspace` node.
//!
//! The spec SQL lives in [`crate::batches_specs_store`] behind the
//! [`BatchesSpecsDatabase`] seam (reached through
//! [`BatchesGraphqlDatabase::specs_database`]); auth, namespace and repository
//! visibility reuse the M9.24c read seam.
//!
//! Explicit boundaries (documented in `m9_26a_batches_specs_overview.md`):
//! batch-spec YAML is parsed as JSON only (no YAML parser in the tree); the
//! JSON-schema validation of `lib/batches` is approximated by the name
//! pattern; `BatchSpec.applyPreview`, `workspaceResolution` and
//! `viewerBatchChangesCodeHosts` are not ported; workspace `steps`/`stages`
//! always resolve to an empty list / null.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use serde_json::Value as JsonValue;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::{Duration, OffsetDateTime};

use crate::batches_graphql::{
    unmarshal_namespace, unmarshal_relay_int, unmarshal_relay_string, viewer_can_administer,
    BatchChangeFilter, BatchChangeLookup, BatchChangeObject, BatchesGraphqlDatabase,
    BatchesHandle, NamespaceIds,
};
use crate::batches_mutations::BATCH_CHANGES_DISABLED_FOR_USER;
use crate::graphql_resolvers::{
    datetime_rfc3339, PageInfoObject, RepositoryData, RepositoryObject, UserObject,
};
use crate::search_stream::convert::MinimalRepo;

/// `resolvers.batchSpecIDKind`.
pub const BATCH_SPEC_KIND: &str = "BatchSpec";
/// `resolvers.changesetSpecIDKind`.
pub const CHANGESET_SPEC_KIND: &str = "ChangesetSpec";
/// `resolvers.batchSpecWorkspaceIDKind`.
pub const BATCH_SPEC_WORKSPACE_KIND: &str = "BatchSpecWorkspace";

/// `btypes.BatchSpecTTL`: 7 days.
pub const BATCH_SPEC_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;
/// `btypes.ChangesetSpecTTL`: 2 days.
pub const CHANGESET_SPEC_TTL_SECONDS: i64 = 2 * 24 * 60 * 60;

/// `resolvers.ErrIDIsZero` (`errors.go:17-23`).
pub const ID_IS_ZERO: &str = "invalid node id";
/// `auth.ErrNotAuthenticated`.
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `rbac.ErrNotAuthorized`.
pub const MISSING_BATCH_CHANGES_WRITE: &str = "user is missing permission BATCH_CHANGES#WRITE";
/// `lib/batches.ParseBatchSpec`'s friendlier name-pattern message.
pub const NAME_PATTERN_MESSAGE: &str = "The batch change name can only contain word characters, \
     dots and dashes. No whitespace or newlines allowed.";
/// `batcheslib.ErrHeadBaseMismatch`.
pub const HEAD_BASE_MISMATCH: &str = "headRepository does not match baseRepository";
/// `batcheslib.ErrNoCommits`.
pub const NO_COMMITS: &str = "changeset description doesn't contain commit descriptions";

/// `enterprise.ErrBatchChangesDisabled.Error()` (re-exported for the caller).
pub use crate::batches_graphql::BATCH_CHANGES_DISABLED;

// ---------------------------------------------------------------------------
// Rows / filters
// ---------------------------------------------------------------------------

/// One full `batch_specs` row (`batchSpecColumns`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSpecRow {
    pub id: i64,
    pub rand_id: String,
    pub raw_spec: String,
    /// Canonicalized `batcheslib.BatchSpec` JSON (Go struct field order).
    pub spec: String,
    pub name: String,
    pub description: String,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub user_id: i32,
    pub created_from_raw: bool,
    pub allow_unsupported: bool,
    pub allow_ignored: bool,
    pub no_cache: bool,
    pub batch_change_id: i64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl BatchSpecRow {
    pub fn namespace(&self) -> NamespaceIds {
        NamespaceIds {
            user: self.namespace_user_id,
            org: self.namespace_org_id,
        }
    }

    /// `btypes.BatchSpec.ExpiresAt`.
    pub fn expires_at(&self) -> OffsetDateTime {
        self.created_at + Duration::seconds(BATCH_SPEC_TTL_SECONDS)
    }
}

/// `store.ListBatchSpecsOpts` / `CountBatchSpecsOpts` (read predicates).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchSpecFilter {
    pub limit: i64,
    pub cursor: i64,
    pub batch_change_id: i64,
    pub newest_first: bool,
    pub exclude_created_from_raw_not_owned_by_user: i32,
    pub include_locally_executed_specs: bool,
    pub exclude_empty_specs: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchSpecPage {
    pub rows: Vec<BatchSpecRow>,
    pub next: i64,
}

/// `btypes.BatchSpecStats` (`store.GetBatchSpecStats`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchSpecStats {
    pub resolution_done: bool,
    pub workspaces: i64,
    pub skipped_workspaces: i64,
    pub cached_workspaces: i64,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub executions: i64,
    pub completed: i64,
    pub processing: i64,
    pub queued: i64,
    pub failed: i64,
    pub canceled: i64,
    pub canceling: i64,
}

/// One full `changeset_specs` row (`changesetSpecColumns`).
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetSpecRow {
    pub id: i64,
    pub rand_id: String,
    pub batch_spec_id: i64,
    pub repo_id: i32,
    pub user_id: i32,
    pub diff_stat_added: i32,
    pub diff_stat_deleted: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub fork_namespace: Option<String>,
    pub external_id: Option<String>,
    pub head_ref: Option<String>,
    pub title: Option<String>,
    pub base_rev: Option<String>,
    pub base_ref: Option<String>,
    pub body: Option<String>,
    pub published: Option<JsonValue>,
    pub diff: Vec<u8>,
    pub commit_message: Option<String>,
    pub commit_author_name: Option<String>,
    pub commit_author_email: Option<String>,
    /// `branch` or `existing`.
    pub kind: String,
}

impl ChangesetSpecRow {
    /// `ChangesetSpec.Type()`: `BRANCH` / `EXISTING`.
    pub fn graphql_type(&self) -> &'static str {
        if self.kind == "existing" {
            "EXISTING"
        } else {
            "BRANCH"
        }
    }

    /// `btypes.ChangesetSpec.ExpiresAt`.
    pub fn expires_at(&self) -> OffsetDateTime {
        self.created_at + Duration::seconds(CHANGESET_SPEC_TTL_SECONDS)
    }

    pub fn is_fork(&self) -> bool {
        self.fork_namespace.is_some()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangesetSpecFilter {
    pub limit: i64,
    pub cursor: i64,
    pub batch_spec_id: i64,
    pub rand_ids: Vec<String>,
    pub ids: Vec<i64>,
    /// `EXISTING` or `BRANCH` (empty = no filter).
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChangesetSpecPage {
    pub rows: Vec<ChangesetSpecRow>,
    pub next: i64,
}

/// One unhydrated `btypes.RewirerMapping` (`GetRewirerMappings` scan).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewirerMappingRow {
    pub changeset_spec_id: i64,
    pub changeset_id: i64,
    pub repo_id: i32,
}

/// One `batch_change_ids` entry (`btypes.BatchChangeAssoc`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchChangeAssocRow {
    pub batch_change_id: i64,
    pub detach: bool,
    pub archive: bool,
    pub is_archived: bool,
}

/// The `changesets` fields the apply-preview dry-run planner reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyChangesetRow {
    pub id: i64,
    pub repo_id: i32,
    pub external_id: String,
    pub external_service_type: String,
    pub external_state: String,
    pub publication_state: String,
    pub owned_by_batch_change_id: i64,
    pub current_spec_id: i64,
    pub previous_spec_id: i64,
    pub reconciler_state: String,
    pub closing: bool,
    pub ui_publication_state: Option<String>,
    pub computed_state: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub batch_changes: Vec<BatchChangeAssocRow>,
}

impl ApplyChangesetRow {
    /// `btypes.Changeset.Published`.
    pub fn published(&self) -> bool {
        self.publication_state == "PUBLISHED"
    }

    /// `btypes.Changeset.Unpublished`.
    pub fn unpublished(&self) -> bool {
        self.publication_state == "UNPUBLISHED"
    }

    /// `btypes.Changeset.AttachedTo`.
    pub fn attached_to(&self, batch_change_id: i64) -> bool {
        self.batch_changes
            .iter()
            .any(|assoc| assoc.batch_change_id == batch_change_id)
    }

    /// `btypes.Changeset.ArchivedIn`.
    pub fn archived_in(&self, batch_change_id: i64) -> bool {
        self.batch_changes
            .iter()
            .any(|assoc| assoc.batch_change_id == batch_change_id && assoc.is_archived)
    }

    /// `btypes.Changeset.Closeable`.
    pub fn closeable(&self) -> bool {
        !matches!(
            self.external_state.as_str(),
            "CLOSED" | "MERGED" | "READ_ONLY" | "READONLY"
        )
    }

    /// `btypes.Changeset.SupportsDraft`.
    pub fn supports_draft(&self) -> bool {
        matches!(
            self.external_service_type.as_str(),
            "github" | "gitlab" | "azureDevOps" | "azuredevops" | "gerrit"
        )
    }
}

/// `batch_spec_workspaces` row plus the latest execution job.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecWorkspaceRow {
    pub id: i64,
    pub batch_spec_id: i64,
    pub repo_id: i32,
    pub branch: String,
    pub commit: String,
    pub path: String,
    pub file_matches: Vec<String>,
    pub only_fetch_workspace: bool,
    pub unsupported: bool,
    pub ignored: bool,
    pub skipped: bool,
    pub cached_result_found: bool,
    /// `btypes.BatchSpecWorkspace.ChangesetSpecIDs`.
    pub changeset_spec_ids: Vec<i64>,
    /// `btypes.BatchSpecWorkspace.StepCacheResults` (jsonb, 1-based keys).
    pub step_cache_results: JsonValue,
    /// Latest execution job, when present.
    pub execution: Option<BatchSpecWorkspaceExecutionRow>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Subset of `batch_spec_workspace_execution_jobs` read by GraphQL.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecWorkspaceExecutionRow {
    pub id: i64,
    pub state: String,
    pub cancel: bool,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub place_in_user_queue: Option<i64>,
    pub place_in_global_queue: Option<i64>,
    pub execution_logs: JsonValue,
    pub worker_hostname: String,
    pub version: i32,
}

/// `store.ListBatchSpecWorkspacesOpts` (read predicates).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchSpecWorkspaceFilter {
    pub limit: i64,
    pub cursor: i64,
    pub batch_spec_id: i64,
    /// Lowercased execution job state (empty = no filter).
    pub state: Option<String>,
    pub only_cached_or_completed: bool,
    pub only_without_execution_and_not_cached: bool,
    pub cancel: Option<bool>,
    pub skipped: Option<bool>,
    pub search: Vec<TextSearchTerm>,
}

/// One parsed `search.TextSearchTerm` (repo name searches only).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextSearchTerm {
    pub negated: bool,
    pub value: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchSpecWorkspacePage {
    pub rows: Vec<BatchSpecWorkspaceRow>,
    pub next: i64,
}

/// `btypes.BatchSpecResolutionJob`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSpecResolutionJobRow {
    pub id: i64,
    pub batch_spec_id: i64,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl BatchSpecResolutionJobRow {
    /// `BatchSpecResolutionJobState.ToGraphQL` (uppercased).
    pub fn state(&self) -> String {
        self.state.to_uppercase()
    }
}

impl BatchSpecWorkspaceRow {
    /// `batchSpecWorkspaceResolver.State`.
    pub fn state(&self) -> String {
        if self.cached_result_found {
            return "COMPLETED".to_owned();
        }
        if self.skipped {
            return "SKIPPED".to_owned();
        }
        let Some(execution) = self.execution.as_ref() else {
            return "PENDING".to_owned();
        };
        if execution.cancel && execution.state == "processing" {
            return "CANCELING".to_owned();
        }
        execution.state.to_uppercase()
    }

    /// `batchSpecWorkspaceResolver.StepCacheResultCount`.
    pub fn step_cache_result_count(&self) -> i64 {
        self.step_cache_results
            .as_object()
            .map(|map| map.len() as i64)
            .unwrap_or(0)
    }
}

/// A fully parsed, ready-to-insert changeset spec (`btypes.ChangesetSpec`).
#[derive(Debug, Clone, PartialEq)]
pub struct NewChangesetSpec {
    pub rand_id: String,
    pub repo_id: i32,
    pub user_id: i32,
    pub diff_stat_added: i32,
    pub diff_stat_deleted: i32,
    pub fork_namespace: Option<String>,
    pub external_id: Option<String>,
    pub head_ref: Option<String>,
    pub title: Option<String>,
    pub base_rev: Option<String>,
    pub base_ref: Option<String>,
    pub body: Option<String>,
    pub published: Option<JsonValue>,
    pub diff: Vec<u8>,
    pub commit_message: Option<String>,
    pub commit_author_name: Option<String>,
    pub commit_author_email: Option<String>,
    /// `branch` / `existing`.
    pub kind: String,
}

/// `service.CreateBatchSpecOpts` after parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct NewBatchSpec {
    pub rand_id: String,
    pub raw_spec: String,
    /// Canonicalized spec JSON.
    pub spec: String,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub user_id: i32,
    pub changeset_spec_rand_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Spec-side store seam (M9.26a). Reached through
/// `BatchesGraphqlDatabase::specs_database`.
pub trait BatchesSpecsDatabase: Send + Sync {
    /// `store.Clock`.
    fn now(&self) -> OffsetDateTime;

    /// `store.CreateBatchSpec` (+ `UpdateChangesetSpecBatchSpecID` in one tx).
    fn create_batch_spec(&self, spec: &NewBatchSpec) -> Result<BatchSpecRow, String>;

    /// `store.GetBatchSpec`.
    fn get_batch_spec(
        &self,
        id: i64,
        rand_id: &str,
        exclude_created_from_raw_not_owned_by_user: i32,
    ) -> Result<Option<BatchSpecRow>, String>;

    /// `store.ListBatchSpecs`.
    fn list_batch_specs(&self, filter: &BatchSpecFilter) -> Result<BatchSpecPage, String>;

    /// `store.CountBatchSpecs`.
    fn count_batch_specs(&self, filter: &BatchSpecFilter) -> Result<i64, String>;

    /// `store.GetBatchSpecStats` for one id.
    fn batch_spec_stats(&self, id: i64) -> Result<Option<BatchSpecStats>, String>;

    /// `store.GetBatchSpecDiffStat` (repo-authz filtered).
    fn batch_spec_diff_stat(&self, id: i64, actor: Actor) -> Result<(i64, i64), String>;

    /// `store.GetBatchSpecResolutionJob.FailureMessage` (None when there is
    /// no resolution job or it succeeded).
    fn batch_spec_resolution_failure(&self, id: i64) -> Result<Option<String>, String>;

    /// `store.ListBatchSpecWorkspaceExecutionJobs` with
    /// `OnlyWithFailureMessage: true, Cancel: false, ExcludeRank: true`
    /// (ordered by job id).
    fn batch_spec_execution_failure_messages(&self, id: i64) -> Result<Vec<String>, String>;

    /// `store.ListChangesetSpecs`.
    fn list_changeset_specs(
        &self,
        filter: &ChangesetSpecFilter,
    ) -> Result<ChangesetSpecPage, String>;

    /// `store.CountChangesetSpecs`.
    fn count_changeset_specs(&self, filter: &ChangesetSpecFilter) -> Result<i64, String>;

    /// `store.GetChangesetSpec`.
    fn get_changeset_spec(&self, id: i64, rand_id: &str) -> Result<Option<ChangesetSpecRow>, String>;

    /// `store.CreateChangesetSpec` (one transaction).
    fn create_changeset_specs(&self, specs: &[NewChangesetSpec]) -> Result<Vec<ChangesetSpecRow>, String>;

    /// `store.GetBatchSpecWorkspace`.
    fn get_batch_spec_workspace(
        &self,
        id: i64,
    ) -> Result<Option<BatchSpecWorkspaceRow>, String>;

    /// `store.ListBatchSpecWorkspaces`.
    fn list_batch_spec_workspaces(
        &self,
        filter: &BatchSpecWorkspaceFilter,
    ) -> Result<BatchSpecWorkspacePage, String>;

    /// `store.CountBatchSpecWorkspaces`.
    fn count_batch_spec_workspaces(&self, filter: &BatchSpecWorkspaceFilter)
        -> Result<i64, String>;

    /// `store.GetBatchSpecResolutionJob` (None when `ErrNoResults`).
    fn get_batch_spec_resolution_job(
        &self,
        batch_spec_id: i64,
    ) -> Result<Option<BatchSpecResolutionJobRow>, String>;

    /// `store.GetNewestBatchSpec` (newest by namespace+name+creator).
    fn get_newest_batch_spec(
        &self,
        user_id: i32,
        namespace_user_id: i32,
        namespace_org_id: i32,
        name: &str,
    ) -> Result<Option<BatchSpecRow>, String>;

    /// `store.ListChangesetSpecsWithConflictingHeadRef` grouped counts:
    /// `(repo_id, head_ref, count)`.
    fn conflicting_head_refs(&self, batch_spec_id: i64) -> Result<Vec<(i32, String, i64)>, String>;

    /// `store.GetRewirerMappings` (unhydrated; SQL order: changeset spec id,
    /// changeset id). Text search and `currentState` follow Go's three-branch
    /// UNION.
    fn get_rewirer_mappings(
        &self,
        batch_spec_id: i64,
        batch_change_id: i64,
        text_search: &[crate::batches_graphql::TextSearchTerm],
        current_state: Option<&str>,
    ) -> Result<Vec<RewirerMappingRow>, String>;

    /// `store.ListChangesets{IDs}` projection used by the dry-run planner.
    fn changesets_for_apply(&self, ids: &[i64]) -> Result<Vec<ApplyChangesetRow>, String>;

    /// `store.ListBatchSpecRepoIDs` (authz-filtered, `DISTINCT repo.id`).
    fn list_batch_spec_repo_ids(&self, batch_spec_id: i64, actor: Actor) -> Result<Vec<i32>, String>;

    /// `store.ListCodeHosts` for a repo-id subset (only supported hosts).
    fn code_hosts_for_repos(
        &self,
        repo_ids: &[i32],
        only_without_webhooks: bool,
    ) -> Result<Vec<crate::batches_changeset_read::CodeHostRow>, String>;
}

pub type BatchesSpecsHandle = Arc<dyn BatchesSpecsDatabase + Send + Sync>;

fn specs(db: &BatchesHandle) -> Result<&dyn BatchesSpecsDatabase, ExecError> {
    db.specs_database()
        .ok_or_else(|| ExecError::new("batch specs store is not configured"))
}

// ---------------------------------------------------------------------------
// Relay IDs
// ---------------------------------------------------------------------------

/// `relay.MarshalID("BatchSpec", randID)`.
pub fn marshal_batch_spec_id(rand_id: &str) -> String {
    marshal_relay_string_id(BATCH_SPEC_KIND, rand_id)
}

/// `relay.MarshalID("ChangesetSpec", randID)`.
pub fn marshal_changeset_spec_id(rand_id: &str) -> String {
    marshal_relay_string_id(CHANGESET_SPEC_KIND, rand_id)
}

/// `relay.MarshalID("BatchSpecWorkspace", id)`.
pub fn marshal_batch_spec_workspace_id(id: i64) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE.encode(format!("{BATCH_SPEC_WORKSPACE_KIND}:{id}"))
}

fn marshal_relay_string_id(kind: &str, value: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE
        .encode(format!("{kind}:{}", serde_json::json!(value)))
}

// ---------------------------------------------------------------------------
// Spec parsing (lib/batches)
// ---------------------------------------------------------------------------

/// Top-level `batcheslib.BatchSpec` JSON field order.
const BATCH_SPEC_FIELDS: &[&str] = &[
    "version",
    "name",
    "description",
    "on",
    "workspaces",
    "steps",
    "transformChanges",
    "importChangesets",
    "changesetTemplate",
];

fn is_empty_json(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => true,
        JsonValue::String(value) => value.is_empty(),
        JsonValue::Array(items) => items.is_empty(),
        JsonValue::Number(number) => number.as_i64() == Some(0) || number.as_f64() == Some(0.0),
        JsonValue::Object(entries) => entries.is_empty(),
        JsonValue::Bool(value) => !*value,
    }
}

/// Re-emit a parsed batch spec in `batcheslib.BatchSpec` field order with
/// `omitempty` semantics. Unknown fields are dropped (Go unmarshals into the
/// typed struct). Nested values keep `serde_json`'s sorted key order, which
/// matches Go only for maps — struct-shaped nested values are a documented
/// approximation.
pub fn canonical_batch_spec(value: &JsonValue) -> Result<String, String> {
    let JsonValue::Object(entries) = value else {
        return Err("json: cannot unmarshal into Go value of type batches.BatchSpec".to_owned());
    };
    let mut ordered: Vec<(String, JsonValue)> = Vec::new();
    for field in BATCH_SPEC_FIELDS {
        let Some(value) = entries.get(*field) else {
            continue;
        };
        if is_empty_json(value) {
            continue;
        }
        ordered.push(((*field).to_owned(), value.clone()));
    }
    // `serde_json::Map` without `preserve_order` is a `BTreeMap`, but we
    // serialize an explicit list here, so order is the Go struct order.
    let text = serde_json::to_string(&ordered).map_err(|error| error.to_string())?;
    // `serde_json::to_string` of `Vec<(String, Value)>` yields `[[k,v],...]`;
    // build the object by hand instead.
    let _ = text;
    let mut out = String::from("{");
    for (index, (key, value)) in ordered.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(key).map_err(|error| error.to_string())?);
        out.push(':');
        out.push_str(&serde_json::to_string(value).map_err(|error| error.to_string())?);
    }
    out.push('}');
    Ok(out)
}

/// `batcheslib.ParseBatchSpec` reduced to JSON parsing, `name` extraction and
/// the friendly name-pattern error.
pub fn parse_batch_spec(raw: &str) -> Result<(String, String, String), String> {
    let value: JsonValue = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse batch spec: {error}"))?;
    let canonical = canonical_batch_spec(&value)?;
    let object = value.as_object().expect("checked object");
    let name = object
        .get("name")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned();
    let description = object
        .get("description")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned();
    if name.is_empty() || !valid_batch_change_name(&name) {
        return Err(NAME_PATTERN_MESSAGE.to_owned());
    }
    Ok((canonical, name, description))
}

/// `^[\w.-]+$` (Go RE2 `\w` = `[0-9A-Za-z_]`).
pub fn valid_batch_change_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

fn decode_repo_id(raw: &str) -> Result<i32, String> {
    Ok(unmarshal_relay_int(raw)? as i32)
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedCommit {
    message: String,
    diff: String,
    author_name: String,
    author_email: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedChangesetSpecInput {
    base_repo_id: i32,
    external_id: String,
    base_rev: String,
    base_ref: String,
    head_repo_id: i32,
    head_ref: String,
    title: String,
    body: String,
    fork: Option<bool>,
    published: Option<JsonValue>,
    commits: Vec<ParsedCommit>,
}

fn parse_changeset_spec_input(raw: &str) -> Result<ParsedChangesetSpecInput, String> {
    let value: JsonValue = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse changeset spec: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "changeset spec must be a JSON object".to_owned())?;
    let field = |name: &str| -> String {
        object
            .get(name)
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let base_repository = field("baseRepository");
    let base_repo_id = decode_repo_id(&base_repository)?;
    let external_id = field("externalID");
    let head_repository = field("headRepository");
    let head_repo_id = if head_repository.is_empty() {
        0
    } else {
        decode_repo_id(&head_repository)?
    };
    if !head_repository.is_empty() && !base_repository.is_empty() && head_repository != base_repository
    {
        return Err(HEAD_BASE_MISMATCH.to_owned());
    }
    let mut commits = Vec::new();
    if let Some(JsonValue::Array(items)) = object.get("commits") {
        for item in items {
            let Some(commit) = item.as_object() else {
                continue;
            };
            commits.push(ParsedCommit {
                message: commit
                    .get("message")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                diff: commit
                    .get("diff")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                author_name: commit
                    .get("authorName")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                author_email: commit
                    .get("authorEmail")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
    }
    let published = match object.get("published") {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Bool(value)) => Some(JsonValue::Bool(*value)),
        Some(JsonValue::String(value)) if value == "draft" => Some(JsonValue::String("draft".into())),
        Some(other) => {
            return Err(format!(
                "invalid PublishedValue: {}",
                serde_json::to_string(other).unwrap_or_default()
            ))
        }
    };
    Ok(ParsedChangesetSpecInput {
        base_repo_id,
        external_id,
        base_rev: field("baseRev"),
        base_ref: field("baseRef"),
        head_repo_id,
        head_ref: field("headRef"),
        title: field("title"),
        body: field("body"),
        fork: object.get("fork").and_then(JsonValue::as_bool),
        published,
        commits,
    })
}

/// Build the `btypes.ChangesetSpec` from the parsed input, mirroring
/// `NewChangesetSpecFromSpec` and `computeDiffStat`.
fn new_changeset_spec(
    rand_id: String,
    user_id: i32,
    input: &ParsedChangesetSpecInput,
) -> Result<NewChangesetSpec, String> {
    let importing = !input.external_id.is_empty();
    let (kind, head_ref, base_rev, base_ref, diff, commit_message, author_name, author_email, stat) =
        if importing {
            ("existing", None, None, None, Vec::new(), None, None, None, (0, 0))
        } else {
            if input.head_repo_id != input.base_repo_id {
                return Err(HEAD_BASE_MISMATCH.to_owned());
            }
            let Some(commit) = input.commits.first() else {
                return Err(NO_COMMITS.to_owned());
            };
            let stat = diff_stat(&commit.diff)?;
            (
                "branch",
                Some(input.head_ref.clone()),
                Some(input.base_rev.clone()),
                Some(input.base_ref.clone()),
                commit.diff.as_bytes().to_vec(),
                Some(commit.message.clone()),
                Some(commit.author_name.clone()),
                Some(commit.author_email.clone()),
                stat,
            )
        };
    let fork_namespace = match input.fork {
        Some(true) => Some("<user>".to_owned()),
        _ => None,
    };
    Ok(NewChangesetSpec {
        rand_id,
        repo_id: input.base_repo_id,
        user_id,
        diff_stat_added: stat.0,
        diff_stat_deleted: stat.1,
        fork_namespace,
        external_id: (!input.external_id.is_empty()).then(|| input.external_id.clone()),
        head_ref,
        title: Some(input.title.clone()),
        base_rev,
        base_ref,
        body: Some(input.body.clone()),
        published: input.published.clone(),
        diff,
        commit_message,
        commit_author_name: author_name,
        commit_author_email: author_email,
        kind: kind.to_owned(),
    })
}

/// `go-diff` `Hunk.Stat` / `FileDiff.Stat`: added/deleted/changed lines.
pub fn diff_stat(diff: &str) -> Result<(i32, i32), String> {
    let (mut added, mut deleted, mut changed) = (0i32, 0i32, 0i32);
    let (mut hunk_added, mut hunk_deleted, mut hunk_changed) = (0i32, 0i32, 0i32);
    let mut in_hunk = false;
    let mut last = 0u8;
    for line in diff.split('\n') {
        if line.starts_with("@@") {
            added += hunk_added;
            deleted += hunk_deleted;
            changed += hunk_changed;
            hunk_added = 0;
            hunk_deleted = 0;
            hunk_changed = 0;
            in_hunk = true;
            last = 0;
            continue;
        }
        if line.starts_with("diff --git") {
            added += hunk_added;
            deleted += hunk_deleted;
            changed += hunk_changed;
            hunk_added = 0;
            hunk_deleted = 0;
            hunk_changed = 0;
            in_hunk = false;
            last = 0;
            continue;
        }
        if !in_hunk {
            continue;
        }
        let bytes = line.as_bytes();
        if bytes.is_empty() {
            last = 0;
            continue;
        }
        match bytes[0] {
            b'-' => {
                if last == b'+' {
                    hunk_added -= 1;
                    hunk_changed += 1;
                    last = 0;
                } else {
                    hunk_deleted += 1;
                    last = b'-';
                }
            }
            b'+' => {
                if last == b'-' {
                    hunk_deleted -= 1;
                    hunk_changed += 1;
                    last = 0;
                } else {
                    hunk_added += 1;
                    last = b'+';
                }
            }
            _ => last = 0,
        }
    }
    added += hunk_added;
    deleted += hunk_deleted;
    changed += hunk_changed;
    Ok((added + changed, deleted + changed))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn json_str(value: impl Into<String>) -> FieldValue {
    FieldValue::Json(Json::Str(value.into()))
}

fn optional_str(value: Option<&str>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.to_owned())),
        None => FieldValue::Null,
    }
}

fn validate_first_param(first: i64) -> Result<(), String> {
    crate::batches_graphql::validate_first_param(first)
}

fn namespace_url(db: &dyn BatchesGraphqlDatabase, namespace: NamespaceIds) -> Result<String, String> {
    if namespace.user != 0 {
        let user = db
            .user_by_id(namespace.user)?
            .ok_or_else(|| "namespace of batch spec has been deleted".to_owned())?;
        Ok(format!("/users/{}", user.username))
    } else if namespace.org != 0 {
        let org = db
            .org_by_id(namespace.org)?
            .ok_or_else(|| "namespace of batch spec has been deleted".to_owned())?;
        Ok(format!("/organizations/{}", org.name))
    } else {
        Err("namespace of batch spec has been deleted".to_owned())
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
            .ok_or_else(|| ExecError::new("namespace of batch spec has been deleted"))?;
        return Ok(FieldValue::Object(Arc::new(
            UserObject::from_data_as_viewer(user, admin),
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
            .ok_or_else(|| ExecError::new("namespace of batch spec has been deleted"))?;
        org.viewer_can_administer = admin || member;
        org.viewer_is_member = member;
        return Ok(FieldValue::Object(Arc::new(
            crate::graphql_resolvers::OrgObject::from_data_as_viewer(org, admin),
        )));
    }
    Err(ExecError::new("namespace of batch spec has been deleted"))
}

fn repository_object(
    db: &BatchesHandle,
    data: RepositoryData,
) -> FieldValue {
    FieldValue::Object(Arc::new(
        RepositoryObject::from_data(data).with_batches(db.clone()),
    ))
}

fn user_object(db: &BatchesHandle, id: i32, actor: Actor) -> Result<FieldValue, ExecError> {
    if id == 0 {
        return Ok(FieldValue::Null);
    }
    let admin = db.current_user_site_admin(actor).map_err(ExecError::new)? == Some(true);
    Ok(match db.user_by_id(id).map_err(ExecError::new)? {
        Some(user) => FieldValue::Object(Arc::new(UserObject::from_data_as_viewer(user, admin))),
        None => FieldValue::Null,
    })
}

fn is_site_admin(db: &BatchesHandle, actor: Actor) -> bool {
    db.current_user_site_admin(actor).ok().flatten() == Some(true)
}

/// `enterprise.BatchChangesEnabledForUser`.
fn batch_changes_enabled_for_user(db: &BatchesHandle, actor: Actor) -> Result<(), String> {
    if !db.batch_changes_enabled() {
        return Err(BATCH_CHANGES_DISABLED.to_owned());
    }
    if db.batch_changes_restricted_to_admins() {
        if !is_site_admin(db, actor) {
            return Err(BATCH_CHANGES_DISABLED_FOR_USER.to_owned());
        }
    }
    Ok(())
}

/// `batchChangesCreateAccess`: enterprise gate + authenticated check.
fn batch_changes_create_access(db: &BatchesHandle, actor: Actor) -> Result<(), String> {
    batch_changes_enabled_for_user(db, actor)?;
    if !actor.is_authenticated() {
        return Err(NOT_AUTHENTICATED.to_owned());
    }
    Ok(())
}

fn parse_cursor(arguments: &Arguments) -> Result<i64, String> {
    match arguments.string("after") {
        Some(raw) => raw
            .parse::<i64>()
            .map_err(|_| format!("strconv.Atoi: parsing {:?}: invalid syntax", raw)),
        None => Ok(0),
    }
}

// ---------------------------------------------------------------------------
// BatchSpec object
// ---------------------------------------------------------------------------

pub struct BatchSpecObject {
    pub row: BatchSpecRow,
    pub db: BatchesHandle,
}

impl BatchSpecObject {
    pub fn new(row: BatchSpecRow, db: BatchesHandle) -> Self {
        Self { row, db }
    }

    fn stats(&self) -> Result<Option<BatchSpecStats>, ExecError> {
        if !self.row.created_from_raw {
            return Ok(None);
        }
        specs(&self.db)?
            .batch_spec_stats(self.row.id)
            .map_err(ExecError::new)
    }

    /// `btypes.ComputeBatchSpecState` (+ the `ValidateChangesetSpecs` step).
    fn state(&self, actor: Actor) -> Result<&'static str, ExecError> {
        let state = if self.row.created_from_raw {
            let stats = self
                .stats()?
                .ok_or_else(|| ExecError::new("no results"))?;
            compute_batch_spec_state(stats)
        } else {
            "COMPLETED"
        };
        // `computeState`: a completed spec with conflicting head refs is FAILED
        // (this applies to local specs too).
        if state == "COMPLETED" && self.validation_error(actor)?.is_some() {
            return Ok("FAILED");
        }
        Ok(state)
    }

    /// `service.ValidateChangesetSpecs` (head-ref conflicts only).
    fn validation_error(&self, actor: Actor) -> Result<Option<String>, ExecError> {
        let conflicts = specs(&self.db)?
            .conflicting_head_refs(self.row.id)
            .map_err(ExecError::new)?;
        if conflicts.is_empty() {
            return Ok(None);
        }
        let repo_ids: Vec<i32> = conflicts.iter().map(|(repo, _, _)| *repo).collect();
        let repos = self
            .db
            .repos_by_ids(&repo_ids, actor)
            .map_err(ExecError::new)?;
        let mut messages = Vec::new();
        for (repo_id, head_ref, count) in conflicts {
            match repos.get(&repo_id) {
                Some(repo) => messages.push(format!(
                    "{count} changeset specs in {} use the same branch: {head_ref}",
                    repo.name
                )),
                None => messages.push(format!(
                    "{count} changeset specs in the same repository use the same branch: {head_ref}"
                )),
            }
        }
        if messages.len() == 1 {
            return Ok(Some(format!(
                "Validating changeset specs resulted in an error:\n* {}\n",
                messages[0]
            )));
        }
        Ok(Some(format!(
            "{} errors when validating changeset specs:\n{}\n",
            messages.len(),
            messages
                .iter()
                .map(|message| format!("* {message}"))
                .collect::<Vec<_>>()
                .join("\n")
        )))
    }

    /// `computeBatchSpecState` for the non-`INVALID` branches.
    fn failure_message(&self, actor: Actor) -> Result<FieldValue, ExecError> {
        let failure = specs(&self.db)?
            .batch_spec_resolution_failure(self.row.id)
            .map_err(ExecError::new)?;
        if failure.is_some() {
            return Ok(optional_str(failure.as_deref()));
        }
        if let Some(message) = self.validation_error(actor)? {
            return Ok(json_str(message));
        }
        // `resolver.batchSpecResolver.FailureMessage`: failed/errored workspace
        // execution jobs (canceled jobs are skipped).
        let messages = specs(&self.db)?
            .batch_spec_execution_failure_messages(self.row.id)
            .map_err(ExecError::new)?;
        if messages.is_empty() {
            return Ok(FieldValue::Null);
        }
        let mut out = String::from("Failures:\n\n");
        for message in messages.iter().take(5) {
            out.push_str("* ");
            out.push_str(message);
            out.push('\n');
        }
        if messages.len() > 5 {
            out.push_str(&format!("\nand {} more", messages.len() - 5));
        }
        Ok(json_str(out))
    }

    fn changeset_specs_connection(
        &self,
        arguments: &Arguments,
        actor: Actor,
        kind: Option<&str>,
    ) -> Result<FieldValue, ExecError> {
        let first = arguments.i64("first").unwrap_or(50);
        validate_first_param(first).map_err(ExecError::new)?;
        let cursor = parse_cursor(arguments).map_err(ExecError::new)?;
        Ok(FieldValue::Object(Arc::new(ChangesetSpecConnectionObject::new(
            self.db.clone(),
            ChangesetSpecFilter {
                limit: first,
                cursor,
                batch_spec_id: self.row.id,
                kind: kind.map(str::to_owned),
                ..ChangesetSpecFilter::default()
            },
            actor,
        ))))
    }
}

fn state_started(state: &str) -> bool {
    !matches!(state, "QUEUED" | "PENDING")
}

fn state_finished(state: &str) -> bool {
    matches!(state, "COMPLETED" | "FAILED" | "ERRORED" | "CANCELED")
}

fn compute_batch_spec_state(stats: BatchSpecStats) -> &'static str {
    if !stats.resolution_done {
        return "PENDING";
    }
    if stats.workspaces == 0 || stats.skipped_workspaces == stats.workspaces {
        return "COMPLETED";
    }
    if stats.executions == 0 {
        return "PENDING";
    }
    if stats.queued == stats.executions {
        return "QUEUED";
    }
    if stats.completed == stats.executions {
        return "COMPLETED";
    }
    if stats.canceled == stats.executions {
        return "CANCELED";
    }
    if stats.failed + stats.completed == stats.executions {
        return "FAILED";
    }
    if stats.canceled + stats.failed + stats.completed == stats.executions {
        return "CANCELED";
    }
    if stats.canceling + stats.failed + stats.completed + stats.canceled == stats.executions {
        return "CANCELING";
    }
    if stats.canceling > 0 || stats.processing > 0 {
        return "PROCESSING";
    }
    if (stats.completed > 0 || stats.failed > 0 || stats.canceled > 0) && stats.queued > 0 {
        return "PROCESSING";
    }
    "INVALID"
}

impl ObjectValue for BatchSpecObject {
    fn type_name(&self) -> &'static str {
        "BatchSpec"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_str(marshal_batch_spec_id(&self.row.rand_id)))),
            "autoApplyEnabled" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "state" => Ok(Some(json_str(self.state(actor)?))),
            "originalInput" => Ok(Some(json_str(self.row.raw_spec.clone()))),
            "parsedInput" => Ok(Some(FieldValue::Json(Json::Raw(self.row.spec.clone())))),
            "description" => Ok(Some(FieldValue::Object(Arc::new(BatchChangeDescriptionObject {
                name: self.row.name.clone(),
                description: self.row.description.clone(),
            })))),
            "changesetSpecs" => Ok(Some(self.changeset_specs_connection(arguments, actor, None)?)),
            "importingChangesets" => Ok(Some(self.changeset_specs_connection(
                arguments,
                actor,
                Some("EXISTING"),
            )?)),
            "creator" => user_object(&self.db, self.row.user_id, actor).map(Some),
            "createdAt" => Ok(Some(json_str(datetime_rfc3339(self.row.created_at)))),
            "expiresAt" => Ok(Some(json_str(datetime_rfc3339(self.row.expires_at())))),
            "startedAt" => {
                if !self.row.created_from_raw {
                    return Ok(Some(FieldValue::Null));
                }
                if !state_started(self.state(actor)?) {
                    return Ok(Some(FieldValue::Null));
                }
                let started_at = self.stats()?.and_then(|stats| stats.started_at);
                Ok(Some(optional_str(
                    started_at.map(datetime_rfc3339).as_deref(),
                )))
            }
            "finishedAt" => {
                if !self.row.created_from_raw {
                    return Ok(Some(FieldValue::Null));
                }
                if !state_finished(self.state(actor)?) {
                    return Ok(Some(FieldValue::Null));
                }
                let finished_at = self.stats()?.and_then(|stats| stats.finished_at);
                Ok(Some(optional_str(
                    finished_at.map(datetime_rfc3339).as_deref(),
                )))
            }
            "namespace" => Ok(Some(namespace_object(
                self.db.as_ref(),
                actor,
                self.row.namespace(),
            )?)),
            "applyURL" => {
                if self.row.created_from_raw {
                    let state = self.state(actor)?;
                    let finished_ok = matches!(state, "COMPLETED" | "FAILED")
                        && self.validation_error(actor)?.is_none();
                    if !finished_ok {
                        return Ok(Some(FieldValue::Null));
                    }
                }
                let base = namespace_url(self.db.as_ref(), self.row.namespace())
                    .map_err(ExecError::new)?;
                Ok(Some(json_str(format!(
                    "{base}/batch-changes/apply/{}",
                    marshal_batch_spec_id(&self.row.rand_id)
                ))))
            }
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(
                viewer_can_administer(self.db.as_ref(), actor, self.row.namespace())
                    .map_err(ExecError::new)?,
            )))),
            "diffStat" => {
                let (added, deleted) = specs(&self.db)?
                    .batch_spec_diff_stat(self.row.id, actor)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::batches_graphql::DiffStatObject {
                        added: added as i32,
                        deleted: deleted as i32,
                    },
                ))))
            }
            "appliesToBatchChange" => {
                let filter = if self.row.batch_change_id != 0 {
                    BatchChangeFilter {
                        id: self.row.batch_change_id,
                        ..BatchChangeFilter::default()
                    }
                } else {
                    BatchChangeFilter {
                        name: Some(self.row.name.clone()),
                        namespace_user_id: self.row.namespace_user_id,
                        namespace_org_id: self.row.namespace_org_id,
                        ..BatchChangeFilter::default()
                    }
                };
                Ok(Some(match self.db.get_batch_change(&filter).map_err(ExecError::new)? {
                    BatchChangeLookup::Found(row) => FieldValue::Object(Arc::new(
                        BatchChangeObject::new(row, self.db.clone()),
                    )),
                    BatchChangeLookup::NotFound => FieldValue::Null,
                    BatchChangeLookup::DeletedNamespace => FieldValue::Null,
                }))
            }
            "allowUnsupported" => Ok(Some(if self.row.created_from_raw {
                FieldValue::Json(Json::Bool(self.row.allow_unsupported))
            } else {
                FieldValue::Null
            })),
            "allowIgnored" => Ok(Some(if self.row.created_from_raw {
                FieldValue::Json(Json::Bool(self.row.allow_ignored))
            } else {
                FieldValue::Null
            })),
            "noCache" => Ok(Some(if self.row.created_from_raw {
                FieldValue::Json(Json::Bool(self.row.no_cache))
            } else {
                FieldValue::Null
            })),
            "failureMessage" => Ok(Some(self.failure_message(actor)?)),
            "viewerCanRetry" => {
                if !self.row.created_from_raw {
                    return Ok(Some(FieldValue::Json(Json::Bool(false))));
                }
                if !viewer_can_administer(self.db.as_ref(), actor, self.row.namespace())
                    .map_err(ExecError::new)?
                {
                    return Ok(Some(FieldValue::Json(Json::Bool(false))));
                }
                let state = self.state(actor)?;
                Ok(Some(FieldValue::Json(Json::Bool(
                    state != "COMPLETED" && state_finished(state),
                ))))
            }
            "source" => Ok(Some(json_str(if self.row.created_from_raw {
                "REMOTE"
            } else {
                "LOCAL"
            }))),
            "files" => Ok(Some(FieldValue::Object(Arc::new(
                crate::batches_graphql::EmptyConnectionObject {
                    type_name: "BatchSpecWorkspaceFileConnection",
                },
            )))),
            "workspaceResolution" => {
                if !self.row.created_from_raw {
                    return Ok(Some(FieldValue::Null));
                }
                Ok(Some(
                    match specs(&self.db)?
                        .get_batch_spec_resolution_job(self.row.id)
                        .map_err(ExecError::new)?
                    {
                        Some(row) => FieldValue::Object(Arc::new(
                            BatchSpecWorkspaceResolutionObject {
                                row,
                                batch_spec: self.row.clone(),
                                db: self.db.clone(),
                            },
                        )),
                        None => FieldValue::Null,
                    },
                ))
            }
            "supersedingBatchSpec" => {
                let Some(uid) = actor.uid() else {
                    return Err(ExecError::new("user is not authenticated"));
                };
                let namespace = self.row.namespace();
                let newest = specs(&self.db)?
                    .get_newest_batch_spec(uid, namespace.user, namespace.org, &self.row.name)
                    .map_err(ExecError::new)?;
                let Some(newest) = newest else {
                    return Ok(Some(FieldValue::Null));
                };
                if newest.id == self.row.id || newest.user_id != self.row.user_id {
                    return Ok(Some(FieldValue::Null));
                }
                Ok(Some(FieldValue::Object(Arc::new(BatchSpecObject::new(
                    newest,
                    self.db.clone(),
                )))))
            }
            "applyPreview" | "viewerBatchChangesCodeHosts" => {
                crate::batches_apply_preview::batch_spec_field(
                    &self.db,
                    &self.row,
                    field,
                    arguments,
                    actor,
                )
            }
            _ => Ok(None),
        }
    }
}

struct BatchChangeDescriptionObject {
    name: String,
    description: String,
}

impl ObjectValue for BatchChangeDescriptionObject {
    fn type_name(&self) -> &'static str {
        "BatchChangeDescription"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(json_str(self.name.clone()))),
            "description" => Ok(Some(json_str(self.description.clone()))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// ChangesetSpec object
// ---------------------------------------------------------------------------

pub struct ChangesetSpecObject {
    pub row: ChangesetSpecRow,
    pub db: BatchesHandle,
    /// `None` → `HiddenChangesetSpec` (repository filtered by authz).
    pub repo: Option<RepositoryData>,
}

impl ChangesetSpecObject {
    pub fn new(row: ChangesetSpecRow, db: BatchesHandle, repo: Option<RepositoryData>) -> Self {
        Self { row, db, repo }
    }

    fn repo_visible(&self) -> bool {
        self.repo.is_some()
    }

    fn description(&self) -> Result<FieldValue, ExecError> {
        let repo = self
            .repo
            .clone()
            .expect("Description is only reachable on VisibleChangesetSpec");
        if self.row.kind == "existing" {
            return Ok(FieldValue::Object(Arc::new(ExistingChangesetReferenceObject {
                repo,
                external_id: self.row.external_id.clone().unwrap_or_default(),
                db: self.db.clone(),
            })));
        }
        Ok(FieldValue::Object(Arc::new(GitBranchChangesetDescriptionObject {
            repo,
            row: self.row.clone(),
            db: self.db.clone(),
        })))
    }
}

impl ObjectValue for ChangesetSpecObject {
    fn type_name(&self) -> &'static str {
        if self.repo_visible() {
            "VisibleChangesetSpec"
        } else {
            "HiddenChangesetSpec"
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_str(marshal_changeset_spec_id(&self.row.rand_id)))),
            "type" => Ok(Some(json_str(self.row.graphql_type()))),
            "expiresAt" => Ok(Some(json_str(datetime_rfc3339(self.row.expires_at())))),
            "description" if self.repo_visible() => Ok(Some(self.description()?)),
            "forkTarget" if self.repo_visible() => {
                Ok(Some(FieldValue::Object(Arc::new(ForkTargetObject {
                    is_fork: self.row.is_fork(),
                    namespace: self.row.fork_namespace.clone(),
                }))))
            }
            // `ChangesetSpecResolver.Workspace` returns `errors.New("not implemented")`.
            "workspace" if self.repo_visible() => Err(ExecError::new("not implemented")),
            _ => Ok(None),
        }
    }
}

struct ExistingChangesetReferenceObject {
    repo: RepositoryData,
    external_id: String,
    db: BatchesHandle,
}

impl ObjectValue for ExistingChangesetReferenceObject {
    fn type_name(&self) -> &'static str {
        "ExistingChangesetReference"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "baseRepository" => Ok(Some(repository_object(&self.db, self.repo.clone()))),
            "externalID" => Ok(Some(json_str(self.external_id.clone()))),
            _ => Ok(None),
        }
    }
}

fn abbreviate_ref(value: &str) -> &str {
    value.strip_prefix("refs/heads/").unwrap_or(value)
}

fn message_subject(message: &str) -> String {
    match message.split_once('\n') {
        Some((subject, _)) => subject.trim().to_owned(),
        None => message.trim().to_owned(),
    }
}

fn message_body(message: &str) -> Option<String> {
    match message.split_once('\n') {
        Some((_, body)) => {
            let body = body.trim();
            (!body.is_empty()).then(|| body.to_owned())
        }
        None => None,
    }
}

struct GitBranchChangesetDescriptionObject {
    repo: RepositoryData,
    row: ChangesetSpecRow,
    db: BatchesHandle,
}

impl ObjectValue for GitBranchChangesetDescriptionObject {
    fn type_name(&self) -> &'static str {
        "GitBranchChangesetDescription"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "baseRepository" => Ok(Some(repository_object(&self.db, self.repo.clone()))),
            "baseRef" => Ok(Some(json_str(
                abbreviate_ref(self.row.base_ref.as_deref().unwrap_or_default()).to_owned(),
            ))),
            "baseRev" => Ok(Some(json_str(
                self.row.base_rev.clone().unwrap_or_default(),
            ))),
            "headRef" => Ok(Some(json_str(
                abbreviate_ref(self.row.head_ref.as_deref().unwrap_or_default()).to_owned(),
            ))),
            "title" => Ok(Some(json_str(self.row.title.clone().unwrap_or_default()))),
            "body" => Ok(Some(json_str(self.row.body.clone().unwrap_or_default()))),
            "commits" => Ok(Some(FieldValue::List {
                items: vec![FieldValue::Object(Arc::new(GitCommitDescriptionObject {
                    message: self.row.commit_message.clone().unwrap_or_default(),
                    diff: String::from_utf8_lossy(&self.row.diff).into_owned(),
                    author_name: self.row.commit_author_name.clone().unwrap_or_default(),
                    author_email: self.row.commit_author_email.clone().unwrap_or_default(),
                }))],
                element_type: Some("GitCommitDescription"),
            })),
            "diffStat" => Ok(Some(FieldValue::Object(Arc::new(
                crate::batches_graphql::DiffStatObject {
                    added: self.row.diff_stat_added,
                    deleted: self.row.diff_stat_deleted,
                },
            )))),
            "published" => Ok(Some(match &self.row.published {
                Some(value) => FieldValue::Json(Json::from_serde(value).map_err(ExecError::new)?),
                None => FieldValue::Null,
            })),
            // `changesetDescriptionResolver.Diff`: an M9.32d production
            // gitserver-backed preview comparison over `baseRev` + the spec
            // patch. Like the `Changeset.diff` preview path, the patch is
            // parsed by the shared diff parser.
            "diff" => {
                let Some(read_db) = self.db.changeset_read() else {
                    return Ok(Some(FieldValue::Null));
                };
                let Some(source) = read_db.changeset_diff_source() else {
                    return Ok(Some(FieldValue::Null));
                };
                let repo = MinimalRepo {
                    id: self.repo.id,
                    name: self.repo.name.clone(),
                };
                let preview = source
                    .preview_with_git(
                        &repo,
                        self.row.base_rev.as_deref().unwrap_or_default(),
                        &self.row.diff,
                    )
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(preview))))
            }
            _ => Ok(None),
        }
    }
}

struct GitCommitDescriptionObject {
    message: String,
    diff: String,
    author_name: String,
    author_email: String,
}

impl ObjectValue for GitCommitDescriptionObject {
    fn type_name(&self) -> &'static str {
        "GitCommitDescription"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "message" => Ok(Some(json_str(self.message.clone()))),
            "subject" => Ok(Some(json_str(message_subject(&self.message)))),
            "body" => Ok(Some(optional_str(message_body(&self.message).as_deref()))),
            "author" => Ok(Some(FieldValue::Object(Arc::new(
                crate::batches_graphql::PersonObject {
                    name: self.author_name.clone(),
                    email: self.author_email.clone(),
                },
            )))),
            "diff" => Ok(Some(json_str(self.diff.clone()))),
            _ => Ok(None),
        }
    }
}

struct ForkTargetObject {
    is_fork: bool,
    namespace: Option<String>,
}

impl ObjectValue for ForkTargetObject {
    fn type_name(&self) -> &'static str {
        "ForkTarget"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "pushUser" => Ok(Some(FieldValue::Json(Json::Bool(self.is_fork)))),
            "namespace" => Ok(Some(optional_str(self.namespace.as_deref()))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

pub struct BatchSpecConnectionObject {
    db: BatchesHandle,
    filter: BatchSpecFilter,
    page: OnceLock<Result<BatchSpecPage, String>>,
    total: OnceLock<Result<i64, String>>,
}

impl BatchSpecConnectionObject {
    pub fn new(db: BatchesHandle, filter: BatchSpecFilter) -> Self {
        Self {
            db,
            filter,
            page: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn page(&self) -> Result<&BatchSpecPage, String> {
        self.page
            .get_or_init(|| {
                specs(&self.db)
                    .map_err(|error| error.message)
                    .and_then(|store| store.list_batch_specs(&self.filter))
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn total(&self) -> Result<i64, String> {
        self.total
            .get_or_init(|| {
                specs(&self.db)
                    .map_err(|error| error.message)
                    .and_then(|store| store.count_batch_specs(&self.filter))
            })
            .clone()
    }
}

impl ObjectValue for BatchSpecConnectionObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecConnection"
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
                            FieldValue::Object(Arc::new(BatchSpecObject::new(
                                row.clone(),
                                self.db.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("BatchSpec"),
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

pub struct ChangesetSpecConnectionObject {
    db: BatchesHandle,
    filter: ChangesetSpecFilter,
    actor: Actor,
    page: OnceLock<
        Result<(Vec<ChangesetSpecRow>, HashMap<i32, RepositoryData>, i64), String>,
    >,
    total: OnceLock<Result<i64, String>>,
}

impl ChangesetSpecConnectionObject {
    pub fn new(db: BatchesHandle, filter: ChangesetSpecFilter, actor: Actor) -> Self {
        Self {
            db,
            filter,
            actor,
            page: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn page(
        &self,
    ) -> Result<&(Vec<ChangesetSpecRow>, HashMap<i32, RepositoryData>, i64), String> {
        self.page
            .get_or_init(|| {
                let page = specs(&self.db)
                    .map_err(|error| error.message)?
                    .list_changeset_specs(&self.filter)?;
                let repo_ids: HashSet<i32> = page.rows.iter().map(|row| row.repo_id).collect();
                let ids: Vec<i32> = repo_ids.into_iter().collect();
                let repos = self.db.repos_by_ids(&ids, self.actor)?;
                Ok((page.rows, repos, page.next))
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn total(&self) -> Result<i64, String> {
        self.total
            .get_or_init(|| {
                specs(&self.db)
                    .map_err(|error| error.message)?
                    .count_changeset_specs(&self.filter)
            })
            .clone()
    }
}

impl ObjectValue for ChangesetSpecConnectionObject {
    fn type_name(&self) -> &'static str {
        "ChangesetSpecConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let (rows, repos, _) = self.page().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: rows
                        .iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(ChangesetSpecObject::new(
                                row.clone(),
                                self.db.clone(),
                                repos.get(&row.repo_id).cloned(),
                            )))
                        })
                        .collect(),
                    element_type: Some("ChangesetSpec"),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total().map_err(ExecError::new)?,
            )))),
            "pageInfo" => {
                let (_, _, next) = self.page().map_err(ExecError::new)?;
                let next = *next;
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
// BatchSpecWorkspaceResolution / connection / workspace
// ---------------------------------------------------------------------------

/// `batchSpecWorkspaceResolutionResolver`.
pub struct BatchSpecWorkspaceResolutionObject {
    pub row: BatchSpecResolutionJobRow,
    pub batch_spec: BatchSpecRow,
    pub db: BatchesHandle,
}

impl ObjectValue for BatchSpecWorkspaceResolutionObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspaceResolution"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "state" => Ok(Some(json_str(self.row.state()))),
            "startedAt" => Ok(Some(optional_str(
                self.row.started_at.map(datetime_rfc3339).as_deref(),
            ))),
            "finishedAt" => Ok(Some(optional_str(
                self.row.finished_at.map(datetime_rfc3339).as_deref(),
            ))),
            "failureMessage" => Ok(Some(optional_str(self.row.failure_message.as_deref()))),
            "workspaces" => {
                let filter = workspaces_args_to_filter(arguments, self.batch_spec.id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    BatchSpecWorkspaceConnectionObject {
                        filter,
                        db: self.db.clone(),
                    },
                ))))
            }
            // `TODO(ssbc): not implemented` in Go.
            "recentlyCompleted" | "recentlyErrored" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `workspacesListArgsToDBOpts`.
fn workspaces_args_to_filter(
    arguments: &Arguments,
    batch_spec_id: i64,
) -> Result<BatchSpecWorkspaceFilter, String> {
    let first = arguments.get("first").and_then(InputValue::as_i64).unwrap_or(50);
    let mut filter = BatchSpecWorkspaceFilter {
        limit: first,
        batch_spec_id,
        ..BatchSpecWorkspaceFilter::default()
    };
    if let Some(after) = arguments.get("after").and_then(InputValue::as_str) {
        filter.cursor = after
            .parse::<i64>()
            .map_err(|_| format!("invalid after cursor {after:?}"))?;
    }
    if let Some(search) = arguments.get("search").and_then(InputValue::as_str) {
        filter.search = parse_text_search(search);
    }
    if let Some(state) = arguments.get("state").and_then(InputValue::as_str) {
        match state {
            "COMPLETED" => filter.only_cached_or_completed = true,
            "PENDING" => filter.only_without_execution_and_not_cached = true,
            "CANCELING" => {
                filter.cancel = Some(true);
                filter.state = Some("processing".to_owned());
            }
            "SKIPPED" => filter.skipped = Some(true),
            other => filter.state = Some(other.to_lowercase()),
        }
    }
    Ok(filter)
}

/// Minimal `search.ParseTextSearch`: whitespace tokens, quoted phrases and
/// `-` negation, matching against repository names.
fn parse_text_search(input: &str) -> Vec<TextSearchTerm> {
    let mut terms = Vec::new();
    let mut current = String::new();
    let mut negated = false;
    let mut quoted = false;
    let mut started = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                started = true;
            }
            c if c.is_whitespace() && !quoted => {
                if started && !current.is_empty() {
                    terms.push(TextSearchTerm {
                        negated,
                        value: current.clone(),
                    });
                }
                current.clear();
                negated = false;
                started = false;
            }
            '-' if !started && !quoted => {
                negated = true;
                started = true;
            }
            other => {
                current.push(other);
                started = true;
            }
        }
    }
    if started && !current.is_empty() {
        terms.push(TextSearchTerm { negated, value: current });
    }
    terms
}

/// `batchSpecWorkspaceConnectionResolver`.
pub struct BatchSpecWorkspaceConnectionObject {
    pub filter: BatchSpecWorkspaceFilter,
    pub db: BatchesHandle,
}

impl BatchSpecWorkspaceConnectionObject {
    fn page(&self) -> Result<BatchSpecWorkspacePage, ExecError> {
        specs(&self.db)?
            .list_batch_spec_workspaces(&self.filter)
            .map_err(ExecError::new)
    }
}

impl ObjectValue for BatchSpecWorkspaceConnectionObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspaceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => {
                let count = specs(&self.db)?
                    .count_batch_spec_workspaces(&self.filter)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Int(count))))
            }
            "pageInfo" => {
                let page = self.page()?;
                let (has_next, cursor) = if page.next != 0 {
                    (true, Some(page.next.to_string()))
                } else {
                    (false, None)
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            "nodes" => {
                let page = self.page()?;
                let repo_ids: Vec<i32> = page.rows.iter().map(|row| row.repo_id).collect();
                let repos = self
                    .db
                    .repos_by_ids(&repo_ids, actor)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: page
                        .rows
                        .into_iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(BatchSpecWorkspaceObject {
                                repo: repos.get(&row.repo_id).cloned(),
                                row,
                                db: self.db.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("BatchSpecWorkspace"),
                }))
            }
            "stats" => {
                let stats = specs(&self.db)?
                    .batch_spec_stats(self.filter.batch_spec_id)
                    .map_err(ExecError::new)?
                    .unwrap_or_default();
                Ok(Some(FieldValue::Object(Arc::new(
                    BatchSpecWorkspacesStatsObject {
                        errored: stats.failed as i64,
                        completed: (stats.completed + stats.cached_workspaces) as i64,
                        processing: stats.processing as i64,
                        queued: stats.queued as i64,
                        ignored: (stats.skipped_workspaces - stats.cached_workspaces) as i64,
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

struct BatchSpecWorkspacesStatsObject {
    errored: i64,
    completed: i64,
    processing: i64,
    queued: i64,
    ignored: i64,
}

impl ObjectValue for BatchSpecWorkspacesStatsObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspacesStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "errored" => self.errored,
            "completed" => self.completed,
            "processing" => self.processing,
            "queued" => self.queued,
            "ignored" => self.ignored,
            _ => return Ok(None),
        };
        Ok(Some(FieldValue::Json(Json::Int(value))))
    }
}

pub struct BatchSpecWorkspaceObject {
    pub row: BatchSpecWorkspaceRow,
    pub db: BatchesHandle,
    pub repo: Option<RepositoryData>,
}

/// One `batcheslib.Step` as parsed from `batch_specs.spec`.
pub struct StepSpec {
    run: String,
    container: String,
    if_condition: Option<String>,
}

fn batch_spec_steps(spec_json: &str) -> Vec<StepSpec> {
    let Ok(value) = serde_json::from_str::<JsonValue>(spec_json) else {
        return Vec::new();
    };
    value
        .get("steps")
        .and_then(JsonValue::as_array)
        .map(|steps| {
            steps
                .iter()
                .map(|step| StepSpec {
                    run: step
                        .get("run")
                        .and_then(JsonValue::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    container: step
                        .get("container")
                        .and_then(JsonValue::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    if_condition: step
                        .get("if")
                        .and_then(JsonValue::as_str)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn execution_logs(row: &BatchSpecWorkspaceRow) -> Vec<JsonValue> {
    row.execution
        .as_ref()
        .and_then(|execution| execution.execution_logs.as_array().cloned())
        .unwrap_or_default()
}

fn matching_log<'a>(logs: &'a [JsonValue], key: &str) -> Option<&'a JsonValue> {
    logs.iter().find(|entry| {
        entry
            .get("key")
            .and_then(JsonValue::as_str)
            .map(|value| value == key || value == format!("step.src.{key}"))
            .unwrap_or(false)
    })
}

fn log_entry_field<'a>(entry: &'a JsonValue, field: &str) -> Option<&'a JsonValue> {
    entry.get(field)
}

fn log_time(entry: &JsonValue, field: &str) -> Option<OffsetDateTime> {
    log_entry_field(entry, field)
        .and_then(JsonValue::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok())
}

/// `batchSpecWorkspaceStepResolver` for either V1 or V2 executions.
pub struct BatchSpecWorkspaceStepObject {
    pub index: usize,
    pub step: StepSpec,
    pub skipped: bool,
    pub cached_result_found: bool,
    pub log_entry: Option<JsonValue>,
    pub v2: bool,
}

impl ObjectValue for BatchSpecWorkspaceStepObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspaceStep"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "number" => Ok(Some(FieldValue::Json(Json::Int(self.index as i64 + 1)))),
            "run" => Ok(Some(json_str(self.step.run.clone()))),
            "container" => Ok(Some(json_str(self.step.container.clone()))),
            "ifCondition" => Ok(Some(optional_str(self.step.if_condition.as_deref()))),
            "cachedResultFound" => Ok(Some(FieldValue::Json(Json::Bool(
                self.cached_result_found,
            )))),
            "skipped" => Ok(Some(FieldValue::Json(Json::Bool(
                self.cached_result_found || self.skipped,
            )))),
            "outputLines" => {
                let lines: Vec<String> = self
                    .log_entry
                    .as_ref()
                    .and_then(|entry| log_entry_field(entry, "out"))
                    .and_then(JsonValue::as_str)
                    .map(|out| out.split('\n').map(str::to_owned).collect())
                    .unwrap_or_default();
                let first = arguments
                    .get("first")
                    .and_then(InputValue::as_i64)
                    .unwrap_or(500);
                let after = arguments
                    .get("after")
                    .and_then(InputValue::as_str)
                    .and_then(|value| value.parse::<i64>().ok());
                Ok(Some(FieldValue::Object(Arc::new(
                    BatchSpecWorkspaceOutputLinesObject {
                        lines,
                        first,
                        after,
                    },
                ))))
            }
            "startedAt" => Ok(Some(optional_str(
                self.log_entry
                    .as_ref()
                    .and_then(|entry| log_time(entry, "startTime"))
                    .map(datetime_rfc3339)
                    .as_deref(),
            ))),
            "finishedAt" => {
                let finished = self.log_entry.as_ref().and_then(|entry| {
                    let start = log_time(entry, "startTime")?;
                    let duration = log_entry_field(entry, "durationMs")?.as_i64()?;
                    Some(start + Duration::milliseconds(duration))
                });
                Ok(Some(optional_str(
                    finished.map(datetime_rfc3339).as_deref(),
                )))
            }
            "exitCode" => {
                let exit = self
                    .log_entry
                    .as_ref()
                    .and_then(|entry| log_entry_field(entry, "exitCode"))
                    .and_then(JsonValue::as_i64);
                Ok(Some(match exit {
                    Some(code) => FieldValue::Json(Json::Int(code)),
                    None => FieldValue::Null,
                }))
            }
            "environment" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("BatchSpecWorkspaceEnvironmentVariable"),
            })),
            "outputVariables" => Ok(Some(FieldValue::Null)),
            "diffStat" | "diff" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `batchSpecWorkspaceOutputLinesResolver`.
pub struct BatchSpecWorkspaceOutputLinesObject {
    pub lines: Vec<String>,
    pub first: i64,
    pub after: Option<i64>,
}

impl BatchSpecWorkspaceOutputLinesObject {
    fn compute(&self) -> (Vec<String>, i64, bool) {
        let total = self.lines.len() as i64;
        let after = self.after.unwrap_or(0);
        let offset = after + self.first;
        let mut subset: Vec<String> = if after < total {
            self.lines[after as usize..].to_vec()
        } else {
            Vec::new()
        };
        if self.first < self.lines.len() as i64 && total > offset {
            subset.truncate(self.first as usize);
        }
        let has_next = total > offset;
        (subset, total, has_next)
    }
}

impl ObjectValue for BatchSpecWorkspaceOutputLinesObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspaceStepOutputLineConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.compute().1)))),
            "nodes" => Ok(Some(FieldValue::List {
                items: self.compute().0.into_iter().map(json_str).collect(),
                element_type: Some("String"),
            })),
            "pageInfo" => {
                let (_, _, has_next) = self.compute();
                let cursor = if has_next {
                    Some((self.after.unwrap_or(0) + self.first).to_string())
                } else {
                    None
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            _ => Ok(None),
        }
    }
}

impl BatchSpecWorkspaceObject {
    fn changeset_specs(&self, actor: Actor) -> Result<Vec<ChangesetSpecRow>, ExecError> {
        if self.row.changeset_spec_ids.is_empty() {
            return Ok(Vec::new());
        }
        let page = specs(&self.db)?
            .list_changeset_specs(&ChangesetSpecFilter {
                ids: self.row.changeset_spec_ids.clone(),
                ..ChangesetSpecFilter::default()
            })
            .map_err(ExecError::new)?;
        let _ = actor;
        Ok(page.rows)
    }
}

impl ObjectValue for BatchSpecWorkspaceObject {
    fn type_name(&self) -> &'static str {
        if self.repo.is_some() {
            "VisibleBatchSpecWorkspace"
        } else {
            "HiddenBatchSpecWorkspace"
        }
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let execution = self.row.execution.as_ref();
        match field {
            "id" => Ok(Some(json_str(marshal_batch_spec_workspace_id(self.row.id)))),
            "batchSpec" => Ok(Some(match specs(&self.db)?.get_batch_spec(
                self.row.batch_spec_id,
                "",
                0,
            ) {
                Ok(Some(row)) => FieldValue::Object(Arc::new(BatchSpecObject::new(
                    row,
                    self.db.clone(),
                ))),
                _ => FieldValue::Null,
            })),
            "onlyFetchWorkspace" => Ok(Some(FieldValue::Json(Json::Bool(
                self.row.only_fetch_workspace,
            )))),
            "ignored" => Ok(Some(FieldValue::Json(Json::Bool(self.row.ignored)))),
            "unsupported" => Ok(Some(FieldValue::Json(Json::Bool(self.row.unsupported)))),
            "cachedResultFound" => Ok(Some(FieldValue::Json(Json::Bool(
                self.row.cached_result_found,
            )))),
            "stepCacheResultCount" => {
                let steps = specs(&self.db)?
                    .get_batch_spec(self.row.batch_spec_id, "", 0)
                    .map_err(ExecError::new)?
                    .map(|row| batch_spec_steps(&row.spec))
                    .unwrap_or_default();
                let count = self
                    .row
                    .step_cache_results
                    .as_object()
                    .map(|map| {
                        map.keys()
                            .filter_map(|key| key.parse::<usize>().ok())
                            .filter(|index| *index >= 1 && *index <= steps.len())
                            .count()
                    })
                    .unwrap_or(0) as i64;
                Ok(Some(FieldValue::Json(Json::Int(count))))
            }
            "state" => Ok(Some(json_str(self.row.state()))),
            "queuedAt" => Ok(Some(optional_str(
                if self.row.skipped {
                    None
                } else {
                    execution
                        .map(|execution| datetime_rfc3339(execution.created_at))
                }
                .as_deref(),
            ))),
            "startedAt" => Ok(Some(optional_str(
                if self.row.skipped {
                    None
                } else {
                    execution.and_then(|execution| execution.started_at.map(datetime_rfc3339))
                }
                .as_deref(),
            ))),
            "finishedAt" => Ok(Some(optional_str(
                if self.row.skipped {
                    None
                } else {
                    execution.and_then(|execution| execution.finished_at.map(datetime_rfc3339))
                }
                .as_deref(),
            ))),
            "failureMessage" => {
                let message = if self.row.skipped {
                    None
                } else {
                    execution.and_then(|execution| {
                        if execution.state == "canceled" {
                            None
                        } else {
                            execution.failure_message.clone()
                        }
                    })
                };
                Ok(Some(optional_str(message.as_deref())))
            }
            "placeInQueue" => Ok(Some(match execution {
                Some(execution) if execution.state == "queued" => {
                    FieldValue::Json(Json::Int(execution.place_in_user_queue.unwrap_or(0)))
                }
                _ => FieldValue::Null,
            })),
            "placeInGlobalQueue" => Ok(Some(match execution {
                Some(execution) if execution.state == "queued" => {
                    FieldValue::Json(Json::Int(execution.place_in_global_queue.unwrap_or(0)))
                }
                _ => FieldValue::Null,
            })),
            "steps" => {
                let spec = specs(&self.db)?
                    .get_batch_spec(self.row.batch_spec_id, "", 0)
                    .map_err(ExecError::new)?;
                let steps = spec
                    .as_ref()
                    .map(|row| batch_spec_steps(&row.spec))
                    .unwrap_or_default();
                let logs = execution_logs(&self.row);
                let v2 = execution.map(|execution| execution.version == 2).unwrap_or(false);
                Ok(Some(FieldValue::List {
                    items: self
                        .step_objects(&steps, &logs, v2)
                        .into_iter()
                        .map(|step| FieldValue::Object(Arc::new(step)))
                        .collect(),
                    element_type: Some("BatchSpecWorkspaceStep"),
                }))
            }
            "step" => {
                let index = arguments
                    .get("index")
                    .and_then(InputValue::as_i64)
                    .unwrap_or(0);
                if index <= 0 {
                    return Err(ExecError::new("invalid step index"));
                }
                let spec = specs(&self.db)?
                    .get_batch_spec(self.row.batch_spec_id, "", 0)
                    .map_err(ExecError::new)?;
                let steps = spec
                    .as_ref()
                    .map(|row| batch_spec_steps(&row.spec))
                    .unwrap_or_default();
                if index as usize > steps.len() {
                    return Ok(Some(FieldValue::Null));
                }
                let logs = execution_logs(&self.row);
                let v2 = execution.map(|execution| execution.version == 2).unwrap_or(false);
                let objects = self.step_objects(&steps, &logs, v2);
                Ok(Some(FieldValue::Object(Arc::new(
                    objects
                        .into_iter()
                        .nth(index as usize - 1)
                        .expect("index checked"),
                ))))
            }
            "searchResultPaths" => Ok(Some(FieldValue::List {
                items: self
                    .row
                    .file_matches
                    .iter()
                    .map(|path| json_str(path.clone()))
                    .collect(),
                element_type: Some("String"),
            })),
            "changesetSpecs" => {
                if self.repo.is_none() {
                    return Ok(Some(FieldValue::Null));
                }
                if self.row.skipped && !self.row.cached_result_found {
                    return Ok(Some(FieldValue::Null));
                }
                let rows = self.changeset_specs(actor)?;
                Ok(Some(FieldValue::List {
                    items: rows
                        .into_iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(ChangesetSpecObject::new(
                                row.clone(),
                                self.db.clone(),
                                self.repo.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("VisibleChangesetSpec"),
                }))
            }
            "diffStat" => {
                if self.repo.is_none() {
                    return Ok(Some(FieldValue::Null));
                }
                let rows = self.changeset_specs(actor)?;
                let mut added = 0i64;
                let mut deleted = 0i64;
                let mut any = false;
                for row in rows {
                    if row.external_id.is_some() {
                        continue;
                    }
                    added += row.diff_stat_added as i64;
                    deleted += row.diff_stat_deleted as i64;
                    any = true;
                }
                if !any {
                    return Ok(Some(FieldValue::Null));
                }
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::batches_graphql::DiffStatObject {
                        added: added as i32,
                        deleted: deleted as i32,
                    },
                ))))
            }
            "stages" => {
                let Some(execution) = execution else {
                    return Ok(Some(FieldValue::Null));
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    BatchSpecWorkspaceStagesObject {
                        logs: execution.execution_logs.as_array().cloned().unwrap_or_default(),
                        v2: execution.version == 2,
                    },
                ))))
            }
            // Only site admins with a live executor row; not ported.
            "executor" => Ok(Some(FieldValue::Null)),
            "repository" if self.repo.is_some() => Ok(Some(repository_object(
                &self.db,
                self.repo.clone().expect("checked repo"),
            ))),
            "branch" if self.repo.is_some() => Ok(Some(FieldValue::Object(Arc::new(GitRefObject {
                name: self.row.branch.clone(),
                target: format!("{}", self.row.commit),
                repo: self.repo.clone().expect("checked repo"),
                db: self.db.clone(),
            })))),
            "path" => Ok(Some(json_str(self.row.path.clone()))),
            _ => Ok(None),
        }
    }
}

impl BatchSpecWorkspaceObject {
    fn step_objects(
        &self,
        steps: &[StepSpec],
        logs: &[JsonValue],
        v2: bool,
    ) -> Vec<BatchSpecWorkspaceStepObject> {
        steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                let number = index + 1;
                let cached = self
                    .row
                    .step_cache_results
                    .get(number.to_string())
                    .is_some();
                let log_key = if v2 {
                    format!("step.docker.step.{index}.run")
                } else {
                    "step.src.batch-exec".to_owned()
                };
                let log_entry = matching_log(logs, &log_key).cloned().or_else(|| {
                    if v2 {
                        matching_log(logs, &format!("step.kubernetes.step.{index}.run")).cloned()
                    } else {
                        matching_log(logs, "step.src.0").cloned()
                    }
                });
                BatchSpecWorkspaceStepObject {
                    index,
                    step: StepSpec {
                        run: step.run.clone(),
                        container: step.container.clone(),
                        if_condition: step.if_condition.clone(),
                    },
                    skipped: self.row.skipped,
                    cached_result_found: self.row.cached_result_found || cached,
                    log_entry,
                    v2,
                }
            })
            .collect()
    }
}

/// `batchSpecWorkspaceStagesResolver`.
struct BatchSpecWorkspaceStagesObject {
    logs: Vec<JsonValue>,
    v2: bool,
}

impl BatchSpecWorkspaceStagesObject {
    fn entries(&self, predicate: impl Fn(&str) -> bool) -> Vec<FieldValue> {
        self.logs
            .iter()
            .filter(|entry| {
                entry
                    .get("key")
                    .and_then(JsonValue::as_str)
                    .map(&predicate)
                    .unwrap_or(false)
            })
            .cloned()
            .map(|entry| FieldValue::Object(Arc::new(ExecutionLogEntryObject { entry })))
            .collect()
    }
}

impl ObjectValue for BatchSpecWorkspaceStagesObject {
    fn type_name(&self) -> &'static str {
        "BatchSpecWorkspaceStages"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let items = match field {
            "setup" => self.entries(|key| {
                key.starts_with("setup.")
                    || key == "step.docker.step.apply-diff"
                    || key == "step.kubernetes.step.apply-diff"
            }),
            "srcExec" => {
                let v2 = self.v2;
                self.entries(move |key| {
                    if v2 {
                        key.starts_with("step.docker.step.")
                            || key.starts_with("step.kubernetes.step.")
                    } else {
                        key == "step.src.batch-exec" || key == "step.src.0"
                    }
                })
            }
            "teardown" => self.entries(|key| key.starts_with("teardown.")),
            _ => return Ok(None),
        };
        Ok(Some(FieldValue::List {
            items,
            element_type: Some("ExecutionLogEntry"),
        }))
    }
}

struct ExecutionLogEntryObject {
    entry: JsonValue,
}

impl ObjectValue for ExecutionLogEntryObject {
    fn type_name(&self) -> &'static str {
        "ExecutionLogEntry"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "key" => Ok(Some(optional_str(
                self.entry.get("key").and_then(JsonValue::as_str),
            ))),
            "command" => Ok(Some(FieldValue::List {
                items: self
                    .entry
                    .get("command")
                    .and_then(JsonValue::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(JsonValue::as_str)
                            .map(|item| json_str(item.to_owned()))
                            .collect()
                    })
                    .unwrap_or_default(),
                element_type: Some("String"),
            })),
            "startTime" => Ok(Some(optional_str(
                log_time(&self.entry, "startTime")
                    .map(datetime_rfc3339)
                    .as_deref(),
            ))),
            "exitCode" => Ok(Some(
                match self.entry.get("exitCode").and_then(JsonValue::as_i64) {
                    Some(code) => FieldValue::Json(Json::Int(code)),
                    None => FieldValue::Null,
                },
            )),
            "out" => Ok(Some(optional_str(
                self.entry.get("out").and_then(JsonValue::as_str),
            ))),
            "durationMs" => Ok(Some(
                match self.entry.get("durationMs").and_then(JsonValue::as_i64) {
                    Some(value) => FieldValue::Json(Json::Int(value)),
                    None => FieldValue::Null,
                },
            )),
            "url" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

struct GitRefObject {
    name: String,
    target: String,
    repo: RepositoryData,
    db: BatchesHandle,
}

impl ObjectValue for GitRefObject {
    fn type_name(&self) -> &'static str {
        "GitRef"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(json_str(self.name.clone()))),
            "displayName" => Ok(Some(json_str(abbreviate_ref(&self.name).to_owned()))),
            "abbrevName" => Ok(Some(json_str(abbreviate_ref(&self.name).to_owned()))),
            "target" => Ok(Some(FieldValue::Object(Arc::new(GitObjectObject {
                oid: self.target.clone(),
            })))),
            "url" => Ok(Some(json_str(format!(
                "/{}/-/blob/{}",
                self.repo.name, self.name
            )))),
            "repository" => Ok(Some(repository_object(&self.db, self.repo.clone()))),
            _ => Ok(None),
        }
    }
}

struct GitObjectObject {
    oid: String,
}

impl ObjectValue for GitObjectObject {
    fn type_name(&self) -> &'static str {
        "GitObject"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "oid" => Ok(Some(json_str(self.oid.clone()))),
            "abbreviatedOID" => Ok(Some(json_str(
                self.oid.chars().take(7).collect::<String>(),
            ))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root fields / node kinds
// ---------------------------------------------------------------------------

/// `Query.batchSpecs`.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "batchSpecs")
}

pub fn root_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "batchSpecs" => Ok(Some(batch_specs(db, arguments, actor)?)),
        _ => Ok(None),
    }
}

fn batch_specs(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    let first = arguments.i64("first").unwrap_or(50);
    validate_first_param(first).map_err(ExecError::new)?;
    let cursor = parse_cursor(arguments).map_err(ExecError::new)?;
    let filter = BatchSpecFilter {
        limit: first,
        cursor,
        newest_first: true,
        include_locally_executed_specs: arguments
            .bool("includeLocallyExecutedSpecs")
            .unwrap_or(false),
        exclude_empty_specs: arguments.bool("excludeEmptySpecs").unwrap_or(false),
        exclude_created_from_raw_not_owned_by_user: if is_site_admin(db, actor) {
            0
        } else {
            actor.uid().unwrap_or(0)
        },
        ..BatchSpecFilter::default()
    };
    Ok(FieldValue::Object(Arc::new(BatchSpecConnectionObject::new(
        db.clone(),
        filter,
    ))))
}

/// `BatchChange.currentSpec` / `BatchChange.batchSpecs` (M9.26a).
pub fn batch_change_field(
    db: &BatchesHandle,
    batch_change: &crate::batches_graphql::BatchChangeRow,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "currentSpec" => {
            let row = specs(db)?
                .get_batch_spec(batch_change.batch_spec_id, "", 0)
                .map_err(ExecError::new)?
                .ok_or_else(|| ExecError::new("no results"))?;
            Ok(Some(FieldValue::Object(Arc::new(BatchSpecObject::new(
                row,
                db.clone(),
            )))))
        }
        "batchSpecs" => {
            let first = arguments.i64("first").unwrap_or(50);
            validate_first_param(first).map_err(ExecError::new)?;
            let cursor = parse_cursor(arguments).map_err(ExecError::new)?;
            let filter = BatchSpecFilter {
                limit: first,
                cursor,
                batch_change_id: batch_change.id,
                newest_first: true,
                include_locally_executed_specs: arguments
                    .bool("includeLocallyExecutedSpecs")
                    .unwrap_or(false),
                exclude_empty_specs: arguments.bool("excludeEmptySpecs").unwrap_or(false),
                exclude_created_from_raw_not_owned_by_user: if is_site_admin(db, actor) {
                    0
                } else {
                    actor.uid().unwrap_or(0)
                },
                ..BatchSpecFilter::default()
            };
            Ok(Some(FieldValue::Object(Arc::new(BatchSpecConnectionObject::new(
                db.clone(),
                filter,
            )))))
        }
        _ => Ok(None),
    }
}

pub fn is_node_kind(kind: &str) -> bool {
    matches!(
        kind,
        BATCH_SPEC_KIND | CHANGESET_SPEC_KIND | BATCH_SPEC_WORKSPACE_KIND
    )
}

pub fn node_field(
    db: &BatchesHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }
    match kind {
        BATCH_SPEC_KIND => {
            let rand_id = unmarshal_relay_string(id).map_err(ExecError::new)?;
            if rand_id.is_empty() {
                return Err(ExecError::new(ID_IS_ZERO));
            }
            Ok(Some(
                match specs(db)?
                    .get_batch_spec(0, &rand_id, 0)
                    .map_err(ExecError::new)?
                {
                    Some(row) => FieldValue::Object(Arc::new(BatchSpecObject::new(
                        row,
                        db.clone(),
                    ))),
                    None => FieldValue::Null,
                },
            ))
        }
        CHANGESET_SPEC_KIND => {
            let rand_id = unmarshal_relay_string(id).map_err(ExecError::new)?;
            if rand_id.is_empty() {
                return Err(ExecError::new(ID_IS_ZERO));
            }
            let row = specs(db)?
                .get_changeset_spec(0, &rand_id)
                .map_err(ExecError::new)?;
            let Some(row) = row else {
                return Ok(Some(FieldValue::Null));
            };
            let repos = db
                .repos_by_ids(&[row.repo_id], actor)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(ChangesetSpecObject::new(
                row.clone(),
                db.clone(),
                repos.get(&row.repo_id).cloned(),
            )))))
        }
        BATCH_SPEC_WORKSPACE_KIND => {
            let workspace_id = unmarshal_relay_int(id).map_err(ExecError::new)?;
            if workspace_id == 0 {
                return Err(ExecError::new(ID_IS_ZERO));
            }
            let Some(row) = specs(db)?
                .get_batch_spec_workspace(workspace_id)
                .map_err(ExecError::new)?
            else {
                return Ok(Some(FieldValue::Null));
            };
            let repos = db
                .repos_by_ids(&[row.repo_id], actor)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                BatchSpecWorkspaceObject {
                    repo: repos.get(&row.repo_id).cloned(),
                    row,
                    db: db.clone(),
                },
            ))))
        }
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createBatchSpec" | "createChangesetSpec" | "createChangesetSpecs"
    )
}

pub fn mutation_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createBatchSpec" => create_batch_spec(db, arguments, actor).map(Some),
        "createChangesetSpec" => create_changeset_spec(db, arguments, actor).map(Some),
        "createChangesetSpecs" => create_changeset_specs(db, arguments, actor).map(Some),
        _ => Ok(None),
    }
}

fn create_batch_spec(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_create_access(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;

    let raw_namespace = arguments
        .string("namespace")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let namespace = unmarshal_namespace(raw_namespace).map_err(ExecError::new)?;
    let raw_spec = arguments
        .string("batchSpec")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let changeset_spec_rand_ids = match arguments.get("changesetSpecs") {
        None | Some(InputValue::Null) => Vec::new(),
        Some(InputValue::List(items)) => {
            let mut ids = Vec::with_capacity(items.len());
            for item in items {
                let raw = item
                    .as_str()
                    .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
                ids.push(unmarshal_relay_string(raw).map_err(ExecError::new)?);
            }
            ids
        }
        Some(_) => return Err(ExecError::new("invalid graphql.ID")),
    };

    // `service.CreateBatchSpec`: parse the raw spec before the namespace check.
    let (spec_json, name, description) = parse_batch_spec(raw_spec).map_err(ExecError::new)?;
    let _ = (&name, &description);

    db.check_namespace_access(actor, namespace)
        .map_err(ExecError::new)?;

    // Validate the referenced changeset specs and their repositories
    // (`service.CreateBatchSpec`), then create the batch spec in one tx.
    if !changeset_spec_rand_ids.is_empty() {
        let page = specs(db)?
            .list_changeset_specs(&ChangesetSpecFilter {
                rand_ids: changeset_spec_rand_ids.clone(),
                ..ChangesetSpecFilter::default()
            })
            .map_err(ExecError::new)?;
        let repo_ids: Vec<i32> = page.rows.iter().map(|row| row.repo_id).collect();
        let repos = db
            .repos_by_ids(&repo_ids, actor)
            .map_err(ExecError::new)?;
        for row in &page.rows {
            if !repos.contains_key(&row.repo_id) {
                return Err(ExecError::new(format!(
                    "repo not found: id={}",
                    row.repo_id
                )));
            }
        }
        let found: HashSet<&str> = page.rows.iter().map(|row| row.rand_id.as_str()).collect();
        for rand_id in &changeset_spec_rand_ids {
            if !found.contains(rand_id.as_str()) {
                return Err(ExecError::new(format!(
                    "changesetSpec not found: id={rand_id}"
                )));
            }
        }
    }

    let store = specs(db)?;
    let rand_id = uuid::Uuid::new_v4().to_string();
    let request = NewBatchSpec {
        rand_id,
        raw_spec: raw_spec.to_owned(),
        spec: spec_json,
        namespace_user_id: namespace.user,
        namespace_org_id: namespace.org,
        user_id: actor.uid().unwrap_or(0),
        changeset_spec_rand_ids,
    };
    let row = store.create_batch_spec(&request).map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(BatchSpecObject::new(
        row,
        db.clone(),
    ))))
}

fn parse_single_changeset_spec(
    db: &BatchesHandle,
    raw: &str,
    actor: Actor,
) -> Result<NewChangesetSpec, ExecError> {
    let input = parse_changeset_spec_input(raw).map_err(ExecError::new)?;
    // `service.CreateChangesetSpec`: repository authz check.
    let repos = db
        .repos_by_ids(&[input.base_repo_id], actor)
        .map_err(ExecError::new)?;
    if !repos.contains_key(&input.base_repo_id) {
        return Err(ExecError::new(format!(
            "repo not found: id={}",
            input.base_repo_id
        )));
    }
    new_changeset_spec(
        uuid::Uuid::new_v4().to_string(),
        actor.uid().unwrap_or(0),
        &input,
    )
    .map_err(ExecError::new)
}

fn create_changeset_spec(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_create_access(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let raw = arguments
        .string("changesetSpec")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let spec = parse_single_changeset_spec(db, raw, actor)?;
    let row = specs(db)?
        .create_changeset_specs(&[spec])
        .map_err(ExecError::new)?
        .into_iter()
        .next()
        .ok_or_else(|| ExecError::new("no results"))?;
    let repos = db
        .repos_by_ids(&[row.repo_id], actor)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(ChangesetSpecObject::new(
        row.clone(),
        db.clone(),
        repos.get(&row.repo_id).cloned(),
    ))))
}

fn create_changeset_specs(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_create_access(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let Some(InputValue::List(items)) = arguments.get("changesetSpecs") else {
        return Err(ExecError::new("invalid graphql.ID"));
    };
    let mut specs_to_create = Vec::with_capacity(items.len());
    for item in items {
        let raw = item
            .as_str()
            .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
        specs_to_create.push(parse_single_changeset_spec(db, raw, actor)?);
    }
    let rows = specs(db)?
        .create_changeset_specs(&specs_to_create)
        .map_err(ExecError::new)?;
    let repo_ids: Vec<i32> = rows.iter().map(|row| row.repo_id).collect();
    let repos = db
        .repos_by_ids(&repo_ids, actor)
        .map_err(ExecError::new)?;
    Ok(FieldValue::List {
        items: rows
            .into_iter()
            .map(|row| {
                FieldValue::Object(Arc::new(ChangesetSpecObject::new(
                    row.clone(),
                    db.clone(),
                    repos.get(&row.repo_id).cloned(),
                )))
            })
            .collect(),
        element_type: Some("ChangesetSpec"),
    })
}
