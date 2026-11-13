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

/// Minimal `batch_spec_workspaces` row.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// `GetBatchSpecWorkspaceExecutionJob` (latest job for the workspace).
    pub execution_state: Option<String>,
    pub execution_cancel: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl BatchSpecWorkspaceRow {
    /// `batchSpecWorkspaceResolver.State`: cached/skipped win, an absent
    /// execution job is PENDING, otherwise the job state (uppercased).
    pub fn state(&self) -> String {
        if self.cached_result_found {
            return "COMPLETED".to_owned();
        }
        if self.skipped {
            return "SKIPPED".to_owned();
        }
        let Some(state) = self.execution_state.as_deref() else {
            return "PENDING".to_owned();
        };
        if self.execution_cancel && state == "processing" {
            return "CANCELING".to_owned();
        }
        state.to_uppercase()
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

    /// `store.ListChangesetSpecsWithConflictingHeadRef` grouped counts:
    /// `(repo_id, head_ref, count)`.
    fn conflicting_head_refs(&self, batch_spec_id: i64) -> Result<Vec<(i32, String, i64)>, String>;
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
            // `applyPreview`, `supersedingBatchSpec`, `viewerBatchChangesCodeHosts`
            // and `workspaceResolution` are not ported (see the overview).
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
            // `diff` requires a gitserver-backed preview comparison.
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
// BatchSpecWorkspace (minimal)
// ---------------------------------------------------------------------------

pub struct BatchSpecWorkspaceObject {
    pub row: BatchSpecWorkspaceRow,
    pub db: BatchesHandle,
    pub repo: Option<RepositoryData>,
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
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
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
            "stepCacheResultCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "state" => Ok(Some(json_str(self.row.state()))),
            "queuedAt" | "startedAt" | "finishedAt" | "placeInQueue" | "placeInGlobalQueue"
            | "diffStat" | "stages" | "executor" => Ok(Some(FieldValue::Null)),
            "steps" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("BatchSpecWorkspaceStep"),
            })),
            "step" => Ok(Some(FieldValue::Null)),
            "searchResultPaths" => Ok(Some(FieldValue::List {
                items: self
                    .row
                    .file_matches
                    .iter()
                    .map(|path| json_str(path.clone()))
                    .collect(),
                element_type: Some("String"),
            })),
            "failureMessage" => Ok(Some(FieldValue::Null)),
            "changesetSpecs" => Ok(Some(FieldValue::Null)),
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
