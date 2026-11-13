#![forbid(unsafe_code)]

//! Changeset deep-read GraphQL surface (M9.26c).
//!
//! Ports the remaining read resolvers of
//! `cmd/frontend/internal/batches/resolvers` that were left unported by
//! M9.24c:
//!
//! - `Changeset.events` (`changeset_event_connection.go`, `changeset_event.go`);
//! - `Changeset.diff` (`changeset.go`, including the
//!   `getBranchSpecDescription` preview path and its error texts);
//! - `Changeset.commitVerification`;
//! - `Changeset.batchChanges` (which, unlike `Query.batchChanges`, does *not*
//!   set `ExcludeDraftsNotOwnedByUserID`);
//! - `BatchChange.changesetCountsOverTime`, `changesetsStats`, `diffStat` and
//!   `bulkOperations` (`batch_change.go`, `bulk_operation{,_connection}.go`);
//! - `Query.globalChangesetsStats` and `Query.batchChangesCodeHosts`
//!   (`resolver.go`, `code_host{,_connection}.go`).
//!
//! The database is reached through [`ChangesetReadDatabase`] (implemented for
//! [`crate::batches_graphql_store::PgBatchesGraphqlStore`]); gitserver access
//! for `Changeset.diff` is a separate injectable seam,
//! [`ChangesetDiffSource`]. Without a wired source the diff field returns
//! `null` for the branches that require the comparison; the branches resolved
//! from the database alone (importing changesets, merged/closed changesets,
//! missing/`existing` specs) still match the Go error/empty behaviour.
//!
//! No table, column or Redis key is added.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use serde_json::Value as JsonValue;
use sg_extsvc::batches_state::{
    self, Changeset, ChangesetCheckState, ChangesetCounts, ChangesetEvent, ChangesetExternalState,
    ChangesetReviewState, ChangesetSyncState,
};
use sg_extsvc::batches_state_load::{parse_changeset_metadata, parse_event_metadata};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use sg_git::{DiffRangeType, RawDiffOptions};
use sg_service_gitserver::diff::{parse_multi_file_diff, FileDiff};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::batches_graphql::{
    marshal_bulk_operation_id, parse_batch_change_state, parse_batch_change_states, type_to_kind,
    validate_first_param, BatchChangeFilter, BatchChangeObject, BatchChangeRow, BatchesHandle,
    BulkOperationRow, ChangesetFilter, ChangesetObject, ChangesetRow, DiffStatObject,
    BATCH_CHANGES_DISABLED, DEFAULT_FIRST,
};
use crate::git_commit_extra::{
    ComparisonData, FileDiffConnectionObject, RepositoryComparisonObject,
};
use crate::git_comparison_graphql::NewFileSource;
use crate::git_graphql::GitObjectSource;
use crate::graphql_resolvers::{
    datetime_rfc3339, PageInfoObject, RepositoryData, RepositoryObject,
};
use crate::search_stream::convert::MinimalRepo;

/// `enterprise.ErrBatchChangesDisabledForUser.Error()`.
pub const BATCH_CHANGES_DISABLED_FOR_USER: &str =
    "batch changes are disabled for non-site-admin users. Ask a site admin to unset \
     'batchChanges.restrictToAdmins' in the site configuration to enable the feature for all \
     users.";

/// `resolvers.changesetEventIDKind` (`changeset_event.go`).
pub const CHANGESET_EVENT_KIND: &str = "ChangesetEvent";

/// `relay.MarshalID("ChangesetEvent", id)`.
pub fn marshal_changeset_event_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{CHANGESET_EVENT_KIND}:{id}"))
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// One `changeset_events` row (`scanChangesetEvent`), reduced to what the
/// GraphQL surface reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetEventRow {
    pub id: i64,
    pub changeset_id: i64,
    pub kind: String,
    pub created_at: OffsetDateTime,
    pub metadata: JsonValue,
}

/// `changesets.commit_verification` + `changesets.sync_state`
/// (`ScanChangeset`).
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetReadExtra {
    pub commit_verification: JsonValue,
    pub sync_state: ChangesetSyncState,
}

/// The `changeset_specs` columns `getBranchSpecDescription` needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetSpecDiffRow {
    pub spec_type: String,
    pub base_rev: String,
    pub diff: Vec<u8>,
}

/// `btypes.ChangesetsStats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangesetsStats {
    pub total: i32,
    pub retrying: i32,
    pub failed: i32,
    pub scheduled: i32,
    pub processing: i32,
    pub unpublished: i32,
    pub closed: i32,
    pub draft: i32,
    pub merged: i32,
    pub open: i32,
    pub deleted: i32,
    pub archived: i32,
}

/// `btypes.GlobalChangesetsStats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GlobalChangesetsStats {
    pub total: i32,
    pub unpublished: i32,
    pub draft: i32,
    pub closed: i32,
    pub merged: i32,
    pub open: i32,
}

/// One `ListBulkOperations` row; `db_id` is `MIN(changeset_jobs.id)` and is
/// the pagination cursor, while `operation.id` is the GraphQL ID payload.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkOperationListRow {
    pub db_id: i64,
    pub operation: BulkOperationRow,
}

/// `btypes.BulkOperationError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkOperationErrorRow {
    pub changeset_id: i64,
    pub error: String,
}

/// `btypes.CodeHost` (`scanCodeHost`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeHostRow {
    pub external_service_type: String,
    pub external_service_id: String,
    pub requires_ssh: bool,
    pub has_webhooks: bool,
}

/// Synchronous gitserver seam for `Changeset.diff`.
///
/// `comparison` backs `NewRepositoryComparison`; `preview` backs
/// `NewPreviewRepositoryComparisonResolver`.
pub trait ChangesetDiffSource: Send + Sync {
    fn comparison(
        &self,
        repo: &MinimalRepo,
        base_revspec: &str,
        head_revspec: &str,
    ) -> Result<ComparisonData, String>;

    fn preview(
        &self,
        repo: &MinimalRepo,
        base_rev: &str,
        patch: &[u8],
    ) -> Result<PreviewComparisonObject, String>;
}

/// Production [`ChangesetDiffSource`] over the shared [`GitObjectSource`]
/// (M9.32d).
///
/// `Changeset.diff` is resolved from the synchronous GraphQL `field` hook, so
/// this adapter drives the async gitserver reads through
/// [`crate::batches_graphql_store::block_on`]:
///
/// - `comparison` mirrors `graphqlbackend.NewRepositoryComparison`
///   (`ResolveRevision` on head, `MergeBase`, `ResolveRevision` on the base /
///   merge base, then `RawDiff`), including the `DevNullSHA` base and the
///   empty-merge-base `..` fallback.
/// - `preview` mirrors `NewPreviewRepositoryComparisonResolver`: it resolves
///   `baseRev` through `ResolveRevision` (a missing revision yields the
///   baseline's `revision not found: <repo>@<rev>`) and parses the changeset
///   spec patch with the shared diff parser.
///
/// `GitserverGitObjects` implements `GitObjectSource`; its `commit_extra()`
/// supplies `merge_base` and `raw_diff`.
pub struct GitserverChangesetDiffSource {
    objects: Arc<dyn GitObjectSource>,
}

impl GitserverChangesetDiffSource {
    pub fn new(objects: Arc<dyn GitObjectSource>) -> Self {
        Self { objects }
    }

    /// `backend.Repos().ResolveRev` / `RepoStore.ResolveRev`: `ResolveRevision`
    /// with `EnsureRevision=true`. The missing-revision error text is the
    /// baseline `gitdomain.RevisionNotFoundError`.
    async fn resolve_revision(&self, repo: &MinimalRepo, rev: &str) -> Result<String, String> {
        match self
            .objects
            .resolve_revision(repo.id, &repo.name, rev)
            .await
        {
            Ok(oid) => Ok(oid),
            Err(error) if error.revision_not_found => {
                Err(format!("revision not found: {}@{}", repo.name, rev))
            }
            Err(error) => Err(error.message),
        }
    }

    fn extra(&self) -> Result<&dyn crate::git_commit_extra::GitCommitExtraSource, String> {
        self.objects
            .commit_extra()
            .ok_or_else(|| "raw diff is not available".to_owned())
    }

    async fn comparison_async(
        &self,
        repo: &MinimalRepo,
        base_revspec: &str,
        head_revspec: &str,
    ) -> Result<ComparisonData, String> {
        // `getCommit(head)`: resolve, then use the OID as the diff head.
        let head_oid = self.resolve_revision(repo, head_revspec).await?;

        let mut range_type = "..".to_owned();
        let mut base_oid = None;
        if base_revspec != sg_git::DEV_NULL_SHA {
            range_type = "...".to_owned();
            let merge_base = self
                .extra()?
                .merge_base(repo.id, &repo.name, base_revspec, head_revspec)
                .await
                .map_err(|error| error.message)?;
            let base_commit = if merge_base.is_empty() {
                // Fallback that works even without a merge base.
                range_type = "..".to_owned();
                base_revspec.to_owned()
            } else {
                merge_base
            };
            if base_commit != sg_git::DEV_NULL_SHA {
                // `getCommit(base)`.
                base_oid = Some(self.resolve_revision(repo, &base_commit).await?);
            }
        }

        // `FileDiffConnectionResolver` diffs the resolved OIDs (the base
        // revspec when the base commit is absent).
        let options = RawDiffOptions {
            base: base_oid.clone().unwrap_or_else(|| base_revspec.to_owned()),
            head: head_oid.clone(),
            range_type: Some(if range_type == ".." {
                DiffRangeType::OnlyInHead
            } else {
                DiffRangeType::Intersection
            }),
            paths: Vec::new(),
        };
        let raw = self
            .extra()?
            .raw_diff(repo.id, &repo.name, &options)
            .await
            .map_err(|error| error.message)?;
        let file_diffs = parse_multi_file_diff(&raw).map_err(|error| error.to_string())?;

        Ok(ComparisonData {
            base_revspec: base_revspec.to_owned(),
            head_revspec: head_revspec.to_owned(),
            range_type,
            base_oid,
            head_oid,
            // The `Changeset.diff` GraphQL surface only reads `fileDiffs`; the
            // commit connection is not prefetched (same as the M9.26c seam).
            file_diffs: Some(Ok(file_diffs)),
            commits: None,
        })
    }

    async fn preview_async(
        &self,
        repo: &MinimalRepo,
        base_rev: &str,
        patch: &[u8],
    ) -> Result<PreviewComparisonObject, String> {
        // `NewPreviewRepositoryComparisonResolver` resolves the base revision
        // (`repo.Commit`) and rejects a missing one.
        let commit_oid = self.resolve_revision(repo, base_rev).await?;
        let file_diffs = parse_multi_file_diff(patch).map_err(|error| error.to_string())?;
        Ok(PreviewComparisonObject {
            repo: repo.clone(),
            commit_oid,
            file_diffs,
        })
    }
}

impl ChangesetDiffSource for GitserverChangesetDiffSource {
    fn comparison(
        &self,
        repo: &MinimalRepo,
        base_revspec: &str,
        head_revspec: &str,
    ) -> Result<ComparisonData, String> {
        crate::batches_graphql_store::block_on(
            self.comparison_async(repo, base_revspec, head_revspec),
        )
    }

    fn preview(
        &self,
        repo: &MinimalRepo,
        base_rev: &str,
        patch: &[u8],
    ) -> Result<PreviewComparisonObject, String> {
        crate::batches_graphql_store::block_on(self.preview_async(repo, base_rev, patch))
    }
}

/// Synchronous read seam for the changeset deep-read surface.
pub trait ChangesetReadDatabase: Send + Sync {
    /// `ListChangesetEvents` with `id >= cursor`, `ORDER BY id ASC` and the
    /// `limit+1` truncation; returns the rows and the next cursor.
    fn changeset_events(
        &self,
        changeset_ids: &[i64],
        kinds: &[&str],
        limit: i64,
        cursor: i64,
    ) -> Result<(Vec<ChangesetEventRow>, i64), String>;

    /// `CountChangesetEvents`.
    fn count_changeset_events(&self, changeset_id: i64) -> Result<i64, String>;

    /// `ScanChangeset`'s `commit_verification` and `sync_state` columns.
    fn changeset_read_extra(&self, changeset_id: i64)
        -> Result<Option<ChangesetReadExtra>, String>;

    /// `GetChangesetSpecByID` (`repo.deleted_at IS NULL`); `None` is
    /// `ErrNoResults`.
    fn changeset_spec_for_diff(
        &self,
        spec_id: i64,
    ) -> Result<Option<ChangesetSpecDiffRow>, String>;

    /// `GetChangesetsStats`.
    fn changesets_stats(&self, batch_change_id: i64) -> Result<ChangesetsStats, String>;

    /// `GetGlobalChangesetsStats`.
    fn global_changesets_stats(&self) -> Result<GlobalChangesetsStats, String>;

    /// `GetBatchChangeDiffStat` (with authz conditions).
    fn batch_change_diff_stat(
        &self,
        batch_change_id: i64,
        actor: Actor,
    ) -> Result<(i64, i64), String>;

    /// `ListBulkOperations`.
    fn bulk_operations(
        &self,
        batch_change_id: i64,
        limit: i64,
        cursor: i64,
        created_after: Option<OffsetDateTime>,
    ) -> Result<(Vec<BulkOperationListRow>, i64), String>;

    /// `CountBulkOperations`.
    fn count_bulk_operations(
        &self,
        batch_change_id: i64,
        created_after: Option<OffsetDateTime>,
    ) -> Result<i64, String>;

    /// `ListBulkOperationErrors`.
    fn bulk_operation_errors(
        &self,
        bulk_group: &str,
    ) -> Result<Vec<BulkOperationErrorRow>, String>;

    /// `ListCodeHosts` (only supported external service types).
    fn code_hosts(&self) -> Result<Vec<CodeHostRow>, String>;

    fn changeset_diff_source(&self) -> Option<&dyn ChangesetDiffSource> {
        None
    }
}

fn read(db: &BatchesHandle) -> Result<&dyn ChangesetReadDatabase, String> {
    db.changeset_read()
        .ok_or_else(|| "batch changes changeset read surface is not wired".to_owned())
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn json_i32(value: i32) -> FieldValue {
    FieldValue::Json(Json::Int(i64::from(value)))
}

fn json_i64(value: i64) -> FieldValue {
    FieldValue::Json(Json::Int(value))
}

fn json_bool(value: bool) -> FieldValue {
    FieldValue::Json(Json::Bool(value))
}

fn json_string(value: impl Into<String>) -> FieldValue {
    FieldValue::Json(Json::Str(value.into()))
}

/// A GraphQL `Float!` value encoded the way graph-gophers/encoding/json
/// encodes `float64`: integral values lose the fractional part (`1.0` -> `1`).
fn json_go_float(value: f64) -> FieldValue {
    if value.is_finite() && value == value.trunc() && value.abs() < 9e15 {
        FieldValue::Json(Json::Int(value as i64))
    } else {
        FieldValue::Json(Json::Float(value))
    }
}

fn parse_datetime_argument(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

/// `strconv.ParseInt(text, 10, 32)` with the baseline's exact error text.
fn go_parse_int32(function: &str, text: &str) -> Result<i64, String> {
    match text.parse::<i64>() {
        Ok(value) if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&value) => Ok(value),
        Ok(_) => Err(format!(
            "strconv.{function}: parsing {text:?}: value out of range"
        )),
        Err(_) => Err(format!("strconv.{function}: parsing {text:?}: invalid syntax")),
    }
}

/// `strconv.Atoi(text)` (64-bit `int`) with the baseline's exact error text.
fn go_atoi(text: &str) -> Result<i64, String> {
    match text.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(_) => {
            let digits = text
                .strip_prefix('+')
                .or_else(|| text.strip_prefix('-'))
                .unwrap_or(text);
            let reason = if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                "value out of range"
            } else {
                "invalid syntax"
            };
            Err(format!("strconv.Atoi: parsing {text:?}: {reason}"))
        }
    }
}

fn minimal_repo(repo: &RepositoryData) -> MinimalRepo {
    MinimalRepo {
        id: repo.id,
        name: repo.name.clone(),
    }
}

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

fn json_string_at(value: &JsonValue, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `Changeset.HeadRefOid`/`HeadRef`/`BaseRefOid`/`BaseRef`, derived from the
/// stored metadata (`internal/batches/types/changeset.go`).
fn refs_from_metadata(
    external_service_type: &str,
    metadata: &JsonValue,
) -> (String, String, String, String) {
    let prefixed = |name: String| format!("refs/heads/{name}");
    match external_service_type {
        "github" => (
            json_string_at(metadata, "/HeadRefOid"),
            prefixed(json_string_at(metadata, "/HeadRefName")),
            json_string_at(metadata, "/BaseRefOid"),
            prefixed(json_string_at(metadata, "/BaseRefName")),
        ),
        "bitbucketServer" => (
            String::new(),
            json_string_at(metadata, "/fromRef/id"),
            String::new(),
            json_string_at(metadata, "/toRef/id"),
        ),
        "gitlab" => (
            json_string_at(metadata, "/diff_refs/head_sha"),
            prefixed(json_string_at(metadata, "/source_branch")),
            json_string_at(metadata, "/diff_refs/base_sha"),
            prefixed(json_string_at(metadata, "/target_branch")),
        ),
        "bitbucketCloud" => (
            json_string_at(metadata, "/source/commit/hash"),
            prefixed(json_string_at(metadata, "/source/branch/name")),
            json_string_at(metadata, "/destination/commit/hash"),
            prefixed(json_string_at(metadata, "/destination/branch/name")),
        ),
        "azuredevops" => (
            String::new(),
            json_string_at(metadata, "/sourceRefName"),
            String::new(),
            json_string_at(metadata, "/targetRefName"),
        ),
        "gerrit" => (
            String::new(),
            String::new(),
            String::new(),
            prefixed(json_string_at(metadata, "/change/branch")),
        ),
        _ => (String::new(), String::new(), String::new(), String::new()),
    }
}

fn parse_check_state(raw: &str) -> Option<ChangesetCheckState> {
    match raw {
        "UNKNOWN" => Some(ChangesetCheckState::Unknown),
        "PENDING" => Some(ChangesetCheckState::Pending),
        "PASSED" => Some(ChangesetCheckState::Passed),
        "FAILED" => Some(ChangesetCheckState::Failed),
        _ => None,
    }
}

/// Build the `batches_state::Changeset` that `CalcCounts` reads.
/// `external_state` is the `changesets.external_state` column, as in
/// `btypes.Changeset`.
fn state_changeset(row: &ChangesetRow) -> Result<Changeset, String> {
    let metadata = parse_changeset_metadata(&row.external_service_type, &row.metadata)?;
    let (head_ref_oid, head_ref, base_ref_oid, base_ref) =
        refs_from_metadata(&row.external_service_type, &row.metadata);
    Ok(Changeset {
        id: row.id,
        external_service_type: row.external_service_type.clone(),
        updated_at: row.updated_at,
        external_deleted_at: row.external_deleted_at,
        metadata,
        external_check_state: parse_check_state(&row.external_check_state),
        external_state: ChangesetExternalState::from_db(&row.external_state),
        external_review_state: ChangesetReviewState::from_db(&row.external_review_state),
        published: row.published(),
        sync_state: ChangesetSyncState::default(),
        base_ref_oid,
        base_ref,
        head_ref_oid,
        head_ref,
        diff_stat: row
            .diff_stat()
            .map(|(added, deleted)| batches_state::DiffStat { added, deleted }),
    })
}

/// `enterprise.BatchChangesEnabledForUser`.
fn require_batch_changes_enabled(db: &BatchesHandle, actor: Actor) -> Result<(), ExecError> {
    if !db.batch_changes_enabled() {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED));
    }
    if db.batch_changes_restricted_to_admins()
        && db
            .current_user_site_admin(actor)
            .map_err(ExecError::new)?
            != Some(true)
    {
        return Err(ExecError::new(BATCH_CHANGES_DISABLED_FOR_USER));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Changeset.events
// ---------------------------------------------------------------------------

pub struct ChangesetEventsConnectionObject {
    db: BatchesHandle,
    row: ChangesetRow,
    repo: Option<RepositoryData>,
    first: i64,
    cursor: i64,
    events: OnceLock<Result<(Vec<ChangesetEventRow>, i64), String>>,
}

impl ChangesetEventsConnectionObject {
    fn new(
        db: BatchesHandle,
        row: ChangesetRow,
        repo: Option<RepositoryData>,
        arguments: &Arguments,
    ) -> Result<Self, ExecError> {
        let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
        validate_first_param(first).map_err(ExecError::new)?;
        let cursor = match arguments.string("after") {
            Some(after) => go_parse_int32("ParseInt", after).map_err(|error| {
                ExecError::new(format!("failed to parse after cursor: {error}"))
            })?,
            None => 0,
        };
        Ok(Self {
            db,
            row,
            repo,
            first,
            cursor,
            events: OnceLock::new(),
        })
    }

    fn compute(&self) -> Result<&(Vec<ChangesetEventRow>, i64), String> {
        self.events
            .get_or_init(|| {
                read(&self.db)?.changeset_events(&[self.row.id], &[], self.first, self.cursor)
            })
            .as_ref()
            .map_err(Clone::clone)
    }
}

impl ObjectValue for ChangesetEventsConnectionObject {
    fn type_name(&self) -> &'static str {
        "ChangesetEventConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let (events, _) = self.compute().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: events
                        .iter()
                        .map(|event| {
                            FieldValue::Object(Arc::new(ChangesetEventObject {
                                db: self.db.clone(),
                                changeset: self.row.clone(),
                                repo: self.repo.clone(),
                                event: event.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("ChangesetEvent"),
                }))
            }
            "totalCount" => {
                let count = read(&self.db)
                    .map_err(ExecError::new)?
                    .count_changeset_events(self.row.id)
                    .map_err(ExecError::new)?;
                Ok(Some(json_i64(count)))
            }
            "pageInfo" => {
                let (_, next) = self.compute().map_err(ExecError::new)?;
                let (has_next, cursor) = if *next != 0 {
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

pub struct ChangesetEventObject {
    db: BatchesHandle,
    changeset: ChangesetRow,
    repo: Option<RepositoryData>,
    event: ChangesetEventRow,
}

impl ObjectValue for ChangesetEventObject {
    fn type_name(&self) -> &'static str {
        "ChangesetEvent"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_string(marshal_changeset_event_id(self.event.id)))),
            "changeset" => Ok(Some(FieldValue::Object(Arc::new(ChangesetObject::new(
                self.changeset.clone(),
                self.db.clone(),
                self.repo.clone(),
            ))))),
            "createdAt" => Ok(Some(json_string(datetime_rfc3339(self.event.created_at)))),
            _ => Ok(None),
        }
    }
}

fn changeset_events_field(
    object: &ChangesetObject,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    Ok(Some(FieldValue::Object(Arc::new(
        ChangesetEventsConnectionObject::new(
            object.db.clone(),
            object.row.clone(),
            object.repo.clone(),
            arguments,
        )?,
    ))))
}

// ---------------------------------------------------------------------------
// Changeset.commitVerification
// ---------------------------------------------------------------------------

pub struct GitHubCommitVerificationObject {
    pub verified: bool,
    pub reason: String,
    pub signature: String,
    pub payload: String,
}

impl ObjectValue for GitHubCommitVerificationObject {
    fn type_name(&self) -> &'static str {
        "GitHubCommitVerification"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "verified" => Ok(Some(json_bool(self.verified))),
            "reason" => Ok(Some(json_string(self.reason.clone()))),
            "signature" => Ok(Some(json_string(self.signature.clone()))),
            "payload" => Ok(Some(json_string(self.payload.clone()))),
            _ => Ok(None),
        }
    }
}

/// `ScanChangeset` only keeps the verification when `verified` is true, and
/// the resolver only exposes it for GitHub.
fn changeset_commit_verification_field(
    object: &ChangesetObject,
) -> Result<Option<FieldValue>, ExecError> {
    if object.row.external_service_type != "github" {
        return Ok(Some(FieldValue::Null));
    }
    let Some(extra) = read(&object.db)
        .map_err(ExecError::new)?
        .changeset_read_extra(object.row.id)
        .map_err(ExecError::new)?
    else {
        return Ok(Some(FieldValue::Null));
    };
    let value = &extra.commit_verification;
    if value.get("verified").and_then(JsonValue::as_bool) != Some(true) {
        return Ok(Some(FieldValue::Null));
    }
    Ok(Some(FieldValue::Object(Arc::new(
        GitHubCommitVerificationObject {
            verified: true,
            reason: json_string_at(value, "/reason"),
            signature: json_string_at(value, "/signature"),
            payload: json_string_at(value, "/payload"),
        },
    ))))
}

// ---------------------------------------------------------------------------
// Changeset.diff
// ---------------------------------------------------------------------------

/// `PreviewRepositoryComparison`: `fileDiffs` are parsed from the changeset
/// spec patch; `baseRepository` is the changeset's repository and
/// `commit_oid` is the gitserver-resolved `baseRev`.
pub struct PreviewComparisonObject {
    pub repo: MinimalRepo,
    pub commit_oid: String,
    pub file_diffs: Vec<FileDiff>,
}

impl ObjectValue for PreviewComparisonObject {
    fn type_name(&self) -> &'static str {
        "PreviewRepositoryComparison"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "baseRepository" => Ok(Some(FieldValue::Object(Arc::new(
                RepositoryObject::from_data(repository_data(&self.repo)),
            )))),
            "fileDiffs" => Ok(Some(FieldValue::Object(Arc::new(FileDiffConnectionObject {
                repo: self.repo.clone(),
                file_diffs: Ok(self.file_diffs.clone()),
                first: arguments.i64("first").map(|value| value as i32),
                after: arguments.string("after").map(str::to_owned),
                base_oid: Some(self.commit_oid.clone()),
                head_oid: self.commit_oid.clone(),
                new_file: NewFileSource::Tree,
                git: None,
            })))),
            _ => Ok(None),
        }
    }
}

fn repository_data(repo: &MinimalRepo) -> RepositoryData {
    RepositoryData {
        id: repo.id,
        name: repo.name.clone(),
        uri: repo.name.clone(),
        description: String::new(),
        fork: false,
        archived: false,
        private: false,
        stars: 0,
        external_id: String::new(),
        external_service_type: String::new(),
        external_service_id: String::new(),
    }
}

fn changeset_diff_field(object: &ChangesetObject) -> Result<Option<FieldValue>, ExecError> {
    if object.row.is_importing() {
        return Ok(Some(FieldValue::Null));
    }
    let read_db = read(&object.db).map_err(ExecError::new)?;
    let extra = read_db
        .changeset_read_extra(object.row.id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("changeset not found"))?;
    let (head_ref_oid, head_ref, base_ref_oid, base_ref) =
        refs_from_metadata(&object.row.external_service_type, &object.row.metadata);

    if !object.row.published() || extra.sync_state.base_ref_oid == extra.sync_state.head_ref_oid {
        // `getBranchSpecDescription`.
        if object.row.current_spec_id == 0 {
            return Err(ExecError::new("Changeset has no ChangesetSpec"));
        }
        let spec = read_db
            .changeset_spec_for_diff(object.row.current_spec_id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new("no results"))?;
        if spec.spec_type == "existing" {
            return Err(ExecError::new("ChangesetSpec imports a changeset"));
        }
        let Some(source) = read_db.changeset_diff_source() else {
            return Ok(Some(FieldValue::Null));
        };
        let Some(repo) = &object.repo else {
            return Ok(Some(FieldValue::Null));
        };
        let preview = source
            .preview(&minimal_repo(repo), &spec.base_rev, &spec.diff)
            .map_err(ExecError::new)?;
        return Ok(Some(FieldValue::Object(Arc::new(preview))));
    }

    // `Changeset.HasDiff`: only draft/open changesets have a diff.
    if object.row.external_state != "DRAFT" && object.row.external_state != "OPEN" {
        return Ok(Some(FieldValue::Null));
    }

    let base = if base_ref_oid.is_empty() {
        base_ref
    } else {
        base_ref_oid
    };
    let head = if head_ref_oid.is_empty() {
        head_ref
    } else {
        head_ref_oid
    };
    let Some(repo) = &object.repo else {
        return Ok(Some(FieldValue::Null));
    };
    let Some(source) = read_db.changeset_diff_source() else {
        return Ok(Some(FieldValue::Null));
    };
    let data = source
        .comparison(&minimal_repo(repo), &base, &head)
        .map_err(ExecError::new)?;
    Ok(Some(FieldValue::Object(Arc::new(
        RepositoryComparisonObject {
            repo: minimal_repo(repo),
            data,
            git: None,
        },
    ))))
}

// ---------------------------------------------------------------------------
// Changeset.batchChanges
// ---------------------------------------------------------------------------

/// `changesetResolver.BatchChanges`. Unlike `Resolver.BatchChanges`, this path
/// does not set `ExcludeDraftsNotOwnedByUserID`.
fn changeset_batch_changes_field(
    object: &ChangesetObject,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let mut states: Vec<String> = Vec::new();
    if let Some(state) =
        parse_batch_change_state(arguments.string("state")).map_err(ExecError::new)?
    {
        states.push(state.to_owned());
    }
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
        Some(after) => go_parse_int32("ParseInt", after).map_err(ExecError::new)?,
        None => 0,
    };

    let mut filter = BatchChangeFilter {
        limit: first,
        cursor,
        states,
        changeset_id: object.row.id,
        ..BatchChangeFilter::default()
    };

    // `auth.CheckCurrentUserIsSiteAdmin`; `ErrMustBeSiteAdmin` (and the
    // baseline's ignored `err`) both mean "not a site admin".
    let is_site_admin =
        object.db.current_user_site_admin(actor).map_err(ExecError::new)? == Some(true);
    if !is_site_admin && arguments.bool("viewerCanAdminister").unwrap_or(false) {
        filter.only_administered_by_user_id = actor.uid().unwrap_or(0);
    }

    Ok(Some(FieldValue::Object(Arc::new(
        crate::batches_graphql::BatchChangeConnectionObject::new(
            object.db.clone(),
            filter,
            actor,
        ),
    ))))
}

// ---------------------------------------------------------------------------
// BatchChange.changesetsStats
// ---------------------------------------------------------------------------

pub struct ChangesetsStatsObject {
    pub stats: ChangesetsStats,
}

impl ChangesetsStatsObject {
    /// `changesetsStatsResolver.IsCompleted`.
    fn is_completed(&self) -> bool {
        let merged_and_closed = self.stats.closed + self.stats.merged;
        let included = self.stats.total - self.stats.archived - self.stats.deleted;
        self.stats.total != 0 && merged_and_closed == included
    }

    /// `changesetsStatsResolver.PercentComplete` (float32 arithmetic).
    fn percent_complete(&self) -> i32 {
        if self.stats.total == 0 {
            return 0;
        }
        let merged_and_closed = (self.stats.merged + self.stats.closed) as f32;
        let included = (self.stats.total - self.stats.archived - self.stats.deleted) as f32;
        if included == 0.0 {
            return 100;
        }
        ((merged_and_closed / included) * 100.0) as i32
    }
}

impl ObjectValue for ChangesetsStatsObject {
    fn type_name(&self) -> &'static str {
        "ChangesetsStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "unpublished" => json_i32(self.stats.unpublished),
            "draft" => json_i32(self.stats.draft),
            "open" => json_i32(self.stats.open),
            "merged" => json_i32(self.stats.merged),
            "closed" => json_i32(self.stats.closed),
            "deleted" => json_i32(self.stats.deleted),
            "retrying" => json_i32(self.stats.retrying),
            "failed" => json_i32(self.stats.failed),
            "scheduled" => json_i32(self.stats.scheduled),
            "processing" => json_i32(self.stats.processing),
            "archived" => json_i32(self.stats.archived),
            "total" => json_i32(self.stats.total),
            "isCompleted" => json_bool(self.is_completed()),
            "percentComplete" => json_i32(self.percent_complete()),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

pub struct GlobalChangesetsStatsObject {
    pub stats: GlobalChangesetsStats,
}

impl ObjectValue for GlobalChangesetsStatsObject {
    fn type_name(&self) -> &'static str {
        "GlobalChangesetsStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "unpublished" => json_i32(self.stats.unpublished),
            "draft" => json_i32(self.stats.draft),
            "open" => json_i32(self.stats.open),
            "merged" => json_i32(self.stats.merged),
            "closed" => json_i32(self.stats.closed),
            "total" => json_i32(self.stats.total),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

// ---------------------------------------------------------------------------
// BatchChange.changesetCountsOverTime
// ---------------------------------------------------------------------------

pub struct ChangesetCountsObject {
    pub counts: ChangesetCounts,
}

impl ObjectValue for ChangesetCountsObject {
    fn type_name(&self) -> &'static str {
        "ChangesetCounts"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "date" => json_string(datetime_rfc3339(self.counts.time)),
            "total" => json_i32(self.counts.total),
            "merged" => json_i32(self.counts.merged),
            "closed" => json_i32(self.counts.closed),
            "draft" => json_i32(self.counts.draft),
            "open" => json_i32(self.counts.open),
            "openApproved" => json_i32(self.counts.open_approved),
            "openChangesRequested" => json_i32(self.counts.open_changes_requested),
            "openPending" => json_i32(self.counts.open_pending),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

fn changeset_counts_over_time(
    db: &BatchesHandle,
    row: &BatchChangeRow,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Vec<ChangesetCounts>, ExecError> {
    let include_archived = arguments.bool("includeArchived").unwrap_or(false);
    let filter = ChangesetFilter {
        batch_change_id: row.id,
        publication_state: Some("PUBLISHED".to_owned()),
        include_archived,
        ..ChangesetFilter::default()
    };
    let changeset_rows = db
        .list_changesets(&filter, actor)
        .map_err(ExecError::new)?
        .rows;

    let mut events: Vec<ChangesetEvent> = Vec::new();
    if !changeset_rows.is_empty() {
        let ids: Vec<i64> = changeset_rows.iter().map(|row| row.id).collect();
        let (event_rows, _) = read(db)
            .map_err(ExecError::new)?
            .changeset_events(&ids, batches_state::REQUIRED_EVENT_TYPES_FOR_HISTORY, 0, 0)
            .map_err(ExecError::new)?;
        for event in event_rows {
            let metadata = parse_event_metadata(&event.kind, &event.metadata, event.created_at)
                .map_err(ExecError::new)?;
            events.push(ChangesetEvent {
                changeset_id: event.changeset_id,
                kind: event.kind,
                metadata,
            });
        }
    }
    // `CalcCounts` depends on globally sorted events.
    batches_state::sort_events(&mut events);

    let mut changesets = Vec::with_capacity(changeset_rows.len());
    for row in &changeset_rows {
        changesets.push(state_changeset(row).map_err(ExecError::new)?);
    }

    let now = db.now();
    let week_ago = now - time::Duration::days(7);
    let mut start = row.created_at;
    if let Some(first) = events.first() {
        if let Some(timestamp) = batches_state::event_timestamp(first) {
            start = timestamp;
        }
    }
    // At least a week lookback, more if the batch change was created earlier.
    if start > week_ago {
        start = week_ago;
    }
    if let Some(from) = arguments.string("from").and_then(parse_datetime_argument) {
        start = from;
    }
    let mut end = now;
    if let Some(to) = arguments.string("to").and_then(parse_datetime_argument) {
        if to < end {
            end = to;
        }
    }

    batches_state::calc_counts(start, end, &changesets, &events).map_err(ExecError::new)
}

// ---------------------------------------------------------------------------
// BatchChange.bulkOperations
// ---------------------------------------------------------------------------

pub struct BulkOperationConnectionObject {
    db: BatchesHandle,
    batch_change_id: i64,
    first: i64,
    cursor: i64,
    created_after: Option<OffsetDateTime>,
    page: OnceLock<Result<(Vec<BulkOperationListRow>, i64), String>>,
    total: OnceLock<Result<i64, String>>,
}

impl BulkOperationConnectionObject {
    fn new(
        db: BatchesHandle,
        batch_change_id: i64,
        arguments: &Arguments,
    ) -> Result<Self, ExecError> {
        let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
        validate_first_param(first).map_err(ExecError::new)?;
        let cursor = match arguments.string("after") {
            Some(after) => go_atoi(after).map_err(ExecError::new)?,
            None => 0,
        };
        let created_after = arguments
            .string("createdAfter")
            .and_then(parse_datetime_argument);
        Ok(Self {
            db,
            batch_change_id,
            first,
            cursor,
            created_after,
            page: OnceLock::new(),
            total: OnceLock::new(),
        })
    }

    fn page(&self) -> Result<&(Vec<BulkOperationListRow>, i64), String> {
        self.page
            .get_or_init(|| {
                read(&self.db)?.bulk_operations(
                    self.batch_change_id,
                    self.first,
                    self.cursor,
                    self.created_after,
                )
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn total(&self) -> Result<i64, String> {
        self.total
            .get_or_init(|| {
                read(&self.db)?
                    .count_bulk_operations(self.batch_change_id, self.created_after)
            })
            .clone()
    }
}

impl ObjectValue for BulkOperationConnectionObject {
    fn type_name(&self) -> &'static str {
        "BulkOperationConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let (rows, _) = self.page().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: rows
                        .iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(BulkOperationObject {
                                db: self.db.clone(),
                                row: row.operation.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("BulkOperation"),
                }))
            }
            "totalCount" => Ok(Some(json_i64(self.total().map_err(ExecError::new)?))),
            "pageInfo" => {
                let (_, next) = self.page().map_err(ExecError::new)?;
                let (has_next, cursor) = if *next != 0 {
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

pub struct BulkOperationObject {
    db: BatchesHandle,
    row: BulkOperationRow,
}

impl BulkOperationObject {
    /// Build a `BulkOperation` resolver from a `store.GetBulkOperation` /
    /// `ListBulkOperations` row (M9.27c node and mutation results).
    pub fn from_row(db: BatchesHandle, row: BulkOperationRow) -> Self {
        Self { db, row }
    }
}

/// `changesetJobTypeToBulkOperationType`.
fn bulk_operation_type(job_type: &str) -> Result<&'static str, ExecError> {
    match job_type {
        "commentatore" => Ok("COMMENT"),
        "detach" => Ok("DETACH"),
        "reenqueue" => Ok("REENQUEUE"),
        "merge" => Ok("MERGE"),
        "export" => Ok("EXPORT"),
        "close" => Ok("CLOSE"),
        "publish" => Ok("PUBLISH"),
        other => Err(ExecError::new(format!("invalid job type {other:?}"))),
    }
}

impl ObjectValue for BulkOperationObject {
    fn type_name(&self) -> &'static str {
        "BulkOperation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_string(marshal_bulk_operation_id(&self.row.id)))),
            "type" => Ok(Some(json_string(bulk_operation_type(&self.row.job_type)?))),
            "state" => Ok(Some(json_string(self.row.state.clone()))),
            "progress" => Ok(Some(json_go_float(self.row.progress))),
            "errors" => {
                let rows = read(&self.db)
                    .map_err(ExecError::new)?
                    .bulk_operation_errors(&self.row.id)
                    .map_err(ExecError::new)?;
                let mut changesets = Vec::with_capacity(rows.len());
                let mut repo_ids = Vec::new();
                for row in &rows {
                    let changeset = self
                        .db
                        .get_changeset(row.changeset_id)
                        .map_err(ExecError::new)?;
                    if let Some(changeset) = &changeset {
                        repo_ids.push(changeset.repo_id);
                    }
                    changesets.push(changeset);
                }
                let repos = self
                    .db
                    .repos_by_ids(&repo_ids, actor)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: rows
                        .iter()
                        .zip(changesets)
                        .map(|(row, changeset)| {
                            let repo = changeset
                                .as_ref()
                                .and_then(|changeset| repos.get(&changeset.repo_id).cloned());
                            let error = repo.as_ref().map(|_| row.error.clone());
                            FieldValue::Object(Arc::new(ChangesetJobErrorObject {
                                db: self.db.clone(),
                                changeset,
                                repo,
                                error,
                            }))
                        })
                        .collect(),
                    element_type: Some("ChangesetJobError"),
                }))
            }
            "createdAt" => Ok(Some(json_string(datetime_rfc3339(self.row.created_at)))),
            "finishedAt" => Ok(Some(match self.row.finished_at {
                Some(finished_at) => json_string(datetime_rfc3339(finished_at)),
                None => FieldValue::Null,
            })),
            "initiator" => match self
                .db
                .user_by_id(self.row.user_id)
                .map_err(ExecError::new)?
            {
                Some(user) => {
                    let admin = self
                        .db
                        .current_user_site_admin(actor)
                        .map_err(ExecError::new)?
                        == Some(true);
                    Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_resolvers::UserObject::from_data_as_viewer(user, admin),
                    ))))
                }
                None => Err(ExecError::new("user not found")),
            },
            "changesetCount" => Ok(Some(json_i64(self.row.changeset_count))),
            _ => Ok(None),
        }
    }
}

pub struct ChangesetJobErrorObject {
    db: BatchesHandle,
    changeset: Option<ChangesetRow>,
    repo: Option<RepositoryData>,
    error: Option<String>,
}

impl ObjectValue for ChangesetJobErrorObject {
    fn type_name(&self) -> &'static str {
        "ChangesetJobError"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "changeset" => match &self.changeset {
                Some(changeset) => Ok(Some(FieldValue::Object(Arc::new(ChangesetObject::new(
                    changeset.clone(),
                    self.db.clone(),
                    self.repo.clone(),
                ))))),
                None => Ok(Some(FieldValue::Null)),
            },
            // Only shown when the changeset's repository is visible.
            "error" => Ok(Some(match &self.error {
                Some(error) => json_string(error.clone()),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Query.batchChangesCodeHosts
// ---------------------------------------------------------------------------

pub struct BatchChangesCodeHostConnectionObject {
    db: BatchesHandle,
    /// `nil` for `Query.batchChangesCodeHosts`; the user for
    /// `User.batchChangesCodeHosts` / `viewerBatchChangesCodeHosts`.
    user_id: Option<i32>,
    only_without_credential: bool,
    all: OnceLock<Result<Vec<CodeHostRow>, String>>,
    credentials: OnceLock<
        Result<HashMap<(String, String), crate::batches_credentials::ResolvedCredential>, String>,
    >,
    first: i64,
    offset: usize,
}

impl BatchChangesCodeHostConnectionObject {
    pub fn new(
        db: BatchesHandle,
        user_id: Option<i32>,
        only_without_credential: bool,
        arguments: &Arguments,
    ) -> Result<Self, ExecError> {
        let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
        validate_first_param(first).map_err(ExecError::new)?;
        let offset = match arguments.string("after") {
            Some(after) => go_parse_int32("ParseInt", after)
                .map_err(ExecError::new)?
                .max(0) as usize,
            None => 0,
        };
        Ok(Self {
            db,
            user_id,
            only_without_credential,
            all: OnceLock::new(),
            credentials: OnceLock::new(),
            first,
            offset,
        })
    }

    fn credentials(
        &self,
    ) -> Result<&HashMap<(String, String), crate::batches_credentials::ResolvedCredential>, String>
    {
        self.credentials
            .get_or_init(|| crate::batches_credentials::resolve_credentials(&self.db, self.user_id))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn all(&self) -> Result<&Vec<CodeHostRow>, String> {
        self.all
            .get_or_init(|| {
                let hosts = read(&self.db)?.code_hosts()?;
                if !self.only_without_credential {
                    return Ok(hosts);
                }
                // `code_host_connection.go:121-133`: keep hosts without a
                // credential for the resolved `(type, url)`.
                let credentials = self.credentials()?;
                Ok(hosts
                    .into_iter()
                    .filter(|host| {
                        !credentials.contains_key(&(
                            host.external_service_type.clone(),
                            host.external_service_id.clone(),
                        ))
                    })
                    .collect())
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn page(&self) -> Result<&[CodeHostRow], String> {
        let all = self.all()?;
        let after_idx = self.offset;
        if after_idx >= all.len() {
            return Ok(&[]);
        }
        if self.first <= 0 {
            return Ok(&all[after_idx..]);
        }
        let mut limit = self.first as usize;
        if limit + after_idx >= all.len() {
            limit = all.len() - after_idx;
        }
        Ok(&all[after_idx..limit + after_idx])
    }
}

impl ObjectValue for BatchChangesCodeHostConnectionObject {
    fn type_name(&self) -> &'static str {
        "BatchChangesCodeHostConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let credentials = self.credentials().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: self
                        .page()
                        .map_err(ExecError::new)?
                        .iter()
                        .map(|host| {
                            let credential = credentials
                                .get(&(
                                    host.external_service_type.clone(),
                                    host.external_service_id.clone(),
                                ))
                                .cloned();
                            FieldValue::Object(Arc::new(BatchChangesCodeHostObject {
                                host: host.clone(),
                                db: Some(self.db.clone()),
                                credential,
                            }))
                        })
                        .collect(),
                    element_type: Some("BatchChangesCodeHost"),
                }))
            }
            "totalCount" => Ok(Some(json_i64(
                self.all().map_err(ExecError::new)?.len() as i64
            ))),
            "pageInfo" => {
                let all = self.all().map_err(ExecError::new)?;
                // `idx := limitOffset.Limit + limitOffset.Offset`.
                let idx = self.first as usize + self.offset;
                let (has_next, cursor) = if idx < all.len() {
                    (true, Some(idx.to_string()))
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

pub struct BatchChangesCodeHostObject {
    host: CodeHostRow,
    db: Option<BatchesHandle>,
    credential: Option<crate::batches_credentials::ResolvedCredential>,
}

impl BatchChangesCodeHostObject {
    /// A code host without credential resolution (used by call sites that do
    /// not have the batches handle at hand).
    pub fn new(host: CodeHostRow) -> Self {
        Self {
            host,
            db: None,
            credential: None,
        }
    }

    /// A code host carrying the credential resolved for its `(type, url)`.
    pub fn with_credential(
        db: BatchesHandle,
        host: CodeHostRow,
        credential: Option<crate::batches_credentials::ResolvedCredential>,
    ) -> Self {
        Self {
            host,
            db: Some(db),
            credential,
        }
    }

    /// `code_host.go` `CommitSigningConfiguration`: only GitHub code hosts
    /// resolve, via `GetByDomainAndKind("batches", "COMMIT_SIGNING", baseURL)`
    /// where the URL match trims a trailing `/`. A missing app is `null`.
    fn commit_signing_configuration(&self) -> Result<FieldValue, String> {
        if self.host.external_service_type != "github" {
            return Ok(FieldValue::Null);
        }
        let Some(db) = &self.db else {
            return Ok(FieldValue::Null);
        };
        let Some(handle) = crate::batches_credentials::github_apps_handle(db) else {
            return Ok(FieldValue::Null);
        };
        match handle.get_app_by_domain_and_kind(
            "batches",
            "COMMIT_SIGNING",
            &self.host.external_service_id,
        )? {
            Some(app) => Ok(FieldValue::Object(Arc::new(
                crate::github_apps_graphql::GitHubAppObject::new(app, handle),
            ))),
            None => Ok(FieldValue::Null),
        }
    }
}

impl ObjectValue for BatchChangesCodeHostObject {
    fn type_name(&self) -> &'static str {
        "BatchChangesCodeHost"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "externalServiceKind" => Ok(Some(json_string(type_to_kind(
                &self.host.external_service_type,
            )))),
            "externalServiceURL" => Ok(Some(json_string(self.host.external_service_id.clone()))),
            "credential" => Ok(Some(match (&self.db, &self.credential) {
                (Some(db), Some(resolved)) => FieldValue::Object(Arc::new(
                    crate::batches_credentials::BatchChangesCredentialObject::new(
                        db.clone(),
                        resolved.clone(),
                    ),
                )),
                _ => FieldValue::Null,
            })),
            "commitSigningConfiguration" => Ok(Some(
                self.commit_signing_configuration().map_err(ExecError::new)?,
            )),
            "requiresSSH" => Ok(Some(json_bool(self.host.requires_ssh))),
            "requiresUsername" => Ok(Some(json_bool(matches!(
                self.host.external_service_type.as_str(),
                "bitbucketCloud" | "azuredevops" | "gerrit" | "perforce"
            )))),
            "supportsCommitSigning" => Ok(Some(json_bool(
                self.host.external_service_type == "github",
            ))),
            "hasWebhooks" => Ok(Some(json_bool(self.host.has_webhooks))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Fields of `BatchChange` owned by this module.
pub fn batch_change_field(
    object: &BatchChangeObject,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "changesetsStats" => {
            let stats = read(&object.db)
                .map_err(ExecError::new)?
                .changesets_stats(object.row.id)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(ChangesetsStatsObject {
                stats,
            }))))
        }
        "changesetCountsOverTime" => {
            let counts = changeset_counts_over_time(&object.db, &object.row, arguments, actor)?;
            Ok(Some(FieldValue::List {
                items: counts
                    .into_iter()
                    .map(|counts| FieldValue::Object(Arc::new(ChangesetCountsObject { counts })))
                    .collect(),
                element_type: Some("ChangesetCounts"),
            }))
        }
        "diffStat" => {
            let (added, deleted) = read(&object.db)
                .map_err(ExecError::new)?
                .batch_change_diff_stat(object.row.id, actor)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(DiffStatObject {
                added: added as i32,
                deleted: deleted as i32,
            }))))
        }
        "bulkOperations" => Ok(Some(FieldValue::Object(Arc::new(
            BulkOperationConnectionObject::new(object.db.clone(), object.row.id, arguments)?,
        )))),
        _ => Ok(None),
    }
}

/// Fields of `Changeset` owned by this module (`ExternalChangeset` fields).
pub fn changeset_field(
    object: &ChangesetObject,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "events" => changeset_events_field(object, arguments),
        "diff" => changeset_diff_field(object),
        "commitVerification" => changeset_commit_verification_field(object),
        "batchChanges" => changeset_batch_changes_field(object, arguments, actor),
        _ => Ok(None),
    }
}

/// `Query.globalChangesetsStats` / `Query.batchChangesCodeHosts`.
pub fn root_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "globalChangesetsStats" => {
            require_batch_changes_enabled(db, actor)?;
            let stats = read(db)
                .map_err(ExecError::new)?
                .global_changesets_stats()
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                GlobalChangesetsStatsObject { stats },
            ))))
        }
        "batchChangesCodeHosts" => {
            code_hosts_connection_field(db, None, false, arguments, actor)
        }
        _ => Ok(None),
    }
}

/// `User.batchChangesCodeHosts`: the resolver passes `args.UserID = user.ID`
/// (`graphqlbackend/user.go:761-764`).
pub fn user_batch_changes_code_hosts(
    db: &BatchesHandle,
    user_id: i32,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    code_hosts_connection_field(db, Some(user_id), false, arguments, actor)?
        .ok_or_else(|| ExecError::new("batchChangesCodeHosts is not wired"))
}

/// `Resolver.BatchChangesCodeHosts` (`resolver.go:874-897`), shared by the
/// `Query`, `User` and viewer entrypoints.
pub fn code_hosts_connection_field(
    db: &BatchesHandle,
    user_id: Option<i32>,
    only_without_credential: bool,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    require_batch_changes_enabled(db, actor)?;
    if let Some(user_id) = user_id {
        // 🚨 SECURITY: Only viewable for self or by site admins.
        crate::batches_credentials::check_site_admin_or_same_user(
            &**db, actor, user_id,
        )
        .map_err(ExecError::new)?;
    }
    Ok(Some(FieldValue::Object(Arc::new(
        BatchChangesCodeHostConnectionObject::new(
            db.clone(),
            user_id,
            only_without_credential,
            arguments,
        )?,
    ))))
}

/// Whether `field` is a root field owned by this module.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "globalChangesetsStats" | "batchChangesCodeHosts")
}
