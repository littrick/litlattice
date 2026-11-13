#![forbid(unsafe_code)]

//! `cmd/worker/internal/batches/{reconciler_job.go,workers/reconciler_worker.go}`
//! + `internal/batches/reconciler/{reconciler.go,plan.go,executor.go,
//! publication_state.go}` + `internal/batches/store/worker_reconciler.go`:
//!   the `batches-reconciler` job.
//!
//! The worker dequeues `changesets` rows through the workerutil store
//! (`reconciler_state` is the queue state column), loads the current and
//! previous `changeset_specs`, computes a [`Plan`] and executes it against a
//! [`ChangesetSource`].
//!
//! Code-host source calls go through the [`ChangesetSourceFactory`] seam.
//! [`ExtsvcSourceFactory`] builds the real GitHub/GitLab/Bitbucket Server
//! clients from `sg-extsvc` with an injectable [`sg_extsvc::HttpDoer`];
//! production wires [`ConfigSourceFactory`] over
//! [`PgSourceConfigProvider`] (decrypted `external_services` configs), while
//! [`NotPortedSourceFactory`] remains the test/injection fallback.
//! gitserver push ([`ChangesetSource::push_commit`]), `Changeset.Events()`
//! derivation and GitHub App commit signing remain explicit seams, see the
//! slice overview §4. `state.SetDerivedState` is applied by
//! [`ReconcilerStore::set_derived_state`]: the production store reloads the
//! persisted metadata/events/repo and calls
//! [`sg_extsvc::batches_state::set_derived_state`] with no gitserver client
//! (sync state/diffstat stay as persisted). Outbound webhooks for the
//! after-done hook go through
//! [`sg_extsvc::batches_webhooks`] into `outbound_webhook_jobs`.
//!
//! Handler transaction semantics match `Reconciler.HandlerFunc`: `process`
//! runs inside a store transaction, `done(err)` commits or rolls back, and the
//! after-done webhook hook runs regardless of the transaction outcome.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_db::batches::{
    self as db, ChangesetSpecRow, ChangesetUpdate, ReconcilerChangesetRow, ReconcilerRepo,
    RECONCILER_MAX_NUM_RESETS, RECONCILER_MAX_NUM_RETRIES, RECONCILER_RETRY_AFTER_SECONDS,
    RECONCILER_STALLED_MAX_AGE_SECONDS, RECONCILER_STORE_NAME, RECONCILER_TABLE, RECONCILER_VIEW,
    RECONCILER_WORKER_DESCRIPTION, RECONCILER_WORKER_NAME,
};
use sg_extsvc::batches_sources as extsvc;
use sg_db::OffsetDateTime;
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use super::batches::batches_enabled;
use super::{Job, JobDeps};
use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbRecord, DbWorkerHandler, DbWorkerStore, WorkerOptions,
    WorkerRoutine,
};
use crate::framework::BackgroundRoutine;

/// `reconcilerJob.Description`.
pub const JOB_DESCRIPTION: &str = "";
/// `workerutil.WorkerOptions.Interval`.
pub const WORKER_INTERVAL: Duration = Duration::from_secs(5);
/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `workerutil.WorkerOptions.NumHandlers`.
pub const NUM_HANDLERS: usize = 5;
/// `sleep()`'s duration.
pub const SYNC_SLEEP: Duration = Duration::from_secs(3);

/// `errCannotPushToArchivedRepo`.
pub const CANNOT_PUSH_TO_ARCHIVED_REPO: &str = "cannot push to an archived repo";
/// `errPublishSameBranch`.
pub const PUBLISH_SAME_BRANCH: &str =
    "cannot create changeset on the same branch in multiple batch changes";
/// `errNoSSHCredential`.
pub const NO_SSH_CREDENTIAL: &str =
    "The used credential doesn't support SSH pushes, but the repo requires pushing over SSH.";
/// `ToDraftChangesetSource`'s error.
pub const NO_DRAFT_SUPPORT: &str = "changeset source doesn't implement DraftChangesetSource";
/// The production seam for unported code-host clients.
pub const SOURCE_NOT_PORTED: &str =
    "changeset source is not ported (M9.9g); no code-host API client is available";

/// `internal/licensing.FeatureBatchChanges` (`features.go`).
///
/// The Go worker never calls `Check`: `cmd/worker/internal/batches/*.go` only
/// gates on `batches.IsEnabled()` (`internal/batches/conf.go`), while the
/// frontend GraphQL resolvers call `checkLicense()` before the batch-change
/// mutations. There is no license subsystem in the worker, so this seam keeps
/// the Go default (allow) and lets a license-aware deployment inject the
/// frontend check without changing the job signatures.
pub trait BatchChangesLicenseGate: Send + Sync {
    /// `(*FeatureBatchChanges).Check`; `Ok(())` when Batch Changes is licensed.
    fn check_feature_batch_changes(&self) -> Result<(), String>;
}

/// Default gate: no license check, matching the worker baseline.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerBatchChangesLicenseGate;

impl BatchChangesLicenseGate for WorkerBatchChangesLicenseGate {
    fn check_feature_batch_changes(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Domain model (`internal/batches/types`)
// ---------------------------------------------------------------------------

/// `ChangesetPublicationState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetPublicationState {
    Unpublished,
    Published,
}

impl ChangesetPublicationState {
    pub fn from_db(raw: &str) -> Self {
        match raw {
            "PUBLISHED" => Self::Published,
            _ => Self::Unpublished,
        }
    }

    pub fn published(self) -> bool {
        self == Self::Published
    }

    pub fn unpublished(self) -> bool {
        self == Self::Unpublished
    }
}

/// `ChangesetExternalState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetExternalState {
    Draft,
    Open,
    Closed,
    Merged,
    Deleted,
    ReadOnly,
}

impl ChangesetExternalState {
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "DRAFT" => Some(Self::Draft),
            "OPEN" => Some(Self::Open),
            "CLOSED" => Some(Self::Closed),
            "MERGED" => Some(Self::Merged),
            "DELETED" => Some(Self::Deleted),
            "READONLY" => Some(Self::ReadOnly),
            _ => None,
        }
    }

    pub fn as_db(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Open => "OPEN",
            Self::Closed => "CLOSED",
            Self::Merged => "MERGED",
            Self::Deleted => "DELETED",
            Self::ReadOnly => "READONLY",
        }
    }
}

/// `ChangesetUiPublicationState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetUiPublicationState {
    Unpublished,
    Draft,
    Published,
}

impl ChangesetUiPublicationState {
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "UNPUBLISHED" => Some(Self::Unpublished),
            "DRAFT" => Some(Self::Draft),
            "PUBLISHED" => Some(Self::Published),
            _ => None,
        }
    }

    pub fn as_db(self) -> &'static str {
        match self {
            Self::Unpublished => "UNPUBLISHED",
            Self::Draft => "DRAFT",
            Self::Published => "PUBLISHED",
        }
    }
}

/// `batches.PublishedValue`: `true`, `false`, `"draft"`, or null.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PublishedValue {
    True,
    False,
    Draft,
    #[default]
    Nil,
}

impl PublishedValue {
    /// Parse `changeset_specs.published` (`text`).
    pub fn from_db(raw: Option<&str>) -> Self {
        match raw {
            Some("true") => Self::True,
            Some("false") => Self::False,
            Some("\"draft\"") | Some("draft") => Self::Draft,
            _ => Self::Nil,
        }
    }

    pub fn is_true(self) -> bool {
        self == Self::True
    }

    pub fn is_false(self) -> bool {
        self == Self::False
    }

    pub fn is_draft(self) -> bool {
        self == Self::Draft
    }

    pub fn is_nil(self) -> bool {
        self == Self::Nil
    }
}

/// `BatchChangeAssoc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchChangeAssoc {
    pub batch_change_id: i64,
    pub detach: bool,
    pub archive: bool,
    pub is_archived: bool,
}

/// `btypes.Changeset`, reduced to the fields the reconciler reads/writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changeset {
    pub id: i64,
    pub repo_id: i32,
    pub batch_changes: Vec<BatchChangeAssoc>,
    pub external_id: String,
    pub external_service_type: String,
    pub external_branch: String,
    pub external_state: Option<ChangesetExternalState>,
    pub owned_by_batch_change_id: i64,
    pub current_spec_id: i64,
    pub previous_spec_id: i64,
    pub publication_state: ChangesetPublicationState,
    pub ui_publication_state: Option<ChangesetUiPublicationState>,
    pub closing: bool,
    pub detached_at: Option<OffsetDateTime>,
    pub failure_message: Option<String>,
    pub previous_failure_message: Option<String>,
}

impl Changeset {
    pub fn published(&self) -> bool {
        self.publication_state.published()
    }

    pub fn unpublished(&self) -> bool {
        self.publication_state.unpublished()
    }

    /// `Changeset.SupportsDraft`.
    pub fn supports_draft(&self) -> bool {
        external_service_supports_draft(&self.external_service_type)
    }

    /// `Changeset.AttachedTo`.
    pub fn attached_to(&self, batch_change_id: i64) -> bool {
        self.batch_changes
            .iter()
            .any(|assoc| assoc.batch_change_id == batch_change_id)
    }

    /// `batchChangesColumn`: jsonb object keyed by batch change id.
    pub fn batch_changes_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for assoc in &self.batch_changes {
            let mut entry = serde_json::Map::new();
            if assoc.detach {
                entry.insert("detach".to_owned(), serde_json::Value::Bool(true));
            }
            if assoc.archive {
                entry.insert("archive".to_owned(), serde_json::Value::Bool(true));
            }
            if assoc.is_archived {
                entry.insert("isArchived".to_owned(), serde_json::Value::Bool(true));
            }
            map.insert(
                assoc.batch_change_id.to_string(),
                serde_json::Value::Object(entry),
            );
        }
        serde_json::Value::Object(map)
    }
}

/// `btypes.ChangesetSpec`, reduced to the fields the reconciler reads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangesetSpec {
    pub id: i64,
    pub external_id: String,
    pub base_rev: String,
    pub base_ref: String,
    pub head_ref: String,
    pub title: String,
    pub body: String,
    pub published: PublishedValue,
    pub diff: Vec<u8>,
    pub commit_message: String,
    pub commit_author_name: String,
    pub commit_author_email: String,
}

impl ChangesetSpec {
    pub fn from_row(row: ChangesetSpecRow) -> Self {
        Self {
            id: row.id,
            external_id: row.external_id,
            base_rev: row.base_rev,
            base_ref: row.base_ref,
            head_ref: row.head_ref,
            title: row.title,
            body: row.body,
            published: PublishedValue::from_db(row.published.as_deref()),
            diff: row.diff,
            commit_message: row.commit_message,
            commit_author_name: row.commit_author_name,
            commit_author_email: row.commit_author_email,
        }
    }
}

/// `types.ExternalServiceSupports(..., CodehostCapabilityDraftChangesets)`.
pub fn external_service_supports_draft(external_service_type: &str) -> bool {
    matches!(
        external_service_type,
        "github" | "gitlab" | "azuredevops" | "gerrit"
    )
}

/// `btypes.Repo` (`Repos().Get`) as the reconciler uses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repo {
    pub id: i32,
    pub name: String,
    pub archived: bool,
}

// ---------------------------------------------------------------------------
// Publication state (`publication_state.go`)
// ---------------------------------------------------------------------------

/// `publicationStateCalculator`.
#[derive(Debug, Clone, Copy)]
pub struct PublicationStateCalculator {
    pub spec: PublishedValue,
    pub ui: Option<ChangesetUiPublicationState>,
}

/// `calculatePublicationState`.
pub fn calculate_publication_state(
    spec: PublishedValue,
    ui: Option<ChangesetUiPublicationState>,
) -> PublicationStateCalculator {
    PublicationStateCalculator { spec, ui }
}

impl PublicationStateCalculator {
    pub fn is_published(&self) -> bool {
        self.spec.is_true()
            || (self.spec.is_nil() && self.ui == Some(ChangesetUiPublicationState::Published))
    }

    pub fn is_draft(&self) -> bool {
        self.spec.is_draft()
            || (self.spec.is_nil() && self.ui == Some(ChangesetUiPublicationState::Draft))
    }

    pub fn is_unpublished(&self) -> bool {
        self.spec.is_false()
            || (self.spec.is_nil()
                && (self.ui.is_none() || self.ui == Some(ChangesetUiPublicationState::Unpublished)))
    }
}

// ---------------------------------------------------------------------------
// Plan (`plan.go`)
// ---------------------------------------------------------------------------

/// `btypes.ReconcilerOperation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcilerOperation {
    Push,
    Detach,
    Archive,
    Reattach,
    Import,
    Publish,
    PublishDraft,
    Close,
    Reopen,
    Undraft,
    Update,
    Sleep,
    Sync,
}

impl ReconcilerOperation {
    /// `operationPrecedence`.
    pub fn precedence(self) -> u8 {
        match self {
            Self::Push | Self::Detach | Self::Archive | Self::Reattach => 0,
            Self::Import | Self::Publish | Self::PublishDraft | Self::Close => 1,
            Self::Reopen => 2,
            Self::Undraft => 3,
            Self::Update => 4,
            Self::Sleep => 5,
            Self::Sync => 6,
        }
    }

    /// `strings.ToLower(string(op))`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Detach => "detach",
            Self::Archive => "archive",
            Self::Reattach => "reattach",
            Self::Import => "import",
            Self::Publish => "publish",
            Self::PublishDraft => "publish_draft",
            Self::Close => "close",
            Self::Reopen => "reopen",
            Self::Undraft => "undraft",
            Self::Update => "update",
            Self::Sleep => "sleep",
            Self::Sync => "sync",
        }
    }
}

/// `btypes.ReconcilerOperations`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Operations(pub Vec<ReconcilerOperation>);

impl Operations {
    pub fn is_none(&self) -> bool {
        self.0.is_empty()
    }

    /// `Operations.Equal` (set semantics).
    pub fn equal(&self, other: &Operations) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .all(|op| other.0.iter().any(|candidate| candidate == op))
    }

    /// `Operations.ExecutionOrder`: de-duplicate (first occurrence), then a
    /// stable sort by precedence.
    pub fn execution_order(&self) -> Vec<ReconcilerOperation> {
        let mut unique: Vec<ReconcilerOperation> = Vec::new();
        for op in &self.0 {
            if !unique.contains(op) {
                unique.push(*op);
            }
        }
        unique.sort_by_key(|op| op.precedence());
        unique
    }

    pub fn contains(&self, op: ReconcilerOperation) -> bool {
        self.0.contains(&op)
    }

    /// `Operations.String`.
    pub fn to_go_string(&self) -> String {
        if self.is_none() {
            return "No operations required".to_owned();
        }
        self.execution_order()
            .iter()
            .map(|op| op.as_str())
            .collect::<Vec<_>>()
            .join(" => ")
    }
}

/// `ChangesetSpecDelta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChangesetSpecDelta {
    pub title_changed: bool,
    pub body_changed: bool,
    pub undraft: bool,
    pub base_ref_changed: bool,
    pub diff_changed: bool,
    pub commit_message_changed: bool,
    pub author_name_changed: bool,
    pub author_email_changed: bool,
}

impl ChangesetSpecDelta {
    pub fn need_commit_update(&self) -> bool {
        self.diff_changed
            || self.commit_message_changed
            || self.author_name_changed
            || self.author_email_changed
    }

    pub fn need_code_host_update(&self) -> bool {
        self.title_changed || self.body_changed || self.base_ref_changed
    }

    pub fn attributes_changed(&self) -> bool {
        self.need_commit_update() || self.need_code_host_update()
    }
}

/// `Plan`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub changeset: Changeset,
    pub changeset_spec: Option<ChangesetSpec>,
    pub ops: Operations,
    pub delta: Option<ChangesetSpecDelta>,
}

impl Plan {
    pub fn add_op(&mut self, op: ReconcilerOperation) {
        self.ops.0.push(op);
    }

    pub fn set_op(&mut self, op: ReconcilerOperation) {
        self.ops = Operations(vec![op]);
    }
}

/// `compareChangesetSpecs`.
pub fn compare_changeset_specs(
    previous: Option<&ChangesetSpec>,
    current: &ChangesetSpec,
    ui_publication_state: Option<ChangesetUiPublicationState>,
) -> ChangesetSpecDelta {
    let mut delta = ChangesetSpecDelta::default();
    let Some(previous) = previous else {
        return delta;
    };

    if previous.title != current.title {
        delta.title_changed = true;
    }
    if previous.body != current.body {
        delta.body_changed = true;
    }
    if previous.base_ref != current.base_ref {
        delta.base_ref_changed = true;
    }

    let previous_calc = calculate_publication_state(previous.published, ui_publication_state);
    let current_calc = calculate_publication_state(current.published, ui_publication_state);
    if previous_calc.is_draft() && current_calc.is_published() {
        delta.undraft = true;
    }

    if previous.diff != current.diff {
        delta.diff_changed = true;
    }
    if previous.commit_message != current.commit_message {
        delta.commit_message_changed = true;
    }
    if previous.commit_author_name != current.commit_author_name {
        delta.author_name_changed = true;
    }
    if previous.commit_author_email != current.commit_author_email {
        delta.author_email_changed = true;
    }

    delta
}

/// `reopenAfterDetach`.
pub fn reopen_after_detach(ch: &Changeset) -> bool {
    let closed = matches!(
        ch.external_state,
        Some(ChangesetExternalState::Closed) | Some(ChangesetExternalState::ReadOnly)
    );
    if !closed {
        return false;
    }
    if ch.owned_by_batch_change_id == 0 {
        return false;
    }
    if ch.closing {
        return false;
    }
    ch.attached_to(ch.owned_by_batch_change_id)
}

/// `DeterminePlan`.
pub fn determine_plan(
    previous_spec: Option<&ChangesetSpec>,
    current_spec: Option<&ChangesetSpec>,
    current_changeset: Option<&Changeset>,
    wanted_changeset: &Changeset,
) -> Result<Plan, String> {
    let mut pl = Plan {
        changeset: wanted_changeset.clone(),
        changeset_spec: current_spec.cloned(),
        ops: Operations::default(),
        delta: None,
    };

    let mut want_detach = false;
    let mut want_archive = false;
    let mut is_archived = false;
    let mut is_still_attached = false;
    let mut is_reattach = false;
    let mut want_detach_from_owner_batch_change = false;

    for assoc in &wanted_changeset.batch_changes {
        if assoc.detach {
            want_detach = true;
            if assoc.batch_change_id == wanted_changeset.owned_by_batch_change_id {
                want_detach_from_owner_batch_change = true;
            }
        } else if assoc.archive
            && assoc.batch_change_id == wanted_changeset.owned_by_batch_change_id
            && wanted_changeset.published()
        {
            want_archive = !assoc.is_archived;
            is_archived = assoc.is_archived;
        } else if current_changeset.is_some_and(|current| current.batch_changes.is_empty()) {
            is_reattach = true;
        } else {
            is_still_attached = true;
        }
    }

    if want_detach {
        pl.set_op(ReconcilerOperation::Detach);
    }
    if want_archive {
        pl.set_op(ReconcilerOperation::Archive);
    }

    if wanted_changeset.closing {
        if wanted_changeset.external_state != Some(ChangesetExternalState::ReadOnly) {
            pl.add_op(ReconcilerOperation::Close);
        }
        // Close is final; nothing else may overwrite it.
        return Ok(pl);
    } else if want_detach_from_owner_batch_change || want_archive || is_archived {
        return Ok(pl);
    }

    // No spec: imported changeset, we cannot write anything.
    let Some(current_spec) = current_spec else {
        if wanted_changeset.unpublished() && is_still_attached {
            pl.add_op(ReconcilerOperation::Import);
        } else if is_reattach && !want_detach {
            pl.add_op(ReconcilerOperation::Reattach);
        }
        return Ok(pl);
    };

    if previous_spec.is_some() && is_reattach && !want_detach {
        pl.add_op(ReconcilerOperation::Reattach);
    }

    let delta = compare_changeset_specs(
        previous_spec,
        current_spec,
        wanted_changeset.ui_publication_state,
    );
    pl.delta = Some(delta);

    match wanted_changeset.publication_state {
        ChangesetPublicationState::Unpublished => {
            let calc = calculate_publication_state(
                current_spec.published,
                wanted_changeset.ui_publication_state,
            );
            if calc.is_published() {
                pl.set_op(ReconcilerOperation::Publish);
                pl.add_op(ReconcilerOperation::Push);
            } else if calc.is_draft() && wanted_changeset.supports_draft() {
                pl.set_op(ReconcilerOperation::PublishDraft);
                pl.add_op(ReconcilerOperation::Push);
            }
            // `Published.Nil()` falls through, same as the baseline.
        }
        ChangesetPublicationState::Published => {
            if matches!(
                wanted_changeset.external_state,
                Some(ChangesetExternalState::Merged) | Some(ChangesetExternalState::ReadOnly)
            ) {
                return Ok(pl);
            }

            if reopen_after_detach(wanted_changeset) {
                pl.set_op(ReconcilerOperation::Reopen);
            }

            if external_service_supports_draft(&wanted_changeset.external_service_type) {
                let calc = calculate_publication_state(
                    current_spec.published,
                    wanted_changeset.ui_publication_state,
                );
                if delta.undraft
                    || (calc.is_published()
                        && wanted_changeset.external_state == Some(ChangesetExternalState::Draft))
                {
                    pl.add_op(ReconcilerOperation::Undraft);
                }
            }

            if delta.attributes_changed() {
                if delta.need_commit_update() {
                    pl.add_op(ReconcilerOperation::Push);
                }
                if !delta.need_code_host_update() {
                    pl.add_op(ReconcilerOperation::Sleep);
                    pl.add_op(ReconcilerOperation::Sync);
                } else {
                    pl.add_op(ReconcilerOperation::Update);
                }
            }
        }
    }

    Ok(pl)
}

// ---------------------------------------------------------------------------
// Source seam (`internal/batches/sources` not ported)
// ---------------------------------------------------------------------------

/// The error type returned by a [`ChangesetSource`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError {
    pub message: String,
    pub non_retryable: bool,
    pub not_found: bool,
    /// `errcode.IsArchived`: the remote repository is archived.
    pub archived: bool,
}

impl SourceError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
            not_found: false,
            archived: false,
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
            not_found: false,
            archived: false,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
            not_found: true,
            archived: false,
        }
    }

    pub fn archived(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
            not_found: false,
            archived: true,
        }
    }

    pub fn not_ported() -> Self {
        Self::non_retryable(SOURCE_NOT_PORTED)
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// `sources.Changeset`: the payload handed to source methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceChangeset {
    pub title: String,
    pub body: String,
    pub base_ref: String,
    pub head_ref: String,
    pub remote_repo: Repo,
    pub target_repo: Repo,
    pub changeset: Changeset,
}

/// `protocol.CreateCommitFromPatchResponse`, reduced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PushResult {
    pub changelist_id: Option<String>,
    pub rev: Option<String>,
}

/// `sources.ChangesetSource`. Every method corresponds to one code-host call;
/// the production factory returns [`SourceError::not_ported`].
#[async_trait]
pub trait ChangesetSource: Send + Sync {
    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    /// `CreateChangeset`, `true` when it already existed.
    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError>;

    /// `CreateDraftChangeset`, `true` when it already existed.
    async fn create_draft_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError>;

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    /// Default: not a draft-capable source (`ToDraftChangesetSource` fails).
    fn supports_draft(&self) -> bool {
        false
    }

    /// `sources.Changeset.IsOutdated`.
    async fn is_outdated(&self, _cs: &SourceChangeset) -> Result<bool, SourceError> {
        Ok(false)
    }

    /// `sources.GetRemoteRepo`: fork handling; default is the target repo.
    async fn remote_repo(
        &self,
        target: &Repo,
        _ch: &Changeset,
        _spec: Option<&ChangesetSpec>,
    ) -> Result<Repo, SourceError> {
        Ok(target.clone())
    }

    /// `GitserverPushConfig` + `BuildCommitOpts` + `CreateCommitFromPatch`.
    async fn push_commit(
        &self,
        _target: &Repo,
        _ch: &Changeset,
        _spec: Option<&ChangesetSpec>,
        _remote: &Repo,
    ) -> Result<PushResult, SourceError> {
        Err(SourceError::not_ported())
    }

    /// `runAfterCommit` (GitHub App commit signing is a seam).
    async fn run_after_commit(&self, _resp: &PushResult) -> Result<(), SourceError> {
        Ok(())
    }

    /// `sources.GitHubSource.DuplicateCommit`: re-create the pushed commit via
    /// the GitHub REST API so the App installation signs it, then move the ref.
    ///
    /// The three REST calls (`GetRef` / `CreateCommit` / `UpdateRef`) are not
    /// ported yet; the signing source resolution above is wired, this step is
    /// an explicit seam.
    async fn duplicate_commit(
        &self,
        _remote: &Repo,
        _resp: &PushResult,
    ) -> Result<CommitVerification, SourceError> {
        Err(SourceError::non_retryable(
            "duplicating a commit requires the GitHub REST API, which is not ported",
        ))
    }
}

/// `github.RestCommit`'s verification projection written back to the changeset.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommitVerification {
    pub sha: String,
    pub verified: bool,
}

/// `sources.Sourcer`.
#[async_trait]
pub trait ChangesetSourceFactory: Send + Sync {
    async fn for_changeset(
        &self,
        changeset: &Changeset,
        target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError>;

    /// `executor.runAfterCommit`'s second `ForChangeset`:
    /// `AuthenticationStrategyGitHubApp` + `CommitSigningGitHubAppKind`.
    ///
    /// `original_strategy` is the strategy used for the initial push; when it
    /// was not `GITHUB_APP` the signing source is credentialless. Returns
    /// `None` when no signing App is configured (and verification is not
    /// required). The duplicate commit itself remains the gitserver/REST seam.
    async fn commit_signing_source(
        &self,
        _changeset: &Changeset,
        _target_repo: &Repo,
        _original_strategy: &str,
        _reject_unverified: bool,
    ) -> Result<Option<Arc<dyn ChangesetSource>>, SourceError> {
        Ok(None)
    }

    /// `conf.RejectUnverifiedCommit()`.
    fn reject_unverified_commit(&self) -> bool {
        false
    }
}

/// Production seam: every source call is explicitly not ported.
pub struct NotPortedSourceFactory;

#[async_trait]
impl ChangesetSourceFactory for NotPortedSourceFactory {
    async fn for_changeset(
        &self,
        _changeset: &Changeset,
        _target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError> {
        Err(SourceError::not_ported())
    }
}

/// Resolves the code-host repository object (`repositoryId`, project path,
/// `slug`) that `types.Repo.Metadata` would carry in the baseline.
///
/// The worker's [`Repo`] is reduced to id/name/archived, so production wires a
/// store-backed implementation; the default derives the minimum from the name.
#[async_trait]
pub trait RepoMetadataProvider: Send + Sync {
    async fn metadata(&self, kind: &str, repo: &Repo) -> Result<serde_json::Value, SourceError>;
}

/// Default provider: derive the code-host identity from `repo.name`.
///
/// Every `sg-extsvc` source decodes `repo.metadata` into its own repository
/// type; the reduced reconciler [`Repo`] carries only id/name/archived, so
/// this provider is the fallback when the store has no metadata (see
/// [`PgRepoMetadataProvider`]). Per-kind fields mirror the code-host types:
/// `github.Repository.NameWithOwner`, `gitlab.Project.PathWithNamespace`,
/// `bitbucketserver.Repo.Slug`/`Project.Key`,
/// `azuredevops.Repository.Name`/`Project.Name`,
/// `bitbucketcloud.Repo.Slug`/`FullName`/`Name`; Gerrit and Perforce sources
/// address the repository by name, so an empty object is sufficient.
#[derive(Debug, Default, Clone, Copy)]
pub struct NameRepoMetadataProvider;

#[async_trait]
impl RepoMetadataProvider for NameRepoMetadataProvider {
    async fn metadata(
        &self,
        kind: &str,
        repo: &Repo,
    ) -> Result<serde_json::Value, SourceError> {
        // `external_services.kind` is upper-case in Go; accept any casing for
        // callers that pass the repository's lower-case service type.
        let kind = kind.to_ascii_uppercase();
        Ok(match kind.as_str() {
            "GITHUB" => serde_json::json!({"name_with_owner": repo.name}),
            "GITLAB" => serde_json::json!({"path_with_namespace": repo.name}),
            "BITBUCKETSERVER" => {
                let (project, slug) = repo
                    .name
                    .split_once('/')
                    .map(|(project, slug)| (project.to_owned(), slug.to_owned()))
                    .unwrap_or_else(|| ("".to_owned(), repo.name.clone()));
                serde_json::json!({"slug": slug, "project": {"key": project}})
            }
            "AZUREDEVOPS" => {
                // `organization/project/repository`; the API URL lives in the
                // stored metadata, so the name-derived object is best-effort.
                let parts: Vec<&str> = repo.name.split('/').collect();
                let name = parts.last().copied().unwrap_or_default();
                let project = parts
                    .len()
                    .checked_sub(2)
                    .and_then(|index| parts.get(index))
                    .copied()
                    .unwrap_or_default();
                serde_json::json!({"name": name, "project": {"name": project}})
            }
            "BITBUCKETCLOUD" => {
                let slug = repo.name.rsplit('/').next().unwrap_or(&repo.name);
                serde_json::json!({
                    "slug": slug,
                    "full_name": repo.name,
                    "name": slug,
                })
            }
            // Gerrit's source uses `repo.name` as the project; Perforce's
            // connection is decoded from the external-service config.
            "GERRIT" | "PERFORCE" => serde_json::json!({}),
            other => {
                return Err(SourceError::non_retryable(format!(
                    "building changeset source: unsupported external service kind {other:?}"
                )))
            }
        })
    }
}

/// Production provider: reads `repo.metadata` (the exact JSON the Go sources
/// decode into their repository types) and falls back to
/// [`NameRepoMetadataProvider`] when the row has no metadata.
pub struct PgRepoMetadataProvider {
    pool: PgPool,
}

impl PgRepoMetadataProvider {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepoMetadataProvider for PgRepoMetadataProvider {
    async fn metadata(
        &self,
        kind: &str,
        repo: &Repo,
    ) -> Result<serde_json::Value, SourceError> {
        let stored = sg_db::repos::RepoStore::new(self.pool.clone())
            .get_by_id(repo.id)
            .await
            .map_err(|err| SourceError::other(format!("loading repository metadata: {err}")))?
            .map(|row| row.metadata)
            .filter(|metadata| !metadata.is_null());
        match stored {
            Some(metadata) => Ok(metadata),
            None => NameRepoMetadataProvider.metadata(kind, repo).await,
        }
    }
}

/// Production [`ChangesetSourceFactory`] backed by `sg-extsvc`'s code-host
/// clients.
///
/// All HTTP goes through the injected [`sg_extsvc::HttpDoer`]; no request is
/// made unless a source operation is executed. The metadata cache keeps the
/// code-host metadata across the per-operation factory calls (`load` caches,
/// a later `update`/`close` reuses it).
pub struct ExtsvcSourceFactory {
    kind: String,
    base_url: url::Url,
    auth: extsvc::SourceAuth,
    http: Arc<dyn sg_extsvc::HttpDoer>,
    repo_metadata: Arc<dyn RepoMetadataProvider>,
    metadata_cache: Arc<Mutex<HashMap<i64, serde_json::Value>>>,
    remote_cache: Arc<Mutex<HashMap<i64, extsvc::Repo>>>,
}

impl ExtsvcSourceFactory {
    pub fn new(
        kind: impl Into<String>,
        base_url: url::Url,
        auth: extsvc::SourceAuth,
        http: Arc<dyn sg_extsvc::HttpDoer>,
        repo_metadata: Arc<dyn RepoMetadataProvider>,
    ) -> Self {
        Self {
            kind: kind.into(),
            base_url,
            auth,
            http,
            repo_metadata,
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
            remote_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Convenience constructor with the name-derived metadata provider.
    pub fn with_name_metadata(
        kind: impl Into<String>,
        base_url: url::Url,
        auth: extsvc::SourceAuth,
        http: Arc<dyn sg_extsvc::HttpDoer>,
    ) -> Self {
        Self::new(
            kind,
            base_url,
            auth,
            http,
            Arc::new(NameRepoMetadataProvider),
        )
    }
}

fn ext_repo(kind: &str, repo: &Repo, metadata: serde_json::Value) -> extsvc::Repo {
    extsvc::Repo {
        id: repo.id,
        name: repo.name.clone(),
        metadata,
        external_repo: extsvc::ExternalRepoSpec {
            id: String::new(),
            service_type: extsvc::kind_to_type(kind),
            service_id: String::new(),
        },
        ..Default::default()
    }
}

fn ext_spec(spec: &ChangesetSpec) -> extsvc::ChangesetSpec {
    extsvc::ChangesetSpec {
        diff: spec.diff.clone(),
        base_rev: spec.base_rev.clone(),
        head_ref: spec.head_ref.clone(),
        commit_message: spec.commit_message.clone(),
        commit_author_name: spec.commit_author_name.clone(),
        commit_author_email: spec.commit_author_email.clone(),
        created_at: String::new(),
        fork_namespace: None,
    }
}

fn map_source_error(err: extsvc::SourceError) -> SourceError {
    match err.kind {
        extsvc::SourceErrorKind::NotFound => SourceError::not_found(err.message),
        extsvc::SourceErrorKind::NonRetryable | extsvc::SourceErrorKind::NotMergeable => {
            SourceError::non_retryable(err.message)
        }
        extsvc::SourceErrorKind::Archived => SourceError::archived(err.message),
        extsvc::SourceErrorKind::Other => SourceError::other(err.message),
    }
}

/// Adapts one `sg-extsvc` source to the reconciler's [`ChangesetSource`].
struct ExtsvcReconcilerSource {
    factory: Arc<ExtsvcSourceFactory>,
    source: Arc<dyn extsvc::ChangesetSource>,
}

impl ExtsvcReconcilerSource {
    async fn build_ext_changeset(
        &self,
        cs: &SourceChangeset,
    ) -> Result<extsvc::SourceChangeset, SourceError> {
        let target_metadata = self
            .factory
            .repo_metadata
            .metadata(&self.factory.kind, &cs.target_repo)
            .await?;
        let target_repo = ext_repo(&self.factory.kind, &cs.target_repo, target_metadata);

        let remote_repo = {
            let cache = self
                .factory
                .remote_cache
                .lock()
                .map_err(|_| SourceError::other("source metadata cache poisoned"))?;
            cache.get(&cs.changeset.id).cloned()
        }
        .unwrap_or_else(|| target_repo.clone());

        let metadata = {
            let cache = self
                .factory
                .metadata_cache
                .lock()
                .map_err(|_| SourceError::other("source metadata cache poisoned"))?;
            cache
                .get(&cs.changeset.id)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        };

        Ok(extsvc::SourceChangeset {
            title: cs.title.clone(),
            body: cs.body.clone(),
            head_ref: cs.head_ref.clone(),
            base_ref: cs.base_ref.clone(),
            remote_repo,
            target_repo,
            changeset: extsvc::Changeset {
                id: cs.changeset.id,
                external_id: cs.changeset.external_id.clone(),
                external_branch: cs.changeset.external_branch.clone(),
                external_fork_namespace: String::new(),
                external_fork_name: String::new(),
                metadata,
            },
        })
    }

    fn cache_result(&self, changeset_id: i64, cs: &extsvc::SourceChangeset) {
        if let Ok(mut cache) = self.factory.metadata_cache.lock() {
            cache.insert(changeset_id, cs.changeset.metadata.clone());
        }
    }

    async fn mutate(
        &self,
        cs: &mut SourceChangeset,
        operation: SourceMutation,
    ) -> Result<(), SourceError> {
        let mut ext = self.build_ext_changeset(cs).await?;
        let result = match operation {
            SourceMutation::Update => self.source.update_changeset(&mut ext).await,
            SourceMutation::Reopen => self.source.reopen_changeset(&mut ext).await,
            SourceMutation::Close => self.source.close_changeset(&mut ext).await,
            SourceMutation::Undraft => self.source.undraft_changeset(&mut ext).await,
        };
        result.map_err(map_source_error)?;
        cs.changeset.external_id = ext.changeset.external_id.clone();
        self.cache_result(cs.changeset.id, &ext);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum SourceMutation {
    Update,
    Reopen,
    Close,
    Undraft,
}

#[async_trait]
impl ChangesetSource for ExtsvcReconcilerSource {
    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let mut ext = self.build_ext_changeset(cs).await?;
        self.source
            .load_changeset(&mut ext)
            .await
            .map_err(map_source_error)?;
        cs.changeset.external_id = ext.changeset.external_id.clone();
        self.cache_result(cs.changeset.id, &ext);
        Ok(())
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        let mut ext = self.build_ext_changeset(cs).await?;
        let existed = self
            .source
            .create_changeset(&mut ext)
            .await
            .map_err(map_source_error)?;
        cs.changeset.external_id = ext.changeset.external_id.clone();
        self.cache_result(cs.changeset.id, &ext);
        Ok(existed)
    }

    async fn create_draft_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let mut ext = self.build_ext_changeset(cs).await?;
        let existed = self
            .source
            .create_draft_changeset(&mut ext)
            .await
            .map_err(map_source_error)?;
        cs.changeset.external_id = ext.changeset.external_id.clone();
        self.cache_result(cs.changeset.id, &ext);
        Ok(existed)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.mutate(cs, SourceMutation::Update).await
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.mutate(cs, SourceMutation::Reopen).await
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.mutate(cs, SourceMutation::Close).await
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.mutate(cs, SourceMutation::Undraft).await
    }

    fn supports_draft(&self) -> bool {
        self.source.supports_draft()
    }

    async fn is_outdated(&self, cs: &SourceChangeset) -> Result<bool, SourceError> {
        let metadata = {
            let cache = self
                .factory
                .metadata_cache
                .lock()
                .map_err(|_| SourceError::other("source metadata cache poisoned"))?;
            cache.get(&cs.changeset.id).cloned()
        };
        let Some(metadata) = metadata else {
            // Nothing loaded yet: the baseline's `Changeset.Metadata` would be
            // decoded from the stored row; without it the changeset is treated
            // as up-to-date, matching the old shell behaviour.
            return Ok(false);
        };
        let current: extsvc::ChangesetMetadata = serde_json::from_value(metadata)
            .map_err(|err| SourceError::other(format!("decoding changeset metadata: {err}")))?;
        let ext = self.build_ext_changeset(cs).await?;
        Ok(self.source.is_outdated(&current, &ext))
    }

    async fn remote_repo(
        &self,
        target: &Repo,
        ch: &Changeset,
        spec: Option<&ChangesetSpec>,
    ) -> Result<Repo, SourceError> {
        let target_metadata = self
            .factory
            .repo_metadata
            .metadata(&self.factory.kind, target)
            .await?;
        let target_repo = ext_repo(&self.factory.kind, target, target_metadata);
        let ext_changeset = extsvc::Changeset {
            id: ch.id,
            external_id: ch.external_id.clone(),
            external_branch: ch.external_branch.clone(),
            external_fork_namespace: String::new(),
            external_fork_name: String::new(),
            metadata: serde_json::Value::Null,
        };
        let spec = spec.map(ext_spec);
        let remote = extsvc::get_remote_repo(
            self.source.as_ref(),
            &target_repo,
            &ext_changeset,
            spec.as_ref(),
        )
        .await
        .map_err(map_source_error)?;
        if let Ok(mut cache) = self.factory.remote_cache.lock() {
            cache.insert(ch.id, remote.clone());
        }
        Ok(Repo {
            id: remote.id,
            name: remote.name,
            archived: target.archived,
        })
    }

    async fn push_commit(
        &self,
        _target: &Repo,
        _ch: &Changeset,
        _spec: Option<&ChangesetSpec>,
        _remote: &Repo,
    ) -> Result<PushResult, SourceError> {
        // `GitserverPushConfig`/`BuildCommitOpts` are ported in `sg-extsvc`,
        // but `CreateCommitFromPatch` needs gitserver, which is not part of
        // this slice.
        Err(SourceError::not_ported().with_message(
            "pushing commits requires gitserver, which is not ported",
        ))
    }
}

impl SourceError {
    fn with_message(self, message: &str) -> Self {
        Self {
            message: message.to_owned(),
            ..self
        }
    }
}

#[async_trait]
impl ChangesetSourceFactory for ExtsvcSourceFactory {
    async fn for_changeset(
        &self,
        _changeset: &Changeset,
        _target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError> {
        let source = extsvc::new_source(
            &self.kind,
            &self.base_url,
            self.auth.clone(),
            self.http.clone(),
        )
        .map_err(map_source_error)?;
        // The adapter outlives this call; it shares the factory's caches via
        // an `Arc`-like handle. `ExtsvcSourceFactory` itself is cheap to clone
        // only through `Arc`, so keep the per-call state self-contained by
        // moving the shared caches into the adapter.
        Ok(Arc::new(ExtsvcReconcilerSource {
            factory: Arc::new(ExtsvcSourceFactory {
                kind: self.kind.clone(),
                base_url: self.base_url.clone(),
                auth: self.auth.clone(),
                http: self.http.clone(),
                repo_metadata: self.repo_metadata.clone(),
                metadata_cache: Arc::clone(&self.metadata_cache),
                remote_cache: Arc::clone(&self.remote_cache),
            }),
            source,
        }))
    }
}

// ---------------------------------------------------------------------------
// External-service configuration provider
// ---------------------------------------------------------------------------

/// Resolves the plaintext code-host connection for a repository.
///
/// Production wires [`PgSourceConfigProvider`]; tests and the differential
/// harness inject [`StaticSourceConfigProvider`]. This mirrors
/// `sources.loadExternalService` + the per-kind constructors without doing any
/// request.
#[async_trait]
pub trait SourceConfigProvider: Send + Sync {
    async fn config_for(
        &self,
        external_service_type: &str,
        repo_name: &str,
    ) -> Result<extsvc::SourceConnection, SourceError>;
}

/// Selects the first (highest-id) service matching the repository kind + host,
/// like `loadExternalService`'s `List` order (`internal/batches/sources/
/// sources.go:500-533`).
fn select_connection<'a>(
    services: &'a [PgSourceService],
    external_service_type: &str,
    repo_name: &str,
) -> Option<&'a Result<extsvc::SourceConnection, SourceError>> {
    let kind = external_service_type.to_ascii_uppercase();
    let host = repo_name
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    services
        .iter()
        .find(|service| {
            service.kind.eq_ignore_ascii_case(&kind)
                && service.host.eq_ignore_ascii_case(&host)
        })
        .map(|service| &service.connection)
}

/// In-memory provider for tests.
#[derive(Debug, Default, Clone)]
pub struct StaticSourceConfigProvider {
    services: Vec<PgSourceService>,
}

impl StaticSourceConfigProvider {
    /// Build from already-resolved connections.
    pub fn new(connections: Vec<extsvc::SourceConnection>) -> Self {
        let services = connections
            .into_iter()
            .enumerate()
            .map(|(index, connection)| PgSourceService {
                id: index as i64,
                kind: connection.kind.clone(),
                host: connection
                    .base_url
                    .host_str()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                connection: Ok(connection),
            })
            .collect();
        Self { services }
    }
}

#[async_trait]
impl SourceConfigProvider for StaticSourceConfigProvider {
    async fn config_for(
        &self,
        external_service_type: &str,
        repo_name: &str,
    ) -> Result<extsvc::SourceConnection, SourceError> {
        match select_connection(&self.services, external_service_type, repo_name) {
            Some(Ok(connection)) => Ok(connection.clone()),
            Some(Err(err)) => Err(err.clone()),
            None => Err(SourceError::other("no external services found")),
        }
    }
}

/// A decrypted external service ready to become a source. The connection is a
/// `Result` so one unsupported config (e.g. a GitHub App connection) does not
/// poison every repository of that kind.
#[derive(Debug, Clone)]
pub struct PgSourceService {
    pub id: i64,
    pub kind: String,
    pub host: String,
    pub connection: Result<extsvc::SourceConnection, SourceError>,
}

/// `github_apps.Store` adapter for the runtime GitHub App authenticator.
///
/// Reads the existing `github_apps`/`github_app_installs` rows and decrypts
/// the App private key with the site `GitHubAppKey`, like
/// `gitHubAppsStore.decrypt`. No table/column/key is added.
pub struct PgBatchesGitHubAppStore {
    pool: PgPool,
    keyring: Arc<sg_keyring::Keyring>,
}

impl PgBatchesGitHubAppStore {
    pub fn new(pool: PgPool, keyring: Arc<sg_keyring::Keyring>) -> Self {
        Self { pool, keyring }
    }
}

#[async_trait]
impl extsvc::BatchesGitHubAppStore for PgBatchesGitHubAppStore {
    async fn get_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<extsvc::BatchesGitHubApp, String> {
        let app = sg_db::github_apps::get_by_domain_and_kind(&self.pool, domain, kind, base_url)
            .await?
            .ok_or_else(|| "no app exists matching criteria".to_owned())?;
        let private_key = sg_encryption::maybe_decrypt(
            self.keyring.github_app_key().as_deref(),
            &app.private_key,
            &app.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok(extsvc::BatchesGitHubApp {
            app_id: app.app_id,
            private_key,
            base_url: app.base_url,
        })
    }

    async fn get_install_id(&self, app_id: i32, account: &str) -> Result<i32, String> {
        sg_db::github_apps::get_install_id(&self.pool, app_id, account)
            .await?
            .ok_or_else(|| "no installation found".to_owned())
    }
}

/// Production provider: decrypts live `external_services` configs with the
/// external-service key and caches the resolved connections. The cache is a
/// `OnceCell` so a failed load is retried (like `internal/object`'s
/// `lazyStore.initOnce`).
pub struct PgSourceConfigProvider {
    pool: PgPool,
    keyring: Arc<sg_keyring::Keyring>,
    cache: tokio::sync::OnceCell<Vec<PgSourceService>>,
    /// `github_apps`/`github_app_installs` store for `GITHUB_APP` runtime
    /// selection; `None` keeps the config/PAT connection.
    github_app_store: Option<Arc<dyn extsvc::BatchesGitHubAppStore>>,
    /// Installation-token cache + HTTP doer.
    github_app_seams: extsvc::GitHubAppTokenSeams,
    /// `conf.RejectUnverifiedCommit()`.
    reject_unverified_commit: bool,
}

impl PgSourceConfigProvider {
    pub fn new(
        pool: PgPool,
        keyring: Arc<sg_keyring::Keyring>,
        http: Arc<dyn sg_extsvc::HttpDoer>,
    ) -> Self {
        Self {
            pool,
            keyring,
            cache: tokio::sync::OnceCell::new(),
            github_app_store: None,
            github_app_seams: extsvc::GitHubAppTokenSeams::offline(http),
            reject_unverified_commit: false,
        }
    }

    /// Enable `GITHUB_APP` runtime authentication (source construction and
    /// commit signing) through the existing App tables.
    pub fn with_github_app_store(
        mut self,
        store: Arc<dyn extsvc::BatchesGitHubAppStore>,
    ) -> Self {
        self.github_app_store = Some(store);
        self
    }

    /// `conf.RejectUnverifiedCommit()`.
    pub fn with_reject_unverified_commit(mut self, reject: bool) -> Self {
        self.reject_unverified_commit = reject;
        self
    }

    async fn load(&self) -> Result<Vec<PgSourceService>, SourceError> {
        let store = sg_db::external_services::ExternalServiceStore::new(self.pool.clone());
        let services = store
            .list()
            .await
            .map_err(|err| SourceError::other(format!("loading external services: {err}")))?;

        let mut resolved = Vec::new();
        for service in services {
            if extsvc::SourceKind::from_external_service_kind(&service.kind).is_none() {
                // `loadExternalService` skips configs it cannot decode into one
                // of the batches connection schemas.
                continue;
            }
            let raw = sg_encryption::maybe_decrypt(
                self.keyring.external_service_key().as_deref(),
                &service.config,
                &service.encryption_key_id,
            )
            .await
            .map_err(|err| {
                SourceError::other(format!(
                    "external service id={} config error: {err}",
                    service.id
                ))
            })?;
            let plain = sg_config::strip_jsonc(&raw);
            let value: serde_json::Value = serde_json::from_str(&plain).map_err(|err| {
                SourceError::other(format!(
                    "external service id={} config error: {err}",
                    service.id
                ))
            })?;
            let host = value
                .get("url")
                .and_then(serde_json::Value::as_str)
                .and_then(|raw| url::Url::parse(raw).ok())
                .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
                .unwrap_or_default();
            let connection = extsvc::resolve_runtime_changeset_source_connection(
                extsvc::RuntimeSourceRequest {
                    external_service_kind: &service.kind,
                    config: &value,
                    strategy: extsvc::AUTHENTICATION_STRATEGY_USER_CREDENTIAL,
                    github_app_kind: None,
                    account: "",
                    as_non_credential: false,
                },
                self.github_app_store.as_deref(),
                self.github_app_seams.clone(),
            )
            .await
            .map_err(map_source_error);
            resolved.push(PgSourceService {
                id: service.id,
                kind: service.kind,
                host,
                connection,
            });
        }
        Ok(resolved)
    }
}

#[async_trait]
impl SourceConfigProvider for PgSourceConfigProvider {
    async fn config_for(
        &self,
        external_service_type: &str,
        repo_name: &str,
    ) -> Result<extsvc::SourceConnection, SourceError> {
        let services = self
            .cache
            .get_or_try_init(|| async { self.load().await })
            .await?;
        match select_connection(services, external_service_type, repo_name) {
            Some(Ok(connection)) => Ok(connection.clone()),
            Some(Err(err)) => Err(err.clone()),
            None => Err(SourceError::other("no external services found")),
        }
    }
}

/// Production [`sg_extsvc::HttpDoer`] backed by `reqwest`.
///
/// Tests inject a recording fake instead, so the differential never contacts a
/// code host.
#[derive(Clone)]
pub struct ReqwestHttpDoer {
    client: reqwest::Client,
}

impl ReqwestHttpDoer {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestHttpDoer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl sg_extsvc::HttpDoer for ReqwestHttpDoer {
    async fn do_request(
        &self,
        request: sg_extsvc::HttpRequest,
    ) -> Result<sg_extsvc::HttpResponse, String> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|err| format!("invalid HTTP method: {err}"))?;
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body);
        }
        let response = builder
            .send()
            .await
            .map_err(|err| format!("request failed: {err}"))?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|err| format!("reading response: {err}"))?
            .to_vec();
        Ok(sg_extsvc::HttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// Production [`ChangesetSourceFactory`]: resolves the repository's external
/// service config and builds one `sg-extsvc` source, matching
/// `sourcer.ForChangeset` → `loadBatchesSource` → `buildChangesetSource`.
pub struct ConfigSourceFactory {
    configs: Arc<dyn SourceConfigProvider>,
    http: Arc<dyn sg_extsvc::HttpDoer>,
    repo_metadata: Arc<dyn RepoMetadataProvider>,
    /// `github_apps`/`github_app_installs` store for `GITHUB_APP` commit
    /// signing; `None` keeps the signing resolution a no-op.
    github_app_store: Option<Arc<dyn extsvc::BatchesGitHubAppStore>>,
    /// Installation-token cache + HTTP doer.
    github_app_seams: extsvc::GitHubAppTokenSeams,
    /// `conf.RejectUnverifiedCommit()`.
    reject_unverified_commit: bool,
}

impl ConfigSourceFactory {
    pub fn new(
        configs: Arc<dyn SourceConfigProvider>,
        http: Arc<dyn sg_extsvc::HttpDoer>,
        repo_metadata: Arc<dyn RepoMetadataProvider>,
    ) -> Self {
        Self {
            configs,
            http: Arc::clone(&http),
            repo_metadata,
            github_app_store: None,
            github_app_seams: extsvc::GitHubAppTokenSeams::offline(http),
            reject_unverified_commit: false,
        }
    }

    /// Enable GitHub App commit signing through the existing App tables.
    pub fn with_github_app_store(
        mut self,
        store: Arc<dyn extsvc::BatchesGitHubAppStore>,
    ) -> Self {
        self.github_app_store = Some(store);
        self
    }

    /// Inject the installation-token cache (existing Redis-backed cache).
    pub fn with_github_app_cache(
        mut self,
        cache: Arc<dyn sg_extsvc::auth::InstallationTokenCache>,
    ) -> Self {
        self.github_app_seams.cache = Some(cache);
        self
    }

    /// `conf.RejectUnverifiedCommit()`.
    pub fn with_reject_unverified_commit(mut self, reject: bool) -> Self {
        self.reject_unverified_commit = reject;
        self
    }
}

#[async_trait]
impl ChangesetSourceFactory for ConfigSourceFactory {
    async fn for_changeset(
        &self,
        changeset: &Changeset,
        target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError> {
        let connection = self
            .configs
            .config_for(&changeset.external_service_type, &target_repo.name)
            .await
            .map_err(|err| SourceError {
                message: format!("loading external service: {}", err.message),
                ..err
            })?;
        let factory = ExtsvcSourceFactory::new(
            connection.kind,
            connection.base_url,
            connection.auth,
            Arc::clone(&self.http),
            Arc::clone(&self.repo_metadata),
        );
        factory.for_changeset(changeset, target_repo).await
    }

    async fn commit_signing_source(
        &self,
        changeset: &Changeset,
        target_repo: &Repo,
        original_strategy: &str,
        reject_unverified: bool,
    ) -> Result<Option<Arc<dyn ChangesetSource>>, SourceError> {
        // `runAfterCommit` only runs for `*sources.GitHubSource`.
        if !changeset.external_service_type.eq_ignore_ascii_case("GITHUB") {
            return Ok(None);
        }
        let connection = self
            .configs
            .config_for(&changeset.external_service_type, &target_repo.name)
            .await
            .map_err(|err| SourceError {
                message: format!("loading external service: {}", err.message),
                ..err
            })?;

        // The App path only reads the connection URL; the authenticator is
        // replaced by the installation token.
        let config = serde_json::json!({ "url": connection.base_url.as_str() });
        let target_metadata = self
            .repo_metadata
            .metadata(&connection.kind, target_repo)
            .await?;
        let target_ext = ext_repo(&connection.kind, target_repo, target_metadata);
        // The worker's reduced spec has no fork namespace, so the owner comes
        // from the repo metadata (the non-fork branch of
        // `getOwnerFromChangeset`). Fork owner derivation is an explicit gap.
        let spec = extsvc::ChangesetSpec {
            fork_namespace: None,
            ..Default::default()
        };
        let account = extsvc::changeset_owner_from_changeset(&target_ext, Some(&spec))
            .map_err(map_source_error)?;

        let resolution = extsvc::resolve_commit_signing_source(
            extsvc::CommitSigningRequest {
                external_service_kind: &connection.kind,
                config: &config,
                account: &account,
                original_strategy,
                reject_unverified,
            },
            self.github_app_store.as_deref(),
            self.github_app_seams.clone(),
        )
        .await
        .map_err(map_source_error)?;

        match resolution {
            extsvc::CommitSigningResolution::UseSource(signing) => {
                let factory = ExtsvcSourceFactory::new(
                    signing.kind,
                    signing.base_url,
                    signing.auth,
                    Arc::clone(&self.http),
                    Arc::clone(&self.repo_metadata),
                );
                Ok(Some(factory.for_changeset(changeset, target_repo).await?))
            }
            extsvc::CommitSigningResolution::SkipNoApp
            | extsvc::CommitSigningResolution::SkipError(_) => Ok(None),
        }
    }

    fn reject_unverified_commit(&self) -> bool {
        self.reject_unverified_commit
    }
}

// ---------------------------------------------------------------------------
// Store seam
// ---------------------------------------------------------------------------

/// The store operations the reconciler performs inside its transaction.
#[async_trait]
pub trait ReconcilerStore: Send {
    /// `tx.Done(err)`: commit when `rollback` is false, roll back otherwise.
    async fn done(&mut self, rollback: bool) -> Result<(), String>;
    async fn get_changeset_spec(&mut self, id: i64) -> Result<Option<ChangesetSpec>, String>;
    async fn get_repo(&mut self, id: i32) -> Result<Repo, String>;
    async fn get_changeset_by_external_branch(
        &mut self,
        external_service_type: &str,
        repo_id: i32,
        external_branch: &str,
    ) -> Result<Option<i64>, String>;
    /// The `[link](url)` markdown for the owning batch change.
    async fn batch_change_link(&mut self, batch_change_id: i64) -> Result<String, String>;
    async fn mark_repo_archived(&mut self, repo_id: i32) -> Result<(), String>;
    /// `UpsertChangesetEvents`; events are not derivable without code-host
    /// metadata (seam, overview §4).
    async fn upsert_changeset_events(&mut self, changeset: &Changeset) -> Result<(), String>;
    async fn update_changeset(&mut self, changeset: &Changeset) -> Result<(), String>;
    /// `state.SetDerivedState` (`executor.go:148`), run after the plan and
    /// the row update. The handler row carried by
    /// [`ChangesetWorkerStore`] has no code-host metadata or events, so the
    /// production store reloads the persisted row inside the open
    /// transaction. Errors are logged and ignored, exactly like the
    /// baseline's void `SetDerivedState`.
    async fn set_derived_state(&mut self, _changeset_id: i64) -> Result<(), String> {
        Ok(())
    }
}

/// Opens a [`ReconcilerStore`] transaction per handler invocation.
#[async_trait]
pub trait ReconcilerStoreFactory: Send + Sync {
    async fn open(&self) -> Result<Box<dyn ReconcilerStore>, String>;

    /// The `afterDone` webhook hook, which runs regardless of the transaction
    /// outcome. Outbound webhook enqueueing is not ported here.
    async fn enqueue_webhook(&self, _kind: WebhookKind, _changeset_id: i64) -> Result<(), String> {
        Ok(())
    }
}

/// The webhook type selected by the executor's `afterDone` closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookKind {
    /// `webhooks.ChangesetUpdateError`
    UpdateError,
    /// `webhooks.ChangesetUpdate`
    Update,
    /// `webhooks.ChangesetPublish`
    Publish,
    /// `webhooks.ChangesetClose`
    Close,
}

// ---------------------------------------------------------------------------
// Reconciler error / retry classification
// ---------------------------------------------------------------------------

/// A handler error plus `errcode.IsNonRetryable` classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerError {
    pub message: String,
    pub non_retryable: bool,
}

impl ReconcilerError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }

    fn from_source(prefix: &str, err: SourceError) -> Self {
        Self {
            message: format!("{prefix}: {err}"),
            non_retryable: err.non_retryable,
        }
    }
}

impl std::fmt::Display for ReconcilerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// `errcode.IsNonRetryable` for the reconciler's terminal errors. The worker
/// only receives the failure message, so the baseline's typed errors are
/// recognised by their fixed text (see overview §4.5).
pub fn is_non_retryable(failure_message: &str) -> bool {
    failure_message == PUBLISH_SAME_BRANCH
        || failure_message == NO_SSH_CREDENTIAL
        || failure_message == CANNOT_PUSH_TO_ARCHIVED_REPO
        || failure_message == SOURCE_NOT_PORTED
        || failure_message.starts_with("user does not have a valid credential for repository ")
        || (failure_message.starts_with("cannot use credentials of type ")
            && failure_message.ends_with(" to push commits"))
        || failure_message.contains("patch does not apply")
        || failure_message.contains(SOURCE_NOT_PORTED)
}

/// `decorateChangesetBody`'s `batch_change_link` handling.
pub fn decorate_body(body: &str, link: &str) -> String {
    if body.contains("batch_change_link") {
        render_batch_change_link(body, link)
    } else {
        format!("{body}\n\n{link}")
    }
}

/// Minimal `text/template` delimiters (`${{` `}}`) renderer for the single
/// `batch_change_link` function; unknown expressions are left literal.
fn render_batch_change_link(body: &str, link: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some(start) = rest.find("${{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 3..];
        match after.find("}}") {
            Some(end) => {
                let inner = after[..end].trim();
                if inner == "batch_change_link" {
                    out.push_str(link);
                } else {
                    out.push_str(&rest[start..start + 3 + end + 2]);
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

// ---------------------------------------------------------------------------
// Executor (`executor.go`)
// ---------------------------------------------------------------------------

/// The executor's result.
#[derive(Debug, Clone)]
pub struct ExecOutcome {
    pub after_done: Option<WebhookKind>,
    pub result: Result<(), ReconcilerError>,
    pub changeset: Changeset,
}

struct Executor<'a> {
    store: &'a mut dyn ReconcilerStore,
    factory: &'a dyn ChangesetSourceFactory,
    no_sleep_before_sync: bool,
    ch: Changeset,
    spec: Option<ChangesetSpec>,
    target_repo: Option<Repo>,
    css: Option<Arc<dyn ChangesetSource>>,
    remote: Option<Repo>,
}

impl<'a> Executor<'a> {
    fn new(
        store: &'a mut dyn ReconcilerStore,
        factory: &'a dyn ChangesetSourceFactory,
        no_sleep_before_sync: bool,
        plan: &Plan,
    ) -> Self {
        Self {
            store,
            factory,
            no_sleep_before_sync,
            ch: plan.changeset.clone(),
            spec: plan.changeset_spec.clone(),
            target_repo: None,
            css: None,
            remote: None,
        }
    }

    async fn changeset_source(&mut self) -> Result<Arc<dyn ChangesetSource>, ReconcilerError> {
        if let Some(css) = &self.css {
            return Ok(Arc::clone(css));
        }
        let target = self
            .target_repo
            .clone()
            .ok_or_else(|| ReconcilerError::retryable("failed to load repository"))?;
        let css = self
            .factory
            .for_changeset(&self.ch, &target)
            .await
            .map_err(|err| ReconcilerError {
                message: err.message.clone(),
                non_retryable: err.non_retryable,
            })?;
        self.css = Some(Arc::clone(&css));
        Ok(css)
    }

    async fn remote_repo(&mut self) -> Result<Repo, ReconcilerError> {
        if let Some(remote) = &self.remote {
            return Ok(remote.clone());
        }
        let css = self
            .changeset_source()
            .await
            .map_err(|err| ReconcilerError {
                message: format!("getting changeset source: {err}"),
                non_retryable: err.non_retryable,
            })?;
        let target = self
            .target_repo
            .clone()
            .ok_or_else(|| ReconcilerError::retryable("failed to load repository"))?;
        let remote = css
            .remote_repo(&target, &self.ch, self.spec.as_ref())
            .await
            .map_err(|err| ReconcilerError {
                message: err.message.clone(),
                non_retryable: err.non_retryable,
            })?;
        self.remote = Some(remote.clone());
        Ok(remote)
    }

    async fn decorate_changeset_body(&mut self) -> Result<String, ReconcilerError> {
        let body = self
            .spec
            .as_ref()
            .map(|spec| spec.body.clone())
            .unwrap_or_default();
        let link = self
            .store
            .batch_change_link(self.ch.owned_by_batch_change_id)
            .await
            .map_err(|err| {
                ReconcilerError::retryable(format!(
                    "decorating body for changeset {}: failed to load batch change: {err}",
                    self.ch.id
                ))
            })?;
        Ok(decorate_body(&body, &link))
    }

    async fn run(mut self, plan: &Plan) -> ExecOutcome {
        if plan.ops.is_none() {
            return ExecOutcome {
                after_done: None,
                result: Ok(()),
                changeset: self.ch,
            };
        }

        match self.store.get_repo(self.ch.repo_id).await {
            Ok(repo) => self.target_repo = Some(repo),
            Err(err) => {
                return self.error(
                    None,
                    ReconcilerError::retryable(format!("failed to load repository: {err}")),
                );
            }
        }

        let trigger_update_webhook = plan.ops.contains(ReconcilerOperation::Push)
            && !plan.ops.contains(ReconcilerOperation::Publish)
            && !plan.ops.contains(ReconcilerOperation::Update);

        let mut after_done = None;
        for op in plan.ops.execution_order() {
            let result = match op {
                ReconcilerOperation::Sync => self.sync_changeset().await,
                ReconcilerOperation::Import => self.import_changeset().await,
                ReconcilerOperation::Push => {
                    let (after, result) = self.push_changeset_patch(trigger_update_webhook).await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Publish => {
                    let (after, result) = self.publish_changeset(false).await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::PublishDraft => {
                    let (after, result) = self.publish_changeset(true).await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Reopen => {
                    let (after, result) = self.reopen_changeset().await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Update => {
                    let (after, result) = self.update_changeset().await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Undraft => {
                    let (after, result) = self.undraft_changeset().await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Close => {
                    let (after, result) = self.close_changeset().await;
                    after_done = after;
                    result
                }
                ReconcilerOperation::Sleep => {
                    if !self.no_sleep_before_sync {
                        tokio::time::sleep(SYNC_SLEEP).await;
                    }
                    Ok(())
                }
                ReconcilerOperation::Detach => {
                    self.detach_changeset();
                    Ok(())
                }
                ReconcilerOperation::Archive => {
                    self.archive_changeset();
                    Ok(())
                }
                ReconcilerOperation::Reattach => {
                    self.reattach_changeset();
                    Ok(())
                }
            };

            if let Err(err) = result {
                return self.error(after_done, err);
            }
        }

        if let Err(err) = self.store.upsert_changeset_events(&self.ch).await {
            return self.error(after_done, ReconcilerError::retryable(err));
        }
        self.ch.previous_failure_message = None;
        if let Err(err) = self.store.update_changeset(&self.ch).await {
            return self.error(after_done, ReconcilerError::retryable(err));
        }
        ExecOutcome {
            after_done,
            result: Ok(()),
            changeset: self.ch,
        }
    }

    fn error(self, after_done: Option<WebhookKind>, err: ReconcilerError) -> ExecOutcome {
        ExecOutcome {
            after_done,
            result: Err(err),
            changeset: self.ch,
        }
    }

    async fn sync_changeset(&mut self) -> Result<(), ReconcilerError> {
        match self.load_changeset().await {
            Ok(()) => Ok(()),
            Err(err) if err.message == "not found" => {
                self.ch.external_state = Some(ChangesetExternalState::Deleted);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn import_changeset(&mut self) -> Result<(), ReconcilerError> {
        self.load_changeset().await?;
        self.ch.publication_state = ChangesetPublicationState::Published;
        Ok(())
    }

    async fn load_changeset(&mut self) -> Result<(), ReconcilerError> {
        let css = self.changeset_source().await?;
        let remote = self.remote_repo().await?;
        let target = self.target_repo.clone().expect("target repo is loaded");
        let mut cs = SourceChangeset {
            title: String::new(),
            body: String::new(),
            base_ref: String::new(),
            head_ref: String::new(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };
        match css.load_changeset(&mut cs).await {
            Ok(()) => {
                self.ch = cs.changeset;
                Ok(())
            }
            Err(err) if err.not_found => Err(ReconcilerError::retryable("not found")),
            Err(err) => Err(ReconcilerError::from_source("loading changeset", err)),
        }
    }

    async fn push_changeset_patch(
        &mut self,
        trigger_update_webhook: bool,
    ) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        let mut after_done = trigger_update_webhook.then_some(WebhookKind::UpdateError);

        let spec = match self.spec.as_ref() {
            Some(spec) => spec.clone(),
            None => {
                return (
                    after_done,
                    Err(ReconcilerError::retryable("changeset has no current spec")),
                )
            }
        };
        match self
            .store
            .get_changeset_by_external_branch(
                &self.ch.external_service_type,
                self.ch.repo_id,
                &spec.head_ref,
            )
            .await
        {
            Err(err) => return (after_done, Err(ReconcilerError::retryable(err))),
            Ok(Some(existing_id)) if existing_id != self.ch.id => {
                return (
                    after_done,
                    Err(ReconcilerError::non_retryable(PUBLISH_SAME_BRANCH)),
                );
            }
            Ok(_) => {}
        }

        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (after_done, Err(err)),
        };
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            Err(err) => return (after_done, Err(err)),
        };
        if remote.archived {
            return (
                after_done,
                Err(ReconcilerError::non_retryable(CANNOT_PUSH_TO_ARCHIVED_REPO)),
            );
        }

        let target = self.target_repo.clone().expect("target repo is loaded");
        match css
            .push_commit(&target, &self.ch, self.spec.as_ref(), &remote)
            .await
        {
            Ok(resp) => {
                if let Some(changelist_id) = resp.changelist_id.clone() {
                    self.ch.external_id = changelist_id;
                }
                // `runAfterCommit`: resolve the `COMMIT_SIGNING` GitHub App
                // source and duplicate the pushed commit through it. The
                // resolution/error/reject semantics are ported; the REST
                // duplicate itself is still a seam.
                let reject_unverified = self.factory.reject_unverified_commit();
                match self
                    .factory
                    .commit_signing_source(
                        &self.ch,
                        &target,
                        extsvc::AUTHENTICATION_STRATEGY_USER_CREDENTIAL,
                        reject_unverified,
                    )
                    .await
                {
                    Ok(Some(signing)) => {
                        if let Err(err) = signing.duplicate_commit(&remote, &resp).await {
                            return (
                                after_done,
                                Err(ReconcilerError::from_source(
                                    "running after commit routine",
                                    err,
                                )),
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        return (
                            after_done,
                            Err(ReconcilerError {
                                message: format!(
                                    "running after commit routine: {}",
                                    err.message
                                ),
                                non_retryable: err.non_retryable,
                            }),
                        )
                    }
                }
                if let Err(err) = css.run_after_commit(&resp).await {
                    return (
                        after_done,
                        Err(ReconcilerError::from_source(
                            "running after commit routine",
                            err,
                        )),
                    );
                }
                if trigger_update_webhook {
                    after_done = Some(WebhookKind::Update);
                }
                (after_done, Ok(()))
            }
            Err(err) if err.archived => {
                // `IsArchivedPushError`: mark the repo archived, then report the
                // terminal "cannot push to an archived repo" error.
                if let Err(err) = self.handle_archived_repo().await {
                    return (
                        after_done,
                        Err(ReconcilerError::retryable(format!(
                            "handling archived repo: {err}"
                        ))),
                    );
                }
                (
                    after_done,
                    Err(ReconcilerError::non_retryable(CANNOT_PUSH_TO_ARCHIVED_REPO)),
                )
            }
            Err(err) if err.non_retryable && err.message.contains("patch does not apply") => (
                // `pushCommitError`: do not wrap, so it is displayed verbatim.
                after_done,
                Err(ReconcilerError::non_retryable(err.message)),
            ),
            Err(err)
                if err
                    .message
                    .starts_with("creating commit from patch for repository ") =>
            {
                // Non-fatal `pushCommitError`: verbatim and retryable.
                (after_done, Err(ReconcilerError::retryable(err.message)))
            }
            Err(err) => (
                after_done,
                Err(ReconcilerError::from_source("pushing commit", err)),
            ),
        }
    }

    async fn publish_changeset(
        &mut self,
        as_draft: bool,
    ) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        // `executor.publishChangeset`: both `afterDoneUpdate` and
        // `afterDonePublish` enqueue `webhooks.ChangesetUpdateError`
        // (executor.go:242-243), so every failure path reports an update
        // error. Only the success path distinguishes publish from update.
        let update_error = Some(WebhookKind::UpdateError);

        let body = match self.decorate_changeset_body().await {
            Ok(body) => body,
            Err(err) => return (update_error, Err(err)),
        };
        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (update_error, Err(err)),
        };
        if as_draft && !css.supports_draft() {
            return (
                update_error,
                Err(ReconcilerError::retryable(NO_DRAFT_SUPPORT)),
            );
        }
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            Err(err) => return (update_error, Err(err)),
        };
        let spec = match self.spec.as_ref() {
            Some(spec) => spec.clone(),
            None => {
                return (
                    update_error,
                    Err(ReconcilerError::retryable("changeset has no current spec")),
                )
            }
        };
        let target = self.target_repo.clone().expect("target repo is loaded");

        let mut cs = SourceChangeset {
            title: spec.title.clone(),
            body,
            base_ref: spec.base_ref.clone(),
            head_ref: spec.head_ref.clone(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };

        let exists = if as_draft {
            match css.create_draft_changeset(&mut cs).await {
                Ok(exists) => exists,
                Err(err) => {
                    return (
                        update_error,
                        Err(ReconcilerError::from_source(
                            "creating draft changeset",
                            err,
                        )),
                    )
                }
            }
        } else {
            match css.create_changeset(&mut cs).await {
                Ok(exists) => exists,
                Err(err) => {
                    return (
                        update_error,
                        Err(ReconcilerError::from_source("creating changeset", err)),
                    )
                }
            }
        };

        let mut outdated = false;
        if exists {
            outdated = match css.is_outdated(&cs).await {
                Ok(value) => value,
                Err(err) => {
                    return (
                        update_error,
                        Err(ReconcilerError::from_source(
                            "could not determine whether changeset needs update",
                            err,
                        )),
                    )
                }
            };
            if outdated {
                if let Err(err) = css.update_changeset(&mut cs).await {
                    return (
                        update_error,
                        Err(ReconcilerError::from_source("updating changeset", err)),
                    );
                }
            }
        }

        self.ch = cs.changeset;
        self.ch.publication_state = ChangesetPublicationState::Published;

        // `exists && outdated` means the changeset already existed on the code
        // host and was updated; every other success publishes it.
        if exists && outdated {
            (Some(WebhookKind::Update), Ok(()))
        } else {
            (Some(WebhookKind::Publish), Ok(()))
        }
    }

    async fn update_changeset(&mut self) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        let after_done = Some(WebhookKind::UpdateError);
        let body = match self.decorate_changeset_body().await {
            Ok(body) => body,
            Err(err) => return (after_done, Err(err)),
        };
        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (after_done, Err(err)),
        };
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            Err(err) => return (after_done, Err(err)),
        };
        let spec = match self.spec.as_ref() {
            Some(spec) => spec.clone(),
            None => {
                return (
                    after_done,
                    Err(ReconcilerError::retryable("changeset has no current spec")),
                )
            }
        };
        let target = self.target_repo.clone().expect("target repo is loaded");

        let mut cs = SourceChangeset {
            title: spec.title.clone(),
            body,
            base_ref: spec.base_ref.clone(),
            head_ref: spec.head_ref.clone(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };

        match css.update_changeset(&mut cs).await {
            Ok(()) => {}
            Err(err) if err.archived => {
                // The source mutated the changeset as far as it got; the
                // archived-repo handler sets `ReadOnly` on the same object in
                // the baseline, so do not overwrite it with the stale clone.
                if let Err(err) = self.handle_archived_repo().await {
                    return (after_done, Err(err));
                }
                return (Some(WebhookKind::Update), Ok(()));
            }
            Err(err) => {
                return (
                    after_done,
                    Err(ReconcilerError::from_source("updating changeset", err)),
                )
            }
        }

        self.ch = cs.changeset;
        (Some(WebhookKind::Update), Ok(()))
    }

    async fn reopen_changeset(&mut self) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        let after_done = Some(WebhookKind::UpdateError);
        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (after_done, Err(err)),
        };
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            Err(err) => return (after_done, Err(err)),
        };
        let spec = match self.spec.as_ref() {
            Some(spec) => spec.clone(),
            None => {
                return (
                    after_done,
                    Err(ReconcilerError::retryable("changeset has no current spec")),
                )
            }
        };
        let target = self.target_repo.clone().expect("target repo is loaded");

        let mut cs = SourceChangeset {
            title: spec.title.clone(),
            body: spec.body.clone(),
            base_ref: spec.base_ref.clone(),
            head_ref: spec.head_ref.clone(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };
        if let Err(err) = css.reopen_changeset(&mut cs).await {
            return (
                after_done,
                Err(ReconcilerError::from_source("reopening changeset", err)),
            );
        }
        self.ch = cs.changeset;
        (Some(WebhookKind::Update), Ok(()))
    }

    async fn close_changeset(&mut self) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        let after_done = Some(WebhookKind::UpdateError);
        self.ch.closing = false;

        if !matches!(
            self.ch.external_state,
            Some(ChangesetExternalState::Draft) | Some(ChangesetExternalState::Open)
        ) {
            // `closeChangeset` returns `(nil, nil)` here: no webhook.
            return (None, Ok(()));
        }

        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (after_done, Err(err)),
        };
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            Err(err) => return (after_done, Err(err)),
        };
        let target = self.target_repo.clone().expect("target repo is loaded");
        let mut cs = SourceChangeset {
            title: String::new(),
            body: String::new(),
            base_ref: String::new(),
            head_ref: String::new(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };
        if let Err(err) = css.close_changeset(&mut cs).await {
            return (
                after_done,
                Err(ReconcilerError::from_source("closing changeset", err)),
            );
        }
        self.ch = cs.changeset;
        (Some(WebhookKind::Close), Ok(()))
    }

    async fn undraft_changeset(&mut self) -> (Option<WebhookKind>, Result<(), ReconcilerError>) {
        let after_done = Some(WebhookKind::UpdateError);
        let css = match self.changeset_source().await {
            Ok(css) => css,
            Err(err) => return (after_done, Err(err)),
        };
        if !css.supports_draft() {
            return (
                after_done,
                Err(ReconcilerError::retryable(NO_DRAFT_SUPPORT)),
            );
        }
        let remote = match self.remote_repo().await {
            Ok(remote) => remote,
            // Baseline bug: this error is swallowed (`return afterDone, nil`).
            Err(_) => return (after_done, Ok(())),
        };
        let spec = match self.spec.as_ref() {
            Some(spec) => spec.clone(),
            None => {
                return (
                    after_done,
                    Err(ReconcilerError::retryable("changeset has no current spec")),
                )
            }
        };
        let target = self.target_repo.clone().expect("target repo is loaded");
        let mut cs = SourceChangeset {
            title: spec.title.clone(),
            body: spec.body.clone(),
            base_ref: spec.base_ref.clone(),
            head_ref: spec.head_ref.clone(),
            remote_repo: remote,
            target_repo: target,
            changeset: self.ch.clone(),
        };
        if let Err(err) = css.undraft_changeset(&mut cs).await {
            return (
                after_done,
                Err(ReconcilerError::from_source("undrafting changeset", err)),
            );
        }
        self.ch = cs.changeset;
        (Some(WebhookKind::Update), Ok(()))
    }

    async fn handle_archived_repo(&mut self) -> Result<(), ReconcilerError> {
        let remote = self.remote_repo().await?;
        self.store
            .mark_repo_archived(remote.id)
            .await
            .map_err(|err| {
                ReconcilerError::retryable(format!(
                    "updating archived status of repo {}: {err}",
                    remote.id
                ))
            })?;
        self.ch.external_state = Some(ChangesetExternalState::ReadOnly);
        Ok(())
    }

    fn detach_changeset(&mut self) {
        self.ch.batch_changes.retain(|assoc| !assoc.detach);
        if self.ch.batch_changes.is_empty() {
            self.ch.detached_at = Some(OffsetDateTime::now_utc());
        }
    }

    fn archive_changeset(&mut self) {
        for assoc in &mut self.ch.batch_changes {
            if assoc.archive {
                assoc.is_archived = true;
                assoc.archive = false;
            }
        }
    }

    fn reattach_changeset(&mut self) {
        if self.ch.detached_at.is_some() {
            self.ch.detached_at = None;
        }
    }
}

/// `executePlan`.
pub async fn execute_plan(
    store: &mut dyn ReconcilerStore,
    factory: &dyn ChangesetSourceFactory,
    no_sleep_before_sync: bool,
    plan: &Plan,
) -> ExecOutcome {
    Executor::new(store, factory, no_sleep_before_sync, plan)
        .run(plan)
        .await
}

// ---------------------------------------------------------------------------
// Handler (`reconciler.go`) + worker store (`worker_reconciler.go`)
// ---------------------------------------------------------------------------

/// A dequeued `changesets` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetRecord(pub Changeset);

impl DbRecord for ChangesetRecord {
    fn record_id(&self) -> i64 {
        self.0.id
    }
}

/// `(*Reconciler).HandlerFunc`.
pub struct ReconcilerHandler {
    store_factory: Arc<dyn ReconcilerStoreFactory>,
    source_factory: Arc<dyn ChangesetSourceFactory>,
    no_sleep_before_sync: bool,
}

impl ReconcilerHandler {
    pub fn new(
        store_factory: Arc<dyn ReconcilerStoreFactory>,
        source_factory: Arc<dyn ChangesetSourceFactory>,
        no_sleep_before_sync: bool,
    ) -> Self {
        Self {
            store_factory,
            source_factory,
            no_sleep_before_sync,
        }
    }

    /// `process`.
    pub async fn process(
        &self,
        store: &mut dyn ReconcilerStore,
        mut ch: Changeset,
    ) -> (Option<WebhookKind>, Result<(), ReconcilerError>, Changeset) {
        // Copy over and reset the previous error message.
        if ch.failure_message.is_some() {
            ch.previous_failure_message = ch.failure_message.take();
            ch.failure_message = None;
        }

        let mut curr = None;
        if ch.current_spec_id != 0 {
            match store.get_changeset_spec(ch.current_spec_id).await {
                Ok(spec) => curr = spec,
                // `loadChangesetSpecs` returns `(nil, nil)` on error.
                Err(_) => return (None, Ok(()), ch),
            }
        }
        let mut prev = None;
        if ch.previous_spec_id != 0 {
            match store.get_changeset_spec(ch.previous_spec_id).await {
                Ok(spec) => prev = spec,
                Err(_) => return (None, Ok(()), ch),
            }
        }

        let plan = match determine_plan(prev.as_ref(), curr.as_ref(), None, &ch) {
            Ok(plan) => plan,
            Err(err) => return (None, Err(ReconcilerError::retryable(err)), ch),
        };

        let outcome = execute_plan(
            store,
            self.source_factory.as_ref(),
            self.no_sleep_before_sync,
            &plan,
        )
        .await;
        // `state.SetDerivedState` runs after the ops and the row update; the
        // store reloads the persisted metadata/events/repo. Not reached when
        // an operation failed, matching `executor.Run`'s early returns.
        if outcome.result.is_ok() {
            if let Err(err) = store.set_derived_state(outcome.changeset.id).await {
                tracing::warn!(
                    changeset_id = outcome.changeset.id,
                    error = %err,
                    "computing derived changeset state"
                );
            }
        }
        (outcome.after_done, outcome.result, outcome.changeset)
    }
}

#[async_trait]
impl DbWorkerHandler<ChangesetWorkerStore> for ReconcilerHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &ChangesetRecord,
    ) -> Result<(), String> {
        let changeset_id = record.0.id;
        let mut store = self.store_factory.open().await?;
        let (after_done, result, _changeset) = self.process(store.as_mut(), record.0.clone()).await;

        // `defer { err = tx.Done(err); afterDone(store) }`.
        let rollback = result.is_err();
        let done = store.done(rollback).await;

        if let Some(kind) = after_done {
            // `afterDone` runs regardless of the transaction outcome.
            let _ = self.store_factory.enqueue_webhook(kind, changeset_id).await;
        }

        match (result, done) {
            (Err(err), _) => Err(err.message),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn non_retryable(&self, failure_message: &str) -> bool {
        is_non_retryable(failure_message)
    }
}

/// The `changesets` dbworker store (`worker_reconciler.go`).
pub struct ChangesetWorkerStore {
    pool: PgPool,
}

impl ChangesetWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbWorkerStore for ChangesetWorkerStore {
    type Record = ChangesetRecord;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        db::dequeue_reconciler_changeset(&self.pool, OffsetDateTime::now_utc(), worker_hostname)
            .await
            .map(|row| row.map(ChangesetRecord::from_row))
            .map_err(|err| err.to_string())
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let (known, canceled) =
            db::heartbeat_reconciler_changesets(&self.pool, OffsetDateTime::now_utc(), ids)
                .await
                .map_err(|err| err.to_string())?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        db::mark_reconciler_complete(&self.pool, record.0.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        db::mark_reconciler_errored(&self.pool, record.0.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        db::mark_reconciler_failed(&self.pool, record.0.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }
}

impl ChangesetRecord {
    fn from_row(row: ReconcilerChangesetRow) -> Self {
        let batch_changes = parse_batch_change_ids(&row.batch_change_ids);
        Self(Changeset {
            id: row.id,
            repo_id: row.repo_id,
            batch_changes,
            external_id: String::new(),
            external_service_type: row.external_service_type,
            external_branch: row.external_branch.unwrap_or_default(),
            external_state: row
                .external_state
                .as_deref()
                .and_then(ChangesetExternalState::from_db),
            owned_by_batch_change_id: row.owned_by_batch_change_id.unwrap_or(0),
            current_spec_id: row.current_spec_id.unwrap_or(0),
            previous_spec_id: row.previous_spec_id.unwrap_or(0),
            publication_state: ChangesetPublicationState::from_db(&row.publication_state),
            ui_publication_state: row
                .ui_publication_state
                .as_deref()
                .and_then(ChangesetUiPublicationState::from_db),
            closing: row.closing,
            detached_at: row.detached_at,
            failure_message: row.failure_message,
            previous_failure_message: row.previous_failure_message,
        })
    }
}

fn parse_batch_change_ids(value: &serde_json::Value) -> Vec<BatchChangeAssoc> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(id, assoc)| {
            let assoc = assoc.as_object()?;
            let flag = |key: &str| {
                assoc
                    .get(key)
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            };
            Some(BatchChangeAssoc {
                batch_change_id: id.parse().ok()?,
                detach: flag("detach"),
                archive: flag("archive"),
                is_archived: flag("isArchived"),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Production store adapter + job
// ---------------------------------------------------------------------------

/// [`sg_extsvc::batches_webhooks::WebhookEnqueueSink`] over the existing
/// `outbound_webhook_jobs` queue (`WebhookJobQueue::create`).
///
/// The sender runs with no key (see the outbound webhook module docs), so the
/// payload is stored as plaintext with an empty `encryption_key_id`, matching
/// Go's `encryption.NewUnencrypted` + nil scope.
pub struct OutboundWebhookQueueSink {
    queue: Arc<crate::jobs::outbound_webhooks::store::WebhookJobQueue>,
}

impl OutboundWebhookQueueSink {
    pub fn new(queue: Arc<crate::jobs::outbound_webhooks::store::WebhookJobQueue>) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl sg_extsvc::batches_webhooks::WebhookEnqueueSink for OutboundWebhookQueueSink {
    async fn enqueue(
        &self,
        event_type: &str,
        scope: Option<&str>,
        payload: &[u8],
    ) -> Result<(), String> {
        self.queue
            .create(event_type, scope, payload)
            .await
            .map(|_| ())
    }
}

/// `(*store.Store)` transaction adapter over a `PgConnection`.
pub struct PgReconcilerStore {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    external_url: String,
}

#[async_trait]
impl ReconcilerStoreFactory for PgReconcilerStoreFactory {
    async fn open(&self) -> Result<Box<dyn ReconcilerStore>, String> {
        let mut conn = self.pool.acquire().await.map_err(|err| err.to_string())?;
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
        Ok(Box::new(PgReconcilerStore {
            conn,
            external_url: self.external_url.clone(),
        }))
    }

    /// `afterDone` (`webhooks.EnqueueChangeset`) for the executor's
    /// publish/close/update/update_error hooks. Fire-and-forget: failures are
    /// logged, never propagated.
    async fn enqueue_webhook(
        &self,
        kind: WebhookKind,
        changeset_id: i64,
    ) -> Result<(), String> {
        let event_type = match kind {
            WebhookKind::UpdateError => sg_extsvc::batches_webhooks::CHANGESET_UPDATE_ERROR,
            WebhookKind::Update => sg_extsvc::batches_webhooks::CHANGESET_UPDATE,
            WebhookKind::Publish => sg_extsvc::batches_webhooks::CHANGESET_PUBLISH,
            WebhookKind::Close => sg_extsvc::batches_webhooks::CHANGESET_CLOSE,
        };
        let id = sg_extsvc::batches_webhooks::changeset_gql_id(changeset_id);
        if let Err(err) = sg_extsvc::batches_webhooks::enqueue_changeset(
            self.webhook_sink.as_ref(),
            self.http.as_ref(),
            &self.internal_api_url,
            event_type,
            &id,
        )
        .await
        {
            tracing::warn!(
                changeset_id,
                event_type,
                error = %err,
                "enqueuing changeset webhook"
            );
        }
        Ok(())
    }
}

/// Opens [`PgReconcilerStore`] transactions.
pub struct PgReconcilerStoreFactory {
    pool: PgPool,
    external_url: String,
    webhook_sink: Arc<dyn sg_extsvc::batches_webhooks::WebhookEnqueueSink>,
    http: Arc<dyn sg_extsvc::HttpDoer>,
    internal_api_url: String,
}

impl PgReconcilerStoreFactory {
    pub fn new(
        pool: PgPool,
        external_url: impl Into<String>,
        webhook_sink: Arc<dyn sg_extsvc::batches_webhooks::WebhookEnqueueSink>,
        http: Arc<dyn sg_extsvc::HttpDoer>,
        internal_api_url: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            external_url: external_url.into(),
            webhook_sink,
            http,
            internal_api_url: internal_api_url.into(),
        }
    }
}

#[async_trait]
impl ReconcilerStore for PgReconcilerStore {
    async fn done(&mut self, rollback: bool) -> Result<(), String> {
        let statement = if rollback { "ROLLBACK" } else { "COMMIT" };
        sqlx::query(statement)
            .execute(&mut *self.conn)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    async fn get_changeset_spec(&mut self, id: i64) -> Result<Option<ChangesetSpec>, String> {
        db::get_changeset_spec(&mut self.conn, id)
            .await
            .map(|row| row.map(ChangesetSpec::from_row))
            .map_err(|err| err.to_string())
    }

    async fn get_repo(&mut self, id: i32) -> Result<Repo, String> {
        db::get_reconciler_repo(&mut self.conn, id)
            .await
            .map_err(|err| err.to_string())?
            .map(Repo::from_row)
            .ok_or_else(|| format!("repository not found: {id}"))
    }

    async fn get_changeset_by_external_branch(
        &mut self,
        external_service_type: &str,
        repo_id: i32,
        external_branch: &str,
    ) -> Result<Option<i64>, String> {
        db::get_changeset_by_external_branch(
            &mut self.conn,
            external_service_type,
            repo_id,
            external_branch,
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn batch_change_link(&mut self, batch_change_id: i64) -> Result<String, String> {
        db::batch_change_link(&mut self.conn, &self.external_url, batch_change_id).await
    }

    async fn mark_repo_archived(&mut self, repo_id: i32) -> Result<(), String> {
        db::mark_reconciler_repo_archived(&mut self.conn, repo_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn upsert_changeset_events(&mut self, _changeset: &Changeset) -> Result<(), String> {
        // Events require code-host metadata types (`Changeset.Events()`); the
        // source seam sets the derived state instead (overview §4.2).
        Ok(())
    }

    async fn update_changeset(&mut self, changeset: &Changeset) -> Result<(), String> {
        let external_state = changeset.external_state.map(|state| state.as_db());
        let ui_state = changeset
            .ui_publication_state
            .map(ChangesetUiPublicationState::as_db);
        let publication_state = match changeset.publication_state {
            ChangesetPublicationState::Unpublished => "UNPUBLISHED",
            ChangesetPublicationState::Published => "PUBLISHED",
        };
        let external_id =
            (!changeset.external_id.is_empty()).then_some(changeset.external_id.as_str());
        let external_branch = (!changeset.external_branch.is_empty())
            .then_some(changeset.external_branch.as_str());
        let update = ChangesetUpdate {
            id: changeset.id,
            batch_change_ids: changeset.batch_changes_json(),
            detached_at: changeset.detached_at,
            closing: changeset.closing,
            publication_state,
            ui_publication_state: ui_state,
            external_id,
            external_branch,
            external_state,
            failure_message: changeset.failure_message.as_deref(),
            previous_failure_message: changeset.previous_failure_message.as_deref(),
        };
        db::update_reconciler_changeset(&mut self.conn, &update)
            .await
            .map_err(|err| err.to_string())
    }

    async fn set_derived_state(&mut self, changeset_id: i64) -> Result<(), String> {
        // Network-independent state is always computed and persisted.
        // Gitserver stays a seam (`None`), so sync state and diff stat keep
        // their persisted values.
        sg_extsvc::batches_state_load::refresh_derived_state(
            &mut *self.conn,
            changeset_id,
            None,
        )
        .await
        .map(|_| ())
    }
}

impl Repo {
    fn from_row(row: ReconcilerRepo) -> Self {
        Self {
            id: row.id,
            name: row.name,
            archived: row.archived,
        }
    }
}

/// `reconcilerJob`.
pub struct BatchesReconcilerJob;

impl BatchesReconcilerJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BatchesReconcilerJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for BatchesReconcilerJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !batches_enabled() {
            return Ok(Vec::new());
        }
        // `FeatureBatchChanges.Check` is a frontend-only gate in Go; the
        // worker's default allows it (see the trait docs).
        if WorkerBatchChangesLicenseGate
            .check_feature_batch_changes()
            .is_err()
        {
            return Ok(Vec::new());
        }

        let external_url = deps
            .live_config
            .current()
            .site
            .raw()
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let internal_api_url = sg_config::normalize_address(
            &std::env::var("SRC_FRONTEND_INTERNAL")
                .unwrap_or_else(|_| sg_config::DEFAULT_FRONTEND_INTERNAL.to_owned()),
        );
        let webhook_queue = Arc::new(
            crate::jobs::outbound_webhooks::store::WebhookJobQueue::new(
                deps.frontend.clone(),
                None,
            ),
        );
        let store_factory = Arc::new(PgReconcilerStoreFactory::new(
            deps.frontend.clone(),
            external_url,
            Arc::new(OutboundWebhookQueueSink::new(webhook_queue)),
            Arc::new(ReqwestHttpDoer::new()),
            internal_api_url,
        ));
        let configs: Arc<dyn SourceConfigProvider> = Arc::new(PgSourceConfigProvider::new(
            deps.frontend.clone(),
            Arc::clone(&deps.keyring),
            Arc::new(ReqwestHttpDoer::new()),
        ));
        let source_factory: Arc<dyn ChangesetSourceFactory> = Arc::new(ConfigSourceFactory::new(
            configs,
            Arc::new(ReqwestHttpDoer::new()),
            Arc::new(PgRepoMetadataProvider::new(deps.frontend.clone())),
        ));
        let handler = Arc::new(ReconcilerHandler::new(
            store_factory,
            source_factory,
            false,
        ));
        let store = Arc::new(ChangesetWorkerStore::new(deps.frontend.clone()));
        let worker = WorkerRoutine::new(
            store,
            handler,
            WorkerOptions {
                name: RECONCILER_WORKER_NAME.to_owned(),
                description: RECONCILER_WORKER_DESCRIPTION.to_owned(),
                interval: WORKER_INTERVAL,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                num_handlers: NUM_HANDLERS,
                worker_hostname: default_hostname(),
            },
        );
        Ok(vec![BackgroundRoutine::DbBacked(
            Arc::new(worker) as Arc<dyn DbBackedRoutine>
        )])
    }
}

// Keep the store option constants referenced so the module documents them.
const _: () = {
    let _ = (
        RECONCILER_STORE_NAME,
        RECONCILER_TABLE,
        RECONCILER_VIEW,
        RECONCILER_STALLED_MAX_AGE_SECONDS,
        RECONCILER_MAX_NUM_RESETS,
        RECONCILER_RETRY_AFTER_SECONDS,
        RECONCILER_MAX_NUM_RETRIES,
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    fn changeset() -> Changeset {
        Changeset {
            id: 1,
            repo_id: 1,
            batch_changes: vec![BatchChangeAssoc {
                batch_change_id: 10,
                detach: false,
                archive: false,
                is_archived: false,
            }],
            external_id: String::new(),
            external_service_type: "github".to_owned(),
            external_branch: "refs/heads/branch".to_owned(),
            external_state: None,
            owned_by_batch_change_id: 10,
            current_spec_id: 100,
            previous_spec_id: 0,
            publication_state: ChangesetPublicationState::Unpublished,
            ui_publication_state: None,
            closing: false,
            detached_at: None,
            failure_message: None,
            previous_failure_message: None,
        }
    }

    fn spec(published: PublishedValue) -> ChangesetSpec {
        ChangesetSpec {
            id: 100,
            external_id: String::new(),
            base_rev: "base".to_owned(),
            base_ref: "main".to_owned(),
            head_ref: "refs/heads/branch".to_owned(),
            title: "title".to_owned(),
            body: "body".to_owned(),
            published,
            diff: b"diff".to_vec(),
            commit_message: "msg".to_owned(),
            commit_author_name: "name".to_owned(),
            commit_author_email: "email".to_owned(),
        }
    }

    #[test]
    fn publication_state_matrix() {
        use ChangesetUiPublicationState as Ui;
        assert!(calculate_publication_state(PublishedValue::True, None).is_published());
        assert!(
            calculate_publication_state(PublishedValue::Nil, Some(Ui::Published)).is_published()
        );
        assert!(calculate_publication_state(PublishedValue::Draft, None).is_draft());
        assert!(calculate_publication_state(PublishedValue::Nil, Some(Ui::Draft)).is_draft());
        assert!(calculate_publication_state(PublishedValue::False, None).is_unpublished());
        assert!(calculate_publication_state(PublishedValue::Nil, None).is_unpublished());
        assert!(
            calculate_publication_state(PublishedValue::Nil, Some(Ui::Unpublished))
                .is_unpublished()
        );
    }

    #[test]
    fn execution_order_dedupes_and_sorts() {
        let ops = Operations(vec![
            ReconcilerOperation::Sync,
            ReconcilerOperation::Push,
            ReconcilerOperation::Push,
            ReconcilerOperation::Publish,
            ReconcilerOperation::Sleep,
        ]);
        assert_eq!(
            ops.execution_order(),
            vec![
                ReconcilerOperation::Push,
                ReconcilerOperation::Publish,
                ReconcilerOperation::Sleep,
                ReconcilerOperation::Sync,
            ]
        );
        assert_eq!(ops.to_go_string(), "push => publish => sleep => sync");
        assert_eq!(
            Operations::default().to_go_string(),
            "No operations required"
        );
    }

    #[test]
    fn plan_publishes_and_pushes_unpublished() {
        let ch = changeset();
        let current = spec(PublishedValue::True);
        let plan = determine_plan(None, Some(&current), None, &ch).unwrap();
        assert_eq!(
            plan.ops.execution_order(),
            vec![ReconcilerOperation::Push, ReconcilerOperation::Publish]
        );
    }

    #[test]
    fn plan_closing_readonly_skips_close() {
        let mut ch = changeset();
        ch.closing = true;
        ch.external_state = Some(ChangesetExternalState::ReadOnly);
        let plan = determine_plan(None, Some(&spec(PublishedValue::True)), None, &ch).unwrap();
        assert!(plan.ops.is_none());
    }

    #[test]
    fn plan_updates_published_with_code_host_delta() {
        let mut ch = changeset();
        ch.publication_state = ChangesetPublicationState::Published;
        ch.external_state = Some(ChangesetExternalState::Open);
        let mut prev = spec(PublishedValue::True);
        prev.title = "old".to_owned();
        let mut curr = spec(PublishedValue::True);
        curr.title = "new".to_owned();
        let plan = determine_plan(Some(&prev), Some(&curr), None, &ch).unwrap();
        assert_eq!(
            plan.ops.execution_order(),
            vec![ReconcilerOperation::Update]
        );
    }

    #[test]
    fn plan_pushes_and_syncs_diff_only_delta() {
        let mut ch = changeset();
        ch.publication_state = ChangesetPublicationState::Published;
        ch.external_state = Some(ChangesetExternalState::Open);
        let mut prev = spec(PublishedValue::True);
        prev.diff = b"old".to_vec();
        let mut curr = spec(PublishedValue::True);
        curr.diff = b"new".to_vec();
        let plan = determine_plan(Some(&prev), Some(&curr), None, &ch).unwrap();
        assert_eq!(
            plan.ops.execution_order(),
            vec![
                ReconcilerOperation::Push,
                ReconcilerOperation::Sleep,
                ReconcilerOperation::Sync
            ]
        );
    }

    #[test]
    fn body_decoration_appends_or_renders_link() {
        assert_eq!(decorate_body("hello", "[link]"), "hello\n\n[link]");
        assert_eq!(
            decorate_body("a ${{ batch_change_link }} b", "[link]"),
            "a [link] b"
        );
        assert_eq!(
            decorate_body("${{     batch_change_link}}", "[link]"),
            "[link]"
        );
    }

    #[test]
    fn non_retryable_classification() {
        assert!(is_non_retryable(PUBLISH_SAME_BRANCH));
        assert!(is_non_retryable(NO_SSH_CREDENTIAL));
        assert!(is_non_retryable("cannot push to an archived repo"));
        assert!(is_non_retryable("pushing commit: patch does not apply"));
        assert!(is_non_retryable(
            "user does not have a valid credential for repository \"github.com/a/b\""
        ));
        assert!(is_non_retryable(
            "cannot use credentials of type oauth to push commits"
        ));
        assert!(!is_non_retryable("loading changeset: boom"));
    }

    #[test]
    fn batch_change_ids_round_trip() {
        let mut ch = changeset();
        ch.batch_changes.push(BatchChangeAssoc {
            batch_change_id: 11,
            detach: true,
            archive: false,
            is_archived: true,
        });
        let json = ch.batch_changes_json();
        let mut parsed = parse_batch_change_ids(&json);
        parsed.sort_by_key(|assoc| assoc.batch_change_id);
        assert_eq!(parsed.len(), 2);
        assert!(!parsed[0].detach);
        assert!(parsed[1].detach && parsed[1].is_archived);
    }

    #[tokio::test]
    async fn name_metadata_covers_every_production_source_kind() {
        let repo = Repo {
            id: 7,
            name: "org/project/repository".to_owned(),
            archived: false,
        };
        for (kind, key) in [
            ("GITHUB", "name_with_owner"),
            ("GITLAB", "path_with_namespace"),
            ("BITBUCKETSERVER", "slug"),
            ("AZUREDEVOPS", "name"),
            ("BITBUCKETCLOUD", "slug"),
        ] {
            let metadata = NameRepoMetadataProvider
                .metadata(kind, &repo)
                .await
                .unwrap();
            assert!(
                metadata.get(key).is_some(),
                "{kind} metadata lacks {key}: {metadata}"
            );
        }
        for kind in ["GERRIT", "PERFORCE"] {
            assert_eq!(
                NameRepoMetadataProvider
                    .metadata(kind, &repo)
                    .await
                    .unwrap(),
                serde_json::json!({})
            );
        }
        // Lower-case service types are accepted too.
        assert_eq!(
            NameRepoMetadataProvider
                .metadata("github", &repo)
                .await
                .unwrap(),
            serde_json::json!({"name_with_owner": "org/project/repository"})
        );
    }

    #[test]
    fn worker_license_gate_defaults_to_allow() {
        // Go's worker never license-gates Batch Changes (`features.go`'s
        // `FeatureBatchChanges` is only called by the frontend resolvers).
        assert!(WorkerBatchChangesLicenseGate
            .check_feature_batch_changes()
            .is_ok());
    }
}
