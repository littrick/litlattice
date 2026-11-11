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
//! Code-host API clients (GitHub/GitLab/Bitbucket/AzureDevOps/Perforce/Gerrit)
//! are **not ported**. The production [`NotPortedSourceFactory`] returns
//! [`SourceError::not_ported`] only when a source call is actually required;
//! the differential harness injects a fake source. `state.SetDerivedState`,
//! `Changeset.Events()` and GitHub App commit signing are likewise seams (the
//! source is responsible for setting `external_state`), see the slice overview
//! §4.
//!
//! Handler transaction semantics match `Reconciler.HandlerFunc`: `process`
//! runs inside a store transaction, `done(err)` commits or rolls back, and the
//! after-done webhook hook runs regardless of the transaction outcome.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::batches::{
    self as db, ChangesetSpecRow, ChangesetUpdate, ReconcilerChangesetRow, ReconcilerRepo,
    RECONCILER_MAX_NUM_RESETS, RECONCILER_MAX_NUM_RETRIES, RECONCILER_RETRY_AFTER_SECONDS,
    RECONCILER_STALLED_MAX_AGE_SECONDS, RECONCILER_STORE_NAME, RECONCILER_TABLE, RECONCILER_VIEW,
    RECONCILER_WORKER_DESCRIPTION, RECONCILER_WORKER_NAME,
};
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
}

/// `sources.Sourcer`.
#[async_trait]
pub trait ChangesetSourceFactory: Send + Sync {
    async fn for_changeset(
        &self,
        changeset: &Changeset,
        target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError>;
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

        if exists {
            let outdated = match css.is_outdated(&cs).await {
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

        // Both `afterDoneUpdate` and `afterDonePublish` enqueue
        // `ChangesetUpdateError` in the baseline (overview §1.4).
        (Some(WebhookKind::UpdateError), Ok(()))
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
}

/// Opens [`PgReconcilerStore`] transactions.
pub struct PgReconcilerStoreFactory {
    pool: PgPool,
    external_url: String,
}

impl PgReconcilerStoreFactory {
    pub fn new(pool: PgPool, external_url: impl Into<String>) -> Self {
        Self {
            pool,
            external_url: external_url.into(),
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
        let update = ChangesetUpdate {
            id: changeset.id,
            batch_change_ids: changeset.batch_changes_json(),
            detached_at: changeset.detached_at,
            closing: changeset.closing,
            publication_state,
            ui_publication_state: ui_state,
            external_id,
            external_state,
            failure_message: changeset.failure_message.as_deref(),
            previous_failure_message: changeset.previous_failure_message.as_deref(),
        };
        db::update_reconciler_changeset(&mut self.conn, &update)
            .await
            .map_err(|err| err.to_string())
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

        let external_url = deps
            .live_config
            .current()
            .site
            .raw()
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned();
        let store_factory = Arc::new(PgReconcilerStoreFactory::new(
            deps.frontend.clone(),
            external_url,
        ));
        let handler = Arc::new(ReconcilerHandler::new(
            store_factory,
            Arc::new(NotPortedSourceFactory),
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
}
