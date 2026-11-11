#![forbid(unsafe_code)]

//! `cmd/worker/internal/batches` + `internal/batches/processor`: the
//! `batches-bulk-processor` job.
//!
//! Baseline composition:
//!
//! * `bulk_operation_processor_job.go` — `batches.IsEnabled()` gate and the
//!   single `NewBulkOperationWorker` routine;
//! * `workers/bulk_processor_worker.go` — `Transact` → `Process` →
//!   `Done(err)` → `afterDone(store)`, where `afterDone` runs **even when the
//!   transaction finished with an error** (worst case: a stale webhook
//!   payload);
//! * `processor/bulk_processor.go` — the job-type switch (comment / detach /
//!   reenqueue / merge / close / publish), the payload checks, and the
//!   retryable/non-retryable classification;
//! * `store/worker_bulk_operations.go` — the `changeset_jobs` dbworker store.
//!
//! Code-host API clients are not ported: [`ChangesetSource`] is the injection
//! seam and [`NotPortedChangesetSource`] is the production implementation.
//! Webhook payload marshalling (GraphQL + internal doer), `reenqueue`'s
//! service-level queries and `state.SetDerivedState` (gitserver) are seams as
//! well; the ordering semantics are reproduced by [`run_bulk_job`].

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::batches::batches_enabled;
use super::{Job, JobDeps};
use crate::framework::dbworker::{
    default_hostname, DbRecord, DbWorkerHandler, DbWorkerStore, WorkerOptions, WorkerRoutine,
};
use crate::framework::BackgroundRoutine;

/// The registry/job name (`builtins` key in `cmd/worker/shared`).
pub const JOB_NAME: &str = "batches-bulk-processor";
/// `workerutil.WorkerOptions.Name` (`NewBulkOperationWorker`).
pub const WORKER_NAME: &str = "batches_bulk_processor";
/// `workerutil.WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str = "executes the bulk operations in the background";
/// `workerutil.WorkerOptions.NumHandlers`.
pub const NUM_HANDLERS: usize = 5;
/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
/// `workerutil.WorkerOptions.Interval`.
pub const INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// `webhooks.ChangesetClose`.
pub const CHANGESET_CLOSE_EVENT: &str = "changeset:close";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// `batches/types.ChangesetJobType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangesetJobKind {
    Comment,
    Detach,
    Reenqueue,
    Merge,
    Close,
    Publish,
    Unknown(String),
}

impl ChangesetJobKind {
    /// `scanChangesetJob`'s `switch c.JobType`.
    pub fn from_db(raw: &str) -> Self {
        match raw {
            "commentatore" => Self::Comment,
            "detach" => Self::Detach,
            "reenqueue" => Self::Reenqueue,
            "merge" => Self::Merge,
            "close" => Self::Close,
            "publish" => Self::Publish,
            other => Self::Unknown(other.to_owned()),
        }
    }

    /// The database spelling (`ChangesetJobType` constants).
    pub fn as_db(&self) -> &str {
        match self {
            Self::Comment => "commentatore",
            Self::Detach => "detach",
            Self::Reenqueue => "reenqueue",
            Self::Merge => "merge",
            Self::Close => "close",
            Self::Publish => "publish",
            Self::Unknown(raw) => raw,
        }
    }
}

/// A `changeset_jobs` row as `scanChangesetJob` reads it.
///
/// `payload_type` models the concrete Go payload type that `scanChangesetJob`
/// installs from the job type (`*ChangesetJobCommentPayload`, …). It exists
/// separately so the differential can exercise the type-assertion failure
/// branches, which a real scan can never hit.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetJob {
    pub id: i64,
    pub bulk_group: String,
    pub batch_change_id: i64,
    pub user_id: i32,
    pub changeset_id: i64,
    pub job_type: ChangesetJobKind,
    pub payload: Value,
    pub payload_type: Option<String>,
    pub state: String,
    pub failure_message: Option<String>,
    pub num_resets: i32,
    pub num_failures: i32,
}

impl DbRecord for ChangesetJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

impl ChangesetJob {
    /// The concrete payload type name the processor expects for this job type.
    pub fn expected_payload_type(&self) -> String {
        go_payload_type_name(self.job_type.as_db())
    }

    /// The concrete payload type the row carries; falls back to the expected
    /// type (a real scan always installs the matching struct).
    pub fn actual_payload_type(&self) -> String {
        match &self.payload_type {
            Some(raw) => go_payload_type_name(raw),
            None => self.expected_payload_type(),
        }
    }
}

/// `*types.ChangesetJob<Kind>Payload` spelling used by the `%T` error text.
pub fn go_payload_type_name(kind: &str) -> String {
    let name = match kind {
        "commentatore" | "comment" => "Comment",
        "detach" => "Detach",
        "reenqueue" => "Reenqueue",
        "merge" => "Merge",
        "close" => "Close",
        "publish" => "Publish",
        other => other,
    };
    format!("*types.ChangesetJob{name}Payload")
}

/// `batches/types.BatchChangeAssoc`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetBatchChange {
    pub batch_change_id: i64,
    pub detach: bool,
}

/// The subset of `batches/types.Changeset` the processor reads.
#[derive(Debug, Clone, PartialEq)]
pub struct Changeset {
    pub id: i64,
    pub repo_id: i32,
    /// `CurrentSpecID`; `0`/`None` means imported.
    pub current_spec_id: i64,
    pub reconciler_state: String,
    /// `UiPublicationState` as stored in the database (`DRAFT`/`PUBLISHED`/…).
    pub ui_publication_state: Option<String>,
    pub batch_changes: Vec<ChangesetBatchChange>,
}

/// `types.Repo` reduced to what `sourcer.ForUser` / `GetRemoteRepo` need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRef {
    pub id: i32,
    pub name: String,
    pub external_service_type: String,
    pub external_service_id: String,
}

/// `types.ChangesetEvent` reduced to the identity fields used by the fake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetEvent {
    pub kind: String,
    pub key: String,
}

/// `errcode.MakeNonRetryable`-aware error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkError {
    pub message: String,
    pub non_retryable: bool,
}

impl BulkError {
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

    /// `errors.Wrap`: keeps the classification (`errcode.IsNonRetryable`
    /// walks the cause chain).
    pub fn with_prefix(self, prefix: &str) -> Self {
        Self {
            message: format!("{prefix}: {}", self.message),
            non_retryable: self.non_retryable,
        }
    }
}

/// `changesetIsProcessingErr`.
pub const CHANGESET_IS_PROCESSING_ERR: &str =
    "cannot update a changeset that is currently being processed; will retry";

/// An enqueued webhook (`afterDone`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEnqueue {
    pub event_type: String,
    pub changeset_id: i64,
}

/// `BulkProcessor.Process`'s result.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProcessOutcome {
    pub after_done: Option<WebhookEnqueue>,
    pub error: Option<BulkError>,
}

impl ProcessOutcome {
    fn ok() -> Self {
        Self::default()
    }

    fn err(err: BulkError) -> Self {
        Self {
            after_done: None,
            error: Some(err),
        }
    }
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// The store operations `internal/batches/processor` drives (a transaction in
/// the baseline).
#[async_trait]
pub trait BulkProcessorStore: Send + Sync {
    async fn get_changeset(&self, id: i64) -> Result<Option<Changeset>, BulkError>;
    async fn get_repo(&self, repo_id: i32) -> Result<Option<RepoRef>, BulkError>;
    /// Outer `None`: no spec; inner `None`: `Published.Nil()`.
    async fn get_changeset_spec_published(
        &self,
        id: i64,
    ) -> Result<Option<Option<bool>>, BulkError>;
    async fn update_changeset_batch_changes(&self, changeset: &Changeset) -> Result<(), BulkError>;
    async fn enqueue_changeset(
        &self,
        changeset_id: i64,
        reset_state: &str,
    ) -> Result<(), BulkError>;
    async fn reenqueue_changeset(&self, changeset_id: i64) -> Result<(), BulkError>;
    async fn set_derived_state(&self, changeset: &Changeset) -> Result<(), BulkError>;
    async fn upsert_changeset_events(
        &self,
        changeset_id: i64,
        events: &[ChangesetEvent],
    ) -> Result<(), BulkError>;
    async fn update_changeset_code_host_state(
        &self,
        changeset: &Changeset,
    ) -> Result<(), BulkError>;
    async fn update_changeset_ui_publication_state(
        &self,
        changeset: &Changeset,
    ) -> Result<(), BulkError>;
    /// `afterDone` → `webhooks.EnqueueChangeset`. Fire-and-forget in the
    /// baseline (errors are only logged).
    async fn enqueue_changeset_webhook(
        &self,
        event_type: &str,
        changeset_id: i64,
    ) -> Result<(), BulkError>;
}

/// `sources.Sourcer` + `sources.ChangesetSource` (code-host clients are not
/// ported).
#[async_trait]
pub trait ChangesetSource: Send + Sync {
    async fn for_user(&self, user_id: i32, repo: &RepoRef) -> Result<(), BulkError>;
    async fn get_remote_repo(&self, repo: &RepoRef, changeset: &Changeset)
        -> Result<(), BulkError>;
    async fn create_comment(&self, changeset: &Changeset, message: &str) -> Result<(), BulkError>;
    async fn merge_changeset(&self, changeset: &Changeset, squash: bool) -> Result<(), BulkError>;
    async fn close_changeset(&self, changeset: &Changeset) -> Result<(), BulkError>;
    /// `cs.Changeset.Events()`: derived from the code-host metadata.
    async fn changeset_events(
        &self,
        changeset: &Changeset,
    ) -> Result<Vec<ChangesetEvent>, BulkError>;
}

/// `store.Transact` / `(*store.Store).Done`.
#[async_trait]
pub trait BulkTransaction: Send + Sync {
    /// `Done(err)`: commit when `err` is `None`, roll back otherwise; the
    /// returned error is the handler's final error.
    async fn done(&self, err: Option<&BulkError>) -> Result<(), BulkError>;
}

/// The production source: every code-host call is an explicit not-ported
/// error (retryable, exactly like a transient source failure).
#[derive(Debug, Default, Clone, Copy)]
pub struct NotPortedChangesetSource;

fn not_ported(operation: &str) -> BulkError {
    BulkError::retryable(format!(
        "changeset source {operation} is not ported (M9.9h)"
    ))
}

#[async_trait]
impl ChangesetSource for NotPortedChangesetSource {
    async fn for_user(&self, _user_id: i32, _repo: &RepoRef) -> Result<(), BulkError> {
        Err(not_ported("ForUser"))
    }
    async fn get_remote_repo(
        &self,
        _repo: &RepoRef,
        _changeset: &Changeset,
    ) -> Result<(), BulkError> {
        Err(not_ported("GetRemoteRepo"))
    }
    async fn create_comment(
        &self,
        _changeset: &Changeset,
        _message: &str,
    ) -> Result<(), BulkError> {
        Err(not_ported("CreateComment"))
    }
    async fn merge_changeset(
        &self,
        _changeset: &Changeset,
        _squash: bool,
    ) -> Result<(), BulkError> {
        Err(not_ported("MergeChangeset"))
    }
    async fn close_changeset(&self, _changeset: &Changeset) -> Result<(), BulkError> {
        Err(not_ported("CloseChangeset"))
    }
    async fn changeset_events(
        &self,
        _changeset: &Changeset,
    ) -> Result<Vec<ChangesetEvent>, BulkError> {
        Err(not_ported("Events"))
    }
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

/// `global.DefaultReconcilerEnqueueState`: the rollout-window configuration is
/// a seam; without an active window the baseline returns `queued`.
pub fn default_reconciler_enqueue_state() -> &'static str {
    "queued"
}

/// `processor.bulkProcessor`.
pub struct BulkProcessor<'a> {
    store: &'a dyn BulkProcessorStore,
    source: &'a dyn ChangesetSource,
    enqueue_state: &'static str,
}

impl<'a> BulkProcessor<'a> {
    pub fn new(store: &'a dyn BulkProcessorStore, source: &'a dyn ChangesetSource) -> Self {
        Self {
            store,
            source,
            enqueue_state: default_reconciler_enqueue_state(),
        }
    }

    /// `Process` (`bulk_processor.go:62-109`).
    pub async fn process(&self, job: &ChangesetJob) -> ProcessOutcome {
        // actor.WithActor(FromUser(job.UserID)): authz seam, no observable
        // behaviour without the repo store's actor filtering.
        let changeset = match self.store.get_changeset(job.changeset_id).await {
            Ok(Some(changeset)) => changeset,
            Ok(None) => {
                return ProcessOutcome::err(
                    BulkError::retryable("no results").with_prefix("loading changeset"),
                )
            }
            Err(err) => {
                return ProcessOutcome::err(err.with_prefix("loading changeset"));
            }
        };

        if changeset.reconciler_state == "processing" {
            return ProcessOutcome::err(BulkError::retryable(CHANGESET_IS_PROCESSING_ERR));
        }

        let repo = match self.store.get_repo(changeset.repo_id).await {
            Ok(Some(repo)) => repo,
            Ok(None) => {
                return ProcessOutcome::err(
                    BulkError::retryable(format!("repo not found: id={}", changeset.repo_id))
                        .with_prefix("loading repo"),
                )
            }
            Err(err) => return ProcessOutcome::err(err.with_prefix("loading repo")),
        };

        if let Err(err) = self.source.for_user(job.user_id, &repo).await {
            return ProcessOutcome::err(err.with_prefix("loading ChangesetSource"));
        }

        match &job.job_type {
            ChangesetJobKind::Comment => {
                if !self.payload_matches(job) {
                    return ProcessOutcome::err(invalid_payload(job));
                }
                let message = job
                    .payload
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                if let Err(err) = self.source.get_remote_repo(&repo, &changeset).await {
                    return ProcessOutcome::err(err.with_prefix("loading remote repo"));
                }
                match self.source.create_comment(&changeset, &message).await {
                    Ok(()) => ProcessOutcome::ok(),
                    Err(err) => ProcessOutcome::err(err),
                }
            }
            ChangesetJobKind::Detach => {
                if !self.payload_matches(job) {
                    return ProcessOutcome::err(invalid_payload(job));
                }
                self.detach(job, &changeset).await
            }
            ChangesetJobKind::Reenqueue => {
                if !self.payload_matches(job) {
                    return ProcessOutcome::err(invalid_payload(job));
                }
                match self.store.reenqueue_changeset(changeset.id).await {
                    Ok(()) => ProcessOutcome::ok(),
                    Err(err) => ProcessOutcome::err(err),
                }
            }
            ChangesetJobKind::Merge => match self.merge(job, &repo, &changeset).await {
                Ok(after_done) => ProcessOutcome {
                    after_done: Some(after_done),
                    error: None,
                },
                Err(err) => ProcessOutcome::err(err),
            },
            ChangesetJobKind::Close => match self.close(&repo, &changeset).await {
                Ok(after_done) => ProcessOutcome {
                    after_done: Some(after_done),
                    error: None,
                },
                Err(err) => ProcessOutcome::err(err),
            },
            ChangesetJobKind::Publish => self.publish(job, &changeset).await,
            ChangesetJobKind::Unknown(raw) => ProcessOutcome::err(BulkError::non_retryable(
                format!("invalid job type {raw:?}"),
            )),
        }
    }

    fn payload_matches(&self, job: &ChangesetJob) -> bool {
        job.actual_payload_type() == job.expected_payload_type()
    }

    /// `detach` (`bulk_processor.go:130-158`).
    async fn detach(&self, job: &ChangesetJob, changeset: &Changeset) -> ProcessOutcome {
        let mut detached = false;
        let mut batch_changes = changeset.batch_changes.clone();
        for assoc in &mut batch_changes {
            if assoc.batch_change_id == job.batch_change_id && !assoc.detach {
                assoc.detach = true;
                detached = true;
            }
        }
        if !detached {
            return ProcessOutcome::ok();
        }

        let updated = Changeset {
            batch_changes,
            ..changeset.clone()
        };
        if let Err(err) = self.store.update_changeset_batch_changes(&updated).await {
            return ProcessOutcome::err(err);
        }
        match self
            .store
            .enqueue_changeset(updated.id, self.enqueue_state)
            .await
        {
            Ok(()) => ProcessOutcome::ok(),
            Err(err) => ProcessOutcome::err(err),
        }
    }

    /// `mergeChangeset` (`bulk_processor.go:166-205`).
    async fn merge(
        &self,
        job: &ChangesetJob,
        repo: &RepoRef,
        changeset: &Changeset,
    ) -> Result<WebhookEnqueue, BulkError> {
        if !self.payload_matches(job) {
            return Err(invalid_payload(job));
        }
        let squash = job
            .payload
            .get("squash")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        self.source
            .get_remote_repo(repo, changeset)
            .await
            .map_err(|err| err.with_prefix("loading remote repo"))?;
        self.source.merge_changeset(changeset, squash).await?;

        let events = self
            .source
            .changeset_events(changeset)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;
        self.store.set_derived_state(changeset).await?;
        self.store
            .upsert_changeset_events(changeset.id, &events)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;
        self.store
            .update_changeset_code_host_state(changeset)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;

        Ok(WebhookEnqueue {
            event_type: CHANGESET_CLOSE_EVENT.to_owned(),
            changeset_id: changeset.id,
        })
    }

    /// `closeChangeset` (`bulk_processor.go:207-241`).
    async fn close(
        &self,
        repo: &RepoRef,
        changeset: &Changeset,
    ) -> Result<WebhookEnqueue, BulkError> {
        self.source
            .get_remote_repo(repo, changeset)
            .await
            .map_err(|err| err.with_prefix("loading remote repo"))?;
        self.source.close_changeset(changeset).await?;

        let events = self
            .source
            .changeset_events(changeset)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;
        self.store.set_derived_state(changeset).await?;
        self.store
            .upsert_changeset_events(changeset.id, &events)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;
        self.store
            .update_changeset_code_host_state(changeset)
            .await
            .map_err(|err| BulkError::non_retryable(err.message))?;

        Ok(WebhookEnqueue {
            event_type: CHANGESET_CLOSE_EVENT.to_owned(),
            changeset_id: changeset.id,
        })
    }

    /// `publishChangeset` (`bulk_processor.go:243-290`).
    async fn publish(&self, job: &ChangesetJob, changeset: &Changeset) -> ProcessOutcome {
        if !self.payload_matches(job) {
            return ProcessOutcome::err(invalid_payload(job));
        }
        if changeset.current_spec_id == 0 {
            return ProcessOutcome::err(BulkError::non_retryable(
                "cannot publish an imported changeset",
            ));
        }

        let published = match self
            .store
            .get_changeset_spec_published(changeset.current_spec_id)
            .await
        {
            Ok(Some(published)) => published,
            Ok(None) => {
                return ProcessOutcome::err(BulkError::non_retryable(format!(
                    "no changeset spec for changeset {}",
                    changeset.id
                )))
            }
            Err(err) => {
                return ProcessOutcome::err(BulkError::non_retryable(format!(
                    "getting changeset spec for changeset {}: {}",
                    changeset.id, err.message
                )))
            }
        };
        if published.is_some() {
            return ProcessOutcome::err(BulkError::non_retryable(
                "cannot publish a changeset that has a published value set in its changesetTemplate",
            ));
        }

        let draft = job
            .payload
            .get("draft")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let updated = Changeset {
            ui_publication_state: Some(if draft { "DRAFT" } else { "PUBLISHED" }.to_owned()),
            ..changeset.clone()
        };

        if let Err(err) = self
            .store
            .update_changeset_ui_publication_state(&updated)
            .await
        {
            return ProcessOutcome::err(BulkError::non_retryable(err.message));
        }
        match self
            .store
            .enqueue_changeset(updated.id, self.enqueue_state)
            .await
        {
            Ok(()) => ProcessOutcome::ok(),
            Err(err) => ProcessOutcome::err(BulkError::non_retryable(err.message)),
        }
    }
}

/// `invalid payload type for changeset_job, want=%T have=%T`.
pub fn invalid_payload(job: &ChangesetJob) -> BulkError {
    BulkError::retryable(format!(
        "invalid payload type for changeset_job, want={} have={}",
        job.expected_payload_type(),
        job.actual_payload_type()
    ))
}

/// The handler's final result (see [`run_bulk_job`]).
#[derive(Debug, Clone, PartialEq)]
pub struct BulkJobOutcome {
    pub process_error: Option<BulkError>,
    pub after_done: Option<WebhookEnqueue>,
    /// Whether a `Done` call happened at all (`Transact` succeeded).
    pub done_called: bool,
    /// Whether the process error was passed into `Done`.
    pub done_input_error: bool,
    pub done_error: Option<BulkError>,
    pub webhook_error: Option<BulkError>,
    pub handler_error: Option<BulkError>,
}

impl BulkJobOutcome {
    /// `workerutil`'s `MarkComplete` / `MarkFailed` / `MarkErrored`.
    pub fn final_state(&self) -> &'static str {
        match &self.handler_error {
            None => "complete",
            Some(err) if err.non_retryable => "failed",
            Some(_) => "errored",
        }
    }
}

/// `bulkProcessorWorker.HandlerFunc` (`bulk_processor_worker.go:50-74`).
///
/// `transaction` models `store.Transact`: `Err` means the handler returns
/// immediately without processing or enqueuing a webhook.
pub async fn run_bulk_job(
    store: &dyn BulkProcessorStore,
    source: &dyn ChangesetSource,
    transaction: Result<&dyn BulkTransaction, BulkError>,
    job: &ChangesetJob,
) -> BulkJobOutcome {
    let tx = match transaction {
        Ok(tx) => tx,
        Err(err) => {
            return BulkJobOutcome {
                process_error: None,
                after_done: None,
                done_called: false,
                done_input_error: false,
                done_error: None,
                webhook_error: None,
                handler_error: Some(err),
            }
        }
    };

    let processor = BulkProcessor::new(store, source);
    let outcome = processor.process(job).await;

    let done_result = tx.done(outcome.error.as_ref()).await;
    let done_error = done_result.as_ref().err().cloned();

    // afterDone runs regardless of the transaction's outcome: the webhook
    // represents the code-host interaction, not the database transaction.
    let webhook_error = match &outcome.after_done {
        Some(webhook) => store
            .enqueue_changeset_webhook(&webhook.event_type, webhook.changeset_id)
            .await
            .err(),
        None => None,
    };

    let handler_error = match done_result {
        Err(err) => Some(err),
        Ok(()) => outcome.error.clone(),
    };

    BulkJobOutcome {
        process_error: outcome.error,
        after_done: outcome.after_done,
        done_called: true,
        done_input_error: false, // patched by the caller-observable value below
        done_error,
        webhook_error,
        handler_error,
    }
}

/// Builds the outcome while recording whether `Done` received an error. Kept
/// separate so the differential can print the exact input flag.
pub async fn run_bulk_job_observed(
    store: &dyn BulkProcessorStore,
    source: &dyn ChangesetSource,
    transaction: Result<&dyn BulkTransaction, BulkError>,
    job: &ChangesetJob,
) -> BulkJobOutcome {
    let mut outcome = run_bulk_job(store, source, transaction, job).await;
    outcome.done_input_error = outcome.process_error.is_some();
    outcome
}

/// Production handler: one `changeset_jobs` record per invocation.
pub struct BulkProcessorHandler {
    pool: PgPool,
    source: Arc<dyn ChangesetSource>,
}

impl BulkProcessorHandler {
    pub fn new(pool: PgPool, source: Arc<dyn ChangesetSource>) -> Self {
        Self { pool, source }
    }
}

/// The non-retryable messages that the production path can produce (see
/// `processor`'s `errcode.MakeNonRetryable` calls). merge/close internal
/// errors are non-retryable in the baseline too, but are unreachable in
/// production until the code-host source is ported.
pub fn is_non_retryable_message(message: &str) -> bool {
    message.starts_with("invalid job type ")
        || message == "cannot publish an imported changeset"
        || message.starts_with("getting changeset spec for changeset ")
        || message.starts_with("no changeset spec for changeset ")
        || message
            == "cannot publish a changeset that has a published value set in its changesetTemplate"
}

#[async_trait]
impl DbWorkerHandler<BulkStore> for BulkProcessorHandler {
    async fn handle(&self, _ctx: CancellationToken, record: &ChangesetJob) -> Result<(), String> {
        // `store.Transact` returns a per-call transaction handle; `NumHandlers`
        // handlers run concurrently, so each invocation owns its own
        // `PgBulkProcessorStore` (and therefore its own transaction slot).
        let store = PgBulkProcessorStore::new(self.pool.clone());
        if let Err(err) = store.begin().await {
            return Err(err.message);
        }
        let tx: &dyn BulkTransaction = &store;
        let outcome = run_bulk_job_observed(&store, self.source.as_ref(), Ok(tx), record).await;
        match outcome.handler_error {
            Some(err) => Err(err.message),
            None => Ok(()),
        }
    }

    fn non_retryable(&self, failure_message: &str) -> bool {
        is_non_retryable_message(failure_message)
    }
}

// ---------------------------------------------------------------------------
// Production store
// ---------------------------------------------------------------------------

/// The `changeset_jobs` dbworker store (`worker_bulk_operations.go`).
pub struct BulkStore {
    pool: PgPool,
}

impl BulkStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbWorkerStore for BulkStore {
    type Record = ChangesetJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        sg_db::batches::dequeue_changeset_job(
            &self.pool,
            OffsetDateTime::now_utc(),
            worker_hostname,
        )
        .await
        .map_err(|err| err.to_string())
        .map(|row| row.map(changeset_job_from_row))
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        sg_db::batches::heartbeat_changeset_jobs(&self.pool, OffsetDateTime::now_utc(), ids)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        sg_db::batches::mark_changeset_job_complete(&self.pool, record.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        sg_db::batches::mark_changeset_job_errored(&self.pool, record.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        sg_db::batches::mark_changeset_job_failed(&self.pool, record.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }
}

/// The processor's transactional store over the frontend database.
///
/// The baseline's `store.Transact` binds the `BulkProcessorStore` methods and
/// `Done` to one transaction. `sqlx::Transaction<'static, Postgres>` allows
/// the same shape here: only one processor call runs at a time, so a mutex is
/// enough.
pub struct PgBulkProcessorStore {
    pool: PgPool,
    tx: tokio::sync::Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>,
}

impl PgBulkProcessorStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            tx: tokio::sync::Mutex::new(None),
        }
    }

    /// `store.Transact`.
    pub async fn begin(&self) -> Result<(), BulkError> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|err| BulkError::retryable(format!("beginning transaction: {err}")))?;
        *self.tx.lock().await = Some(tx);
        Ok(())
    }
}

fn no_active_transaction() -> BulkError {
    BulkError::retryable("no active transaction")
}

fn db_error(err: sqlx::Error) -> BulkError {
    BulkError::retryable(err.to_string())
}

#[async_trait]
impl BulkProcessorStore for PgBulkProcessorStore {
    async fn get_changeset(&self, id: i64) -> Result<Option<Changeset>, BulkError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        let row = sg_db::batches::get_changeset_for_bulk_job(&mut *tx, id)
            .await
            .map_err(db_error)?;
        Ok(row.map(changeset_from_row))
    }

    async fn get_repo(&self, repo_id: i32) -> Result<Option<RepoRef>, BulkError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        let row = sg_db::batches::get_repo_for_bulk_job(&mut *tx, repo_id)
            .await
            .map_err(db_error)?;
        Ok(row.map(|row| RepoRef {
            id: row.id,
            name: row.name,
            external_service_type: row.external_service_type,
            external_service_id: row.external_service_id,
        }))
    }

    async fn get_changeset_spec_published(
        &self,
        id: i64,
    ) -> Result<Option<Option<bool>>, BulkError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        sg_db::batches::get_changeset_spec_published(&mut *tx, id)
            .await
            .map_err(db_error)
    }

    async fn update_changeset_batch_changes(&self, changeset: &Changeset) -> Result<(), BulkError> {
        let value = batch_changes_json(changeset);
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        sg_db::batches::update_changeset_batch_changes(&mut *tx, changeset.id, &value)
            .await
            .map_err(db_error)
    }

    async fn enqueue_changeset(
        &self,
        changeset_id: i64,
        reset_state: &str,
    ) -> Result<(), BulkError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        let updated = sg_db::batches::enqueue_changeset(&mut *tx, changeset_id, reset_state, None)
            .await
            .map_err(db_error)?;
        if !updated {
            return Err(BulkError::retryable(
                "cannot re-enqueue changeset not in failed state",
            ));
        }
        Ok(())
    }

    async fn reenqueue_changeset(&self, _changeset_id: i64) -> Result<(), BulkError> {
        Err(BulkError::retryable(
            "service.ReenqueueChangeset is not ported (M9.9h)",
        ))
    }

    async fn set_derived_state(&self, _changeset: &Changeset) -> Result<(), BulkError> {
        // state.SetDerivedState needs gitserver; merge/close never reach it
        // until the code-host source is ported.
        Ok(())
    }

    async fn upsert_changeset_events(
        &self,
        _changeset_id: i64,
        _events: &[ChangesetEvent],
    ) -> Result<(), BulkError> {
        Err(BulkError::retryable(
            "UpsertChangesetEvents is not ported (M9.9h)",
        ))
    }

    async fn update_changeset_code_host_state(
        &self,
        _changeset: &Changeset,
    ) -> Result<(), BulkError> {
        Err(BulkError::retryable(
            "UpdateChangesetCodeHostState is not ported (M9.9h)",
        ))
    }

    async fn update_changeset_ui_publication_state(
        &self,
        changeset: &Changeset,
    ) -> Result<(), BulkError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().ok_or_else(no_active_transaction)?;
        sg_db::batches::update_changeset_ui_publication_state(
            &mut *tx,
            changeset.id,
            changeset.ui_publication_state.as_deref(),
        )
        .await
        .map_err(db_error)
    }

    async fn enqueue_changeset_webhook(
        &self,
        _event_type: &str,
        _changeset_id: i64,
    ) -> Result<(), BulkError> {
        // webhooks.EnqueueChangeset marshals a GraphQL payload with the
        // internal doer before writing `outbound_webhook_jobs`; that
        // marshaller is not ported. The ordering is still reproduced by
        // `run_bulk_job`.
        Err(BulkError::retryable(
            "changeset webhook enqueue is not ported (M9.9h)",
        ))
    }
}

#[async_trait]
impl BulkTransaction for PgBulkProcessorStore {
    async fn done(&self, err: Option<&BulkError>) -> Result<(), BulkError> {
        let mut guard = self.tx.lock().await;
        let Some(tx) = guard.take() else {
            return Err(BulkError::retryable(
                "transaction already completed (ErrNotInTransaction)",
            ));
        };
        match err {
            Some(err) => {
                let _ = tx.rollback().await;
                Err(err.clone())
            }
            None => tx
                .commit()
                .await
                .map_err(|err| BulkError::retryable(format!("committing transaction: {err}"))),
        }
    }
}

/// Maps a dequeued row to the processor's record type.
pub fn changeset_job_from_row(row: sg_db::batches::ChangesetJobRow) -> ChangesetJob {
    ChangesetJob {
        id: row.id,
        bulk_group: row.bulk_group,
        batch_change_id: row.batch_change_id,
        user_id: row.user_id,
        changeset_id: row.changeset_id,
        job_type: ChangesetJobKind::from_db(&row.job_type),
        payload: row.payload,
        payload_type: None,
        state: row.state,
        failure_message: row.failure_message,
        num_resets: row.num_resets,
        num_failures: row.num_failures,
    }
}

/// `batchChangesColumn` parsing (`UnmarshalJSON` into `[]BatchChangeAssoc`).
pub fn batch_changes_from_json(value: &Value) -> Vec<ChangesetBatchChange> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    let mut assocs = Vec::with_capacity(map.len());
    for (key, entry) in map {
        let Ok(batch_change_id) = key.parse::<i64>() else {
            continue;
        };
        let detach = entry
            .get("detach")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        assocs.push(ChangesetBatchChange {
            batch_change_id,
            detach,
        });
    }
    assocs.sort_by_key(|assoc| assoc.batch_change_id);
    assocs
}

/// Maps a `changesets` row to the processor's type (`CurrentSpecID` is `0`
/// when the column is `NULL`).
pub fn changeset_from_row(row: sg_db::batches::BulkChangesetRow) -> Changeset {
    Changeset {
        id: row.id,
        repo_id: row.repo_id,
        current_spec_id: row.current_spec_id.unwrap_or(0),
        reconciler_state: row.reconciler_state,
        ui_publication_state: row.ui_publication_state,
        batch_changes: batch_changes_from_json(&row.batch_change_ids),
    }
}

/// `batchChangesColumn`: a JSON object keyed by batch change id.
pub fn batch_changes_json(changeset: &Changeset) -> Value {
    let mut map = serde_json::Map::new();
    for assoc in &changeset.batch_changes {
        let mut value = serde_json::Map::new();
        if assoc.detach {
            value.insert("detach".to_owned(), Value::Bool(true));
        }
        map.insert(assoc.batch_change_id.to_string(), Value::Object(value));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

/// `bulkOperationProcessorJob`.
#[derive(Default)]
pub struct BulkOperationProcessorJob;

impl BulkOperationProcessorJob {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for BulkOperationProcessorJob {
    fn description(&self) -> String {
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !batches_enabled() {
            return Ok(Vec::new());
        }

        let bulk_store = Arc::new(BulkStore::new(deps.frontend.clone()));
        let handler = Arc::new(BulkProcessorHandler::new(
            deps.frontend.clone(),
            Arc::new(NotPortedChangesetSource),
        ));

        let worker = WorkerRoutine::new(
            bulk_store,
            handler,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: INTERVAL,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                num_handlers: NUM_HANDLERS,
                worker_hostname: default_hostname(),
            },
        );

        Ok(vec![BackgroundRoutine::DbBacked(
            Arc::new(worker) as Arc<dyn crate::framework::dbworker::DbBackedRoutine>
        )])
    }
}

/// A chart of the `changeset_jobs` columns used by the store (documentation
/// and compile-time sanity for the differential).
pub const CHANGESET_JOB_PAYLOAD_KINDS: [(&str, &str); 6] = [
    ("commentatore", "message"),
    ("detach", ""),
    ("reenqueue", ""),
    ("merge", "squash"),
    ("close", ""),
    ("publish", "draft"),
];

/// Deterministic event log helper shared by the differential fakes.
pub fn format_event(name: &str, fields: &BTreeMap<&str, String>) -> String {
    if fields.is_empty() {
        return name.to_owned();
    }
    let rendered: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    format!("{name}({})", rendered.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_metadata_matches_the_baseline() {
        assert_eq!(JOB_NAME, "batches-bulk-processor");
        assert_eq!(WORKER_NAME, "batches_bulk_processor");
        assert_eq!(
            WORKER_DESCRIPTION,
            "executes the bulk operations in the background"
        );
        assert_eq!(NUM_HANDLERS, 5);
        assert_eq!(HEARTBEAT_INTERVAL, std::time::Duration::from_secs(15));
        assert_eq!(INTERVAL, std::time::Duration::from_secs(5));
    }

    #[test]
    fn job_kind_round_trips() {
        for raw in [
            "commentatore",
            "detach",
            "reenqueue",
            "merge",
            "close",
            "publish",
        ] {
            assert_eq!(ChangesetJobKind::from_db(raw).as_db(), raw);
        }
        assert_eq!(
            ChangesetJobKind::from_db("bogus"),
            ChangesetJobKind::Unknown("bogus".to_owned())
        );
    }

    #[test]
    fn payload_type_names_match_go() {
        assert_eq!(
            go_payload_type_name("commentatore"),
            "*types.ChangesetJobCommentPayload"
        );
        assert_eq!(
            go_payload_type_name("publish"),
            "*types.ChangesetJobPublishPayload"
        );
    }

    #[test]
    fn non_retryable_classification_matches_reachable_paths() {
        assert!(is_non_retryable_message("invalid job type \"bogus\""));
        assert!(is_non_retryable_message(
            "cannot publish an imported changeset"
        ));
        assert!(is_non_retryable_message(
            "getting changeset spec for changeset 1: boom"
        ));
        assert!(is_non_retryable_message(
            "no changeset spec for changeset 1"
        ));
        assert!(is_non_retryable_message(
            "cannot publish a changeset that has a published value set in its changesetTemplate"
        ));
        assert!(!is_non_retryable_message(CHANGESET_IS_PROCESSING_ERR));
        assert!(!is_non_retryable_message("loading remote repo: boom"));
    }

    #[test]
    fn invalid_payload_message_matches_go() {
        let job = ChangesetJob {
            id: 1,
            bulk_group: String::new(),
            batch_change_id: 0,
            user_id: 0,
            changeset_id: 1,
            job_type: ChangesetJobKind::Comment,
            payload: Value::Null,
            payload_type: Some("merge".to_owned()),
            state: "processing".to_owned(),
            failure_message: None,
            num_resets: 0,
            num_failures: 0,
        };
        assert_eq!(
            invalid_payload(&job).message,
            "invalid payload type for changeset_job, \
             want=*types.ChangesetJobCommentPayload have=*types.ChangesetJobMergePayload"
        );
    }

    #[test]
    fn batch_changes_column_matches_go_json() {
        let changeset = Changeset {
            id: 1,
            repo_id: 1,
            current_spec_id: 0,
            reconciler_state: "completed".to_owned(),
            ui_publication_state: None,
            batch_changes: vec![
                ChangesetBatchChange {
                    batch_change_id: 2,
                    detach: true,
                },
                ChangesetBatchChange {
                    batch_change_id: 3,
                    detach: false,
                },
            ],
        };
        assert_eq!(
            batch_changes_json(&changeset),
            serde_json::json!({"2": {"detach": true}, "3": {}})
        );
    }

    #[tokio::test]
    async fn after_done_runs_after_a_failed_transaction_commit() {
        // merge succeeds, the commit fails: the webhook must still be enqueued.
        let store = FakeStore::default();
        let source = FakeSource;
        let tx = FakeTx {
            commit_error: Some(BulkError::retryable("commit boom")),
        };
        let job = merge_job();
        let outcome = run_bulk_job_observed(&store, &source, Ok(&tx), &job).await;
        assert!(outcome.after_done.is_some());
        assert_eq!(
            outcome.handler_error.as_ref().map(|e| e.message.as_str()),
            Some("commit boom")
        );
        assert_eq!(store.webhooks.lock().unwrap().len(), 1);
    }

    #[derive(Default)]
    struct FakeStore {
        webhooks: std::sync::Mutex<Vec<(String, i64)>>,
    }

    #[async_trait]
    impl BulkProcessorStore for FakeStore {
        async fn get_changeset(&self, id: i64) -> Result<Option<Changeset>, BulkError> {
            Ok(Some(Changeset {
                id,
                repo_id: 1,
                current_spec_id: 1,
                reconciler_state: "completed".to_owned(),
                ui_publication_state: None,
                batch_changes: Vec::new(),
            }))
        }
        async fn get_repo(&self, repo_id: i32) -> Result<Option<RepoRef>, BulkError> {
            Ok(Some(RepoRef {
                id: repo_id,
                name: "github.com/x/y".to_owned(),
                external_service_type: "github".to_owned(),
                external_service_id: "1".to_owned(),
            }))
        }
        async fn get_changeset_spec_published(
            &self,
            _id: i64,
        ) -> Result<Option<Option<bool>>, BulkError> {
            Ok(Some(None))
        }
        async fn update_changeset_batch_changes(
            &self,
            _changeset: &Changeset,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn enqueue_changeset(
            &self,
            _changeset_id: i64,
            _reset_state: &str,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn reenqueue_changeset(&self, _changeset_id: i64) -> Result<(), BulkError> {
            Ok(())
        }
        async fn set_derived_state(&self, _changeset: &Changeset) -> Result<(), BulkError> {
            Ok(())
        }
        async fn upsert_changeset_events(
            &self,
            _changeset_id: i64,
            _events: &[ChangesetEvent],
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn update_changeset_code_host_state(
            &self,
            _changeset: &Changeset,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn update_changeset_ui_publication_state(
            &self,
            _changeset: &Changeset,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn enqueue_changeset_webhook(
            &self,
            event_type: &str,
            changeset_id: i64,
        ) -> Result<(), BulkError> {
            self.webhooks
                .lock()
                .unwrap()
                .push((event_type.to_owned(), changeset_id));
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeSource;

    #[async_trait]
    impl ChangesetSource for FakeSource {
        async fn for_user(&self, _user_id: i32, _repo: &RepoRef) -> Result<(), BulkError> {
            Ok(())
        }
        async fn get_remote_repo(
            &self,
            _repo: &RepoRef,
            _changeset: &Changeset,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn create_comment(
            &self,
            _changeset: &Changeset,
            _message: &str,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn merge_changeset(
            &self,
            _changeset: &Changeset,
            _squash: bool,
        ) -> Result<(), BulkError> {
            Ok(())
        }
        async fn close_changeset(&self, _changeset: &Changeset) -> Result<(), BulkError> {
            Ok(())
        }
        async fn changeset_events(
            &self,
            _changeset: &Changeset,
        ) -> Result<Vec<ChangesetEvent>, BulkError> {
            Ok(Vec::new())
        }
    }

    struct FakeTx {
        commit_error: Option<BulkError>,
    }

    #[async_trait]
    impl BulkTransaction for FakeTx {
        async fn done(&self, err: Option<&BulkError>) -> Result<(), BulkError> {
            match err {
                Some(err) => Err(err.clone()),
                None => match &self.commit_error {
                    Some(err) => Err(err.clone()),
                    None => Ok(()),
                },
            }
        }
    }

    fn merge_job() -> ChangesetJob {
        ChangesetJob {
            id: 1,
            bulk_group: String::new(),
            batch_change_id: 1,
            user_id: 1,
            changeset_id: 10,
            job_type: ChangesetJobKind::Merge,
            payload: serde_json::json!({"squash": true}),
            payload_type: None,
            state: "processing".to_owned(),
            failure_message: None,
            num_resets: 0,
            num_failures: 0,
        }
    }
}
