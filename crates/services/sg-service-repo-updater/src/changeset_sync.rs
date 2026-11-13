#![forbid(unsafe_code)]

//! `internal/batches/syncer`: the Batch Changes changeset sync registry.
//!
//! The registry owns one `changesetSyncer` per code host. Every minute it
//! reconciles the set of running syncers against `ListCodeHosts`; each syncer
//! computes a schedule from `ListChangesetSyncData` every two minutes and
//! runs the changesets that are due. High-priority enqueues arrive through
//! the registry's buffered channel (capacity 500) and are fetched from the
//! database with a 10s timeout before being routed to the matching host.
//!
//! [`SourceSyncFn`] wires the baseline's `syncer.SyncChangeset` ordering to a
//! production code-host loader ([`PgChangesetSourceLoader`]) and the
//! PostgreSQL backend ([`PgChangesetSyncBackend`]). The loader selects the
//! changeset repository's external service like `sources.Sourcer.ForChangeset`
//! and loads the code-host object through `sg_extsvc::batches_sources`
//! (GitHub / GitLab / Bitbucket Server). The backend performs the baseline's
//! full 15-column `UpdateChangesetCodeHostState` write and scans the
//! `RETURNING` row back like `ScanChangeset`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::{json, Value};
use sg_config::LiveConfig;
use sg_db::batches::{ChangesetSyncData, CodeHost};
use sg_db::offset_to_system_time;
use sg_db::repos::{Repo, RepoStore};
use sg_extsvc::batches_sources::{
    abbreviate_ref, ensure_ref_prefix, is_batches_source_kind, new_source,
    resolve_runtime_changeset_source_connection, BatchesGitHubAppStore,
    Changeset as SourceChangesetInner, ChangesetMetadata as SourceChangesetMetadata,
    ChangesetSource, ExternalRepoSpec, GitHubAppTokenSeams, Repo as SourceRepo, RepoSource,
    RuntimeSourceRequest, SourceErrorKind, AUTHENTICATION_STRATEGY_USER_CREDENTIAL,
};
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use sg_metrics::prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::grpc::ChangesetSyncEnqueuer;
use crate::scheduler::Clock;

/// `externalServiceSyncerInterval`: how often the registry reconciles its
/// syncers with `ListCodeHosts`.
pub const EXTERNAL_SERVICE_SYNCER_INTERVAL: Duration = Duration::from_secs(60);

/// `defaultChangesetScheduleInterval`: how often a syncer recomputes its
/// schedule. It involves a database query but no code-host communication.
pub const DEFAULT_CHANGESET_SCHEDULE_INTERVAL: Duration = Duration::from_secs(120);

/// `minSyncDelay` / `maxSyncDelay` (`syncer/sync.go`).
pub const MIN_SYNC_DELAY: Duration = Duration::from_secs(120);
pub const MAX_SYNC_DELAY: Duration = Duration::from_secs(8 * 60 * 60);

/// `make(chan []int64, 500)` for the registry and for every syncer.
pub const PRIORITY_NOTIFY_CAPACITY: usize = 500;

/// `context.WithTimeout(s.ctx, 10*time.Second)` around `ListChangesetSyncData`
/// for priority items.
pub const PRIORITY_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// `syncer.makeMetrics`.
pub struct ChangesetSyncerMetrics {
    pub syncs: CounterVec,
    pub priority_queued: CounterVec,
    pub sync_duration_seconds: HistogramVec,
    pub compute_schedule_duration_seconds: HistogramVec,
    pub schedule_size: GaugeVec,
    pub behind_schedule: GaugeVec,
}

impl ChangesetSyncerMetrics {
    /// Build and register the six `src_repoupdater_changeset_syncer_*`
    /// families. Like the rest of repo-updater's families, registration is
    /// best-effort so tests can build several metric sets in one process.
    pub fn new() -> Self {
        let syncs = CounterVec::new(
            Opts::new(
                "src_repoupdater_changeset_syncer_syncs",
                "Total number of changeset syncs",
            ),
            &["codehost", "success"],
        )
        .expect("valid metric");
        let priority_queued = CounterVec::new(
            Opts::new(
                "src_repoupdater_changeset_syncer_priority_queued",
                "Total number of priority items added to queue",
            ),
            &["codehost"],
        )
        .expect("valid metric");
        let sync_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "src_repoupdater_changeset_syncer_sync_duration_seconds",
                "Time spent syncing changesets",
            )
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]),
            &["codehost", "success"],
        )
        .expect("valid metric");
        let compute_schedule_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "src_repoupdater_changeset_syncer_compute_schedule_duration_seconds",
                "Time spent computing changeset schedule",
            )
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]),
            &["codehost", "success"],
        )
        .expect("valid metric");
        let schedule_size = GaugeVec::new(
            Opts::new(
                "src_repoupdater_changeset_syncer_schedule_size",
                "The number of changesets scheduled to sync",
            ),
            &["codehost"],
        )
        .expect("valid metric");
        let behind_schedule = GaugeVec::new(
            Opts::new(
                "src_repoupdater_changeset_syncer_behind_schedule",
                "The number of changesets behind schedule",
            ),
            &["codehost"],
        )
        .expect("valid metric");

        let registry = sg_metrics::registry();
        for family in [
            Box::new(syncs.clone()) as Box<dyn sg_metrics::prometheus::core::Collector>,
            Box::new(priority_queued.clone()),
            Box::new(sync_duration_seconds.clone()),
            Box::new(compute_schedule_duration_seconds.clone()),
            Box::new(schedule_size.clone()),
            Box::new(behind_schedule.clone()),
        ] {
            let _ = registry.register(family);
        }

        Self {
            syncs,
            priority_queued,
            sync_duration_seconds,
            compute_schedule_duration_seconds,
            schedule_size,
            behind_schedule,
        }
    }
}

impl Default for ChangesetSyncerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// `priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncPriority {
    Normal = 0,
    High = 1,
}

/// `scheduledSync`. `next_sync: None` is Go's zero `time.Time` (a
/// high-priority item for a changeset that was not in the queue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledSync {
    pub changeset_id: i64,
    pub next_sync: Option<SystemTime>,
    pub priority: SyncPriority,
}

impl Ord for ScheduledSync {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // High priority first, then NextSync ascending, then id ascending.
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.next_sync.is_some().cmp(&other.next_sync.is_some()))
            .then_with(|| self.next_sync.cmp(&other.next_sync))
            .then_with(|| self.changeset_id.cmp(&other.changeset_id))
    }
}

impl PartialOrd for ScheduledSync {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// `changesetPriorityQueue`: a min-heap ordered by priority and time of next
/// sync. It is not safe for concurrent use; syncers keep it inside their run
/// loop.
#[derive(Debug, Default)]
pub struct ChangesetPriorityQueue {
    items: BTreeSet<ScheduledSync>,
    index: HashMap<i64, ScheduledSync>,
}

impl ChangesetPriorityQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// `Peek`: the highest-priority item without removing it.
    pub fn peek(&self) -> Option<&ScheduledSync> {
        self.items.iter().next()
    }

    /// `Upsert`: replaces an existing item or adds a new one. If the existing
    /// item was high priority it stays high priority, so a high-priority
    /// request survives schedule recomputation.
    pub fn upsert(&mut self, items: impl IntoIterator<Item = ScheduledSync>) {
        for mut item in items {
            match self.index.get(&item.changeset_id).cloned() {
                None => {
                    self.index.insert(item.changeset_id, item.clone());
                    self.items.insert(item);
                }
                Some(old) => {
                    if old.priority == SyncPriority::High {
                        item.priority = SyncPriority::High;
                    }
                    self.items.remove(&old);
                    self.index.insert(item.changeset_id, item.clone());
                    self.items.insert(item);
                }
            }
        }
    }

    pub fn get(&self, id: i64) -> Option<ScheduledSync> {
        self.index.get(&id).cloned()
    }

    pub fn remove(&mut self, id: i64) {
        if let Some(item) = self.index.remove(&id) {
            self.items.remove(&item);
        }
    }
}

/// `syncer.NextSync`: the time we want the next sync to happen.
pub fn next_sync(_now: SystemTime, data: &ChangesetSyncData) -> SystemTime {
    let last_sync = offset_to_system_time(data.updated_at);

    // `h.LatestEvent.IsZero()` cannot happen for a non-NULL timestamp, but
    // treat a missing value like the baseline's zero time.
    let latest_event = data.latest_event.map(offset_to_system_time);
    let external_updated = data.external_updated_at.map(offset_to_system_time);

    let last_change = match latest_event {
        Some(latest)
            if signed_nanos(latest, last_sync).unsigned_abs() < MIN_SYNC_DELAY.as_nanos() =>
        {
            external_updated
        }
        _ => max_time(external_updated, latest_event),
    };

    // Simple linear backoff for now.
    let diff = match last_change {
        Some(last_change) => signed_nanos(last_sync, last_change),
        // Both timestamps are NULL: Go's zero time makes `diff` enormous, so
        // the delay saturates at maxSyncDelay.
        None => MAX_SYNC_DELAY.as_nanos() as i128,
    };

    // If the last change happened after our last sync, a webhook arrived;
    // wait for a quiet period of at least minSyncDelay after it.
    if diff < 0 {
        return last_change.expect("diff < 0 implies last_change is set") + MIN_SYNC_DELAY;
    }

    let diff = diff.clamp(
        MIN_SYNC_DELAY.as_nanos() as i128,
        MAX_SYNC_DELAY.as_nanos() as i128,
    ) as u128;
    last_sync + Duration::from_nanos(diff as u64)
}

fn max_time(a: Option<SystemTime>, b: Option<SystemTime>) -> Option<SystemTime> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// `a - b` in nanoseconds, negative when `a` is before `b`.
fn signed_nanos(a: SystemTime, b: SystemTime) -> i128 {
    match a.duration_since(b) {
        Ok(after) => after.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    }
}

/// The subset of `batches/store.Store` the syncer uses. Object-safe so tests
/// can supply in-memory implementations.
#[async_trait]
pub trait ChangesetSyncStore: Send + Sync {
    async fn list_code_hosts(
        &self,
        repo_ids: &[i32],
        only_without_webhooks: bool,
        include_perforce: bool,
    ) -> Result<Vec<CodeHost>, String>;

    async fn list_changeset_sync_data(
        &self,
        changeset_ids: &[i64],
        external_service_id: &str,
    ) -> Result<Vec<ChangesetSyncData>, String>;

    async fn list_changeset_ids_for_repos(&self, repo_ids: &[i32]) -> Result<Vec<i64>, String>;

    /// `GetChangeset` with the syncer's preconditions; `None` for
    /// `store.ErrNoResults`. Returns the changeset's repo id.
    async fn get_changeset_for_sync(&self, id: i64) -> Result<Option<i32>, String>;
}

#[async_trait]
impl ChangesetSyncStore for sg_db::batches::BatchesStore {
    async fn list_code_hosts(
        &self,
        repo_ids: &[i32],
        only_without_webhooks: bool,
        include_perforce: bool,
    ) -> Result<Vec<CodeHost>, String> {
        self.list_code_hosts(repo_ids, only_without_webhooks, include_perforce)
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_changeset_sync_data(
        &self,
        changeset_ids: &[i64],
        external_service_id: &str,
    ) -> Result<Vec<ChangesetSyncData>, String> {
        self.list_changeset_sync_data(changeset_ids, external_service_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_changeset_ids_for_repos(&self, repo_ids: &[i32]) -> Result<Vec<i64>, String> {
        self.list_changeset_ids_for_repos(repo_ids)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_changeset_for_sync(&self, id: i64) -> Result<Option<i32>, String> {
        self.get_changeset_for_sync(id)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `changesetSyncer.SyncChangeset` / `syncFunc`, replaceable for tests.
#[async_trait]
pub trait ChangesetSyncFn: Send + Sync {
    async fn sync(&self, id: i64) -> Result<(), String>;
}

/// A minimal [`ChangesetSyncFn`]: applies the `GetChangeset` precondition and
/// reports that no source layer was injected. Kept for tests; production uses
/// [`SourceSyncFn`].
pub struct StoreSyncFn {
    store: Arc<dyn ChangesetSyncStore>,
}

impl StoreSyncFn {
    pub fn new(store: Arc<dyn ChangesetSyncStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ChangesetSyncFn for StoreSyncFn {
    async fn sync(&self, id: i64) -> Result<(), String> {
        match self.store.get_changeset_for_sync(id).await? {
            // `store.ErrNoResults`: `SyncChangeset not found`.
            None => Ok(()),
            Some(_repo_id) => Err(
                "syncing changesets requires the Batch Changes code-host sources, which are not implemented"
                    .to_owned(),
            ),
        }
    }
}

/// A `ChangesetEvent` as the syncer persists it (`kind` + `key`; metadata is
/// the code-host payload).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncedEvent {
    pub kind: String,
    pub key: String,
    pub metadata: serde_json::Value,
}

/// The result of `source.LoadChangeset`:
///
/// * `Loaded` — metadata refreshed (possibly with an error the syncer must
///   record before returning it);
/// * `NotFound` — `ChangesetNotFoundError`, the changeset is marked deleted.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LoadedChangeset {
    /// `c.Events()` after the load; deduplicated.
    pub events: Vec<SyncedEvent>,
    /// `SyncErrorMessage` to store; the caller returns the error afterwards.
    pub sync_error: Option<String>,
    /// `c.IsDeleted()` should be set (`ChangesetNotFoundError`).
    pub deleted: bool,
    /// The 15 code-host columns `UpdateChangesetCodeHostState` rewrites after
    /// a successful load. `None` when the changeset was not found on the code
    /// host (the syncer then only sets the deleted timestamp).
    pub state: Option<ChangesetCodeHostState>,
}

/// A [`ChangesetSourceLoader`] failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLoadError {
    pub message: String,
    /// `true` when the baseline persists the error as `SyncErrorMessage`
    /// (`source.LoadChangeset` failures). Repository / external-service /
    /// source-construction failures are returned before `SyncChangeset` and
    /// are *not* written (`syncer.go:478-510`).
    pub persist: bool,
}

impl SourceLoadError {
    pub fn load(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            persist: true,
        }
    }

    pub fn not_persisted(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            persist: false,
        }
    }
}

/// The code-host-owned columns written by
/// `Store.UpdateChangesetCodeHostState` (`changesetCodeHostStateInsertColumns`,
/// `internal/batches/store/changesets.go:174`).
///
/// `external_title` and `updated_at` are produced by the backend: the title is
/// computed from `metadata` with [`changeset_title`] (a title failure becomes
/// `NULL`, like `c.Title()`), and `updated_at` is the store's `now()`.
/// Values set by the caller for those two fields are ignored on input and
/// populated from the scanned `RETURNING` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetCodeHostState {
    pub external_service_type: String,
    pub metadata: Value,
    pub external_branch: Option<String>,
    pub external_fork_name: Option<String>,
    pub external_fork_namespace: Option<String>,
    pub external_deleted_at: Option<OffsetDateTime>,
    pub external_updated_at: Option<OffsetDateTime>,
    pub external_state: Option<String>,
    pub external_review_state: Option<String>,
    pub external_check_state: Option<String>,
    pub diff_stat_added: Option<i32>,
    pub diff_stat_deleted: Option<i32>,
    /// `c.SyncState` marshalled by Go; the zero value marshals to
    /// `{"BaseRefOid":"","HeadRefOid":"","IsComplete":false}`.
    pub sync_state: Value,
    pub syncer_error: Option<String>,
    /// Scanned `external_title` (ignored on input).
    pub external_title: Option<String>,
    /// Scanned `updated_at` (ignored on input).
    pub updated_at: Option<OffsetDateTime>,
}

impl Default for ChangesetCodeHostState {
    fn default() -> Self {
        Self {
            external_service_type: String::new(),
            metadata: json!({}),
            external_branch: None,
            external_fork_name: None,
            external_fork_namespace: None,
            external_deleted_at: None,
            external_updated_at: None,
            external_state: None,
            external_review_state: None,
            external_check_state: None,
            diff_stat_added: None,
            diff_stat_deleted: None,
            sync_state: json!({"BaseRefOid": "", "HeadRefOid": "", "IsComplete": false}),
            syncer_error: None,
            external_title: None,
            updated_at: None,
        }
    }
}

/// `UpdateChangesetCodeHostState` inputs.
#[derive(Debug, Clone, Copy)]
pub struct CodeHostStateUpdate<'a> {
    /// The refreshed 15-column state. `None` keeps the existing row values:
    /// used for load errors (`syncer_error` is written) and for
    /// `ChangesetNotFoundError` (`deleted` is set), exactly the columns the
    /// baseline touches before re-writing an otherwise unchanged changeset.
    pub state: Option<&'a ChangesetCodeHostState>,
    pub sync_error: Option<&'a str>,
    pub deleted: bool,
}

impl<'a> CodeHostStateUpdate<'a> {
    pub fn error(message: &'a str) -> Self {
        Self {
            state: None,
            sync_error: Some(message),
            deleted: false,
        }
    }

    pub fn loaded(state: &'a ChangesetCodeHostState, deleted: bool) -> Self {
        Self {
            state: Some(state),
            sync_error: None,
            deleted,
        }
    }

    pub fn deleted() -> Self {
        Self {
            state: None,
            sync_error: None,
            deleted: true,
        }
    }
}

/// Source-side of `syncer.SyncChangeset`: loads the changeset and returns its
/// derived events. Production implements this over `sg-extsvc` +
/// gitserver; the DB writes stay in [`ChangesetSyncBackend`].
#[async_trait]
pub trait ChangesetSourceLoader: Send + Sync {
    async fn load_changeset(&self, changeset_id: i64) -> Result<LoadedChangeset, SourceLoadError>;
}

/// Persistence side of `syncer.SyncChangeset`.
#[async_trait]
pub trait ChangesetSyncBackend: Send + Sync {
    /// `UpdateChangesetCodeHostState`: the full 15-column update, with the
    /// `RETURNING` row scanned back like `ScanChangeset`. Returns the scanned
    /// state (also for callers/tests), or `None` for the reduced updates.
    async fn update_changeset_code_host_state(
        &self,
        changeset_id: i64,
        update: CodeHostStateUpdate<'_>,
    ) -> Result<Option<ChangesetCodeHostState>, String>;
    /// `UpsertChangesetEvents`.
    async fn upsert_changeset_events(
        &self,
        changeset_id: i64,
        events: &[SyncedEvent],
    ) -> Result<(), String>;
    /// `state.SetDerivedState`, run in the same store transaction after the
    /// events are upserted (`syncer.go:538`). The shared implementation is
    /// [`sg_extsvc::batches_state::set_derived_state`]; a backend needs the
    /// changeset's code-host metadata plus its repo to call it. The default is
    /// a no-op so pure-persistence backends keep the previous behaviour; like
    /// the baseline, errors are logged rather than propagated.
    async fn set_derived_state(
        &self,
        _changeset_id: i64,
        _events: &[SyncedEvent],
    ) -> Result<(), String> {
        Ok(())
    }
}

/// `syncer.SyncChangeset`'s ordering (`syncer.go:516-554`), with the
/// changeset load and persistence injected.
///
/// * source-selection / repository failures (`persist == false`): return the
///   error without a database write;
/// * `LoadChangeset` failures (`persist == true`): record the error, then
///   return it;
/// * not found: mark deleted (when not already deleted);
/// * success: clear the error, update the 15 code-host columns, upsert
///   events, then recompute derived state
///   ([`ChangesetSyncBackend::set_derived_state`], whose default is a no-op).
///   As in the baseline, a derived-state failure is logged and does not fail
///   the sync.
pub async fn sync_changeset(
    loader: &dyn ChangesetSourceLoader,
    backend: &dyn ChangesetSyncBackend,
    changeset_id: i64,
) -> Result<(), String> {
    let loaded = match loader.load_changeset(changeset_id).await {
        Ok(loaded) => loaded,
        Err(err) => {
            if err.persist {
                backend
                    .update_changeset_code_host_state(
                        changeset_id,
                        CodeHostStateUpdate::error(&err.message),
                    )
                    .await?;
            }
            return Err(err.message);
        }
    };

    let update = match loaded.state.as_ref() {
        Some(state) => CodeHostStateUpdate::loaded(state, loaded.deleted),
        None => CodeHostStateUpdate::deleted(),
    };
    // Reset the syncer error message (or mark the changeset deleted).
    backend
        .update_changeset_code_host_state(changeset_id, update)
        .await?;
    backend
        .upsert_changeset_events(changeset_id, &loaded.events)
        .await?;
    // `state.SetDerivedState` logs its own errors and never fails the sync.
    let _ = backend
        .set_derived_state(changeset_id, &loaded.events)
        .await;
    Ok(())
}

/// `Changeset.Title()` (`internal/batches/types/changeset.go:494`).
///
/// The title is derived from the typed code-host metadata, stored in
/// `external_title` and is `NULL` when the field is missing/empty or the
/// external-service type is unknown (Go's `title, _ := c.Title()` plus
/// `dbutil.NullStringColumn`).
pub fn changeset_title(external_service_type: &str, metadata: &Value) -> Option<String> {
    let raw = match external_service_type {
        "github" => metadata.get("Title").and_then(Value::as_str),
        "gitlab" | "bitbucketServer" | "azuredevops" => {
            metadata.get("title").and_then(Value::as_str)
        }
        "bitbucketCloud" | "perforce" => metadata.get("Title").and_then(Value::as_str),
        "gerrit" => metadata
            .pointer("/change/subject")
            .and_then(Value::as_str)
            // `strings.Cut(subject, "\n")`: only the first line.
            .map(|subject| subject.split('\n').next().unwrap_or_default())
            // `TrimPrefix(TrimSuffix(title, "\""), "\"")`.
            .map(|title| {
                let title = title.strip_suffix('"').unwrap_or(title);
                title.strip_prefix('"').unwrap_or(title)
            }),
        _ => None,
    }?;
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_owned())
    }
}

/// `dbutil.NullStringColumn` + `NULLIF`-style semantics.
fn null_string(value: Option<&str>) -> Option<String> {
    match value {
        Some(value) if !value.is_empty() => Some(value.to_owned()),
        _ => None,
    }
}

/// Production [`ChangesetSyncBackend`] over the frontend database.
///
/// It performs the baseline's 15-column `UpdateChangesetCodeHostState` write
/// and the `state.SetDerivedState` outputs (via
/// [`sg_extsvc::batches_state_load::refresh_derived_state_pool`]).
pub struct PgChangesetSyncBackend {
    pool: PgPool,
}

impl PgChangesetSyncBackend {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Build the backend from an existing batches store.
    pub fn from_store(store: &sg_db::batches::BatchesStore) -> Self {
        Self::new(store.pool().clone())
    }
}

/// The `RETURNING ChangesetColumns` projection of the 15 written columns plus
/// `id`/`updated_at`.
const CODE_HOST_STATE_RETURNING: &str = r#"
  id, updated_at, metadata, external_branch, external_fork_name,
  external_fork_namespace, external_deleted_at, external_updated_at,
  external_state, external_review_state, external_check_state,
  diff_stat_added, diff_stat_deleted, sync_state, syncer_error, external_title
"#;

fn scan_code_host_state(row: &sqlx::postgres::PgRow, external_service_type: &str) -> Result<ChangesetCodeHostState, String> {
    use sqlx::Row;
    let metadata: Value = row.try_get("metadata").map_err(|err| err.to_string())?;
    let sync_state: Value = row.try_get("sync_state").map_err(|err| err.to_string())?;
    Ok(ChangesetCodeHostState {
        external_service_type: external_service_type.to_owned(),
        metadata,
        external_branch: row.try_get("external_branch").map_err(|err| err.to_string())?,
        external_fork_name: row
            .try_get("external_fork_name")
            .map_err(|err| err.to_string())?,
        external_fork_namespace: row
            .try_get("external_fork_namespace")
            .map_err(|err| err.to_string())?,
        external_deleted_at: row
            .try_get("external_deleted_at")
            .map_err(|err| err.to_string())?,
        external_updated_at: row
            .try_get("external_updated_at")
            .map_err(|err| err.to_string())?,
        external_state: row.try_get("external_state").map_err(|err| err.to_string())?,
        external_review_state: row
            .try_get("external_review_state")
            .map_err(|err| err.to_string())?,
        external_check_state: row
            .try_get("external_check_state")
            .map_err(|err| err.to_string())?,
        diff_stat_added: row.try_get("diff_stat_added").map_err(|err| err.to_string())?,
        diff_stat_deleted: row
            .try_get("diff_stat_deleted")
            .map_err(|err| err.to_string())?,
        sync_state,
        syncer_error: row.try_get("syncer_error").map_err(|err| err.to_string())?,
        external_title: row.try_get("external_title").map_err(|err| err.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
    })
}

#[async_trait]
impl ChangesetSyncBackend for PgChangesetSyncBackend {
    async fn update_changeset_code_host_state(
        &self,
        changeset_id: i64,
        update: CodeHostStateUpdate<'_>,
    ) -> Result<Option<ChangesetCodeHostState>, String> {
        let Some(state) = update.state else {
            // `UpdateChangesetCodeHostState` after a load error or a
            // `ChangesetNotFoundError` rewrites columns that still hold the
            // values just read from the same row; only `updated_at`,
            // `syncer_error` and the deleted marker can change.
            sqlx::query(
                r#"
UPDATE changesets
SET updated_at = clock_timestamp(),
    syncer_error = $2,
    external_deleted_at = CASE
        WHEN $3 THEN COALESCE(external_deleted_at, clock_timestamp())
        ELSE external_deleted_at
    END
WHERE id = $1
"#,
            )
            .bind(changeset_id)
            .bind(update.sync_error)
            .bind(update.deleted)
            .execute(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
            return Ok(None);
        };

        // `updateChangesetCodeHostStateQuery`: every value comes from the
        // changeset, the title is derived (failure -> NULL), and the row is
        // scanned back into the changeset.
        let metadata = serde_json::to_string(&state.metadata).map_err(|err| err.to_string())?;
        let sync_state =
            serde_json::to_string(&state.sync_state).map_err(|err| err.to_string())?;
        let title = changeset_title(&state.external_service_type, &state.metadata);
        let query = format!(
            r#"
UPDATE changesets
SET (updated_at, metadata, external_branch, external_fork_name,
     external_fork_namespace, external_deleted_at, external_updated_at,
     external_state, external_review_state, external_check_state,
     diff_stat_added, diff_stat_deleted, sync_state, syncer_error,
     external_title)
  = (clock_timestamp(), $2::jsonb, $3, $4, $5, $6, $7, $8, $9, $10,
     $11, $12, $13::jsonb, $14, $15)
WHERE id = $1
RETURNING {}
"#,
            CODE_HOST_STATE_RETURNING
        );
        let row = sqlx::query(&query)
            .bind(changeset_id)
            .bind(metadata)
            .bind(null_string(state.external_branch.as_deref()))
            .bind(null_string(state.external_fork_name.as_deref()))
            .bind(null_string(state.external_fork_namespace.as_deref()))
            .bind(state.external_deleted_at)
            .bind(state.external_updated_at)
            .bind(null_string(state.external_state.as_deref()))
            .bind(null_string(state.external_review_state.as_deref()))
            .bind(null_string(state.external_check_state.as_deref()))
            .bind(state.diff_stat_added)
            .bind(state.diff_stat_deleted)
            .bind(sync_state)
            .bind(null_string(state.syncer_error.as_deref()))
            .bind(null_string(title.as_deref()))
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
        let Some(row) = row else {
            return Ok(None);
        };
        scan_code_host_state(&row, &state.external_service_type).map(Some)
    }

    async fn upsert_changeset_events(
        &self,
        changeset_id: i64,
        events: &[SyncedEvent],
    ) -> Result<(), String> {
        // `UpsertChangesetEvents`: `CreatedAt` defaults to the store's now and
        // `UpdatedAt` to now when it is not after `CreatedAt`; the conflict
        // clause updates metadata and `updated_at` only.
        for event in events {
            sqlx::query(
                r#"
INSERT INTO changeset_events (changeset_id, kind, key, created_at, updated_at, metadata)
VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp(), $4::jsonb)
ON CONFLICT ON CONSTRAINT changeset_events_changeset_id_kind_key_unique
DO UPDATE SET metadata = excluded.metadata, updated_at = excluded.updated_at
"#,
            )
            .bind(changeset_id)
            .bind(&event.kind)
            .bind(&event.key)
            .bind(event.metadata.to_string())
            .execute(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    async fn set_derived_state(
        &self,
        changeset_id: i64,
        _events: &[SyncedEvent],
    ) -> Result<(), String> {
        // Gitserver stays injectable; the network-independent derived state
        // (check/external/review state, labels) is still computed and written.
        match sg_extsvc::batches_state_load::refresh_derived_state_pool(
            &self.pool,
            changeset_id,
            None,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::warn!(changeset_id, error = %err, "computing derived changeset state");
                Ok(())
            }
        }
    }
}

/// Selects and builds the code-host client for a changeset repository:
/// `sources.Sourcer.ForChangeset`'s external-service selection plus
/// `buildChangesetSource`. Injectable so tests can script a source without a
/// database or credentials.
#[async_trait]
pub trait ChangesetSourceFactory: Send + Sync {
    async fn source_for_repo(&self, repo: &Repo) -> Result<Arc<dyn ChangesetSource>, String>;
}

/// Production factory: `loadExternalService` over the repository's live
/// external services (newest first), decrypted with the external-service key
/// and built through [`sg_extsvc::batches_sources::new_source`].
pub struct PgChangesetSourceFactory {
    pool: PgPool,
    keyring: Arc<sg_keyring::Keyring>,
    http: Arc<dyn sg_extsvc::HttpDoer>,
    /// `github_apps`/`github_app_installs` store used when the runtime selects
    /// `GITHUB_APP`; `None` keeps the config/PAT connection.
    github_app_store: Option<Arc<dyn BatchesGitHubAppStore>>,
    /// Installation-token cache + HTTP doer for the App authenticator.
    github_app_seams: GitHubAppTokenSeams,
}

/// `github_apps.Store` adapter for runtime GitHub App authentication (syncer).
///
/// Reads the existing `github_apps`/`github_app_installs` rows and decrypts
/// the App private key with the site `GitHubAppKey`. No table/column/key is
/// added.
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
impl BatchesGitHubAppStore for PgBatchesGitHubAppStore {
    async fn get_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<sg_extsvc::batches_sources::BatchesGitHubApp, String> {
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
        Ok(sg_extsvc::batches_sources::BatchesGitHubApp {
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

/// An external service selected for a changeset repository
/// (`loadExternalService`'s result plus the resolved connection).
#[derive(Debug, Clone)]
pub struct SelectedExternalService {
    pub id: i64,
    pub kind: String,
    pub config: Value,
    pub connection: sg_extsvc::batches_sources::SourceConnection,
}

impl PgChangesetSourceFactory {
    pub fn new(
        pool: PgPool,
        keyring: Arc<sg_keyring::Keyring>,
        http: Arc<dyn sg_extsvc::HttpDoer>,
    ) -> Self {
        Self {
            pool,
            keyring,
            http: Arc::clone(&http),
            github_app_store: None,
            github_app_seams: GitHubAppTokenSeams::offline(http),
        }
    }

    /// Enable `GITHUB_APP` runtime authentication for this factory. The store
    /// reads the existing `github_apps`/`github_app_installs` rows.
    pub fn with_github_app_store(
        mut self,
        store: Arc<dyn BatchesGitHubAppStore>,
    ) -> Self {
        self.github_app_store = Some(store);
        self
    }

    /// Inject the installation-token cache (the baseline's Redis-backed
    /// `GitHubAppKey` cache); the HTTP doer defaults to the factory's.
    pub fn with_github_app_cache(
        mut self,
        cache: Arc<dyn sg_extsvc::auth::InstallationTokenCache>,
    ) -> Self {
        self.github_app_seams.cache = Some(cache);
        self
    }

    /// `loadExternalService`: the first live external service of the
    /// repository (newest first) whose config decodes into a batches
    /// connection schema.
    pub async fn select_external_service(
        &self,
        repo: &Repo,
    ) -> Result<SelectedExternalService, String> {
        let service_ids: HashSet<i64> = repo
            .sources
            .iter()
            .map(|source| source.external_service_id)
            .collect();
        let services = sg_db::external_services::ExternalServiceStore::new(self.pool.clone())
            .list()
            .await
            .map_err(|err| format!("loading external service: {err}"))?;

        // `ExternalServices().List` is `ORDER BY id DESC`; the first service
        // whose config decodes into one of the batches connection schemas
        // wins.
        for service in services {
            if !service_ids.contains(&service.id) {
                continue;
            }
            if !is_batches_source_kind(&service.kind) {
                // `Configuration` would not decode into one of the batches
                // connection prototypes (`OTHER`, package hosts, ...).
                continue;
            }
            let raw = sg_encryption::maybe_decrypt(
                self.keyring.external_service_key().as_deref(),
                &service.config,
                &service.encryption_key_id,
            )
            .await
            .map_err(|err| {
                format!(
                    "loading external service: external service id={} config error: {err}",
                    service.id
                )
            })?;
            let plain = sg_config::strip_jsonc(&raw);
            let config: Value = serde_json::from_str(&plain).map_err(|err| {
                format!(
                    "loading external service: external service id={} config error: {err}",
                    service.id
                )
            })?;
            // `sourcer.ForChangeset` with `AuthenticationStrategyUserCredential`:
            // keep the config-derived site/PAT connection, but route through
            // the M9.32b resolver so a `GITHUB_APP` runtime selection reuses
            // the same path.
            let connection = resolve_runtime_changeset_source_connection(
                RuntimeSourceRequest {
                    external_service_kind: &service.kind,
                    config: &config,
                    strategy: AUTHENTICATION_STRATEGY_USER_CREDENTIAL,
                    github_app_kind: None,
                    account: "",
                    as_non_credential: false,
                },
                self.github_app_store.as_deref(),
                self.github_app_seams.clone(),
            )
            .await
            .map_err(|err| format!("loading external service: {}", err.message))?;
            return Ok(SelectedExternalService {
                id: service.id,
                kind: service.kind,
                config,
                connection,
            });
        }

        Err("loading external service: no external services found".to_owned())
    }
}

/// `types.Repo` reduced to the fields the sources read (`metadata` is the
/// code-host repository object). GitHub's stored metadata is PascalCase
/// (`NameWithOwner`), while the ported source reads `name_with_owner`.
fn source_repo(repo: &Repo, kind: &str) -> SourceRepo {
    let metadata = match kind {
        "GITHUB" => {
            let mut metadata = repo.metadata.clone();
            if metadata.get("name_with_owner").is_none() {
                if let Some(name) = metadata.get("NameWithOwner").cloned() {
                    metadata["name_with_owner"] = name;
                }
            }
            metadata
        }
        _ => repo.metadata.clone(),
    };
    let sources = repo
        .sources
        .iter()
        .map(|source| {
            (
                source.clone_url.clone(),
                RepoSource {
                    id: source.external_service_id.to_string(),
                    clone_url: source.clone_url.clone(),
                },
            )
        })
        .collect();
    SourceRepo {
        id: repo.id,
        name: repo.name.clone(),
        external_repo: ExternalRepoSpec {
            id: repo.external_id.clone().unwrap_or_default(),
            service_type: repo.external_service_type.clone().unwrap_or_default(),
            service_id: repo.external_service_id.clone().unwrap_or_default(),
        },
        sources,
        metadata,
    }
}

#[async_trait]
impl ChangesetSourceFactory for PgChangesetSourceFactory {
    async fn source_for_repo(&self, repo: &Repo) -> Result<Arc<dyn ChangesetSource>, String> {
        let selected = self.select_external_service(repo).await?;
        // `buildChangesetSource`: its errors are returned unwrapped.
        new_source(
            &selected.connection.kind,
            &selected.connection.base_url,
            selected.connection.auth,
            Arc::clone(&self.http),
        )
        .map_err(|err| err.message)
    }
}

/// The concrete `changesets` row read before the code-host load.
struct SyncChangesetRow {
    repo_id: i32,
    external_id: String,
    external_service_type: String,
    metadata: Value,
    external_branch: Option<String>,
    external_fork_name: Option<String>,
    external_fork_namespace: Option<String>,
    external_deleted_at: Option<OffsetDateTime>,
    external_updated_at: Option<OffsetDateTime>,
    external_state: Option<String>,
    external_review_state: Option<String>,
    external_check_state: Option<String>,
    diff_stat_added: Option<i32>,
    diff_stat_deleted: Option<i32>,
    sync_state: Value,
    syncer_error: Option<String>,
}

/// The baseline's stored metadata is the concrete code-host type (for
/// `github.PullRequest` the untagged struct marshals PascalCase, for
/// `gitlab.MergeRequest` / `bitbucketserver.PullRequest` the JSON tags are
/// lowercase). `sg_extsvc` returns a normalized projection; rebuild the fields
/// the store, `state.SetDerivedState` and indexing read.
fn go_changeset_metadata(kind: &str, metadata: &SourceChangesetMetadata) -> Value {
    match kind {
        "github" => json!({
            "ID": metadata.external_id,
            "Number": metadata.number,
            "Title": metadata.title,
            "Body": metadata.body,
            "State": metadata.state,
            "URL": metadata.url,
            "HeadRefName": abbreviate_ref(&metadata.head_ref_name),
            "BaseRefName": abbreviate_ref(&metadata.base_ref_name),
            "HeadRefOid": metadata.head_ref_oid,
            "BaseRefOid": metadata.base_ref_oid,
            "IsDraft": metadata.is_draft,
            "Author": {"Login": metadata.author_login},
        }),
        "gitlab" => {
            let mut value = json!({
                "iid": metadata.number,
                "title": metadata.title,
                "description": metadata.body,
                "state": metadata.state,
                "web_url": metadata.url,
                "source_branch": metadata.head_ref_name,
                "target_branch": abbreviate_ref(&metadata.base_ref_name),
                "work_in_progress": metadata.is_draft,
                "author": {"username": metadata.author_login},
            });
            if !metadata.head_ref_oid.is_empty() || !metadata.base_ref_oid.is_empty() {
                value["diff_refs"] = json!({
                    "head_sha": metadata.head_ref_oid,
                    "base_sha": metadata.base_ref_oid,
                    "start_sha": "",
                });
            }
            value
        }
        "bitbucketServer" => json!({
            "id": metadata.number,
            "title": metadata.title,
            "description": metadata.body,
            "state": metadata.state,
            "fromRef": {
                "id": ensure_ref_prefix(&metadata.head_ref_name),
                "latestCommit": metadata.head_ref_oid,
            },
            "toRef": {
                "id": ensure_ref_prefix(&metadata.base_ref_name),
                "latestCommit": metadata.base_ref_oid,
            },
        }),
        _ => serde_json::to_value(metadata).unwrap_or(Value::Null),
    }
}

/// Production [`ChangesetSourceLoader`]:
/// `SyncChangeset`'s repository/source lookup followed by
/// `source.LoadChangeset`.
pub struct PgChangesetSourceLoader {
    pool: PgPool,
    factory: Arc<dyn ChangesetSourceFactory>,
}

impl PgChangesetSourceLoader {
    pub fn new(pool: PgPool, factory: Arc<dyn ChangesetSourceFactory>) -> Self {
        Self { pool, factory }
    }
}

#[async_trait]
impl ChangesetSourceLoader for PgChangesetSourceLoader {
    async fn load_changeset(&self, changeset_id: i64) -> Result<LoadedChangeset, SourceLoadError> {
        use sqlx::Row;

        let row = sqlx::query(
            r#"
SELECT id, repo_id, external_id, external_service_type, metadata,
       external_branch, external_fork_name, external_fork_namespace,
       external_deleted_at, external_updated_at, external_state,
       external_review_state, external_check_state, diff_stat_added,
       diff_stat_deleted, sync_state, syncer_error
FROM changesets
WHERE id = $1
"#,
        )
        .bind(changeset_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| SourceLoadError::not_persisted(err.to_string()))?;
        let Some(row) = row else {
            // The syncer's precondition already filtered missing/not-published
            // changesets; keep a defensive error for direct callers.
            return Err(SourceLoadError::not_persisted(format!(
                "changeset {changeset_id} not found"
            )));
        };
        let row = SyncChangesetRow {
            repo_id: row.try_get("repo_id").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_id: row.try_get("external_id").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_service_type: row.try_get("external_service_type").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            metadata: row.try_get("metadata").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_branch: row.try_get("external_branch").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_fork_name: row.try_get("external_fork_name").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_fork_namespace: row.try_get("external_fork_namespace").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_deleted_at: row.try_get("external_deleted_at").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_updated_at: row.try_get("external_updated_at").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_state: row.try_get("external_state").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_review_state: row.try_get("external_review_state").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            external_check_state: row.try_get("external_check_state").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            diff_stat_added: row.try_get("diff_stat_added").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            diff_stat_deleted: row.try_get("diff_stat_deleted").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            sync_state: row.try_get("sync_state").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
            syncer_error: row.try_get("syncer_error").map_err(|err| SourceLoadError::not_persisted(err.to_string()))?,
        };

        // `syncStore.Repos().Get` (`syncer.go:493`): returned unwrapped.
        let repo = RepoStore::new(self.pool.clone())
            .get_by_id(row.repo_id)
            .await
            .map_err(|err| SourceLoadError::not_persisted(err.to_string()))?
            .ok_or_else(|| {
                SourceLoadError::not_persisted(format!("repo not found: id={}", row.repo_id))
            })?;

        // `sources.NewSourcer(...).ForChangeset`: failures here abort before
        // `SyncChangeset`, so nothing is persisted.
        let source = self
            .factory
            .source_for_repo(&repo)
            .await
            .map_err(SourceLoadError::not_persisted)?;

        let mut source_changeset = sg_extsvc::batches_sources::SourceChangeset {
            target_repo: source_repo(&repo, &row.external_service_type),
            remote_repo: source_repo(&repo, &row.external_service_type),
            changeset: SourceChangesetInner {
                id: changeset_id,
                external_id: row.external_id.clone(),
                external_branch: row.external_branch.clone().unwrap_or_default(),
                external_fork_namespace: row
                    .external_fork_namespace
                    .clone()
                    .unwrap_or_default(),
                external_fork_name: row.external_fork_name.clone().unwrap_or_default(),
                metadata: row.metadata.clone(),
            },
            ..Default::default()
        };

        match source.load_changeset(&mut source_changeset).await {
            Err(err) if err.kind == SourceErrorKind::NotFound => {
                // `ChangesetNotFoundError`: `c.SetDeleted()` (unless already
                // deleted) and continue. The remaining columns are unchanged,
                // so the reduced update is equivalent to rewriting them.
                Ok(LoadedChangeset {
                    deleted: true,
                    ..Default::default()
                })
            }
            Err(err) => Err(SourceLoadError::load(err.message)),
            Ok(()) => {
                let normalized: SourceChangesetMetadata =
                    serde_json::from_value(source_changeset.changeset.metadata.clone())
                        .unwrap_or_default();
                let external_branch = null_string(Some(&normalized.head_ref_name))
                    .map(|branch| ensure_ref_prefix(&branch));
                // The normalized projection does not carry the code-host
                // `UpdatedAt` or fork-repository identity. Those columns keep
                // their stored values (documented seam); the source step owns
                // branch/title/metadata, and `SetDerivedState` rewrites
                // state/diffstat/sync-state afterwards.
                Ok(LoadedChangeset {
                    events: Vec::new(),
                    sync_error: None,
                    deleted: false,
                    state: Some(ChangesetCodeHostState {
                        external_service_type: row.external_service_type.clone(),
                        metadata: go_changeset_metadata(
                            &row.external_service_type,
                            &normalized,
                        ),
                        external_branch,
                        external_fork_name: row.external_fork_name,
                        external_fork_namespace: row.external_fork_namespace,
                        external_deleted_at: row.external_deleted_at,
                        external_updated_at: row.external_updated_at,
                        external_state: row.external_state,
                        external_review_state: row.external_review_state,
                        external_check_state: row.external_check_state,
                        diff_stat_added: row.diff_stat_added,
                        diff_stat_deleted: row.diff_stat_deleted,
                        sync_state: row.sync_state,
                        syncer_error: row.syncer_error,
                        external_title: None,
                        updated_at: None,
                    }),
                })
            }
        }
    }
}

/// [`ChangesetSyncFn`] wired to an injected code-host loader/backend.
pub struct SourceSyncFn {
    store: Arc<dyn ChangesetSyncStore>,
    loader: Arc<dyn ChangesetSourceLoader>,
    backend: Arc<dyn ChangesetSyncBackend>,
}

impl SourceSyncFn {
    pub fn new(
        store: Arc<dyn ChangesetSyncStore>,
        loader: Arc<dyn ChangesetSourceLoader>,
        backend: Arc<dyn ChangesetSyncBackend>,
    ) -> Self {
        Self {
            store,
            loader,
            backend,
        }
    }
}

#[async_trait]
impl ChangesetSyncFn for SourceSyncFn {
    async fn sync(&self, id: i64) -> Result<(), String> {
        // `GetChangeset` precondition: `ErrNoResults` is a no-op.
        if self.store.get_changeset_for_sync(id).await?.is_none() {
            return Ok(());
        }
        sync_changeset(self.loader.as_ref(), self.backend.as_ref(), id).await
    }
}

/// `batches/syncer.SyncRegistry`.
pub struct SyncRegistry {
    store: Arc<dyn ChangesetSyncStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<ChangesetSyncerMetrics>,
    sync_fn: Arc<dyn ChangesetSyncFn>,
    live: Arc<LiveConfig>,
    priority_notify: mpsc::Sender<Vec<i64>>,
    syncers: Mutex<HashMap<String, Arc<SyncerHandle>>>,
    tasks: TaskTracker,
}

struct SyncerHandle {
    priority_notify: mpsc::Sender<Vec<i64>>,
    cancel: CancellationToken,
}

impl SyncRegistry {
    /// `NewSyncRegistry`. The receiver of the registry's priority channel is
    /// returned so the caller can start [`SyncRegistry::spawn`].
    pub fn new(
        store: Arc<dyn ChangesetSyncStore>,
        clock: Arc<dyn Clock>,
        metrics: Arc<ChangesetSyncerMetrics>,
        sync_fn: Arc<dyn ChangesetSyncFn>,
        live: Arc<LiveConfig>,
        tasks: TaskTracker,
    ) -> (Self, mpsc::Receiver<Vec<i64>>) {
        let (priority_notify, priority_rx) = mpsc::channel(PRIORITY_NOTIFY_CAPACITY);
        (
            Self {
                sync_fn,
                store,
                clock,
                metrics,
                live,
                priority_notify,
                syncers: Mutex::new(HashMap::new()),
                tasks,
            },
            priority_rx,
        )
    }

    /// Replace [`StoreSyncFn`] (tests).
    pub fn with_sync_fn(mut self, sync_fn: Arc<dyn ChangesetSyncFn>) -> Self {
        self.sync_fn = sync_fn;
        self
    }

    /// Start the registry: initial `syncCodeHosts`, the priority dispatcher
    /// and the one-minute `syncCodeHosts` loop. Per-host syncers are spawned
    /// on the same tracker.
    pub fn spawn(
        self: &Arc<Self>,
        priority_rx: mpsc::Receiver<Vec<i64>>,
        shutdown: CancellationToken,
    ) {
        let loop_shutdown = shutdown.clone();
        let registry = Arc::clone(self);
        self.tasks.spawn(async move {
            if let Err(err) = registry.sync_code_hosts().await {
                tracing::error!(error = %err, "Fetching initial list of code hosts");
            }
            registry
                .run_priority_dispatcher(priority_rx, shutdown.clone())
                .await
        });
        let registry = Arc::clone(self);
        self.tasks
            .spawn(async move { registry.run_sync_code_hosts_loop(loop_shutdown).await });
    }

    fn syncers_snapshot(&self) -> HashMap<String, Arc<SyncerHandle>> {
        self.syncers
            .lock()
            .expect("changeset syncers mutex")
            .iter()
            .map(|(key, handle)| (key.clone(), Arc::clone(handle)))
            .collect()
    }
}

#[async_trait]
impl ChangesetSyncEnqueuer for SyncRegistry {
    /// `EnqueueChangesetSyncs`: non-blocking; a full channel reports
    /// `high priority sync capacity reached`.
    async fn enqueue_changeset_syncs(&self, ids: Vec<i64>) -> Result<(), String> {
        match self.priority_notify.try_send(ids) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) | Err(mpsc::error::TrySendError::Closed(_)) => {
                Err("high priority sync capacity reached".to_owned())
            }
        }
    }
}

impl SyncRegistry {
    /// `EnqueueChangesetSyncsForRepos`: queue a sync for every changeset of
    /// the given repos.
    pub async fn enqueue_changeset_syncs_for_repos(&self, repo_ids: &[i32]) -> Result<(), String> {
        let ids = self
            .store
            .list_changeset_ids_for_repos(repo_ids)
            .await
            .map_err(|err| format!("listing changesets for repos {}: {err}", go_ints(repo_ids)))?;
        if ids.is_empty() {
            return Ok(());
        }
        tracing::debug!(
            repo_count = repo_ids.len(),
            changeset_count = ids.len(),
            "enqueuing syncs for changesets on repos"
        );
        self.enqueue_changeset_syncs(ids).await
    }

    /// `handlePriorityItems`: fetch the sync data for the enqueued ids and
    /// forward them to the matching host's syncer.
    async fn run_priority_dispatcher(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<Vec<i64>>,
        shutdown: CancellationToken,
    ) {
        loop {
            let ids = tokio::select! {
                _ = shutdown.cancelled() => return,
                received = rx.recv() => match received {
                    Some(ids) => ids,
                    None => return,
                },
            };

            let sync_data = match tokio::time::timeout(
                PRIORITY_FETCH_TIMEOUT,
                self.store.list_changeset_sync_data(&ids, ""),
            )
            .await
            {
                Ok(Ok(sync_data)) => sync_data,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "Fetching sync data");
                    continue;
                }
                Err(_) => {
                    tracing::error!(error = "context deadline exceeded", "Fetching sync data");
                    continue;
                }
            };

            // Group changesets by code host.
            let mut by_host: HashMap<String, Vec<i64>> = HashMap::new();
            for data in sync_data {
                by_host
                    .entry(data.repo_external_service_id)
                    .or_default()
                    .push(data.changeset_id);
            }

            let syncers = self.syncers_snapshot();
            for (host, changesets) in by_host {
                let Some(syncer) = syncers.get(&host) else {
                    continue;
                };
                // Non-blocking: a full per-host channel drops the batch.
                let _ = syncer.priority_notify.try_send(changesets);
            }
        }
    }

    /// `syncCodeHosts`: add syncers for new code hosts and stop the ones whose
    /// host disappeared.
    pub async fn sync_code_hosts(&self) -> Result<(), String> {
        let include_perforce = self.live.current().site.batch_changes_enable_perforce();
        let code_hosts = self
            .store
            .list_code_hosts(&[], false, include_perforce)
            .await
            .map_err(|err| err.to_string())?;

        let mut by_external_service_id: HashMap<String, CodeHost> =
            HashMap::with_capacity(code_hosts.len());
        for host in code_hosts {
            by_external_service_id.insert(host.external_service_id.clone(), host.clone());
            self.add_code_host_syncer(&host, include_perforce);
        }

        let mut syncers = self.syncers.lock().expect("changeset syncers mutex");
        syncers.retain(|key, handle| {
            if by_external_service_id.contains_key(key) {
                true
            } else {
                // Cancel the syncer; its loop removes itself from scheduling.
                handle.cancel.cancel();
                false
            }
        });
        Ok(())
    }

    /// `addCodeHostSyncer`.
    fn add_code_host_syncer(&self, code_host: &CodeHost, include_perforce: bool) {
        if !code_host.is_supported(include_perforce) {
            tracing::info!(
                r#type = %code_host.external_service_type,
                url = %code_host.external_service_id,
                "Code host not supported by batch changes"
            );
            return;
        }

        let syncer_key = code_host.external_service_id.clone();
        let mut syncers = self.syncers.lock().expect("changeset syncers mutex");
        if syncers.contains_key(&syncer_key) {
            return;
        }

        let (priority_notify, priority_rx) = mpsc::channel(PRIORITY_NOTIFY_CAPACITY);
        let cancel = CancellationToken::new();
        syncers.insert(
            syncer_key.clone(),
            Arc::new(SyncerHandle {
                priority_notify,
                cancel: cancel.clone(),
            }),
        );
        drop(syncers);

        let syncer = Arc::new(ChangesetSyncer {
            code_host_url: syncer_key,
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            metrics: Arc::clone(&self.metrics),
            sync_fn: Arc::clone(&self.sync_fn),
            live: Arc::clone(&self.live),
        });
        self.tasks
            .spawn(async move { syncer.run(priority_rx, cancel).await });
    }

    /// `goroutine.NewPeriodicGoroutine(..., WithInterval(1m))` around
    /// `syncCodeHosts`.
    async fn run_sync_code_hosts_loop(self: Arc<Self>, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + EXTERNAL_SERVICE_SYNCER_INTERVAL,
            EXTERNAL_SERVICE_SYNCER_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(err) = self.sync_code_hosts().await {
                        tracing::error!(error = %err, "Batch Changes syncer external service sync");
                    }
                }
            }
        }
    }
}

/// `changesetSyncer`: periodically syncs the metadata of one code host's
/// changesets.
struct ChangesetSyncer {
    code_host_url: String,
    store: Arc<dyn ChangesetSyncStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<ChangesetSyncerMetrics>,
    sync_fn: Arc<dyn ChangesetSyncFn>,
    live: Arc<LiveConfig>,
}

impl ChangesetSyncer {
    /// `(*changesetSyncer).Run`.
    async fn run(
        self: Arc<Self>,
        mut priority_notify: mpsc::Receiver<Vec<i64>>,
        cancel: CancellationToken,
    ) {
        tracing::debug!("Starting changeset syncer");

        let mut queue = ChangesetPriorityQueue::new();
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + DEFAULT_CHANGESET_SCHEDULE_INTERVAL,
            DEFAULT_CHANGESET_SCHEDULE_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        if !self.live.current().site.disable_auto_code_host_syncs() {
            match self.compute_schedule().await {
                Ok(schedule) => queue.upsert(schedule),
                // Non-fatal: the main loop retries on the next tick.
                Err(err) => tracing::error!(error = %err, "Computing schedule"),
            }
        }

        loop {
            let next = queue.peek().cloned();
            let delay = next.as_ref().map(|item| match item.priority {
                SyncPriority::High => Duration::ZERO,
                SyncPriority::Normal => item
                    .next_sync
                    .map(|next_sync| {
                        next_sync
                            .duration_since(SystemTime::now())
                            .unwrap_or(Duration::ZERO)
                    })
                    .unwrap_or(Duration::ZERO),
            });

            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    if self.live.current().site.disable_auto_code_host_syncs() {
                        continue;
                    }
                    let start = self.clock.now();
                    let computed = self.compute_schedule().await;
                    let success = computed.is_ok();
                    self.metrics
                        .compute_schedule_duration_seconds
                        .with_label_values(&[self.code_host_url.as_str(), bool_label(success)])
                        .observe(duration_secs(self.clock.now().duration_since(start).unwrap_or(Duration::ZERO)));
                    let schedule = match computed {
                        Ok(schedule) => schedule,
                        Err(err) => {
                            tracing::error!(error = %err, "Computing queue");
                            continue;
                        }
                    };
                    self.metrics
                        .schedule_size
                        .with_label_values(&[self.code_host_url.as_str()])
                        .set(schedule.len() as f64);
                    let now = self.clock.now();
                    let behind = schedule
                        .iter()
                        .filter(|item| item.next_sync.is_some_and(|next_sync| next_sync < now))
                        .count();
                    queue.upsert(schedule);
                    self.metrics
                        .behind_schedule
                        .with_label_values(&[self.code_host_url.as_str()])
                        .set(behind as f64);
                }
                _ = sleep_for(delay) => {
                    let Some(next) = next else { continue };
                    let start = self.clock.now();
                    let result = self.sync_fn.sync(next.changeset_id).await;
                    let success = result.is_ok();
                    let labels = [self.code_host_url.as_str(), bool_label(success)];
                    self.metrics
                        .sync_duration_seconds
                        .with_label_values(&labels)
                        .observe(duration_secs(self.clock.now().duration_since(start).unwrap_or(Duration::ZERO)));
                    self.metrics.syncs.with_label_values(&labels).inc();
                    if let Err(err) = result {
                        tracing::warn!(
                            changesetID = next.changeset_id,
                            error = %err,
                            "Syncing changeset"
                        );
                    }
                    // Remove the item now that it has been processed.
                    queue.remove(next.changeset_id);
                    self.metrics
                        .schedule_size
                        .with_label_values(&[self.code_host_url.as_str()])
                        .dec();
                }
                received = priority_notify.recv() => {
                    let Some(ids) = received else { return };
                    let count = ids.len();
                    for id in ids {
                        let mut item = queue.get(id).unwrap_or(ScheduledSync {
                            changeset_id: id,
                            next_sync: None,
                            priority: SyncPriority::High,
                        });
                        item.priority = SyncPriority::High;
                        queue.upsert([item]);
                        self.metrics
                            .schedule_size
                            .with_label_values(&[self.code_host_url.as_str()])
                            .inc();
                    }
                    self.metrics
                        .priority_queued
                        .with_label_values(&[self.code_host_url.as_str()])
                        .inc_by(count as f64);
                }
            }
        }
    }

    /// `computeSchedule`: `ListChangesetSyncData` for this code host, mapped
    /// through [`next_sync`].
    async fn compute_schedule(&self) -> Result<Vec<ScheduledSync>, String> {
        let sync_data = self
            .store
            .list_changeset_sync_data(&[], &self.code_host_url)
            .await
            .map_err(|err| format!("listing changeset sync data: {err}"))?;

        let now = self.clock.now();
        Ok(sync_data
            .iter()
            .map(|data| ScheduledSync {
                changeset_id: data.changeset_id,
                next_sync: Some(next_sync(now, data)),
                priority: SyncPriority::Normal,
            })
            .collect())
    }
}

async fn sleep_for(delay: Option<Duration>) {
    match delay {
        Some(delay) => tokio::time::sleep(delay).await,
        None => std::future::pending::<()>().await,
    }
}

fn duration_secs(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

fn bool_label(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

/// Go's `%v` for `[]api.RepoID`: `[1 2 3]`.
fn go_ints(values: &[i32]) -> String {
    let mut out = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    use crate::scheduler::SystemClock;

    fn sync_data(
        id: i64,
        updated_at: SystemTime,
        latest_event: Option<SystemTime>,
        external_updated_at: Option<SystemTime>,
    ) -> ChangesetSyncData {
        ChangesetSyncData {
            changeset_id: id,
            updated_at: sg_db::system_time_to_offset(updated_at),
            latest_event: latest_event.map(sg_db::system_time_to_offset),
            external_updated_at: external_updated_at.map(sg_db::system_time_to_offset),
            repo_external_service_id: "https://example.com".to_owned(),
        }
    }

    fn at(secs: i64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64)
    }

    #[test]
    fn priority_queue_orders_by_priority_then_next_sync_then_id() {
        let mut queue = ChangesetPriorityQueue::new();
        queue.upsert([
            ScheduledSync {
                changeset_id: 3,
                next_sync: Some(at(300)),
                priority: SyncPriority::Normal,
            },
            ScheduledSync {
                changeset_id: 1,
                next_sync: Some(at(200)),
                priority: SyncPriority::Normal,
            },
            ScheduledSync {
                changeset_id: 2,
                next_sync: Some(at(100)),
                priority: SyncPriority::High,
            },
            ScheduledSync {
                changeset_id: 4,
                next_sync: None,
                priority: SyncPriority::High,
            },
        ]);

        let ids: Vec<i64> = std::iter::from_fn(|| {
            let next = queue.peek().map(|item| item.changeset_id);
            if let Some(id) = next {
                queue.remove(id);
            }
            next
        })
        .collect();
        // High priority first (zero time before 100), then normal by time.
        assert_eq!(ids, vec![4, 2, 1, 3]);
        assert!(queue.is_empty());
    }

    #[test]
    fn upsert_preserves_high_priority_across_reschedules() {
        let mut queue = ChangesetPriorityQueue::new();
        queue.upsert([ScheduledSync {
            changeset_id: 7,
            next_sync: Some(at(1000)),
            priority: SyncPriority::Normal,
        }]);
        queue.upsert([ScheduledSync {
            changeset_id: 7,
            next_sync: None,
            priority: SyncPriority::High,
        }]);
        // A schedule recomputation sends a normal item; the stored item stays
        // high priority but takes the new time.
        queue.upsert([ScheduledSync {
            changeset_id: 7,
            next_sync: Some(at(50)),
            priority: SyncPriority::Normal,
        }]);
        let item = queue.get(7).expect("item");
        assert_eq!(item.priority, SyncPriority::High);
        assert_eq!(item.next_sync, Some(at(50)));
    }

    #[test]
    fn next_sync_uses_linear_backoff_between_min_and_max() {
        // Event 10 minutes before the last sync: the 10 minute gap is the
        // delay, so the next sync is 600s after the last one.
        let now = at(10_000);
        let data = sync_data(1, at(3_000), Some(at(2_400)), None);
        assert_eq!(next_sync(now, &data), at(3_600));

        // A zero gap saturates at maxSyncDelay (external_updated_at is NULL,
        // which Go treats as the zero time).
        let data = sync_data(1, at(3_000), Some(at(3_000)), None);
        assert_eq!(next_sync(now, &data), at(3_000) + MAX_SYNC_DELAY);

        // A gap above the maximum is clamped.
        let data = sync_data(1, at(100_000), Some(at(0)), None);
        assert_eq!(next_sync(now, &data), at(100_000) + MAX_SYNC_DELAY);
    }

    #[test]
    fn next_sync_waits_for_a_quiet_period_after_a_webhook() {
        // Change after the last sync: wait minSyncDelay after the change.
        let now = at(10_000);
        let data = sync_data(1, at(5_000), Some(at(5_100)), Some(at(5_100)));
        assert_eq!(next_sync(now, &data), at(5_100) + MIN_SYNC_DELAY);
    }

    #[test]
    fn next_sync_ignores_a_small_event_diff_and_uses_external_updated() {
        // The event diff is below minSyncDelay, so the external update time
        // decides; it is a long time ago, so the max delay applies.
        let now = at(100_000);
        let data = sync_data(1, at(50_000), Some(at(50_000 + 60)), Some(at(0)));
        assert_eq!(next_sync(now, &data), at(50_000) + MAX_SYNC_DELAY);
    }

    struct FakeStore {
        code_hosts: Vec<CodeHost>,
        sync_data: Vec<ChangesetSyncData>,
        changeset_ids: StdMutex<Vec<i64>>,
        changeset_repo: Option<i32>,
        calls: StdMutex<Vec<String>>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self {
                code_hosts: Vec::new(),
                sync_data: Vec::new(),
                changeset_ids: StdMutex::new(Vec::new()),
                changeset_repo: None,
                calls: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ChangesetSyncStore for FakeStore {
        async fn list_code_hosts(
            &self,
            _repo_ids: &[i32],
            _only_without_webhooks: bool,
            _include_perforce: bool,
        ) -> Result<Vec<CodeHost>, String> {
            self.calls
                .lock()
                .unwrap()
                .push("list_code_hosts".to_owned());
            Ok(self.code_hosts.clone())
        }

        async fn list_changeset_sync_data(
            &self,
            changeset_ids: &[i64],
            external_service_id: &str,
        ) -> Result<Vec<ChangesetSyncData>, String> {
            self.calls.lock().unwrap().push(format!(
                "list_changeset_sync_data {changeset_ids:?} {external_service_id}"
            ));
            Ok(self.sync_data.clone())
        }

        async fn list_changeset_ids_for_repos(
            &self,
            _repo_ids: &[i32],
        ) -> Result<Vec<i64>, String> {
            Ok(self.changeset_ids.lock().unwrap().clone())
        }

        async fn get_changeset_for_sync(&self, _id: i64) -> Result<Option<i32>, String> {
            Ok(self.changeset_repo)
        }
    }

    struct CountingSyncFn {
        ids: StdMutex<Vec<i64>>,
    }

    #[async_trait]
    impl ChangesetSyncFn for CountingSyncFn {
        async fn sync(&self, id: i64) -> Result<(), String> {
            self.ids.lock().unwrap().push(id);
            Ok(())
        }
    }

    fn test_registry(store: Arc<FakeStore>) -> (Arc<SyncRegistry>, mpsc::Receiver<Vec<i64>>) {
        let unified =
            sg_config::Unified::parse(&sg_config::RawUnified::default()).expect("site config");
        let live = Arc::new(LiveConfig::new(unified));
        let sync_fn = Arc::new(StoreSyncFn::new(Arc::clone(&store) as Arc<dyn ChangesetSyncStore>));
        let (registry, rx) = SyncRegistry::new(
            store,
            Arc::new(SystemClock),
            Arc::new(ChangesetSyncerMetrics::new()),
            sync_fn,
            live,
            TaskTracker::new(),
        );
        (Arc::new(registry), rx)
    }

    #[tokio::test]
    async fn enqueue_reports_capacity_once_the_channel_is_full() {
        let store = Arc::new(FakeStore::new());
        let (registry, rx) = test_registry(store);
        for _ in 0..PRIORITY_NOTIFY_CAPACITY {
            registry
                .enqueue_changeset_syncs(vec![1])
                .await
                .expect("capacity");
        }
        let err = registry
            .enqueue_changeset_syncs(vec![2])
            .await
            .expect_err("full");
        assert_eq!(err, "high priority sync capacity reached");
        drop(rx);
    }

    #[tokio::test]
    async fn enqueue_for_repos_lists_changesets_and_skips_empty_results() {
        let store = FakeStore::new();
        *store.changeset_ids.lock().unwrap() = vec![5, 6];
        let store = Arc::new(store);
        let (registry, mut rx) = test_registry(Arc::clone(&store));
        registry
            .enqueue_changeset_syncs_for_repos(&[1, 2])
            .await
            .expect("enqueue");
        assert_eq!(rx.try_recv().expect("queued"), vec![5, 6]);

        store.changeset_ids.lock().unwrap().clear();
        registry
            .enqueue_changeset_syncs_for_repos(&[])
            .await
            .expect("no changesets");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sync_code_hosts_adds_only_supported_hosts() {
        let mut store = FakeStore::new();
        store.code_hosts = vec![
            CodeHost {
                external_service_type: "github".to_owned(),
                external_service_id: "https://github.example.com".to_owned(),
                requires_ssh: false,
                has_webhooks: true,
            },
            CodeHost {
                external_service_type: "other".to_owned(),
                external_service_id: "file:///tmp/x".to_owned(),
                requires_ssh: false,
                has_webhooks: false,
            },
        ];
        let store = Arc::new(store);
        let (registry, _rx) = test_registry(Arc::clone(&store));
        registry.sync_code_hosts().await.expect("sync");
        assert_eq!(registry.syncers_snapshot().len(), 1);
    }

    #[tokio::test]
    async fn dispatcher_routes_priority_items_to_the_matching_host() {
        let mut store = FakeStore::new();
        store.code_hosts = vec![CodeHost {
            external_service_type: "github".to_owned(),
            external_service_id: "https://github.example.com".to_owned(),
            requires_ssh: false,
            has_webhooks: false,
        }];
        // A fresh `updated_at` puts the initial schedule 8h in the future, so
        // only the high-priority enqueue below reaches the sync function.
        store.sync_data = vec![ChangesetSyncData {
            changeset_id: 42,
            updated_at: sg_db::system_time_to_offset(SystemTime::now()),
            latest_event: None,
            external_updated_at: None,
            repo_external_service_id: "https://github.example.com".to_owned(),
        }];
        let sync_fn = Arc::new(CountingSyncFn {
            ids: StdMutex::new(Vec::new()),
        });
        let store = Arc::new(store);
        let unified =
            sg_config::Unified::parse(&sg_config::RawUnified::default()).expect("site config");
        let live = Arc::new(LiveConfig::new(unified));
        let metrics = Arc::new(ChangesetSyncerMetrics::new());
        let tasks = TaskTracker::new();
        let placeholder: Arc<dyn ChangesetSyncFn> =
            Arc::new(StoreSyncFn::new(Arc::clone(&store) as Arc<dyn ChangesetSyncStore>));
        let (registry, priority_rx) = SyncRegistry::new(
            store,
            Arc::new(SystemClock),
            metrics,
            placeholder,
            live,
            tasks.clone(),
        );
        let registry =
            Arc::new(registry.with_sync_fn(Arc::clone(&sync_fn) as Arc<dyn ChangesetSyncFn>));

        let shutdown = CancellationToken::new();
        // `spawn` also starts the one-minute loop; cancel it promptly.
        registry.spawn(priority_rx, shutdown.clone());
        registry.sync_code_hosts().await.expect("sync code hosts");

        registry
            .enqueue_changeset_syncs(vec![42])
            .await
            .expect("enqueue");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sync_fn.ids.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*sync_fn.ids.lock().unwrap(), vec![42]);

        shutdown.cancel();
        tasks.close();
        let _ = tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await;
    }

    #[test]
    fn changeset_title_matches_the_typed_metadata_rules() {
        assert_eq!(
            changeset_title("github", &json!({"Title": "Fix it"})),
            Some("Fix it".to_owned())
        );
        assert_eq!(
            changeset_title("gitlab", &json!({"title": "MR title"})),
            Some("MR title".to_owned())
        );
        assert_eq!(
            changeset_title("bitbucketServer", &json!({"title": "PR title"})),
            Some("PR title".to_owned())
        );
        assert_eq!(
            changeset_title("bitbucketCloud", &json!({"Title": "Cloud PR"})),
            Some("Cloud PR".to_owned())
        );
        assert_eq!(
            changeset_title("perforce", &json!({"Title": "CL"})),
            Some("CL".to_owned())
        );
        // `strings.Cut` keeps only the first line; surrounding quotes from the
        // commit message are trimmed (`TrimSuffix` before `TrimPrefix`).
        assert_eq!(
            changeset_title(
                "gerrit",
                &json!({"change": {"subject": "\"Fix it\"\n\nBody text"}})
            ),
            Some("Fix it".to_owned())
        );
        // Empty / missing / unknown types are stored as NULL.
        assert_eq!(changeset_title("github", &json!({"Title": ""})), None);
        assert_eq!(changeset_title("github", &json!({})), None);
        assert_eq!(changeset_title("other", &json!({"Title": "x"})), None);
    }

    #[test]
    fn go_metadata_projection_keeps_the_fields_the_store_reads() {
        let metadata = SourceChangesetMetadata {
            kind: "github".to_owned(),
            external_id: "123".to_owned(),
            title: "Title".to_owned(),
            body: "Body".to_owned(),
            state: "OPEN".to_owned(),
            url: "https://github.example.com/o/r/pull/123".to_owned(),
            head_ref_name: "feature".to_owned(),
            base_ref_name: "refs/heads/main".to_owned(),
            head_ref_oid: "headsha".to_owned(),
            base_ref_oid: "basesha".to_owned(),
            number: 123,
            is_draft: false,
            author_login: "alice".to_owned(),
        };
        let value = go_changeset_metadata("github", &metadata);
        assert_eq!(value["Title"], json!("Title"));
        assert_eq!(value["HeadRefName"], json!("feature"));
        assert_eq!(value["BaseRefName"], json!("main"));
        assert_eq!(value["HeadRefOid"], json!("headsha"));
        assert_eq!(value["Number"], json!(123));
        assert_eq!(value["Author"]["Login"], json!("alice"));

        let value = go_changeset_metadata("gitlab", &metadata);
        assert_eq!(value["target_branch"], json!("main"));
        assert_eq!(value["source_branch"], json!("feature"));
    }

    struct ScriptedLoader {
        result: StdMutex<Option<Result<LoadedChangeset, SourceLoadError>>>,
    }

    #[async_trait]
    impl ChangesetSourceLoader for ScriptedLoader {
        async fn load_changeset(
            &self,
            _changeset_id: i64,
        ) -> Result<LoadedChangeset, SourceLoadError> {
            self.result
                .lock()
                .unwrap()
                .take()
                .expect("one load per test")
        }
    }

    #[derive(Default)]
    struct RecordingBackend {
        updates: StdMutex<Vec<(i64, bool, Option<String>)>>,
    }

    #[async_trait]
    impl ChangesetSyncBackend for RecordingBackend {
        async fn update_changeset_code_host_state(
            &self,
            changeset_id: i64,
            update: CodeHostStateUpdate<'_>,
        ) -> Result<Option<ChangesetCodeHostState>, String> {
            self.updates.lock().unwrap().push((
                changeset_id,
                update.state.is_some(),
                update.sync_error.map(str::to_owned),
            ));
            Ok(update.state.cloned())
        }

        async fn upsert_changeset_events(
            &self,
            _changeset_id: i64,
            _events: &[SyncedEvent],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn sync_changeset_persists_only_load_errors() {
        let loader = ScriptedLoader {
            result: StdMutex::new(Some(Err(SourceLoadError::load("code host exploded")))),
        };
        let backend = RecordingBackend::default();
        let err = sync_changeset(&loader, &backend, 1).await.expect_err("error");
        assert_eq!(err, "code host exploded");
        assert_eq!(
            *backend.updates.lock().unwrap(),
            vec![(1, false, Some("code host exploded".to_owned()))]
        );

        // Source-selection failures return before `SyncChangeset` and are not
        // written (`syncer.go:478-510`).
        let loader = ScriptedLoader {
            result: StdMutex::new(Some(Err(SourceLoadError::not_persisted(
                "loading external service: no external services found",
            )))),
        };
        let backend = RecordingBackend::default();
        let err = sync_changeset(&loader, &backend, 2).await.expect_err("error");
        assert_eq!(
            err,
            "loading external service: no external services found"
        );
        assert!(backend.updates.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sync_changeset_marks_not_found_changesets_deleted() {
        let loader = ScriptedLoader {
            result: StdMutex::new(Some(Ok(LoadedChangeset {
                deleted: true,
                ..Default::default()
            }))),
        };
        let backend = RecordingBackend::default();
        sync_changeset(&loader, &backend, 3).await.expect("sync");
        // The reduced update leaves the other columns untouched.
        assert_eq!(*backend.updates.lock().unwrap(), vec![(3, false, None)]);
    }
}
