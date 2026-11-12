#![forbid(unsafe_code)]

//! Port of `internal/repos/syncer.go`: external service source syncing.
//!
//! One sync streams the repositories a source yields, upserts each of them in
//! its own transaction (so very large syncs persist incrementally), then
//! soft-deletes the repositories the external service owns but no longer
//! sources. Every write is followed by a `types.RepoSyncDiff` on the
//! `Synced` channel that the update scheduler consumes.
//!
//! Divergences from the baseline, all documented in `docs`:
//!
//! - Source errors are plain strings. The baseline classifies warnings,
//!   `ErrUnauthorized`/`ErrForbidden`/`ErrAccountSuspended` and license
//!   errors; without those types any error aborts the delete phase, which is
//!   what the baseline does for non-warning plain errors.
//! - The license hooks (`CreateRepoLicenseHook`/`UpdateRepoLicenseHook`) run
//!   against the license key in the live site configuration, like the
//!   baseline; with no key configured the free plan's one-private-repository
//!   limit applies. `licensing.IsLicenseValid()`'s Redis flag is not read.
//! - `dotcom.SourcegraphDotComMode()` is always false, so the private-repo
//!   guard never fires.
//! - `Synced` is a `tokio::sync::broadcast` channel rather than Go's blocking
//!   send: publishing never stalls a sync, and a diff published before the
//!   relay task subscribes is dropped (the relay is started before any sync).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use sg_db::external_service_repos::{
    ExternalRepoSpec as StoredSpec, ExternalServiceReposStore, RepoWrite, StoredRepo,
};
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_extsvc::{HttpGetter, ModifiedRepo, Repo, RepoSyncDiff, Source, SourceInfo, SourceService};
use sg_keyring::Keyring;
use sg_licensing::PrivateRepositories;

use crate::metrics::{sync_error_reason, Metrics, OWNER_SITE, OWNER_UNDEFINED};
use crate::scheduler::Clock;

/// `calcSyncInterval`'s `maxSyncInterval`.
pub const MAX_SYNC_INTERVAL: Duration = Duration::from_secs(8 * 60 * 60);

/// `repos.SyncProgress`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncProgress {
    pub synced: i32,
    pub errors: i32,
    /// Diff stats.
    pub added: i32,
    pub removed: i32,
    pub modified: i32,
    pub unmodified: i32,
    pub deleted: i32,
}

/// `progressRecorderFunc`.
#[async_trait]
pub trait ProgressRecorder: Send + Sync {
    /// `final_call` lets the implementation drop intermediate updates.
    async fn record(&self, progress: SyncProgress, final_call: bool) -> Result<(), String>;
}

/// Discards progress, for tests and callers without a sync job.
pub struct NoopProgressRecorder;

#[async_trait]
impl ProgressRecorder for NoopProgressRecorder {
    async fn record(&self, _progress: SyncProgress, _final_call: bool) -> Result<(), String> {
        Ok(())
    }
}

/// `repos.Sourcer`: builds the [`Source`] for an external service.
#[async_trait]
pub trait SourceFactory: Send + Sync {
    async fn source(&self, service: &ExternalService) -> Result<Box<dyn Source>, String>;
}

/// `repos.NewSource` for the kinds `sg-extsvc` implements (`OTHER`,
/// `GITHUB`, `GITLAB`, `BITBUCKETCLOUD`).
pub struct ExtsvcSourceFactory {
    http: Option<Arc<dyn HttpGetter>>,
    /// The code-host transport (`httpcli.Doer`). Production passes an
    /// `sg-http::HttpClient` adapter; tests inject fixed responses.
    doer: Option<Arc<dyn sg_extsvc::HttpDoer>>,
    /// `database.ExternalServices.getEncryptionKey()`: the ring is read on
    /// every source construction, so a rebuilt ring applies immediately.
    keyring: Arc<Keyring>,
}

impl ExtsvcSourceFactory {
    pub fn new(
        http: Option<Arc<dyn HttpGetter>>,
        doer: Option<Arc<dyn sg_extsvc::HttpDoer>>,
        keyring: Arc<Keyring>,
    ) -> Self {
        Self {
            http,
            doer,
            keyring,
        }
    }
}

#[async_trait]
impl SourceFactory for ExtsvcSourceFactory {
    async fn source(&self, service: &ExternalService) -> Result<Box<dyn Source>, String> {
        // `svc.Config.Decrypt(ctx)` with the ring's `externalServiceKey`.
        // Plaintext configurations (`encryption_key_id == ""`) pass through
        // unchanged; a missing or mismatched key is reported with the
        // baseline's `external service id=%d config error: …` wording.
        let config = sg_encryption::maybe_decrypt(
            self.keyring.external_service_key().as_deref(),
            &service.config,
            &service.encryption_key_id,
        )
        .await
        .map_err(|err| format!("external service id={} config error: {err}", service.id))?;
        let source_service = SourceService {
            id: service.id,
            kind: service.kind.clone(),
            display_name: service.display_name.clone(),
            config,
            encryption_key_id: service.encryption_key_id.clone(),
            unrestricted: service.unrestricted,
        };
        sg_extsvc::new_source(source_service, self.http.clone(), self.doer.clone())
    }
}

/// Source of the site configuration's `licenseKey`
/// (`conf.Get().LicenseKey`).
pub trait LicenseKeySource: Send + Sync {
    fn license_key(&self) -> String;
}

impl LicenseKeySource for sg_config::LiveConfig {
    fn license_key(&self) -> String {
        self.current().site.license_key()
    }
}

/// A fixed license key, for tests and callers without a live configuration.
/// The empty key is the "no license configured" case (the free plan).
#[derive(Debug, Clone, Default)]
pub struct StaticLicenseKey(pub String);

impl LicenseKeySource for StaticLicenseKey {
    fn license_key(&self) -> String {
        self.0.clone()
    }
}

/// A per-repo sync failure. `license` marks the baseline's `LicenseError`,
/// which does not abort the delete phase (unlike every other error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncError {
    pub message: String,
    pub license: bool,
}

impl SyncError {
    fn other(message: String) -> Self {
        Self {
            message,
            license: false,
        }
    }

    fn license(message: String) -> Self {
        Self {
            message,
            license: true,
        }
    }
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<String> for SyncError {
    fn from(message: String) -> Self {
        Self::other(message)
    }
}

/// `repos.Syncer`.
pub struct Syncer {
    external_services: ExternalServiceStore,
    store: ExternalServiceReposStore,
    sourcer: Arc<dyn SourceFactory>,
    /// `Syncer.Synced`; `None` disables publishing (as in the baseline).
    synced: Option<broadcast::Sender<RepoSyncDiff>>,
    clock: Arc<dyn Clock>,
    metrics: Arc<Metrics>,
    license: Arc<dyn LicenseKeySource>,
}

impl Syncer {
    pub fn new(
        external_services: ExternalServiceStore,
        store: ExternalServiceReposStore,
        sourcer: Arc<dyn SourceFactory>,
        synced: Option<broadcast::Sender<RepoSyncDiff>>,
        clock: Arc<dyn Clock>,
        metrics: Arc<Metrics>,
        license: Arc<dyn LicenseKeySource>,
    ) -> Self {
        Self {
            external_services,
            store,
            sourcer,
            synced,
            clock,
            metrics,
            license,
        }
    }

    /// `SyncExternalService`: sync repos in a streaming fashion, then delete
    /// the ones that were not sourced.
    pub async fn sync_external_service(
        &self,
        external_service_id: i64,
        min_sync_interval: Duration,
        progress: &dyn ProgressRecorder,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let began = self.clock.now();
        tracing::info!(external_service_id, "syncing external service");

        let svc = match self.external_services.get_by_id(external_service_id).await {
            Ok(svc) => svc,
            Err(err) => {
                let message = format!("fetching external services: {err}");
                self.observe_sync_finished(
                    began,
                    &Result::<(), String>::Err(message.clone()),
                    None,
                );
                return Err(message);
            }
        };

        let mut modified = false;
        let outcome = self
            .sync_service(&svc, progress, cancel, &mut modified)
            .await;

        // From this point on we always make a best-effort attempt to update
        // the service timestamps (the baseline's deferred `Update`). A
        // failure is logged, never returned.
        let now = self.clock.now();
        let interval = calc_sync_interval(
            now,
            svc.last_sync_at.map(SystemTime::from),
            min_sync_interval,
            modified,
            outcome.is_err(),
        );
        let next_sync_at = now + interval;
        if let Err(err) = self
            .external_services
            .update_sync_timestamps(
                svc.id,
                sg_db::OffsetDateTime::from(now),
                sg_db::OffsetDateTime::from(next_sync_at),
            )
            .await
        {
            tracing::error!(
                external_service_id = svc.id,
                error = %err,
                "upserting external service"
            );
        }
        tracing::debug!(backoff = ?interval, "synced external service");

        self.observe_sync_finished(began, &outcome, Some(&svc));

        outcome
    }

    /// `Syncer.observeSync`'s deferred closure: increments the started counter,
    /// records the finish time, duration and (on failure) the error reason.
    fn observe_sync_finished(
        &self,
        began: SystemTime,
        outcome: &Result<(), String>,
        svc: Option<&ExternalService>,
    ) {
        let ended = self.clock.now();
        let seconds = ended
            .duration_since(began)
            .unwrap_or_default()
            .as_secs_f64();
        // `float64(now.Unix())`: whole seconds, matching the baseline.
        let now_unix = ended
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as f64;
        let owner = if svc.is_some() {
            OWNER_SITE
        } else {
            OWNER_UNDEFINED
        };
        let reason = match outcome {
            Ok(()) => "",
            Err(err) => sync_error_reason(err),
        };
        self.metrics
            .observe_sync_finished(outcome.is_ok(), seconds, now_unix, owner, reason);
    }

    async fn sync_service(
        &self,
        svc: &ExternalService,
        progress: &dyn ProgressRecorder,
        cancel: &CancellationToken,
        modified: &mut bool,
    ) -> Result<(), String> {
        let src = self.sourcer.source(svc).await?;

        if let Err(err) = src.check_connection().await {
            tracing::warn!(
                error = %err,
                "connection check failed. syncing repositories might still succeed."
            );
        }

        // `ObservedSource.ListRepos` -> `REDMetrics.Observe`: on success the
        // duration and the result count (including error results) are
        // recorded; on any error only the error counter increments and the
        // duration sample is skipped, like the baseline.
        let source_began = std::time::Instant::now();
        let results = src.list_repos().await;
        let had_error = results.iter().any(|res| res.err.is_some());
        self.metrics.observe_source(
            source_began.elapsed().as_secs_f64(),
            results.len(),
            had_error,
        );

        // `seen` holds the ids of every repo this sync touched, so the delete
        // phase can tell what the external service no longer sources.
        let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
        let mut errors: Vec<String> = Vec::new();
        // The baseline only skips the delete phase for warnings that are
        // `Forbidden`/`Unauthorized` (or license errors); with string errors
        // every error is treated as aborting.
        let mut abort_deletion = false;
        let mut sync_progress = SyncProgress::default();

        for res in results {
            // Go's `ListRepos` respects ctx and stops producing results; the
            // materialised port stops iterating instead.
            if cancel.is_cancelled() {
                break;
            }

            if let Err(err) = progress.record(sync_progress, false).await {
                tracing::warn!(error = %err, "recording sync progress");
            }

            let Some(sourced) = res.repo else {
                let err = res
                    .err
                    .unwrap_or_else(|| "source returned no repo".to_owned());
                sync_progress.errors += 1;
                tracing::error!(seen = seen.len(), error = %err, "error from codehost");
                errors.push(format!(
                    "fetching from code host {}: {err}",
                    svc.display_name
                ));
                abort_deletion = true;
                continue;
            };

            match self.sync_one(svc, &sourced).await {
                Ok(diff) => {
                    sync_progress.added += diff.added.len() as i32;
                    sync_progress.removed += diff.deleted.len() as i32;
                    sync_progress.modified += diff.modified.len() as i32;
                    sync_progress.unmodified += diff.unmodified.len() as i32;
                    for repo in diff.repos() {
                        seen.insert(repo.id);
                    }
                    sync_progress.synced = seen.len() as i32;
                    *modified = *modified || !diff.modified.is_empty() || !diff.added.is_empty();
                }
                Err(err) => {
                    sync_progress.errors += 1;
                    tracing::error!(repo = %sourced.name, error = %err, "failed to sync, skipping");
                    // The baseline only skips the delete phase for license
                    // errors; every other per-repo error aborts it.
                    if !err.license {
                        abort_deletion = true;
                    }
                    errors.push(err.message);
                }
            }
        }

        // `delete` runs before the final progress recorder in the baseline
        // (the recorder is deferred, so it observes the final `Deleted`
        // counter).
        let mut deleted = 0;
        let seen: Vec<i64> = seen.into_iter().map(i64::from).collect();
        if !abort_deletion {
            match self.store.delete_not_in(svc.id, &seen).await {
                Ok((ids, first_error)) => {
                    if let Some(err) = first_error {
                        tracing::warn!(
                            seen = seen.len(),
                            deleted = ids.len(),
                            error = %err,
                            "failed to delete some repos"
                        );
                        errors.push(format!("some repos couldn't be deleted: {err}"));
                    }
                    if !ids.is_empty() {
                        deleted = ids.len();
                        tracing::warn!(seen = seen.len(), deleted, "deleted not seen repos");
                    }
                    self.notify_deleted(&ids);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to delete not seen repos");
                    errors.push(format!("some repos couldn't be deleted: {err}"));
                }
            }
        }
        sync_progress.deleted += deleted as i32;
        *modified = *modified || deleted > 0;

        if let Err(err) = progress.record(sync_progress, true).await {
            tracing::error!(error = %err, "recording final sync progress");
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// `Syncer.sync` for one sourced repo: read-decide-write in a single
    /// transaction, then publish the diff after the commit.
    async fn sync_one(
        &self,
        svc: &ExternalService,
        sourced: &Repo,
    ) -> Result<RepoSyncDiff, SyncError> {
        let mut tx = self
            .store
            .begin()
            .await
            .map_err(|err| SyncError::other(format!("syncer: opening transaction: {err}")))?;

        let diff = self.sync_one_tx(&mut tx, svc, sourced).await?;

        // The baseline observes the diff in a deferred block, so it is
        // recorded even if the commit fails.
        self.metrics.observe_diff(&diff);

        // Commit before publishing: gitserver must be able to find the repo in
        // the database when it reacts to the diff.
        tx.commit()
            .await
            .map_err(|err| SyncError::other(format!("syncer: committing transaction: {err}")))?;

        if let Some(sender) = &self.synced {
            if !diff.is_empty() {
                // A broadcast send only fails when no relay is subscribed;
                // the baseline would block instead.
                let _ = sender.send(diff.clone());
            }
        }

        Ok(diff)
    }

    async fn sync_one_tx(
        &self,
        tx: &mut sg_db::external_service_repos::RepoTransaction<'_>,
        svc: &ExternalService,
        sourced: &Repo,
    ) -> Result<RepoSyncDiff, SyncError> {
        let mut diff = RepoSyncDiff::default();

        let stored = tx
            .list_for_sync(&sourced.name, &stored_spec(&sourced.external_repo))
            .await
            .map_err(|err| format!("syncer: getting repo from the database: {err}"))?;

        let stored = match stored.len() {
            // Existing repo with a naming conflict: delete the repo that owns
            // the sourced name but has a different external repo spec, then
            // fall through to update the matching one (the baseline's
            // `fallthrough`).
            2 => {
                tracing::debug!("naming conflict");
                let mut existing = None;
                let mut conflicting = None;
                for repo in stored {
                    if repo
                        .external_repo
                        .equal(&stored_spec(&sourced.external_repo))
                    {
                        existing = Some(repo);
                    } else {
                        conflicting = Some(repo);
                    }
                }
                let Some(conflicting) = conflicting else {
                    return Err(SyncError::other(
                        "syncer: naming conflict without a conflicting repo".to_owned(),
                    ));
                };
                let Some(existing) = existing else {
                    return Err(SyncError::other(
                        "syncer: naming conflict without a matching repo".to_owned(),
                    ));
                };
                tx.delete_conflicting(conflicting.id)
                    .await
                    .map_err(|err| format!("syncer: failed to delete conflicting repo: {err}"))?;
                vec![existing]
            }
            _ => stored,
        };

        match stored.len() {
            1 => {
                let mut repo = stored_to_repo(&stored[0]);
                let was_deleted = repo.deleted;
                self.update_repo_license_hook(tx, &repo, sourced).await?;
                let modified = repo.update(sourced);

                if modified.is_empty() {
                    diff.unmodified.push(repo);
                } else {
                    let urn = svc_urn(svc);
                    let clone_url = repo
                        .sources
                        .get(&urn)
                        .map(|src| src.clone_url.clone())
                        .unwrap_or_default();
                    if clone_url.is_empty() {
                        return Err(SyncError::other(format!(
                            "UpdateExternalServiceRepo: repo {:?} missing source info for external service",
                            repo.name
                        )));
                    }
                    let write = repo_to_write(&repo);
                    tx.update(svc.id, repo.id, &write, &clone_url)
                        .await
                        .map_err(|err| {
                            format!("syncer: failed to update external service repo: {err}")
                        })?;
                    if was_deleted {
                        // Revived soft-deleted repo: report it as added.
                        diff.added.push(repo);
                    } else {
                        diff.modified.push(ModifiedRepo { repo, modified });
                    }
                }
            }
            0 => {
                self.create_repo_license_hook(tx, sourced).await?;
                let urn = svc_urn(svc);
                let source = sourced.sources.get(&urn);
                let Some(source) = source else {
                    return Err(SyncError::other(format!(
                        "CreateExternalServiceRepo: repo {:?} missing source info for external service",
                        sourced.name
                    )));
                };
                if source.clone_url.is_empty() {
                    return Err(SyncError::other(format!(
                        "CreateExternalServiceRepo: repo (ID={:?}) missing CloneURL for external service",
                        source.id
                    )));
                }
                let write = repo_to_write(sourced);
                let outcome =
                    tx.create(svc.id, &write, &source.clone_url)
                        .await
                        .map_err(|err| {
                            format!("syncer: failed to create external service repo: {err}")
                        })?;
                // `CreateExternalServiceRepo` scans the new id into the repo;
                // the diff (and `seen`) must carry the database id.
                let mut created = sourced.clone();
                if let sg_db::external_service_repos::SyncRepoOutcome::Created { id, .. } = outcome
                {
                    created.id = id;
                }
                diff.added.push(created);
            }
            n => {
                return Err(SyncError::other(format!(
                    "syncer: unexpected number of repos for {:?}: {n}",
                    sourced.name
                )));
            }
        }

        Ok(diff)
    }

    /// `CreateRepoLicenseHook`: a public repository never counts against the
    /// license; a private one is rejected once the license's private
    /// repositories are exhausted.
    async fn create_repo_license_hook(
        &self,
        tx: &mut sg_db::external_service_repos::RepoTransaction<'_>,
        sourced: &Repo,
    ) -> Result<(), SyncError> {
        if !sourced.private {
            return Ok(());
        }
        match sg_licensing::check_private_repositories(&self.license.license_key()) {
            PrivateRepositories::Unrestricted => Ok(()),
            PrivateRepositories::Limited(max) => {
                let count = tx
                    .count_private()
                    .await
                    .map_err(|err| license_hook_error(sourced, &err.to_string()))?;
                if count >= max as i64 {
                    Err(license_hook_error(
                        sourced,
                        &PrivateRepositories::Limited(max).error_message(),
                    ))
                } else {
                    Ok(())
                }
            }
            PrivateRepositories::NotActivated => Err(license_hook_error(
                sourced,
                &PrivateRepositories::NotActivated.error_message(),
            )),
        }
    }

    /// `UpdateRepoLicenseHook`: only reviving a deleted private repository or
    /// turning a public one private consumes a new slot; an update that
    /// leaves a private repository private does not.
    async fn update_repo_license_hook(
        &self,
        tx: &mut sg_db::external_service_repos::RepoTransaction<'_>,
        existing: &Repo,
        sourced: &Repo,
    ) -> Result<(), SyncError> {
        // `!newRepo.Private || !newRepo.DeletedAt.IsZero()`: sourced repos are
        // never deleted, so the second condition only matters for callers
        // that pass a deleted repo.
        if !sourced.private || sourced.deleted {
            return Ok(());
        }
        match sg_licensing::check_private_repositories(&self.license.license_key()) {
            PrivateRepositories::Unrestricted => Ok(()),
            PrivateRepositories::Limited(max) => {
                let count = tx
                    .count_private()
                    .await
                    .map_err(|err| license_hook_error(sourced, &err.to_string()))?;
                // `Count > max`: over the limit. `Count == max`: only a repo
                // that consumes a new slot (revived or public→private) is
                // rejected; one that is already private may stay private.
                let consumes_new_slot = existing.deleted || !existing.private;
                if count > max as i64 || (count == max as i64 && consumes_new_slot) {
                    Err(license_hook_error(
                        sourced,
                        &PrivateRepositories::Limited(max).error_message(),
                    ))
                } else {
                    Ok(())
                }
            }
            PrivateRepositories::NotActivated => Err(license_hook_error(
                sourced,
                &PrivateRepositories::NotActivated.error_message(),
            )),
        }
    }

    /// `Syncer.notifyDeleted`: the deleted repos are published by id only.
    fn notify_deleted(&self, deleted: &[i32]) {
        let mut diff = RepoSyncDiff::default();
        for id in deleted {
            diff.deleted.push(Repo {
                id: *id,
                ..Repo::default()
            });
        }
        // `observeDiff` runs before the `Synced != nil` check in the baseline.
        self.metrics.observe_diff(&diff);
        let Some(sender) = &self.synced else {
            return;
        };
        if !diff.is_empty() {
            let _ = sender.send(diff);
        }
    }
}

/// The baseline wraps every license-hook failure (including the count query
/// error) in `LicenseError{errors.Wrapf(err, "syncer: failed to update repo %s",
/// sourced.Name)}` — note that it says `update` on the create path too.
fn license_hook_error(sourced: &Repo, message: &str) -> SyncError {
    SyncError::license(format!(
        "syncer: failed to update repo {}: {message}",
        sourced.name
    ))
}

/// `extsvc.URN` for an external service.
fn svc_urn(svc: &ExternalService) -> String {
    sg_extsvc::urn(&svc.kind, svc.id)
}

fn stored_spec(spec: &sg_extsvc::ExternalRepoSpec) -> StoredSpec {
    StoredSpec {
        id: spec.id.clone(),
        service_type: spec.service_type.clone(),
        service_id: spec.service_id.clone(),
    }
}

/// `scanRepo`'s stored repo, as the `types.Repo` the baseline mutates.
fn stored_to_repo(stored: &StoredRepo) -> Repo {
    let sources = stored
        .sources
        .iter()
        .map(|source| {
            let urn = source.urn();
            (
                urn.clone(),
                SourceInfo {
                    id: urn,
                    clone_url: source.clone_url.clone(),
                },
            )
        })
        .collect();
    Repo {
        id: stored.id,
        deleted: stored.deleted_at.is_some(),
        name: stored.name.clone(),
        uri: stored.uri.clone(),
        description: stored.description.clone(),
        external_repo: sg_extsvc::ExternalRepoSpec {
            id: stored.external_repo.id.clone(),
            service_type: stored.external_repo.service_type.clone(),
            service_id: stored.external_repo.service_id.clone(),
        },
        sources,
        metadata: Some(stored.metadata.clone()),
        archived: stored.archived,
        fork: stored.fork,
        stars: stored.stars,
        private: stored.private,
    }
}

/// `newRepoRecord`'s metadata column (`nil` → `{}`).
fn repo_to_write(repo: &Repo) -> RepoWrite {
    RepoWrite {
        name: repo.name.clone(),
        uri: repo.uri.clone(),
        description: repo.description.clone(),
        external_repo: StoredSpec {
            id: repo.external_repo.id.clone(),
            service_type: repo.external_repo.service_type.clone(),
            service_id: repo.external_repo.service_id.clone(),
        },
        archived: repo.archived,
        fork: repo.fork,
        stars: repo.stars,
        private: repo.private,
        metadata: repo
            .metadata
            .clone()
            .unwrap_or_else(|| serde_json::json!({})),
    }
}

/// `calcSyncInterval`: use the minimum interval when the service was never
/// synced or the sync changed something; otherwise double the time since the
/// last sync, clamped to `[min, 8h]`. Errors back off the same way.
pub fn calc_sync_interval(
    now: SystemTime,
    last_sync: Option<SystemTime>,
    min_sync_interval: Duration,
    modified: bool,
    failed: bool,
) -> Duration {
    if !failed && (last_sync.is_none() || modified) {
        return min_sync_interval;
    }

    let interval = match last_sync {
        Some(last) => match now.duration_since(last) {
            Ok(elapsed) => elapsed.saturating_mul(2),
            // `now.Sub(lastSync)` is negative → below the minimum.
            Err(_) => return min_sync_interval,
        },
        // `now.Sub(time.Time{})` is enormous → clamped to the maximum.
        None => MAX_SYNC_INTERVAL,
    };

    interval.clamp(min_sync_interval, MAX_SYNC_INTERVAL)
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    #[test]
    fn calc_sync_interval_matches_the_baseline() {
        let min = Duration::from_secs(60);
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);

        // Never synced: minimum.
        assert_eq!(calc_sync_interval(now, None, min, false, false), min);
        // Modified: minimum.
        let last = now - Duration::from_secs(10);
        assert_eq!(calc_sync_interval(now, Some(last), min, true, false), min);
        // Unmodified, 10 minutes since the last sync: 20 minutes.
        let last = now - Duration::from_secs(600);
        assert_eq!(
            calc_sync_interval(now, Some(last), min, false, false),
            Duration::from_secs(1_200)
        );
        // Below the minimum clamps up.
        let last = now - Duration::from_secs(10);
        assert_eq!(calc_sync_interval(now, Some(last), min, false, false), min);
        // Above the maximum clamps down.
        let last = now - Duration::from_secs(100 * 60 * 60);
        assert_eq!(
            calc_sync_interval(now, Some(last), min, false, false),
            MAX_SYNC_INTERVAL
        );
        // Errors back off even when never synced.
        assert_eq!(
            calc_sync_interval(now, None, min, false, true),
            MAX_SYNC_INTERVAL
        );
        // A future last-sync is negative in Go → minimum.
        assert_eq!(
            calc_sync_interval(now, Some(now + Duration::from_secs(5)), min, false, false),
            min
        );
    }

    #[test]
    fn stored_repo_round_trips_into_the_write_shape() {
        let stored = StoredRepo {
            id: 7,
            name: "github.com/o/r".to_owned(),
            uri: "https://github.com/o/r".to_owned(),
            description: String::new(),
            external_repo: StoredSpec {
                id: "r".to_owned(),
                service_type: "other".to_owned(),
                service_id: "1".to_owned(),
            },
            archived: false,
            fork: false,
            stars: 0,
            private: true,
            metadata: serde_json::json!({"RelativePath": "/r"}),
            blocked: None,
            deleted_at: None,
            sources: vec![sg_db::external_service_repos::StoredSource {
                external_service_id: 1,
                kind: "other".to_owned(),
                clone_url: "https://example.com/r".to_owned(),
            }],
        };
        let repo = stored_to_repo(&stored);
        assert_eq!(repo.id, 7);
        assert!(!repo.deleted);
        assert_eq!(
            repo.sources["extsvc:other:1"].clone_url,
            "https://example.com/r"
        );
        let write = repo_to_write(&repo);
        assert_eq!(write.name, "github.com/o/r");
        assert_eq!(write.metadata, serde_json::json!({"RelativePath": "/r"}));
    }
}
