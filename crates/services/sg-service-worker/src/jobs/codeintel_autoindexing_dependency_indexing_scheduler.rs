#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/background/dependencies/
//! job_dependency_indexing_scheduler.go` — the index-queue half of the
//! `codeintel-autoindexing-dependency-scheduler` job.
//!
//! [`DependencyIndexingSchedulerHandler::handle`] ports the baseline handler:
//!
//! 1. a no-op when auto-indexing is disabled or when
//!    `CODEINTEL_DEPENDENCY_INDEX_SCHEDULER_DISABLED` was set at process
//!    start;
//! 2. when the job carries an external-service kind, list services of that
//!    kind; no service means "nothing to do", while a service whose
//!    `last_sync_at` predates the job's `external_service_sync` requeues the
//!    job 30 seconds out;
//! 3. scan the upload's package references, map each one through
//!    `infer.InferRepositoryAndRevision` and group the packages by inferred
//!    repository;
//! 4. for jobs without an external-service kind, drop repositories unknown to
//!    the instance using the baseline's merge-based [`set_difference`] after
//!    sorting `repo_names` in place (the baseline's `cast.ToStrings` alias
//!    makes its `sort.Strings` mutate `repoNames`);
//! 5. drop repositories that are missing or not cloned/cloning; a cloning
//!    repository requeues the job and stops processing;
//! 6. enqueue every remaining package reference (duplicates included) and
//!    aggregate failures.
//!
//! Unported dependencies are injected as traits: [`UploadService`] (the
//! streaming scanner is flattened to a vector), [`ReposStore`],
//! [`ExternalServiceStore`], [`GitserverRepoStore`], [`IndexEnqueuer`] and
//! [`DependencyIndexingWorkerStore`] (`Requeue`).
//!
//! Representation notes:
//!
//! * the baseline iterates a Go map when enqueuing, so call order is not
//!   deterministic; failures are returned as [`HandleError::Multiple`] with the
//!   individual texts preserved (the differential harness sorts them);
//! * `errors.Wrap` prefixes are reproduced verbatim;
//! * a `ListMinimalRepos` failure is logged and ignored, like the baseline.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::gitserver_repos::CloneStatus;
use sg_db::OffsetDateTime;

use super::codeintel_autoindexing_enqueuer::VersionedPackageRepo;
use super::codeintel_autoindexing_package_inference::infer_repository_and_revision;

/// `requeueBackoff`.
pub const REQUEUE_BACKOFF: Duration = Duration::from_secs(30);

/// `CODEINTEL_DEPENDENCY_INDEX_SCHEDULER_DISABLED` (parsed once by the
/// baseline at package init; callers pass the parsed value in
/// [`HandlerSettings`] so tests stay deterministic).
pub const DISABLE_INDEX_SCHEDULER_ENV: &str = "CODEINTEL_DEPENDENCY_INDEX_SCHEDULER_DISABLED";

/// `dependencyIndexingJob` (subset of `lsif_dependency_indexing_jobs`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyIndexingJob {
    pub id: i64,
    pub upload_id: i32,
    pub external_service_kind: String,
    /// Go `time.Time` zero value is modelled as `None`; it compares before
    /// every real timestamp.
    pub external_service_sync: Option<OffsetDateTime>,
}

/// `shared.PackageReference` / `shared.Package` subset used by the handler.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageReference {
    pub scheme: String,
    pub name: String,
    pub version: String,
}

/// `types.ExternalService` subset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalService {
    pub id: i64,
    /// `types.ExternalService.LastSyncAt`; `None` is the Go zero time.
    pub last_sync_at: Option<OffsetDateTime>,
}

/// `workerutil/dbworker/store.Store.Requeue`.
#[async_trait]
pub trait DependencyIndexingWorkerStore: Send + Sync + 'static {
    async fn requeue(&self, id: i64, process_after: OffsetDateTime) -> Result<(), String>;
}

/// `UploadService.ReferencesForUpload` (streaming scanner flattened).
#[async_trait]
pub trait UploadService: Send + Sync + 'static {
    async fn references_for_upload(&self, upload_id: i32) -> Result<Vec<PackageReference>, String>;
}

/// `ReposStore.ListMinimalRepos`: returns names in `ORDER BY name` order.
#[async_trait]
pub trait ReposStore: Send + Sync + 'static {
    async fn list_minimal_repos(&self, names: &[String]) -> Result<Vec<String>, String>;
}

/// `ExternalServiceStore.List`.
#[async_trait]
pub trait ExternalServiceStore: Send + Sync + 'static {
    async fn list(&self, kinds: &[String]) -> Result<Vec<ExternalService>, String>;
}

/// `GitserverRepoStore.GetByNames`.
#[async_trait]
pub trait GitserverRepoStore: Send + Sync + 'static {
    async fn get_by_names(&self, names: &[String]) -> Result<HashMap<String, CloneStatus>, String>;
}

/// `IndexEnqueuer.QueueAutoIndexJobsForPackage`.
#[async_trait]
pub trait IndexEnqueuer: Send + Sync + 'static {
    async fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String>;
}

/// Behaviour switches normally read from site config / package init.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerSettings {
    /// `conf.CodeIntelAutoIndexingEnabled`.
    pub auto_indexing_enabled: bool,
    /// `CODEINTEL_DEPENDENCY_INDEX_SCHEDULER_DISABLED`.
    pub disabled: bool,
}

impl Default for HandlerSettings {
    fn default() -> Self {
        Self {
            auto_indexing_enabled: true,
            disabled: false,
        }
    }
}

/// The handler's error: the baseline returns `errs[0]` for a single failure
/// and `errors.Append(nil, errs...)` for several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleError {
    Message(String),
    Multiple(Vec<String>),
}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandleError::Message(message) => f.write_str(message),
            // `errors.Append(nil, errs...)`: `N errors occurred:` plus one
            // `\n\t* ` entry per error (the worker stores this text in
            // `failure_message`).
            HandleError::Multiple(errors) => {
                f.write_str(&crate::config::combine_errors(errors).unwrap_or_default())
            }
        }
    }
}

impl std::error::Error for HandleError {}

/// `dependencyIndexingSchedulerHandler`.
pub struct DependencyIndexingSchedulerHandler {
    pub uploads: Arc<dyn UploadService>,
    pub repos: Arc<dyn ReposStore>,
    pub external_services: Arc<dyn ExternalServiceStore>,
    pub gitserver_repos: Arc<dyn GitserverRepoStore>,
    pub index_enqueuer: Arc<dyn IndexEnqueuer>,
    pub worker_store: Arc<dyn DependencyIndexingWorkerStore>,
    pub settings: HandlerSettings,
}

impl DependencyIndexingSchedulerHandler {
    /// `Handle`.
    pub async fn handle(&self, job: &DependencyIndexingJob) -> Result<(), HandleError> {
        if !self.settings.auto_indexing_enabled || self.settings.disabled {
            return Ok(());
        }

        if !job.external_service_kind.is_empty() {
            let external_services = self
                .external_services
                .list(std::slice::from_ref(&job.external_service_kind))
                .await
                .map_err(|error| HandleError::Message(format!("extsvcStore.List: {error}")))?;

            if external_services.is_empty() {
                // The baseline only logs; the message is not part of the
                // differential contract.
                return Ok(());
            }

            let outdated = external_services
                .iter()
                .any(|service| last_sync_before(service.last_sync_at, job.external_service_sync));
            if outdated {
                self.requeue(job).await?;
                return Ok(());
            }
        }

        let references = self
            .uploads
            .references_for_upload(job.upload_id)
            .await
            .map_err(|error| {
                HandleError::Message(format!("dbstore.ReferencesForUpload: {error}"))
            })?;

        let mut repo_to_packages: HashMap<String, Vec<VersionedPackageRepo>> = HashMap::new();
        let mut repo_names: Vec<String> = Vec::new();

        for reference in references {
            let package = VersionedPackageRepo {
                scheme: reference.scheme,
                name: reference.name,
                version: reference.version,
            };

            let Some((repo_name, _revision)) = infer_repository_and_revision(&package) else {
                continue;
            };

            repo_to_packages
                .entry(repo_name.clone())
                .or_default()
                .push(package);
            repo_names.push(repo_name);
        }

        // No dependencies found, we can return early.
        if repo_names.is_empty() {
            return Ok(());
        }

        // If this job is not associated with an external service kind that
        // was just synced, then we need to guarantee that the repos are
        // visible to the instance, else skip them.
        if job.external_service_kind.is_empty() {
            // `cast.ToStrings` reinterprets the slice header with unsafe, so
            // the baseline's `sort.Strings(repoNameStrings)` sorts
            // `repoNames` in place. Reproduce that side effect: the later
            // gitserver loop observes the sorted order too.
            repo_names.sort();

            // A listing failure is logged and ignored by the baseline; the
            // (empty) result therefore removes every inferred repository.
            let listed_repos = self
                .repos
                .list_minimal_repos(&repo_names)
                .await
                .unwrap_or_default();

            let difference = set_difference(&repo_names, &listed_repos);
            for repo in difference {
                repo_to_packages.remove(&repo);
            }
        }

        let results = self
            .gitserver_repos
            .get_by_names(&repo_names)
            .await
            .map_err(|error| HandleError::Message(format!("gitserver.RepoInfo: {error}")))?;

        for repo_name in &repo_names {
            match results.get(repo_name) {
                None => {
                    repo_to_packages.remove(repo_name);
                }
                Some(CloneStatus::Cloned) => {}
                Some(CloneStatus::Cloning) => {
                    // We can't enqueue if still cloning.
                    self.requeue(job).await?;
                    return Ok(());
                }
                Some(_) => {
                    repo_to_packages.remove(repo_name);
                }
            }
        }

        let mut errors = Vec::new();
        for packages in repo_to_packages.values() {
            for package in packages {
                if let Err(error) = self
                    .index_enqueuer
                    .queue_auto_index_jobs_for_package(package)
                    .await
                {
                    errors.push(format!("enqueuer.QueueAutoIndexJobsForPackage: {error}"));
                }
            }
        }

        match errors.len() {
            0 => Ok(()),
            1 => Err(HandleError::Message(errors.remove(0))),
            _ => Err(HandleError::Multiple(errors)),
        }
    }

    async fn requeue(&self, job: &DependencyIndexingJob) -> Result<(), HandleError> {
        let process_after = OffsetDateTime::now_utc() + REQUEUE_BACKOFF;
        self.worker_store
            .requeue(job.id, process_after)
            .await
            .map_err(|error| HandleError::Message(format!("store.Requeue: {error}")))
    }
}

/// `externalService.LastSyncAt.Before(job.ExternalServiceSync)` with the Go
/// zero time modelled as `None`.
fn last_sync_before(
    last_sync_at: Option<OffsetDateTime>,
    external_service_sync: Option<OffsetDateTime>,
) -> bool {
    match (last_sync_at, external_service_sync) {
        // A zero `ExternalServiceSync` never triggers a requeue: no real
        // timestamp is before it.
        (_, None) => false,
        // Go's zero time (year 1) is before every timestamp representable in
        // the database, including the Unix epoch.
        (None, Some(_)) => true,
        (Some(last), Some(sync)) => last < sync,
    }
}

/// `setDifference`: a merge-based difference that assumes `superset` and
/// `subset` are ordered. The caller sorts `repo_names` in place first, like
/// the baseline's `cast.ToStrings` alias does through `sort.Strings`.
pub fn set_difference<T: PartialEq + Clone>(superset: &[T], subset: &[T]) -> Vec<T> {
    let mut difference = Vec::new();
    let mut j = 0usize;

    for i in 0..superset.len() {
        let value = &superset[i];
        if i > 0 && *value == superset[i - 1] {
            continue;
        }
        if j >= subset.len() {
            difference.push(value.clone());
            continue;
        }
        if *value == subset[j] {
            j += 1;
        } else {
            difference.push(value.clone());
        }
    }

    difference
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_difference_merges_ordered_inputs() {
        let superset = ["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let subset = ["a".to_owned(), "c".to_owned()];
        assert_eq!(set_difference(&superset, &subset), vec!["b".to_owned()]);
    }

    #[test]
    fn set_difference_skips_duplicates_in_superset() {
        let superset = ["a".to_owned(), "a".to_owned(), "b".to_owned()];
        let subset = ["a".to_owned(), "b".to_owned()];
        assert!(set_difference(&superset, &subset).is_empty());
    }

    #[test]
    fn set_difference_with_empty_subset_returns_everything() {
        let superset = ["a".to_owned(), "b".to_owned()];
        assert_eq!(set_difference(&superset, &[]), superset.to_vec());
    }

    #[test]
    fn zero_external_service_sync_is_never_outdated() {
        assert!(!last_sync_before(None, None));
        assert!(!last_sync_before(Some(OffsetDateTime::UNIX_EPOCH), None));
        // Go's zero time is year 1, so it precedes even the Unix epoch.
        assert!(last_sync_before(None, Some(OffsetDateTime::UNIX_EPOCH)));
        assert!(!last_sync_before(
            Some(OffsetDateTime::UNIX_EPOCH),
            Some(OffsetDateTime::UNIX_EPOCH)
        ));
    }
}
