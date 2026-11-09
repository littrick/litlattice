#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/autoindexing_dependencies.go` — the
//! `codeintel-autoindexing-dependency-scheduler` job and the production
//! adapters behind its handlers.
//!
//! The baseline's `NewDependencyIndexSchedulers`
//! (`internal/codeintel/autoindexing/internal/background/init.go:52`) returns
//! four routines:
//!
//! | routine | store | options |
//! |---|---|---|
//! | dependency sync scheduler | `lsif_dependency_syncing_jobs` | 1 handler, `DependencySyncSchedulerPollInterval` |
//! | dependency indexing scheduler | `lsif_dependency_indexing_jobs` | `DependencyIndexerSchedulerConcurrency`, `DependencyIndexerSchedulerPollInterval` |
//! | `precise_code_intel_index_worker_resetter` | `lsif_indexes` | `ResetterInterval` |
//! | `precise_code_intel_dependency_index_worker_resetter` | `lsif_dependency_indexing_jobs` | `ResetterInterval` |
//!
//! Both workers use a 1s heartbeat. The dbworker stores live in
//! [`super::codeintel_dependency_jobs`]; this module adds the production
//! implementations of the handler seams from M9.8dg/M9.8dh on top of
//! `sg-db` and assembles the job.
//!
//! The indexing scheduler's `IndexEnqueuer` is
//! `autoindexing.Service.QueueAutoIndexJobsForPackage`; its production chain
//! (`autoindexing.NewService` → `newService` → `jobselector.NewJobSelector` →
//! `enqueuer.NewIndexEnqueuer`) is assembled here from the production
//! adapters in [`super::codeintel_autoindexing_inference_production`] (which
//! bind gitserver, the site-config indexer map and the inference env limits
//! to the M9.8dm inference service) and the M9.8dc/M9.8de job-selector and
//! enqueuer ports.
//!
//! Not ported: worker/resetter metrics, observation spans and the
//! `actor.WithInternalActor` context (the SQL has no authz condition).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_dependencies::{self, insert_package_repo_refs, PackageRepoFilter};
use sg_db::codeintel_uploads;
use sg_db::external_services::ExternalServiceStore;
use sg_db::gitserver_repos::{CloneStatus, GitserverReposStore};
use sg_db::repos::RepoStore;
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use time::{Date, Month, Time};
use tokio_util::sync::CancellationToken;

use super::codeintel_autoindexing_dependency_indexing_scheduler as indexing;
use super::codeintel_autoindexing_dependency_sync_scheduler as sync;
use super::codeintel_autoindexing_enqueuer::{IndexEnqueuer, VersionedPackageRepo};
use super::codeintel_autoindexing_inference::{InferenceService, SandboxService};
use super::codeintel_autoindexing_inference_production::{
    inference_environment, GitserverGitService, GitserverRevisionResolver, GitserverYamlReader,
    InferenceAdapter, TokenBucketLimiter,
};
use super::codeintel_autoindexing_package_inference::DefaultPackageRepoInference;
use super::codeintel_autoindexing_scheduler::auto_indexing_enabled;
use super::codeintel_dependency_jobs::{
    DependencyIndexingJobRecord, DependencyIndexingJobStore, DependencySyncingJobRecord,
    DependencySyncingJobStore, IndexResetter, INDEXING_WORKER_DESCRIPTION, INDEXING_WORKER_NAME,
};
use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::env::parse_go_bool;
use crate::framework::dbworker::{
    DbRecord, DbWorkerHandler, ResetterOptions, ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::BackgroundRoutine;
use sg_git::GitserverClient;
use sg_luasandbox::Service as LuaSandboxService;

/// `autoindexingDependencyScheduler.Description()`.
pub const JOB_DESCRIPTION: &str = "";

/// `Config.ResetterInterval`'s variable and default.
pub const RESETTER_INTERVAL_VAR: &str = "PRECISE_CODE_INTEL_DEPENDENCY_RESETTER_INTERVAL";
/// `Config.DependencySyncSchedulerPollInterval`'s variable and default.
pub const SYNC_SCHEDULER_POLL_INTERVAL_VAR: &str =
    "PRECISE_CODE_INTEL_DEPENDENCY_SYNC_SCHEDULER_POLL_INTERVAL";
/// `Config.DependencyIndexerSchedulerPollInterval`'s variable and default.
pub const INDEXER_SCHEDULER_POLL_INTERVAL_VAR: &str =
    "PRECISE_CODE_INTEL_DEPENDENCY_INDEXER_SCHEDULER_POLL_INTERVAL";
/// `Config.DependencyIndexerSchedulerConcurrency`'s variable and default.
pub const INDEXER_SCHEDULER_CONCURRENCY_VAR: &str =
    "PRECISE_CODE_INTEL_DEPENDENCY_INDEXER_SCHEDULER_CONCURRENCY";

const RESETTER_INTERVAL_DESCRIPTION: &str = "Interval between dependency sync and index resets.";
const SYNC_SCHEDULER_POLL_INTERVAL_DESCRIPTION: &str =
    "Interval between queries to the dependency syncing job queue.";
const INDEXER_SCHEDULER_POLL_INTERVAL_DESCRIPTION: &str =
    "Interval between queries to the dependency indexing job queue.";
const INDEXER_SCHEDULER_CONCURRENCY_DESCRIPTION: &str =
    "The maximum number of dependency graphs that can be processed concurrently.";

/// `workerutil.WorkerOptions.HeartbeatInterval` for both dependency workers.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// `Config` (`autoindexing.DependenciesConfigInst`).
#[derive(Debug, Default)]
pub struct DependenciesConfig {
    resetter_interval: Mutex<Duration>,
    sync_scheduler_poll_interval: Mutex<Duration>,
    indexer_scheduler_poll_interval: Mutex<Duration>,
    indexer_scheduler_concurrency: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

macro_rules! config_getter {
    ($name:ident, $field:ident, $ty:ty) => {
        pub fn $name(&self) -> $ty {
            *self
                .$field
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    };
}

impl DependenciesConfig {
    config_getter!(resetter_interval, resetter_interval, Duration);
    config_getter!(
        sync_scheduler_poll_interval,
        sync_scheduler_poll_interval,
        Duration
    );
    config_getter!(
        indexer_scheduler_poll_interval,
        indexer_scheduler_poll_interval,
        Duration
    );
    config_getter!(
        indexer_scheduler_concurrency,
        indexer_scheduler_concurrency,
        i64
    );
}

impl JobConfig for DependenciesConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        *self
            .resetter_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            RESETTER_INTERVAL_VAR,
            "30s",
            RESETTER_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self
            .sync_scheduler_poll_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            SYNC_SCHEDULER_POLL_INTERVAL_VAR,
            "1s",
            SYNC_SCHEDULER_POLL_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self
            .indexer_scheduler_poll_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            INDEXER_SCHEDULER_POLL_INTERVAL_VAR,
            "1s",
            INDEXER_SCHEDULER_POLL_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self
            .indexer_scheduler_concurrency
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_int(
            env,
            INDEXER_SCHEDULER_CONCURRENCY_VAR,
            "1",
            INDEXER_SCHEDULER_CONCURRENCY_DESCRIPTION,
            &mut errs,
        );
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::new(combine_errors(&errs).unwrap_or_default()))
        }
    }
}

/// `BaseConfig.GetInt`.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!(
                "invalid int {raw:?} for {name}: strconv.ParseInt: parsing {raw:?}: invalid syntax"
            ));
            0
        }
    }
}

/// `disableIndexScheduler` from the baseline's package init:
/// `strconv.ParseBool(os.Getenv("CODEINTEL_DEPENDENCY_INDEX_SCHEDULER_DISABLED"))`
/// with the error ignored (an unparsable value is `false`).
pub fn index_scheduler_disabled() -> bool {
    std::env::var(indexing::DISABLE_INDEX_SCHEDULER_ENV)
        .ok()
        .map(|raw| parse_go_bool(&raw).unwrap_or(false))
        .unwrap_or(false)
}

/// Go's zero `time.Time{}` (`0001-01-01T00:00:00Z`), the `external_service_sync`
/// the sync handler writes when no real external-service kind was seen.
pub fn go_zero_time() -> OffsetDateTime {
    Date::from_calendar_date(1, Month::January, 1)
        .expect("year 1 is representable")
        .with_time(Time::MIDNIGHT)
        .assume_utc()
}

// ---------------------------------------------------------------------------
// Production adapters for the handler seams
// ---------------------------------------------------------------------------

/// `uploads.UploadService` (`GetUploadByID` + `ReferencesForUpload`) backed by
/// the uploads store queries in [`sg_db::codeintel_uploads`].
pub struct UploadsAdapter {
    pool: PgPool,
}

impl UploadsAdapter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl sync::DependencySyncUploads for UploadsAdapter {
    async fn references_for_upload(
        &self,
        upload_id: i32,
    ) -> Result<sync::PackageReferences, String> {
        let references = codeintel_uploads::references_for_upload(&self.pool, upload_id)
            .await
            .map_err(|err| err.to_string())?;
        Ok(sync::PackageReferences {
            references: references
                .into_iter()
                .map(|reference| sync::Package {
                    scheme: reference.scheme,
                    manager: reference.manager,
                    name: reference.name,
                    version: reference.version,
                })
                .collect(),
            // The store query is executed eagerly; `Next` and `Close` cannot
            // fail separately as they do for the baseline's streaming scan.
            next_error: None,
            close_error: None,
        })
    }

    async fn get_upload_by_id(&self, upload_id: i32) -> Result<Option<sync::Upload>, String> {
        codeintel_uploads::get_upload_by_id(&self.pool, upload_id)
            .await
            .map_err(|err| err.to_string())
            .map(|upload| {
                upload.map(|upload| sync::Upload {
                    id: upload.id,
                    indexer: upload.indexer,
                })
            })
    }
}

#[async_trait]
impl indexing::UploadService for UploadsAdapter {
    async fn references_for_upload(
        &self,
        upload_id: i32,
    ) -> Result<Vec<indexing::PackageReference>, String> {
        let references = codeintel_uploads::references_for_upload(&self.pool, upload_id)
            .await
            .map_err(|err| err.to_string())?;
        Ok(references
            .into_iter()
            .map(|reference| indexing::PackageReference {
                scheme: reference.scheme,
                name: reference.name,
                version: reference.version,
            })
            .collect())
    }
}

/// `dependencies.DependenciesService` backed by
/// [`sg_db::codeintel_dependencies`].
pub struct DependenciesAdapter {
    pool: PgPool,
}

impl DependenciesAdapter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl sync::DependencySyncDependencies for DependenciesAdapter {
    async fn list_package_repo_filters(&self) -> Result<Vec<PackageRepoFilter>, String> {
        codeintel_dependencies::list_package_repo_filters(&self.pool).await
    }

    async fn insert_package_repo_refs(
        &self,
        refs: &[sync::MinimalPackageRepoRef],
    ) -> Result<sync::InsertedPackageRepoRefs, String> {
        let (new_refs, new_versions) = insert_package_repo_refs(&self.pool, refs).await?;
        Ok(sync::InsertedPackageRepoRefs {
            new_repo_refs: new_refs.len(),
            new_versions: new_versions.len(),
        })
    }
}

/// `store.Store.InsertDependencyIndexingJob` backed by
/// [`sg_db::codeintel_autoindexing_dependencies`].
pub struct InsertDependencyIndexingJobAdapter {
    pool: PgPool,
}

impl InsertDependencyIndexingJobAdapter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl sync::DependencyIndexingJobStore for InsertDependencyIndexingJobAdapter {
    async fn insert_dependency_indexing_job(
        &self,
        upload_id: i32,
        external_service_kind: &str,
        sync_time: Option<OffsetDateTime>,
    ) -> Result<i32, String> {
        sg_db::codeintel_autoindexing_dependencies::insert_dependency_indexing_job(
            &self.pool,
            upload_id,
            external_service_kind,
            sync_time.unwrap_or_else(go_zero_time),
        )
        .await
        .map_err(|err| err.to_string())
    }
}

/// `ExternalServiceStore` for the sync scheduler (`List` + `Upsert`).
pub struct SyncExternalServicesAdapter {
    store: ExternalServiceStore,
}

impl SyncExternalServicesAdapter {
    pub fn new(store: ExternalServiceStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl sync::DependencySyncExternalServices for SyncExternalServicesAdapter {
    async fn list(&self, kinds: &[String]) -> Result<Vec<sync::ExternalService>, String> {
        let kinds: Vec<&str> = kinds.iter().map(String::as_str).collect();
        self.store
            .list_by_kinds(&kinds)
            .await
            .map_err(|err| err.to_string())
            .map(|services| {
                services
                    .into_iter()
                    .map(|service| sync::ExternalService {
                        id: service.id,
                        display_name: service.display_name,
                    })
                    .collect()
            })
    }

    async fn upsert_next_sync_at(
        &self,
        service: &sync::ExternalService,
        next_sync_at: OffsetDateTime,
    ) -> Result<(), String> {
        self.store
            .set_next_sync_at(service.id, next_sync_at)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `ExternalServiceStore` for the indexing scheduler (`List` with
/// `last_sync_at`).
pub struct IndexingExternalServicesAdapter {
    store: ExternalServiceStore,
}

impl IndexingExternalServicesAdapter {
    pub fn new(store: ExternalServiceStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl indexing::ExternalServiceStore for IndexingExternalServicesAdapter {
    async fn list(&self, kinds: &[String]) -> Result<Vec<indexing::ExternalService>, String> {
        let kinds: Vec<&str> = kinds.iter().map(String::as_str).collect();
        self.store
            .list_by_kinds(&kinds)
            .await
            .map_err(|err| err.to_string())
            .map(|services| {
                services
                    .into_iter()
                    .map(|service| indexing::ExternalService {
                        id: service.id,
                        last_sync_at: service.last_sync_at,
                    })
                    .collect()
            })
    }
}

/// `ReposStore.ListMinimalRepos` backed by [`RepoStore`].
pub struct ReposAdapter {
    store: RepoStore,
}

impl ReposAdapter {
    pub fn new(store: RepoStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl indexing::ReposStore for ReposAdapter {
    async fn list_minimal_repos(&self, names: &[String]) -> Result<Vec<String>, String> {
        self.store
            .list_minimal_repos_by_names(names)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `GitserverRepoStore.GetByNames` backed by [`GitserverReposStore`].
pub struct GitserverRepoStoreAdapter {
    store: GitserverReposStore,
}

impl GitserverRepoStoreAdapter {
    pub fn new(store: GitserverReposStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl indexing::GitserverRepoStore for GitserverRepoStoreAdapter {
    async fn get_by_names(&self, names: &[String]) -> Result<HashMap<String, CloneStatus>, String> {
        self.store
            .get_by_names(names)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `IndexEnqueuer.QueueAutoIndexJobsForPackage` backed by the ported
/// [`IndexEnqueuer`]. Production construction of that enqueuer waits on the
/// Lua inference pipeline (see the module docs).
pub struct IndexEnqueuerAdapter {
    enqueuer: Arc<IndexEnqueuer>,
}

impl IndexEnqueuerAdapter {
    pub fn new(enqueuer: Arc<IndexEnqueuer>) -> Self {
        Self { enqueuer }
    }
}

#[async_trait]
impl indexing::IndexEnqueuer for IndexEnqueuerAdapter {
    async fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String> {
        self.enqueuer
            .queue_auto_index_jobs_for_package(package)
            .await
    }
}

// ---------------------------------------------------------------------------
// Handler adapters for the framework worker loop
// ---------------------------------------------------------------------------

/// `dbworker.Handler` over the syncing queue: maps the dequeued row to the
/// handler's [`sync::DependencySyncingJob`].
pub struct SyncSchedulerWorker {
    pub handler: sync::DependencySyncSchedulerHandler,
}

#[async_trait]
impl DbWorkerHandler<DependencySyncingJobStore> for SyncSchedulerWorker {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &DependencySyncingJobRecord,
    ) -> Result<(), String> {
        let job = sync::DependencySyncingJob {
            id: record.record_id(),
            upload_id: record.0.upload_id.unwrap_or_default(),
        };
        self.handler
            .handle(&job)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `dbworker.Handler` over the indexing queue: maps the dequeued row to the
/// handler's [`indexing::DependencyIndexingJob`].
pub struct IndexingSchedulerWorker {
    pub handler: indexing::DependencyIndexingSchedulerHandler,
}

#[async_trait]
impl DbWorkerHandler<DependencyIndexingJobStore> for IndexingSchedulerWorker {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &DependencyIndexingJobRecord,
    ) -> Result<(), String> {
        let job = indexing::DependencyIndexingJob {
            id: record.record_id(),
            upload_id: record.0.upload_id.unwrap_or_default(),
            external_service_kind: record.0.external_service_kind.clone(),
            external_service_sync: record.0.external_service_sync,
        };
        self.handler
            .handle(&job)
            .await
            .map_err(|err| err.to_string())
    }
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

/// `cmd/worker/internal/codeintel.NewAutoindexingDependencySchedulerJob`.
#[derive(Debug, Default)]
pub struct AutoindexingDependencyScheduler {
    config: Arc<DependenciesConfig>,
}

impl AutoindexingDependencyScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &DependenciesConfig {
        &self.config
    }
}

#[async_trait]
impl Job for AutoindexingDependencyScheduler {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let pool = deps.frontend.clone();
        let auto_indexing_enabled = auto_indexing_enabled(&deps.live_config.current().site);

        let sync_store = Arc::new(DependencySyncingJobStore::new(pool.clone()));
        let sync_handler = Arc::new(SyncSchedulerWorker {
            handler: sync::DependencySyncSchedulerHandler {
                uploads: Arc::new(UploadsAdapter::new(pool.clone())),
                dependencies: Arc::new(DependenciesAdapter::new(pool.clone())),
                store: Arc::new(InsertDependencyIndexingJobAdapter::new(pool.clone())),
                external_services: Arc::new(SyncExternalServicesAdapter::new(
                    ExternalServiceStore::new(pool.clone()),
                )),
                settings: sync::HandlerSettings {
                    auto_indexing_enabled,
                },
            },
        });

        let sync_worker = WorkerRoutine::new(
            sync_store,
            sync_handler,
            WorkerOptions {
                name: super::codeintel_dependency_jobs::SYNC_WORKER_NAME.to_owned(),
                description: super::codeintel_dependency_jobs::SYNC_WORKER_DESCRIPTION.to_owned(),
                interval: self.config.sync_scheduler_poll_interval(),
                heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
                num_handlers: 1,
                worker_hostname: crate::framework::dbworker::default_hostname(),
            },
        );

        // `autoindexing.NewService`'s production chain: the gitserver client
        // feeds the inference service (M9.8dm), the job selector and the
        // enqueuer (M9.8dc/M9.8de). The env limits are read here because the
        // baseline reads them in package init (`env.MustGetInt`).
        let unified = deps.live_config.current();
        let git = Arc::new(GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let mut env = EnvReader::new();
        let environment = inference_environment(&unified.site, &mut env)?;
        let sandbox: Arc<dyn SandboxService> = Arc::new(LuaSandboxService::new());
        let inference_service = Arc::new(InferenceService::new(
            sandbox,
            Arc::new(GitserverGitService::new(git.clone())),
            Arc::new(TokenBucketLimiter::new(
                environment.gitserver_request_rate_limit,
            )),
            environment.settings,
        ));
        let enqueuer = Arc::new(IndexEnqueuer::from_env(
            pool.clone(),
            Arc::new(GitserverRevisionResolver::new(git.clone())),
            Arc::new(DefaultPackageRepoInference),
            Arc::new(InferenceAdapter::new(inference_service)),
            Arc::new(GitserverYamlReader::new(git)),
            &mut env,
        )?);

        let indexing_store = Arc::new(DependencyIndexingJobStore::new(pool.clone()));
        let indexing_worker = WorkerRoutine::new(
            indexing_store.clone(),
            Arc::new(IndexingSchedulerWorker {
                handler: indexing::DependencyIndexingSchedulerHandler {
                    uploads: Arc::new(UploadsAdapter::new(pool.clone())),
                    repos: Arc::new(ReposAdapter::new(RepoStore::new(pool.clone()))),
                    external_services: Arc::new(IndexingExternalServicesAdapter::new(
                        ExternalServiceStore::new(pool.clone()),
                    )),
                    gitserver_repos: Arc::new(GitserverRepoStoreAdapter::new(
                        GitserverReposStore::new(pool.clone()),
                    )),
                    index_enqueuer: Arc::new(IndexEnqueuerAdapter::new(enqueuer)),
                    worker_store: indexing_store.clone(),
                    settings: indexing::HandlerSettings {
                        auto_indexing_enabled,
                        disabled: index_scheduler_disabled(),
                    },
                },
            }),
            WorkerOptions {
                name: INDEXING_WORKER_NAME.to_owned(),
                description: INDEXING_WORKER_DESCRIPTION.to_owned(),
                interval: self.config.indexer_scheduler_poll_interval(),
                heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
                num_handlers: usize::try_from(self.config.indexer_scheduler_concurrency())
                    .unwrap_or_default(),
                worker_hostname: crate::framework::dbworker::default_hostname(),
            },
        );

        let index_resetter = ResetterRoutine::new(
            Arc::new(IndexResetter::new(pool.clone())),
            ResetterOptions {
                name: super::codeintel_dependency_jobs::INDEX_RESETTER_NAME.to_owned(),
                interval: self.config.resetter_interval(),
            },
        );

        let dependency_index_resetter = ResetterRoutine::new(
            Arc::new(super::codeintel_dependency_jobs::DependencyIndexResetter::new(pool.clone())),
            ResetterOptions {
                name: super::codeintel_dependency_jobs::DEPENDENCY_INDEX_RESETTER_NAME.to_owned(),
                interval: self.config.resetter_interval(),
            },
        );

        Ok(vec![
            BackgroundRoutine::DbBacked(Arc::new(sync_worker)),
            BackgroundRoutine::DbBacked(Arc::new(indexing_worker)),
            BackgroundRoutine::DbBacked(Arc::new(index_resetter)),
            BackgroundRoutine::DbBacked(Arc::new(dependency_index_resetter)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::{LiveConfig, Unified};

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool")
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let config = DependenciesConfig::default();
        config.load(&mut EnvReader::new());
        assert_eq!(config.resetter_interval(), Duration::from_secs(30));
        assert_eq!(
            config.sync_scheduler_poll_interval(),
            Duration::from_secs(1)
        );
        assert_eq!(
            config.indexer_scheduler_poll_interval(),
            Duration::from_secs(1)
        );
        assert_eq!(config.indexer_scheduler_concurrency(), 1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn go_zero_time_is_year_one() {
        let zero = go_zero_time();
        assert_eq!(zero.year(), 1);
        assert_eq!(zero.month() as u8, 1);
        assert_eq!(zero.day(), 1);
        assert_eq!(zero.hour(), 0);
    }

    #[test]
    fn index_scheduler_disabled_parses_go_bool() {
        // The helper is exercised through the process environment; the
        // baseline parses the same values with `strconv.ParseBool`.
        for raw in ["1", "t", "TRUE", "true", "True"] {
            assert!(parse_go_bool(raw).unwrap_or(false), "{raw}");
        }
        for raw in ["0", "f", "FALSE", "false", "False"] {
            assert!(!parse_go_bool(raw).unwrap_or(false), "{raw}");
        }
        assert!(!parse_go_bool("garbage").unwrap_or(false));
    }

    #[tokio::test]
    async fn routines_match_the_baseline_order() {
        let job = AutoindexingDependencyScheduler::new();
        let deps = JobDeps {
            frontend: lazy_pool(),
            live_config: Arc::new(LiveConfig::new(Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = job
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");
        let names: Vec<&str> = routines.iter().map(|routine| routine.name()).collect();
        assert_eq!(
            names,
            vec![
                "precise_code_intel_dependency_sync_scheduler_worker",
                "precise_code_intel_dependency_indexing_scheduler_worker",
                "precise_code_intel_index_worker_resetter",
                "precise_code_intel_dependency_index_worker_resetter",
            ]
        );
    }
}
