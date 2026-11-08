#![forbid(unsafe_code)]

//! `repo-perms-syncer` (`cmd/worker/internal/authz/perms_syncer_job.go` +
//! `perms_syncer_worker.go`).
//!
//! The job owns `permsSyncer` plus two queue workers over
//! `permission_sync_jobs` and one stalled-record resetter:
//!
//! * `repo_permissions_sync_job_worker` — one handler, dequeues rows with
//!   `repository_id IS NOT NULL`;
//! * `user_permissions_sync_job_worker` — `PermissionsSyncUsersMaxConcurrency`
//!   handlers (at least one), dequeues rows with `user_id IS NOT NULL`;
//! * `permissions_sync_job_worker_resetter` — every 30 seconds over the repo
//!   store (the store name is what matters in the baseline, and the resetter
//!   has its own name for logging/metrics).
//!
//! The handler calls `syncUserPerms` / `syncRepoPerms`, turns "all providers
//! failed" into an error, and stores the result with `SaveSyncResult`.
//!
//! Porting notes (docs/modules/06_worker.md §58):
//!
//! * `providers.ProvidersFromConfig` is not ported yet, so the registry is
//!   empty: the queue, handler and storage paths run end to end, but no code
//!   host is queried and every sync is a successful no-op. This is the next
//!   slice's main gap.
//! * Prometheus metrics (`workerutil.NewMetrics`,
//!   `dbworker.NewResetterMetrics`) and the debug/error log lines are not
//!   ported, consistent with the other worker slices.
//! * `PreDequeue` becomes the store's fixed predicate, since both stores are
//!   constructed per sync type (the baseline returns the predicate on every
//!   dequeue).

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::permission_sync_jobs::{
    PermissionSyncJob, PermissionSyncJobType, PermissionSyncJobsWorkerStore,
};
use sg_db::perms_store::{count_statuses, save_sync_result, CodeHostState, SetPermissionsResult};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use super::perms_syncer::{AuthzProvider, FetchPermsOptions, PermsSyncer, ProviderRegistry};
use super::perms_syncer_fetch::ExternalAccountPermsFetcher;
use super::{Job, JobDeps};
use crate::config::combine_errors;
use crate::framework::dbworker::{
    DbBackedRoutine, DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore, ResetterOptions,
    ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::BackgroundRoutine;

/// `workerutil.WorkerOptions.Name` for the repository queue.
pub const REPO_WORKER_NAME: &str = "repo_permissions_sync_job_worker";
/// `workerutil.WorkerOptions.Name` for the user queue.
pub const USER_WORKER_NAME: &str = "user_permissions_sync_job_worker";
/// `dbworker.ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "permissions_sync_job_worker_resetter";
/// `Job.Description`.
pub const DESCRIPTION: &str =
    "Background job that syncs repository permissions from code hosts to the database.";

/// `permsSyncerJob`.
#[derive(Default)]
pub struct PermsSyncerJob;

impl PermsSyncerJob {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for PermsSyncerJob {
    fn description(&self) -> String {
        DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let pool = deps.frontend.clone();
        let registry: Arc<dyn ProviderRegistry> = Arc::new(NoAuthzProviders);
        let fetcher = Arc::new(ExternalAccountPermsFetcher::new(
            pool.clone(),
            Arc::clone(&registry),
        ));
        let syncer = Arc::new(PermsSyncer::new(pool.clone(), registry, fetcher));

        let repo_store = Arc::new(PermissionSyncJobsWorkerStore::new(
            pool.clone(),
            PermissionSyncJobType::Repo,
        ));
        let user_store = Arc::new(PermissionSyncJobsWorkerStore::new(
            pool.clone(),
            PermissionSyncJobType::User,
        ));

        let repo_worker = WorkerRoutine::new(
            Arc::clone(&repo_store),
            Arc::new(PermsSyncerWorkerHandler::new(
                pool.clone(),
                Arc::clone(&syncer),
            )),
            WorkerOptions {
                name: REPO_WORKER_NAME.to_owned(),
                description: String::new(),
                interval: std::time::Duration::from_secs(1),
                heartbeat_interval: std::time::Duration::from_secs(10),
                num_handlers: 1,
                worker_hostname: crate::framework::dbworker::default_hostname(),
            },
        );

        let user_worker = WorkerRoutine::new(
            Arc::clone(&user_store),
            Arc::new(PermsSyncerWorkerHandler::new(pool, syncer)),
            WorkerOptions {
                name: USER_WORKER_NAME.to_owned(),
                description: String::new(),
                interval: std::time::Duration::from_secs(1),
                heartbeat_interval: std::time::Duration::from_secs(10),
                num_handlers: sync_users_max_concurrency(&deps.live_config.current().site),
                worker_hostname: crate::framework::dbworker::default_hostname(),
            },
        );

        let resetter = ResetterRoutine::new(
            repo_store as Arc<dyn DbResetter>,
            ResetterOptions {
                name: RESETTER_NAME.to_owned(),
                interval: std::time::Duration::from_secs(30),
            },
        );

        Ok(vec![
            BackgroundRoutine::DbBacked(Arc::new(repo_worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(user_worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(resetter) as Arc<dyn DbBackedRoutine>),
        ])
    }
}

/// `syncUsersMaxConcurrency`: `conf.Get().PermissionsSyncUsersMaxConcurrency`,
/// defaulting to 1 when unset or non-positive.
pub fn sync_users_max_concurrency(site: &sg_config::SiteConfiguration) -> usize {
    match site.permissions_sync_users_max_concurrency() {
        value if value > 0 => value as usize,
        _ => 1,
    }
}

/// `providers.ProvidersFromConfig` is not ported yet; the handler and queue
/// machinery are exercised with no configured provider.
pub struct NoAuthzProviders;

impl ProviderRegistry for NoAuthzProviders {
    fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        Vec::new()
    }
}

/// `permsSyncerWorker.Handle` / `handlePermsSync`.
pub struct PermsSyncerWorkerHandler {
    pool: PgPool,
    syncer: Arc<PermsSyncer>,
}

impl PermsSyncerWorkerHandler {
    pub fn new(pool: PgPool, syncer: Arc<PermsSyncer>) -> Self {
        Self { pool, syncer }
    }
}

#[async_trait]
impl DbWorkerHandler<PermissionSyncJobsWorkerStore> for PermsSyncerWorkerHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &PermissionSyncJob,
    ) -> Result<(), String> {
        let opts = FetchPermsOptions {
            invalidate_caches: record.invalidate_caches,
        };

        let (result, statuses, mut error): (
            Option<SetPermissionsResult>,
            Vec<CodeHostState>,
            Option<String>,
        ) = match record.repository_id {
            Some(repo_id) => {
                let outcome = self
                    .syncer
                    .sync_repo_perms(repo_id, record.no_perms, opts)
                    .await;
                (outcome.result, outcome.statuses, outcome.error)
            }
            None => {
                let user_id = record.user_id.ok_or_else(|| {
                    "permission sync job has neither repository nor user".to_owned()
                })?;
                let outcome = self
                    .syncer
                    .sync_user_perms(user_id, record.no_perms, opts)
                    .await;
                (outcome.result, outcome.statuses, outcome.error)
            }
        };

        if error.is_none() {
            let (total, _, failed) = count_statuses(&statuses);
            if failed == total && total > 0 {
                error = Some("All providers failed to sync permissions.".to_owned());
            }
        }

        if let Err(save_err) = save_sync_result(
            &self.pool,
            record.id,
            error.is_none(),
            result.as_ref(),
            &statuses,
        )
        .await
        {
            error = combine_errors(&[error.unwrap_or_default(), save_err.to_string()])
                .or_else(|| Some(save_err.to_string()));
        }

        match error {
            Some(message) => Err(message),
            None => Ok(()),
        }
    }
}

impl DbRecord for PermissionSyncJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

#[async_trait]
impl DbWorkerStore for PermissionSyncJobsWorkerStore {
    type Record = PermissionSyncJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        PermissionSyncJobsWorkerStore::dequeue(self, worker_hostname)
            .await
            .map_err(|err| err.to_string())
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let mut parsed = Vec::with_capacity(ids.len());
        for id in ids {
            parsed.push(
                id.parse::<i32>()
                    .map_err(|err| format!("invalid record id {id:?}: {err}"))?,
            );
        }
        let (known, canceled) = PermissionSyncJobsWorkerStore::heartbeat(self, &parsed)
            .await
            .map_err(|err| err.to_string())?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        PermissionSyncJobsWorkerStore::mark_complete(self, record.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        PermissionSyncJobsWorkerStore::mark_errored(self, record.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        PermissionSyncJobsWorkerStore::mark_failed(self, record.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }
}

#[async_trait]
impl DbResetter for PermissionSyncJobsWorkerStore {
    async fn reset_stalled(
        &self,
    ) -> Result<
        (
            Vec<(i64, std::time::Duration)>,
            Vec<(i64, std::time::Duration)>,
        ),
        String,
    > {
        let result = PermissionSyncJobsWorkerStore::reset_stalled(self)
            .await
            .map_err(|err| err.to_string())?;
        Ok((
            result
                .reset
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
            result
                .failed
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
        ))
    }
}
