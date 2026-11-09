#![forbid(unsafe_code)]

//! The dbworker stores for the `codeintel-autoindexing-dependency-scheduler`
//! job (`internal/codeintel/autoindexing/internal/background/init.go:52`):
//!
//! * [`DependencySyncingJobStore`] over `lsif_dependency_syncing_jobs`
//!   (`DependencySyncingJobWorkerStoreOptions`);
//! * [`DependencyIndexingJobStore`] over `lsif_dependency_indexing_jobs`
//!   (`DependencyIndexingJobWorkerStoreOptions`);
//! * [`DependencyIndexResetter`] (`precise_code_intel_dependency_index_worker_resetter`);
//! * [`IndexResetter`] (`precise_code_intel_index_worker_resetter`) over
//!   `lsif_indexes`.
//!
//! Both queues use `StalledMaxAge = 25s`, `MaxNumResets = 3` and no
//! `RetryAfter`/`MaxNumRetries`; the SQL (and its quirks) lives in
//! `sg-db::codeintel_dependency_jobs`.
//!
//! The store objects also implement the handler-side seams from M9.8dg
//! ([`super::codeintel_autoindexing_dependency_indexing_scheduler::DependencyIndexingWorkerStore`]'s
//! `Requeue`) so the job wrapper can pass one object on both sides once the
//! production adapters land (next slice).

use std::time::Duration;

use async_trait::async_trait;
use sg_db::codeintel_dependency_jobs as jobs;
use sg_db::OffsetDateTime;
use sqlx::PgPool;

use super::codeintel_autoindexing_dependency_indexing_scheduler::DependencyIndexingWorkerStore;
use crate::framework::dbworker::{DbRecord, DbResetter, DbWorkerStore};

/// `NewDependencySyncScheduler`'s `workerutil.WorkerOptions.Name`.
pub const SYNC_WORKER_NAME: &str = "precise_code_intel_dependency_sync_scheduler_worker";
/// `NewDependencySyncScheduler`'s description.
pub const SYNC_WORKER_DESCRIPTION: &str =
    "reads dependency package references from code-intel uploads to be synced to the instance";
/// `NewDependencyIndexingScheduler`'s `workerutil.WorkerOptions.Name`.
pub const INDEXING_WORKER_NAME: &str = "precise_code_intel_dependency_indexing_scheduler_worker";
/// `NewDependencyIndexingScheduler`'s description.
pub const INDEXING_WORKER_DESCRIPTION: &str =
    "queues code-intel auto-indexing jobs for dependency packages";
/// `dbworker.WorkerOptions.HeartbeatInterval` for both workers.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// `NewIndexResetter`'s `ResetterOptions.Name`.
pub const INDEX_RESETTER_NAME: &str = "precise_code_intel_index_worker_resetter";
/// `NewDependencyIndexResetter`'s `ResetterOptions.Name`.
pub const DEPENDENCY_INDEX_RESETTER_NAME: &str =
    "precise_code_intel_dependency_index_worker_resetter";

/// `dependencySyncingJob` as dequeued by the generic store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencySyncingJobRecord(pub jobs::DependencySyncingJobRow);

impl DbRecord for DependencySyncingJobRecord {
    fn record_id(&self) -> i64 {
        i64::from(self.0.id)
    }
}

/// `dependencyIndexingJob` as dequeued by the generic store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyIndexingJobRecord(pub jobs::DependencyIndexingJobRow);

impl DbRecord for DependencyIndexingJobRecord {
    fn record_id(&self) -> i64 {
        i64::from(self.0.id)
    }
}

/// The `lsif_dependency_syncing_jobs` store.
pub struct DependencySyncingJobStore {
    pool: PgPool,
}

impl DependencySyncingJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbWorkerStore for DependencySyncingJobStore {
    type Record = DependencySyncingJobRecord;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        jobs::dequeue_syncing_job(&self.pool, OffsetDateTime::now_utc(), worker_hostname)
            .await
            .map(|record| record.map(DependencySyncingJobRecord))
            .map_err(|err| err.to_string())
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let (known, canceled) = jobs::heartbeat(
            &self.pool,
            jobs::SYNCING_JOBS_TABLE,
            OffsetDateTime::now_utc(),
            ids,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        jobs::mark_complete(&self.pool, jobs::SYNCING_JOBS_TABLE, record.0.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        jobs::mark_errored(
            &self.pool,
            jobs::SYNCING_JOBS_TABLE,
            record.0.id,
            failure_message,
            jobs::MAX_NUM_RETRIES,
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        jobs::mark_failed(
            &self.pool,
            jobs::SYNCING_JOBS_TABLE,
            record.0.id,
            failure_message,
        )
        .await
        .map_err(|err| err.to_string())
    }
}

/// The `lsif_dependency_indexing_jobs` store.
pub struct DependencyIndexingJobStore {
    pool: PgPool,
}

impl DependencyIndexingJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbWorkerStore for DependencyIndexingJobStore {
    type Record = DependencyIndexingJobRecord;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        jobs::dequeue_indexing_job(&self.pool, OffsetDateTime::now_utc(), worker_hostname)
            .await
            .map(|record| record.map(DependencyIndexingJobRecord))
            .map_err(|err| err.to_string())
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let (known, canceled) = jobs::heartbeat(
            &self.pool,
            jobs::INDEXING_JOBS_TABLE,
            OffsetDateTime::now_utc(),
            ids,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        jobs::mark_complete(&self.pool, jobs::INDEXING_JOBS_TABLE, record.0.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        jobs::mark_errored(
            &self.pool,
            jobs::INDEXING_JOBS_TABLE,
            record.0.id,
            failure_message,
            jobs::MAX_NUM_RETRIES,
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        jobs::mark_failed(
            &self.pool,
            jobs::INDEXING_JOBS_TABLE,
            record.0.id,
            failure_message,
        )
        .await
        .map_err(|err| err.to_string())
    }
}

/// `workerStore.Requeue` used by the indexing scheduler handler (M9.8dg).
#[async_trait]
impl DependencyIndexingWorkerStore for DependencyIndexingJobStore {
    async fn requeue(&self, id: i64, process_after: OffsetDateTime) -> Result<(), String> {
        let id = i32::try_from(id).map_err(|err| err.to_string())?;
        jobs::requeue(&self.pool, jobs::INDEXING_JOBS_TABLE, id, process_after)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `dbworker.NewResetter` over `lsif_dependency_indexing_jobs`.
pub struct DependencyIndexResetter {
    pool: PgPool,
}

impl DependencyIndexResetter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbResetter for DependencyIndexResetter {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let now = OffsetDateTime::now_utc();
        let (reset, failed) = jobs::reset_stalled(
            &self.pool,
            jobs::INDEXING_JOBS_TABLE,
            now,
            jobs::STALLED_MAX_AGE_SECONDS,
            jobs::MAX_NUM_RESETS,
            jobs::DEFAULT_RESET_FAILURE_MESSAGE,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok((
            reset
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
            failed
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
        ))
    }
}

/// `NewIndexResetter` over `lsif_indexes` (the auto-index queue, not a
/// dependency queue).
pub struct IndexResetter {
    pool: PgPool,
}

impl IndexResetter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbResetter for IndexResetter {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let now = OffsetDateTime::now_utc();
        let (reset, failed) = jobs::reset_stalled(
            &self.pool,
            jobs::LSIF_INDEXES_TABLE,
            now,
            jobs::STALLED_MAX_AGE_SECONDS,
            jobs::MAX_NUM_RESETS,
            jobs::DEFAULT_RESET_FAILURE_MESSAGE,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok((
            reset
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
            failed
                .into_iter()
                .map(|(id, age)| (i64::from(id), age))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool")
    }

    #[test]
    fn worker_metadata_matches_the_baseline() {
        assert_eq!(
            SYNC_WORKER_NAME,
            "precise_code_intel_dependency_sync_scheduler_worker"
        );
        assert_eq!(
            INDEXING_WORKER_NAME,
            "precise_code_intel_dependency_indexing_scheduler_worker"
        );
        assert_eq!(
            INDEX_RESETTER_NAME,
            "precise_code_intel_index_worker_resetter"
        );
        assert_eq!(
            DEPENDENCY_INDEX_RESETTER_NAME,
            "precise_code_intel_dependency_index_worker_resetter"
        );
    }

    #[test]
    fn records_report_the_numeric_id() {
        let syncing = DependencySyncingJobRecord(jobs::DependencySyncingJobRow {
            id: 42,
            state: "processing".to_owned(),
            failure_message: None,
            started_at: None,
            finished_at: None,
            process_after: None,
            num_resets: 0,
            num_failures: 0,
            upload_id: Some(7),
        });
        assert_eq!(syncing.record_id(), 42);
        assert_eq!(syncing.record_uid(), "42");

        let indexing = DependencyIndexingJobRecord(jobs::DependencyIndexingJobRow {
            id: 43,
            state: "queued".to_owned(),
            failure_message: None,
            started_at: None,
            finished_at: None,
            process_after: None,
            num_resets: 0,
            num_failures: 0,
            upload_id: Some(8),
            external_service_kind: String::new(),
            external_service_sync: None,
        });
        assert_eq!(indexing.record_id(), 43);
    }

    #[tokio::test]
    async fn requeue_rejects_ids_that_do_not_fit_the_column() {
        let store = DependencyIndexingJobStore::new(lazy_pool());
        let error = store
            .requeue(i64::from(i32::MAX) + 1, OffsetDateTime::now_utc())
            .await
            .expect_err("out-of-range id");
        assert!(error.contains("too large"), "unexpected error: {error}");
    }
}
