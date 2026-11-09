#![forbid(unsafe_code)]

//! Exhaustive search job dbworkers
//! (`internal/search/exhaustive/store/exhaustive_search_jobs.go` and friends).
//!
//! The baseline constructs three generic `dbworkerstore.New` instances:
//!
//! * `exhaustive_search_worker_store` over `exhaustive_search_jobs`
//!   (`NewExhaustiveSearchJobWorkerStore`);
//! * `exhaustive_search_repo_worker_store` over `exhaustive_search_repo_jobs`
//!   (`NewRepoSearchJobWorkerStore`);
//! * `exhaustive_search_repo_revision_worker_store` over
//!   `exhaustive_search_repo_revision_jobs` (`NewRevSearchJobWorkerStore`).
//!
//! All three use `ORDER BY <table>.state = 'errored', <table>.updated_at DESC`,
//! `StalledMaxAge` 60s, `MaxNumResets` 5, `RetryAfter` 5s and
//! `MaxNumRetries` 3. The SQL lives in [`sg_db::exhaustive_search`]; this
//! module only adapts the stores to the shared
//! [`crate::framework::dbworker`] traits and exposes the baseline
//! constructors.
//!
//! Not ported yet: the worker routines themselves (the job/repo/rev handlers,
//! the resetter wiring, and the `exhaustive-search-job` registration). Their
//! options are recorded here so the next slice can construct them without
//! re-deriving the baseline values.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;

use sg_db::exhaustive_search::{
    ExhaustiveSearchJob, ExhaustiveSearchJobWorkerStore, ExhaustiveSearchRepoJob,
    ExhaustiveSearchRepoJobWorkerStore, ExhaustiveSearchRepoRevisionJob,
    ExhaustiveSearchRepoRevisionJobWorkerStore, WORKER_MAX_NUM_RESETS, WORKER_MAX_NUM_RETRIES,
    WORKER_RETRY_AFTER_SECS, WORKER_STALLED_MAX_AGE_SECS,
};

use crate::framework::dbworker::{DbRecord, DbResetter, DbWorkerStore};

/// `exhaustiveSearchJobWorkerOpts.Name`.
pub const EXHAUSTIVE_SEARCH_JOB_STORE_NAME: &str = "exhaustive_search_worker_store";
/// `repoSearchJobWorkerOpts.Name`.
pub const EXHAUSTIVE_SEARCH_REPO_JOB_STORE_NAME: &str = "exhaustive_search_repo_worker_store";
/// `revSearchJobWorkerOpts.Name`.
pub const EXHAUSTIVE_SEARCH_REPO_REVISION_JOB_STORE_NAME: &str =
    "exhaustive_search_repo_revision_worker_store";

/// All three stores' `StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(WORKER_STALLED_MAX_AGE_SECS as u64);
/// All three stores' `RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(WORKER_RETRY_AFTER_SECS as u64);
/// All three stores' `MaxNumResets`.
pub const MAX_NUM_RESETS: i32 = WORKER_MAX_NUM_RESETS;
/// All three stores' `MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = WORKER_MAX_NUM_RETRIES;

/// `NewExhaustiveSearchJobWorkerStore`.
pub fn new_exhaustive_search_job_worker_store(pool: PgPool) -> ExhaustiveSearchJobWorkerStore {
    ExhaustiveSearchJobWorkerStore::new(pool)
}

/// `NewRepoSearchJobWorkerStore`.
pub fn new_repo_search_job_worker_store(pool: PgPool) -> ExhaustiveSearchRepoJobWorkerStore {
    ExhaustiveSearchRepoJobWorkerStore::new(pool)
}

/// `NewRevSearchJobWorkerStore`.
pub fn new_rev_search_job_worker_store(pool: PgPool) -> ExhaustiveSearchRepoRevisionJobWorkerStore {
    ExhaustiveSearchRepoRevisionJobWorkerStore::new(pool)
}

impl DbRecord for ExhaustiveSearchJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

impl DbRecord for ExhaustiveSearchRepoJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

impl DbRecord for ExhaustiveSearchRepoRevisionJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

fn parse_ids(ids: &[String]) -> Result<Vec<i32>, String> {
    let mut parsed = Vec::with_capacity(ids.len());
    for id in ids {
        parsed.push(
            id.parse::<i32>()
                .map_err(|err| format!("invalid record id {id:?}: {err}"))?,
        );
    }
    Ok(parsed)
}

fn string_ids(ids: Vec<i32>) -> Vec<String> {
    ids.into_iter().map(|id| id.to_string()).collect()
}

fn record_id_i32(record_id: i64) -> Result<i32, String> {
    i32::try_from(record_id).map_err(|err| format!("invalid record id {record_id}: {err}"))
}

fn ages_to_i64(ages: Vec<(i32, Duration)>) -> Vec<(i64, Duration)> {
    ages.into_iter()
        .map(|(id, age)| (i64::from(id), age))
        .collect()
}

#[async_trait]
impl DbWorkerStore for ExhaustiveSearchJobWorkerStore {
    type Record = ExhaustiveSearchJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        self.dequeue(worker_hostname, sg_db::OffsetDateTime::now_utc())
            .await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed = parse_ids(ids)?;
        let (known, canceled) = self
            .heartbeat(&parsed, sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((string_ids(known), string_ids(canceled)))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        self.mark_complete(record_id_i32(record.record_id())?).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_errored(record_id_i32(record.record_id())?, failure_message)
            .await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_failed(record_id_i32(record.record_id())?, failure_message)
            .await
    }
}

#[async_trait]
impl DbResetter for ExhaustiveSearchJobWorkerStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) = self
            .reset_stalled_at(sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((ages_to_i64(reset), ages_to_i64(failed)))
    }
}

#[async_trait]
impl DbWorkerStore for ExhaustiveSearchRepoJobWorkerStore {
    type Record = ExhaustiveSearchRepoJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        self.dequeue(worker_hostname, sg_db::OffsetDateTime::now_utc())
            .await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed = parse_ids(ids)?;
        let (known, canceled) = self
            .heartbeat(&parsed, sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((string_ids(known), string_ids(canceled)))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        self.mark_complete(record_id_i32(record.record_id())?).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_errored(record_id_i32(record.record_id())?, failure_message)
            .await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_failed(record_id_i32(record.record_id())?, failure_message)
            .await
    }
}

#[async_trait]
impl DbResetter for ExhaustiveSearchRepoJobWorkerStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) = self
            .reset_stalled_at(sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((ages_to_i64(reset), ages_to_i64(failed)))
    }
}

#[async_trait]
impl DbWorkerStore for ExhaustiveSearchRepoRevisionJobWorkerStore {
    type Record = ExhaustiveSearchRepoRevisionJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        self.dequeue(worker_hostname, sg_db::OffsetDateTime::now_utc())
            .await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed = parse_ids(ids)?;
        let (known, canceled) = self
            .heartbeat(&parsed, sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((string_ids(known), string_ids(canceled)))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        self.mark_complete(record_id_i32(record.record_id())?).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_errored(record_id_i32(record.record_id())?, failure_message)
            .await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        self.mark_failed(record_id_i32(record.record_id())?, failure_message)
            .await
    }
}

#[async_trait]
impl DbResetter for ExhaustiveSearchRepoRevisionJobWorkerStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let (reset, failed) = self
            .reset_stalled_at(sg_db::OffsetDateTime::now_utc())
            .await?;
        Ok((ages_to_i64(reset), ages_to_i64(failed)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_names_match_baseline() {
        assert_eq!(
            EXHAUSTIVE_SEARCH_JOB_STORE_NAME,
            "exhaustive_search_worker_store"
        );
        assert_eq!(
            EXHAUSTIVE_SEARCH_REPO_JOB_STORE_NAME,
            "exhaustive_search_repo_worker_store"
        );
        assert_eq!(
            EXHAUSTIVE_SEARCH_REPO_REVISION_JOB_STORE_NAME,
            "exhaustive_search_repo_revision_worker_store"
        );
    }

    #[test]
    fn store_options_match_baseline() {
        assert_eq!(STALLED_MAX_AGE, Duration::from_secs(60));
        assert_eq!(RETRY_AFTER, Duration::from_secs(5));
        assert_eq!(MAX_NUM_RESETS, 5);
        assert_eq!(MAX_NUM_RETRIES, 3);
    }

    #[test]
    fn ids_round_trip() {
        assert_eq!(parse_ids(&["1".into(), "2".into()]).unwrap(), vec![1, 2]);
        assert_eq!(string_ids(vec![1, 2]), vec!["1".to_owned(), "2".to_owned()]);
        assert!(parse_ids(&["x".into()]).is_err());
        assert_eq!(record_id_i32(7).unwrap(), 7);
    }
}
