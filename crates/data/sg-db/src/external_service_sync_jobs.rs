//! Port of the `external_service_sync_jobs` database store used by the
//! repo-updater syncer:
//!
//! - `internal/repos/store.go` (`EnqueueSyncJobs`, `EnqueueSingleSyncJob`,
//!   `ListSyncJobs`)
//! - `internal/workerutil/dbworker/store` as configured in
//!   `internal/repos/sync_worker.go` (`Dequeue`, `Heartbeat`, `MarkComplete`,
//!   `MarkErrored`, `ResetStalled`)
//! - `internal/database/external_services.go`
//!   (`UpdateSyncJobCounters`, `CleanupSyncJobs`)
//!
//! The worker is configured with `MaxNumRetries: 0`, `RetryAfter: 0`,
//! `StalledMaxAge: 30s` and `MaxNumResets: 5`; the SQL below reproduces that
//! configuration.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `repos.SyncJob` (the subset the syncer reads). The debug dump
/// (`types.ExternalServiceSyncJob`) also uses `queued_at`, `cancel` and the
/// progress counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncJob {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: Option<OffsetDateTime>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub external_service_id: i64,
    pub next_sync_at: Option<OffsetDateTime>,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub cancel: bool,
    pub repos_synced: i32,
    pub repo_sync_errors: i32,
    pub repos_added: i32,
    pub repos_deleted: i32,
    pub repos_modified: i32,
    pub repos_unmodified: i32,
}

/// `types.ExternalServiceSyncJob` progress counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncJobCounters {
    pub repos_synced: i32,
    pub repo_sync_errors: i32,
    pub repos_added: i32,
    pub repos_modified: i32,
    pub repos_unmodified: i32,
    pub repos_deleted: i32,
}

/// Ids reset by [`SyncJobsStore::reset_stalled`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetStalledResult {
    /// Records moved back to `queued`.
    pub reset: Vec<i32>,
    /// Records that exceeded `MaxNumResets` and were marked `failed`.
    pub failed: Vec<i32>,
}

#[derive(Clone)]
pub struct SyncJobsStore {
    pool: PgPool,
}

impl SyncJobsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `repos.Store.EnqueueSyncJobs`: enqueues one job per due external service
    /// that is not already queued or processing.
    pub async fn enqueue_sync_jobs(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
WITH due AS (
    SELECT id
    FROM external_services
    WHERE (next_sync_at <= clock_timestamp() OR next_sync_at IS NULL)
    AND deleted_at IS NULL
    AND LOWER(kind) != 'phabricator'
    FOR UPDATE OF external_services
),
busy AS (
    SELECT DISTINCT external_service_id id FROM external_service_sync_jobs
    WHERE state = 'queued'
    OR state = 'processing'
)
INSERT INTO external_service_sync_jobs (external_service_id)
SELECT id from due EXCEPT SELECT id from busy
"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// `repos.Store.EnqueueSingleSyncJob`.
    pub async fn enqueue_single_sync_job(
        &self,
        external_service_id: i64,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
WITH es AS (
	SELECT id
	FROM external_services es
	WHERE
		id = $1
		AND deleted_at IS NULL
	FOR UPDATE
)
INSERT INTO external_service_sync_jobs (external_service_id)
SELECT es.id
FROM es
WHERE NOT EXISTS (
	SELECT 1
	FROM external_service_sync_jobs j
	WHERE
		es.id = j.external_service_id
		AND j.state IN ('queued', 'processing')
)
"#,
        )
        .bind(external_service_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// `workerutil/dbworker/store.Dequeue` with the repo-sync worker options.
    pub async fn dequeue(&self, worker_hostname: &str) -> Result<Option<SyncJob>, sqlx::Error> {
        let now = OffsetDateTime::now_utc();
        let row = sqlx::query(
            r#"
WITH potential_candidates AS (
	SELECT
		j.id AS candidate_id,
		j.next_sync_at AS candidate_order
	FROM external_service_sync_jobs_with_next_sync_at j
	WHERE
		j.state = 'queued' AND
		(j.process_after IS NULL OR j.process_after <= $1)
	ORDER BY j.next_sync_at
	LIMIT 50
),
candidate AS (
	SELECT j.id FROM external_service_sync_jobs j
	JOIN potential_candidates pc ON pc.candidate_id = j.id
	WHERE
		j.state IN ('queued', 'errored')
	ORDER BY pc.candidate_order
	FOR UPDATE OF j SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE external_service_sync_jobs
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $2
	WHERE id IN (SELECT id FROM candidate)
	RETURNING id, state, failure_message, queued_at, started_at, finished_at,
	          process_after, num_resets, num_failures, external_service_id,
	          last_heartbeat_at, worker_hostname, cancel,
	          repos_synced, repo_sync_errors, repos_added, repos_deleted,
	          repos_modified, repos_unmodified
)
SELECT u.*, v.next_sync_at
FROM updated_record u
JOIN external_service_sync_jobs_with_next_sync_at v ON v.id = u.id
"#,
        )
        .bind(now)
        .bind(worker_hostname)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SyncJob {
                id: row.try_get("id")?,
                state: row.try_get("state")?,
                failure_message: row.try_get("failure_message")?,
                queued_at: row.try_get("queued_at")?,
                started_at: row.try_get("started_at")?,
                finished_at: row.try_get("finished_at")?,
                process_after: row.try_get("process_after")?,
                num_resets: row.try_get("num_resets")?,
                num_failures: row.try_get("num_failures")?,
                external_service_id: row.try_get("external_service_id")?,
                next_sync_at: row.try_get("next_sync_at")?,
                last_heartbeat_at: row.try_get("last_heartbeat_at")?,
                worker_hostname: row.try_get("worker_hostname")?,
                cancel: row.try_get("cancel")?,
                repos_synced: row.try_get("repos_synced")?,
                repo_sync_errors: row.try_get("repo_sync_errors")?,
                repos_added: row.try_get("repos_added")?,
                repos_deleted: row.try_get("repos_deleted")?,
                repos_modified: row.try_get("repos_modified")?,
                repos_unmodified: row.try_get("repos_unmodified")?,
            })
        })
        .transpose()
    }

    /// `Store.Heartbeat`: refreshes `last_heartbeat_at` for processing jobs and
    /// reports which ids are still known and which are canceled.
    pub async fn heartbeat(&self, ids: &[i32]) -> Result<(Vec<i32>, Vec<i32>), sqlx::Error> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query(
            r#"
WITH alive_candidates AS (
	SELECT id
	FROM external_service_sync_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE external_service_sync_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
        )
        .bind(ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        let mut known = Vec::with_capacity(rows.len());
        let mut canceled = Vec::new();
        for row in rows {
            let id: i32 = row.try_get("id")?;
            let cancel: bool = row.try_get("cancel")?;
            known.push(id);
            if cancel {
                canceled.push(id);
            }
        }
        Ok((known, canceled))
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(&self, id: i32) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE external_service_sync_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkErrored` with `MaxNumRetries = 0`: an errored handler always
    /// lands in `failed` (or `canceled` when the cancel flag is set).
    pub async fn mark_errored(&self, id: i32, failure_message: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE external_service_sync_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= 0 THEN 'failed' ELSE 'errored' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .bind(failure_message)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.MarkFailed` (non-retryable handler errors).
    pub async fn mark_failed(&self, id: i32, failure_message: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
UPDATE external_service_sync_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
	finished_at = clock_timestamp(),
	failure_message = $2,
	num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
"#,
        )
        .bind(id)
        .bind(failure_message)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// `Store.ResetStalled` (`StalledMaxAge = 30s`, `MaxNumResets = 5`).
    pub async fn reset_stalled(&self) -> Result<ResetStalledResult, sqlx::Error> {
        const STALLED_MAX_AGE_SECONDS: i64 = 30;
        const MAX_NUM_RESETS: i32 = 5;
        const FAILURE_MESSAGE: &str =
            "job processor died while handling this message too many times";

        let reset_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM external_service_sync_jobs
	WHERE
		state = 'processing' AND
		clock_timestamp() - last_heartbeat_at > ($1 * '1 second'::interval) AND
		num_resets < $2
	FOR UPDATE SKIP LOCKED
)
UPDATE external_service_sync_jobs
SET
	state = 'queued',
	queued_at = clock_timestamp(),
	started_at = null,
	num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id
"#,
        )
        .bind(STALLED_MAX_AGE_SECONDS)
        .bind(MAX_NUM_RESETS)
        .fetch_all(&self.pool)
        .await?;

        let failed_rows = sqlx::query(
            r#"
WITH stalled AS (
	SELECT id FROM external_service_sync_jobs
	WHERE
		state = 'processing' AND
		clock_timestamp() - last_heartbeat_at > ($1 * '1 second'::interval) AND
		num_resets >= $2
	FOR UPDATE SKIP LOCKED
)
UPDATE external_service_sync_jobs
SET
	state = 'failed',
	finished_at = clock_timestamp(),
	failure_message = $3
WHERE id IN (SELECT id FROM stalled)
RETURNING id
"#,
        )
        .bind(STALLED_MAX_AGE_SECONDS)
        .bind(MAX_NUM_RESETS)
        .bind(FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await?;

        Ok(ResetStalledResult {
            reset: reset_rows
                .into_iter()
                .map(|row| row.try_get::<i32, _>("id"))
                .collect::<Result<_, _>>()?,
            failed: failed_rows
                .into_iter()
                .map(|row| row.try_get::<i32, _>("id"))
                .collect::<Result<_, _>>()?,
        })
    }

    /// `ExternalServices.UpdateSyncJobCounters`.
    pub async fn update_counters(
        &self,
        id: i32,
        counters: SyncJobCounters,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
UPDATE external_service_sync_jobs
SET
	repos_synced = $2,
	repo_sync_errors = $3,
	repos_added = $4,
	repos_modified = $5,
	repos_unmodified = $6,
	repos_deleted = $7
WHERE
    id = $1
"#,
        )
        .bind(id)
        .bind(counters.repos_synced)
        .bind(counters.repo_sync_errors)
        .bind(counters.repos_added)
        .bind(counters.repos_modified)
        .bind(counters.repos_unmodified)
        .bind(counters.repos_deleted)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// `ExternalServices.CleanupSyncJobs`: keeps the newest
    /// `max_per_external_service` finished jobs per service and deletes jobs
    /// older than `older_than`.
    pub async fn cleanup_sync_jobs(
        &self,
        older_than: std::time::Duration,
        max_per_external_service: i32,
    ) -> Result<u64, sqlx::Error> {
        assert!(max_per_external_service >= 1);
        let cutoff = OffsetDateTime::now_utc()
            - time::Duration::try_from(older_than).expect("duration in range");
        let result = sqlx::query(
            r#"
WITH ranked_jobs AS (
    SELECT
        id,
        external_service_id,
        state,
        finished_at,
        ROW_NUMBER() OVER (PARTITION BY external_service_id ORDER BY finished_at DESC) as rn
    FROM
        external_service_sync_jobs
    WHERE
        state IN ('completed', 'failed', 'canceled')
		AND
		finished_at IS NOT NULL
)
DELETE FROM
    external_service_sync_jobs
WHERE
    id IN (
        SELECT id FROM ranked_jobs
        WHERE
			rn > $1
			OR
			finished_at < $2
    )
"#,
        )
        .bind(max_per_external_service)
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// `repos.Store.ListSyncJobs` (the debug page and tests).
    pub async fn list_sync_jobs(&self) -> Result<Vec<SyncJob>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
SELECT
	j.id, j.state, j.failure_message, j.queued_at, j.started_at, j.finished_at,
	j.process_after, j.num_resets, j.num_failures, j.external_service_id,
	v.next_sync_at, j.last_heartbeat_at, j.worker_hostname, j.cancel,
	j.repos_synced, j.repo_sync_errors, j.repos_added, j.repos_deleted,
	j.repos_modified, j.repos_unmodified
FROM external_service_sync_jobs j
JOIN external_service_sync_jobs_with_next_sync_at v ON v.id = j.id
ORDER BY j.id
"#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(SyncJob {
                    id: row.try_get("id")?,
                    state: row.try_get("state")?,
                    failure_message: row.try_get("failure_message")?,
                    queued_at: row.try_get("queued_at")?,
                    started_at: row.try_get("started_at")?,
                    finished_at: row.try_get("finished_at")?,
                    process_after: row.try_get("process_after")?,
                    num_resets: row.try_get("num_resets")?,
                    num_failures: row.try_get("num_failures")?,
                    external_service_id: row.try_get("external_service_id")?,
                    next_sync_at: row.try_get("next_sync_at")?,
                    last_heartbeat_at: row.try_get("last_heartbeat_at")?,
                    worker_hostname: row.try_get("worker_hostname")?,
                    cancel: row.try_get("cancel")?,
                    repos_synced: row.try_get("repos_synced")?,
                    repo_sync_errors: row.try_get("repo_sync_errors")?,
                    repos_added: row.try_get("repos_added")?,
                    repos_deleted: row.try_get("repos_deleted")?,
                    repos_modified: row.try_get("repos_modified")?,
                    repos_unmodified: row.try_get("repos_unmodified")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    }

    /// `EnqueueSyncJobs`, `Dequeue` and `CleanupSyncJobs` operate on the whole
    /// table, so the tests that exercise them must not interleave.
    static QUEUE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

    async fn queue_guard() -> tokio::sync::MutexGuard<'static, ()> {
        QUEUE_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    async fn insert_service(
        pool: &PgPool,
        suffix: u128,
        kind: &str,
        next_sync_at: Option<OffsetDateTime>,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config, next_sync_at) \
             VALUES ($1, $2, '{}', $3) RETURNING id",
        )
        .bind(kind)
        .bind(format!("sync-test-{suffix}"))
        .bind(next_sync_at)
        .fetch_one(pool)
        .await
        .expect("insert external service")
    }

    fn hours_ago(hours: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(
            OffsetDateTime::now_utc().unix_timestamp() - hours * 3600,
        )
        .expect("timestamp")
    }

    #[tokio::test]
    async fn enqueues_due_services_without_duplicates() {
        let _guard = queue_guard().await;
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = SyncJobsStore::new(pool.clone());
        let suffix = unique_suffix();
        let due = insert_service(&pool, suffix, "OTHER", Some(hours_ago(1))).await;
        let phab = insert_service(&pool, suffix + 1, "PHABRICATOR", Some(hours_ago(1))).await;
        let future = insert_service(&pool, suffix + 2, "OTHER", Some(hours_ago(-24))).await;

        let result = async {
            store.enqueue_sync_jobs().await.expect("enqueue");
            store.enqueue_sync_jobs().await.expect("enqueue again");

            let jobs: Vec<i64> =
                sqlx::query_scalar("SELECT external_service_id FROM external_service_sync_jobs")
                    .fetch_all(&pool)
                    .await
                    .expect("jobs");
            assert_eq!(jobs.iter().filter(|id| **id == due).count(), 1);
            assert!(!jobs.contains(&phab), "phabricator is excluded");
            assert!(!jobs.contains(&future), "not due yet");

            // An explicit enqueue respects the queued/running guard.
            assert_eq!(store.enqueue_single_sync_job(due).await.expect("single"), 0);
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = ANY($1)")
            .bind([due, phab, future])
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }

    #[tokio::test]
    async fn dequeues_oldest_next_sync_first_and_tracks_heartbeats() {
        let _guard = queue_guard().await;
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = SyncJobsStore::new(pool.clone());
        let suffix = unique_suffix();
        let older = insert_service(&pool, suffix, "OTHER", Some(hours_ago(2))).await;
        let newer = insert_service(&pool, suffix + 1, "OTHER", Some(hours_ago(1))).await;

        let result = async {
            // `enqueue_single_sync_job` keeps the queue scoped to this test;
            // the other tests in this module may still hold live services.
            store
                .enqueue_single_sync_job(older)
                .await
                .expect("enqueue older");
            store
                .enqueue_single_sync_job(newer)
                .await
                .expect("enqueue newer");
            let first = store
                .dequeue("worker-1")
                .await
                .expect("dequeue")
                .expect("job");
            assert_eq!(
                first.external_service_id, older,
                "oldest next_sync_at first"
            );
            assert_eq!(first.state, "processing");
            assert_eq!(first.worker_hostname, "worker-1");
            assert!(first.started_at.is_some());
            assert_eq!(first.last_heartbeat_at, first.started_at);

            let second = store
                .dequeue("worker-2")
                .await
                .expect("dequeue")
                .expect("job");
            assert_eq!(second.external_service_id, newer);

            assert!(
                store.dequeue("worker-3").await.expect("dequeue").is_none(),
                "no queued jobs left"
            );

            let (known, canceled) = store
                .heartbeat(&[first.id, second.id])
                .await
                .expect("heartbeat");
            assert_eq!(known.len(), 2);
            assert!(canceled.is_empty());

            sqlx::query("UPDATE external_service_sync_jobs SET cancel = true WHERE id = $1")
                .bind(first.id)
                .execute(&pool)
                .await
                .expect("set cancel");
            let (_, canceled) = store.heartbeat(&[first.id]).await.expect("heartbeat");
            assert_eq!(canceled, vec![first.id]);

            assert!(store.mark_errored(first.id, "boom").await.expect("errored"));
            assert!(store.mark_complete(second.id).await.expect("complete"));

            let jobs = store.list_sync_jobs().await.expect("list");
            let canceled_job = jobs.iter().find(|job| job.id == first.id).expect("job");
            assert_eq!(canceled_job.state, "canceled");
            assert_eq!(canceled_job.failure_message.as_deref(), Some("boom"));
            let completed = jobs.iter().find(|job| job.id == second.id).expect("job");
            assert_eq!(completed.state, "completed");
            assert!(completed.finished_at.is_some());
            assert_eq!(completed.worker_hostname, "worker-2");
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = ANY($1)")
            .bind([older, newer])
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }

    #[tokio::test]
    async fn resets_stalled_jobs_and_fails_after_max_resets() {
        let _guard = queue_guard().await;
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = SyncJobsStore::new(pool.clone());
        let suffix = unique_suffix();
        let service = insert_service(&pool, suffix, "OTHER", Some(hours_ago(1))).await;

        let result = async {
            store.enqueue_single_sync_job(service).await.expect("enqueue");
            let mut job = store.dequeue("worker").await.expect("dequeue").expect("job");
            for expected_resets in 1..=5 {
                assert!(
                    store.heartbeat(&[job.id]).await.expect("heartbeat").1.is_empty()
                );
                sqlx::query(
                    "UPDATE external_service_sync_jobs SET last_heartbeat_at = clock_timestamp() - '1 minute'::interval WHERE id = $1",
                )
                .bind(job.id)
                .execute(&pool)
                .await
                .expect("age heartbeat");
                let reset = store.reset_stalled().await.expect("reset");
                assert_eq!(reset.reset, vec![job.id], "reset #{expected_resets}");
                assert!(reset.failed.is_empty());
                let num_resets: i32 =
                    sqlx::query_scalar("SELECT num_resets FROM external_service_sync_jobs WHERE id = $1")
                        .bind(job.id)
                        .fetch_one(&pool)
                        .await
                        .expect("num_resets");
                assert_eq!(num_resets, expected_resets);
                job = store.dequeue("worker").await.expect("dequeue").expect("job");
            }

            sqlx::query(
                "UPDATE external_service_sync_jobs SET last_heartbeat_at = clock_timestamp() - '1 minute'::interval WHERE id = $1",
            )
            .bind(job.id)
            .execute(&pool)
            .await
            .expect("age heartbeat");
            let reset = store.reset_stalled().await.expect("reset");
            assert_eq!(reset.failed, vec![job.id]);
            let (state, message): (String, Option<String>) =
                sqlx::query_as("SELECT state, failure_message FROM external_service_sync_jobs WHERE id = $1")
                    .bind(job.id)
                    .fetch_one(&pool)
                    .await
                    .expect("job");
            assert_eq!(state, "failed");
            assert_eq!(
                message.as_deref(),
                Some("job processor died while handling this message too many times")
            );
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service)
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }

    #[tokio::test]
    async fn updates_counters_and_cleans_old_jobs() {
        let _guard = queue_guard().await;
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = SyncJobsStore::new(pool.clone());
        let suffix = unique_suffix();
        let service = insert_service(&pool, suffix, "OTHER", Some(hours_ago(1))).await;

        let result = async {
            store.enqueue_sync_jobs().await.expect("enqueue");
            let job = store.dequeue("worker").await.expect("dequeue").expect("job");
            store
                .update_counters(
                    job.id,
                    SyncJobCounters {
                        repos_synced: 4,
                        repo_sync_errors: 1,
                        repos_added: 1,
                        repos_modified: 1,
                        repos_unmodified: 1,
                        repos_deleted: 1,
                    },
                )
                .await
                .expect("counters");
            let counters: (i32, i32, i32, i32, i32, i32) = sqlx::query_as(
                "SELECT repos_synced, repo_sync_errors, repos_added, repos_modified, repos_unmodified, repos_deleted \
                 FROM external_service_sync_jobs WHERE id = $1",
            )
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .expect("counters");
            assert_eq!(counters, (4, 1, 1, 1, 1, 1));

            // Three finished jobs; the cleanup keeps the newest two, then a
            // second pass with a 1ms TTL removes the rest.
            sqlx::query(
                "UPDATE external_service_sync_jobs SET state = 'completed', finished_at = clock_timestamp() WHERE id = $1",
            )
            .bind(job.id)
            .execute(&pool)
            .await
            .expect("complete");
            for offset in [2, 1] {
                sqlx::query(
                    "INSERT INTO external_service_sync_jobs(external_service_id, state, finished_at) \
                     VALUES ($1, 'completed', clock_timestamp() - ($2 || ' hour')::interval)",
                )
                .bind(service)
                .bind(offset)
                .execute(&pool)
                .await
                .expect("insert finished job");
            }

            let deleted = store
                .cleanup_sync_jobs(std::time::Duration::from_secs(100 * 365 * 24 * 3600), 2)
                .await
                .expect("cleanup");
            assert_eq!(deleted, 1, "only the oldest job is beyond max_per_service");
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM external_service_sync_jobs WHERE external_service_id = $1")
                    .bind(service)
                    .fetch_one(&pool)
                    .await
                    .expect("count");
            assert_eq!(remaining, 2);

            let deleted = store
                .cleanup_sync_jobs(std::time::Duration::from_millis(1), 100)
                .await
                .expect("cleanup");
            assert_eq!(deleted, 2, "everything older than the ttl is removed");
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service)
            .execute(&pool)
            .await
            .expect("cleanup");
        result
    }
}
