#![forbid(unsafe_code)]

//! Port of `internal/repos/sync_worker.go` plus the parts of
//! `internal/workerutil` the repo-sync worker uses.
//!
//! Five background loops, matching `Syncer.Routines`:
//!
//! - an enqueue loop (`repo-updater.repo-sync-scheduler`) that inserts a job
//!   for every due external service every `repoListUpdateInterval`;
//! - `NumHandlers` dequeue loops (`repo_sync_worker`) that claim jobs with
//!   `FOR UPDATE SKIP LOCKED` and run the syncer;
//! - a heartbeat loop that refreshes `last_heartbeat_at` every 15s and
//!   cancels jobs whose `cancel` flag is set;
//! - a resetter that requeues stalled jobs every 5 minutes;
//! - a janitor that deletes finished jobs older than 30 days, keeping the
//!   newest 100 per external service, every hour.
//!
//! All tasks are spawned on the service's [`TaskTracker`] (structured
//! concurrency); the per-job cancellation tokens are the equivalent of
//! workerutil's `runningIDSet`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_db::external_service_sync_jobs::{SyncJob, SyncJobCounters, SyncJobsStore};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::syncer::{ProgressRecorder, SyncProgress, Syncer};

/// `SyncWorkerOptions.NumHandlers` default.
pub const DEFAULT_NUM_HANDLERS: usize = 3;
/// `SyncWorkerOptions.WorkerInterval` default.
pub const DEFAULT_WORKER_INTERVAL: Duration = Duration::from_secs(10);
/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `dbworker.ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// `CleanupOldJobsInterval` default.
pub const CLEANUP_OLD_JOBS_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// `newJobCleanerRoutine`'s `OlderThan`.
pub const CLEANUP_OLDER_THAN: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// `newJobCleanerRoutine`'s `MaxPerExternalService`.
pub const CLEANUP_MAX_PER_EXTERNAL_SERVICE: i32 = 100;

/// `rate.NewLimiter(1.0, 1)` used by `syncHandler.Handle`: intermediate
/// progress updates are written at most once per second, the final one always.
struct ProgressLimiter {
    interval: Duration,
    last: Mutex<Option<Instant>>,
}

impl ProgressLimiter {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: Mutex::new(None),
        }
    }

    fn allow(&self) -> bool {
        let now = Instant::now();
        let mut last = self.last.lock().expect("progress limiter poisoned");
        match *last {
            Some(previous) if now.duration_since(previous) < self.interval => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

/// `syncHandler`'s progress recorder: writes the job's counters.
pub struct JobProgressRecorder {
    jobs: SyncJobsStore,
    job_id: i32,
    limiter: ProgressLimiter,
}

impl JobProgressRecorder {
    pub fn new(jobs: SyncJobsStore, job_id: i32) -> Self {
        Self {
            jobs,
            job_id,
            limiter: ProgressLimiter::new(Duration::from_secs(1)),
        }
    }
}

#[async_trait]
impl ProgressRecorder for JobProgressRecorder {
    async fn record(&self, progress: SyncProgress, final_call: bool) -> Result<(), String> {
        if !final_call && !self.limiter.allow() {
            return Ok(());
        }
        self.jobs
            .update_counters(
                self.job_id,
                SyncJobCounters {
                    repos_synced: progress.synced,
                    repo_sync_errors: progress.errors,
                    repos_added: progress.added,
                    repos_modified: progress.modified,
                    repos_unmodified: progress.unmodified,
                    repos_deleted: progress.deleted,
                },
            )
            .await
            .map_err(|err| err.to_string())
    }
}

/// `repos.NewSyncWorker`'s worker + resetter + janitor, and the enqueue loop
/// from `Syncer.Routines`.
pub struct SyncWorker {
    jobs: SyncJobsStore,
    syncer: Arc<Syncer>,
    live: Arc<LiveConfig>,
    hostname: String,
    num_handlers: usize,
    worker_interval: Duration,
    cleanup_old_jobs: bool,
    /// workerutil's `runningIDSet`: job id → the per-job cancellation token.
    running: Arc<Mutex<HashMap<i32, CancellationToken>>>,
}

impl SyncWorker {
    pub fn new(
        jobs: SyncJobsStore,
        syncer: Arc<Syncer>,
        live: Arc<LiveConfig>,
        hostname: impl Into<String>,
    ) -> Self {
        Self {
            jobs,
            syncer,
            live,
            hostname: hostname.into(),
            num_handlers: DEFAULT_NUM_HANDLERS,
            worker_interval: DEFAULT_WORKER_INTERVAL,
            cleanup_old_jobs: true,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_num_handlers(mut self, num_handlers: usize) -> Self {
        self.num_handlers = num_handlers.max(1);
        self
    }

    pub fn with_worker_interval(mut self, interval: Duration) -> Self {
        self.worker_interval = interval;
        self
    }

    pub fn with_cleanup_old_jobs(mut self, cleanup: bool) -> Self {
        self.cleanup_old_jobs = cleanup;
        self
    }

    /// Spawn every loop on `tasks`.
    pub fn spawn(self: Arc<Self>, shutdown: &CancellationToken, tasks: &TaskTracker) {
        let worker = Arc::clone(&self);
        let token = shutdown.clone();
        tasks.spawn(async move { worker.enqueue_loop(token).await });

        let worker = Arc::clone(&self);
        let token = shutdown.clone();
        let tracker = tasks.clone();
        tasks.spawn(async move { worker.dequeue_loop(token, tracker).await });

        let worker = Arc::clone(&self);
        let token = shutdown.clone();
        tasks.spawn(async move { worker.heartbeat_loop(token).await });

        let worker = Arc::clone(&self);
        let token = shutdown.clone();
        tasks.spawn(async move { worker.resetter_loop(token).await });

        if self.cleanup_old_jobs {
            let worker = Arc::clone(&self);
            let token = shutdown.clone();
            tasks.spawn(async move { worker.janitor_loop(token).await });
        }
    }

    /// `Syncer.Routines`' enqueue goroutine: immediately, then every
    /// `repoListUpdateInterval`, enqueue due services unless
    /// `disableAutoCodeHostSyncs`.
    async fn enqueue_loop(&self, shutdown: CancellationToken) {
        loop {
            // The baseline checks this inside the handler; skipping the
            // enqueue is the whole handler.
            if !self.live.current().site.disable_auto_code_host_syncs() {
                if let Err(err) = self.jobs.enqueue_sync_jobs().await {
                    tracing::error!(error = %err, "enqueueing sync jobs");
                }
            }

            let interval = self.live.current().site.repo_list_update_interval();
            if !sleep_or_shutdown(interval, &shutdown).await {
                return;
            }
        }
    }

    /// `workerutil.Worker.Start`: poll for jobs, keeping at most
    /// `num_handlers` in flight. The poll interval only applies when the
    /// dequeue came back empty.
    async fn dequeue_loop(self: Arc<Self>, shutdown: CancellationToken, tasks: TaskTracker) {
        let semaphore = Arc::new(Semaphore::new(self.num_handlers));
        loop {
            let permit = tokio::select! {
                _ = shutdown.cancelled() => return,
                permit = Arc::clone(&semaphore).acquire_owned() => {
                    permit.expect("semaphore is never closed")
                }
            };

            match self.jobs.dequeue(&self.hostname).await {
                Ok(Some(job)) => {
                    let worker = Arc::clone(&self);
                    let token = shutdown.clone();
                    tasks.spawn(async move {
                        worker.handle(job, &token).await;
                        drop(permit);
                    });
                    // A successful dequeue does not wait for the poll
                    // interval.
                }
                Ok(None) => {
                    drop(permit);
                    if !sleep_or_shutdown(self.worker_interval, &shutdown).await {
                        return;
                    }
                }
                Err(err) => {
                    drop(permit);
                    tracing::error!(error = %err, "Failed to dequeue and handle record");
                    if !sleep_or_shutdown(self.worker_interval, &shutdown).await {
                        return;
                    }
                }
            }
        }
    }

    /// `syncHandler.Handle` + `workerutil.Worker.handle`'s terminal-state
    /// update.
    async fn handle(&self, job: SyncJob, shutdown: &CancellationToken) {
        let job_token = CancellationToken::new();
        self.running
            .lock()
            .expect("running set poisoned")
            .insert(job.id, job_token.clone());

        let min_sync_interval = self.live.current().site.repo_list_update_interval();
        let recorder = JobProgressRecorder::new(self.jobs.clone(), job.id);

        tracing::debug!(
            job_id = job.id,
            external_service_id = job.external_service_id,
            "Dequeued record for processing"
        );

        let outcome = tokio::select! {
            _ = shutdown.cancelled() => {
                job_token.cancel();
                Err("job canceled".to_owned())
            }
            _ = job_token.cancelled() => Err("job canceled".to_owned()),
            result = self.syncer.sync_external_service(
                job.external_service_id,
                min_sync_interval,
                &recorder,
                &job_token,
            ) => result,
        };

        self.running
            .lock()
            .expect("running set poisoned")
            .remove(&job.id);

        match outcome {
            Ok(()) => match self.jobs.mark_complete(job.id).await {
                Ok(true) => tracing::debug!(job_id = job.id, "Marked record as complete"),
                Ok(false) => tracing::warn!(job_id = job.id, "record was not processing"),
                Err(err) => tracing::error!(job_id = job.id, error = %err, "store.MarkComplete"),
            },
            Err(err) => {
                // A canceled job takes `MarkFailed` in the baseline
                // (`isJobCanceled`); with `MaxNumRetries = 0` both paths end
                // in `failed`, or `canceled` when the cancel flag is set.
                let result = if job_token.is_cancelled() {
                    self.jobs.mark_failed(job.id, &err).await
                } else {
                    self.jobs.mark_errored(job.id, &err).await
                };
                match result {
                    Ok(true) => {
                        tracing::warn!(job_id = job.id, error = %err, "Marked record as failed")
                    }
                    Ok(false) => tracing::warn!(job_id = job.id, "record was not processing"),
                    Err(err) => {
                        tracing::error!(job_id = job.id, error = %err, "marking record failed")
                    }
                }
            }
        }
    }

    /// `workerutil.Worker.Start`'s heartbeat goroutine.
    async fn heartbeat_loop(&self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
            HEARTBEAT_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            let ids: Vec<i32> = self
                .running
                .lock()
                .expect("running set poisoned")
                .keys()
                .copied()
                .collect();
            let (known, canceled) = match self.jobs.heartbeat(&ids).await {
                Ok(result) => result,
                Err(err) => {
                    tracing::error!(ids = ?ids, error = %err, "Failed to refresh heartbeats");
                    continue;
                }
            };
            let known: HashSet<i32> = known.into_iter().collect();
            let mut running = self.running.lock().expect("running set poisoned");
            for id in &ids {
                if !known.contains(id) && running.remove(id).is_some() {
                    tracing::error!(job_id = id, "Removed unknown job from running set");
                }
            }
            if !canceled.is_empty() {
                tracing::info!(ids = ?canceled, "Found jobs to cancel");
            }
            for id in canceled {
                if let Some(token) = running.get(&id) {
                    token.cancel();
                }
            }
        }
    }

    /// `dbworker.Resetter`: requeue stalled jobs every 5 minutes.
    async fn resetter_loop(&self, shutdown: CancellationToken) {
        loop {
            match self.jobs.reset_stalled().await {
                Ok(result) if !result.reset.is_empty() || !result.failed.is_empty() => {
                    tracing::info!(
                        reset = ?result.reset,
                        failed = ?result.failed,
                        "Requeued stalled sync jobs"
                    );
                }
                Ok(_) => {}
                Err(err) => tracing::error!(error = %err, "resetting stalled sync jobs"),
            }
            if !sleep_or_shutdown(RESETTER_INTERVAL, &shutdown).await {
                return;
            }
        }
    }

    /// `newJobCleanerRoutine`.
    async fn janitor_loop(&self, shutdown: CancellationToken) {
        loop {
            match self
                .jobs
                .cleanup_sync_jobs(CLEANUP_OLDER_THAN, CLEANUP_MAX_PER_EXTERNAL_SERVICE)
                .await
            {
                Ok(deleted) if deleted > 0 => {
                    tracing::debug!(deleted, "cleaned old sync jobs");
                }
                Ok(_) => {}
                Err(err) => tracing::error!(error = %err, "error while running job cleaner"),
            }
            if !sleep_or_shutdown(CLEANUP_OLD_JOBS_INTERVAL, &shutdown).await {
                return;
            }
        }
    }
}

/// Sleep for `duration`, returning `false` when the service is shutting down.
async fn sleep_or_shutdown(duration: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

/// `hostname.Get`: `$HOSTNAME`, else the kernel host name.
pub fn hostname() -> String {
    if let Ok(value) = std::env::var("HOSTNAME") {
        if !value.is_empty() {
            return value;
        }
    }
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|name| name.trim().to_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_limiter_allows_the_first_update_then_rate_limits() {
        let limiter = ProgressLimiter::new(Duration::from_millis(50));
        assert!(limiter.allow());
        assert!(!limiter.allow());
        std::thread::sleep(Duration::from_millis(60));
        assert!(limiter.allow());
    }

    #[tokio::test]
    async fn sleep_or_shutdown_stops_on_cancellation() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(!sleep_or_shutdown(Duration::from_secs(3600), &token).await);
    }
}
