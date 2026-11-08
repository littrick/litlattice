//! `workerutil.Worker` loop for the upload queue.
//!
//! Mirrors `internal/workerutil/worker.go`:
//!
//! - a dequeue loop bounded by `NumHandlers` (semaphore), polling every
//!   `Interval` only when the previous dequeue came back empty;
//! - a heartbeat loop that refreshes `last_heartbeat_at` every
//!   `HeartbeatInterval`, drops ids the database no longer knows and signals
//!   cancellation for ids flagged `cancel = true`;
//! - a handler invocation with an optional `MaximumRuntimePerJob` deadline;
//! - terminal state selection: `MarkComplete` on success, `MarkErrored` on a
//!   retryable error, `MarkFailed` on a non-retryable error or cancellation.
//!
//! The upload handler itself (object storage + SCIP parsing + code-graph
//! writes) lands in 9.7d; this loop is exercised by the Go/Rust differential
//! harness in `tests/upload_worker.rs`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::queue::{Upload, UploadQueueStore};

/// `Handler.PreDequeue` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreDequeue {
    /// Do not dequeue a record this iteration (the handler is over budget).
    Decline,
    /// Dequeue a record, optionally restricted to `upload_size <= max`.
    Dequeue { max_upload_size: Option<i64> },
}

/// Handler error with the baseline's retry classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    pub message: String,
    /// `errcode.IsNonRetryable`: goes straight to `MarkFailed`.
    pub non_retryable: bool,
    /// The handler observed the job's cancellation token (`ctx.Err()`).
    pub canceled: bool,
}

impl HandlerError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
            canceled: false,
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
            canceled: false,
        }
    }

    pub fn canceled() -> Self {
        Self {
            message: "context canceled".to_owned(),
            non_retryable: false,
            canceled: true,
        }
    }
}

/// The job-processing side of the worker.
#[async_trait]
pub trait UploadHandler: Send + Sync {
    /// `Handler.Handle`. `cancel` is cancelled when the database flags the
    /// record for cancellation or when the worker shuts the job down.
    async fn handle(&self, upload: Upload, cancel: CancellationToken) -> Result<(), HandlerError>;

    /// Optional `WithPreDequeue` hook.
    fn pre_dequeue(&self) -> PreDequeue {
        PreDequeue::Dequeue {
            max_upload_size: None,
        }
    }

    /// Optional `WithHooks.PreHandle`.
    fn pre_handle(&self, _upload: &Upload) {}

    /// Optional `WithHooks.PostHandle`.
    fn post_handle(&self, _upload: &Upload) {}
}

/// `workerutil.WorkerOptions` as configured by the upload processor.
#[derive(Debug, Clone)]
pub struct UploadWorkerOptions {
    pub name: String,
    pub worker_hostname: String,
    pub num_handlers: usize,
    pub interval: Duration,
    pub heartbeat_interval: Duration,
    /// `MaximumRuntimePerJob`; zero disables the deadline.
    pub maximum_runtime_per_job: Duration,
    /// `NumTotalJobs`; `None` means unlimited.
    pub num_total_jobs: Option<usize>,
    /// `MaxActiveTime`; `None` means unlimited.
    pub max_active_time: Option<Duration>,
}

impl UploadWorkerOptions {
    /// The baseline's `NewUploadProcessorWorker` configuration.
    pub fn for_service(
        name: impl Into<String>,
        worker_hostname: impl Into<String>,
        num_handlers: usize,
        interval: Duration,
        maximum_runtime_per_job: Duration,
    ) -> Self {
        Self {
            name: name.into(),
            worker_hostname: worker_hostname.into(),
            num_handlers: num_handlers.max(1),
            interval,
            heartbeat_interval: Duration::from_secs(1),
            maximum_runtime_per_job,
            num_total_jobs: None,
            max_active_time: None,
        }
    }
}

#[derive(Debug)]
struct RunningJob {
    cancel: CancellationToken,
}

type RunningSet = Arc<Mutex<HashMap<i32, RunningJob>>>;

pub struct UploadWorker {
    store: Arc<UploadQueueStore>,
    handler: Arc<dyn UploadHandler>,
    options: UploadWorkerOptions,
    running: RunningSet,
}

impl UploadWorker {
    pub fn new(
        store: Arc<UploadQueueStore>,
        handler: Arc<dyn UploadHandler>,
        options: UploadWorkerOptions,
    ) -> Self {
        Self {
            store,
            handler,
            options,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Spawn the worker on the service's task tracker.
    pub fn spawn(self: Arc<Self>, shutdown: CancellationToken, tasks: &TaskTracker) {
        tasks.spawn(async move { self.run(shutdown).await });
    }

    /// `Worker.Start`: run the dequeue loop, then wait for in-flight handlers.
    /// The heartbeat loop outlives the dequeue loop — the baseline's heartbeat
    /// goroutine only stops when `Start` returns, i.e. after `wg.Wait()`, so
    /// cancellation of a handler that is still running still happens.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let finished = CancellationToken::new();
        let heartbeat_tasks = TaskTracker::new();
        let jobs = TaskTracker::new();
        {
            let worker = Arc::clone(&self);
            let finished = finished.clone();
            heartbeat_tasks.spawn(async move { worker.heartbeat_loop(finished).await });
        }

        self.dequeue_loop(shutdown, &jobs).await;

        jobs.close();
        jobs.wait().await;

        finished.cancel();
        heartbeat_tasks.close();
        heartbeat_tasks.wait().await;
    }

    /// `Worker.Start`'s main loop.
    async fn dequeue_loop(self: &Arc<Self>, shutdown: CancellationToken, jobs: &TaskTracker) {
        let semaphore = Arc::new(Semaphore::new(self.options.num_handlers));
        let deadline = self
            .options
            .max_active_time
            .map(|duration| Instant::now() + duration);
        let mut num_dequeues = 0usize;

        loop {
            if let Some(total) = self.options.num_total_jobs {
                if num_dequeues >= total {
                    tracing::info!(name = self.options.name, "Shutting down dequeue loop");
                    break;
                }
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                tracing::info!(
                    name = self.options.name,
                    "Shutting down dequeue loop (MaxActiveTime elapsed)"
                );
                break;
            }

            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                permit = Arc::clone(&semaphore).acquire_owned() => {
                    permit.expect("the worker semaphore is never closed")
                }
            };

            let max_upload_size = match self.handler.pre_dequeue() {
                PreDequeue::Decline => {
                    drop(permit);
                    if !sleep_or_shutdown(self.options.interval, &shutdown).await {
                        break;
                    }
                    continue;
                }
                PreDequeue::Dequeue { max_upload_size } => max_upload_size,
            };

            match self
                .store
                .dequeue_with_budget(&self.options.worker_hostname, max_upload_size)
                .await
            {
                Ok(Some(upload)) => {
                    num_dequeues += 1;
                    let cancel = CancellationToken::new();
                    let duplicate = {
                        let mut running = self.running.lock().expect("running set");
                        match running.entry(upload.id) {
                            std::collections::hash_map::Entry::Occupied(_) => true,
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(RunningJob {
                                    cancel: cancel.clone(),
                                });
                                false
                            }
                        }
                    };
                    if duplicate {
                        tracing::error!(id = upload.id, "job already exists");
                        drop(permit);
                        continue;
                    }

                    self.handler.pre_handle(&upload);
                    let worker = Arc::clone(self);
                    jobs.spawn(async move {
                        worker.handle(upload, cancel, permit).await;
                    });
                    // A successful dequeue does not wait the poll interval.
                }
                Ok(None) => {
                    drop(permit);
                    if !sleep_or_shutdown(self.options.interval, &shutdown).await {
                        break;
                    }
                }
                Err(err) => {
                    drop(permit);
                    tracing::error!(
                        name = self.options.name,
                        error = %err,
                        "Failed to dequeue and handle record"
                    );
                    if !sleep_or_shutdown(self.options.interval, &shutdown).await {
                        break;
                    }
                }
            }
        }
    }

    /// `Worker.handle`: run the handler with the per-job deadline and move the
    /// record into its terminal state.
    async fn handle(
        self: Arc<Self>,
        upload: Upload,
        cancel: CancellationToken,
        permit: OwnedSemaphorePermit,
    ) {
        let result = if self.options.maximum_runtime_per_job.is_zero() {
            self.handler.handle(upload.clone(), cancel.clone()).await
        } else {
            match timeout(
                self.options.maximum_runtime_per_job,
                self.handler.handle(upload.clone(), cancel.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(HandlerError::retryable(format!(
                    // `errors.Wrap(err, msg)` formats as "msg: cause".
                    "job exceeded maximum execution time of {}: context deadline exceeded",
                    format_go_duration(self.options.maximum_runtime_per_job)
                ))),
            }
        };

        match result {
            Ok(()) => match self.store.mark_complete(upload.id, None).await {
                Ok(true) => tracing::debug!(id = upload.id, "Marked record as complete"),
                Ok(false) => {}
                Err(err) => tracing::error!(id = upload.id, error = %err, "store.MarkComplete"),
            },
            Err(err) if err.non_retryable || (err.canceled && cancel.is_cancelled()) => {
                match self.store.mark_failed(upload.id, &err.message, None).await {
                    Ok(true) => {
                        tracing::warn!(id = upload.id, error = %err.message, "Marked record as failed")
                    }
                    Ok(false) => {}
                    Err(mark_err) => {
                        tracing::error!(id = upload.id, error = %mark_err, "store.MarkFailed")
                    }
                }
            }
            Err(err) => match self.store.mark_errored(upload.id, &err.message, None).await {
                Ok(true) => {
                    tracing::warn!(id = upload.id, error = %err.message, "Marked record as errored")
                }
                Ok(false) => {}
                Err(mark_err) => {
                    tracing::error!(id = upload.id, error = %mark_err, "store.MarkErrored")
                }
            },
        }

        self.handler.post_handle(&upload);
        self.running.lock().expect("running set").remove(&upload.id);
        drop(permit);
    }

    /// `Worker.Start`'s heartbeat goroutine.
    async fn heartbeat_loop(self: Arc<Self>, finished: CancellationToken) {
        loop {
            tokio::select! {
                _ = finished.cancelled() => return,
                _ = sleep(self.options.heartbeat_interval) => {}
            }

            let ids: Vec<i32> = {
                let running = self.running.lock().expect("running set");
                running.keys().copied().collect()
            };
            if ids.is_empty() {
                continue;
            }

            match self.store.heartbeat(&ids, None).await {
                Ok(result) => {
                    let known: HashSet<i32> = result.known_ids.into_iter().collect();
                    let mut running = self.running.lock().expect("running set");
                    for id in &ids {
                        if !known.contains(id) && running.remove(id).is_some() {
                            tracing::error!(id, "Removed unknown job from running set");
                        }
                    }
                    if !result.cancel_ids.is_empty() {
                        tracing::info!(ids = ?result.cancel_ids, "Found jobs to cancel");
                    }
                    for id in result.cancel_ids {
                        if let Some(job) = running.get(&id) {
                            job.cancel.cancel();
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(
                        ids = ?ids,
                        error = %err,
                        "Failed to refresh heartbeats"
                    );
                }
            }
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &CancellationToken) -> bool {
    if duration.is_zero() {
        return !shutdown.is_cancelled();
    }
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = sleep(duration) => true,
    }
}

/// Go `time.Duration.String()` for non-negative durations (the deadline error
/// message uses `fmt.Sprintf("%s", MaximumRuntimePerJob)`).
fn format_go_duration(duration: Duration) -> String {
    let mut u = duration.as_nanos();
    if u == 0 {
        return "0s".to_owned();
    }
    if u < 1_000_000_000 {
        let (unit, prec) = if u < 1_000 {
            ("ns", 0)
        } else if u < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let frac = fmt_frac(&mut u, prec);
        return format!("{}{}{}", u, frac, unit);
    }
    let frac = fmt_frac(&mut u, 9);
    let mut out = format!("{}{}s", u % 60, frac);
    u /= 60;
    if u > 0 {
        out = format!("{}m{}", u % 60, out);
        u /= 60;
        if u > 0 {
            out = format!("{u}h{out}");
        }
    }
    out
}

fn fmt_frac(v: &mut u128, prec: u32) -> String {
    let mut digits = String::new();
    let mut print = false;
    for _ in 0..prec {
        let digit = *v % 10;
        print = print || digit != 0;
        if print {
            digits.insert(0, char::from(b'0' + digit as u8));
        }
        *v /= 10;
    }
    if print {
        format!(".{digits}")
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_go_durations() {
        assert_eq!(format_go_duration(Duration::ZERO), "0s");
        assert_eq!(format_go_duration(Duration::from_millis(100)), "100ms");
        assert_eq!(format_go_duration(Duration::from_secs(1)), "1s");
        assert_eq!(format_go_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_go_duration(Duration::from_secs(25 * 60)), "25m0s");
        assert_eq!(format_go_duration(Duration::from_millis(1500)), "1.5s");
    }

    #[test]
    fn handler_error_classification() {
        assert!(!HandlerError::retryable("x").non_retryable);
        assert!(HandlerError::non_retryable("x").non_retryable);
        assert!(HandlerError::canceled().canceled);
    }
}
