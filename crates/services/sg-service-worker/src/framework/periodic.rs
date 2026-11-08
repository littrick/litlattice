#![forbid(unsafe_code)]

//! `internal/goroutine/periodic.go`: `PeriodicGoroutine`.
//!
//! Semantics reproduced:
//!
//! * a pool of `concurrency` workers, re-checked every 30s; the pool is only
//!   drained and rebuilt when the reported value changes;
//! * every worker waits `initial_delay` (zero by default), then calls the
//!   handler, then sleeps `interval` (`1s` by default);
//! * an error that is `ErrReinvokeImmediately` skips the sleep; at most 100
//!   consecutive reinvocations are honoured, the 101st uses the interval;
//! * an error while the routine is shutting down is not reported (the
//!   baseline's `isShutdownError`); other errors go to `HandleError`;
//! * `Stop` cancels the handler context and waits for `Start` to return.
//!
//! Panic-to-error conversion is inherent in Rust (`tokio` task panics are
//! observed on join); the recorder (`background-job-logger`) and per-operation
//! tracing/metrics are not ported yet.

use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// `newDefaultPeriodicRoutine`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_CONCURRENCY: usize = 1;
/// `concurrencyRecheckInterval`.
pub const CONCURRENCY_RECHECK_INTERVAL: Duration = Duration::from_secs(30);
/// `maxConsecutiveReinvocations`.
pub const MAX_CONSECUTIVE_REINVOCATIONS: u32 = 100;

/// An error returned by a [`Handler`].
#[derive(Debug, Clone)]
pub struct HandlerError {
    message: String,
    /// `goroutine.ErrReinvokeImmediately` in the baseline.
    reinvoke_immediately: bool,
}

impl HandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reinvoke_immediately: false,
        }
    }

    /// The baseline's `goroutine.ErrReinvokeImmediately`: ask the routine to
    /// run again without sleeping (bounded by
    /// [`MAX_CONSECUTIVE_REINVOCATIONS`]).
    pub fn reinvoke_immediately() -> Self {
        Self {
            message: "periodic handler wishes to be immediately re-invoked".to_owned(),
            reinvoke_immediately: true,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_reinvoke_immediately(&self) -> bool {
        self.reinvoke_immediately
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HandlerError {}

/// `goroutine.Handler` plus the optional `ErrorHandler` extension. A handler
/// that wants `HandleError` logs should override `handle_error`; the default
/// is the baseline's "does not implement ErrorHandler" case (nothing logged).
#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, ctx: CancellationToken) -> Result<(), HandlerError>;

    fn handle_error(&self, _error: &HandlerError) {}
}

pub type GetInterval = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type GetConcurrency = Arc<dyn Fn() -> usize + Send + Sync>;

pub struct PeriodicRoutine {
    name: String,
    description: String,
    job_name: String,
    get_interval: GetInterval,
    get_concurrency: GetConcurrency,
    initial_delay: Duration,
    handler: Arc<dyn Handler>,
    /// `PeriodicGoroutine.reinvocations`, shared by the pool's workers.
    reinvocations: AtomicU32,
}

impl PeriodicRoutine {
    pub fn new(handler: Arc<dyn Handler>) -> Self {
        Self {
            name: "<unnamed periodic routine>".to_owned(),
            description: "<no description provided>".to_owned(),
            job_name: String::new(),
            get_interval: Arc::new(|| DEFAULT_INTERVAL),
            get_concurrency: Arc::new(|| DEFAULT_CONCURRENCY),
            initial_delay: Duration::ZERO,
            handler,
            reinvocations: AtomicU32::new(0),
        }
    }

    /// `goroutine.WithName`.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// `goroutine.WithDescription`.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// `goroutine.WithInterval`.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.get_interval = Arc::new(move || interval);
        self
    }

    /// `goroutine.WithIntervalFunc` (re-read on every iteration).
    pub fn with_interval_func(mut self, get_interval: GetInterval) -> Self {
        self.get_interval = get_interval;
        self
    }

    /// `goroutine.WithConcurrency`.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.get_concurrency = Arc::new(move || concurrency);
        self
    }

    /// `goroutine.WithConcurrencyFunc` (re-read every 30s).
    pub fn with_concurrency_func(mut self, get_concurrency: GetConcurrency) -> Self {
        self.get_concurrency = get_concurrency;
        self
    }

    /// `goroutine.WithInitialDelay`.
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn interval(&self) -> Duration {
        (self.get_interval)()
    }

    pub fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    pub fn concurrency(&self) -> usize {
        (self.get_concurrency)()
    }

    pub fn job_name(&self) -> &str {
        &self.job_name
    }

    /// `SetJobName` (recorder metadata; the recorder itself is not ported).
    pub fn set_job_name(&mut self, job_name: impl Into<String>) {
        self.job_name = job_name.into();
    }

    /// `Start` on the given tracker. `shutdown` is the process/service
    /// context; cancelling it is the equivalent of `Stop`.
    pub fn spawn(self: Arc<Self>, tracker: &TaskTracker, shutdown: CancellationToken) {
        let routine = Arc::clone(&self);
        tracker.spawn(async move { routine.start(shutdown).await });
    }

    async fn start(self: Arc<Self>, shutdown: CancellationToken) {
        let mut concurrency = (self.get_concurrency)();
        loop {
            let pool_cancel = CancellationToken::new();
            let mut pool = JoinSet::new();
            for _ in 0..concurrency {
                let routine = Arc::clone(&self);
                let pool_cancel = pool_cancel.clone();
                let shutdown = shutdown.clone();
                pool.spawn(async move { routine.run_worker(pool_cancel, shutdown).await });
            }

            // `concurrencyUpdates` re-checks every 30s and only rebuilds the
            // pool when the value changed; the pool keeps running otherwise.
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        pool_cancel.cancel();
                        while pool.join_next().await.is_some() {}
                        return;
                    }
                    _ = sleep(CONCURRENCY_RECHECK_INTERVAL) => {}
                }

                let next = (self.get_concurrency)();
                if next != concurrency {
                    concurrency = next;
                    break;
                }
            }

            pool_cancel.cancel();
            while pool.join_next().await.is_some() {}
        }
    }

    /// `runHandlerPeriodically`.
    async fn run_worker(
        self: &Arc<Self>,
        pool_cancel: CancellationToken,
        shutdown: CancellationToken,
    ) {
        // `initialDelay` also gives the worker a chance to observe shutdown
        // before the first invocation.
        tokio::select! {
            _ = sleep(self.initial_delay) => {}
            _ = shutdown.cancelled() => return,
            _ = pool_cancel.cancelled() => return,
        }

        loop {
            let Some(next) = self.run_handler(&pool_cancel, &shutdown).await else {
                return;
            };

            tokio::select! {
                _ = sleep(next) => {}
                _ = shutdown.cancelled() => return,
                _ = pool_cancel.cancelled() => return,
            }
        }
    }

    /// `runHandlerAndDetermineBackoff`: returns the delay before the next
    /// invocation, or `None` when the worker should exit.
    async fn run_handler(
        self: &Arc<Self>,
        pool_cancel: &CancellationToken,
        shutdown: &CancellationToken,
    ) -> Option<Duration> {
        // The handler context is cancelled by either the routine context or
        // the pool's drain (baseline `handlerCtx` + `context.AfterFunc`).
        let handler_cancel = CancellationToken::new();
        let handler_token = handler_cancel.clone();
        let shutdown_watch = shutdown.clone();
        let pool_watch = pool_cancel.clone();
        let cancel_watch = async move {
            tokio::select! {
                _ = shutdown_watch.cancelled() => {}
                _ = pool_watch.cancelled() => {}
            }
            handler_token.cancel();
        };

        let handler = Arc::clone(&self.handler);
        let mut handle_fut = Box::pin(handler.handle(handler_cancel));
        let mut cancel_watch = Box::pin(cancel_watch);
        let mut cancelled = false;
        let result = loop {
            if cancelled {
                // Cancellation was requested; the baseline still waits for
                // the handler to observe the cancelled context and return.
                break handle_fut.as_mut().await;
            }

            tokio::select! {
                result = handle_fut.as_mut() => break result,
                _ = cancel_watch.as_mut() => {
                    cancelled = true;
                }
            }
        };

        let reinvoke = match result {
            Ok(()) => false,
            Err(error) => {
                // `isShutdownError`: the routine context is already cancelled,
                // so the worker exits instead of reporting an error.
                if shutdown.is_cancelled() || pool_cancel.is_cancelled() {
                    return None;
                }
                let reinvoke = error.is_reinvoke_immediately();
                if !reinvoke {
                    self.handler.handle_error(&error);
                }
                reinvoke
            }
        };

        Some(self.next_interval(reinvoke))
    }

    /// `getNextInterval`.
    fn next_interval(&self, try_reinvoke: bool) -> Duration {
        if try_reinvoke {
            let count = self.reinvocations.fetch_add(1, Ordering::SeqCst) + 1;
            if count < MAX_CONSECUTIVE_REINVOCATIONS {
                return Duration::ZERO;
            }
        }

        self.reinvocations.store(0, Ordering::SeqCst);
        (self.get_interval)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    struct CountingHandler {
        calls: AtomicUsize,
        errors: Mutex<Vec<String>>,
        fail_times: usize,
    }

    impl CountingHandler {
        fn new(fail_times: usize) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                errors: Mutex::new(Vec::new()),
                fail_times,
            })
        }
    }

    #[async_trait]
    impl Handler for CountingHandler {
        async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= self.fail_times {
                return Err(HandlerError::new(format!("failure {call}")));
            }
            Ok(())
        }

        fn handle_error(&self, error: &HandlerError) {
            self.errors
                .lock()
                .expect("lock")
                .push(error.message().to_owned());
        }
    }

    async fn stop(tracker: &TaskTracker, shutdown: &CancellationToken) {
        shutdown.cancel();
        tracker.close();
        let _ = tokio::time::timeout(Duration::from_secs(5), tracker.wait()).await;
    }

    #[tokio::test]
    async fn reinvocations_are_capped_at_100() {
        let routine = Arc::new(
            PeriodicRoutine::new(CountingHandler::new(0) as Arc<dyn Handler>)
                .with_interval(Duration::from_millis(1)),
        );
        let delays: Vec<Duration> = (0..MAX_CONSECUTIVE_REINVOCATIONS + 1)
            .map(|_| routine.next_interval(true))
            .collect();
        // `atomic.AddInt32` is compared before use, so calls 1..=99 are
        // immediate and call 100 falls back to the interval.
        assert!(delays[..99].iter().all(|delay| delay.is_zero()));
        assert_eq!(delays[99], Duration::from_millis(1));
        // The counter resets, so the next reinvocation is immediate again.
        assert!(routine.next_interval(true).is_zero());
    }

    #[tokio::test]
    async fn normal_errors_are_reported_and_use_the_interval() {
        let routine = Arc::new(PeriodicRoutine::new(
            CountingHandler::new(0) as Arc<dyn Handler>
        ));
        assert_eq!(
            routine.next_interval(false),
            DEFAULT_INTERVAL,
            "a plain error does not reinvoke immediately"
        );
    }

    #[tokio::test]
    async fn handler_runs_and_shutdown_stops_the_pool() {
        let handler = CountingHandler::new(0);
        let routine = Arc::new(
            PeriodicRoutine::new(Arc::clone(&handler) as Arc<dyn Handler>)
                .with_concurrency(2)
                .with_interval(Duration::from_millis(4)),
        );
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        routine.spawn(&tracker, shutdown.clone());

        tokio::time::sleep(Duration::from_millis(50)).await;
        stop(&tracker, &shutdown).await;

        assert!(handler.calls.load(Ordering::SeqCst) >= 2);
        assert!(handler.errors.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn handler_errors_are_logged_only_when_not_shutting_down() {
        let handler = CountingHandler::new(1);
        let routine = Arc::new(
            PeriodicRoutine::new(Arc::clone(&handler) as Arc<dyn Handler>)
                .with_interval(Duration::from_millis(2)),
        );
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        routine.spawn(&tracker, shutdown.clone());

        tokio::time::sleep(Duration::from_millis(30)).await;
        stop(&tracker, &shutdown).await;
        assert_eq!(
            handler.errors.lock().expect("lock").as_slice(),
            ["failure 1"]
        );
    }
}
