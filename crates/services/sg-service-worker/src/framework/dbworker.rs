#![forbid(unsafe_code)]

//! `internal/workerutil` + `internal/workerutil/dbworker`: the DB-backed
//! worker loop and the stalled-record resetter, generalised over a store and a
//! handler.
//!
//! Semantics reproduced from the baseline for the knobs the worker jobs use:
//!
//! * the dequeue loop only polls while a handler slot is free (`NumHandlers`);
//!   a successful dequeue is handled immediately, an empty queue waits
//!   `Interval`;
//! * every dequeued record is registered in a running set; a heartbeat task
//!   refreshes `last_heartbeat_at` every `HeartbeatInterval`, drops records the
//!   store no longer knows about and cancels the handler token of records whose
//!   `cancel` flag is set;
//! * the handler result maps to `MarkComplete` / `MarkErrored`; an error raised
//!   while the token was cancelled maps to `MarkFailed` (the baseline's
//!   `isJobCanceled`); a store error while marking is logged, not retried;
//! * the resetter periodically calls `ResetStalled`, logs every reset/failure
//!   and its errors, and sleeps `Interval`.
//!
//! Not ported yet: per-worker Prometheus metrics (`workerutil.NewMetrics`),
//! `NumTotalJobs` / `MaxActiveTime` / `MaximumRuntimePerJob`, `PreDequeue` /
//! `WithHooks`, tracing spans, and the `RecordUID` beyond the numeric id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::recorder::{Recordable, Recorder, RoutineType};

/// `workerutil.WorkerOptions` subset used by the ported jobs.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// `options.Name`.
    pub name: String,
    /// `options.Description`.
    pub description: String,
    /// `options.Interval`.
    pub interval: Duration,
    /// `options.HeartbeatInterval`.
    pub heartbeat_interval: Duration,
    /// `options.NumHandlers`.
    pub num_handlers: usize,
    /// `hostname.Get()`; written to `worker_hostname` on dequeue.
    pub worker_hostname: String,
}

impl WorkerOptions {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            num_handlers: 1,
            worker_hostname: default_hostname(),
        }
    }
}

/// `hostname.Get`: `HOSTNAME` or the kernel host name.
pub fn default_hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .map(|value| value.trim().to_owned())
                .unwrap_or_default()
        })
}

/// `workerutil.Record`: the two identifiers the loop needs.
pub trait DbRecord: Send + Sync + 'static {
    fn record_id(&self) -> i64;

    /// `RecordUID()`; the baseline's outbound webhook job uses the decimal id.
    fn record_uid(&self) -> String {
        self.record_id().to_string()
    }
}

/// The store operations `workerutil.Worker` drives.
#[async_trait]
pub trait DbWorkerStore: Send + Sync + 'static {
    type Record: DbRecord;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String>;

    /// Returns `(known, canceled)` record UIDs.
    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String>;

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String>;

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String>;

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String>;
}

/// `workerutil.Handler`.
#[async_trait]
pub trait DbWorkerHandler<S: DbWorkerStore>: Send + Sync + 'static {
    async fn handle(&self, ctx: CancellationToken, record: &S::Record) -> Result<(), String>;

    /// `workerutil.NonRetryableError`: a failed record whose error implements
    /// this is marked failed instead of errored (`mark_failed` rather than
    /// `mark_errored`). The baseline's `insights.queryRunner` terminal
    /// streaming error is the only ported user.
    fn non_retryable(&self, _failure_message: &str) -> bool {
        false
    }
}

/// The store operation `dbworker.Resetter` drives.
#[async_trait]
pub trait DbResetter: Send + Sync + 'static {
    /// `ResetStalled`: `(reset-to-queued, reset-to-failed)`, each with the age
    /// of the last heartbeat.
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String>;
}

/// A background routine that is `DB_BACKED` for the recorder and blocks in
/// `start` until `shutdown` is cancelled.
pub trait DbBackedRoutine: Recordable {
    fn start_arc(
        self: Arc<Self>,
        shutdown: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

    /// The `recorder.Recordable` view. `dbworker.Resetter` does not implement
    /// `Recordable` in the baseline, so it returns `None`.
    fn recordable(self: Arc<Self>) -> Option<Arc<dyn Recordable>>;
}

/// `workerutil.Worker` / `dbworker.Worker`.
pub struct WorkerRoutine<S: DbWorkerStore, H: DbWorkerHandler<S>> {
    options: WorkerOptions,
    store: Arc<S>,
    handler: Arc<H>,
    job_name: Mutex<String>,
    recorder: Mutex<Option<Arc<Recorder>>>,
}

impl<S: DbWorkerStore, H: DbWorkerHandler<S>> WorkerRoutine<S, H> {
    pub fn new(store: Arc<S>, handler: Arc<H>, options: WorkerOptions) -> Self {
        Self {
            options,
            store,
            handler,
            job_name: Mutex::new(String::new()),
            recorder: Mutex::new(None),
        }
    }

    pub fn options(&self) -> &WorkerOptions {
        &self.options
    }

    fn recorder(&self) -> Option<Arc<Recorder>> {
        self.recorder.lock().expect("recorder lock").clone()
    }
}

impl<S: DbWorkerStore, H: DbWorkerHandler<S>> Recordable for WorkerRoutine<S, H> {
    fn name(&self) -> &str {
        &self.options.name
    }

    fn routine_type(&self) -> RoutineType {
        RoutineType::DbBacked
    }

    fn job_name(&self) -> String {
        self.job_name.lock().expect("job name lock").clone()
    }

    fn set_job_name(&self, job_name: &str) {
        *self.job_name.lock().expect("job name lock") = job_name.to_owned();
    }

    fn description(&self) -> &str {
        &self.options.description
    }

    fn interval(&self) -> Duration {
        self.options.interval
    }

    fn register_recorder(&self, recorder: Arc<Recorder>) {
        *self.recorder.lock().expect("recorder lock") = Some(recorder);
    }
}

impl<S: DbWorkerStore, H: DbWorkerHandler<S>> DbBackedRoutine for WorkerRoutine<S, H> {
    fn start_arc(
        self: Arc<Self>,
        shutdown: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move { self.run(shutdown).await })
    }

    fn recordable(self: Arc<Self>) -> Option<Arc<dyn Recordable>> {
        Some(self)
    }
}

impl<S: DbWorkerStore, H: DbWorkerHandler<S>> WorkerRoutine<S, H> {
    async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Some(recorder) = self.recorder() {
            recorder.log_start(self.as_ref()).await;
        }

        let running: Arc<Mutex<HashMap<String, CancellationToken>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let semaphore = Arc::new(Semaphore::new(self.options.num_handlers));
        let mut tasks: JoinSet<()> = JoinSet::new();

        // Heartbeat task; heartbeats outlive individual handler slots, exactly
        // like the baseline's dedicated goroutine.
        {
            let store = Arc::clone(&self.store);
            let running = Arc::clone(&running);
            let interval = self.options.heartbeat_interval;
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = sleep(interval) => {}
                    }

                    let ids: Vec<String> = running
                        .lock()
                        .expect("running set lock")
                        .keys()
                        .cloned()
                        .collect();
                    let (known, canceled) = match store.heartbeat(&ids).await {
                        Ok(result) => result,
                        Err(err) => {
                            tracing::error!(?ids, error = %err, "Failed to refresh heartbeats");
                            continue;
                        }
                    };

                    let known: std::collections::HashSet<&String> = known.iter().collect();
                    let mut running = running.lock().expect("running set lock");
                    for id in &ids {
                        if !known.contains(id) && running.remove(id).is_some() {
                            tracing::error!(id = %id, "Removed unknown job from running set");
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
            });
        }

        loop {
            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                permit = semaphore.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };

            let record = match self.store.dequeue(&self.options.worker_hostname).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    drop(permit);
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(self.options.interval) => continue,
                    }
                }
                Err(err) => {
                    drop(permit);
                    tracing::error!(
                        name = %self.options.name,
                        error = %err,
                        "Failed to dequeue and handle record"
                    );
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(self.options.interval) => continue,
                    }
                }
            };

            let token = shutdown.child_token();
            let uid = record.record_uid();
            running
                .lock()
                .expect("running set lock")
                .insert(uid.clone(), token.clone());

            let store = Arc::clone(&self.store);
            let handler = Arc::clone(&self.handler);
            let running = Arc::clone(&running);
            let recorder = self.recorder();
            let recordable: Arc<dyn Recordable> = self.clone();
            tasks.spawn(async move {
                let started = Instant::now();
                let result = handler.handle(token.clone(), &record).await;
                let run_error = result.as_ref().err().cloned();

                if let Err(handle_err) = result {
                    tracing::error!(error = %handle_err, "Failed to finalize record");
                    let marked = if token.is_cancelled() || handler.non_retryable(&handle_err) {
                        store.mark_failed(&record, &handle_err).await
                    } else {
                        store.mark_errored(&record, &handle_err).await
                    };
                    if let Err(mark_err) = marked {
                        tracing::error!(
                            error = %mark_err,
                            "Failed to finalize record"
                        );
                    }
                } else if let Err(mark_err) = store.mark_complete(&record).await {
                    tracing::error!(error = %mark_err, "Failed to finalize record");
                }

                running.lock().expect("running set lock").remove(&uid);
                if let Some(recorder) = recorder {
                    recorder
                        .log_run(recordable.as_ref(), started.elapsed(), run_error.as_deref())
                        .await;
                }
                drop(permit);
            });
        }

        // The baseline's `wg.Wait()`: dequeueing stops, in-flight handlers
        // finish and mark their records.
        while tasks.join_next().await.is_some() {}

        if let Some(recorder) = self.recorder() {
            recorder.log_stop(self.as_ref()).await;
        }
    }
}

/// `dbworker.ResetterOptions`.
#[derive(Clone, Debug)]
pub struct ResetterOptions {
    pub name: String,
    pub interval: Duration,
}

/// `dbworker.Resetter`.
pub struct ResetterRoutine {
    options: ResetterOptions,
    store: Arc<dyn DbResetter>,
    job_name: Mutex<String>,
    recorder: Mutex<Option<Arc<Recorder>>>,
}

impl ResetterRoutine {
    pub fn new(store: Arc<dyn DbResetter>, options: ResetterOptions) -> Self {
        Self {
            options,
            store,
            job_name: Mutex::new(String::new()),
            recorder: Mutex::new(None),
        }
    }

    fn recorder(&self) -> Option<Arc<Recorder>> {
        self.recorder.lock().expect("recorder lock").clone()
    }
}

impl Recordable for ResetterRoutine {
    fn name(&self) -> &str {
        &self.options.name
    }

    /// `Resetter.Type()` is not defined (it does not implement
    /// `recorder.Recordable` in the baseline); treat it like a DB-backed
    /// routine so the recorder can still describe it.
    fn routine_type(&self) -> RoutineType {
        RoutineType::DbBacked
    }

    fn job_name(&self) -> String {
        self.job_name.lock().expect("job name lock").clone()
    }

    fn set_job_name(&self, job_name: &str) {
        *self.job_name.lock().expect("job name lock") = job_name.to_owned();
    }

    fn description(&self) -> &str {
        ""
    }

    fn interval(&self) -> Duration {
        self.options.interval
    }

    fn register_recorder(&self, recorder: Arc<Recorder>) {
        *self.recorder.lock().expect("recorder lock") = Some(recorder);
    }
}

impl DbBackedRoutine for ResetterRoutine {
    fn start_arc(
        self: Arc<Self>,
        shutdown: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move { self.run(shutdown).await })
    }

    fn recordable(self: Arc<Self>) -> Option<Arc<dyn Recordable>> {
        // `dbworker.Resetter` is not a `recorder.Recordable`.
        None
    }
}

impl ResetterRoutine {
    async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Some(recorder) = self.recorder() {
            recorder.log_start(self.as_ref()).await;
        }

        loop {
            match self.store.reset_stalled().await {
                Ok((reset, failed)) => {
                    for (id, age) in &reset {
                        tracing::warn!(
                            name = %self.options.name,
                            id = *id,
                            time_since_last_heartbeat = ?age,
                            "Reset stalled record back to 'queued' state"
                        );
                    }
                    for (id, age) in &failed {
                        tracing::warn!(
                            name = %self.options.name,
                            id = *id,
                            time_since_last_heartbeat = ?age,
                            "Reset stalled record to 'failed' state"
                        );
                    }
                }
                Err(err) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    tracing::error!(
                        name = %self.options.name,
                        error = %err,
                        "Failed to reset stalled records"
                    );
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(self.options.interval) => {}
            }
        }

        if let Some(recorder) = self.recorder() {
            recorder.log_stop(self.as_ref()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct TestRecord(i64);

    impl DbRecord for TestRecord {
        fn record_id(&self) -> i64 {
            self.0
        }
    }

    struct TestStore {
        records: Mutex<Vec<TestRecord>>,
        completed: Mutex<Vec<i64>>,
        errored: Mutex<Vec<(i64, String)>>,
        failed: Mutex<Vec<(i64, String)>>,
        empty: Notify,
    }

    #[async_trait]
    impl DbWorkerStore for TestStore {
        type Record = TestRecord;

        async fn dequeue(&self, _worker_hostname: &str) -> Result<Option<Self::Record>, String> {
            let mut records = self.records.lock().expect("records");
            let record = records.pop();
            if record.is_none() {
                self.empty.notify_waiters();
            }
            Ok(record)
        }

        async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
            Ok((ids.to_vec(), Vec::new()))
        }

        async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
            self.completed.lock().expect("completed").push(record.0);
            Ok(true)
        }

        async fn mark_errored(
            &self,
            record: &Self::Record,
            failure_message: &str,
        ) -> Result<bool, String> {
            self.errored
                .lock()
                .expect("errored")
                .push((record.0, failure_message.to_owned()));
            Ok(true)
        }

        async fn mark_failed(
            &self,
            record: &Self::Record,
            failure_message: &str,
        ) -> Result<bool, String> {
            self.failed
                .lock()
                .expect("failed")
                .push((record.0, failure_message.to_owned()));
            Ok(true)
        }
    }

    struct TestHandler {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DbWorkerHandler<TestStore> for TestHandler {
        async fn handle(&self, _ctx: CancellationToken, record: &TestRecord) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if record.0 == 2 {
                return Err("boom".to_owned());
            }
            Ok(())
        }
    }

    struct TestResetter {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DbResetter for TestResetter {
        async fn reset_stalled(
            &self,
        ) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok((vec![(7, Duration::from_secs(11))], Vec::new()))
        }
    }

    #[tokio::test]
    async fn worker_marks_every_record_and_stops() {
        let store = Arc::new(TestStore {
            records: Mutex::new(vec![TestRecord(3), TestRecord(2), TestRecord(1)]),
            completed: Mutex::new(Vec::new()),
            errored: Mutex::new(Vec::new()),
            failed: Mutex::new(Vec::new()),
            empty: Notify::new(),
        });
        let handler = Arc::new(TestHandler {
            calls: AtomicUsize::new(0),
        });
        let mut options = WorkerOptions::new("test_worker");
        options.interval = Duration::from_millis(20);
        let routine = Arc::new(WorkerRoutine::new(
            Arc::clone(&store),
            Arc::clone(&handler),
            options,
        ));

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&routine).start_arc(shutdown.clone()));

        // Wait until all three records were handled and marked.
        for _ in 0..200 {
            if handler.calls.load(Ordering::SeqCst) == 3
                && store.completed.lock().expect("completed").len() == 2
                && store.errored.lock().expect("errored").len() == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown.cancel();
        task.await.expect("join");

        assert_eq!(handler.calls.load(Ordering::SeqCst), 3);
        let mut completed = store.completed.lock().expect("completed").clone();
        completed.sort_unstable();
        assert_eq!(completed, vec![1, 3]);
        assert_eq!(
            store.errored.lock().expect("errored").as_slice(),
            &[(2, "boom".to_owned())]
        );
        assert!(store.failed.lock().expect("failed").is_empty());
        assert_eq!(routine.routine_type(), RoutineType::DbBacked);
    }

    #[tokio::test]
    async fn resetter_runs_until_shutdown() {
        let store = Arc::new(TestResetter {
            calls: AtomicUsize::new(0),
        });
        let routine = Arc::new(ResetterRoutine::new(
            store.clone(),
            ResetterOptions {
                name: "test_resetter".to_owned(),
                interval: Duration::from_millis(20),
            },
        ));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&routine).start_arc(shutdown.clone()));

        for _ in 0..100 {
            if store.calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shutdown.cancel();
        task.await.expect("join");

        assert!(store.calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(routine.routine_type(), RoutineType::DbBacked);
        assert_eq!(routine.name(), "test_resetter");
    }
}
