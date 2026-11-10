#![forbid(unsafe_code)]

//! Search result aggregation for zoekt.
//!
//! Mirrors `internal/search/backend/aggregate.go`:
//!
//! - [`CollectSender`] is `collectSender`: it combines `Stats`, appends
//!   `Files`, tracks `SizeBytes`, and on [`CollectSender::done`] ranks
//!   (`SortFiles`) and truncates to `MaxDocDisplayCount`.
//! - [`FlushSender`] / [`NopFlushCollector`] / [`FlushCollectSender`] are
//!   `FlushSender` / `nopFlushCollector` / `flushCollectSender`: results are
//!   collected until every endpoint has sent a first non-empty result (or
//!   `SendDone`), then flushed with a [`FlushReason`] and streamed directly
//!   afterwards. A `2 * FlushWallTime` timer is a fallback.
//!
//! The baseline records two Prometheus histograms when it flushes. This port
//! deliberately has no exporter: [`FlushSender::last_flush_metrics`] exposes
//! the same counts as a plain struct.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::file_ranking::sort_files;
use crate::types::{FlushReason, SearchOptions, SearchResult};

/// `zoekt.Sender`: receives aggregated and streamed search results.
pub trait ResultSender: Send + Sync {
    fn send(&self, result: SearchResult);
}

/// A sender for aggregated search results.
///
/// Methods take `&self`: the mutex inside [`FlushCollectSender`] provides the
/// same serialization as the baseline's `sync.Mutex`, without requiring an
/// exclusive borrow from every caller/goroutine.
pub trait FlushSender: Send + Sync {
    /// Consume a search event. `endpoint` identifies the zoekt-webserver.
    fn send(&self, endpoint: &str, event: SearchResult);

    /// Signal that `endpoint` will not send any more events.
    fn send_done(&self, endpoint: &str);

    /// Flush whatever has been collected (`FlushReasonFinalFlush`).
    fn flush(&self);

    /// Counts observed by the baseline's two Prometheus histograms, if a
    /// flush has happened.
    fn last_flush_metrics(&self) -> Option<FlushMetrics> {
        None
    }
}

/// The values the baseline observes in `stopCollectingAndFlush`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushMetrics {
    pub reason: FlushReason,
    /// `len(agg.Files)` after `MaxDocDisplayCount` truncation.
    pub aggregate_size: usize,
    pub overflow_size: usize,
}

/// `collectSender`: aggregate results, ranking and truncating on [`done`].
///
/// [`done`]: CollectSender::done
#[derive(Debug)]
pub struct CollectSender {
    aggregate: Option<SearchResult>,
    overflow: Vec<SearchResult>,
    opts: SearchOptions,
    size_bytes: u64,
}

impl CollectSender {
    pub fn new(opts: SearchOptions) -> Self {
        Self {
            aggregate: None,
            overflow: Vec::new(),
            opts,
            size_bytes: 0,
        }
    }

    /// `(*collectSender).Send`.
    pub fn send(&mut self, result: SearchResult) {
        let size = result.size_bytes();

        let aggregate = self.aggregate.get_or_insert_with(SearchResult::default);
        aggregate.stats.add(&result.stats);
        if !result.files.is_empty() {
            aggregate.files.extend(result.files);
        }

        self.size_bytes += size;
    }

    /// `(*collectSender).SendOverflow`.
    pub fn send_overflow(&mut self, result: SearchResult) {
        self.size_bytes += result.size_bytes();
        self.overflow.push(result);
    }

    /// `(*collectSender).Done`. `None` means no result was aggregated.
    pub fn done(&mut self) -> Option<(SearchResult, Vec<SearchResult>)> {
        let mut aggregate = self.aggregate.take()?;

        sort_files(&mut aggregate.files);
        let max = self.opts.max_doc_display_count;
        if max > 0 && aggregate.files.len() > max as usize {
            aggregate.files.truncate(max as usize);
        }

        let overflow = std::mem::take(&mut self.overflow);
        self.size_bytes = 0;
        Some((aggregate, overflow))
    }

    /// Accumulated `SizeBytes` for the collected results so far.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// `nopFlushCollector`: pass results straight through (`opts == nil`).
pub struct NopFlushCollector {
    sender: Arc<dyn ResultSender>,
}

impl FlushSender for NopFlushCollector {
    fn send(&self, _endpoint: &str, event: SearchResult) {
        self.sender.send(event);
    }

    fn send_done(&self, _endpoint: &str) {}

    fn flush(&self) {}
}

/// A timer used for the `2 * FlushWallTime` fallback. Injectable so tests are
/// deterministic; [`TokioTimer`] is the production implementation.
pub trait Timer: Send + Sync {
    fn schedule(&self, delay: Duration, callback: Box<dyn FnOnce() + Send>)
        -> Box<dyn TimerHandle>;
}

/// Handle returned by [`Timer::schedule`]; cancelling prevents the callback.
pub trait TimerHandle: Send {
    fn cancel(self: Box<Self>);
}

/// Production timer backed by `tokio::time` and a `oneshot` cancel channel.
pub struct TokioTimer;

struct TokioTimerHandle {
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TimerHandle for TokioTimerHandle {
    fn cancel(mut self: Box<Self>) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

impl Timer for TokioTimer {
    fn schedule(
        &self,
        delay: Duration,
        callback: Box<dyn FnOnce() + Send>,
    ) -> Box<dyn TimerHandle> {
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(delay) => callback(),
                _ = cancelled => {}
            }
        });
        Box::new(TokioTimerHandle {
            cancel: Some(cancel),
        })
    }
}

struct Inner {
    collect_sender: Option<CollectSender>,
    sender: Arc<dyn ResultSender>,
    /// Endpoints we have not yet received a first non-empty result from.
    first_results: HashSet<String>,
    max_size_bytes: i64,
    timer_cancel: Option<Box<dyn TimerHandle>>,
    metrics: Option<FlushMetrics>,
}

impl Inner {
    fn send(&mut self, endpoint: &str, event: SearchResult) {
        let mut flush_reason = None;

        if let Some(collect_sender) = self.collect_sender.as_mut() {
            let has_files = !event.files.is_empty();
            if self.first_results.contains(endpoint) {
                collect_sender.send(event);
                // Ignore first events with no files, like stats-only events.
                if has_files {
                    self.first_results.remove(endpoint);
                }
            } else {
                collect_sender.send_overflow(event);
            }

            if self.first_results.is_empty() {
                flush_reason = Some(FlushReason::TimerExpired);
            } else if self.max_size_bytes >= 0
                && collect_sender.size_bytes() > self.max_size_bytes as u64
            {
                // Protect against too large aggregates.
                flush_reason = Some(FlushReason::MaxSize);
            }
        } else {
            self.sender.send(event);
            return;
        }

        if let Some(reason) = flush_reason {
            self.stop_collecting_and_flush(reason);
        }
    }

    fn send_done(&mut self, endpoint: &str) {
        self.first_results.remove(endpoint);
        if self.first_results.is_empty() {
            self.stop_collecting_and_flush(FlushReason::TimerExpired);
        }
    }

    fn stop_collecting_and_flush(&mut self, reason: FlushReason) {
        let Some(collect_sender) = self.collect_sender.as_mut() else {
            return;
        };
        let done = collect_sender.done();
        self.collect_sender = None;

        if let Some((mut aggregate, overflow)) = done {
            self.metrics = Some(FlushMetrics {
                reason,
                aggregate_size: aggregate.files.len(),
                overflow_size: overflow.len(),
            });

            aggregate.stats.flush_reason = reason;
            self.sender.send(aggregate);

            for mut result in overflow {
                result.stats.flush_reason = reason;
                self.sender.send(result);
            }
        }

        // Stop the timer if it is still running.
        if let Some(timer_cancel) = self.timer_cancel.take() {
            timer_cancel.cancel();
        }
    }
}

/// `flushCollectSender`.
pub struct FlushCollectSender {
    inner: Arc<Mutex<Inner>>,
}

impl FlushSender for FlushCollectSender {
    fn send(&self, endpoint: &str, event: SearchResult) {
        lock(&self.inner).send(endpoint, event);
    }

    fn send_done(&self, endpoint: &str) {
        lock(&self.inner).send_done(endpoint);
    }

    fn flush(&self) {
        lock(&self.inner).stop_collecting_and_flush(FlushReason::FinalFlush);
    }

    fn last_flush_metrics(&self) -> Option<FlushMetrics> {
        lock(&self.inner).metrics
    }
}

/// `newFlushCollectSender`, using the production [`TokioTimer`].
///
/// Must be called from within a Tokio runtime when `opts` is `Some` (it
/// schedules the fallback timer). `opts == None` returns a
/// [`NopFlushCollector`], matching zoekt's "Streamer" interface.
pub fn new_flush_collect_sender(
    opts: Option<SearchOptions>,
    endpoints: &[String],
    max_size_bytes: i64,
    sender: Arc<dyn ResultSender>,
) -> Box<dyn FlushSender> {
    new_flush_collect_sender_with_timer(
        opts,
        endpoints,
        max_size_bytes,
        sender,
        Arc::new(TokioTimer),
    )
}

/// `newFlushCollectSender` with an injectable [`Timer`].
pub fn new_flush_collect_sender_with_timer(
    opts: Option<SearchOptions>,
    endpoints: &[String],
    max_size_bytes: i64,
    sender: Arc<dyn ResultSender>,
    timer: Arc<dyn Timer>,
) -> Box<dyn FlushSender> {
    // Nil options are permitted by zoekt's "Streamer" interface.
    let Some(opts) = opts else {
        return Box::new(NopFlushCollector { sender });
    };

    let first_results: HashSet<String> = endpoints.iter().cloned().collect();
    let collect_sender = CollectSender::new(opts.clone());
    let inner = Arc::new(Mutex::new(Inner {
        collect_sender: Some(collect_sender),
        sender,
        first_results,
        max_size_bytes,
        timer_cancel: None,
        metrics: None,
    }));

    {
        let mut guard = lock(&inner);
        let timer_inner = Arc::clone(&inner);
        let handle = timer.schedule(
            // As an escape hatch, stop collecting after twice the
            // FlushWallTime.
            opts.flush_wall_time.saturating_mul(2),
            Box::new(move || {
                lock(&timer_inner).stop_collecting_and_flush(FlushReason::TimerExpired);
            }),
        );
        guard.timer_cancel = Some(handle);
    }

    Box::new(FlushCollectSender { inner })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileMatch;

    #[derive(Default)]
    struct RecordingSender {
        sent: Mutex<Vec<SearchResult>>,
    }

    impl ResultSender for RecordingSender {
        fn send(&self, result: SearchResult) {
            lock(&self.sent).push(result);
        }
    }

    impl RecordingSender {
        fn take(&self) -> Vec<SearchResult> {
            std::mem::take(&mut *lock(&self.sent))
        }
    }

    type MockCallback = Option<Box<dyn FnOnce() + Send>>;

    #[derive(Default)]
    struct MockTimerState {
        callbacks: Mutex<Vec<MockCallback>>,
    }

    #[derive(Default, Clone)]
    struct MockTimer {
        state: Arc<MockTimerState>,
    }

    impl MockTimer {
        fn fire(&self) {
            let callbacks = std::mem::take(&mut *lock(&self.state.callbacks));
            for callback in callbacks.into_iter().flatten() {
                callback();
            }
        }
    }

    struct MockTimerHandle {
        state: Arc<MockTimerState>,
        index: usize,
    }

    impl TimerHandle for MockTimerHandle {
        fn cancel(self: Box<Self>) {
            if let Some(slot) = lock(&self.state.callbacks).get_mut(self.index) {
                *slot = None;
            }
        }
    }

    impl Timer for MockTimer {
        fn schedule(
            &self,
            _delay: Duration,
            callback: Box<dyn FnOnce() + Send>,
        ) -> Box<dyn TimerHandle> {
            let mut callbacks = lock(&self.state.callbacks);
            callbacks.push(Some(callback));
            let index = callbacks.len() - 1;
            Box::new(MockTimerHandle {
                state: Arc::clone(&self.state),
                index,
            })
        }
    }

    fn file(name: &str, score: f64) -> FileMatch {
        FileMatch {
            file_name: name.as_bytes().to_vec(),
            score,
            ..FileMatch::default()
        }
    }

    fn result(files: Vec<FileMatch>) -> SearchResult {
        SearchResult {
            files,
            ..SearchResult::default()
        }
    }

    fn endpoints(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn collect_sender_returns_none_when_empty() {
        let mut sender = CollectSender::new(SearchOptions::default());
        assert!(sender.done().is_none());
    }

    #[test]
    fn collect_sender_aggregates_stats_and_truncates() {
        let mut first = result(vec![file("a.go", 1.0), file("b.md", 2.0)]);
        first.stats.match_count = 3;
        let first_size = first.size_bytes();
        let mut sender = CollectSender::new(SearchOptions {
            max_doc_display_count: 1,
            ..SearchOptions::default()
        });
        sender.send(first);
        assert_eq!(sender.size_bytes(), first_size);

        let (aggregate, overflow) = sender.done().expect("aggregated");
        assert!(overflow.is_empty());
        assert_eq!(aggregate.stats.match_count, 3);
        // Sorted by score descending, then truncated to one file.
        assert_eq!(aggregate.files.len(), 1);
        assert_eq!(aggregate.files[0].file_name, b"b.md");
        assert_eq!(sender.size_bytes(), 0);
    }

    #[test]
    fn collects_until_every_endpoint_sent_a_non_empty_result() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            -1,
            recorder.clone(),
            Arc::new(MockTimer::default()),
        );

        sender.send("a", result(vec![file("a.go", 1.0)]));
        assert!(recorder.take().is_empty());
        sender.send("b", result(vec![file("b.go", 2.0)]));

        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::TimerExpired);
        assert_eq!(sent[0].files.len(), 2);
        assert_eq!(sent[0].files[0].file_name, b"b.go");
        assert_eq!(
            sender.last_flush_metrics(),
            Some(FlushMetrics {
                reason: FlushReason::TimerExpired,
                aggregate_size: 2,
                overflow_size: 0,
            })
        );
    }

    #[test]
    fn stats_only_first_event_does_not_complete_an_endpoint() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            -1,
            recorder.clone(),
            Arc::new(MockTimer::default()),
        );

        sender.send(
            "a",
            SearchResult {
                stats: crate::types::Stats {
                    match_count: 1,
                    ..crate::types::Stats::default()
                },
                ..SearchResult::default()
            },
        );
        assert!(recorder.take().is_empty());
        sender.send("a", result(vec![file("a.go", 1.0)]));
        assert!(recorder.take().is_empty());
        sender.send_done("b");

        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::TimerExpired);
        assert_eq!(sent[0].stats.match_count, 1);
        assert_eq!(sent[0].files.len(), 1);
    }

    #[test]
    fn later_events_from_completed_endpoints_become_overflow() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            -1,
            recorder.clone(),
            Arc::new(MockTimer::default()),
        );

        sender.send("a", result(vec![file("a.go", 1.0)]));
        sender.send("a", result(vec![file("a2.go", 3.0)]));
        sender.send("b", result(vec![file("b.go", 2.0)]));

        let sent = recorder.take();
        assert_eq!(sent.len(), 2);
        // The aggregate holds a.go and b.go, ranked by score descending.
        assert_eq!(sent[0].files.len(), 2);
        assert_eq!(sent[0].files[0].file_name, b"b.go");
        assert_eq!(sent[0].files[1].file_name, b"a.go");
        // The second event from `a` was overflow.
        assert_eq!(sent[1].files.len(), 1);
        assert_eq!(sent[1].files[0].file_name, b"a2.go");
        assert!(sent
            .iter()
            .all(|result| result.stats.flush_reason == FlushReason::TimerExpired));
        assert_eq!(
            sender.last_flush_metrics(),
            Some(FlushMetrics {
                reason: FlushReason::TimerExpired,
                aggregate_size: 2,
                overflow_size: 1,
            })
        );
    }

    #[test]
    fn max_size_flushes_with_max_size_reason() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            0,
            recorder.clone(),
            Arc::new(MockTimer::default()),
        );

        sender.send("a", result(vec![]));
        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::MaxSize);
        assert_eq!(
            sender.last_flush_metrics().map(|m| m.reason),
            Some(FlushReason::MaxSize)
        );
    }

    #[test]
    fn flush_uses_final_flush_reason() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            -1,
            recorder.clone(),
            Arc::new(MockTimer::default()),
        );

        sender.send("a", result(vec![file("a.go", 1.0)]));
        sender.flush();
        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::FinalFlush);
        assert_eq!(
            sender.last_flush_metrics().map(|m| m.reason),
            Some(FlushReason::FinalFlush)
        );

        // After the flush, events stream through untouched.
        sender.send("b", result(vec![file("b.go", 1.0)]));
        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::Unknown);
        assert_eq!(sent[0].files[0].file_name, b"b.go");
    }

    #[test]
    fn timer_fires_timer_expired_reason() {
        let recorder = Arc::new(RecordingSender::default());
        let timer = MockTimer::default();
        let sender = new_flush_collect_sender_with_timer(
            Some(SearchOptions::default()),
            &endpoints(&["a", "b"]),
            -1,
            recorder.clone(),
            Arc::new(timer.clone()),
        );

        sender.send(
            "a",
            SearchResult {
                stats: crate::types::Stats {
                    match_count: 1,
                    ..crate::types::Stats::default()
                },
                ..SearchResult::default()
            },
        );
        assert!(recorder.take().is_empty());
        timer.fire();

        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::TimerExpired);
    }

    #[test]
    fn nil_options_pass_through() {
        let recorder = Arc::new(RecordingSender::default());
        let sender = new_flush_collect_sender(None, &endpoints(&["a"]), -1, recorder.clone());

        sender.send("a", result(vec![file("a.go", 1.0)]));
        sender.send_done("a");
        sender.flush();

        let sent = recorder.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].stats.flush_reason, FlushReason::Unknown);
        assert_eq!(sender.last_flush_metrics(), None);
    }
}
