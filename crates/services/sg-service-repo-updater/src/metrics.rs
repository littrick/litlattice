#![forbid(unsafe_code)]

//! `cmd/repo-updater/internal/scheduler/metrics.go`.
//!
//! Only the scheduler families are implemented in this slice; the
//! `repos.Store`/`Source` families from `internal/repos` land with the syncer.

use sg_metrics::prometheus::{Counter, CounterVec, Gauge, Opts};

/// `src_repoupdater_sched_*`.
pub struct Metrics {
    pub sched_error: CounterVec,
    pub sched_loops: Counter,
    pub sched_auto_fetch: Counter,
    pub sched_manual_fetch: Counter,
    pub sched_known_repos: Gauge,
    pub sched_update_queue_length: Gauge,
}

impl Metrics {
    /// Build and register the scheduler families. Registration is best-effort:
    /// tests build several schedulers in one process.
    pub fn new() -> Self {
        let sched_error = CounterVec::new(
            Opts::new(
                "src_repoupdater_sched_error",
                "Incremented each time we encounter an error updating a repository.",
            ),
            &["type"],
        )
        .expect("valid metric");
        let sched_loops = Counter::new(
            "src_repoupdater_sched_loops",
            "Incremented each time the scheduler loops.",
        )
        .expect("valid metric");
        let sched_auto_fetch = Counter::new(
            "src_repoupdater_sched_auto_fetch",
            "Incremented each time the scheduler updates a managed repository due to hitting a deadline.",
        )
        .expect("valid metric");
        let sched_manual_fetch = Counter::new(
            "src_repoupdater_sched_manual_fetch",
            "Incremented each time the scheduler updates a repository due to user traffic.",
        )
        .expect("valid metric");
        let sched_known_repos = Gauge::new(
            "src_repoupdater_sched_known_repos",
            "The number of repositories that are managed by the scheduler.",
        )
        .expect("valid metric");
        let sched_update_queue_length = Gauge::new(
            "src_repoupdater_sched_update_queue_length",
            "The number of repositories that are currently queued for update",
        )
        .expect("valid metric");

        let registry = sg_metrics::registry();
        for family in [
            Box::new(sched_error.clone()) as Box<dyn sg_metrics::prometheus::core::Collector>,
            Box::new(sched_loops.clone()),
            Box::new(sched_auto_fetch.clone()),
            Box::new(sched_manual_fetch.clone()),
            Box::new(sched_known_repos.clone()),
            Box::new(sched_update_queue_length.clone()),
        ] {
            let _ = registry.register(family);
        }

        Self {
            sched_error,
            sched_loops,
            sched_auto_fetch,
            sched_manual_fetch,
            sched_known_repos,
            sched_update_queue_length,
        }
    }

    pub fn inc_error(&self, kind: &str) {
        self.sched_error.with_label_values(&[kind]).inc();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
