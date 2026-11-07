#![forbid(unsafe_code)]

//! Metric families exported by `symbols`, named exactly like the Go baseline
//! (`cmd/symbols/internal/*/observability.go`).
//!
//! The baseline registers RED metrics through `internal/metrics.NewREDMetrics`
//! (namespace `src`), plus a few standalone gauges/counters. All families are
//! registered eagerly so `/metrics` shows them at zero from startup, which is
//! what dashboards rely on during a gray rollout.

use std::sync::OnceLock;
use std::time::Instant;

use sg_metrics::prometheus::{
    Counter, CounterVec, HistogramOpts, HistogramVec, IntGauge, Opts, Registry,
};

/// One RED metric family set (duration histogram + count + errors).
pub struct Red {
    duration: HistogramVec,
    count: CounterVec,
    errors: CounterVec,
}

impl Red {
    fn new(
        registry: &Registry,
        prefix: &str,
        count_help: &str,
        labels: &[&str],
        buckets: Vec<f64>,
    ) -> Self {
        let duration = HistogramVec::new(
            HistogramOpts::new(
                format!("{prefix}_duration_seconds"),
                format!("Time in seconds spent performing successful {prefix} operations"),
            )
            .buckets(buckets),
            labels,
        )
        .expect("valid duration metric");
        let count = CounterVec::new(Opts::new(format!("{prefix}_total"), count_help), labels)
            .expect("valid count metric");
        let errors = CounterVec::new(
            Opts::new(
                format!("{prefix}_errors_total"),
                format!("Total number of {prefix} operations resulting in an unexpected error"),
            ),
            labels,
        )
        .expect("valid error metric");

        registry
            .register(Box::new(duration.clone()))
            .expect("register duration metric");
        registry
            .register(Box::new(count.clone()))
            .expect("register count metric");
        registry
            .register(Box::new(errors.clone()))
            .expect("register error metric");

        Self {
            duration,
            count,
            errors,
        }
    }

    /// `REDMetrics.Observe`: failed operations only bump the error counter
    /// (and materialize the count series at zero), like the baseline.
    fn observe(&self, labels: &[&str], started: Instant, failed: bool) {
        if failed {
            self.errors.with_label_values(labels).inc();
            self.count.with_label_values(labels).inc_by(0.0);
        } else {
            self.duration
                .with_label_values(labels)
                .observe(started.elapsed().as_secs_f64());
            self.count.with_label_values(labels).inc();
        }
    }
}

pub struct Metrics {
    api: Red,
    parser: Red,
    fetcher: Red,
    gitserver: Red,
    parsing: IntGauge,
    parse_failed: Counter,
    parse_canceled: Counter,
    pub parse_queue_size: IntGauge,
    parse_queue_timeouts: Counter,
    fetching: IntGauge,
    fetch_queue_size: IntGauge,
    store_cache_size_bytes: IntGauge,
    store_evictions: Counter,
    store_errors: Counter,
}

impl Metrics {
    pub fn new(registry: &Registry) -> Self {
        fn gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
            let gauge = IntGauge::new(format!("src_{name}"), help).expect("valid gauge");
            registry
                .register(Box::new(gauge.clone()))
                .expect("register gauge");
            gauge
        }
        fn counter(registry: &Registry, name: &str, help: &str) -> Counter {
            let counter = Counter::new(format!("src_{name}"), help).expect("valid counter");
            registry
                .register(Box::new(counter.clone()))
                .expect("register counter");
            counter
        }

        Self {
            api: Red::new(
                registry,
                "src_codeintel_symbols_api",
                "Total number of method invocations.",
                &["op", "parseAmount"],
                vec![1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
            ),
            parser: Red::new(
                registry,
                "src_codeintel_symbols_parser",
                "Total number of method invocations.",
                &["op"],
                vec![1.0, 5.0, 10.0, 60.0, 300.0, 1200.0],
            ),
            fetcher: Red::new(
                registry,
                "src_codeintel_symbols_repository_fetcher",
                "Total number of method invocations.",
                &["op"],
                prometheus_default_buckets(),
            ),
            gitserver: Red::new(
                registry,
                "src_codeintel_symbols_gitserver",
                "Total number of method invocations.",
                &["op"],
                prometheus_default_buckets(),
            ),
            parsing: gauge(
                registry,
                "codeintel_symbols_parsing",
                "The number of parse jobs currently running.",
            ),
            parse_failed: counter(
                registry,
                "codeintel_symbols_parse_failed_total",
                "The total number of parse jobs that failed.",
            ),
            parse_canceled: counter(
                registry,
                "codeintel_symbols_parse_canceled_total",
                "The total number of parse jobs that are canceled. Seperate to failed since we don't treat these as failed parses.",
            ),
            parse_queue_size: gauge(
                registry,
                "codeintel_symbols_parse_queue_size",
                "The number of parse jobs enqueued.",
            ),
            parse_queue_timeouts: counter(
                registry,
                "codeintel_symbols_parse_queue_timeouts_total",
                "The total number of parse jobs that timed out while enqueued.",
            ),
            fetching: gauge(
                registry,
                "codeintel_symbols_fetching",
                "The number of fetches currently running.",
            ),
            fetch_queue_size: gauge(
                registry,
                "codeintel_symbols_fetch_queue_size",
                "The number of fetch jobs enqueued.",
            ),
            store_cache_size_bytes: gauge(
                registry,
                "codeintel_symbols_store_cache_size_bytes",
                "The total size of items in the on disk cache.",
            ),
            store_evictions: counter(
                registry,
                "codeintel_symbols_store_evictions_total",
                "The total number of items evicted from the cache.",
            ),
            store_errors: counter(
                registry,
                "codeintel_symbols_store_errors_total",
                "The total number of failures evicting items from the cache.",
            ),
        }
    }

    pub fn global() -> &'static Metrics {
        static METRICS: OnceLock<Metrics> = OnceLock::new();
        METRICS.get_or_init(|| Metrics::new(sg_metrics::registry()))
    }

    /// `parse_amount` is the baseline label value; an empty string means the
    /// request failed before the index path was chosen.
    pub fn api_observe(&self, parse_amount: &str, started: Instant, failed: bool) {
        self.api.observe(&["Search", parse_amount], started, failed);
    }

    pub fn parser_observe(&self, op: &str, started: Instant, failed: bool) {
        self.parser.observe(&[op], started, failed);
    }

    pub fn fetcher_observe(&self, op: &str, started: Instant, failed: bool) {
        self.fetcher.observe(&[op], started, failed);
    }

    pub fn gitserver_observe(&self, op: &str, started: Instant, failed: bool) {
        self.gitserver.observe(&[op], started, failed);
    }

    pub fn parse_started(&self) {
        self.parsing.inc();
    }

    pub fn parse_finished(&self, failed: bool, canceled: bool) {
        self.parsing.dec();
        if failed {
            self.parse_failed.inc();
        }
        if canceled {
            self.parse_canceled.inc();
        }
    }

    /// A queued parse job that was canceled before it started.
    pub fn parse_canceled_job(&self) {
        self.parse_canceled.inc();
    }

    pub fn fetch_started(&self) {
        self.fetching.inc();
    }

    pub fn fetch_finished(&self) {
        self.fetching.dec();
    }

    pub fn fetch_enqueued(&self) {
        self.fetch_queue_size.inc();
    }

    pub fn fetch_dequeued(&self) {
        self.fetch_queue_size.dec();
    }

    pub fn cache_size(&self, bytes: u64) {
        self.store_cache_size_bytes
            .set(bytes.min(i64::MAX as u64) as i64);
    }

    pub fn eviction(&self) {
        self.store_evictions.inc();
    }

    pub fn eviction_error(&self) {
        self.store_errors.inc();
    }

    pub fn parse_queue_timeout(&self) {
        self.parse_queue_timeouts.inc();
    }
}

/// `prometheus.DefBuckets`.
fn prometheus_default_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_metrics::prometheus::Registry;

    #[test]
    fn exports_baseline_metric_names() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry);
        metrics.api_observe(
            crate::cache::ParseAmount::Cached.label(),
            Instant::now(),
            false,
        );
        metrics.api_observe("", Instant::now(), true);
        metrics.gitserver_observe("FetchTar", Instant::now(), false);
        metrics.fetcher_observe("FetchRepositoryArchive", Instant::now(), false);

        let text = sg_metrics::render(&registry);
        for name in [
            "src_codeintel_symbols_api_duration_seconds",
            "src_codeintel_symbols_api_total",
            "src_codeintel_symbols_api_errors_total",
            "src_codeintel_symbols_parsing",
            "src_codeintel_symbols_parse_failed_total",
            "src_codeintel_symbols_parse_canceled_total",
            "src_codeintel_symbols_parse_queue_size",
            "src_codeintel_symbols_parse_queue_timeouts_total",
            "src_codeintel_symbols_fetching",
            "src_codeintel_symbols_fetch_queue_size",
            "src_codeintel_symbols_repository_fetcher_total",
            "src_codeintel_symbols_gitserver_total",
            "src_codeintel_symbols_store_cache_size_bytes",
            "src_codeintel_symbols_store_evictions_total",
            "src_codeintel_symbols_store_errors_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
        assert!(
            text.contains(
                "src_codeintel_symbols_api_total{op=\"Search\",parseAmount=\"cached-parse\"} 1"
            ),
            "{text}"
        );
    }
}
