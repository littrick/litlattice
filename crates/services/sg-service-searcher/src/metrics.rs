#![forbid(unsafe_code)]

//! Metric families exported by `searcher`, named exactly like the Go baseline
//! (`cmd/searcher/internal/search/*.go`).
//!
//! Families are registered eagerly so gauges appear at zero from startup.
//! Counter/histogram vec families only emit series once observed, matching
//! `promauto` in the baseline.

use std::sync::OnceLock;

use sg_metrics::prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};

pub struct Metrics {
    service_running: Gauge,
    service_request_total: CounterVec,
    service_request_total_structural_search: CounterVec,
    service_archive_size_bytes: Histogram,
    service_archive_files: Histogram,
    store_max_cache_size_bytes: Gauge,
    store_cache_size_bytes: Gauge,
    store_evictions: Counter,
    store_fetching: Gauge,
    store_fetch_queue_size: Gauge,
    store_fetch_failed: Counter,
    store_zip_prepare_duration: HistogramVec,
    hybrid_retry_total: CounterVec,
    hybrid_final_state_total: CounterVec,
}

impl Metrics {
    pub fn new(registry: &Registry) -> Self {
        fn gauge(registry: &Registry, name: &str, help: &str) -> Gauge {
            let gauge = Gauge::new(name, help).expect("valid gauge");
            registry
                .register(Box::new(gauge.clone()))
                .expect("register gauge");
            gauge
        }
        fn counter(registry: &Registry, name: &str, help: &str) -> Counter {
            let counter = Counter::new(name, help).expect("valid counter");
            registry
                .register(Box::new(counter.clone()))
                .expect("register counter");
            counter
        }
        fn histogram(registry: &Registry, name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
            let histogram = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
                .expect("valid histogram");
            registry
                .register(Box::new(histogram.clone()))
                .expect("register histogram");
            histogram
        }
        fn counter_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> CounterVec {
            let counter =
                CounterVec::new(Opts::new(name, help), labels).expect("valid counter vec");
            registry
                .register(Box::new(counter.clone()))
                .expect("register counter vec");
            counter
        }
        fn histogram_vec(
            registry: &Registry,
            name: &str,
            help: &str,
            labels: &[&str],
            buckets: Vec<f64>,
        ) -> HistogramVec {
            let histogram =
                HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
                    .expect("valid histogram vec");
            registry
                .register(Box::new(histogram.clone()))
                .expect("register histogram vec");
            histogram
        }

        const MEGABYTE: f64 = 1_000_000.0;

        Self {
            service_running: gauge(
                registry,
                "searcher_service_running",
                "Number of running search requests.",
            ),
            service_request_total: counter_vec(
                registry,
                "searcher_service_request_total",
                "Number of returned search requests.",
                &["code"],
            ),
            service_request_total_structural_search: counter_vec(
                registry,
                "searcher_service_request_total_structural_search",
                "Number of returned structural search requests.",
                &["language"],
            ),
            service_archive_size_bytes: histogram(
                registry,
                "searcher_service_archive_size_bytes",
                "Observes the size when an archive is searched.",
                vec![
                    1.0 * MEGABYTE,
                    10.0 * MEGABYTE,
                    100.0 * MEGABYTE,
                    500.0 * MEGABYTE,
                    1000.0 * MEGABYTE,
                    5000.0 * MEGABYTE,
                ],
            ),
            service_archive_files: histogram(
                registry,
                "searcher_service_archive_files",
                "Observes the number of files when an archive is searched.",
                vec![100.0, 1000.0, 10000.0, 50000.0, 100000.0],
            ),
            store_max_cache_size_bytes: gauge(
                registry,
                "searcher_store_max_cache_size_bytes",
                "The configured maximum size of items in the on disk cache before eviction.",
            ),
            store_cache_size_bytes: gauge(
                registry,
                "searcher_store_cache_size_bytes",
                "The total size of items in the on disk cache.",
            ),
            store_evictions: counter(
                registry,
                "searcher_store_evictions",
                "The total number of items evicted from the cache.",
            ),
            store_fetching: gauge(
                registry,
                "searcher_store_fetching",
                "The number of fetches currently running.",
            ),
            store_fetch_queue_size: gauge(
                registry,
                "searcher_store_fetch_queue_size",
                "The number of fetch jobs enqueued.",
            ),
            store_fetch_failed: counter(
                registry,
                "searcher_store_fetch_failed",
                "The total number of archive fetches that failed.",
            ),
            store_zip_prepare_duration: histogram_vec(
                registry,
                "searcher_store_zip_prepare_duration",
                "Observes the duration to prepare the zip file for searching.",
                &["cache_hit"],
                prometheus_default_buckets(),
            ),
            hybrid_retry_total: counter_vec(
                registry,
                "searcher_hybrid_retry_total",
                "Total number of times we retry zoekt indexed search for hybrid search.",
                &["reason"],
            ),
            hybrid_final_state_total: counter_vec(
                registry,
                "searcher_hybrid_final_state_total",
                "Total number of times a hybrid search ended in a specific state.",
                &["state"],
            ),
        }
    }

    pub fn global() -> &'static Metrics {
        static METRICS: OnceLock<Metrics> = OnceLock::new();
        METRICS.get_or_init(|| Metrics::new(sg_metrics::registry()))
    }

    pub fn request_started(&self) {
        self.service_running.inc();
    }

    pub fn request_finished(&self) {
        self.service_running.dec();
    }

    pub fn request_code(&self, code: &str) {
        self.service_request_total.with_label_values(&[code]).inc();
    }

    pub fn structural_request(&self, language: &str) {
        self.service_request_total_structural_search
            .with_label_values(&[language])
            .inc();
    }

    pub fn archive(&self, files: u64, bytes: usize) {
        self.service_archive_files.observe(files as f64);
        self.service_archive_size_bytes.observe(bytes as f64);
    }

    pub fn max_cache_size(&self, bytes: i64) {
        self.store_max_cache_size_bytes.set(bytes as f64);
    }

    pub fn cache_size(&self, bytes: i64) {
        self.store_cache_size_bytes.set(bytes as f64);
    }

    pub fn evictions(&self, count: u64) {
        self.store_evictions.inc_by(count as f64);
    }

    pub fn fetch_started(&self) {
        self.store_fetching.inc();
    }

    pub fn fetch_finished(&self) {
        self.store_fetching.dec();
    }

    pub fn fetch_enqueued(&self) {
        self.store_fetch_queue_size.inc();
    }

    pub fn fetch_dequeued(&self) {
        self.store_fetch_queue_size.dec();
    }

    pub fn fetch_failed(&self) {
        self.store_fetch_failed.inc();
    }

    pub fn zip_prepare(&self, cache_hit: bool, seconds: f64) {
        self.store_zip_prepare_duration
            .with_label_values(&[if cache_hit { "true" } else { "false" }])
            .observe(seconds);
    }

    pub fn hybrid_retry(&self, reason: &str) {
        self.hybrid_retry_total.with_label_values(&[reason]).inc();
    }

    pub fn hybrid_final_state(&self, state: &str) {
        self.hybrid_final_state_total
            .with_label_values(&[state])
            .inc();
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

    #[test]
    fn exports_baseline_metric_names() {
        let registry = Registry::new();
        let metrics = Metrics::new(&registry);
        metrics.request_started();
        metrics.request_finished();
        metrics.request_code("200");
        metrics.structural_request("go");
        metrics.archive(10, 1024);
        metrics.max_cache_size(100);
        metrics.cache_size(10);
        metrics.evictions(1);
        metrics.fetch_started();
        metrics.fetch_finished();
        metrics.fetch_enqueued();
        metrics.fetch_dequeued();
        metrics.fetch_failed();
        metrics.zip_prepare(true, 0.01);
        metrics.hybrid_retry("max-retrys");
        metrics.hybrid_final_state("ok");

        let text = sg_metrics::render(&registry);
        for name in [
            "searcher_service_running",
            "searcher_service_request_total",
            "searcher_service_request_total_structural_search",
            "searcher_service_archive_size_bytes",
            "searcher_service_archive_files",
            "searcher_store_max_cache_size_bytes",
            "searcher_store_cache_size_bytes",
            "searcher_store_evictions",
            "searcher_store_fetching",
            "searcher_store_fetch_queue_size",
            "searcher_store_fetch_failed",
            "searcher_store_zip_prepare_duration",
            "searcher_hybrid_retry_total",
            "searcher_hybrid_final_state_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
        assert!(
            text.contains("searcher_service_request_total{code=\"200\"} 1"),
            "{text}"
        );
    }
}
