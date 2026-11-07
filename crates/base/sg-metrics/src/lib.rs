#![forbid(unsafe_code)]

//! `sg-metrics`: process-wide Prometheus registry and debug exposition.
//!
//! Mirrors the baseline `internal/debugserver` contract that every service
//! exposes `SRC_PROF_HTTP/metrics` and registers a constant
//! `src_service_metadata` gauge. Services own their metric families; this
//! crate only provides the registry, the Go-identical service metadata gauge
//! and the text encoder.

use std::sync::OnceLock;

use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};

pub use prometheus;

/// Version reported by `src_service_metadata` for a development build
/// (`version.Version()` in a `go build` without release variables).
pub const VERSION: &str = "0.0.0+dev";

/// Process-wide registry, the equivalent of Go's `prometheus.DefaultRegisterer`.
///
/// A single Rust binary hosts one Sourcegraph service, so one registry per
/// process matches the baseline.
pub fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::new)
}

/// Register `src_service_metadata{version="..."} 1` once per process.
pub fn register_service_metadata(version: &str) {
    static METADATA: OnceLock<IntGaugeVec> = OnceLock::new();
    let gauge = METADATA.get_or_init(|| {
        let opts = Opts::new(
            "src_service_metadata",
            "A metric with constant '1' value labelled with Sourcegraph service metadata.",
        );
        let gauge = IntGaugeVec::new(opts, &["version"]).expect("valid service metadata metric");
        // Duplicate registration cannot happen: the gauge is created once.
        let _ = registry().register(Box::new(gauge.clone()));
        gauge
    });
    if let Ok(metric) = gauge.get_metric_with_label_values(&[version]) {
        metric.set(1);
    }
}

/// Render a registry in the Prometheus text exposition format.
pub fn render(registry: &Registry) -> String {
    let mut buffer = Vec::new();
    if TextEncoder::new()
        .encode(&registry.gather(), &mut buffer)
        .is_err()
    {
        return String::new();
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

/// Render the process-wide registry.
pub fn render_default() -> String {
    render(registry())
}

/// Content type used by Go's `promhttp.Handler`.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Counter;

    #[test]
    fn renders_text_exposition() {
        let registry = Registry::new();
        let counter = Counter::new("src_test_total", "test counter").expect("counter");
        registry
            .register(Box::new(counter.clone()))
            .expect("register");
        counter.inc_by(2.0);

        let text = render(&registry);
        assert!(text.contains("# TYPE src_test_total counter"), "{text}");
        assert!(text.contains("src_test_total 2"), "{text}");
    }

    #[test]
    fn service_metadata_is_constant_one() {
        register_service_metadata(VERSION);
        let text = render_default();
        assert!(
            text.contains(&format!("src_service_metadata{{version=\"{VERSION}\"}} 1")),
            "{text}"
        );
    }
}
