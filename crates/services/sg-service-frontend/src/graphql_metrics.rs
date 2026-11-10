#![forbid(unsafe_code)]

//! Prometheus observation of the GraphQL HTTP entry point.
//!
//! Two baselines live here:
//!
//! * `src_graphql_cost_distribution` (`graphql.go:36-46,168-169`), labelled by
//!   [`actor_type_label`] and observed once per request whose query cost could
//!   be estimated;
//! * `src_graphql_request_duration_seconds` (`metrics.go:14-31`), observed by
//!   `instrumentGraphQL` for every request that reaches the defer in
//!   `serveGraphQL` (`graphql.go:146-150,220`).
//!
//! Names, help texts, label names/order and bucket boundaries are part of the
//! `/metrics` contract and must match the baseline byte for byte. Both
//! families are registered when the frontend starts (`promauto` registers at
//! package init), so an empty family is exposed before the first request.

use std::sync::OnceLock;

use sg_metrics::prometheus::{HistogramOpts, HistogramVec};

/// `prometheus.ExponentialBucketsRange(1, 500_000, 8)` as produced by Go —
/// note that the last bound is `499999.999999999`, not an exact 500000.
pub const COST_BUCKETS: [f64; 8] = [
    1.0,
    6.5183634486883895,
    42.4890620491968,
    276.9591490305374,
    1805.3203938204954,
    11767.734468251245,
    76706.37023171943,
    499_999.999_999_999,
];

/// `trace.UserLatencyBuckets` (`internal/trace/buckets.go:7`).
pub const REQUEST_DURATION_BUCKETS: [f64; 7] = [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0];

/// `time.Since(time.Time{})` for early returns: the baseline never sets
/// `execStart` before `:220`, so the deferred histogram observes a value in
/// the year-one epoch (about 6.4e10 seconds) and lands in `+Inf`.
pub const PRE_EXEC_DURATION_SECONDS: f64 = 6.4e10;

static COST_HISTOGRAM: OnceLock<HistogramVec> = OnceLock::new();
static REQUEST_DURATION_HISTOGRAM: OnceLock<HistogramVec> = OnceLock::new();

/// Register both families in the process registry (idempotent).
pub fn register() {
    cost_histogram();
    request_duration_histogram();
}

fn cost_histogram() -> &'static HistogramVec {
    COST_HISTOGRAM.get_or_init(|| {
        let opts = HistogramOpts::new(
            "src_graphql_cost_distribution",
            "The maximum cost seen from a GraphQL query",
        )
        .buckets(COST_BUCKETS.to_vec());
        let histogram = HistogramVec::new(opts, &["actor_type"]).expect("valid cost histogram");
        let _ = sg_metrics::registry().register(Box::new(histogram.clone()));
        histogram
    })
}

fn request_duration_histogram() -> &'static HistogramVec {
    REQUEST_DURATION_HISTOGRAM.get_or_init(|| {
        let opts = HistogramOpts::new(
            "src_graphql_request_duration_seconds",
            "GraphQL request latencies in seconds.",
        )
        .buckets(REQUEST_DURATION_BUCKETS.to_vec());
        let histogram = HistogramVec::new(opts, &["mutation", "route", "success"])
            .expect("valid request duration histogram");
        let _ = sg_metrics::registry().register(Box::new(histogram.clone()));
        histogram
    })
}

/// `actorTypeLabel` (`graphql.go:45-56`).
pub fn actor_type_label(is_internal: bool, anonymous: bool, request_source: &str) -> String {
    if is_internal {
        return "internal".to_owned();
    }
    if anonymous {
        return "anonymous".to_owned();
    }
    if !request_source.is_empty() {
        return request_source.to_owned();
    }
    "unknown".to_owned()
}

/// `costHistogram.WithLabelValues(...).Observe(float64(cost.FieldCount))`.
pub fn observe_cost(actor_type: &str, field_count: i64) {
    if let Ok(metric) = cost_histogram().get_metric_with_label_values(&[actor_type]) {
        metric.observe(field_count as f64);
    }
}

/// `requestDuration.With(labels).Observe(duration.Seconds())`.
pub fn observe_request_duration(mutation: bool, route: &str, success: bool, seconds: f64) {
    let mutation = mutation.to_string();
    let success = success.to_string();
    if let Ok(metric) =
        request_duration_histogram().get_metric_with_label_values(&[&mutation, route, &success])
    {
        metric.observe(seconds);
    }
}

/// `strings.Contains(query, "mutation")` used for the `mutation` label.
pub fn is_mutation_query(query: &str) -> bool {
    query.contains("mutation")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_type_label_matches_the_baseline_precedence() {
        assert_eq!(actor_type_label(true, true, "browser"), "internal");
        assert_eq!(actor_type_label(false, true, "browser"), "anonymous");
        assert_eq!(actor_type_label(false, false, "browser"), "browser");
        assert_eq!(actor_type_label(false, false, ""), "unknown");
    }

    #[test]
    fn registers_families_with_baseline_metadata() {
        register();
        let text = sg_metrics::render_default();
        assert!(
            text.contains(
                "# HELP src_graphql_cost_distribution The maximum cost seen from a GraphQL query"
            ),
            "{text}"
        );
        assert!(
            text.contains("# TYPE src_graphql_cost_distribution histogram"),
            "{text}"
        );
        assert!(
            text.contains(
                "# HELP src_graphql_request_duration_seconds GraphQL request latencies in seconds."
            ),
            "{text}"
        );
        assert!(
            text.contains("# TYPE src_graphql_request_duration_seconds histogram"),
            "{text}"
        );
    }

    #[test]
    fn observes_cost_with_actor_type_label() {
        observe_cost("anonymous", 3);
        let text = sg_metrics::render_default();
        assert!(
            text.contains("src_graphql_cost_distribution_count{actor_type=\"anonymous\"} 1"),
            "{text}"
        );
    }

    #[test]
    fn early_returns_land_in_the_infinite_bucket() {
        observe_request_duration(false, "unknown", true, PRE_EXEC_DURATION_SECONDS);
        let text = sg_metrics::render_default();
        let suffix = "src_graphql_request_duration_seconds_bucket{mutation=\"false\",route=\"unknown\",success=\"true\",le=\"+Inf\"} 1";
        assert!(text.contains(suffix), "{text}");
    }
}
