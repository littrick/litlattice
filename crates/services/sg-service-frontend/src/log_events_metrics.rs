#![forbid(unsafe_code)]

//! The three Prometheus families observed by the deprecated `logEvent`
//! mutations (`cmd/frontend/graphqlbackend/user_usage_stats.go:288-352`).
//!
//! These events are **not** written to `event_logs`; they only update
//! histograms. Names, help texts, label names/order and bucket boundaries are
//! part of the `/metrics` contract and must match the baseline byte for byte.
//! `promauto` registers the families at package init, so [`register`] runs at
//! frontend startup to expose them (empty) before the first request.

use std::sync::OnceLock;

use serde_json::Value;
use sg_metrics::prometheus::{HistogramOpts, HistogramVec};

/// `trace.UserLatencyBuckets` (`internal/trace/buckets.go:7`).
pub const USER_LATENCY_BUCKETS: [f64; 7] = [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0];

/// `prometheus.LinearBuckets(1, 1, 10)`.
pub const RANKING_BUCKETS: [f64; 10] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

static CODE_LOAD: OnceLock<HistogramVec> = OnceLock::new();
static FIRST_RESULT: OnceLock<HistogramVec> = OnceLock::new();
static RANKING_CLICKED: OnceLock<HistogramVec> = OnceLock::new();

/// Register the three families in the process registry (idempotent).
pub fn register() {
    code_load();
    first_result();
    ranking_clicked();
}

fn code_load() -> &'static HistogramVec {
    CODE_LOAD.get_or_init(|| {
        let opts = HistogramOpts::new(
            "src_search_latency_frontend_code_load_seconds",
            "Milliseconds the webapp frontend spends waiting for search result code snippets to load.",
        )
        .buckets(USER_LATENCY_BUCKETS.to_vec());
        let histogram = HistogramVec::new(opts, &[]).expect("valid code-load histogram");
        let _ = sg_metrics::registry().register(Box::new(histogram.clone()));
        histogram
    })
}

fn first_result() -> &'static HistogramVec {
    FIRST_RESULT.get_or_init(|| {
        let opts = HistogramOpts::new(
            "src_search_latency_frontend_first_result_seconds",
            "Milliseconds the webapp frontend spends waiting for the first search result to load.",
        )
        .buckets(USER_LATENCY_BUCKETS.to_vec());
        let histogram = HistogramVec::new(opts, &["type"]).expect("valid first-result histogram");
        let _ = sg_metrics::registry().register(Box::new(histogram.clone()));
        histogram
    })
}

fn ranking_clicked() -> &'static HistogramVec {
    RANKING_CLICKED.get_or_init(|| {
        let opts = HistogramOpts::new(
            "src_search_ranking_result_clicked",
            "the index of the search result which was clicked on by the user",
        )
        .buckets(RANKING_BUCKETS.to_vec());
        let histogram = HistogramVec::new(opts, &["type", "resultsLength", "ranked"])
            .expect("valid ranking histogram");
        let _ = sg_metrics::registry().register(Box::new(histogram.clone()));
        histogram
    })
}

/// `exportPrometheusSearchLatencies`. The caller has already validated the
/// payload as JSON (`decode`); a typed unmarshal failure is logged by the
/// baseline and ignored, so `None` here means "nothing observed".
pub fn observe_search_latencies(event: &str, payload: &Option<Value>) {
    let Some(duration_ms) = duration_ms(payload) else {
        return;
    };
    if event == "search.latencies.frontend.code-load" {
        code_load()
            .with_label_values(&[] as &[&str])
            .observe(duration_ms / 1000.0);
    }
    if event.starts_with("search.latencies.frontend.") && event.ends_with(".first-result") {
        let search_type = event
            .trim_start_matches("search.latencies.frontend.")
            .trim_end_matches(".first-result");
        first_result()
            .with_label_values(&[search_type])
            .observe(duration_ms / 1000.0);
    }
}

/// `exportPrometheusSearchRanking`; the same logged-and-ignored typed failure.
pub fn observe_search_ranking(payload: &Option<Value>) {
    let Some(ranking) = ranking(payload) else {
        return;
    };
    let results_length = if ranking.results_length <= 3 {
        "<=3"
    } else {
        ">3"
    };
    let ranked = if ranking.ranked { "true" } else { "false" };
    ranking_clicked()
        .with_label_values(&[&ranking.event_type, results_length, ranked])
        .observe(ranking.index);
}

/// `json.Unmarshal(payload, &struct{ DurationMS float64 })`.
///
/// A nil payload (argument absent) fails like `json.Unmarshal(nil)`; `null`
/// leaves the struct untouched; objects accept numbers and `null` for the
/// field and ignore unknown members; every other shape is a type error.
fn duration_ms(payload: &Option<Value>) -> Option<f64> {
    match payload {
        None => None,
        Some(Value::Null) => Some(0.0),
        Some(Value::Object(map)) => match map.get("durationMs") {
            None | Some(Value::Null) => Some(0.0),
            Some(Value::Number(number)) => number.as_f64(),
            Some(_) => None,
        },
        Some(_) => None,
    }
}

struct Ranking {
    index: f64,
    event_type: String,
    results_length: i64,
    ranked: bool,
}

/// `json.Unmarshal(payload, &struct{ Index float64; Type string; ResultsLength int; Ranked bool })`.
fn ranking(payload: &Option<Value>) -> Option<Ranking> {
    let Value::Object(map) = payload.as_ref()? else {
        return match payload {
            Some(Value::Null) => Some(Ranking {
                index: 0.0,
                event_type: String::new(),
                results_length: 0,
                ranked: false,
            }),
            _ => None,
        };
    };
    let index = match map.get("index") {
        None | Some(Value::Null) => 0.0,
        Some(Value::Number(number)) => number.as_f64()?,
        Some(_) => return None,
    };
    let event_type = match map.get("type") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(_) => return None,
    };
    let results_length = match map.get("resultsLength") {
        None | Some(Value::Null) => 0,
        Some(Value::Number(number)) => number.as_i64()?,
        Some(_) => return None,
    };
    let ranked = match map.get("ranked") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return None,
    };
    Some(Ranking {
        index,
        event_type,
        results_length,
        ranked,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parsing_matches_go_json() {
        assert_eq!(duration_ms(&None), None);
        assert_eq!(duration_ms(&Some(Value::Null)), Some(0.0));
        assert_eq!(duration_ms(&Some(serde_json::json!({}))), Some(0.0));
        assert_eq!(
            duration_ms(&Some(serde_json::json!({"durationMs": 1500}))),
            Some(1500.0)
        );
        assert_eq!(
            duration_ms(&Some(serde_json::json!({"durationMs": 1.5}))),
            Some(1.5)
        );
        assert_eq!(
            duration_ms(&Some(serde_json::json!({"durationMs": null}))),
            Some(0.0)
        );
        assert_eq!(
            duration_ms(&Some(serde_json::json!({"durationMs": "5"}))),
            None
        );
        assert_eq!(duration_ms(&Some(serde_json::json!([1]))), None);
    }

    #[test]
    fn ranking_parsing_matches_go_json() {
        let Some(parsed) = ranking(&Some(serde_json::json!({
            "index": 4,
            "type": "file",
            "resultsLength": 7,
            "ranked": true
        }))) else {
            panic!("ranking");
        };
        assert_eq!(parsed.index, 4.0);
        assert_eq!(parsed.event_type, "file");
        assert_eq!(parsed.results_length, 7);
        assert!(parsed.ranked);

        let Some(zero) = ranking(&Some(Value::Null)) else {
            panic!("null ranking");
        };
        assert_eq!(zero.results_length, 0);
        assert!(ranking(&None).is_none());
        assert!(ranking(&Some(serde_json::json!({"resultsLength": 3.5}))).is_none());
    }

    #[test]
    fn families_register_once() {
        register();
        register();
        // A `HistogramVec` only appears in a registry scrape once a child
        // metric exists, so observe one sample per family.
        observe_search_latencies(
            "search.latencies.frontend.code-load",
            &Some(serde_json::json!({"durationMs": 10})),
        );
        observe_search_latencies(
            "search.latencies.frontend.keyword.first-result",
            &Some(serde_json::json!({"durationMs": 10})),
        );
        observe_search_ranking(&Some(serde_json::json!({
            "index": 1,
            "type": "file",
            "resultsLength": 2,
            "ranked": false
        })));
        let names: Vec<String> = sg_metrics::registry()
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_owned())
            .collect();
        for name in [
            "src_search_latency_frontend_code_load_seconds",
            "src_search_latency_frontend_first_result_seconds",
            "src_search_ranking_result_clicked",
        ] {
            assert!(names.iter().any(|existing| existing == name), "{name}");
        }
    }
}
