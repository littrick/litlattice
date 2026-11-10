//! M9.8fv differential of the GraphQL Prometheus metrics against
//! `cmd/frontend/internal/httpapi/zz_m9fv_metrics_oracle_test.go`.
//!
//! Both sides observe the same corpus into their process registry and dump
//! the two families as normalised JSON: metadata, label-sorted samples,
//! finite bucket bounds (Go's shortest `%g` text), cumulative counts and —
//! for the cost histogram only — the sum. Durations are pinned to 2.5s so
//! the bucket assignment is deterministic; the duration sum is wall-clock
//! dependent and omitted.
//!
//! Env:
//!
//! * `SG_M9FV_DIFF=1`
//! * `M9FV_CORPUS`, `M9FV_OUT`

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use sg_metrics::prometheus::proto::MetricFamily;
use sg_service_frontend::graphql_metrics::{
    actor_type_label, is_mutation_query, observe_cost, observe_request_duration, register,
};

const COST_FAMILY: &str = "src_graphql_cost_distribution";
const DURATION_FAMILY: &str = "src_graphql_request_duration_seconds";

#[derive(Deserialize)]
struct CostCase {
    internal: bool,
    anonymous: bool,
    source: String,
    value: i64,
}

#[derive(Deserialize)]
struct DurationCase {
    query: String,
    route: String,
    success: bool,
}

#[derive(Deserialize)]
struct Corpus {
    cost: Vec<CostCase>,
    duration: Vec<DurationCase>,
}

#[derive(Serialize)]
struct Bucket {
    #[serde(rename = "upperBound")]
    upper_bound: String,
    cumulative: u64,
}

#[derive(Serialize)]
struct Sample {
    labels: BTreeMap<String, String>,
    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    sum: Option<i64>,
    buckets: Vec<Bucket>,
}

#[derive(Serialize)]
struct Family {
    name: String,
    help: String,
    #[serde(rename = "type")]
    kind: String,
    samples: Vec<Sample>,
}

fn label_key(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value};"))
        .collect()
}

/// Go's `strconv.FormatFloat(v, 'g', -1, 64)` for the finite bounds we emit.
fn format_bound(value: f64) -> String {
    format!("{value}")
}

fn dump(families: &[MetricFamily]) -> Vec<Family> {
    let mut out = Vec::new();
    for family in families {
        let name = family.get_name();
        if name != COST_FAMILY && name != DURATION_FAMILY {
            continue;
        }
        let with_sum = name == COST_FAMILY;
        let mut samples = Vec::new();
        for metric in family.get_metric() {
            let labels: BTreeMap<String, String> = metric
                .get_label()
                .iter()
                .map(|label| (label.get_name().to_owned(), label.get_value().to_owned()))
                .collect();
            let histogram = metric.get_histogram();
            let mut buckets: Vec<Bucket> = histogram
                .get_bucket()
                .iter()
                .filter(|bucket| bucket.get_upper_bound().is_finite())
                .map(|bucket| Bucket {
                    upper_bound: format_bound(bucket.get_upper_bound()),
                    cumulative: bucket.get_cumulative_count(),
                })
                .collect();
            buckets.sort_by(|left, right| {
                let left: f64 = left.upper_bound.parse().unwrap_or(f64::INFINITY);
                let right: f64 = right.upper_bound.parse().unwrap_or(f64::INFINITY);
                left.partial_cmp(&right)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            samples.push(Sample {
                labels,
                count: histogram.get_sample_count(),
                sum: with_sum.then(|| histogram.get_sample_sum() as i64),
                buckets,
            });
        }
        samples.sort_by_key(|sample| label_key(&sample.labels));
        out.push(Family {
            name: name.to_owned(),
            help: family.get_help().to_owned(),
            kind: "HISTOGRAM".to_owned(),
            samples,
        });
    }
    out.sort_by(|left, right| left.name.cmp(&right.name));
    out
}

#[test]
fn graphql_metrics_harness() {
    if std::env::var("SG_M9FV_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9FV_CORPUS").expect("M9FV_CORPUS");
    let out_path = std::env::var("M9FV_OUT").expect("M9FV_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    register();
    for case in &corpus.cost {
        observe_cost(
            &actor_type_label(case.internal, case.anonymous, &case.source),
            case.value,
        );
    }
    for case in &corpus.duration {
        observe_request_duration(
            is_mutation_query(&case.query),
            &case.route,
            case.success,
            2.5,
        );
    }

    let encoded = serde_json::to_vec(&dump(&sg_metrics::registry().gather())).expect("encode");
    std::fs::write(Path::new(&out_path), &encoded).expect("write metrics");
    println!("RUST metrics len={}", encoded.len());
}
