//! Differential harness (M9.17c) for `internal/ratelimit` and
//! `extsvc.GetDefaultRateLimit`: normalised URLs, default limits, the
//! `golang.org/x/time/rate` token bucket at injected times, and `Monitor`
//! (`Update`/`Get`/`RecommendedWaitForBackgroundOp`/`calcRateLimitWaitTime`).
//!
//! Replays `test_ws/p9-go/data/ratelimit_core/corpus.json` and writes one JSON
//! line per case to `M917C_RATELIMIT_OUT`; the Go oracle
//! (`internal/ratelimit/zz_m917c_ratelimit_oracle_test.go`) writes the same
//! lines. `run_ratelimit_diff.sh` compares them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use sg_extsvc::ratelimit::{
    get_default_rate_limit, normalise_url, InMemoryLimiter, Monitor, Variant,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    normalise_url_cases: Vec<NormaliseCase>,
    #[serde(default)]
    default_limit_cases: Vec<LimitCase>,
    #[serde(default)]
    limiter_cases: Vec<LimiterCase>,
    #[serde(default)]
    monitor_cases: Vec<MonitorCase>,
}

#[derive(Deserialize)]
struct NormaliseCase {
    input: String,
}

#[derive(Deserialize)]
struct LimitCase {
    kind: String,
}

#[derive(Deserialize)]
struct LimiterCase {
    name: String,
    limit: f64,
    burst: usize,
    steps: Vec<LimiterStep>,
}

#[derive(Deserialize)]
struct LimiterStep {
    now_ns: i64,
    n: usize,
}

#[derive(Deserialize)]
struct MonitorCase {
    name: String,
    header_prefix: String,
    steps: Vec<MonitorStep>,
}

#[derive(Deserialize)]
struct MonitorStep {
    op: String,
    #[serde(default)]
    now_ns: i64,
    #[serde(default)]
    cost: i64,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
}

fn limit_value(limit: f64) -> Value {
    if limit.is_infinite() && limit > 0.0 {
        json!("inf")
    } else {
        json!(limit)
    }
}

#[test]
fn ratelimit_diff() {
    if std::env::var("SG_M917C_RATELIMIT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M917C_RATELIMIT_CORPUS").expect("M917C_RATELIMIT_CORPUS");
    let out_path = std::env::var("M917C_RATELIMIT_OUT").expect("M917C_RATELIMIT_OUT");
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let mut lines = Vec::new();
    for case in &corpus.normalise_url_cases {
        lines.push(
            serde_json::to_string(&json!({
                "kind": "normalise_url",
                "input": case.input,
                "output": normalise_url(&case.input),
            }))
            .expect("encode"),
        );
    }
    for case in &corpus.default_limit_cases {
        let variant = Variant::from_kind(&case.kind).unwrap_or(Variant::Other);
        lines.push(
            serde_json::to_string(&json!({
                "kind": "default_limit",
                "name": case.kind,
                "limit": limit_value(get_default_rate_limit(variant)),
            }))
            .expect("encode"),
        );
    }
    for case in &corpus.limiter_cases {
        let limiter = InMemoryLimiter::new(case.limit, case.burst);
        let mut delays = Vec::new();
        for step in &case.steps {
            let wait = limiter
                .reserve_n_at(Duration::from_nanos(step.now_ns.max(0) as u64), step.n)
                .unwrap_or_else(|err| panic!("case {}: {err}", case.name));
            delays.push(wait.as_nanos() as i64);
        }
        lines.push(
            serde_json::to_string(&json!({
                "kind": "limiter",
                "name": case.name,
                "delays_ns": delays,
            }))
            .expect("encode"),
        );
    }
    for case in &corpus.monitor_cases {
        let now = Arc::new(AtomicU64::new(0));
        let clock_now = Arc::clone(&now);
        let mut monitor = Monitor::default().with_clock(move || {
            Duration::from_nanos(clock_now.load(Ordering::SeqCst))
        });
        monitor.header_prefix = case.header_prefix.clone();
        let mut results = Vec::new();
        for step in &case.steps {
            now.store(step.now_ns.max(0) as u64, Ordering::SeqCst);
            match step.op.as_str() {
                "update" => {
                    let headers: Vec<(String, String)> = step
                        .headers
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    monitor.update(&headers);
                    results.push(Value::Null);
                }
                "get" => {
                    let (remaining, reset, retry, known) = monitor.get();
                    results.push(json!({
                        "op": "get",
                        "remaining": remaining,
                        "reset_ns": reset,
                        "retry_ns": retry,
                        "known": known,
                    }));
                }
                "recommended" => {
                    results.push(json!({
                        "op": "recommended",
                        "cost": step.cost,
                        "wait_ns": monitor.recommended_wait_for_background_op(step.cost),
                    }));
                }
                "calc" => {
                    results.push(json!({
                        "op": "calc",
                        "cost": step.cost,
                        "wait_ns": monitor.calc_rate_limit_wait_time(step.cost),
                    }));
                }
                other => panic!("case {}: unknown monitor op {other:?}", case.name),
            }
        }
        lines.push(
            serde_json::to_string(&json!({
                "kind": "monitor",
                "name": case.name,
                "steps": results,
            }))
            .expect("encode"),
        );
    }

    let mut output = lines.join("\n");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");
}
