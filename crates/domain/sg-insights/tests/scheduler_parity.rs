//! M9.8bx differential: the pure `internal/insights/scheduler` helpers,
//! mirroring `internal/insights/scheduler/parity_harness_test.go`.
//! Gated by `SG_INSIGHTS_SCHEDULER_CORPUS`; each corpus line is
//! `<op>\t<args...>` and produces one output line with a stable prefix.

use sg_insights::scheduler::{
    calculate_error_threshold, parse_query, translate_incomplete_reasons, BackfillState,
    SearchTimeoutError, SeriesBackfill,
};
use sg_search_query::basic::plan_to_q;

/// A wrapped error to exercise Go's `errors.Is` unwrapping.
struct Wrapped {
    source: SearchTimeoutError,
}

impl std::fmt::Debug for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Wrapped")
    }
}

impl std::fmt::Display for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("searchHandler")
    }
}

impl std::error::Error for Wrapped {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
struct Boom;

impl std::fmt::Display for Boom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("boom")
    }
}

impl std::error::Error for Boom {}

#[test]
fn insights_scheduler_parity() {
    let Ok(path) = std::env::var("SG_INSIGHTS_SCHEDULER_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "parsequery" => match parse_query(parts[2], parts[1] == "true") {
                Ok(plan) => {
                    let q = plan_to_q(&plan);
                    println!(
                        "pq\t{}\t{}\t{}\t{}\t{}",
                        parts[1],
                        parts[2],
                        plan.len(),
                        sg_search_query::query_to_string(&q),
                        sg_search_query::printer::string_human(&q)
                    );
                }
                Err(err) => println!("pq\t{}\t{}\terror: {err}", parts[1], parts[2]),
            },
            "threshold" => {
                let percent: f64 = parts[1].parse().expect("percent");
                let floor: i64 = parts[2].parse().expect("floor");
                let cardinality: i64 = parts[3].parse().expect("cardinality");
                println!(
                    "th\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    parts[3],
                    calculate_error_threshold(percent, floor, cardinality)
                );
            }
            "terminal" => {
                let is_terminal = match BackfillState::from_str_value(parts[1]) {
                    Some(state) => SeriesBackfill {
                        id: 1,
                        series_id: 2,
                        repo_iterator_id: 0,
                        estimated_cost: 0.0,
                        state,
                    }
                    .is_terminal_state(),
                    None => false,
                };
                println!("tm\t{}\t{}", parts[1], is_terminal);
            }
            "reason" => {
                let wrapped = Wrapped {
                    source: SearchTimeoutError,
                };
                let boom = Boom;
                let err: &(dyn std::error::Error + 'static) = if parts[1] == "timeout" {
                    &wrapped
                } else {
                    &boom
                };
                println!(
                    "rs\t{}\t{}",
                    parts[1],
                    translate_incomplete_reasons(err).as_str()
                );
            }
            other => panic!("unknown op {other:?}"),
        }
    }
}
