//! M9.8bv differential: `internal/insights/priority` over the corpus,
//! mirroring `internal/insights/priority/parity_harness_test.go`.
//! Gated by `SG_INSIGHTS_PRIORITY_CORPUS`; each corpus line is
//! `<op>\t<args...>` and produces one output line with a stable prefix.
//! Floats are printed as raw `f64` bits (lowercase hex) so formatting cannot
//! hide differences.

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_insights::priority::*;
use sg_insights_query::parse_query;

fn harness_handlers(spec: &str) -> Vec<CostHeuristic> {
    match spec {
        "default" => vec![query_cost, repositories_cost],
        "query" => vec![query_cost],
        "repos" => vec![repositories_cost],
        "none" => Vec::new(),
        other => panic!("unknown handlers {other}"),
    }
}

fn harness_sizes(spec: &str) -> Vec<i64> {
    if spec.is_empty() || spec == "-" {
        return Vec::new();
    }
    spec.split(',')
        .map(|part| part.parse::<i64>().expect("size"))
        .collect()
}

fn harness_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("valid RFC3339")
}

fn harness_bits(value: f64) -> String {
    format!("{:x}", value.to_bits())
}

#[test]
fn insights_priority_parity() {
    let Ok(path) = std::env::var("SG_INSIGHTS_PRIORITY_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        match op {
            "frominterval" => {
                let got = from_time_interval(harness_time(parts[1]), harness_time(parts[2]));
                println!("fi\t{}\t{}\t{}", parts[1], parts[2], got.0);
            }
            "arith" => {
                let p: i64 = parts[1].parse().expect("priority");
                let v: i64 = parts[2].parse().expect("val");
                let priority = Priority(p);
                println!(
                    "ar\t{}\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    priority.lower_by(v).0,
                    priority.raise_by(v).0,
                    priority.lower().0,
                    priority.raise().0
                );
            }
            "consts" => {
                println!(
                    "co\t{}\t{}\t{}\t{}\t{}\t{}",
                    Priority::CRITICAL.0,
                    Priority::HIGH.0,
                    Priority::MEDIUM.0,
                    Priority::LOW.0,
                    INDEXED,
                    UNINDEXED
                );
                println!(
                    "cf\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    harness_bits(LITERAL_COST),
                    harness_bits(REGEXP_COST),
                    harness_bits(STRUCTURAL_COST),
                    harness_bits(DIFF_MULTIPLIER),
                    harness_bits(COMMIT_MULTIPLIER),
                    harness_bits(AUTHOR_MULTIPLIER),
                    harness_bits(UNINDEXED_MULTIPLIER),
                    harness_bits(YES_MULTIPLIER),
                    harness_bits(ONLY_MULTIPLIER),
                    harness_bits(FILE_MULTIPLIER),
                    harness_bits(LANG_MULTIPLIER),
                    harness_bits(MANY_REPOSITORIES_MULTIPLIER),
                    harness_bits(MEGAREPO_MULTIPLIER),
                    harness_bits(GIGAREPO_MULTIPLIER)
                );
            }
            "thresholds" => {
                println!(
                    "th\t{}\t{}",
                    MEGAREPO_SIZE_THRESHOLD, GIGAREPO_SIZE_THRESHOLD
                );
            }
            "cost" => match parse_query(parts[2], "literal") {
                Err(err) => println!(
                    "cost\t{}\t{}\t{}\t{}\terror: {err}",
                    parts[1], parts[2], parts[3], parts[4]
                ),
                Ok(plan) => {
                    let repos: i64 = parts[3].parse().expect("repos");
                    let mut object = QueryObject::new(plan);
                    object.number_of_repositories = repos;
                    object.repository_byte_sizes = harness_sizes(parts[4]);
                    let analyzer = QueryAnalyzer::new(harness_handlers(parts[1]));
                    let returned = analyzer.cost(&mut object);
                    println!(
                        "cost\t{}\t{}\t{}\t{}\t{}\t{}",
                        parts[1],
                        parts[2],
                        parts[3],
                        parts[4],
                        harness_bits(object.cost),
                        harness_bits(returned)
                    );
                }
            },
            other => panic!("unknown op {other:?}"),
        }
    }
}
