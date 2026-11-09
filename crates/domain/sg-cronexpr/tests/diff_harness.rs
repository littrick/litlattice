#![forbid(unsafe_code)]

//! M9.8du differential harness (Rust side) for `sg-cronexpr`.
//!
//! `test_ws/p9-go/run_cronexpr_diff.sh` writes a corpus JSON (expressions,
//! Unix-second start instants, `next_count`) and runs the Go harness
//! `internal/codeintel/ranking/internal/background/coordinator`
//! (`TestCronExpressionHarness`, which calls `cronexpr.Parse`/`NextN`) and this
//! test, then diffs the canonical `cron ` JSON lines. Both sides print one
//! line per (expression, start) pair: parse errors as `{"error": ...}`, matches
//! as `[unix_seconds, nanoseconds]` pairs.
//!
//! Start instants are (seconds, nanoseconds) pairs so neither side depends on
//! timestamp formatting.

use serde::Deserialize;
use serde_json::{json, Value};
use sg_cronexpr::Expression;
use time::{Duration, OffsetDateTime};

#[derive(Deserialize)]
struct Corpus {
    expressions: Vec<String>,
    /// `(unix seconds, nanoseconds)`.
    froms: Vec<(i64, u32)>,
    next_count: usize,
}

fn print(value: Value) {
    println!(
        "cron {}",
        serde_json::to_string(&value).expect("serialize report line")
    );
}

fn at(seconds: i64, nanos: u32) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("valid unix timestamp")
        + Duration::nanoseconds(i64::from(nanos))
}

#[test]
fn cronexpr_diff_harness() {
    if std::env::var("SG_CRONEXPR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(path) = std::env::var("SG_CRONEXPR_DIFF_CORPUS") else {
        return;
    };
    let raw = std::fs::read_to_string(&path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    for expression in &corpus.expressions {
        match Expression::parse(expression) {
            Err(err) => print(json!({"expr": expression, "error": err.to_string()})),
            Ok(parsed) => {
                for (seconds, nanos) in &corpus.froms {
                    let mut cursor = at(*seconds, *nanos);
                    let mut nexts = Vec::new();
                    for _ in 0..corpus.next_count {
                        let Some(next) = parsed.next(cursor) else {
                            break;
                        };
                        nexts.push(json!([next.unix_timestamp(), next.nanosecond()]));
                        cursor = next;
                    }
                    print(json!({
                        "expr": expression,
                        "from": [seconds, nanos],
                        "nexts": nexts,
                    }));
                }
            }
        }
    }
}
