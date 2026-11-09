//! M9.8bw differential: `internal/insights/timeseries` over the corpus,
//! mirroring `internal/insights/timeseries/parity_harness_test.go`.
//! Gated by `SG_INSIGHTS_TIMESERIES_CORPUS`; each corpus line is
//! `<op>\t<args...>` and produces one output line with a stable prefix.
//! Timestamps are canonicalized to UTC and printed with nanosecond precision
//! so that neither formatting nor offsets can hide differences.

use sg_insights::timeseries::{
    build_sample_times, default_interval, make_recordings_from_times, TimeInterval,
};
use sg_insights::types::IntervalUnit;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339)
        .unwrap_or_else(|err| panic!("parse time {value:?}: {err}"))
}

fn format_time(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
        value.nanosecond()
    )
}

fn interval(unit: &str, value: &str) -> TimeInterval {
    TimeInterval {
        unit: IntervalUnit::from_str_value(unit),
        value: value.parse().expect("interval value"),
    }
}

#[test]
fn insights_timeseries_parity() {
    let Ok(path) = std::env::var("SG_INSIGHTS_TIMESERIES_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "step" => {
                let interval = interval(parts[1], parts[2]);
                let start = parse_time(parts[4]);
                let got = if parts[3] == "fwd" {
                    interval.step_forwards(start)
                } else {
                    interval.step_backwards(start)
                };
                println!(
                    "step\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    parts[3],
                    parts[4],
                    format_time(got)
                );
            }
            "valid" => {
                let interval = interval(parts[1], parts[2]);
                println!("valid\t{}\t{}\t{}", parts[1], parts[2], interval.is_valid());
            }
            "build" => {
                let count: i64 = parts[1].parse().expect("count");
                let interval = interval(parts[2], parts[3]);
                let now = parse_time(parts[4]);
                let times = build_sample_times(count, &interval, now);
                let formatted: Vec<String> = times.into_iter().map(format_time).collect();
                println!(
                    "build\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    parts[3],
                    parts[4],
                    formatted.join(",")
                );
            }
            "recordings" => {
                let snapshot: bool = parts[1].parse().expect("snapshot");
                let times: Vec<OffsetDateTime> = if parts[2].is_empty() || parts[2] == "-" {
                    Vec::new()
                } else {
                    parts[2].split(',').map(parse_time).collect()
                };
                let recordings = make_recordings_from_times(&times, snapshot);
                let formatted: Vec<String> = recordings
                    .into_iter()
                    .map(|r| format!("{}|{}", format_time(r.timestamp), r.snapshot))
                    .collect();
                println!("rec\t{}\t{}", parts[1], formatted.join(","));
            }
            "default" => {
                let interval = default_interval();
                println!("default\t{}\t{}", interval.unit.as_str(), interval.value);
            }
            other => panic!("unknown op {other:?}"),
        }
    }
}
