// M9.8ch differential harness for `sg_insights::compression`.
//
// Prints canonical `cp ` lines for the corpus in
// test_ws/p9-go/data/insights_compression_corpus.txt; the Go baseline prints
// the same lines from internal/insights/compression/parity_harness_test.go.
// Scenes mirror the baseline's compression tests: scripted commit fetchers,
// an out-of-order sample set, the noop filter and ToRecording.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_insights::compression::{
    BackfillPlan, Commit, CommitFetcher, DataFrameFilter, GitserverFilter, NoopFilter,
    QueryExecution,
};
use time::format_description::well_known::Rfc3339;
use time::macros::datetime;
use time::OffsetDateTime;

const RECORDING_TIME: OffsetDateTime = datetime!(2020-01-01 0:00 UTC);

struct FakeFetcher {
    hashes: HashMap<OffsetDateTime, String>,
    errors: HashMap<OffsetDateTime, String>,
}

#[async_trait]
impl CommitFetcher for FakeFetcher {
    async fn recent_commits(
        &self,
        _repo_name: &str,
        target: OffsetDateTime,
        _revision: &str,
    ) -> Result<Vec<Commit>, String> {
        match self.hashes.get(&target) {
            Some(hash) => Ok(vec![Commit { id: hash.clone() }]),
            None => match self.errors.get(&target) {
                Some(error) => Err(error.clone()),
                None => Ok(Vec::new()),
            },
        }
    }
}

fn month_times() -> Vec<OffsetDateTime> {
    vec![
        datetime!(2021-01-01 0:00 UTC),
        datetime!(2021-02-01 0:00 UTC),
        datetime!(2021-03-01 0:00 UTC),
        datetime!(2021-04-01 0:00 UTC),
    ]
}

fn build_fake_fetcher(values: &[Result<&str, &str>]) -> FakeFetcher {
    let mut hashes = HashMap::new();
    let mut errors = HashMap::new();
    for (time, value) in month_times().into_iter().zip(values.iter()) {
        match value {
            Ok(hash) => {
                hashes.insert(time, (*hash).to_owned());
            }
            Err(error) => {
                errors.insert(time, (*error).to_owned());
            }
        }
    }
    FakeFetcher { hashes, errors }
}

fn fetcher_for_scene(scene: &str) -> FakeFetcher {
    match scene {
        "distinct" | "noop" => build_fake_fetcher(&[Ok("1"), Ok("2"), Ok("3"), Ok("4")]),
        "inner" | "shuffled" => build_fake_fetcher(&[Ok("1"), Ok("1"), Ok("1"), Ok("2")]),
        "all" => build_fake_fetcher(&[Ok("1"), Ok("1"), Ok("1"), Ok("1")]),
        "error" => build_fake_fetcher(&[Ok("1"), Ok("2"), Err("asdf"), Ok("4")]),
        "empty" => build_fake_fetcher(&[]),
        _ => build_fake_fetcher(&[]),
    }
}

fn parse_times(spec: &str, fallback: Vec<OffsetDateTime>) -> Vec<OffsetDateTime> {
    if spec == "-" || spec.is_empty() {
        return fallback;
    }
    spec.split(',')
        .map(|raw| {
            OffsetDateTime::parse(raw, &Rfc3339).unwrap_or_else(|err| panic!("parse {raw}: {err}"))
        })
        .collect()
}

fn canonical_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).expect("format time")
}

fn print_plan(label: &str, plan: &BackfillPlan) {
    println!(
        "cp {label} plan record_count={} executions={}",
        plan.record_count,
        plan.executions.len()
    );
    for (index, execution) in plan.executions.iter().enumerate() {
        let revision = if execution.revision.is_empty() {
            "-".to_owned()
        } else {
            execution.revision.clone()
        };
        let shared = if execution.shared_recordings.is_empty() {
            "-".to_owned()
        } else {
            execution
                .shared_recordings
                .iter()
                .map(|time| canonical_time(*time))
                .collect::<Vec<_>>()
                .join(",")
        };
        println!(
            "cp {label} execution i={index} revision={revision} recording={} shared={shared}",
            canonical_time(execution.recording_time)
        );
    }
}

fn print_recordings(label: &str, execution: &QueryExecution) {
    for (index, recording) in execution
        .to_recording("series1", "repoName1", 1, 5.0)
        .iter()
        .enumerate()
    {
        println!(
            "cp {label} recording i={index} series_id={} time={} value={} repo_name={} repo_id={} persist_mode={}",
            recording.series_id,
            canonical_time(recording.time),
            recording.value,
            recording.repo_name.as_deref().unwrap_or("-"),
            recording.repo_id.unwrap_or(-1),
            recording.persist_mode
        );
    }
}

#[tokio::test]
async fn compression_parity() {
    if std::env::var("SG_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("set SG_HARNESS_ON=1 to run the compression parity harness");
        return;
    }
    let corpus = std::env::var("SG_INSIGHTS_COMPRESSION_CORPUS")
        .expect("SG_INSIGHTS_COMPRESSION_CORPUS is required");
    let contents = std::fs::read_to_string(&corpus)
        .unwrap_or_else(|err| panic!("read corpus {corpus}: {err}"));

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split('\t');
        let label = parts.next().expect("label");
        let scene = parts.next().expect("scene");
        let spec = parts.next().unwrap_or("-");

        match scene {
            "recording" | "recording-no-dependents" => {
                let mut execution = QueryExecution {
                    revision: "asdf1234".to_owned(),
                    recording_time: RECORDING_TIME,
                    shared_recordings: Vec::new(),
                };
                if scene == "recording" {
                    execution
                        .shared_recordings
                        .push(datetime!(2020-01-02 0:00 UTC));
                }
                print_recordings(label, &execution);
            }
            "noop" => {
                let fetcher = fetcher_for_scene(scene);
                let fallback = month_times()
                    .into_iter()
                    .filter(|time| {
                        fetcher.hashes.contains_key(time) || fetcher.errors.contains_key(time)
                    })
                    .collect();
                let times = parse_times(spec, fallback);
                let plan = NoopFilter.filter(times, "myrepo").await;
                print_plan(label, &plan);
            }
            _ => {
                let fetcher = fetcher_for_scene(scene);
                let fallback = month_times()
                    .into_iter()
                    .filter(|time| {
                        fetcher.hashes.contains_key(time) || fetcher.errors.contains_key(time)
                    })
                    .collect();
                let times = parse_times(spec, fallback);
                let plan = GitserverFilter::new(Arc::new(fetcher))
                    .filter(times, "myrepo")
                    .await;
                print_plan(label, &plan);
            }
        }
    }
}
