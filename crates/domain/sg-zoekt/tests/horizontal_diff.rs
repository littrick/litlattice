//! M9.8gt differential harness:
//! `internal/search/backend/horizontal.go` helpers
//! (`AggregateStreamSearch`, `dedupper.Dedup`, `crashEvent`,
//! `zoektRolloutReason`) against [`sg_zoekt::horizontal`].
//!
//! Run with `SG_M9GT_DIFF=1 M9GT_CORPUS=<json> M9GT_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON. The aggregate duration is compared indirectly: the raw duration is
//! omitted (Go uses the real clock, Rust a [`ManualClock`]) and
//! `duration_equals_event_sum` confirms it is not the sum of event durations.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::cached::ManualClock;
use sg_zoekt::horizontal::{
    aggregate_stream_search_with_clock, crash_event, zoekt_rollout_reason, Deduper, RolloutError,
};
use sg_zoekt::query::Query;
use sg_zoekt::types::{FileMatch, FlushReason, SearchResult, Stats};

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    kind: String,
    #[serde(default)]
    actions: Vec<DedupAction>,
    #[serde(default)]
    events: Vec<CorpusEvent>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    input: String,
}

#[derive(Deserialize)]
struct DedupAction {
    endpoint: String,
    #[serde(default)]
    files: Vec<CorpusFile>,
}

#[derive(Deserialize)]
struct CorpusEvent {
    #[serde(default)]
    files: Vec<CorpusFile>,
    #[serde(default)]
    stats: CorpusStats,
}

#[derive(Deserialize)]
struct CorpusFile {
    #[serde(default)]
    repository: String,
    file_name: String,
}

impl CorpusFile {
    fn to_file_match(&self) -> FileMatch {
        FileMatch {
            repository: self.repository.clone(),
            file_name: self.file_name.as_bytes().to_vec(),
            ..FileMatch::default()
        }
    }
}

#[derive(Deserialize, Default)]
struct CorpusStats {
    #[serde(default)]
    content_bytes_loaded: i64,
    #[serde(default)]
    index_bytes_loaded: i64,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    duration_ms: i64,
    #[serde(default)]
    file_count: i64,
    #[serde(default)]
    shard_files_considered: i64,
    #[serde(default)]
    files_considered: i64,
    #[serde(default)]
    files_loaded: i64,
    #[serde(default)]
    files_skipped: i64,
    #[serde(default)]
    shards_scanned: i64,
    #[serde(default)]
    shards_skipped: i64,
    #[serde(default)]
    shards_skipped_filter: i64,
    #[serde(default)]
    match_count: i64,
    #[serde(default)]
    ngram_matches: i64,
    #[serde(default)]
    ngram_lookups: i64,
    #[serde(default)]
    wait_ms: i64,
    #[serde(default)]
    match_tree_construction_ms: i64,
    #[serde(default)]
    match_tree_search_ms: i64,
    #[serde(default)]
    regexps_considered: i64,
    #[serde(default)]
    flush_reason: i32,
}

impl CorpusStats {
    fn to_stats(&self) -> Stats {
        Stats {
            content_bytes_loaded: self.content_bytes_loaded,
            index_bytes_loaded: self.index_bytes_loaded,
            crashes: self.crashes,
            duration: Duration::from_millis(self.duration_ms as u64),
            file_count: self.file_count,
            shard_files_considered: self.shard_files_considered,
            files_considered: self.files_considered,
            files_loaded: self.files_loaded,
            files_skipped: self.files_skipped,
            shards_scanned: self.shards_scanned,
            shards_skipped: self.shards_skipped,
            shards_skipped_filter: self.shards_skipped_filter,
            match_count: self.match_count,
            ngram_matches: self.ngram_matches,
            ngram_lookups: self.ngram_lookups,
            wait: Duration::from_millis(self.wait_ms as u64),
            match_tree_construction: Duration::from_millis(self.match_tree_construction_ms as u64),
            match_tree_search: Duration::from_millis(self.match_tree_search_ms as u64),
            regexps_considered: self.regexps_considered,
            flush_reason: FlushReason::from_proto(self.flush_reason),
        }
    }
}

#[test]
fn horizontal_diff() {
    if std::env::var("SG_M9GT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GT_CORPUS").expect("M9GT_CORPUS must be set when SG_M9GT_DIFF=1");
    let out_path = std::env::var("M9GT_OUT").expect("M9GT_OUT must be set when SG_M9GT_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "dedup" => run_dedup(case),
            "aggregate" => run_aggregate(case),
            "crash" => run_crash(case),
            "rollout" => run_rollout(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_dedup(case: &CorpusCase) -> Value {
    let mut deduper = Deduper::new();
    let mut results = Vec::with_capacity(case.actions.len());
    for action in &case.actions {
        let mut files: Vec<FileMatch> =
            action.files.iter().map(CorpusFile::to_file_match).collect();
        deduper.dedup(&action.endpoint, &mut files);
        let names: Vec<String> = files
            .iter()
            .map(|file| String::from_utf8_lossy(&file.file_name).into_owned())
            .collect();
        results.push(json!({
            "endpoint": action.endpoint,
            "files": names,
        }));
    }

    let table: BTreeMap<&str, &str> = deduper
        .entries()
        .iter()
        .map(|(repo, endpoint)| (repo.as_str(), endpoint.as_str()))
        .collect();

    json!({
        "name": case.name,
        "kind": case.kind,
        "results": results,
        "table": table,
    })
}

fn run_aggregate(case: &CorpusCase) -> Value {
    let clock = ManualClock::new();
    let advance = clock.clone();
    let error = case.error.clone();
    let events: Vec<SearchResult> = case
        .events
        .iter()
        .map(|event| SearchResult {
            files: event.files.iter().map(CorpusFile::to_file_match).collect(),
            stats: event.stats.to_stats(),
            ..SearchResult::default()
        })
        .collect();
    let event_duration_sum = events.iter().fold(Duration::ZERO, |sum, event| {
        sum.saturating_add(event.stats.duration)
    });

    let result: Result<SearchResult, String> = aggregate_stream_search_with_clock(
        &clock,
        &(),
        &Query::Const(true),
        None,
        move |_ctx, _query, _opts, sender| {
            for event in events {
                sender.send(event);
            }
            // The Rust clock is injected; the Go oracle uses the real
            // `time.Since`. Only the non-accumulation property is compared.
            advance.advance(Duration::from_millis(3));
            match error {
                Some(message) => Err(message),
                None => Ok(()),
            }
        },
    );

    match result {
        Ok(result) => {
            let files: Vec<String> = result
                .files
                .iter()
                .map(|file| String::from_utf8_lossy(&file.file_name).into_owned())
                .collect();
            json!({
                "name": case.name,
                "kind": case.kind,
                "error": "",
                "files": files,
                "stats": normalize_stats(&result.stats),
                "duration_equals_event_sum": result.stats.duration == event_duration_sum,
            })
        }
        Err(message) => json!({
            "name": case.name,
            "kind": case.kind,
            "error": message,
            "files": [],
            "stats": {},
            "duration_equals_event_sum": false,
        }),
    }
}

fn run_crash(case: &CorpusCase) -> Value {
    let event = crash_event();
    json!({
        "name": case.name,
        "kind": case.kind,
        "crashes": event.stats.crashes,
        "files_len": event.files.len(),
    })
}

fn run_rollout(case: &CorpusCase) -> Value {
    let error = match case.input.as_str() {
        "dns_not_found" => RolloutError::DnsNotFound,
        "dial_timeout" => RolloutError::DialTimeout,
        "dial_refused" => RolloutError::DialRefused,
        "read_failed" => RolloutError::ReadFailed,
        "other_plain" | "other_write_op" => RolloutError::Other,
        other => panic!("unknown rollout input {other}"),
    };
    json!({
        "name": case.name,
        "kind": case.kind,
        "reason": zoekt_rollout_reason(&error),
    })
}

fn normalize_stats(stats: &Stats) -> Value {
    json!({
        "content_bytes_loaded": stats.content_bytes_loaded,
        "index_bytes_loaded": stats.index_bytes_loaded,
        "crashes": stats.crashes,
        "file_count": stats.file_count,
        "shard_files_considered": stats.shard_files_considered,
        "files_considered": stats.files_considered,
        "files_loaded": stats.files_loaded,
        "files_skipped": stats.files_skipped,
        "shards_scanned": stats.shards_scanned,
        "shards_skipped": stats.shards_skipped,
        "shards_skipped_filter": stats.shards_skipped_filter,
        "match_count": stats.match_count,
        "ngram_matches": stats.ngram_matches,
        "ngram_lookups": stats.ngram_lookups,
        "wait_ms": stats.wait.as_millis() as i64,
        "match_tree_construction_ms": stats.match_tree_construction.as_millis() as i64,
        "match_tree_search_ms": stats.match_tree_search.as_millis() as i64,
        "regexps_considered": stats.regexps_considered,
        "flush_reason": stats.flush_reason.as_str(),
    })
}
