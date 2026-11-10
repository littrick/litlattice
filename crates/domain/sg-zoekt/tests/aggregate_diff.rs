//! M9.8gq differential harness: `internal/search/backend/aggregate.go` and
//! zoekt's `SortFiles` against [`sg_zoekt::aggregate`] /
//! [`sg_zoekt::file_ranking`].
//!
//! Run with `SG_M9GQ_DIFF=1 M9GQ_CORPUS=<json> M9GQ_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON; the flush timer is injected as a mock so the harness is deterministic.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::aggregate::{new_flush_collect_sender_with_timer, ResultSender, Timer, TimerHandle};
use sg_zoekt::file_ranking::sort_files;
use sg_zoekt::types::{FileMatch, SearchOptions, SearchResult, Stats};

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    kind: String,
    #[serde(default)]
    endpoints: Vec<String>,
    #[serde(default)]
    opts: Option<Opts>,
    #[serde(default)]
    actions: Vec<Action>,
    #[serde(default)]
    files: Vec<CorpusFile>,
}

#[derive(Deserialize)]
struct Opts {
    #[serde(default)]
    max_doc_display_count: i64,
    #[serde(default)]
    max_size_bytes: i64,
    #[serde(default)]
    flush_wall_time_ms: u64,
}

#[derive(Deserialize)]
struct Action {
    action: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    result: Option<CorpusResult>,
}

#[derive(Deserialize)]
struct CorpusResult {
    #[serde(default)]
    files: Vec<CorpusFile>,
    #[serde(default)]
    stats: CorpusStats,
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

#[derive(Deserialize)]
struct CorpusFile {
    file_name: String,
    #[serde(default)]
    score: f64,
    #[serde(default)]
    debug: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    branches: Vec<String>,
    #[serde(default)]
    content: String,
}

impl CorpusFile {
    fn to_file_match(&self) -> FileMatch {
        FileMatch {
            score: self.score,
            debug: self.debug.clone(),
            file_name: self.file_name.as_bytes().to_vec(),
            repository: self.repository.clone(),
            language: self.language.clone(),
            version: self.version.clone(),
            branches: self.branches.clone(),
            content: self.content.as_bytes().to_vec(),
            ..FileMatch::default()
        }
    }
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
            flush_reason: sg_zoekt::types::FlushReason::from_proto(self.flush_reason),
        }
    }
}

impl CorpusResult {
    fn to_search_result(&self) -> SearchResult {
        SearchResult {
            stats: self.stats.to_stats(),
            files: self.files.iter().map(CorpusFile::to_file_match).collect(),
            ..SearchResult::default()
        }
    }
}

#[derive(Default)]
struct RecordingSender {
    sent: Mutex<Vec<SearchResult>>,
}

impl ResultSender for RecordingSender {
    fn send(&self, result: SearchResult) {
        lock(&self.sent).push(result);
    }
}

impl RecordingSender {
    fn take(&self) -> Vec<SearchResult> {
        std::mem::take(&mut *lock(&self.sent))
    }
}

type MockCallback = Option<Box<dyn FnOnce() + Send>>;

#[derive(Default)]
struct MockTimerState {
    callbacks: Mutex<Vec<MockCallback>>,
}

#[derive(Default, Clone)]
struct MockTimer {
    state: Arc<MockTimerState>,
}

impl MockTimer {
    fn fire(&self) {
        let callbacks = std::mem::take(&mut *lock(&self.state.callbacks));
        for callback in callbacks.into_iter().flatten() {
            callback();
        }
    }
}

struct MockTimerHandle {
    state: Arc<MockTimerState>,
    index: usize,
}

impl TimerHandle for MockTimerHandle {
    fn cancel(self: Box<Self>) {
        if let Some(slot) = lock(&self.state.callbacks).get_mut(self.index) {
            *slot = None;
        }
    }
}

impl Timer for MockTimer {
    fn schedule(
        &self,
        _delay: Duration,
        callback: Box<dyn FnOnce() + Send>,
    ) -> Box<dyn TimerHandle> {
        let mut callbacks = lock(&self.state.callbacks);
        callbacks.push(Some(callback));
        let index = callbacks.len() - 1;
        Box::new(MockTimerHandle {
            state: Arc::clone(&self.state),
            index,
        })
    }
}

#[test]
fn aggregate_diff() {
    if std::env::var("SG_M9GQ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GQ_CORPUS").expect("M9GQ_CORPUS must be set when SG_M9GQ_DIFF=1");
    let out_path = std::env::var("M9GQ_OUT").expect("M9GQ_OUT must be set when SG_M9GQ_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases
        .iter()
        .map(|case| {
            if case.kind == "sort" {
                run_sort(case)
            } else {
                run_flush(case)
            }
        })
        .collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_sort(case: &CorpusCase) -> Value {
    let mut files: Vec<FileMatch> = case.files.iter().map(CorpusFile::to_file_match).collect();
    sort_files(&mut files);
    let sorted: Vec<String> = files
        .iter()
        .map(|file| String::from_utf8_lossy(&file.file_name).into_owned())
        .collect();
    json!({
        "name": case.name,
        "kind": case.kind,
        "sorted": sorted,
    })
}

fn run_flush(case: &CorpusCase) -> Value {
    let recorder = Arc::new(RecordingSender::default());
    let timer = MockTimer::default();

    let options = case.opts.as_ref().map(|opts| SearchOptions {
        max_doc_display_count: opts.max_doc_display_count,
        flush_wall_time: Duration::from_millis(opts.flush_wall_time_ms),
        ..SearchOptions::default()
    });
    let max_size_bytes = case.opts.as_ref().map_or(-1, |opts| opts.max_size_bytes);

    let sender = new_flush_collect_sender_with_timer(
        options,
        &case.endpoints,
        max_size_bytes,
        recorder.clone(),
        Arc::new(timer.clone()),
    );

    for action in &case.actions {
        match action.action.as_str() {
            "send" => {
                let result = action
                    .result
                    .as_ref()
                    .expect("send action needs a result")
                    .to_search_result();
                sender.send(&action.endpoint, result);
            }
            "send_done" => sender.send_done(&action.endpoint),
            "flush" => sender.flush(),
            "wait_timer" => timer.fire(),
            other => panic!("unknown action {other}"),
        }
    }

    let sent: Vec<Value> = recorder
        .take()
        .iter()
        .map(|result| {
            let files: Vec<Value> = result
                .files
                .iter()
                .map(|file| {
                    json!({
                        "file_name": String::from_utf8_lossy(&file.file_name).into_owned(),
                        "score": format_score(file.score),
                        "debug_score": file.debug,
                    })
                })
                .collect();
            json!({
                "files": files,
                "stats": normalize_stats(&result.stats),
                "flush_reason": result.stats.flush_reason.as_str(),
            })
        })
        .collect();

    json!({
        "name": case.name,
        "kind": case.kind,
        "sent": sent,
    })
}

fn normalize_stats(stats: &Stats) -> Value {
    json!({
        "content_bytes_loaded": stats.content_bytes_loaded,
        "index_bytes_loaded": stats.index_bytes_loaded,
        "crashes": stats.crashes,
        "duration_ms": stats.duration.as_millis() as i64,
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

/// Canonical score text matching Go's `strconv.FormatFloat(v, 'g', -1, 64)`.
fn format_score(score: f64) -> String {
    if score.is_finite() && score.fract() == 0.0 {
        format!("{}", score as i64)
    } else {
        format!("{score}")
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
