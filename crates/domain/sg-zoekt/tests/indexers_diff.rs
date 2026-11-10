//! M9.8gu differential harness: `internal/search/backend/indexers.go` and the
//! cached dialer from `internal/search/backend/zoekt.go` against
//! [`sg_zoekt::indexers`] / [`sg_zoekt::dialer`].
//!
//! Run with `SG_M9GU_DIFF=1 M9GU_CORPUS=<json> M9GU_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON: one array, in corpus category order, with per-case objects.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::dialer::{CachedStreamerCloser, CachedZoektDialer, CloseableStreamer, ZoektDialer};
use sg_zoekt::indexers::{
    endpoints_string, find_endpoint, EndpointMap, Indexers, IndexersError, MinimalRepo, ReposMap,
};
use sg_zoekt::streamer::Streamer;
use sg_zoekt::types::{ListOptions, ListResponse, SearchChunk, SearchOptions};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    find_endpoint_cases: Vec<FindEndpointCase>,
    #[serde(default)]
    endpoints_string_cases: Vec<EndpointsStringCase>,
    #[serde(default)]
    repos_subset_cases: Vec<ReposSubsetCase>,
    #[serde(default)]
    dialer_cases: Vec<DialerCase>,
}

#[derive(Deserialize)]
struct FindEndpointCase {
    name: String,
    hostname: String,
    #[serde(default)]
    endpoints: Vec<String>,
}

#[derive(Deserialize)]
struct EndpointsStringCase {
    name: String,
    #[serde(default)]
    endpoints: Vec<String>,
}

#[derive(Deserialize)]
struct MinimalRepoCase {
    id: i32,
    name: String,
}

#[derive(Deserialize)]
struct ReposSubsetCase {
    name: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    hostname: String,
    #[serde(default)]
    endpoints: Vec<String>,
    #[serde(default)]
    assigned: BTreeMap<String, String>,
    #[serde(default)]
    indexed: BTreeMap<String, Vec<i32>>,
    #[serde(default)]
    repos: Vec<MinimalRepoCase>,
    #[serde(default)]
    endpoints_error: String,
    #[serde(default)]
    get_error: String,
}

#[derive(Deserialize)]
struct DialOp {
    op: String,
    #[serde(default)]
    endpoint: String,
}

#[derive(Deserialize)]
struct DialerCase {
    name: String,
    kind: String,
    #[serde(default)]
    ops: Vec<DialOp>,
    #[serde(default)]
    compare: Vec<usize>,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    threads: usize,
    #[serde(default)]
    dial_delay_ms: u64,
}

#[test]
fn indexers_diff() {
    if std::env::var("SG_M9GU_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GU_CORPUS").expect("M9GU_CORPUS must be set when SG_M9GU_DIFF=1");
    let out_path = std::env::var("M9GU_OUT").expect("M9GU_OUT must be set when SG_M9GU_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut entries = Vec::new();
    for case in &corpus.find_endpoint_cases {
        entries.push(run_find_endpoint(case));
    }
    for case in &corpus.endpoints_string_cases {
        entries.push(run_endpoints_string(case));
    }
    for case in &corpus.repos_subset_cases {
        entries.push(run_repos_subset(case));
    }
    for case in &corpus.dialer_cases {
        match case.kind.as_str() {
            "sequence" => entries.push(run_dialer_sequence(case)),
            "concurrent" => entries.push(run_dialer_concurrent(case)),
            other => panic!("unknown dialer kind {other}"),
        }
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_find_endpoint(case: &FindEndpointCase) -> Value {
    match find_endpoint(&case.endpoints, &case.hostname) {
        Ok(endpoint) => json!({
            "name": case.name,
            "kind": "find_endpoint",
            "endpoint": endpoint,
            "error": "",
        }),
        Err(error) => json!({
            "name": case.name,
            "kind": "find_endpoint",
            "endpoint": "",
            "error": error.to_string(),
        }),
    }
}

fn run_endpoints_string(case: &EndpointsStringCase) -> Value {
    json!({
        "name": case.name,
        "kind": "endpoints_string",
        "string": endpoints_string(&case.endpoints),
    })
}

struct FakeEndpointMap {
    endpoints: Vec<String>,
    assignments: BTreeMap<String, String>,
    endpoints_error: Option<String>,
    get_error: Option<String>,
}

impl EndpointMap for FakeEndpointMap {
    fn endpoints(&self) -> Result<Vec<String>, IndexersError> {
        match &self.endpoints_error {
            Some(message) => Err(IndexersError::new(message.clone())),
            None => Ok(self.endpoints.clone()),
        }
    }

    fn get(&self, key: &str) -> Result<String, IndexersError> {
        match &self.get_error {
            Some(message) => Err(IndexersError::new(message.clone())),
            None => Ok(self.assignments.get(key).cloned().unwrap_or_default()),
        }
    }
}

fn indexed_map(indexed: &BTreeMap<String, Vec<i32>>, endpoint: &str) -> ReposMap {
    indexed
        .get(endpoint)
        .map(|ids| ids.iter().map(|id| *id as u32).collect())
        .unwrap_or_default()
}

fn run_repos_subset(case: &ReposSubsetCase) -> Value {
    let indexed_calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&indexed_calls);
    let indexed_snapshot = case.indexed.clone();

    let map: Option<Box<dyn EndpointMap>> = if case.enabled {
        Some(Box::new(FakeEndpointMap {
            endpoints: case.endpoints.clone(),
            assignments: case.assigned.clone(),
            endpoints_error: (!case.endpoints_error.is_empty())
                .then(|| case.endpoints_error.clone()),
            get_error: (!case.get_error.is_empty()).then(|| case.get_error.clone()),
        }))
    } else {
        None
    };

    let indexers = Indexers {
        map,
        indexed: Box::new(move |endpoint: &str| {
            recorded.lock().unwrap().push(endpoint.to_owned());
            indexed_map(&indexed_snapshot, endpoint)
        }),
    };

    let repos: Vec<MinimalRepo> = case
        .repos
        .iter()
        .map(|repo| MinimalRepo {
            id: repo.id,
            name: repo.name.clone(),
        })
        .collect();
    let host_indexed = indexed_map(&case.indexed, &case.hostname);

    let (error, returned) = match indexers.repos_subset(&case.hostname, &host_indexed, &repos) {
        Ok(subset) => (String::new(), subset),
        Err(error) => (error.to_string(), Vec::new()),
    };

    let names: Vec<String> = returned.into_iter().map(|repo| repo.name).collect();
    let calls: BTreeSet<String> = indexed_calls.lock().unwrap().iter().cloned().collect();

    json!({
        "name": case.name,
        "kind": "repos_subset",
        "error": error,
        "repos": names,
        "indexed_calls": calls,
    })
}

#[derive(Default)]
struct FakeDialer {
    calls: Mutex<BTreeMap<String, usize>>,
    closed: AtomicUsize,
    delay: Duration,
}

struct FakeStreamer {
    dialer: Arc<FakeDialer>,
}

/// Orphan rules: the integration test cannot implement the foreign
/// `ZoektDialer` trait directly for `Arc<FakeDialer>`, so wrap it.
struct FakeDialerHandle(Arc<FakeDialer>);

impl Streamer for FakeStreamer {
    type Error = Infallible;

    async fn list(
        &self,
        _query: &sg_zoekt::query::Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, Self::Error> {
        Ok(ListResponse::default())
    }

    async fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &SearchOptions,
        _sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl CloseableStreamer for FakeStreamer {
    fn close(&self) {
        self.dialer.closed.fetch_add(1, Ordering::SeqCst);
    }
}

impl ZoektDialer for FakeDialerHandle {
    type Streamer = FakeStreamer;

    fn dial(&self, endpoint: &str) -> FakeStreamer {
        let fake = &self.0;
        *fake
            .calls
            .lock()
            .unwrap()
            .entry(endpoint.to_owned())
            .or_insert(0) += 1;
        if !fake.delay.is_zero() {
            std::thread::sleep(fake.delay);
        }
        FakeStreamer {
            dialer: Arc::clone(fake),
        }
    }
}

fn snapshot(fake: &FakeDialer) -> Value {
    let calls: BTreeMap<String, usize> = fake.calls.lock().unwrap().clone();
    json!(calls)
}

fn dialer_output(
    case: &DialerCase,
    fake: &FakeDialer,
    pointer_equal: bool,
    cache_size: usize,
) -> Value {
    json!({
        "name": case.name,
        "kind": "dialer",
        "calls": snapshot(fake),
        "pointer_equal": pointer_equal,
        "closed": fake.closed.load(Ordering::SeqCst),
        "cache_size": cache_size,
    })
}

fn fake_dialer(delay_ms: u64) -> Arc<FakeDialer> {
    Arc::new(FakeDialer {
        delay: Duration::from_millis(delay_ms),
        ..FakeDialer::default()
    })
}

fn run_dialer_sequence(case: &DialerCase) -> Value {
    let fake = fake_dialer(case.dial_delay_ms);
    let dialer = CachedZoektDialer::new(FakeDialerHandle(Arc::clone(&fake)));

    let mut results: Vec<CachedStreamerCloser<FakeDialerHandle>> = Vec::new();
    for op in &case.ops {
        match op.op.as_str() {
            "dial" => results.push(dialer.dial(&op.endpoint)),
            "close" => results.last().expect("close with no dial").close(),
            other => panic!("unknown op {other}"),
        }
    }

    let pointer_equal =
        case.compare.len() == 2 && results[case.compare[0]].ptr_eq(&results[case.compare[1]]);
    dialer_output(case, &fake, pointer_equal, dialer.cache_len())
}

fn run_dialer_concurrent(case: &DialerCase) -> Value {
    let fake = fake_dialer(case.dial_delay_ms);
    let dialer = Arc::new(CachedZoektDialer::new(FakeDialerHandle(Arc::clone(&fake))));
    let barrier = Arc::new(Barrier::new(case.threads));
    let mut handles = Vec::with_capacity(case.threads);

    for _ in 0..case.threads {
        let dialer = Arc::clone(&dialer);
        let barrier = Arc::clone(&barrier);
        let endpoint = case.endpoint.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            dialer.dial(&endpoint)
        }));
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("thread panicked"))
        .collect();
    let pointer_equal = results.iter().all(|streamer| streamer.ptr_eq(&results[0]));
    dialer_output(case, &fake, pointer_equal, dialer.cache_len())
}
