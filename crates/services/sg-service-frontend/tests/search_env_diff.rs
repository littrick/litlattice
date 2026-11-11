//! M9.8ir differential harness: `internal/search/env.go` against
//! `sg_service_frontend::search_env`.
//!
//! Covers drain parsing, `endpointMapDrain` endpoints/gets, the `Indexed()`
//! wrapper stack (aggregation and endpoint-set refresh), `ListAllIndexed`, and
//! the TTL cache. The Go side is split across
//! `internal/search/zz_m9ir_oracle_test.go` (environment half) and
//! `internal/search/backend/zz_m9ir_oracle_cache_test.go` (cache half); the
//! script concatenates their outputs in the same order this harness emits.
//!
//! Env:
//!
//! * `SG_M9IR_DIFF=1` enable the harness
//! * `M9IR_CORPUS` corpus JSON
//! * `M9IR_OUT` output JSON

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_service_frontend::search_env::{
    list_all_indexed, parse_drain_servers, EndpointMapDrain, IndexedError, IndexedJobStreamer,
    IndexedStreamer,
};
use sg_zoekt::cached::{CachedStreamer, ManualClock};
use sg_zoekt::horizontal::RolloutError;
use sg_zoekt::horizontal_searcher::{Client as HorizontalClient, ClientError};
use sg_zoekt::indexers::EndpointMap;
use sg_zoekt::metered::ErrorClassifier;
use sg_zoekt::query::Query;
use sg_zoekt::streamer::Streamer;
use sg_zoekt::types::{
    FileMatch, ListOptions, ListResponse, MinimalRepoListEntry, RepoListEntry, RepoListField,
    Repository, SearchChunk, SearchOptions, SearchResult, Stats,
};

// ------------------------------------------------------------------ corpus

#[derive(Debug, Default, Deserialize)]
struct Corpus {
    #[serde(default)]
    aggregate_cases: Vec<AggregateCase>,
    #[serde(default)]
    cache_cases: Vec<CacheCase>,
    #[serde(default)]
    drain_cases: Vec<DrainCase>,
    #[serde(default)]
    endpoint_map_cases: Vec<EndpointMapCase>,
    #[serde(default)]
    list_all_cases: Vec<ListAllCase>,
    #[serde(default)]
    sync_cases: Vec<SyncCase>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct Behavior {
    #[serde(default)]
    events: Vec<Event>,
    #[serde(default)]
    repos: Vec<String>,
    #[serde(default)]
    repos_map: Vec<i32>,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    error: String,
    #[serde(default)]
    rollout: String,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct Event {
    #[serde(default)]
    files: Vec<SpecFile>,
    #[serde(default)]
    crashes: i64,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct SpecFile {
    #[serde(default)]
    repository: String,
    #[serde(default)]
    file_name: String,
}

#[derive(Debug, Default, Deserialize)]
struct AggregateCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    endpoints: Vec<String>,
    #[serde(default)]
    behaviors: BTreeMap<String, Behavior>,
}

#[derive(Debug, Default, Deserialize)]
struct DrainCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    raw: String,
}

#[derive(Debug, Default, Deserialize)]
struct EndpointMapCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    conf: Vec<String>,
    #[serde(default)]
    drain: Vec<String>,
    #[serde(default)]
    lookup_keys: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ListAllCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    endpoints: Vec<String>,
    #[serde(default)]
    behaviors: BTreeMap<String, Behavior>,
}

#[derive(Debug, Default, Deserialize)]
struct SyncCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    steps: Vec<SyncStep>,
}

#[derive(Debug, Default, Deserialize)]
struct SyncStep {
    #[serde(default)]
    op: String,
    #[serde(default)]
    endpoints: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CacheCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    ttl_ms: i64,
    #[serde(default)]
    steps: Vec<CacheStep>,
}

#[derive(Debug, Default, Deserialize)]
struct CacheStep {
    #[serde(default)]
    op: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    response: Option<CacheResponse>,
    #[serde(default)]
    ms: i64,
    #[serde(default)]
    record_calls: Option<bool>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct CacheResponse {
    #[serde(default)]
    repos: Vec<String>,
    #[serde(default)]
    repos_map: Vec<i32>,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    error: String,
}

// ------------------------------------------------------------- fake result client

#[derive(Debug, Clone)]
struct FakeError {
    message: String,
    rollout: RolloutError,
}

impl FakeError {
    fn from_behavior(behavior: &Behavior) -> Option<Self> {
        if !behavior.rollout.is_empty() {
            let rollout = match behavior.rollout.as_str() {
                "dns_not_found" => RolloutError::DnsNotFound,
                "dial_timeout" => RolloutError::DialTimeout,
                "dial_refused" => RolloutError::DialRefused,
                "read_failed" => RolloutError::ReadFailed,
                other => {
                    return Some(Self {
                        message: other.to_owned(),
                        rollout: RolloutError::Other,
                    })
                }
            };
            return Some(Self {
                message: "rollout".to_owned(),
                rollout,
            });
        }
        if !behavior.error.is_empty() {
            return Some(Self {
                message: behavior.error.clone(),
                rollout: RolloutError::Other,
            });
        }
        None
    }
}

impl std::fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FakeError {}

impl ClientError for FakeError {
    fn rollout_error(&self) -> RolloutError {
        self.rollout
    }
}

impl ErrorClassifier for FakeError {}

#[derive(Debug, Default)]
struct FakeState {
    dials: Mutex<HashMap<String, usize>>,
    closed: AtomicUsize,
}

impl FakeState {
    fn record_dial(&self, endpoint: &str) {
        *self
            .dials
            .lock()
            .expect("dials lock")
            .entry(endpoint.to_owned())
            .or_insert(0) += 1;
    }

    fn dials(&self) -> BTreeMap<String, usize> {
        self.dials
            .lock()
            .expect("dials lock")
            .iter()
            .map(|(endpoint, count)| (endpoint.clone(), *count))
            .collect()
    }

    fn closed(&self) -> usize {
        self.closed.load(Ordering::SeqCst)
    }
}

struct FakeClient {
    behavior: Behavior,
    state: Arc<FakeState>,
}

impl FakeClient {
    fn new(behavior: Behavior, state: Arc<FakeState>) -> Self {
        Self { behavior, state }
    }
}

impl HorizontalClient for FakeClient {
    type Error = FakeError;

    fn list<'a>(
        &'a self,
        _query: &'a Query,
        _opts: Option<ListOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResponse, FakeError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(error) = FakeError::from_behavior(&self.behavior) {
                return Err(error);
            }
            Ok(ListResponse {
                repos: self
                    .behavior
                    .repos
                    .iter()
                    .map(|name| RepoListEntry {
                        repository: Repository {
                            name: name.clone(),
                            ..Repository::default()
                        },
                        index_time_unix: 0,
                    })
                    .collect(),
                repos_map: self
                    .behavior
                    .repos_map
                    .iter()
                    .map(|id| (*id as u32, MinimalRepoListEntry::default()))
                    .collect(),
                crashes: self.behavior.crashes,
            })
        })
    }

    fn stream_search<'a>(
        &'a self,
        _query: &'a Query,
        _options: Option<&'a SearchOptions>,
        sender: &'a mut (dyn FnMut(SearchResult) + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), FakeError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(error) = FakeError::from_behavior(&self.behavior) {
                return Err(error);
            }
            for event in &self.behavior.events {
                let files = event
                    .files
                    .iter()
                    .map(|file| FileMatch {
                        repository: file.repository.clone(),
                        file_name: file.file_name.as_bytes().to_vec(),
                        ..FileMatch::default()
                    })
                    .collect();
                sender(SearchResult {
                    files,
                    stats: Stats {
                        crashes: event.crashes,
                        ..Stats::default()
                    },
                    ..SearchResult::default()
                });
            }
            Ok(())
        })
    }

    fn close(&self) {
        self.state.closed.fetch_add(1, Ordering::SeqCst);
    }
}

fn behavior_map(behaviors: &BTreeMap<String, Behavior>) -> BTreeMap<String, Behavior> {
    behaviors.clone()
}

fn indexed_over(
    endpoints: Arc<Mutex<Vec<String>>>,
    dial_state: Arc<FakeState>,
    behaviors: BTreeMap<String, Behavior>,
) -> IndexedStreamer<FakeClient> {
    let dial_behaviors = behaviors;
    let dial_state_for_dial = Arc::clone(&dial_state);
    sg_service_frontend::search_env::compose_indexed(
        move || Ok(endpoints.lock().expect("endpoints lock").clone()),
        move |endpoint: &str| {
            dial_state_for_dial.record_dial(endpoint);
            let behavior = dial_behaviors.get(endpoint).cloned().unwrap_or_default();
            FakeClient::new(behavior, Arc::clone(&dial_state_for_dial))
        },
        Duration::from_secs(10),
    )
}

// --------------------------------------------------------------- env cases

async fn run_aggregate(case: &AggregateCase) -> Value {
    let state = Arc::new(FakeState::default());
    let searcher = indexed_over(
        Arc::new(Mutex::new(case.endpoints.clone())),
        Arc::clone(&state),
        behavior_map(&case.behaviors),
    );

    let mut files: Vec<String> = Vec::new();
    let mut crashes = 0i64;
    // Chunk boundaries are flush-timer dependent in both implementations, so
    // only the concatenated results are compared (they are sorted below).
    let mut sender = |chunk: SearchChunk| {
        crashes += chunk.stats.crashes;
        for file in &chunk.files {
            files.push(format!(
                "{}:{}",
                file.repository,
                String::from_utf8_lossy(&file.file_name)
            ));
        }
    };
    let error = match Streamer::stream_search(
        &searcher,
        &Query::Const(true),
        &SearchOptions::default(),
        &mut sender,
    )
    .await
    {
        Ok(()) => String::new(),
        Err(error) => error.to_string(),
    };
    files.sort();

    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("kind", Value::String("aggregate".to_owned())),
        (
            "files",
            Value::Array(files.into_iter().map(Value::String).collect()),
        ),
        ("crashes", Value::from(crashes)),
        ("error", Value::String(error)),
    ])
}

fn run_drain(case: &DrainCase) -> Value {
    let parsed: Vec<Value> = parse_drain_servers(&case.raw)
        .into_iter()
        .map(Value::String)
        .collect();
    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("kind", Value::String("drain_parse".to_owned())),
        ("parsed", Value::Array(parsed)),
    ])
}

fn run_endpoint_map(case: &EndpointMapCase) -> Value {
    let conf = case.conf.clone();
    let map = EndpointMapDrain::new(move || conf.clone(), case.drain.clone());
    let mut endpoints: Vec<String> = map.endpoints().expect("endpoints");
    endpoints.sort();
    let gets: Vec<Value> = case
        .lookup_keys
        .iter()
        .map(|key| {
            let resolved = map.get(key);
            let (ok, endpoint) = match resolved {
                Ok(endpoint) => (true, endpoint),
                Err(_) => (false, String::new()),
            };
            obj(vec![
                ("key", Value::String(key.clone())),
                ("ok", Value::Bool(ok)),
                ("endpoint", Value::String(endpoint)),
            ])
        })
        .collect();
    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("kind", Value::String("endpoint_map".to_owned())),
        (
            "endpoints",
            Value::Array(endpoints.into_iter().map(Value::String).collect()),
        ),
        ("gets", Value::Array(gets)),
        ("error", Value::String(String::new())),
    ])
}

async fn run_list_all(case: &ListAllCase) -> Value {
    let state = Arc::new(FakeState::default());
    let searcher = indexed_over(
        Arc::new(Mutex::new(case.endpoints.clone())),
        Arc::clone(&state),
        behavior_map(&case.behaviors),
    );

    match list_all_indexed(&searcher).await {
        Ok(all) => obj(vec![
            ("name", Value::String(case.name.clone())),
            ("kind", Value::String("list_all".to_owned())),
            ("error", Value::String(String::new())),
            (
                "repos_map",
                Value::Array(all.repos_map.keys().map(|id| Value::from(*id)).collect()),
            ),
            ("crashes", Value::from(all.crashes)),
            ("repos", Value::from(all.stats.repos)),
        ]),
        Err(error) => obj(vec![
            ("name", Value::String(case.name.clone())),
            ("kind", Value::String("list_all".to_owned())),
            ("error", Value::String(error.to_string())),
            ("repos_map", Value::Array(Vec::new())),
            ("crashes", Value::from(0)),
            ("repos", Value::from(0)),
        ]),
    }
}

async fn run_sync(case: &SyncCase) -> Value {
    let endpoints = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(FakeState::default());
    let behaviors: BTreeMap<String, Behavior> = [
        (
            "a".to_owned(),
            Behavior {
                events: vec![Event {
                    files: vec![SpecFile {
                        repository: "a".to_owned(),
                        file_name: "a.go".to_owned(),
                    }],
                    crashes: 0,
                }],
                ..Behavior::default()
            },
        ),
        (
            "b".to_owned(),
            Behavior {
                events: vec![Event {
                    files: vec![SpecFile {
                        repository: "b".to_owned(),
                        file_name: "b.go".to_owned(),
                    }],
                    crashes: 0,
                }],
                ..Behavior::default()
            },
        ),
    ]
    .into_iter()
    .collect();
    let searcher = indexed_over(Arc::clone(&endpoints), Arc::clone(&state), behaviors);

    let mut steps = Vec::new();
    for step in &case.steps {
        if step.op == "set" {
            *endpoints.lock().expect("endpoints lock") = step.endpoints.clone();
            steps.push(obj(vec![("op", Value::String("set".to_owned()))]));
            continue;
        }

        let mut files: Vec<String> = Vec::new();
        let mut crashes = 0i64;
        let mut sender = |chunk: SearchChunk| {
            crashes += chunk.stats.crashes;
            for file in &chunk.files {
                files.push(format!(
                    "{}:{}",
                    file.repository,
                    String::from_utf8_lossy(&file.file_name)
                ));
            }
        };
        let error = match Streamer::stream_search(
            &searcher,
            &Query::Const(true),
            &SearchOptions::default(),
            &mut sender,
        )
        .await
        {
            Ok(()) => String::new(),
            Err(error) => error.to_string(),
        };
        files.sort();
        let dials: Map<String, Value> = state
            .dials()
            .into_iter()
            .map(|(endpoint, count)| (endpoint, Value::from(count)))
            .collect();
        steps.push(obj(vec![
            ("op", Value::String("search".to_owned())),
            ("dials", Value::Object(dials)),
            ("closed", Value::from(state.closed())),
            (
                "files",
                Value::Array(files.into_iter().map(Value::String).collect()),
            ),
            ("crashes", Value::from(crashes)),
            ("error", Value::String(error)),
        ]));
    }

    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("kind", Value::String("sync".to_owned())),
        ("steps", Value::Array(steps)),
    ])
}

// ------------------------------------------------------------- cache cases

struct FakeCacheStreamer {
    responses: Mutex<VecDeque<CacheResponse>>,
    calls: Arc<AtomicUsize>,
}

impl FakeCacheStreamer {
    fn new(responses: VecDeque<CacheResponse>, calls: Arc<AtomicUsize>) -> Self {
        Self {
            responses: Mutex::new(responses),
            calls,
        }
    }
}

impl Streamer for FakeCacheStreamer {
    type Error = FakeError;

    async fn list(
        &self,
        _query: &Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, FakeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self
            .responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .unwrap_or_default();
        if !response.error.is_empty() {
            return Err(FakeError {
                message: response.error,
                rollout: RolloutError::Other,
            });
        }
        Ok(ListResponse {
            repos: response
                .repos
                .iter()
                .map(|name| RepoListEntry {
                    repository: Repository {
                        name: name.clone(),
                        ..Repository::default()
                    },
                    index_time_unix: 0,
                })
                .collect(),
            repos_map: response
                .repos_map
                .iter()
                .map(|id| (*id as u32, MinimalRepoListEntry::default()))
                .collect(),
            crashes: response.crashes,
        })
    }

    async fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), FakeError> {
        Ok(())
    }
}

async fn run_cache(case: &CacheCase) -> Value {
    let responses: VecDeque<CacheResponse> = case
        .steps
        .iter()
        .filter(|step| step.op == "list")
        .filter_map(|step| step.response.clone())
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let fake = FakeCacheStreamer::new(responses, Arc::clone(&calls));
    let clock = ManualClock::new();
    let searcher = CachedStreamer::with_clock_and_jitter(
        Duration::from_millis(case.ttl_ms.max(0) as u64),
        fake,
        clock.clone(),
        |_| 0,
    );

    let mut steps = Vec::new();
    for step in &case.steps {
        match step.op.as_str() {
            "advance" => {
                clock.advance(Duration::from_millis(step.ms.max(0) as u64));
                steps.push(obj(vec![("op", Value::String("advance".to_owned()))]));
            }
            "snapshot" => {
                let entries: Vec<Value> = searcher
                    .debug_cache_entries()
                    .await
                    .into_iter()
                    .map(|entry| {
                        let key = match entry.key {
                            0 => "repos",
                            2 => "repos_map",
                            _ => "unknown",
                        };
                        obj(vec![
                            ("key", Value::String(key.to_owned())),
                            ("ttl_ms", Value::from(entry.ttl.as_millis() as i64)),
                            ("has_error", Value::Bool(entry.has_error)),
                            ("crashes", Value::from(entry.crashes)),
                        ])
                    })
                    .collect();
                steps.push(obj(vec![
                    ("op", Value::String("snapshot".to_owned())),
                    ("entries", Value::Array(entries)),
                ]));
            }
            "list" => {
                let field = if step.field == "repos_map" {
                    RepoListField::ReposMap
                } else {
                    RepoListField::Repos
                };
                let result = searcher
                    .list(&Query::Const(true), Some(ListOptions { field }))
                    .await;
                // Let a stale async refresh finish before the next step.
                searcher.wait_for_refresh().await;
                let (ok, repos, repos_map, crashes, error) = match result {
                    Ok(list) => {
                        let mut repos: Vec<String> = list
                            .repos
                            .iter()
                            .map(|entry| entry.repository.name.clone())
                            .collect();
                        repos.sort();
                        (
                            true,
                            repos,
                            list.repos_map.keys().map(|id| *id as i64).collect(),
                            list.crashes,
                            String::new(),
                        )
                    }
                    Err(error) => (false, Vec::new(), Vec::new(), 0, error.to_string()),
                };
                let mut fields = vec![
                    ("op", Value::String("list".to_owned())),
                    ("field", Value::String(step.field.clone())),
                    ("ok", Value::Bool(ok)),
                    (
                        "repos",
                        Value::Array(repos.into_iter().map(Value::String).collect()),
                    ),
                    (
                        "repos_map",
                        Value::Array(repos_map.into_iter().map(Value::from).collect()),
                    ),
                    ("crashes", Value::from(crashes)),
                    ("error", Value::String(error)),
                ];
                if step.record_calls.unwrap_or(true) {
                    let count = calls.load(Ordering::SeqCst);
                    fields.push(("calls", Value::from(count)));
                }
                steps.push(obj(fields));
            }
            other => {
                steps.push(obj(vec![("op", Value::String(other.to_owned()))]));
            }
        }
    }

    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("kind", Value::String("cache".to_owned())),
        ("steps", Value::Array(steps)),
    ])
}

// ---------------------------------------------------------------- utilities

fn obj(fields: Vec<(&str, Value)>) -> Value {
    let mut object = Map::new();
    for (key, value) in fields {
        object.insert(key.to_owned(), value);
    }
    Value::Object(object)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_env_diff() {
    if std::env::var("SG_M9IR_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9IR_DIFF not set; skipping");
        return;
    }
    // Absent endpoints must keep the existing not-ported fallback: the
    // assembly refuses to build a zoekt bundle and the caller keeps
    // `TreeJobRunner::unconfigured()` (the `"search execution is not ported
    // yet"` path covered by `search_runtime`'s unit test).
    assert!(matches!(
        IndexedJobStreamer::new(&[], Duration::from_secs(600)),
        Err(IndexedError::MissingZoekt)
    ));

    let corpus_path = std::env::var("M9IR_CORPUS").expect("M9IR_CORPUS must be set");
    let out_path = std::env::var("M9IR_OUT").expect("M9IR_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let corpus: Corpus =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let mut output: Vec<Value> = Vec::new();
    for case in &corpus.aggregate_cases {
        output.push(run_aggregate(case).await);
    }
    for case in &corpus.drain_cases {
        output.push(run_drain(case));
    }
    for case in &corpus.endpoint_map_cases {
        output.push(run_endpoint_map(case));
    }
    for case in &corpus.list_all_cases {
        output.push(run_list_all(case).await);
    }
    for case in &corpus.sync_cases {
        output.push(run_sync(case).await);
    }
    for case in &corpus.cache_cases {
        output.push(run_cache(case).await);
    }

    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
