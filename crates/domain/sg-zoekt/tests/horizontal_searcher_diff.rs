//! M9.8ha differential harness:
//! `internal/search/backend/horizontal.go:33-260` (`HorizontalSearcher`
//! `StreamSearch` / `Search` style aggregation / `List` / `searchers` /
//! `syncSearchers` / `Close` / `String`) against
//! [`sg_zoekt::horizontal_searcher`].
//!
//! Run with `SG_M9HA_DIFF=1 M9HA_CORPUS=<json> M9HA_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON. Event order across endpoints is nondeterministic in both languages,
//! so `stream_search` compare the sorted union of `repo:file` names, the
//! summed crash count and the number of delivered events. `List` compares
//! sorted repository names / ids. `sync` compares cumulative dial and close
//! counts after each explicit synchronization.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::horizontal::RolloutError;
use sg_zoekt::horizontal_searcher::{Client, ClientError, HorizontalSearcher};
use sg_zoekt::query::Query;
use sg_zoekt::types::{
    ListOptions, ListResponse, RepoListEntry, Repository, SearchOptions, SearchResult, Stats,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    stream_search_cases: Vec<StreamSearchCase>,
    #[serde(default)]
    list_cases: Vec<ListCase>,
    #[serde(default)]
    sync_cases: Vec<SyncCase>,
}

#[derive(Deserialize)]
struct StreamSearchCase {
    name: String,
    endpoints: Vec<String>,
    #[serde(default)]
    behaviors: BTreeMap<String, Behavior>,
}

#[derive(Deserialize)]
struct ListCase {
    name: String,
    endpoints: Vec<String>,
    #[serde(default)]
    behaviors: BTreeMap<String, Behavior>,
}

#[derive(Deserialize)]
struct SyncCase {
    name: String,
    steps: Vec<SyncStep>,
}

#[derive(Deserialize)]
struct SyncStep {
    op: String,
    #[serde(default)]
    endpoints: Vec<String>,
    #[serde(default)]
    endpoints_error: String,
}

#[derive(Deserialize, Default, Clone)]
struct Behavior {
    #[serde(default)]
    events: Vec<Event>,
    #[serde(default)]
    repos: Vec<String>,
    #[serde(default)]
    repos_map: Vec<u32>,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    error: String,
    #[serde(default)]
    rollout: String,
}

#[derive(Deserialize, Default, Clone)]
struct Event {
    #[serde(default)]
    files: Vec<CorpusFile>,
    #[serde(default)]
    crashes: i64,
}

#[derive(Deserialize, Default, Clone)]
struct CorpusFile {
    #[serde(default)]
    repository: String,
    #[serde(default)]
    file_name: String,
}

#[derive(Debug, Clone)]
struct FakeError {
    message: String,
    rollout: RolloutError,
}

impl FakeError {
    fn plain(message: &str) -> Self {
        Self {
            message: message.to_owned(),
            rollout: RolloutError::Other,
        }
    }

    fn rollout(rollout: RolloutError) -> Self {
        Self {
            message: "rollout".to_owned(),
            rollout,
        }
    }
}

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FakeError {}

impl ClientError for FakeError {
    fn rollout_error(&self) -> RolloutError {
        self.rollout
    }
}

#[derive(Default)]
struct State {
    dials: Mutex<BTreeMap<String, usize>>,
    closed: AtomicUsize,
}

struct FakeClient {
    events: Vec<SearchResult>,
    list: ListResponse,
    error: Option<FakeError>,
    state: Arc<State>,
}

impl Client for FakeClient {
    type Error = FakeError;

    fn list<'a>(
        &'a self,
        _query: &'a Query,
        _opts: Option<ListOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResponse, FakeError>> + Send + 'a>> {
        Box::pin(async move {
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(self.list.clone()),
            }
        })
    }

    fn stream_search<'a>(
        &'a self,
        _query: &'a Query,
        _options: Option<&'a SearchOptions>,
        sender: &'a mut (dyn FnMut(SearchResult) + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), FakeError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            for event in &self.events {
                sender(event.clone());
            }
            Ok(())
        })
    }

    fn close(&self) {
        self.state.closed.fetch_add(1, Ordering::SeqCst);
    }
}

fn rollout_error(value: &str) -> RolloutError {
    match value {
        "dns_not_found" => RolloutError::DnsNotFound,
        "dial_timeout" => RolloutError::DialTimeout,
        "dial_refused" => RolloutError::DialRefused,
        "read_failed" => RolloutError::ReadFailed,
        _ => RolloutError::Other,
    }
}

fn make_client(behavior: Behavior, state: Arc<State>) -> FakeClient {
    let events = behavior
        .events
        .iter()
        .map(|event| SearchResult {
            files: event
                .files
                .iter()
                .map(|file| sg_zoekt::types::FileMatch {
                    repository: file.repository.clone(),
                    file_name: file.file_name.as_bytes().to_vec(),
                    ..Default::default()
                })
                .collect(),
            stats: Stats {
                crashes: event.crashes,
                ..Stats::default()
            },
            ..SearchResult::default()
        })
        .collect();

    let list = ListResponse {
        repos: behavior
            .repos
            .iter()
            .map(|name| RepoListEntry {
                repository: Repository {
                    name: name.clone(),
                    ..Repository::default()
                },
                ..RepoListEntry::default()
            })
            .collect(),
        repos_map: behavior
            .repos_map
            .iter()
            .map(|id| (*id, Default::default()))
            .collect(),
        crashes: behavior.crashes,
        stats: Default::default(),
    };

    let error = if !behavior.rollout.is_empty() {
        Some(FakeError::rollout(rollout_error(&behavior.rollout)))
    } else if !behavior.error.is_empty() {
        Some(FakeError::plain(&behavior.error))
    } else {
        None
    };

    FakeClient {
        events,
        list,
        error,
        state,
    }
}

fn build_searcher(
    endpoints: Vec<String>,
    behaviors: BTreeMap<String, Behavior>,
) -> (HorizontalSearcher<FakeClient>, Arc<State>) {
    let state = Arc::new(State::default());
    let dial_state = Arc::clone(&state);
    let behaviors = Arc::new(behaviors);
    let searcher = HorizontalSearcher::new(
        move || Ok(endpoints.clone()),
        move |endpoint: &str| {
            *dial_state
                .dials
                .lock()
                .unwrap()
                .entry(endpoint.to_owned())
                .or_insert(0) += 1;
            make_client(
                behaviors.get(endpoint).cloned().unwrap_or_default(),
                Arc::clone(&dial_state),
            )
        },
    );
    (searcher, state)
}

#[tokio::test]
async fn horizontal_searcher_diff() {
    if std::env::var("SG_M9HA_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HA_CORPUS").expect("M9HA_CORPUS must be set when SG_M9HA_DIFF=1");
    let out_path = std::env::var("M9HA_OUT").expect("M9HA_OUT must be set when SG_M9HA_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut entries = Vec::new();
    for case in &corpus.stream_search_cases {
        entries.push(run_stream_search(case).await);
    }
    for case in &corpus.list_cases {
        entries.push(run_list(case).await);
    }
    for case in &corpus.sync_cases {
        entries.push(run_sync(case));
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

async fn run_stream_search(case: &StreamSearchCase) -> Value {
    let (searcher, _state) = build_searcher(case.endpoints.clone(), case.behaviors.clone());

    let mut files = Vec::new();
    let mut crashes = 0i64;
    let mut events = 0usize;
    let mut error = String::new();
    {
        let mut sender = |event: SearchResult| {
            events += 1;
            crashes += event.stats.crashes;
            for file in &event.files {
                files.push(format!(
                    "{}:{}",
                    file.repository,
                    String::from_utf8_lossy(&file.file_name)
                ));
            }
        };
        if let Err(stream_error) = searcher
            .stream_search(&Query::Const(true), None, &mut sender)
            .await
        {
            error = stream_error.to_string();
        }
    }
    files.sort();

    json!({
        "name": case.name,
        "kind": "stream_search",
        "error": error,
        "files": files,
        "crashes": crashes,
        "events": events,
    })
}

async fn run_list(case: &ListCase) -> Value {
    let (searcher, _state) = build_searcher(case.endpoints.clone(), case.behaviors.clone());

    let mut repos = Vec::new();
    let mut repos_map = Vec::new();
    let mut crashes = 0i64;
    let mut stats_repos = 0usize;
    let mut error = String::new();

    match searcher.list(&Query::Const(true), None).await {
        Ok(list) => {
            repos = list
                .repos
                .iter()
                .map(|entry| entry.repository.name.clone())
                .collect();
            repos_map = list.repos_map.keys().copied().collect();
            crashes = list.crashes;
            stats_repos = list.repos.len() + list.repos_map.len();
        }
        Err(list_error) => error = list_error.to_string(),
    }
    repos.sort();

    json!({
        "name": case.name,
        "kind": "list",
        "error": error,
        "repos": repos,
        "repos_map": repos_map,
        "crashes": crashes,
        "stats_repos": stats_repos,
    })
}

#[derive(Default)]
struct EndpointsState {
    endpoints: Vec<String>,
    error: Option<String>,
}

fn run_sync(case: &SyncCase) -> Value {
    let endpoints = Arc::new(Mutex::new(EndpointsState::default()));
    let state = Arc::new(State::default());

    let source = Arc::clone(&endpoints);
    let dial_state = Arc::clone(&state);
    let searcher = HorizontalSearcher::new(
        move || {
            let current = source.lock().unwrap();
            match &current.error {
                Some(message) => Err(FakeError::plain(message)),
                None => Ok(current.endpoints.clone()),
            }
        },
        move |endpoint: &str| {
            *dial_state
                .dials
                .lock()
                .unwrap()
                .entry(endpoint.to_owned())
                .or_insert(0) += 1;
            make_client(Behavior::default(), Arc::clone(&dial_state))
        },
    );

    let mut steps = Vec::new();
    for step in &case.steps {
        let mut error = String::new();
        match step.op.as_str() {
            "sync" => {
                {
                    let mut current = endpoints.lock().unwrap();
                    current.endpoints = step.endpoints.clone();
                    current.error = if step.endpoints_error.is_empty() {
                        None
                    } else {
                        Some(step.endpoints_error.clone())
                    };
                }
                if let Err(sync_error) = searcher.searchers() {
                    error = sync_error.to_string();
                }
            }
            "close" => searcher.close(),
            other => panic!("unknown sync op {other}"),
        }

        let dials = state.dials.lock().unwrap().clone();
        let closed = state.closed.load(Ordering::SeqCst);
        steps.push(json!({
            "op": step.op,
            "endpoints": step.endpoints,
            "string": searcher.to_string(),
            "dials": dials,
            "closed": closed,
            "error": error,
        }));
    }

    json!({
        "name": case.name,
        "kind": "sync",
        "steps": steps,
    })
}
