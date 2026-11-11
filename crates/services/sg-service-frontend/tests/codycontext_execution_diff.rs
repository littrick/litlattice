//! M9.8jy differential harness: codycontext `searchJob.Run` execution.
//!
//! Drives [`CodyContextExecutionJob`] with fake `CombinatorJob` children that
//! return fixed matches/alerts/errors and record their invocations, compared
//! with the Go oracle `internal/search/codycontext/zz_m9jy_oracle_test.go`
//! (which drives the real `searchJob.Run`).
//!
//! Env:
//!
//! * `SG_M9JY_DIFF=1` enable the harness
//! * `M9JY_CORPUS` corpus JSON
//! * `M9JY_OUT` output JSON

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::codycontext::{CodyContextExecutionJob, CodyFileMatcher};
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients, Verbosity,
};
use sg_service_frontend::search_jobutil_combinators::{
    Alert, CombinatorJob, JobFailure, RunContext, Sender,
};
use sg_service_frontend::search_stream::convert::{
    FileMatch, FileRef, Match, MinimalRepo, RepoMatch,
};
use sg_service_frontend::search_stream::progress::SearchEvent;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    code_count: i32,
    #[serde(default)]
    text_count: i32,
    #[serde(default)]
    matcher: bool,
    #[serde(default)]
    deny_paths: Vec<String>,
    #[serde(default)]
    parent_canceled: bool,
    #[serde(default)]
    symbol: ChildSpec,
    #[serde(default)]
    code: ChildSpec,
    #[serde(default)]
    text: ChildSpec,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct ChildSpec {
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    alert: Option<AlertSpec>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct EventSpec {
    #[serde(default)]
    results: Vec<MatchSpec>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct MatchSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    input_rev: Option<String>,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    rev: String,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct AlertSpec {
    #[serde(default)]
    prometheus_type: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    priority: i32,
}

impl AlertSpec {
    fn to_alert(&self) -> Alert {
        Alert {
            prometheus_type: self.prometheus_type.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            proposed_queries: Vec::new(),
            kind: self.kind.clone(),
            priority: self.priority,
        }
    }
}

impl MatchSpec {
    fn to_match(&self) -> Match {
        match self.kind.as_str() {
            "file" => Match::File(FileMatch {
                file: FileRef {
                    repo: MinimalRepo {
                        id: self.repo_id,
                        name: self.repo_name.clone(),
                    },
                    commit_id: self.commit.clone(),
                    path: self.path.clone(),
                    input_rev: self.input_rev.clone(),
                    precise_language: String::new(),
                },
                chunk_matches: Vec::new(),
                symbols: Vec::new(),
                path_matches: Vec::new(),
                limit_hit: false,
                debug: None,
            }),
            "repo" => Match::Repo(RepoMatch {
                name: if self.name.is_empty() {
                    self.repo_name.clone()
                } else {
                    self.name.clone()
                },
                id: if self.id != 0 { self.id } else { self.repo_id },
                rev: self.rev.clone(),
                description_matches: Vec::new(),
                repo_name_matches: Vec::new(),
            }),
            other => panic!("unknown match kind {other}"),
        }
    }
}

impl EventSpec {
    fn to_search_event(&self) -> SearchEvent {
        SearchEvent {
            results: self.results.iter().map(MatchSpec::to_match).collect(),
            stats: Default::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Recording fake child
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct Invocation {
    count: i32,
    canceled_at_entry: bool,
    canceled_after: bool,
}

#[derive(Default)]
struct Recorder {
    invocations: Mutex<BTreeMap<String, Invocation>>,
}

impl Recorder {
    fn invoke(&self, name: &str, canceled: bool) {
        let mut invocations = self.invocations.lock().expect("recorder mutex");
        let entry = invocations.entry(name.to_owned()).or_default();
        entry.count += 1;
        entry.canceled_at_entry |= canceled;
    }

    fn after(&self, name: &str, canceled: bool) {
        let mut invocations = self.invocations.lock().expect("recorder mutex");
        let entry = invocations.entry(name.to_owned()).or_default();
        entry.canceled_after |= canceled;
    }

    fn to_json(&self) -> Value {
        let invocations = self.invocations.lock().expect("recorder mutex");
        let mut out = BTreeMap::new();
        for (name, invocation) in invocations.iter() {
            out.insert(
                name.clone(),
                obj(vec![
                    ("count", json!(invocation.count)),
                    ("canceled_at_entry", json!(invocation.canceled_at_entry)),
                    ("canceled_after", json!(invocation.canceled_after)),
                ]),
            );
        }
        Value::Object(out.into_iter().collect())
    }
}

struct FakeChild {
    name: &'static str,
    spec: ChildSpec,
    recorder: Arc<Recorder>,
}

impl CombinatorJob for FakeChild {
    fn run(
        &mut self,
        ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        self.recorder.invoke(self.name, ctx.is_canceled());
        for event in &self.spec.events {
            stream(event.to_search_event());
        }
        self.recorder.after(self.name, ctx.is_canceled());
        (
            self.spec.alert.as_ref().map(AlertSpec::to_alert),
            self.spec.error.clone().map(JobFailure::Message),
        )
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

struct NoopStreamer;

impl JobStreamer for NoopStreamer {
    fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &sg_zoekt::types::SearchOptions,
        _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

struct NoopLister;

impl PrivateRepoLister for NoopLister {
    fn list_minimal_repos(
        &self,
        _options: &ReposListOptions,
    ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let map: BTreeMap<String, Value> = pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn normalize_match(match_: &Match) -> Value {
    match match_ {
        Match::File(file) => obj(vec![
            ("kind", json!("file")),
            ("repo_id", json!(file.file.repo.id)),
            ("repo_name", json!(file.file.repo.name)),
            ("commit", json!(file.file.commit_id)),
            ("path", json!(file.file.path)),
            (
                "input_rev",
                match &file.file.input_rev {
                    Some(rev) => json!(rev),
                    None => Value::Null,
                },
            ),
        ]),
        Match::Repo(repo) => obj(vec![
            ("kind", json!("repo")),
            ("id", json!(repo.id)),
            ("name", json!(repo.name)),
            ("rev", json!(repo.rev)),
        ]),
        _ => obj(vec![("kind", json!("other"))]),
    }
}

fn normalize_alert(alert: Option<Alert>) -> Value {
    match alert {
        None => Value::Null,
        Some(alert) => obj(vec![
            ("prometheus_type", json!(alert.prometheus_type)),
            ("title", json!(alert.title)),
            ("description", json!(alert.description)),
            ("kind", json!(alert.kind)),
            ("priority", json!(alert.priority)),
            ("proposed_queries", Value::Array(Vec::new())),
        ]),
    }
}

fn normalize_attributes(attributes: Vec<(String, String)>) -> Value {
    Value::Array(
        attributes
            .into_iter()
            .map(|(key, value)| Value::Array(vec![Value::String(key), Value::String(value)]))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Case runner
// ---------------------------------------------------------------------------

fn run_case(case: &Case) -> Value {
    let recorder = Arc::new(Recorder::default());
    let deny_paths = case.deny_paths.clone();
    let file_matcher = if case.matcher {
        CodyFileMatcher::Custom(Arc::new(move |_repo: i32, path: &str| {
            !deny_paths.iter().any(|denied| denied == path)
        }))
    } else {
        CodyFileMatcher::AcceptAll
    };

    let mut job = CodyContextExecutionJob::new(
        Some(Box::new(FakeChild {
            name: "fake-symbol",
            spec: case.symbol.clone(),
            recorder: recorder.clone(),
        })),
        Box::new(FakeChild {
            name: "fake-code",
            spec: case.code.clone(),
            recorder: recorder.clone(),
        }),
        Box::new(FakeChild {
            name: "fake-text",
            spec: case.text.clone(),
            recorder: recorder.clone(),
        }),
        case.patterns.clone(),
        case.code_count,
        case.text_count,
        file_matcher,
    );

    let ctx = RunContext::new(None);
    if case.parent_canceled {
        ctx.cancel();
    }

    let streamer = NoopStreamer;
    let lister = NoopLister;
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
        commit: None,
        repo_pager: None,
    };

    let mut events: Vec<Value> = Vec::new();
    let mut sender = |event: SearchEvent| {
        events.push(Value::Array(
            event.results.iter().map(normalize_match).collect(),
        ));
    };
    let (alert, err) = job.run(&ctx, &clients, &mut sender);
    drop(sender);

    let map_children_same = std::ptr::eq(
        job.map_children(&mut |_| panic!("MapChildren must not invoke the map function")),
        &job,
    );
    let errors = match err {
        Some(err) => Value::Array(
            err.parts()
                .iter()
                .map(|part| Value::String(part.to_string()))
                .collect(),
        ),
        None => Value::Null,
    };

    obj(vec![
        ("name", json!(case.name)),
        (
            "result",
            obj(vec![
                ("name", json!(job.name())),
                (
                    "attributes",
                    obj(vec![
                        (
                            "none",
                            normalize_attributes(job.attributes(Verbosity::None)),
                        ),
                        (
                            "basic",
                            normalize_attributes(job.attributes(Verbosity::Basic)),
                        ),
                        ("max", normalize_attributes(job.attributes(Verbosity::Max))),
                    ]),
                ),
                ("children", json!(job.children().len())),
                ("map_children_same", json!(map_children_same)),
                ("events", Value::Array(events)),
                ("alert", normalize_alert(alert)),
                ("errors", errors),
                ("invocations", recorder.to_json()),
            ]),
        ),
    ])
}

#[test]
fn codycontext_execution_differential() {
    if std::env::var("SG_M9JY_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9JY_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9JY_CORPUS").expect("M9JY_CORPUS must be set");
    let out_path = std::env::var("M9JY_OUT").expect("M9JY_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run_case).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
