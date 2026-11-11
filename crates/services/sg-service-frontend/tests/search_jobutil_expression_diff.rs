//! M9.8hk differential harness: the jobutil expression jobs
//! (`internal/search/job/jobutil/expression_job.go`) against
//! [`sg_service_frontend::search_jobutil_expression`].
//!
//! Run with `SG_M9HK_DIFF=1 M9HK_CORPUS=<json> M9HK_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients, Verbosity,
};
use sg_service_frontend::search_jobutil_combinators::{
    Alert, CombinatorJob, JobFailure, RunContext, Sender,
};
use sg_service_frontend::search_jobutil_expression::{new_and_job, new_or_job};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, FileMatch, FileRef, Match, MinimalRepo, RepoMatch,
};
use sg_service_frontend::search_stream::events::Range;
use sg_service_frontend::search_stream::progress::{
    repo_status, RepoStatusMap, SearchEvent, Stats,
};
use sg_zoekt::indexed_repo_revs::MinimalRepo as ZoektMinimalRepo;
use sg_zoekt::query::Query;
use sg_zoekt::types::{SearchOptions, SearchResult};

// ---------------------------------------------------------------------------
// Corpus schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    order: String,
    #[serde(default)]
    job: Option<JobSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct JobSpec {
    #[serde(rename = "type", default)]
    typ: String,
    #[serde(default)]
    children: Vec<JobSpec>,
    #[serde(default)]
    label: String,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    error: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    alert: Option<AlertSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct EventSpec {
    #[serde(default)]
    matches: Vec<MatchSpec>,
    #[serde(default)]
    stats: StatsSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct StatsSpec {
    #[serde(default)]
    is_limit_hit: bool,
    #[serde(default)]
    timedout_repo_ids: Vec<i32>,
    #[serde(default)]
    missing_repo_ids: Vec<i32>,
    #[serde(default)]
    cloning_repo_ids: Vec<i32>,
    #[serde(default)]
    backends_missing: i64,
    #[serde(default)]
    excluded_forks: i64,
    #[serde(default)]
    excluded_archived: i64,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct MatchSpec {
    #[serde(rename = "type", default)]
    typ: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    input_rev: Option<String>,
    #[serde(default)]
    chunks: Vec<usize>,
    #[serde(default)]
    limit_hit: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
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

// ---------------------------------------------------------------------------
// Fake child job
// ---------------------------------------------------------------------------

struct EmitJob {
    spec: JobSpec,
    log: Arc<Mutex<Vec<String>>>,
}

impl CombinatorJob for EmitJob {
    fn run(
        &mut self,
        _ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        self.log
            .lock()
            .expect("run log mutex poisoned")
            .push(self.spec.label.clone());
        for event in &self.spec.events {
            stream(event.to_search_event());
        }
        let err = match self.spec.error.as_str() {
            "" | "none" => None,
            "canceled" => Some(JobFailure::Canceled),
            "deadline" => Some(JobFailure::DeadlineExceeded),
            "message" => Some(JobFailure::Message(self.spec.message.clone())),
            other => panic!("unknown emit error {other}"),
        };
        (self.spec.alert.as_ref().map(AlertSpec::to_alert), err)
    }

    fn name(&self) -> &'static str {
        "M9HKEmitJob"
    }
}

// ---------------------------------------------------------------------------
// Conversion from the corpus
// ---------------------------------------------------------------------------

impl MatchSpec {
    fn to_match(&self) -> Match {
        match self.typ.as_str() {
            "repo" => Match::Repo(RepoMatch {
                name: self.name.clone(),
                id: self.id,
                rev: self.rev.clone(),
                description_matches: Vec::new(),
                repo_name_matches: Vec::new(),
            }),
            "file" => {
                let chunk_matches = self
                    .chunks
                    .iter()
                    .map(|count| ChunkMatch {
                        content: String::new(),
                        content_start: Default::default(),
                        ranges: vec![Range::default(); *count],
                    })
                    .collect();
                Match::File(FileMatch {
                    file: FileRef {
                        repo: MinimalRepo {
                            id: self.id,
                            name: self.repo.clone(),
                        },
                        commit_id: self.commit.clone(),
                        path: self.path.clone(),
                        input_rev: self.input_rev.clone(),
                        precise_language: String::new(),
                    },
                    chunk_matches,
                    symbols: Vec::new(),
                    path_matches: Vec::new(),
                    limit_hit: self.limit_hit,
                    debug: None,
                })
            }
            other => panic!("unknown match type {other}"),
        }
    }
}

impl StatsSpec {
    fn to_stats(&self) -> Stats {
        let mut status = RepoStatusMap::default();
        for id in &self.timedout_repo_ids {
            status.update(*id, repo_status::TIMED_OUT);
        }
        for id in &self.missing_repo_ids {
            status.update(*id, repo_status::MISSING);
        }
        for id in &self.cloning_repo_ids {
            status.update(*id, repo_status::CLONING);
        }
        Stats {
            is_limit_hit: self.is_limit_hit,
            repos: HashSet::new(),
            status,
            backends_missing: self.backends_missing,
            excluded_forks: self.excluded_forks,
            excluded_archived: self.excluded_archived,
        }
    }
}

impl EventSpec {
    fn to_search_event(&self) -> SearchEvent {
        SearchEvent {
            results: self.matches.iter().map(MatchSpec::to_match).collect(),
            stats: self.stats.to_stats(),
        }
    }
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

fn build_job(spec: &JobSpec, log: &Arc<Mutex<Vec<String>>>) -> Box<dyn CombinatorJob> {
    match spec.typ.as_str() {
        "and" => new_and_job(
            spec.children
                .iter()
                .map(|child| build_job(child, log))
                .collect(),
        ),
        "or" => new_or_job(
            spec.children
                .iter()
                .map(|child| build_job(child, log))
                .collect(),
        ),
        "emit" => Box::new(EmitJob {
            spec: spec.clone(),
            log: Arc::clone(log),
        }),
        other => panic!("unknown job type {other}"),
    }
}

// ---------------------------------------------------------------------------
// Normalized output
// ---------------------------------------------------------------------------

fn norm_match(match_: &Match) -> Value {
    match match_ {
        Match::File(file) => json!({
            "type": "file",
            "repo": file.file.repo.name,
            "id": file.file.repo.id,
            "commit": file.file.commit_id,
            "path": file.file.path,
            "input_rev": file.file.input_rev,
            "chunks": file.chunk_matches.iter().map(|chunk| chunk.ranges.len()).collect::<Vec<_>>(),
            "symbols": file.symbols.len(),
            "limit_hit": file.limit_hit,
        }),
        Match::Repo(repo) => json!({
            "type": "repo",
            "name": repo.name,
            "id": repo.id,
            "rev": repo.rev,
        }),
        other => json!({"type": format!("{other:?}")}),
    }
}

fn norm_stats(stats: &Stats) -> Value {
    let mut repos: Vec<i32> = stats.repos.iter().copied().collect();
    repos.sort_unstable();
    json!({
        "is_limit_hit": stats.is_limit_hit,
        "repos": repos,
        "timedout": stats.status.filter(repo_status::TIMED_OUT),
        "missing": stats.status.filter(repo_status::MISSING),
        "cloning": stats.status.filter(repo_status::CLONING),
        "backends_missing": stats.backends_missing,
        "excluded_forks": stats.excluded_forks,
        "excluded_archived": stats.excluded_archived,
    })
}

fn sort_values(values: &mut [Value]) {
    values.sort_by_key(|value| serde_json::to_string(value).expect("serialize value"));
}

fn norm_event(event: &SearchEvent, sorted: bool) -> Value {
    let mut matches = event.results.iter().map(norm_match).collect::<Vec<_>>();
    if sorted {
        sort_values(&mut matches);
    }
    json!({
        "matches": matches,
        "stats": norm_stats(&event.stats),
    })
}

fn norm_alert(alert: &Option<Alert>) -> Value {
    let Some(alert) = alert else {
        return Value::Null;
    };
    json!({
        "prometheus_type": alert.prometheus_type,
        "title": alert.title,
        "description": alert.description,
        "kind": alert.kind,
        "priority": alert.priority,
        "proposed_queries": Vec::<Value>::new(),
    })
}

fn norm_error(err: &Option<JobFailure>, sorted: bool) -> Value {
    let Some(err) = err else {
        return Value::Null;
    };
    let mut parts = err
        .parts()
        .into_iter()
        .map(|part| {
            if part.is_canceled() {
                json!({"kind": "canceled"})
            } else if part.is_deadline_exceeded() {
                json!({"kind": "deadline"})
            } else if let JobFailure::Message(message) = part {
                json!({"kind": "message", "message": message})
            } else {
                json!({"kind": "message", "message": part.to_string()})
            }
        })
        .collect::<Vec<_>>();
    if sorted {
        sort_values(&mut parts);
    }
    json!({"parts": parts})
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

struct NoDb;

impl PrivateRepoLister for NoDb {
    fn list_minimal_repos(
        &self,
        _options: &ReposListOptions,
    ) -> Result<Vec<ZoektMinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct NoZoekt;

impl JobStreamer for NoZoekt {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

fn run_case(case: &Case) -> Value {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = case.job.as_ref().expect("run case requires a job");
    let mut job = build_job(spec, &log);

    let db = NoDb;
    let zoekt = NoZoekt;
    let clients = RuntimeClients {
        db: &db,
        zoekt: &zoekt,
        max_repos: None,
        searcher: None,
        commit: None,
    };
    let ctx = RunContext::default();
    let mut events: Vec<SearchEvent> = Vec::new();
    let (alert, err) = {
        let mut sender = |event: SearchEvent| events.push(event);
        job.run(&ctx, &clients, &mut sender)
    };

    let mut run_log = log.lock().expect("run log mutex poisoned").clone();
    // Launches are concurrent in the baseline; their order is not part of this
    // slice's semantics.
    run_log.sort();

    let sorted = case.order == "sorted";
    let mut event_values = events
        .iter()
        .map(|event| norm_event(event, sorted))
        .collect::<Vec<_>>();
    if sorted {
        sort_values(&mut event_values);
    }

    json!({
        "run_log": run_log,
        "events": event_values,
        "alert": norm_alert(&alert),
        "error": norm_error(&err, sorted),
    })
}

fn describe_case(case: &Case) -> Value {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = case.job.as_ref().expect("describe case requires a job");
    let job = build_job(spec, &log);
    let attributes = job
        .attributes(Verbosity::Max)
        .into_iter()
        .map(|(key, value)| json!([key, value]))
        .collect::<Vec<_>>();
    let children = job
        .children()
        .into_iter()
        .map(|child| Value::String(child.name().to_owned()))
        .collect::<Vec<_>>();
    json!({
        "name": job.name(),
        "attributes": attributes,
        "children": children,
    })
}

#[test]
fn search_jobutil_expression_diff() {
    if std::env::var("SG_M9HK_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HK_CORPUS").expect("M9HK_CORPUS");
    let out_path = std::env::var("M9HK_OUT").expect("M9HK_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Vec::with_capacity(cases.len());
    for case in &cases {
        let result = match case.kind.as_str() {
            "run" => run_case(case),
            "describe" => describe_case(case),
            other => panic!("unknown kind {other}"),
        };
        output.push(json!({
            "name": case.name,
            "kind": case.kind,
            "result": result,
        }));
    }

    let encoded = serde_json::to_string(&output).expect("serialize output");
    std::fs::write(&out_path, encoded).expect("write output");
}
