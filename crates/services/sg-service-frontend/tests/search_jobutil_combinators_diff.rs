//! M9.8hh differential harness: the `jobutil` combinator tree
//! (`internal/search/job/jobutil/{combinators,limit,alert}.go`) against
//! [`sg_service_frontend::search_jobutil_combinators`].
//!
//! Run with `SG_M9HH_DIFF=1 M9HH_CORPUS=<json> M9HH_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_core::parse_go_duration;
use sg_search_query::parser::SearchType;
use sg_service_frontend::search_client::{Protocol, SearchMode};
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients, Verbosity,
};
use sg_service_frontend::search_jobutil_combinators::{
    alert_for_smart_search, alert_for_timeout, format_go_duration, longer, max_priority_alert,
    new_alert_job, new_limit_job, new_noop_job, new_parallel_job, new_sequential_job,
    new_timeout_job, pattern_type_str, Alert, AlertInputs, AlertObserver, CombinatorJob,
    JobFailure, QueryDescription, RunContext,
};
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
    context: ContextSpec,
    #[serde(default)]
    job: Option<JobSpec>,
    #[serde(default)]
    verbosity: String,
    #[serde(default)]
    alerts: Vec<Option<AlertSpec>>,
    #[serde(default)]
    n: i64,
    #[serde(default)]
    duration: String,
    #[serde(default)]
    used: String,
    #[serde(default)]
    suggest: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    pattern_type: String,
}

#[derive(Debug, Deserialize, Default)]
struct ContextSpec {
    #[serde(default)]
    canceled: bool,
    #[serde(default)]
    deadline: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct JobSpec {
    #[serde(rename = "type", default)]
    typ: String,
    #[serde(default)]
    ensure_unique: bool,
    #[serde(default)]
    children: Vec<JobSpec>,
    #[serde(default)]
    child: Option<Box<JobSpec>>,
    #[serde(default)]
    timeout: String,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    inputs: InputsSpec,
    #[serde(default)]
    label: String,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    error: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    return_ctx_err: bool,
    #[serde(default)]
    check_cancel: bool,
    #[serde(default)]
    alert: Option<AlertSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct InputsSpec {
    #[serde(default)]
    query: String,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    search_mode: String,
    #[serde(default)]
    features: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    on_sourcegraph_dot_com: bool,
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
    /// Present in the corpus schema, but the Go oracle normalizer does not
    /// emit symbol information, so the differential deliberately ignores it.
    #[allow(dead_code)]
    #[serde(default)]
    symbols: usize,
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
    #[serde(default)]
    proposed_queries: Vec<QueryDescriptionSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct QueryDescriptionSpec {
    #[serde(default)]
    description: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
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
        ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> (Option<Alert>, Option<JobFailure>) {
        self.log
            .lock()
            .expect("run log mutex poisoned")
            .push(self.spec.label.clone());
        for event in &self.spec.events {
            if self.spec.check_cancel && ctx.err().is_some() {
                break;
            }
            stream(event.to_search_event());
        }
        let mut err = match self.spec.error.as_str() {
            "" | "none" => None,
            "canceled" => Some(JobFailure::Canceled),
            "deadline" => Some(JobFailure::DeadlineExceeded),
            "message" => Some(JobFailure::Message(self.spec.message.clone())),
            other => panic!("unknown emit error {other}"),
        };
        if err.is_none() && self.spec.return_ctx_err {
            err = ctx.err();
        }
        (self.spec.alert.as_ref().map(AlertSpec::to_alert), err)
    }

    fn name(&self) -> &'static str {
        "M9HHEmitJob"
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
                    limit_hit: false,
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
            proposed_queries: self
                .proposed_queries
                .iter()
                .map(|pq| QueryDescription {
                    description: pq.description.clone(),
                    query: pq.query.clone(),
                    pattern_type: search_type(&pq.pattern_type),
                    annotations: pq.annotations.clone(),
                })
                .collect(),
            kind: self.kind.clone(),
            priority: self.priority,
        }
    }
}

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        _ => SearchType::Standard,
    }
}

impl InputsSpec {
    fn to_alert_inputs(&self) -> AlertInputs {
        AlertInputs {
            query: self.query.clone(),
            original_query: self.original_query.clone(),
            pattern_type: search_type(&self.pattern_type),
            search_mode: match self.search_mode.as_str() {
                "smart" => SearchMode::SmartSearch,
                _ => SearchMode::Precise,
            },
            features: self.features.clone(),
            protocol: match self.protocol.as_str() {
                "batch" => Protocol::Batch,
                "exhaustive" => Protocol::Exhaustive,
                _ => Protocol::Streaming,
            },
            on_sourcegraph_dot_com: self.on_sourcegraph_dot_com,
        }
    }
}

fn build_job(spec: &JobSpec, log: &Arc<Mutex<Vec<String>>>) -> Box<dyn CombinatorJob> {
    match spec.typ.as_str() {
        "noop" => new_noop_job(),
        "emit" => Box::new(EmitJob {
            spec: spec.clone(),
            log: Arc::clone(log),
        }),
        "sequential" => {
            let children = spec
                .children
                .iter()
                .map(|child| build_job(child, log))
                .collect();
            new_sequential_job(spec.ensure_unique, children)
        }
        "parallel" => {
            let children = spec
                .children
                .iter()
                .map(|child| build_job(child, log))
                .collect();
            new_parallel_job(children)
        }
        "timeout" => new_timeout_job(parse_duration(&spec.timeout), build_child(spec, log)),
        "limit" => new_limit_job(spec.limit, build_child(spec, log)),
        "alert" => {
            let inputs = spec.inputs.to_alert_inputs();
            let observer_inputs = inputs.clone();
            new_alert_job(
                inputs,
                build_child(spec, log),
                Box::new(move || Box::new(RealObserver::new(observer_inputs.clone()))),
            )
        }
        other => panic!("unknown job type {other}"),
    }
}

fn build_child(spec: &JobSpec, log: &Arc<Mutex<Vec<String>>>) -> Box<dyn CombinatorJob> {
    let child = spec.child.as_deref().expect("job requires a child");
    build_job(child, log)
}

fn parse_duration(value: &str) -> Duration {
    parse_go_duration(value).unwrap_or_else(|err| panic!("bad duration {value:?}: {err}"))
}

// ---------------------------------------------------------------------------
// Alert observer (the parts of `alert.Observer` reachable from this slice)
// ---------------------------------------------------------------------------

struct RealObserver {
    inputs: AlertInputs,
    has_results: bool,
    alert: Option<Alert>,
    err: Option<JobFailure>,
}

impl RealObserver {
    fn new(inputs: AlertInputs) -> Self {
        Self {
            inputs,
            has_results: false,
            alert: None,
            err: None,
        }
    }

    fn append_error(&mut self, error: JobFailure) {
        self.err = Some(match self.err.take() {
            None => error,
            Some(JobFailure::Multi(mut errors)) => {
                errors.push(error);
                JobFailure::Multi(errors)
            }
            Some(existing) => JobFailure::Multi(vec![existing, error]),
        });
    }
}

impl AlertObserver for RealObserver {
    fn set_has_results(&mut self, has_results: bool) {
        self.has_results = has_results;
    }

    fn observe_error(&mut self, ctx: &RunContext, err: &JobFailure) {
        // `isContextError`: the context being canceled wins over the error's
        // own identity, and a Multi containing any context error is skipped.
        if ctx.err().is_some() || err.is_canceled() || err.is_deadline_exceeded() {
            return;
        }
        // Every residual error that `errorToAlert` cannot convert is kept.
        for part in err.parts() {
            if let JobFailure::Message(message) = part {
                self.append_error(JobFailure::Message(message.clone()));
            }
        }
    }

    fn done(&mut self) -> (Option<Alert>, Option<JobFailure>) {
        if !self.has_results
            && self.inputs.pattern_type != SearchType::Structural
            && has_match_hole(&self.inputs.original_query)
        {
            // `Observer.update` only replaces on a strictly higher priority.
            let structural = alert_for_structural_search_not_set(&self.inputs.original_query);
            if self
                .alert
                .as_ref()
                .is_none_or(|alert| structural.priority > alert.priority)
            {
                self.alert = Some(structural);
            }
        }
        // With results, `Done` logs (and drops) the residual errors.
        if self.has_results && self.err.is_some() {
            return (self.alert.take(), None);
        }
        (self.alert.take(), self.err.take())
    }
}

/// `search.AlertForStructuralSearchNotSet` (`internal/search/alert.go:152-165`).
fn alert_for_structural_search_not_set(query_string: &str) -> Alert {
    Alert {
        prometheus_type: "structural_search_not_set".to_owned(),
        title: "No results".to_owned(),
        description: "It looks like you're trying to run a structural search, but it is not \
                      enabled using the patterntype keyword or UI toggle."
            .to_owned(),
        proposed_queries: vec![QueryDescription {
            description: "Activate structural search".to_owned(),
            query: query_string.to_owned(),
            pattern_type: SearchType::Structural,
            annotations: BTreeMap::new(),
        }],
        kind: String::new(),
        priority: 0,
    }
}

/// Go `comby.MatchHoleRegexp` (`internal/comby/translate.go:12-31`).
fn has_match_hole(query: &str) -> bool {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    let regex = REGEX.get_or_init(|| {
        Regex::new(r":\[(\w+)\]|:\[\[(\w+)\]\]|:\[(\w+)\.\]|:\[(\w+)\\n\]|:\[( +(\w+)?)?\]|\.\.\.")
            .expect("match-hole regex")
    });
    regex.is_match(query)
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

fn norm_event(event: &SearchEvent) -> Value {
    json!({
        "matches": event.results.iter().map(norm_match).collect::<Vec<_>>(),
        "stats": norm_stats(&event.stats),
    })
}

fn norm_alert(alert: &Option<Alert>) -> Value {
    let Some(alert) = alert else {
        return Value::Null;
    };
    let proposed = alert
        .proposed_queries
        .iter()
        .map(|pq| {
            json!({
                "description": pq.description,
                "query": pq.query,
                "pattern_type": pattern_type_str(pq.pattern_type),
                "annotations": pq.annotations,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "prometheus_type": alert.prometheus_type,
        "title": alert.title,
        "description": alert.description,
        "kind": alert.kind,
        "priority": alert.priority,
        "proposed_queries": proposed,
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

fn sort_values(values: &mut [Value]) {
    values.sort_by_key(|value| serde_json::to_string(value).expect("serialize value"));
}

fn sort_strings(values: &mut [String]) {
    values.sort();
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

fn build_run_context(spec: &ContextSpec) -> RunContext {
    let deadline = match spec.deadline.as_str() {
        "past" => Some(Instant::now() - Duration::from_secs(3600)),
        "future" => Some(Instant::now() + Duration::from_secs(3600)),
        _ => None,
    };
    let mut ctx = RunContext::new(deadline);
    if spec.canceled {
        let (child, handle) = ctx.with_cancel();
        handle.cancel();
        ctx = child;
    }
    ctx
}

fn run_case(case: &Case) -> Value {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = case.job.as_ref().expect("run case requires a job");
    let mut job = build_job(spec, &log);
    let ctx = build_run_context(&case.context);
    let db = NoDb;
    let zoekt = NoZoekt;
    let clients = RuntimeClients {
        db: &db,
        zoekt: &zoekt,
        max_repos: None,
        searcher: None,
        commit: None,
        repo_pager: None,
    };

    let mut events: Vec<SearchEvent> = Vec::new();
    let (alert, err) = {
        let mut sender = |event: SearchEvent| events.push(event);
        job.run(&ctx, &clients, &mut sender)
    };

    let mut run_log = log.lock().expect("run log mutex poisoned").clone();
    let mut event_values = events.iter().map(norm_event).collect::<Vec<_>>();
    let sorted = case.order == "sorted";
    if sorted {
        sort_strings(&mut run_log);
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
        .attributes(verbosity(&case.verbosity))
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

fn verbosity(name: &str) -> Verbosity {
    match name {
        "basic" => Verbosity::Basic,
        "max" => Verbosity::Max,
        _ => Verbosity::None,
    }
}

fn alert_max_case(case: &Case) -> Value {
    let alerts = case
        .alerts
        .iter()
        .map(|alert| alert.as_ref().map(AlertSpec::to_alert))
        .collect::<Vec<_>>();
    json!({"alert": norm_alert(&max_priority_alert(&alerts))})
}

fn alert_longer_case(case: &Case) -> Value {
    let duration = parse_duration(&case.duration);
    json!({"duration": format_go_duration(longer(case.n, duration))})
}

fn alert_timeout_case(case: &Case) -> Value {
    let used = parse_duration(&case.used);
    let suggest = parse_duration(&case.suggest);
    let alert = alert_for_timeout(used, suggest, &case.query, search_type(&case.pattern_type));
    json!({"alert": norm_alert(&Some(alert))})
}

#[test]
fn search_jobutil_combinators_diff() {
    if std::env::var("SG_M9HH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HH_CORPUS").expect("M9HH_CORPUS");
    let out_path = std::env::var("M9HH_OUT").expect("M9HH_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Vec::with_capacity(cases.len());
    for case in &cases {
        let result = match case.kind.as_str() {
            "run" => run_case(case),
            "describe" => describe_case(case),
            "alert_max" => alert_max_case(case),
            "alert_longer" => alert_longer_case(case),
            "alert_smart" => json!({"alert": norm_alert(&Some(alert_for_smart_search()))}),
            "alert_timeout" => alert_timeout_case(case),
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
