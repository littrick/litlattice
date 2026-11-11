//! M9.8kb differential harness: codycontext runtime injection through the
//! production `SearchInputs` -> `TreeInputs` path, `NewSearchJob` construction
//! driven by the injected cody options (limits + `.cody/ignore` matcher), and
//! `CodyContextExecutionJob` execution/output quotas.
//!
//! Go oracles:
//!
//! * `internal/search/job/jobutil/zz_m9kb_oracle_test.go` (tree + execution)
//! * `cmd/frontend/internal/codycontext/zz_m9kb_dotcom_oracle_test.go`
//!   (`.cody/ignore` acceptance/rejection)
//!
//! Env:
//!
//! * `SG_M9KB_DIFF=1` enable the harness
//! * `M9KB_CORPUS` corpus JSON
//! * `M9KB_OUT` output JSON

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_search_query::printer::string_human;
use sg_service_frontend::codycontext::{
    new_search_job, CodyContextExecutionJob, CodyContextJobParts, CodyContextOptions,
    RepoContentFilter,
};
use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients, Verbosity,
};
use sg_service_frontend::search_jobutil_combinators::{
    Alert, CombinatorJob, JobFailure, RunContext, Sender,
};
use sg_service_frontend::search_jobutil_tree::{new_plan_job, JobNode, TreeInputs};
use sg_service_frontend::search_stream::convert::{
    FileMatch, FileRef, Match, MinimalRepo, RepoMatch,
};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_service_frontend::search_stream::{tree_inputs, SearchInputs};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    context_lines: i32,
    #[serde(default)]
    cody: CodySpec,
    #[serde(default)]
    child: ChildSpec,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    probes: Vec<Probe>,
}

#[derive(Debug, Default, Deserialize)]
struct CodySpec {
    #[serde(default)]
    features_null: bool,
    #[serde(default)]
    code_count: i32,
    #[serde(default)]
    text_count: i32,
    #[serde(default)]
    deny_paths: Vec<String>,
    #[serde(default)]
    ignore_repos: Vec<IgnoreRepo>,
}

#[derive(Debug, Default, Deserialize)]
struct IgnoreRepo {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    contents: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    lookup_error: bool,
    #[serde(default)]
    ignore_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Probe {
    #[serde(default)]
    repo: i32,
    #[serde(default)]
    path: String,
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
// Options
// ---------------------------------------------------------------------------

fn protocol(value: &str) -> Protocol {
    match value {
        "" | "streaming" => Protocol::Streaming,
        "batch" => Protocol::Batch,
        "exhaustive" => Protocol::Exhaustive,
        other => panic!("unknown protocol {other}"),
    }
}

/// Build the post-`Plan` cody options the way `addLimitsAndFilter` does:
/// counts from the corpus and the `.cody/ignore` matcher built from the
/// injected per-repo filter contents.
fn cody_options(spec: &CodySpec) -> Option<CodyContextOptions> {
    if spec.features_null {
        return None;
    }

    let deny_paths = spec.deny_paths.clone();
    let mut filter = RepoContentFilter::new();
    let mut has_ignore = false;
    for repo in &spec.ignore_repos {
        has_ignore = true;
        // A parse error leaves the repo without a filter entry, matching
        // `getFilter` skipping it (accept everything).
        let _ = filter.insert(repo.id, repo.contents.as_deref());
    }

    let file_matcher = if has_ignore {
        Some(
            Arc::new(move |repo_id: i32, path: &str| filter.accepts(repo_id, path))
                as Arc<dyn Fn(i32, &str) -> bool + Send + Sync>,
        )
    } else if !deny_paths.is_empty() {
        Some(
            Arc::new(move |_repo: i32, path: &str| !deny_paths.iter().any(|denied| denied == path))
                as Arc<dyn Fn(i32, &str) -> bool + Send + Sync>,
        )
    } else {
        None
    };

    Some(CodyContextOptions {
        code_count: spec.code_count,
        text_count: spec.text_count,
        file_matcher,
    })
}

fn search_inputs(case: &Case) -> SearchInputs {
    SearchInputs {
        max_results: 10_000,
        original_query: case.original_query.clone(),
        plan: pipeline_query(&case.query, SearchType::Standard)
            .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query)),
        search_mode: SearchMode::Precise,
        pattern_type: SearchType::CodyContext,
        features: Features::default(),
        protocol: protocol(&case.protocol),
        context_lines: case.context_lines,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        cody_context: cody_options(&case.cody),
    }
}

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------

fn attributes(values: Vec<(String, String)>) -> Value {
    Value::Array(
        values
            .into_iter()
            .map(|(key, value)| Value::Array(vec![Value::String(key), Value::String(value)]))
            .collect(),
    )
}

fn tree(node: &JobNode) -> Value {
    let (name, attrs, children) = node.describe();
    json!({
        "name": name,
        "attributes": attributes(attrs),
        "children": children.into_iter().map(tree).collect::<Vec<Value>>(),
    })
}

fn run_tree(case: &Case) -> Value {
    let inputs = search_inputs(case);
    let tree_inputs: TreeInputs = tree_inputs(&inputs);
    match new_plan_job(&tree_inputs, &inputs.plan) {
        Ok(node) => json!({"ok": true, "tree": tree(&node)}),
        Err(error) => json!({"ok": false, "error": error.0}),
    }
}

// ---------------------------------------------------------------------------
// Matcher
// ---------------------------------------------------------------------------

fn run_matcher(case: &Case) -> Value {
    let mut filter = RepoContentFilter::new();
    for repo in &case.repos {
        // `getFilter` skips repos whose default branch lookup fails or whose
        // commit is empty; the matcher then accepts every path.
        if repo.lookup_error || repo.commit.is_empty() {
            continue;
        }
        let _ = filter.insert(repo.id, repo.ignore_file.as_deref());
    }
    let accepts: Vec<Value> = case
        .probes
        .iter()
        .map(|probe| Value::Bool(filter.accepts(probe.repo, &probe.path)))
        .collect();
    json!({"probes": accepts})
}

// ---------------------------------------------------------------------------
// Exec
// ---------------------------------------------------------------------------

struct FakeChild {
    name: &'static str,
    spec: ChildSpec,
}

impl CombinatorJob for FakeChild {
    fn run(
        &mut self,
        _ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        for event in &self.spec.events {
            stream(event.to_search_event());
        }
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

fn normalize_match(match_: &Match) -> Value {
    match match_ {
        Match::File(file) => json!({
            "kind": "file",
            "repo_id": file.file.repo.id,
            "repo_name": file.file.repo.name,
            "commit": file.file.commit_id,
            "path": file.file.path,
            "input_rev": file.file.input_rev,
        }),
        Match::Repo(repo) => json!({
            "kind": "repo",
            "id": repo.id,
            "name": repo.name,
            "rev": repo.rev,
        }),
        _ => json!({"kind": "other"}),
    }
}

fn normalize_alert(alert: Option<Alert>) -> Value {
    match alert {
        None => Value::Null,
        Some(alert) => json!({
            "prometheus_type": alert.prometheus_type,
            "title": alert.title,
            "description": alert.description,
            "kind": alert.kind,
            "priority": alert.priority,
            "proposed_queries": [],
        }),
    }
}

fn run_exec(case: &Case) -> Value {
    let inputs = search_inputs(case);
    let parts = match new_search_job(&inputs.plan, inputs.cody_context.as_ref()) {
        Ok(parts) => parts,
        Err(error) => return json!({"ok": false, "error": error.0}),
    };

    let CodyContextJobParts::Jobs {
        symbol_query,
        code_query,
        text_query,
        patterns,
        code_count,
        text_count,
        file_matcher,
    } = parts
    else {
        return json!({"ok": true, "noop": true});
    };

    let mut calls = Vec::new();
    if let Some(query) = &symbol_query {
        calls.push(string_human(&query.to_parse_tree()));
    }
    calls.push(string_human(&code_query.to_parse_tree()));
    calls.push(string_human(&text_query.to_parse_tree()));

    let fake = |name: &'static str| {
        Box::new(FakeChild {
            name,
            spec: case.child.clone(),
        }) as Box<dyn CombinatorJob>
    };
    let symbol_job = symbol_query.as_ref().map(|_| fake("fake-symbol"));
    let code_job = fake("fake-code");
    let text_job = fake("fake-text");

    let mut job = CodyContextExecutionJob::new(
        symbol_job,
        code_job,
        text_job,
        patterns.clone(),
        code_count,
        text_count,
        file_matcher,
    );

    let ctx = RunContext::new(None);
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

    let errors = match err {
        Some(err) => Value::Array(
            err.parts()
                .iter()
                .map(|part| Value::String(part.to_string()))
                .collect(),
        ),
        None => Value::Null,
    };

    json!({
        "ok": true,
        "noop": false,
        "calls": calls,
        "events": events,
        "alert": normalize_alert(alert),
        "errors": errors,
        "attributes": {
            "none": attributes(job.attributes(Verbosity::None)),
            "basic": attributes(job.attributes(Verbosity::Basic)),
            "max": attributes(job.attributes(Verbosity::Max)),
        },
    })
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn run(case: &Case) -> Value {
    let result = match case.kind.as_str() {
        "tree" => run_tree(case),
        "matcher" => run_matcher(case),
        "exec" => run_exec(case),
        other => panic!("unknown kind {other}"),
    };
    Value::Object(
        BTreeMap::from_iter([
            ("kind".to_owned(), Value::String(case.kind.clone())),
            ("name".to_owned(), Value::String(case.name.clone())),
            ("result".to_owned(), result),
        ])
        .into_iter()
        .collect(),
    )
}

#[test]
fn codycontext_injection_differential() {
    if std::env::var("SG_M9KB_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9KB_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9KB_CORPUS").expect("M9KB_CORPUS must be set");
    let out_path = std::env::var("M9KB_OUT").expect("M9KB_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
