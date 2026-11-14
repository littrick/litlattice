//! M9.39a differential harness: drive the production `TreeAlertObserver`
//! (through `JobNode::alert(...).run_with_alert(...)`) over the
//! no-resolved-repos corpus and compare alerts/errors against the Go oracle
//! (`internal/search/alert/zz_m939a_oracle_test.go`).
//!
//! Env:
//!
//! * `M939A_CORPUS` corpus JSON
//! * `M939A_OUT` output JSON

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::parser::SearchType;
use sg_service_frontend::search_client::SearchMode;
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
};
use sg_service_frontend::search_jobutil_combinators::{Alert, AlertInputs, JobFailure};
use sg_service_frontend::search_jobutil_repos::{
    RepoPage, RepoPageSource, ZoektRepoLister,
};
use sg_service_frontend::search_jobutil_tree::{JobNode, StaticEvent};
use sg_service_frontend::search_stream::convert::{Match, RepoMatch};
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListResponse, SearchOptions, SearchResult};

#[derive(Debug, Default, Deserialize)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    original_query: String,
    #[serde(default = "default_pattern_type")]
    pattern_type: String,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    code_host_count: i64,
    #[serde(default)]
    has_results: bool,
    #[serde(default = "default_error_kind")]
    error_kind: String,
    #[serde(default)]
    error_message: String,
}

fn default_pattern_type() -> String {
    "literal".to_owned()
}

fn default_error_kind() -> String {
    "no_resolved".to_owned()
}

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        _ => SearchType::Literal,
    }
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

struct FakeDb {
    code_host_count: i64,
    site_admin: bool,
}

impl PrivateRepoLister for FakeDb {
    fn list_minimal_repos(
        &self,
        _options: &ReposListOptions,
    ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
        Ok(Vec::new())
    }

    fn count_code_host_external_services(&self) -> Result<i64, JobError> {
        Ok(self.code_host_count)
    }

    fn current_user_is_site_admin(&self) -> bool {
        self.site_admin
    }
}

/// `repos.Resolver.Iterator` with no pages: `reposExist` is always false.
struct EmptyPages;

impl RepoPageSource for EmptyPages {
    fn pages(&self) -> Vec<RepoPage> {
        Vec::new()
    }

    fn error(&self) -> Option<JobError> {
        None
    }
}

struct EmptyZoektLister;

impl ZoektRepoLister for EmptyZoektLister {
    fn list(&self) -> Result<ListResponse, JobError> {
        Ok(ListResponse::default())
    }
}

struct NoopStreamer;

impl JobStreamer for NoopStreamer {
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
// model -> normalized JSON
// ---------------------------------------------------------------------------

fn pattern_type_str(search_type: SearchType) -> String {
    sg_service_frontend::search_jobutil_combinators::pattern_type_str(search_type).to_owned()
}

fn alert_out(alert: &Alert) -> Value {
    json!({
        "prometheus_type": alert.prometheus_type,
        "title": alert.title,
        "description": alert.description,
        "proposed_queries": alert
            .proposed_queries
            .iter()
            .map(|query| {
                json!({
                    "description": query.description,
                    "query": query.query,
                    "pattern_type": pattern_type_str(query.pattern_type),
                })
            })
            .collect::<Vec<_>>(),
        "priority": alert.priority,
        "kind": alert.kind,
    })
}

fn result_match() -> Match {
    Match::Repo(RepoMatch {
        name: "fixture/result".to_owned(),
        id: 1,
        rev: String::new(),
        description_matches: Vec::new(),
        repo_name_matches: Vec::new(),
    })
}

#[test]
fn search_alerts_diff() {
    let Ok(corpus_path) = std::env::var("M939A_CORPUS") else {
        return;
    };
    let out_path = std::env::var("M939A_OUT").expect("M939A_OUT is not set");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut results = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let inputs = AlertInputs {
            query: case.query.clone(),
            original_query: case.original_query.clone(),
            pattern_type: search_type(&case.pattern_type),
            search_mode: SearchMode::Precise,
            on_sourcegraph_dot_com: case.dotcom,
            ..AlertInputs::default()
        };
        let events = if case.has_results {
            vec![StaticEvent {
                results: vec![result_match()],
                is_limit_hit: false,
                status: Vec::new(),
            }]
        } else {
            Vec::new()
        };
        let error = match case.error_kind.as_str() {
            "no_resolved" => Some(JobFailure::Message(
                sg_service_frontend::search_jobutil_repos::ERR_NO_RESOLVED_REPOS.to_owned(),
            )),
            "other" => Some(JobFailure::Message(case.error_message.clone())),
            _ => None,
        };
        let mut node = JobNode::alert(&inputs, JobNode::static_events(events, error));

        let db = FakeDb {
            code_host_count: case.code_host_count,
            site_admin: case.site_admin,
        };
        let pages = EmptyPages;
        let zoekt_lister = EmptyZoektLister;
        let streamer = NoopStreamer;
        let pager = sg_service_frontend::search_job::RepoPagerClients {
            pages: &pages,
            zoekt: &zoekt_lister,
            descriptions: None,
        };
        let clients = RuntimeClients {
            db: &db,
            zoekt: &streamer,
            max_repos: None,
            searcher: None,
            commit: None,
            repo_pager: Some(&pager),
        };

        let (alert, error) = node.run_with_alert(&JobContext::default(), &clients, &mut |_| {});
        results.push(json!({
            "name": case.name,
            "alert": alert.as_ref().map(alert_out),
            "error": error.map(|failure| failure.to_string()),
        }));
    }

    let encoded = serde_json::to_string_pretty(&results).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}
