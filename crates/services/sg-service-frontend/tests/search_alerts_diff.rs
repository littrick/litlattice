//! M9.40b differential harness: drive the production `TreeAlertObserver`
//! (through `JobNode::alert(...).run_with_alert(...)`) over the search-alert
//! corpus (M9.39a no-resolved-repos cases plus M9.40b missing-revs /
//! over-repo-limit / structural-OOM / MultiError / reposExist cases) and
//! compare alerts/errors against the Go oracle
//! (`internal/search/alert/zz_m940b_oracle_test.go`).
//!
//! Env:
//!
//! * `M940B_CORPUS` corpus JSON
//! * `M940B_OUT` output JSON

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::parser::SearchType;
use sg_service_frontend::search_client::SearchMode;
use sg_service_frontend::search_contexts::RevisionSpecifier;
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
};
use sg_service_frontend::search_jobutil_combinators::{
    Alert, AlertInputs, JobFailure, QueryDescription,
};
use sg_service_frontend::search_jobutil_repos::{
    RepoPage, RepoPageSource, ResolvedPages, ZoektRepoLister, ERR_NO_RESOLVED_REPOS,
};
use sg_service_frontend::search_jobutil_tree::{JobNode, StaticEvent};
use sg_service_frontend::search_repos::{RepoRevSpecs, ResolveOptions};
use sg_service_frontend::search_stream::convert::{Match, RepoMatch};
use sg_zoekt::indexed_repo_revs::{RepositoryRevisions, RepositoryRevisionsRepo};
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListResponse, SearchOptions, SearchResult};

#[derive(Debug, Default, Deserialize)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct MissingSpec {
    #[serde(default)]
    repo: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct QueryDescSpec {
    #[serde(default)]
    description: String,
    #[serde(default)]
    query: String,
    #[serde(default = "default_pattern_type")]
    pattern_type: String,
}

/// One leaf of a `MultiError` (`error_kind: "multi"`).
#[derive(Debug, Default, Deserialize, Clone)]
struct ErrorSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    missing: Vec<MissingSpec>,
    #[serde(default)]
    proposed_queries: Vec<QueryDescSpec>,
    #[serde(default)]
    description: String,
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
    #[serde(default)]
    missing: Vec<MissingSpec>,
    #[serde(default)]
    over_repo_limit_proposed: Vec<QueryDescSpec>,
    #[serde(default)]
    over_repo_limit_description: String,
    /// `MultiError` parts, used when `error_kind == "multi"`.
    #[serde(default)]
    errors: Vec<ErrorSpec>,
    #[serde(default)]
    repos_exist_fork: bool,
    #[serde(default)]
    repos_exist_archived: bool,
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
// corpus -> model
// ---------------------------------------------------------------------------

fn rev_specs_from(missing: &[MissingSpec]) -> Vec<RepoRevSpecs> {
    missing
        .iter()
        .enumerate()
        .map(|(index, spec)| RepoRevSpecs {
            repo: RepositoryRevisionsRepo {
                id: index as u32 + 1,
                name: spec.repo.clone(),
            },
            revs: spec
                .revs
                .iter()
                .map(|rev| RevisionSpecifier {
                    rev_spec: rev.clone(),
                    ..RevisionSpecifier::default()
                })
                .collect(),
            external_service_type: None,
        })
        .collect()
}

fn query_descs(specs: &[QueryDescSpec]) -> Vec<QueryDescription> {
    specs
        .iter()
        .map(|spec| QueryDescription {
            description: spec.description.clone(),
            query: spec.query.clone(),
            pattern_type: search_type(&spec.pattern_type),
            annotations: BTreeMap::new(),
        })
        .collect()
}

/// Build the `JobFailure` the baseline observer would receive for one leaf.
fn failure_for(
    kind: &str,
    message: &str,
    missing: &[MissingSpec],
    proposed_queries: &[QueryDescSpec],
    description: &str,
) -> Option<JobFailure> {
    match kind {
        "missing_revs" => Some(JobFailure::MissingRepoRevs(rev_specs_from(missing))),
        "over_repo_limit" => Some(JobFailure::OverRepoLimit {
            proposed_queries: query_descs(proposed_queries),
            description: description.to_owned(),
        }),
        "no_resolved" => Some(JobFailure::Message(ERR_NO_RESOLVED_REPOS.to_owned())),
        "other" => Some(JobFailure::Message(message.to_owned())),
        "" | "nil" => None,
        other => panic!("unknown error kind {other:?}"),
    }
}

fn case_failure(case: &Case) -> Option<JobFailure> {
    if case.error_kind == "multi" {
        let parts: Vec<JobFailure> = case
            .errors
            .iter()
            .map(|part| {
                failure_for(
                    &part.kind,
                    &part.message,
                    &part.missing,
                    &part.proposed_queries,
                    &part.description,
                )
                .expect("multi parts must be non-nil")
            })
            .collect();
        Some(JobFailure::Multi(parts))
    } else {
        failure_for(
            &case.error_kind,
            &case.error_message,
            &case.missing,
            &case.over_repo_limit_proposed,
            &case.over_repo_limit_description,
        )
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

/// Injectable `repos.Resolver.Iterator` seam for `reposExist`. The alert code
/// probes with `OnlyArchived` for the archived proposal and without it for
/// the fork proposal, so the fake keys off `ResolveOptions::only_archived`.
struct SeamPages {
    fork_exists: bool,
    archived_exists: bool,
}

impl SeamPages {
    fn page(&self) -> RepoPage {
        RepoPage {
            repo_revs: vec![RepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: 1,
                    name: "r1".to_owned(),
                },
                revs: vec![String::new()],
            }],
            backends_missing: 0,
        }
    }
}

impl RepoPageSource for SeamPages {
    fn pages(&self) -> Vec<RepoPage> {
        Vec::new()
    }

    fn error(&self) -> Option<JobError> {
        None
    }

    fn resolve_pages(&self, options: &ResolveOptions) -> ResolvedPages {
        let exists = if options.only_archived {
            self.archived_exists
        } else {
            self.fork_exists
        };
        ResolvedPages {
            pages: if exists { vec![self.page()] } else { Vec::new() },
            error: None,
            missing: Vec::new(),
        }
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
    let Ok(corpus_path) = std::env::var("M940B_CORPUS") else {
        return;
    };
    let out_path = std::env::var("M940B_OUT").expect("M940B_OUT is not set");
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
        let error = case_failure(case);
        let mut node = JobNode::alert(&inputs, JobNode::static_events(events, error));

        let db = FakeDb {
            code_host_count: case.code_host_count,
            site_admin: case.site_admin,
        };
        let pages = SeamPages {
            fork_exists: case.repos_exist_fork,
            archived_exists: case.repos_exist_archived,
        };
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
