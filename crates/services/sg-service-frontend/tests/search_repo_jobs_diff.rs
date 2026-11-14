//! M9.37a differential harness: repo-level job execution in the frontend tree
//! (`ComputeExcluded`, `ReposPartial`, `RepoSearch`, `Sanitize`,
//! `SubRepoPerms`) against the Go oracle
//! (`internal/search/job/jobutil/zz_m937a_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M937A_DIFF=1` enable the harness
//! * `M937A_CORPUS` corpus JSON
//! * `M937A_OUT` output JSON

use std::cell::RefCell;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_contexts::{ParsedRepoFilter, RepoVisibility};
use sg_service_frontend::search_job::{
    JobContext, JobError, PrivateRepoLister, ReposListOptions, RuntimeClients,
};
use sg_service_frontend::search_jobutil_repos::{
    RepoDescriptionLister, RepoPage, RepoPageSource, ResolvedPages, ZoektRepoLister,
};
use sg_service_frontend::search_jobutil_tree::{JobNode, TreeRepoOptions};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, FileMatch, FileRef, Match, MinimalRepo as EventMinimalRepo, RepoMatch,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_zoekt::indexed_repo_revs::{
    MinimalRepo, RepositoryRevisions, RepositoryRevisionsRepo,
};
use sg_zoekt::types::ListResponse;

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoFilterSpec {
    #[serde(default)]
    repo: String,
}

/// `actor.FromContext(ctx)` for the case (M9.39b).
#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct ActorSpec {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    internal: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoOptsSpec {
    #[serde(default)]
    repo_filters: Vec<RepoFilterSpec>,
    #[serde(default)]
    minus_repo_filters: Vec<String>,
    #[serde(default)]
    description_patterns: Vec<String>,
    #[serde(default)]
    search_context_spec: String,
    /// Request actor for `ResolveSearchContextSpec` (M9.39b).
    #[serde(default)]
    actor: ActorSpec,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    fork_set: bool,
    #[serde(default)]
    no_forks: bool,
    #[serde(default)]
    only_forks: bool,
    #[serde(default)]
    archived_set: bool,
    #[serde(default)]
    no_archived: bool,
    #[serde(default)]
    only_archived: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ResultSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    chunk_content: String,
    #[serde(default)]
    chunk_ranges: Vec<[i64; 2]>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct EventSpec {
    #[serde(default)]
    results: Vec<ResultSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoRevSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct PageSpec {
    #[serde(default)]
    repo_revs: Vec<RepoRevSpec>,
    #[serde(default)]
    backends_missing: i64,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    repo_opts: RepoOptsSpec,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    repo_name_patterns: Vec<String>,
    #[serde(default)]
    pages: Vec<PageSpec>,
    #[serde(default)]
    descriptions: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    count_error: String,
    #[serde(default)]
    canceled: bool,
    #[serde(default)]
    no_resolved_repos: bool,
    #[serde(default)]
    iterator_error: String,
    #[serde(default)]
    description_error: String,
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

struct FakeDb {
    case: Case,
    calls: RefCell<Vec<ReposListOptions>>,
}

impl PrivateRepoLister for FakeDb {
    fn list_minimal_repos(&self, _options: &ReposListOptions) -> Result<Vec<MinimalRepo>, JobError> {
        Ok(Vec::new())
    }

    fn count_repos(&self, options: &ReposListOptions) -> Result<i64, JobError> {
        if self.case.canceled {
            return Err(JobError("context canceled".to_owned()));
        }
        if !self.case.count_error.is_empty() {
            return Err(JobError(self.case.count_error.clone()));
        }
        let mut count = 3 + options.include_patterns.len() as i64;
        if options.only_forks {
            count += 100;
        }
        if options.only_archived {
            count += 200;
        }
        self.calls.borrow_mut().push(options.clone());
        Ok(count)
    }

    /// `searchcontexts.ResolveSearchContextSpec(ctx, ...)`. Global/empty
    /// needs no database; other specs resolve against `SG_QTEST_DSN` exactly
    /// like the Go oracle (`search_contexts`/`names`), applying `ctx`'s actor
    /// permission predicate.
    fn resolve_search_context(
        &self,
        spec: &str,
        ctx: &sg_service_frontend::search_contexts::RequestContext,
    ) -> Result<sg_service_frontend::search_contexts::SearchContext, JobError> {
        use sg_service_frontend::search_contexts::{
            get_global_search_context, is_global_search_context_spec,
            resolve_search_context_spec,
        };
        if is_global_search_context_spec(spec) {
            return Ok(get_global_search_context());
        }
        let dsn = std::env::var("SG_QTEST_DSN")
            .map_err(|_| JobError("SG_QTEST_DSN is not set".to_owned()))?;
        // sqlx pools need a Tokio context; the store's synchronous bridge uses
        // `block_in_place`, which requires a multi-thread runtime.
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> =
            std::sync::OnceLock::new();
        let runtime = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("search context diff runtime")
        });
        let pool = runtime
            .block_on(async {
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&dsn)
                    .await
            })
            .map_err(|error| JobError(error.to_string()))?;
        let _guard = runtime.enter();
        let store = sg_service_frontend::search_contexts_store::PgSearchContextsStore::new(pool);
        resolve_search_context_spec(&store, ctx, spec)
            .map_err(|error| JobError(error.to_string()))
    }
}

struct FakePages {
    case: Case,
}

impl RepoPageSource for FakePages {
    fn pages(&self) -> Vec<RepoPage> {
        self.case
            .pages
            .iter()
            .map(|page| RepoPage {
                repo_revs: page
                    .repo_revs
                    .iter()
                    .map(|repo| RepositoryRevisions {
                        repo: RepositoryRevisionsRepo {
                            id: repo.id as u32,
                            name: repo.name.clone(),
                        },
                        revs: repo.revs.clone(),
                    })
                    .collect(),
                backends_missing: page.backends_missing,
            })
            .collect()
    }

    fn error(&self) -> Option<JobError> {
        if !self.case.iterator_error.is_empty() {
            Some(JobError(self.case.iterator_error.clone()))
        } else {
            None
        }
    }

    fn resolve_pages(&self, _options: &sg_service_frontend::search_repos::ResolveOptions) -> ResolvedPages {
        ResolvedPages {
            pages: self.pages(),
            error: if self.case.no_resolved_repos {
                Some(JobError(
                    sg_service_frontend::search_jobutil_repos::ERR_NO_RESOLVED_REPOS.to_owned(),
                ))
            } else {
                self.error()
            },
            missing: Vec::new(),
        }
    }
}

struct FakeStreamer;

impl sg_service_frontend::search_job::JobStreamer for FakeStreamer {
    fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &sg_zoekt::types::SearchOptions,
        _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

struct FakeZoekt;

impl ZoektRepoLister for FakeZoekt {
    fn list(&self) -> Result<ListResponse, JobError> {
        Ok(ListResponse {
            repos: Vec::new(),
            repos_map: std::collections::BTreeMap::new(),
            crashes: 0,
            stats: Default::default(),
        })
    }
}

struct FakeDescriptions {
    case: Case,
}

impl RepoDescriptionLister for FakeDescriptions {
    fn get_repo_descriptions_by_ids(
        &self,
        _ids: &[u32],
    ) -> Result<std::collections::BTreeMap<u32, String>, JobError> {
        if !self.case.description_error.is_empty() {
            return Err(JobError(self.case.description_error.clone()));
        }
        Ok(self
            .case
            .descriptions
            .iter()
            .filter_map(|(id, description)| id.parse::<u32>().ok().map(|id| (id, description.clone())))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// construction / normalization
// ---------------------------------------------------------------------------

fn repo_opts(spec: &RepoOptsSpec) -> TreeRepoOptions {
    let visibility = match spec.visibility.as_str() {
        "Public" => RepoVisibility::Public,
        "Private" => RepoVisibility::Private,
        _ => RepoVisibility::Any,
    };
    TreeRepoOptions {
        repo_filters: spec
            .repo_filters
            .iter()
            .map(|filter| ParsedRepoFilter {
                repo: filter.repo.clone(),
                revs: Vec::new(),
            })
            .collect(),
        minus_repo_filters: spec.minus_repo_filters.clone(),
        description_patterns: spec.description_patterns.clone(),
        search_context_spec: spec.search_context_spec.clone(),
        request_context: sg_service_frontend::search_contexts::RequestContext {
            actor_uid: spec.actor.uid,
            internal: spec.actor.internal,
            sourcegraph_dot_com_mode: false,
        },
        visibility,
        limit: spec.limit,
        fork_set: spec.fork_set,
        no_forks: spec.no_forks,
        only_forks: spec.only_forks,
        archived_set: spec.archived_set,
        no_archived: spec.no_archived,
        only_archived: spec.only_archived,
        ..TreeRepoOptions::default()
    }
}

fn location_json(location: &Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn ranges_json(ranges: &[Range]) -> Vec<Value> {
    ranges
        .iter()
        .map(|range| {
            json!({
                "start": location_json(&range.start),
                "end": location_json(&range.end),
            })
        })
        .collect()
}

fn match_json(match_: &Match) -> Value {
    match match_ {
        Match::Repo(repo) => json!({
            "kind": "repo",
            "name": repo.name,
            "id": repo.id,
            "rev": repo.rev,
            "repo_name_matches": ranges_json(&repo.repo_name_matches),
            "description_matches": ranges_json(&repo.description_matches),
        }),
        Match::File(file) => json!({
            "kind": "file",
            "path": file.file.path,
            "chunk_matches": file.chunk_matches.iter().map(|chunk| json!({
                "content": chunk.content,
                "ranges": ranges_json(&chunk.ranges),
            })).collect::<Vec<Value>>(),
        }),
        _ => json!({"kind": "unknown"}),
    }
}

fn event_json(event: &SearchEvent) -> Value {
    json!({
        "results": event.results.iter().map(match_json).collect::<Vec<Value>>(),
        "stats": {
            "excluded_forks": event.stats.excluded_forks,
            "excluded_archived": event.stats.excluded_archived,
            "backends_missing": event.stats.backends_missing,
        },
    })
}

fn static_events(case: &Case) -> JobNode {
    let events = case
        .events
        .iter()
        .map(|event| {
            let results = event
                .results
                .iter()
                .map(|spec| match spec.kind.as_str() {
                    "repo" => Match::Repo(RepoMatch {
                        name: spec.name.clone(),
                        id: spec.id,
                        rev: spec.rev.clone(),
                        description_matches: Vec::new(),
                        repo_name_matches: Vec::new(),
                    }),
                    "file" => {
                        let ranges = spec
                            .chunk_ranges
                            .iter()
                            .map(|r| Range {
                                start: Location {
                                    offset: r[0],
                                    line: 0,
                                    column: 0,
                                },
                                end: Location {
                                    offset: r[1],
                                    line: 0,
                                    column: 0,
                                },
                            })
                            .collect();
                        Match::File(FileMatch {
                            file: FileRef {
                                repo: EventMinimalRepo {
                                    id: spec.repo_id,
                                    name: spec.repo_name.clone(),
                                },
                                commit_id: String::new(),
                                path: spec.path.clone(),
                                input_rev: None,
                                precise_language: String::new(),
                            },
                            chunk_matches: vec![ChunkMatch {
                                content: spec.chunk_content.clone(),
                                content_start: Location {
                                    offset: 0,
                                    line: 0,
                                    column: 0,
                                },
                                ranges,
                            }],
                            symbols: Vec::new(),
                            path_matches: Vec::new(),
                            limit_hit: false,
                            debug: None,
                        })
                    }
                    other => panic!("unsupported result kind {other}"),
                })
                .collect();
            sg_service_frontend::search_jobutil_tree::StaticEvent {
                results,
                is_limit_hit: false,
                status: Vec::new(),
            }
        })
        .collect();
    JobNode::static_events(events, None)
}

fn count_calls_json(calls: &[ReposListOptions]) -> Vec<Value> {
    let mut calls = calls.to_vec();
    calls.sort_by_key(|options| (options.only_forks, options.only_archived));
    calls
        .iter()
        .map(|options| {
            json!({
                "only_forks": options.only_forks,
                "only_archived": options.only_archived,
                "no_forks": options.no_forks,
                "no_archived": options.no_archived,
                "only_private": options.only_private,
                "no_private": options.no_private,
                "include_patterns": options.include_patterns,
                "exclude_pattern": options.exclude_pattern,
                "limit": options.limit,
                "search_context_id": options.search_context_id,
            })
        })
        .collect()
}

fn run_case(case: &Case) -> Value {
    let db = FakeDb {
        case: case.clone(),
        calls: RefCell::new(Vec::new()),
    };
    let pages = FakePages {
        case: case.clone(),
    };
    let zoekt = FakeZoekt;
    let streamer = FakeStreamer;
    let descriptions = FakeDescriptions {
        case: case.clone(),
    };
    let pager = sg_service_frontend::search_job::RepoPagerClients {
        pages: &pages,
        zoekt: &zoekt,
        descriptions: Some(&descriptions),
    };
    let clients = RuntimeClients {
        db: &db,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
        commit: None,
        repo_pager: Some(&pager),
    };
    let ctx = JobContext { deadline: None };

    let mut tree = match case.kind.as_str() {
        "compute_excluded" => JobNode::compute_excluded(repo_opts(&case.repo_opts)),
        "sanitize" => JobNode::sanitize(case.patterns.clone(), static_events(case)),
        "sub_repo_perms" => JobNode::sub_repo_perms(static_events(case)),
        "repos_partial" => JobNode::repos_partial(static_events(case)),
        "repo_search" => JobNode::repo_search(repo_opts(&case.repo_opts), case.repo_name_patterns.clone()),
        other => panic!("unknown case kind {other}"),
    };

    let mut events = Vec::new();
    let mut sink = |event: SearchEvent| events.push(event);
    let (_alert, failure) = tree.run_with_alert(&ctx, &clients, &mut sink);
    let error = failure.map(|failure| failure.to_string()).unwrap_or_default();

    json!({
        "name": case.name,
        "error": error,
        "events": events.iter().map(event_json).collect::<Vec<Value>>(),
        "count_calls": count_calls_json(&db.calls.borrow()),
    })
}

fn load_cases(path: &str) -> Vec<Case> {
    let raw = std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {path}: {error}"));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {path}: {error}"))
}

#[test]
fn search_repo_jobs_diff() {
    if std::env::var("SG_M937A_DIFF").as_deref() != Ok("1")
        && std::env::var("SG_M938A_DIFF").as_deref() != Ok("1")
        && std::env::var("SG_M939B_DIFF").as_deref() != Ok("1")
    {
        eprintln!("SG_M937A_DIFF/SG_M938A_DIFF/SG_M939B_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M937A_CORPUS").expect("M937A_CORPUS must be set");
    let out_path = std::env::var("M937A_OUT").expect("M937A_OUT must be set");

    let mut cases = load_cases(&corpus_path);
    // M9.38a appends the search-context corpus in the same order as the Go
    // oracle's concatenated output.
    if let Ok(context_corpus) = std::env::var("M938A_CORPUS") {
        cases.extend(load_cases(&context_corpus));
    }
    // M9.39b appends the actor corpus (same order as the Go oracle output).
    if let Ok(actor_corpus) = std::env::var("M939B_CORPUS") {
        cases.extend(load_cases(&actor_corpus));
    }

    let output: Vec<Value> = cases.iter().map(run_case).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
