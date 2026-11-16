//! M9.37b differential harness: structural / file:contains.content() /
//! file:has.contributor() leaf execution against the Go oracle
//! (`internal/search/job/jobutil/zz_m937b_leaf_oracle_test.go`).
//!
//! Env: `SG_M937B_DIFF=1`, `M937B_CORPUS`, `M937B_OUT`.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_commit_job::{CommitRuntimeClients, CommitUserDB, GitserverClient};
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, RuntimeClients, SearchJob,
};
use sg_service_frontend::search_jobutil_combinators as comb;
use sg_service_frontend::search_jobutil_filters::{ContributorCount, ContributorFetcher};
use sg_service_frontend::search_leaf_jobs::{
    FileContainsLeafJob, FileHasContributorsLeafJob, LeafJobClients, StructuralSearchJob,
};
use sg_service_frontend::search_searcher_jobs::TextPatternInfo;
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MinimalRepo, Signature,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_zoekt::types::{SearchOptions, SearchResult};

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    batch_retry: bool,
    #[serde(default)]
    unindexed_repos: usize,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    original: Vec<String>,
    #[serde(default)]
    event: EventSpec,
    #[serde(default)]
    contributors: HashMap<String, Vec<ContributorSpec>>,
    #[serde(default)]
    fetch_errors: Vec<String>,
    #[serde(default)]
    child_error: bool,
}

#[derive(Debug, Default, Deserialize)]
struct EventSpec {
    #[serde(default)]
    results: Vec<ResultSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct ResultSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    chunks: Vec<ChunkSpec>,
    #[serde(default)]
    diff: String,
    #[serde(default)]
    diff_ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: i64,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct RangeSpec {
    #[serde(default)]
    start: i64,
    #[serde(default)]
    end: i64,
}

#[derive(Debug, Default, Deserialize)]
struct ContributorSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    count: i32,
}

struct NoDb;
impl PrivateRepoLister for NoDb {
    fn list_minimal_repos(
        &self,
        _options: &sg_service_frontend::search_job::ReposListOptions,
    ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct NoStreamer;
impl JobStreamer for NoStreamer {
    fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

struct FakeUserDb;
impl CommitUserDB for FakeUserDb {
    fn get_user_id_by_username(&self, _username: &str) -> Result<Option<i32>, JobError> {
        Ok(None)
    }
    fn list_emails_by_user(
        &self,
        _user_id: i32,
    ) -> Result<Vec<sg_service_frontend::search_commit_job::CommitUserEmail>, JobError> {
        Ok(Vec::new())
    }
}

struct FakeGit;
impl GitserverClient for FakeGit {
    fn search(
        &self,
        _request: &sg_proto::gitserver::SearchRequest,
        _on_matches: &mut dyn FnMut(Vec<sg_proto::gitserver::CommitMatch>),
    ) -> Result<bool, sg_service_frontend::search_searcher_client::SearcherError> {
        Ok(false)
    }
}

struct FakeContributors {
    counts: HashMap<String, Vec<ContributorCount>>,
    errors: Vec<String>,
}

impl ContributorFetcher for FakeContributors {
    fn contributor_count(
        &self,
        _repo_name: &str,
        options: &sg_service_frontend::search_jobutil_filters::ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError> {
        if self.errors.iter().any(|path| path == &options.path) {
            return Err(JobError("fetch failed".to_owned()));
        }
        Ok(self.counts.get(&options.path).cloned().unwrap_or_default())
    }
}

fn loc(offset: i64) -> Location {
    Location {
        offset,
        line: 1,
        column: 1,
    }
}

fn range(spec: &RangeSpec) -> Range {
    Range {
        start: loc(spec.start),
        end: loc(spec.end),
    }
}

fn minimal_repo(spec: &ResultSpec) -> MinimalRepo {
    MinimalRepo {
        id: spec.repo_id,
        name: spec.repo_name.clone(),
    }
}

fn build_match(spec: &ResultSpec) -> Option<Match> {
    match spec.kind.as_str() {
        "file" => Some(Match::File(FileMatch {
            file: FileRef {
                repo: minimal_repo(spec),
                commit_id: spec.commit.clone(),
                path: spec.path.clone(),
                input_rev: None,
                precise_language: String::new(),
            },
            chunk_matches: spec
                .chunks
                .iter()
                .map(|chunk| ChunkMatch {
                    content: chunk.content.clone(),
                    content_start: loc(chunk.content_start),
                    ranges: chunk.ranges.iter().map(range).collect(),
                })
                .collect(),
            symbols: Vec::new(),
            path_matches: Vec::new(),
            limit_hit: false,
            debug: None,
        })),
        "commit" => Some(Match::Commit(CommitMatch {
            commit: GitCommit {
                id: spec.commit.clone(),
                message: String::new(),
                author: Signature {
                    name: String::new(),
                    email: String::new(),
                    date: time::OffsetDateTime::UNIX_EPOCH,
                },
                committer: Signature {
                    name: String::new(),
                    email: String::new(),
                    date: time::OffsetDateTime::UNIX_EPOCH,
                },
                parents: Vec::new(),
            },
            repo: minimal_repo(spec),
            message_preview: None,
            diff_preview: (!spec.diff.is_empty()).then(|| {
                sg_service_frontend::search_stream::convert::MatchedString {
                    content: spec.diff.clone(),
                    matched_ranges: spec.diff_ranges.iter().map(range).collect(),
                }
            }),
            refs: Vec::new(),
            source_refs: Vec::new(),
            diff: Vec::new(),
            modified_files: Vec::new(),
        })),
        "repo" => Some(Match::Repo(
            sg_service_frontend::search_stream::convert::RepoMatch {
                name: spec.repo_name.clone(),
                id: spec.repo_id,
                rev: String::new(),
                description_matches: Vec::new(),
                repo_name_matches: Vec::new(),
            },
        )),
        _ => None,
    }
}

fn event_from(spec: &EventSpec) -> SearchEvent {
    SearchEvent {
        results: spec.results.iter().filter_map(build_match).collect(),
        stats: Default::default(),
    }
}

fn result_out(m: &Match) -> Value {
    match m {
        Match::File(fm) => json!({
            "kind": "file",
            "path": fm.file.path,
            "chunks": fm.chunk_matches.iter().map(|chunk| json!({
                "content": chunk.content,
                "start": chunk.content_start.offset,
                "ranges": chunk.ranges.iter().map(|r| json!({"start": r.start.offset, "end": r.end.offset})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }),
        Match::Commit(cm) => {
            let (content, ranges) = match &cm.diff_preview {
                Some(preview) => (
                    preview.content.clone(),
                    preview
                        .matched_ranges
                        .iter()
                        .map(|r| json!({"start": r.start.offset, "end": r.end.offset}))
                        .collect::<Vec<_>>(),
                ),
                None => (String::new(), Vec::new()),
            };
            json!({"kind": "commit", "content": content, "ranges": ranges})
        }
        _ => json!({"kind": "other"}),
    }
}

fn event_out(event: &SearchEvent) -> Value {
    json!({
        "results": event.results.iter().map(result_out).collect::<Vec<_>>(),
        "stats": {"is_limit_hit": event.stats.is_limit_hit},
    })
}

fn base(case: &Case) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    map.insert("name".to_owned(), json!(case.name));
    map.insert("kind".to_owned(), json!(case.kind));
    map
}

fn run_structural(case: &Case) -> Value {
    let db = NoDb;
    let streamer = NoStreamer;
    let clients = RuntimeClients {
        db: &db,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
        commit: None,
        repo_pager: None,
    };
    let mut job = StructuralSearchJob {
        pattern_info: TextPatternInfo::default(),
        use_full_deadline: false,
        batch_retry: case.batch_retry,
        unindexed: (0..case.unindexed_repos)
            .map(|i| sg_service_frontend::search_leaf_jobs::RepositoryRevisions {
                repo: MinimalRepo {
                    id: i as i32 + 1,
                    name: "repo".to_owned(),
                },
                revs: vec!["HEAD".to_owned()],
            })
            .collect(),
        indexed: None,
    };
    let ctx = JobContext {
        deadline: None,
        ..JobContext::default()
    };
    let mut events = Vec::new();
    let error = job
        .run(&ctx, &clients, &mut |event| events.push(event))
        .is_err();
    let mut map = base(case);
    map.insert(
        "events".to_owned(),
        Value::Array(events.iter().map(event_out).collect()),
    );
    map.insert("error".to_owned(), json!(error));
    Value::Object(map)
}

fn run_filter(case: &Case) -> Value {
    let db = FakeUserDb;
    let git = FakeGit;
    let lister = NoDb;
    let streamer = NoStreamer;
    let counts = case
        .contributors
        .iter()
        .map(|(path, specs)| {
            (
                path.clone(),
                specs
                    .iter()
                    .map(|spec| ContributorCount {
                        name: spec.name.clone(),
                        email: spec.email.clone(),
                        count: spec.count,
                    })
                    .collect(),
            )
        })
        .collect();
    let fetcher = Arc::new(FakeContributors {
        counts,
        errors: case.fetch_errors.clone(),
    });
    let leaf = LeafJobClients {
        contributors: Some(fetcher.clone()),
        content_searcher: None,
        ownership: None,
    };
    let commit_clients = CommitRuntimeClients {
        db: &db,
        gitserver: &git,
        leaf: Some(&leaf),
    };
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
        commit: Some(&commit_clients),
        repo_pager: None,
    };

    let static_event = event_from(&case.event);
    let child_error = case
        .child_error
        .then(|| comb::JobFailure::Message("child failed".to_owned()));
    let child = comb::StaticJob::new(vec![static_event], child_error);

    let mut job: Box<dyn comb::CombinatorJob> = match case.kind.as_str() {
        "contains" => Box::new(FileContainsLeafJob {
            include_patterns: case.include.clone(),
            original_patterns: case.original.clone(),
            raw_include_patterns: case.include.clone(),
            case_sensitive: true,
            searcher: None,
            child: Box::new(child),
        }),
        "contributor" => Box::new(FileHasContributorsLeafJob {
            include: case.include.clone(),
            exclude: case.exclude.clone(),
            child: Box::new(child),
        }),
        other => panic!("unknown filter kind {other}"),
    };

    let ctx = comb::RunContext::default();
    let mut events = Vec::new();
    let (_, failure) = job.run(&ctx, &clients, &mut |event| events.push(event));
    let mut map = base(case);
    map.insert(
        "events".to_owned(),
        Value::Array(events.iter().map(event_out).collect()),
    );
    map.insert("error".to_owned(), json!(failure.is_some()));
    Value::Object(map)
}

#[test]
fn m937b_leaf_diff() {
    if std::env::var("SG_M937B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M937B_CORPUS").expect("M937B_CORPUS");
    let out_path = std::env::var("M937B_OUT").expect("M937B_OUT");
    let raw = std::fs::read_to_string(corpus).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "structural" => run_structural(case),
            "contains" | "contributor" => run_filter(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();
    std::fs::write(out_path, serde_json::to_vec(&out).unwrap()).expect("write output");
}
