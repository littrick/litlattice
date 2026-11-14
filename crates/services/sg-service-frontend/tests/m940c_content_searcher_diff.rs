//! M9.40c differential harness: `file:contains.content()` diff/commit
//! filtering with a wired `LeafJobClients.content_searcher`.
//!
//! Rust side drives [`FileContainsLeafJob`] (combinator leaf) with the
//! production-shaped `CommitContentSearcher` seam injected through
//! `LeafJobClients`. The Go oracle
//! (`internal/search/job/jobutil/zz_m940c_content_searcher_oracle_test.go`)
//! drives `NewFileContainsFilterJob` with `searcher.MockSearch`.
//!
//! Env: `SG_M940C_DIFF=1`, `M940C_CORPUS`, `M940C_OUT`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_commit_job::{CommitRuntimeClients, CommitUserDB, GitserverClient};
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, RuntimeClients,
};
use sg_service_frontend::search_jobutil_combinators as comb;
use sg_service_frontend::search_jobutil_combinators::CombinatorJob;
use sg_service_frontend::search_jobutil_filters::{
    CommitContentSearcher, IncludeSearchRequest,
};
use sg_service_frontend::search_leaf_jobs::{FileContainsLeafJob, LeafJobClients};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MinimalRepo, Signature,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_zoekt::types::{SearchOptions, SearchResult};

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    original: Vec<String>,
    #[serde(default = "default_true")]
    case_sensitive: bool,
    #[serde(default)]
    event: EventSpec,
    #[serde(default)]
    searcher: HashMap<String, Vec<String>>,
    #[serde(default)]
    searcher_errors: Vec<String>,
    #[serde(default)]
    no_searcher: bool,
}

fn default_true() -> bool {
    true
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

/// `searcher.Search` double: records the request, returns the corpus paths or
/// an error for the patterns listed in `searcher_errors`.
struct FakeContentSearcher {
    results: HashMap<String, Vec<String>>,
    errors: Vec<String>,
    requests: Mutex<Vec<Value>>,
}

impl CommitContentSearcher for FakeContentSearcher {
    fn search_include(&self, request: &IncludeSearchRequest) -> Result<Vec<String>, JobError> {
        self.requests.lock().unwrap().push(json!({
            "commit": request.commit_id,
            "include_paths": request.include_paths,
            "is_case_sensitive": request.is_case_sensitive,
            "limit": request.file_match_limit,
            "pattern": request.pattern,
            "pattern_matches_content": true,
            "repo": request.repo_name,
            "repo_id": request.repo_id,
        }));
        if self.errors.iter().any(|pattern| pattern == &request.pattern) {
            return Err(JobError("search failed".to_owned()));
        }
        Ok(self
            .results
            .get(&request.pattern)
            .cloned()
            .unwrap_or_default())
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
            diff_preview: (!spec.diff.is_empty())
                .then(|| sg_service_frontend::search_stream::convert::MatchedString {
                    content: spec.diff.clone(),
                    matched_ranges: spec.diff_ranges.iter().map(range).collect(),
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

/// Mirror the job-tree construction: case-fold the compiled sources when the
/// query is case-insensitive, but keep the raw pattern + flag for the searcher
/// request (Go `filter_file_contains.go:39-72`, `:187-192`).
fn compiled_source(pattern: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        pattern.to_owned()
    } else {
        format!("(?i:{pattern})")
    }
}

fn run_case(case: &Case) -> Value {
    let db = FakeUserDb;
    let git = FakeGit;
    let lister = NoDb;
    let streamer = NoStreamer;

    let mut fake_searcher: Option<Arc<FakeContentSearcher>> = None;
    let leaf = if case.no_searcher {
        LeafJobClients::default()
    } else {
        let fake = Arc::new(FakeContentSearcher {
            results: case.searcher.clone(),
            errors: case.searcher_errors.clone(),
            requests: Mutex::new(Vec::new()),
        });
        fake_searcher = Some(Arc::clone(&fake));
        LeafJobClients {
            content_searcher: Some(fake),
            ..LeafJobClients::default()
        }
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

    let child = comb::StaticJob::new(vec![event_from(&case.event)], None);
    let mut job = FileContainsLeafJob {
        include_patterns: case
            .include
            .iter()
            .map(|pattern| compiled_source(pattern, case.case_sensitive))
            .collect(),
        original_patterns: case
            .original
            .iter()
            .map(|pattern| compiled_source(pattern, case.case_sensitive))
            .collect(),
        raw_include_patterns: case.include.clone(),
        case_sensitive: case.case_sensitive,
        searcher: None,
        child: Box::new(child),
    };

    let ctx = comb::RunContext::default();
    let mut events = Vec::new();
    let (_, failure) = job.run(&ctx, &clients, &mut |event| events.push(event));

    let requests: Vec<Value> = fake_searcher
        .map(|fake| fake.requests.lock().unwrap().clone())
        .unwrap_or_default();

    json!({
        "name": case.name,
        "kind": "contains",
        "events": events.iter().map(event_out).collect::<Vec<_>>(),
        "requests": requests,
        "error": failure.is_some(),
    })
}

#[test]
fn m940c_content_searcher_diff() {
    if std::env::var("SG_M940C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M940C_CORPUS").expect("M940C_CORPUS");
    let out_path = std::env::var("M940C_OUT").expect("M940C_OUT");
    let raw = std::fs::read_to_string(corpus).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let out: Vec<Value> = cases.iter().map(run_case).collect();
    std::fs::write(out_path, serde_json::to_vec(&out).unwrap()).expect("write output");
}
