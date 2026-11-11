//! M9.8hx differential harness: `TextSearchJob.Run` and `SymbolSearchJob.Run`
//! (`internal/search/searcher/search.go`, `symbol_search_job.go`) against
//! [`sg_service_frontend::search_searcher_jobs`].
//!
//! Run with `SG_M9HX_DIFF=1 M9HX_CORPUS=<json> M9HX_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
    SearchJob, Verbosity,
};
use sg_service_frontend::search_searcher_client::{
    protocol, SearcherClient, SearcherClients, SearcherError, SearcherErrorKind, SearcherGitserver,
    SearcherLimiter, SymbolsClient, SymbolsParameters, SymbolsResponse,
};
use sg_service_frontend::search_searcher_jobs::{
    RepositoryRevisions, SymbolSearchJob, SymbolSearchRequest, TextPatternInfo, TextSearchJob,
};
use sg_service_frontend::search_stream::convert::{self, Match};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;

fn default_commit() -> String {
    "0123456789abcdef0123456789abcdef01234567".to_owned()
}

#[derive(Debug, Deserialize, Default, Clone)]
struct QuerySpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    is_negated: bool,
    #[serde(default)]
    is_regexp: bool,
    #[serde(default)]
    boost: bool,
    #[serde(default)]
    children: Vec<QuerySpec>,
}

fn to_query_node(spec: &QuerySpec) -> protocol::QueryNode {
    match spec.kind.as_str() {
        "pattern" => protocol::QueryNode::Pattern(protocol::PatternNode {
            value: spec.value.clone(),
            is_negated: spec.is_negated,
            is_regexp: spec.is_regexp,
            boost: spec.boost,
        }),
        "and" => protocol::QueryNode::And(spec.children.iter().map(to_query_node).collect()),
        "or" => protocol::QueryNode::Or(spec.children.iter().map(to_query_node).collect()),
        other => panic!("unknown query kind {other}"),
    }
}

#[derive(Debug, Deserialize, Default)]
struct PatternInfoSpec {
    #[serde(default)]
    query: Option<QuerySpec>,
    #[serde(default)]
    is_structural_pat: bool,
    #[serde(default)]
    comby_rule: String,
    #[serde(default)]
    is_case_sensitive: bool,
    #[serde(default)]
    file_match_limit: i32,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    include_paths: Vec<String>,
    #[serde(default)]
    exclude_paths: String,
    #[serde(default)]
    include_langs: Vec<String>,
    #[serde(default)]
    exclude_langs: Vec<String>,
    #[serde(default)]
    path_patterns_are_case_sensitive: bool,
    #[serde(default)]
    pattern_matches_content: bool,
    #[serde(default)]
    pattern_matches_path: bool,
    #[serde(default)]
    languages: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
    #[serde(default)]
    resolve_commit: String,
    #[serde(default)]
    resolve_error: String,
    #[serde(default)]
    clone_in_progress: bool,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct LocationSpec {
    #[serde(default)]
    offset: i32,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    column: i32,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct FileMatchSpec {
    #[serde(default)]
    path: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct SearcherSpec {
    #[serde(default)]
    endpoint_error: bool,
    #[serde(default)]
    endpoint_count: usize,
    #[serde(default)]
    done_limit_hit: bool,
    #[serde(default)]
    matches: Vec<FileMatchSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct SymbolRequestSpec {
    #[serde(default)]
    regexp_pattern: String,
    #[serde(default)]
    is_case_sensitive: bool,
    #[serde(default)]
    include_patterns: Vec<String>,
    #[serde(default)]
    exclude_pattern: String,
    #[serde(default)]
    include_langs: Vec<String>,
    #[serde(default)]
    exclude_langs: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct SymbolSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    character: i32,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

#[derive(Debug, Deserialize, Default)]
struct SymbolsSpec {
    #[serde(default)]
    error: String,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    symbols: Vec<SymbolSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    job: String,
    #[serde(default)]
    verbosity: String,
    #[serde(default)]
    pattern_info: PatternInfoSpec,
    #[serde(default)]
    path_regexps: Vec<String>,
    #[serde(default)]
    indexed: bool,
    #[serde(default)]
    num_context_lines: i32,
    #[serde(default)]
    use_full_deadline: bool,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    searcher: SearcherSpec,
    #[serde(default)]
    symbol_request: SymbolRequestSpec,
    #[serde(default)]
    limit: i32,
    #[serde(default)]
    symbols: SymbolsSpec,
}

fn pattern_info(spec: &PatternInfoSpec) -> TextPatternInfo {
    TextPatternInfo {
        query: spec.query.as_ref().map(to_query_node),
        is_structural_pat: spec.is_structural_pat,
        comby_rule: spec.comby_rule.clone(),
        is_case_sensitive: spec.is_case_sensitive,
        file_match_limit: spec.file_match_limit,
        select: spec.select.clone(),
        include_paths: spec.include_paths.clone(),
        exclude_paths: spec.exclude_paths.clone(),
        include_langs: spec.include_langs.clone(),
        exclude_langs: spec.exclude_langs.clone(),
        path_patterns_are_case_sensitive: spec.path_patterns_are_case_sensitive,
        pattern_matches_content: spec.pattern_matches_content,
        pattern_matches_path: spec.pattern_matches_path,
        languages: spec.languages.clone(),
    }
}

fn repos(case: &Case) -> Vec<RepositoryRevisions> {
    case.repos
        .iter()
        .map(|spec| RepositoryRevisions {
            repo: convert::MinimalRepo {
                id: spec.id,
                name: spec.name.clone(),
            },
            revs: spec.revs.clone(),
        })
        .collect()
}

fn text_job(case: &Case) -> TextSearchJob {
    TextSearchJob {
        pattern_info: pattern_info(&case.pattern_info),
        repos: repos(case),
        path_regexps: case
            .path_regexps
            .iter()
            .map(|pattern| regex::Regex::new(pattern).expect("valid path regexp"))
            .collect(),
        indexed: case.indexed,
        num_context_lines: case.num_context_lines,
        use_full_deadline: case.use_full_deadline,
    }
}

fn symbol_job(case: &Case) -> SymbolSearchJob {
    SymbolSearchJob {
        request: SymbolSearchRequest {
            regexp_pattern: case.symbol_request.regexp_pattern.clone(),
            is_case_sensitive: case.symbol_request.is_case_sensitive,
            include_patterns: case.symbol_request.include_patterns.clone(),
            exclude_pattern: case.symbol_request.exclude_pattern.clone(),
            include_langs: case.symbol_request.include_langs.clone(),
            exclude_langs: case.symbol_request.exclude_langs.clone(),
        },
        repos: repos(case),
        limit: case.limit,
    }
}

fn verbosity(name: &str) -> Verbosity {
    match name {
        "basic" => Verbosity::Basic,
        "max" => Verbosity::Max,
        _ => Verbosity::None,
    }
}

fn deadline(kind: &str) -> Option<Instant> {
    match kind {
        "expired" => Some(Instant::now() - Duration::from_secs(1)),
        "future" => Some(Instant::now() + Duration::from_secs(30)),
        _ => None,
    }
}

struct FakeGit {
    repos: Vec<RepoSpec>,
}

impl FakeGit {
    fn spec(&self, repo: &str) -> &RepoSpec {
        self.repos
            .iter()
            .find(|spec| spec.name == repo)
            .unwrap_or_else(|| panic!("unexpected repo {repo}"))
    }
}

impl SearcherGitserver for FakeGit {
    fn resolve_revision(&self, repo: &str, revspec: &str) -> Result<String, SearcherError> {
        let spec = self.spec(repo);
        match spec.resolve_error.as_str() {
            "" | "none" => Ok(if spec.resolve_commit.is_empty() {
                default_commit()
            } else {
                spec.resolve_commit.clone()
            }),
            "other" => Err(SearcherError::other("resolve failed")),
            "revision_not_found" => Err(SearcherError::new(
                SearcherErrorKind::RevisionNotFound,
                format!("revision not found: {repo}@{revspec}"),
            )),
            "repo_not_found" => Err(SearcherError::new(
                SearcherErrorKind::RepoNotFound {
                    clone_in_progress: spec.clone_in_progress,
                },
                if spec.clone_in_progress {
                    format!("repository does not exist (clone in progress): {repo}")
                } else {
                    format!("repository does not exist: {repo}")
                },
            )),
            "timeout" => Err(SearcherError::new(
                SearcherErrorKind::Timeout,
                "resolve timed out",
            )),
            "temporary" => Err(SearcherError::new(
                SearcherErrorKind::Temporary,
                "resolve temporary",
            )),
            "not_found" => Err(SearcherError::new(SearcherErrorKind::NotFound, "not found")),
            other => panic!("unknown resolve_error {other}"),
        }
    }

    fn default_branch(&self, _repo: &str) -> Result<String, SearcherError> {
        Ok("refs/heads/main".to_owned())
    }
}

struct FakeSearcher {
    endpoint_error: bool,
    endpoint_count: usize,
    matches: Vec<protocol::FileMatch>,
    done_limit_hit: bool,
    requests: RefCell<Vec<protocol::Request>>,
}

impl FakeSearcher {
    fn new(case: &Case) -> Self {
        Self {
            endpoint_error: case.searcher.endpoint_error,
            endpoint_count: case.searcher.endpoint_count.max(1),
            matches: case.searcher.matches.iter().map(to_file_match).collect(),
            done_limit_hit: case.searcher.done_limit_hit,
            requests: RefCell::new(Vec::new()),
        }
    }
}

fn to_file_match(spec: &FileMatchSpec) -> protocol::FileMatch {
    protocol::FileMatch {
        path: spec.path.clone(),
        language: spec.language.clone(),
        limit_hit: spec.limit_hit,
        chunk_matches: spec
            .chunk_matches
            .iter()
            .map(|chunk| protocol::ChunkMatch {
                content: chunk.content.clone(),
                content_start: protocol::Location {
                    offset: chunk.content_start.offset,
                    line: chunk.content_start.line,
                    column: chunk.content_start.column,
                },
                ranges: chunk
                    .ranges
                    .iter()
                    .map(|range| protocol::Range {
                        start: protocol::Location {
                            offset: range.start.offset,
                            line: range.start.line,
                            column: range.start.column,
                        },
                        end: protocol::Location {
                            offset: range.end.offset,
                            line: range.end.line,
                            column: range.end.column,
                        },
                    })
                    .collect(),
            })
            .collect(),
    }
}

impl SearcherClient for FakeSearcher {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        if self.endpoint_error {
            return Err(SearcherError::other("endpoints failed"));
        }
        Ok(vec!["http://searcher".to_owned(); self.endpoint_count])
    }

    fn search(
        &self,
        request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        self.requests.borrow_mut().push(request.clone());
        for file_match in &self.matches {
            on_match(file_match.clone());
        }
        Ok(self.done_limit_hit)
    }
}

struct FakeLimiter {
    set_limit: Cell<Option<usize>>,
}

impl FakeLimiter {
    fn cap(&self) -> usize {
        self.set_limit.get().unwrap_or(32)
    }
}

impl SearcherLimiter for FakeLimiter {
    fn set_limit(&self, limit: usize) {
        self.set_limit.set(Some(limit));
    }

    fn acquire(&self) -> Result<(), SearcherError> {
        Ok(())
    }
}

struct FakeSymbols {
    response: Result<(Vec<convert::Symbol>, bool), SearcherError>,
    requests: RefCell<Vec<SymbolsParameters>>,
}

impl FakeSymbols {
    fn new(case: &Case) -> Self {
        let response = if case.symbols.error.is_empty() {
            let symbols = case
                .symbols
                .symbols
                .iter()
                .map(|spec| convert::Symbol {
                    name: spec.name.clone(),
                    path: spec.path.clone(),
                    line: spec.line,
                    character: spec.character,
                    kind: spec.kind.clone(),
                    language: spec.language.clone(),
                    parent: spec.parent.clone(),
                    parent_kind: spec.parent_kind.clone(),
                })
                .collect();
            Ok((symbols, case.symbols.limit_hit))
        } else {
            Err(SearcherError::other(case.symbols.error.clone()))
        };
        Self {
            response,
            requests: RefCell::new(Vec::new()),
        }
    }
}

impl SymbolsClient for FakeSymbols {
    fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        self.requests.borrow_mut().push(params.clone());
        match &self.response {
            Ok((symbols, limit_hit)) => Ok(SymbolsResponse {
                symbols: symbols.clone(),
                limit_hit: *limit_hit,
                file_limited: vec![false; symbols.len()],
            }),
            Err(error) => Err(error.clone()),
        }
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

#[test]
fn searcher_jobs_diff() {
    if std::env::var("SG_M9HX_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HX_CORPUS").expect("M9HX_CORPUS must be set when SG_M9HX_DIFF=1");
    let out_path = std::env::var("M9HX_OUT").expect("M9HX_OUT must be set when SG_M9HX_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_case(case: &Case) -> Value {
    match case.kind.as_str() {
        "describe" => run_describe(case),
        "run" => run_job(case),
        other => panic!("unknown case kind {other}"),
    }
}

fn run_describe(case: &Case) -> Value {
    let (name_value, children_count, attributes) = match case.job.as_str() {
        "text" => {
            let job = text_job(case);
            describe(&job, case)
        }
        "symbol" => {
            let job = symbol_job(case);
            describe(&job, case)
        }
        other => panic!("unknown job {other}"),
    };
    json!({
        "name": case.name,
        "kind": case.kind,
        "job": case.job,
        "name_value": name_value,
        "children_count": children_count,
        "map_children_same": true,
        "attributes": attributes,
    })
}

fn describe<J: SearchJob>(job: &J, case: &Case) -> (String, usize, Vec<[String; 2]>) {
    let mapped = job.map_children(&|_| unreachable!("childless job must not map"));
    assert!(std::ptr::eq(mapped, job));
    let attributes = job
        .attributes(verbosity(&case.verbosity))
        .into_iter()
        .map(|(key, value)| [key, value])
        .collect();
    (job.name().to_owned(), job.children().len(), attributes)
}

fn run_job(case: &Case) -> Value {
    let git = FakeGit {
        repos: case.repos.clone(),
    };
    let searcher = FakeSearcher::new(case);
    let limiter = FakeLimiter {
        set_limit: Cell::new(None),
    };
    let symbols = FakeSymbols::new(case);
    let searcher_clients = SearcherClients {
        client: &searcher,
        limiter: &limiter,
        gitserver: &git,
        symbols: &symbols,
    };
    let lister = NoopLister;
    let streamer = NoopStreamer;
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
        searcher: Some(&searcher_clients),
        commit: None,
    };
    let ctx = JobContext {
        deadline: deadline(&case.deadline),
    };

    let mut events: Vec<SearchEvent> = Vec::new();
    let mut sink = |event: SearchEvent| events.push(event);
    let outcome = match case.job.as_str() {
        "text" => {
            let mut job = text_job(case);
            job.run(&ctx, &clients, &mut sink)
        }
        "symbol" => {
            let mut job = symbol_job(case);
            job.run(&ctx, &clients, &mut sink)
        }
        other => panic!("unknown job {other}"),
    };

    let mut event_values: Vec<Value> = events.iter().map(event_json).collect();
    sort_values(&mut event_values);

    let (requests, symbols_requests, limiter_cap) = match case.job.as_str() {
        "text" => {
            let mut requests: Vec<Value> = searcher
                .requests
                .borrow()
                .iter()
                .map(request_json)
                .collect();
            sort_values(&mut requests);
            (json!(requests), Value::Null, json!(limiter.cap()))
        }
        "symbol" => {
            let mut requests: Vec<Value> = symbols
                .requests
                .borrow()
                .iter()
                .map(symbol_request_json)
                .collect();
            sort_values(&mut requests);
            (Value::Null, json!(requests), Value::Null)
        }
        other => panic!("unknown job {other}"),
    };

    json!({
        "name": case.name,
        "kind": case.kind,
        "job": case.job,
        "error": match &outcome {
            Ok(()) => String::new(),
            Err(err) => err.to_string(),
        },
        "events": event_values,
        "requests": requests,
        "symbols_requests": symbols_requests,
        "limiter_cap": limiter_cap,
    })
}

fn sort_values(values: &mut [Value]) {
    values.sort_by_key(|value| serde_json::to_string(value).expect("encode value"));
}

fn fetch_kind(duration: Duration) -> &'static str {
    match duration {
        duration if duration == Duration::from_millis(500) => "fixed_500ms",
        duration if duration == Duration::from_secs(60) => "one_minute",
        _ => "deadline",
    }
}

fn request_json(request: &protocol::Request) -> Value {
    let info = &request.pattern_info;
    json!({
        "repo": request.repo,
        "repo_id": request.repo_id,
        "commit": request.commit,
        "branch": request.branch,
        "indexed": request.indexed,
        "num_context_lines": request.num_context_lines,
        "fetch_timeout": fetch_kind(request.fetch_timeout),
        "pattern_info": {
            "query": info.query.as_ref().map(ToString::to_string),
            "is_structural": info.is_structural,
            "is_case_sensitive": info.is_case_sensitive,
            "exclude_paths": info.exclude_paths,
            "include_paths": info.include_paths,
            "include_langs": info.include_langs,
            "exclude_langs": info.exclude_langs,
            "path_patterns_are_case_sensitive": info.path_patterns_are_case_sensitive,
            "limit": info.limit,
            "pattern_matches_content": info.pattern_matches_content,
            "pattern_matches_path": info.pattern_matches_path,
            "comby_rule": info.comby_rule,
            "select": info.select,
            "languages": info.languages,
        },
    })
}

fn symbol_request_json(params: &SymbolsParameters) -> Value {
    json!({
        "repo": params.repo,
        "commit_id": params.commit_id,
        "query": params.query,
        "is_reg_exp": params.is_regexp,
        "is_case_sensitive": params.is_case_sensitive,
        "include_patterns": params.include_patterns,
        "exclude_pattern": params.exclude_pattern,
        "include_langs": params.include_langs,
        "exclude_langs": params.exclude_langs,
        "first": params.first,
    })
}

fn event_json(event: &SearchEvent) -> Value {
    let results: Vec<Value> = event.results.iter().map(match_json).collect();
    let status: Vec<[i32; 2]> = event
        .stats
        .status
        .entries()
        .into_iter()
        .map(|(id, mask)| [id, i32::from(mask)])
        .collect();
    json!({
        "results": results,
        "stats": {
            "is_limit_hit": event.stats.is_limit_hit,
            "backends_missing": event.stats.backends_missing,
            "status": status,
        },
    })
}

fn match_json(match_: &Match) -> Value {
    match match_ {
        Match::File(file_match) => json!({
            "kind": "file",
            "file": file_json(&file_match.file),
            "chunk_matches": file_match.chunk_matches.iter().map(chunk_json).collect::<Vec<Value>>(),
            "symbols": file_match.symbols.iter().map(symbol_json).collect::<Vec<Value>>(),
            "path_matches": ranges_json(&file_match.path_matches),
            "debug": file_match.debug,
        }),
        Match::Repo(repo) => json!({
            "kind": "repo",
            "name": repo.name,
            "id": repo.id,
        }),
        _ => json!({"kind": "unknown"}),
    }
}

fn file_json(file: &convert::FileRef) -> Value {
    json!({
        "repo": {"id": file.repo.id, "name": file.repo.name},
        "commit_id": file.commit_id,
        "path": file.path,
        "input_rev": file.input_rev.clone().unwrap_or_default(),
        "precise_language": file.precise_language,
    })
}

fn chunk_json(chunk: &convert::ChunkMatch) -> Value {
    json!({
        "content": chunk.content,
        "content_start": location_json(&chunk.content_start),
        "ranges": ranges_json(&chunk.ranges),
    })
}

fn symbol_json(symbol_match: &convert::SymbolMatch) -> Value {
    let symbol = &symbol_match.symbol;
    json!({
        "file": file_json(&symbol_match.file),
        "symbol": {
            "name": symbol.name,
            "path": symbol.path,
            "line": symbol.line,
            "character": symbol.character,
            "kind": symbol.kind,
            "language": symbol.language,
            "parent": symbol.parent,
            "parent_kind": symbol.parent_kind,
        },
    })
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
