//! M9.18b differential harness: run the `new_text_search_job` tree
//! (`repoPagerJob` → `setRepos` → `searcher.TextSearchJob`) through
//! [`sg_service_frontend::search_stream::TreeJobRunner`] over a fake repo
//! resolver + searcher transport and emit normalized JSON. The Go oracle is
//! `internal/search/job/jobutil/zz_m918b_oracle_test.go` (UNTRACKED, gated by
//! `SG_M918B_DIFF=1`), which runs the same corpus through
//! `jobutil.NewTextSearchJob` and `searcher.MockSearch`.
//!
//! Both sides read `test_ws/p9-go/data/search_execution/corpus.json` and emit
//! the normalized searcher request sequence and streamed events. No network:
//! the searcher transport is an injectable fixture recorder on both sides.
//!
//! Env: `SG_M918B_DIFF=1`, `M9B_CORPUS`, `M9B_OUT`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_db::repos::RepoListItem;
use sg_search_query::basic::Basic;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_service_frontend::search_client::{Features, Protocol};
use sg_service_frontend::search_contexts::{RequestContext, SearchContext};
use sg_service_frontend::search_env::SearcherGrpcConnectionCache;
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions,
};
use sg_service_frontend::search_jobutil_job_builders as jb;
use sg_service_frontend::search_jobutil_tree::{
    new_text_search_job, JobNode, TreeInputs, TreeRepoOptions,
};
use sg_service_frontend::search_repos::{
    GitserverError, RepoResolverDb, Resolver, ResolverGitserver, ResolverZoektLister,
    ResolverRepoPageSource,
};
use sg_service_frontend::search_runtime::SearcherRuntimeClients;
use sg_service_frontend::search_searcher_client::{
    protocol, SearcherClient, SearcherError, SearcherErrorKind, SearcherGitserver,
    SearcherLimiter, SymbolsClient, SymbolsParameters, SymbolsResponse,
};
use sg_service_frontend::search_stream::convert::{self, Match};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_service_frontend::search_stream::{
    JobRunner, RepoPagerRuntimeClients, TreeJobRunner, TreeRuntimeClients,
};
use sg_zoekt::types::{ListOptions, ListResponse, MinimalRepoListEntry, SearchOptions};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct UserSettingsSpec {
    #[serde(default)]
    search_include_forks: Option<bool>,
    #[serde(default)]
    search_include_archived: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct FeaturesSpec {
    #[serde(default)]
    content_based_lang_filters: bool,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    stars: i64,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct LocationSpec {
    #[serde(default)]
    offset: i32,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    column: i32,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
struct SearcherSpec {
    #[serde(default)]
    matches_by_repo: BTreeMap<String, Vec<FileMatchSpec>>,
    #[serde(default)]
    error_by_repo: BTreeMap<String, String>,
    #[serde(default)]
    limit_hit_by_repo: BTreeMap<String, bool>,
}

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    search_type: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    context_lines: i32,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    user_settings: UserSettingsSpec,
    #[serde(default)]
    features: FeaturesSpec,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    db_error: String,
    #[serde(default)]
    max_repos: Option<i64>,
    #[serde(default)]
    zoekt_indexed: Vec<u32>,
    /// `repo@rev` → `revision_not_found` | `other` | `timeout`.
    #[serde(default)]
    resolve_errors: BTreeMap<String, String>,
    #[serde(default)]
    searcher: SearcherSpec,
}

fn search_type(value: &str) -> SearchType {
    match value {
        "" | "regexp" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        other => panic!("unknown search type {other}"),
    }
}

fn protocol(value: &str) -> Protocol {
    match value {
        "" | "streaming" => Protocol::Streaming,
        "batch" => Protocol::Batch,
        "exhaustive" => Protocol::Exhaustive,
        other => panic!("unknown protocol {other}"),
    }
}

fn basic(case: &Case) -> Basic {
    let plan = pipeline_query(&case.query, search_type(&case.search_type))
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query));
    assert_eq!(plan.len(), 1, "{}: expected one basic query", case.name);
    plan.into_iter().next().unwrap()
}

fn inputs(case: &Case) -> TreeInputs {
    TreeInputs {
        protocol: protocol(&case.protocol),
        pattern_type: search_type(&case.search_type),
        user_settings: jb::UserSettings {
            search_include_forks: case.user_settings.search_include_forks,
            search_include_archived: case.user_settings.search_include_archived,
        },
        features: Features {
            content_based_lang_filters: case.features.content_based_lang_filters,
            ..Features::default()
        },
        context_lines: case.context_lines,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        original_query: case.original_query.clone(),
        request_context: sg_service_frontend::search_contexts::RequestContext::anonymous(),
        sub_repo_enabled: false,
        cody_context_hook: None,
        cody_context: None,
    }
}

fn tree(case: &Case) -> JobNode {
    let input = inputs(case);
    let parsed = basic(case);
    let result_types = jb::compute_result_types(&parsed, input.pattern_type, jb::DEFAULT_RESULT_TYPES);
    let repo_options =
        TreeRepoOptions::from_job_builders(&jb::to_repo_options(&parsed, &input.user_settings));
    new_text_search_job(&input, &parsed, result_types, repo_options).expect("build text job")
}

/// The commit/resolve convention shared with the Go oracle.
fn commit_for(repo: &str, rev: &str) -> String {
    format!("commit-{repo}-{rev}")
}

fn resolve_key(repo: &str, rev: &str) -> String {
    format!("{repo}@{rev}")
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

struct FakeDb {
    repos: Vec<RepoListItem>,
    error: Option<String>,
}

impl RepoResolverDb for FakeDb {
    fn resolve_search_context(
        &self,
        _spec: &str,
        _ctx: &RequestContext,
    ) -> Result<SearchContext, JobError> {
        Ok(SearchContext {
            name: "global".to_owned(),
            public: true,
            auto_defined: true,
            ..SearchContext::default()
        })
    }

    fn list_minimal_repos(
        &self,
        _options: &sg_db::repos::RepoListOptions,
    ) -> Result<Vec<RepoListItem>, JobError> {
        match &self.error {
            Some(error) => Err(JobError(error.clone())),
            None => Ok(self.repos.clone()),
        }
    }

    fn get_search_context_repository_revisions(
        &self,
        _id: i64,
    ) -> Result<Vec<sg_service_frontend::search_contexts::SearchContextRepositoryRevisions>, JobError>
    {
        Ok(Vec::new())
    }

    fn batch_get_repo_commit_changelist(
        &self,
        _repos: &[sg_db::repos::RepoChangelistIDs],
    ) -> Result<BTreeMap<u32, BTreeMap<i64, String>>, JobError> {
        Ok(BTreeMap::new())
    }
}

struct FakeGit {
    errors: BTreeMap<String, String>,
    calls: Mutex<Vec<String>>,
}

impl ResolverGitserver for FakeGit {
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, GitserverError> {
        let key = resolve_key(repo, rev);
        self.calls.lock().unwrap().push(format!("resolve:{key}"));
        match self.errors.get(&key).map(String::as_str) {
            Some("revision_not_found") => Err(GitserverError::Missing),
            Some("other") => Err(GitserverError::Fatal("resolve failed".to_owned())),
            _ => Ok(commit_for(repo, rev)),
        }
    }

    fn list_refs(&self, repo: &str) -> Result<Vec<String>, JobError> {
        self.calls.lock().unwrap().push(format!("refs:{repo}"));
        Ok(Vec::new())
    }

    fn commits_after(
        &self,
        repo: &str,
        rev: &str,
        _after: time::OffsetDateTime,
    ) -> Result<usize, GitserverError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("commits:{}", resolve_key(repo, rev)));
        Err(GitserverError::Missing)
    }
}

struct FakeResolverZoekt;

impl ResolverZoektLister for FakeResolverZoekt {
    fn list(
        &self,
        _query: &sg_zoekt::query::Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, JobError> {
        Ok(ListResponse {
            repos: Vec::new(),
            repos_map: BTreeMap::new(),
            crashes: 0,
            stats: Default::default(),
        })
    }
}

struct FakePagerZoekt {
    indexed: Vec<u32>,
}

impl sg_service_frontend::search_jobutil_repos::ZoektRepoLister for FakePagerZoekt {
    fn list(&self) -> Result<ListResponse, JobError> {
        let mut repos_map = BTreeMap::new();
        for id in &self.indexed {
            repos_map.insert(
                *id,
                MinimalRepoListEntry {
                    has_symbols: true,
                    branches: Vec::new(),
                    index_time_unix: 0,
                },
            );
        }
        Ok(ListResponse {
            repos: Vec::new(),
            repos_map,
            crashes: 0,
            stats: Default::default(),
        })
    }
}

struct FakeSearchGit {
    errors: BTreeMap<String, String>,
}

impl SearcherGitserver for FakeSearchGit {
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, SearcherError> {
        match self.errors.get(&resolve_key(repo, rev)).map(String::as_str) {
            Some("revision_not_found") => Err(SearcherError::new(
                SearcherErrorKind::RevisionNotFound,
                format!("revision not found: {repo}@{rev}"),
            )),
            Some("other") => Err(SearcherError::other("resolve failed")),
            _ => Ok(commit_for(repo, rev)),
        }
    }

    fn default_branch(&self, _repo: &str) -> Result<String, SearcherError> {
        Ok("refs/heads/main".to_owned())
    }
}

struct FakeSearcherClient {
    matches_by_repo: BTreeMap<String, Vec<protocol::FileMatch>>,
    error_by_repo: BTreeMap<String, String>,
    limit_hit_by_repo: BTreeMap<String, bool>,
    requests: Mutex<Vec<protocol::Request>>,
}

impl SearcherClient for FakeSearcherClient {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        Ok(vec!["http://searcher".to_owned()])
    }

    fn search(
        &self,
        request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        self.requests.lock().unwrap().push(request.clone());
        if let Some(error) = self.error_by_repo.get(&request.repo) {
            return Err(SearcherError::other(error.clone()));
        }
        if let Some(matches) = self.matches_by_repo.get(&request.repo) {
            for file_match in matches {
                on_match(file_match.clone());
            }
        }
        Ok(self
            .limit_hit_by_repo
            .get(&request.repo)
            .copied()
            .unwrap_or(false))
    }
}

struct NoopLimiter;

impl SearcherLimiter for NoopLimiter {
    fn set_limit(&self, _limit: usize) {}
    fn acquire(&self) -> Result<(), SearcherError> {
        Ok(())
    }
}

struct NoopSymbols;

impl SymbolsClient for NoopSymbols {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Err(SearcherError::other("symbols not used by the text search job"))
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
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

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

fn file_json(file: &convert::FileRef) -> Value {
    json!({
        "repo": {"id": file.repo.id, "name": file.repo.name},
        "commit_id": file.commit_id,
        "path": file.path,
        "input_rev": file.input_rev.clone().unwrap_or_default(),
        "precise_language": file.precise_language,
    })
}

fn match_json(match_: &Match) -> Value {
    match match_ {
        Match::File(file_match) => json!({
            "kind": "file",
            "file": file_json(&file_match.file),
            "chunk_matches": file_match.chunk_matches.iter().map(|chunk| json!({
                "content": chunk.content,
                "content_start": location_json(&chunk.content_start),
                "ranges": ranges_json(&chunk.ranges),
            })).collect::<Vec<Value>>(),
            "symbols": file_match.symbols.iter().map(|symbol_match| json!({
                "file": file_json(&symbol_match.file),
                "symbol": {
                    "name": symbol_match.symbol.name,
                    "path": symbol_match.symbol.path,
                    "line": symbol_match.symbol.line,
                    "character": symbol_match.symbol.character,
                    "kind": symbol_match.symbol.kind,
                    "language": symbol_match.symbol.language,
                    "parent": symbol_match.symbol.parent,
                    "parent_kind": symbol_match.symbol.parent_kind,
                },
            })).collect::<Vec<Value>>(),
            "path_matches": ranges_json(&file_match.path_matches),
            "debug": file_match.debug,
        }),
        Match::Repo(repo) => json!({"kind": "repo", "name": repo.name, "id": repo.id}),
        _ => json!({"kind": "unknown"}),
    }
}

fn event_json(event: &SearchEvent) -> Value {
    let status: Vec<[i32; 2]> = event
        .stats
        .status
        .entries()
        .into_iter()
        .map(|(id, mask)| [id, i32::from(mask)])
        .collect();
    json!({
        "results": event.results.iter().map(match_json).collect::<Vec<Value>>(),
        "stats": {
            "is_limit_hit": event.stats.is_limit_hit,
            "backends_missing": event.stats.backends_missing,
            "status": status,
        },
    })
}

fn sorted(mut values: Vec<Value>) -> Vec<Value> {
    values.sort_by_key(|value| serde_json::to_string(value).expect("encode value"));
    values
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

fn run_case(case: &Case) -> Value {
    let repos: Vec<RepoListItem> = case
        .repos
        .iter()
        .map(|repo| RepoListItem {
            id: repo.id,
            name: repo.name.clone(),
            stars: repo.stars,
            external_id: String::new(),
            external_service_type: String::new(),
            external_service_id: String::new(),
        })
        .collect();

    let db = Arc::new(FakeDb {
        repos,
        error: Some(case.db_error.clone()).filter(|error| !error.is_empty()),
    });
    let resolver_git = Arc::new(FakeGit {
        errors: case.resolve_errors.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let resolver = Arc::new(Resolver {
        db: db.clone(),
        gitserver: resolver_git,
        zoekt: Arc::new(FakeResolverZoekt),
        searcher: None,
        max_repos: case.max_repos,
    });

    let searcher_client = Arc::new(FakeSearcherClient {
        matches_by_repo: case
            .searcher
            .matches_by_repo
            .iter()
            .map(|(repo, matches)| (repo.clone(), matches.iter().map(to_file_match).collect()))
            .collect(),
        error_by_repo: case.searcher.error_by_repo.clone(),
        limit_hit_by_repo: case.searcher.limit_hit_by_repo.clone(),
        requests: Mutex::new(Vec::new()),
    });
    let searcher = Arc::new(SearcherRuntimeClients {
        client: searcher_client.clone(),
        limiter: Arc::new(NoopLimiter),
        gitserver: Arc::new(FakeSearchGit {
            errors: case.resolve_errors.clone(),
        }),
        symbols: Arc::new(NoopSymbols),
        searcher_connection_cache: Arc::new(SearcherGrpcConnectionCache::new()),
    });

    let repo_pager = Arc::new(RepoPagerRuntimeClients::new(
        Arc::new(ResolverRepoPageSource::new(resolver)),
        Arc::new(FakePagerZoekt {
            indexed: case.zoekt_indexed.clone(),
        }),
    ));

    let runtime = TreeRuntimeClients {
        db: Arc::new(NoopLister),
        zoekt: Arc::new(NoopStreamer),
        max_repos: case.max_repos,
        searcher: Some(searcher),
        commit: None,
        repo_pager: Some(repo_pager),
    };
    let runner = TreeJobRunner::with_clients(runtime);

    let mut job = tree(case);
    let mut events: Vec<SearchEvent> = Vec::new();
    let mut sink = |event: SearchEvent| events.push(event);
    let output = runner.run(&mut job, &mut sink);

    let requests = searcher_client
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(request_json)
        .collect::<Vec<_>>();

    json!({
        "name": case.name,
        "error": output.error.as_ref().map(|error| error.message.clone()).unwrap_or_default(),
        "alert": output.alert.is_some(),
        "requests": sorted(requests),
        "events": sorted(events.iter().map(event_json).collect::<Vec<_>>()),
    })
}

#[test]
fn search_execution_diff() {
    if std::env::var("SG_M918B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9B_CORPUS").expect("M9B_CORPUS must be set");
    let out_path = std::env::var("M9B_OUT").expect("M9B_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}
