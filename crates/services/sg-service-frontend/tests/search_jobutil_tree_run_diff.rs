//! M9.8hw differential harness: run the constructed job tree for the indexed
//! (zoekt) path against the Go oracle
//! (`internal/search/job/jobutil/zz_m9hw_oracle_test.go`).
//!
//! Both sides read the same corpus and emit byte-identical normalized JSON.
//!
//! Env:
//!
//! * `SG_M9HW_DIFF=1` enable the harness
//! * `M9HW_CORPUS` corpus JSON
//! * `M9HW_OUT` output JSON

use std::cell::RefCell;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
    SearchJob,
};
use sg_service_frontend::search_jobutil_tree::{JobNode, TreeRepoOptions};
use sg_service_frontend::search_stream::convert::Match;
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_service_frontend::search_stream::zoekt_search::{SelectPath, ZoektParameters};
use sg_zoekt::global_query::{GlobalZoektQuery, RepoOptions, RepoVisibility};
use sg_zoekt::indexed_repo_revs::{
    IndexedRepoRevs, MinimalRepo as RepoMinimal, RepositoryRevisions, RepositoryRevisionsRepo,
};
use sg_zoekt::query::{IndexedRequestType, Query};
use sg_zoekt::types::{
    ChunkMatch, FileMatch, Location as ZoektLocation, MinimalRepoListEntry, Range as ZoektRange,
    RepositoryBranch, SearchOptions, SearchResult as ZoektSearchResult, Stats as ZoektStats,
    SymbolInfo,
};

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    /// `null` means nil `*IndexedRepoRevs`; `[]` means non-nil but empty.
    #[serde(default)]
    repos: Option<Vec<RepoSpec>>,
    #[serde(default)]
    query: QuerySpec,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    file_match_limit: i32,
    #[serde(default)]
    path_regexps: Vec<String>,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    batches: Vec<ResultSpec>,
    #[serde(default)]
    stream_error: bool,
    #[serde(default)]
    global_query: GlobalQuerySpec,
    #[serde(default)]
    repo_opts: RepoOptsSpec,
    #[serde(default)]
    private_repos: Vec<RepoRef>,
    #[serde(default)]
    private_repos_error: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    timeout_nanos: i64,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct QuerySpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    value: bool,
    #[serde(default)]
    exact: bool,
}

#[derive(Debug, Deserialize, Default)]
struct GlobalQuerySpec {
    #[serde(default)]
    query: QuerySpec,
    #[serde(default)]
    scope: Option<QuerySpec>,
    #[serde(default)]
    include_private: bool,
}

#[derive(Debug, Deserialize, Default)]
struct RepoOptsSpec {
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    minus_repo_filters: Vec<String>,
    #[serde(default)]
    only_archived: bool,
    #[serde(default)]
    no_archived: bool,
    #[serde(default)]
    only_forks: bool,
    #[serde(default)]
    no_forks: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize, Default)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
    #[serde(default)]
    branches: Vec<BranchSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct BranchSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

#[derive(Debug, Deserialize, Default)]
struct ResultSpec {
    #[serde(default)]
    files: Vec<FileSpec>,
    #[serde(default)]
    stats: StatsSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatsSpec {
    #[serde(default)]
    file_count: i64,
    #[serde(default)]
    match_count: i64,
    #[serde(default)]
    files_skipped: i64,
    #[serde(default)]
    shards_skipped: i64,
    #[serde(default)]
    crashes: i64,
}

#[derive(Debug, Deserialize, Default)]
struct FileSpec {
    #[serde(default)]
    file_name: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    repository_id: u32,
    #[serde(default)]
    version: String,
    #[serde(default)]
    branches: Vec<String>,
    #[serde(default)]
    language: String,
    #[serde(default)]
    debug: String,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
}

#[derive(Debug, Deserialize, Default)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
    #[serde(default)]
    symbol_info: Vec<Option<SymbolSpec>>,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
struct LocationSpec {
    #[serde(default)]
    byte_offset: u32,
    #[serde(default)]
    line_number: u32,
    #[serde(default)]
    column: u32,
}

#[derive(Debug, Deserialize, Default)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Debug, Deserialize, Default)]
struct SymbolSpec {
    #[serde(default)]
    sym: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

impl LocationSpec {
    fn to_zoekt(self) -> ZoektLocation {
        ZoektLocation {
            byte_offset: self.byte_offset,
            line_number: self.line_number,
            column: self.column,
        }
    }
}

fn to_query(spec: &QuerySpec) -> Query {
    match spec.kind.as_str() {
        "const" => Query::Const(spec.value),
        "substring" => Query::Substring {
            pattern: spec.pattern.clone(),
            case_sensitive: false,
            file_name: false,
            content: true,
        },
        "branch" => Query::Branch {
            pattern: spec.pattern.clone(),
            exact: spec.exact,
        },
        other => panic!("unknown query kind {other}"),
    }
}

fn to_file(spec: &FileSpec) -> FileMatch {
    FileMatch {
        file_name: spec.file_name.as_bytes().to_vec(),
        repository: spec.repository.clone(),
        repository_id: spec.repository_id,
        version: spec.version.clone(),
        branches: spec.branches.clone(),
        language: spec.language.clone(),
        debug: spec.debug.clone(),
        chunk_matches: spec.chunk_matches.iter().map(to_chunk).collect(),
        ..FileMatch::default()
    }
}

fn to_chunk(spec: &ChunkSpec) -> ChunkMatch {
    ChunkMatch {
        content: spec.content.as_bytes().to_vec(),
        content_start: spec.content_start.to_zoekt(),
        file_name: spec.file_name,
        ranges: spec
            .ranges
            .iter()
            .map(|range| ZoektRange {
                start: range.start.to_zoekt(),
                end: range.end.to_zoekt(),
            })
            .collect(),
        symbol_info: spec
            .symbol_info
            .iter()
            .map(|symbol| {
                symbol.as_ref().map(|symbol| SymbolInfo {
                    sym: symbol.sym.clone(),
                    kind: symbol.kind.clone(),
                    parent: symbol.parent.clone(),
                    parent_kind: symbol.parent_kind.clone(),
                })
            })
            .collect(),
        score: 0.0,
    }
}

fn to_result(spec: &ResultSpec) -> ZoektSearchResult {
    ZoektSearchResult {
        stats: ZoektStats {
            file_count: spec.stats.file_count,
            match_count: spec.stats.match_count,
            files_skipped: spec.stats.files_skipped,
            shards_skipped: spec.stats.shards_skipped,
            crashes: spec.stats.crashes,
            ..ZoektStats::default()
        },
        files: spec.files.iter().map(to_file).collect(),
        ..ZoektSearchResult::default()
    }
}

fn to_indexed_repos(specs: &[RepoSpec]) -> IndexedRepoRevs {
    let mut indexed = IndexedRepoRevs::default();
    for spec in specs {
        indexed.add(
            &RepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: spec.id as u32,
                    name: spec.name.clone(),
                },
                revs: spec.revs.clone(),
            },
            MinimalRepoListEntry {
                branches: spec
                    .branches
                    .iter()
                    .map(|branch| RepositoryBranch {
                        name: branch.name.clone(),
                        version: branch.version.clone(),
                    })
                    .collect(),
                ..MinimalRepoListEntry::default()
            },
        );
    }
    indexed
}

fn repo_options(spec: &RepoOptsSpec) -> RepoOptions {
    RepoOptions {
        visibility: match spec.visibility.as_str() {
            "Any" => RepoVisibility::Any,
            "Private" => RepoVisibility::Private,
            "Public" => RepoVisibility::Public,
            other => RepoVisibility::Other(other.to_owned()),
        },
        minus_repo_filters: spec.minus_repo_filters.clone(),
        only_archived: spec.only_archived,
        no_archived: spec.no_archived,
        only_forks: spec.only_forks,
        no_forks: spec.no_forks,
    }
}

fn global_query(case: &Case) -> GlobalZoektQuery {
    GlobalZoektQuery::new(
        to_query(&case.global_query.query),
        case.global_query.scope.as_ref().map(to_query),
        case.global_query.include_private,
    )
}

fn params(case: &Case, typ: IndexedRequestType) -> ZoektParameters {
    ZoektParameters {
        typ,
        file_match_limit: case.file_match_limit,
        select: SelectPath(case.select.clone()),
        ..ZoektParameters::default()
    }
}

fn deadline(kind: &str) -> Option<Instant> {
    match kind {
        "expired" => Some(Instant::now() - Duration::from_secs(1)),
        "future" => Some(Instant::now() + Duration::from_secs(30)),
        _ => None,
    }
}

fn text_leaf(case: &Case) -> JobNode {
    JobNode::zoekt_repo_subset_text(
        case.repos.as_ref().map(|specs| to_indexed_repos(specs)),
        to_query(&case.query),
        case.path_regexps.clone(),
        params(case, IndexedRequestType::Text),
    )
}

fn symbol_leaf(case: &Case) -> JobNode {
    JobNode::zoekt_symbol(
        case.repos.as_ref().map(|specs| to_indexed_repos(specs)),
        to_query(&case.query),
        params(case, IndexedRequestType::Symbol),
    )
}

fn global_text_leaf(case: &Case) -> JobNode {
    JobNode::zoekt_global_text(
        global_query(case),
        params(case, IndexedRequestType::Text),
        repo_options(&case.repo_opts),
        case.path_regexps.clone(),
        TreeRepoOptions::default(),
    )
}

fn global_symbol_leaf(case: &Case) -> JobNode {
    JobNode::zoekt_global_symbol(
        global_query(case),
        params(case, IndexedRequestType::Symbol),
        repo_options(&case.repo_opts),
        TreeRepoOptions::default(),
    )
}

fn build_tree(case: &Case) -> JobNode {
    match case.kind.as_str() {
        "noop" => JobNode::noop(),
        "text" => text_leaf(case),
        "symbol" => symbol_leaf(case),
        "global_text" => global_text_leaf(case),
        "global_symbol" => global_symbol_leaf(case),
        "log_text" => JobNode::log(text_leaf(case)),
        "limit_text" => JobNode::limit(case.limit, text_leaf(case)),
        "timeout_text" => JobNode::timeout(case.timeout_nanos, text_leaf(case)),
        "log_limit_text" => JobNode::log(JobNode::limit(case.limit, text_leaf(case))),
        "sequential_text_symbol" => {
            JobNode::sequential(false, vec![text_leaf(case), symbol_leaf(case)])
        }
        other => panic!("unknown case kind {other}"),
    }
}

fn is_global(case: &Case) -> bool {
    case.kind.starts_with("global_")
}

struct FakeStreamer {
    batches: Vec<ZoektSearchResult>,
    error: bool,
    called: RefCell<bool>,
}

impl FakeStreamer {
    fn new(case: &Case) -> Self {
        Self {
            batches: case.batches.iter().map(to_result).collect(),
            error: case.stream_error,
            called: RefCell::new(false),
        }
    }

    fn called(&self) -> bool {
        *self.called.borrow()
    }
}

impl JobStreamer for FakeStreamer {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        sender: &mut dyn FnMut(ZoektSearchResult),
    ) -> Result<(), JobError> {
        *self.called.borrow_mut() = true;
        for batch in &self.batches {
            sender(batch.clone());
        }
        if self.error {
            return Err(JobError("stream failed".to_owned()));
        }
        Ok(())
    }
}

struct FakeLister {
    repos: Vec<RepoMinimal>,
    error: bool,
    options: RefCell<Option<ReposListOptions>>,
}

impl FakeLister {
    fn new(case: &Case) -> Self {
        Self {
            repos: case
                .private_repos
                .iter()
                .map(|repo| RepoMinimal {
                    id: repo.id as u32,
                    name: repo.name.clone(),
                })
                .collect(),
            error: case.private_repos_error,
            options: RefCell::new(None),
        }
    }
}

impl PrivateRepoLister for FakeLister {
    fn list_minimal_repos(&self, options: &ReposListOptions) -> Result<Vec<RepoMinimal>, JobError> {
        *self.options.borrow_mut() = Some(options.clone());
        if self.error {
            return Err(JobError("list failed".to_owned()));
        }
        Ok(self.repos.clone())
    }
}

fn lister_json(options: &ReposListOptions) -> Value {
    json!({
        "only_private": options.only_private,
        "only_forks": options.only_forks,
        "no_forks": options.no_forks,
        "only_archived": options.only_archived,
        "no_archived": options.no_archived,
        "exclude_pattern": options.exclude_pattern,
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

fn file_json(file: &sg_service_frontend::search_stream::convert::FileRef) -> Value {
    json!({
        "repo": {"id": file.repo.id, "name": file.repo.name},
        "commit_id": file.commit_id,
        "path": file.path,
        "input_rev": file.input_rev.clone().unwrap_or_default(),
        "precise_language": file.precise_language,
    })
}

fn chunk_json(chunk: &sg_service_frontend::search_stream::convert::ChunkMatch) -> Value {
    json!({
        "content": chunk.content,
        "content_start": location_json(&chunk.content_start),
        "ranges": ranges_json(&chunk.ranges),
    })
}

fn symbol_json(symbol_match: &sg_service_frontend::search_stream::convert::SymbolMatch) -> Value {
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

fn run_case(case: &Case) -> Value {
    let streamer = FakeStreamer::new(case);
    let lister = FakeLister::new(case);
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
    };
    let ctx = JobContext {
        deadline: deadline(&case.deadline),
    };

    let mut events: Vec<SearchEvent> = Vec::new();
    let mut sink = |event: SearchEvent| events.push(event);
    let mut tree = build_tree(case);
    let error = match tree.run(&ctx, &clients, &mut sink) {
        Ok(()) => String::new(),
        Err(err) => err.to_string(),
    };

    json!({
        "name": case.name,
        "error": error,
        "stream_called": streamer.called(),
        "events": events.iter().map(event_json).collect::<Vec<Value>>(),
        "lister": if is_global(case) {
            lister_json(lister.options.borrow().as_ref().expect("lister called"))
        } else {
            Value::Null
        },
    })
}

#[test]
fn search_jobutil_tree_run_diff() {
    if std::env::var("SG_M9HW_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9HW_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9HW_CORPUS").expect("M9HW_CORPUS must be set");
    let out_path = std::env::var("M9HW_OUT").expect("M9HW_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run_case).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
