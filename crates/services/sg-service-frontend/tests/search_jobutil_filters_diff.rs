//! M9.8hi differential harness: `internal/search/job/jobutil/`
//! `filter_file_contains.go`, `filter_file_contributor.go` and `select.go`
//! against [`sg_service_frontend::search_jobutil_filters`].
//!
//! Run with `SG_M9HI_DIFF=1 M9HI_CORPUS=<json> M9HI_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
    SearchJob, Verbosity,
};
use sg_service_frontend::search_jobutil_filters::{
    ContributorCount, ContributorFetcher, ContributorOptions, FilterFileContainsJob,
    FilterFileHasContributorsJob, PatternNode, SelectJob,
};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    RepoMatch, Signature, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::{SearchEvent, Stats};
use sg_zoekt::indexed_repo_revs::MinimalRepo as ZoektMinimalRepo;
use sg_zoekt::query::Query;
use sg_zoekt::types::{SearchOptions, SearchResult};

// ---------------------------------------------------------------------------
// corpus schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default, Clone)]
struct NodeSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    operands: Vec<NodeSpec>,
}

impl NodeSpec {
    fn to_node(&self) -> PatternNode {
        if self.kind == "operator" {
            PatternNode::Operator(self.operands.iter().map(NodeSpec::to_node).collect())
        } else {
            PatternNode::Pattern(self.value.clone())
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct LocationSpec {
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    line: i64,
    #[serde(default)]
    column: i64,
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
struct SymbolInfoSpec {
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

#[derive(Debug, Deserialize, Default, Clone)]
struct SymbolSpec {
    #[serde(default)]
    file: FileSpec,
    #[serde(default)]
    symbol: SymbolInfoSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct FileSpec {
    #[serde(default)]
    repo: RepoRef,
    #[serde(default)]
    commit_id: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    input_rev: Option<String>,
    #[serde(default)]
    precise_language: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct MatchedStringSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    matched_ranges: Vec<RangeSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct SignatureSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    date: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct CommitInfoSpec {
    #[serde(default)]
    id: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    author: SignatureSpec,
    #[serde(default)]
    committer: SignatureSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoMatchSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    description_matches: Vec<RangeSpec>,
    #[serde(default)]
    repo_name_matches: Vec<RangeSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ResultSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    file: Option<FileSpec>,
    #[serde(default)]
    chunk_matches: Vec<ChunkSpec>,
    #[serde(default)]
    symbols: Vec<SymbolSpec>,
    #[serde(default)]
    path_matches: Vec<RangeSpec>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    debug: Option<String>,
    #[serde(default)]
    repo_match: Option<RepoMatchSpec>,
    #[serde(default)]
    commit: Option<CommitInfoSpec>,
    #[serde(default)]
    repo: Option<RepoRef>,
    #[serde(default)]
    message_preview: Option<MatchedStringSpec>,
    #[serde(default)]
    diff_preview: Option<MatchedStringSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ContributorSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    count: i32,
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
    include_patterns: Vec<String>,
    #[serde(default)]
    original: Option<NodeSpec>,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    results: Vec<ResultSpec>,
    #[serde(default)]
    is_limit_hit: bool,
    #[serde(default)]
    content: String,
    #[serde(default)]
    matched_ranges: Vec<RangeSpec>,
    #[serde(default)]
    matched_file_counts: HashMap<String, usize>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    contributors: BTreeMap<String, Vec<ContributorSpec>>,
    #[serde(default)]
    fetch_errors: Vec<String>,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    child_error: bool,
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

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

struct NoopLister;

impl PrivateRepoLister for NoopLister {
    fn list_minimal_repos(
        &self,
        _options: &ReposListOptions,
    ) -> Result<Vec<ZoektMinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct StaticJob {
    events: Vec<SearchEvent>,
    err: Option<JobError>,
}

impl SearchJob for StaticJob {
    fn run(
        &mut self,
        _ctx: &JobContext,
        _clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        for event in &self.events {
            stream(event.clone());
        }
        match &self.err {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    fn name(&self) -> &'static str {
        "m9hiChild"
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

struct SpecFetcher {
    contributors: BTreeMap<String, Vec<ContributorSpec>>,
    errors: HashSet<String>,
}

impl ContributorFetcher for SpecFetcher {
    fn contributor_count(
        &self,
        _repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError> {
        if self.errors.contains(&options.path) {
            return Err(JobError("fetch failed".to_owned()));
        }
        Ok(self
            .contributors
            .get(&options.path)
            .into_iter()
            .flatten()
            .map(|spec| ContributorCount {
                name: spec.name.clone(),
                email: spec.email.clone(),
                count: spec.count,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// spec -> model
// ---------------------------------------------------------------------------

fn location(spec: &LocationSpec) -> Location {
    Location {
        offset: spec.offset,
        line: spec.line,
        column: spec.column,
    }
}

fn ranges(specs: &[RangeSpec]) -> Vec<Range> {
    specs
        .iter()
        .map(|spec| Range {
            start: location(&spec.start),
            end: location(&spec.end),
        })
        .collect()
}

fn minimal_repo(spec: &RepoRef) -> MinimalRepo {
    MinimalRepo {
        id: spec.id,
        name: spec.name.clone(),
    }
}

fn file_ref(spec: &FileSpec) -> FileRef {
    FileRef {
        repo: minimal_repo(&spec.repo),
        commit_id: spec.commit_id.clone(),
        path: spec.path.clone(),
        input_rev: spec.input_rev.clone(),
        precise_language: spec.precise_language.clone(),
    }
}

fn chunk_matches(specs: &[ChunkSpec]) -> Vec<ChunkMatch> {
    specs
        .iter()
        .map(|spec| ChunkMatch {
            content: spec.content.clone(),
            content_start: location(&spec.content_start),
            ranges: ranges(&spec.ranges),
        })
        .collect()
}

fn symbol_match(spec: &SymbolSpec) -> SymbolMatch {
    SymbolMatch {
        file: file_ref(&spec.file),
        symbol: Symbol {
            name: spec.symbol.name.clone(),
            path: spec.symbol.path.clone(),
            line: spec.symbol.line,
            character: spec.symbol.character,
            kind: spec.symbol.kind.clone(),
            language: spec.symbol.language.clone(),
            parent: spec.symbol.parent.clone(),
            parent_kind: spec.symbol.parent_kind.clone(),
        },
    }
}

fn matched_string(spec: &Option<MatchedStringSpec>) -> Option<MatchedString> {
    spec.as_ref().map(|spec| MatchedString {
        content: spec.content.clone(),
        matched_ranges: ranges(&spec.matched_ranges),
    })
}

fn signature(spec: &SignatureSpec) -> Signature {
    Signature {
        name: spec.name.clone(),
        email: spec.email.clone(),
        date: OffsetDateTime::parse(&spec.date, &Rfc3339).expect("valid corpus date"),
    }
}

fn result_match(spec: &ResultSpec) -> Match {
    match spec.kind.as_str() {
        "file" => Match::File(FileMatch {
            file: file_ref(spec.file.as_ref().expect("file spec")),
            chunk_matches: chunk_matches(&spec.chunk_matches),
            symbols: spec.symbols.iter().map(symbol_match).collect(),
            path_matches: ranges(&spec.path_matches),
            limit_hit: spec.limit_hit,
            debug: spec.debug.clone(),
        }),
        "repo" => {
            let repo = spec.repo_match.as_ref().expect("repo_match spec");
            Match::Repo(RepoMatch {
                name: repo.name.clone(),
                id: repo.id,
                rev: repo.rev.clone(),
                description_matches: ranges(&repo.description_matches),
                repo_name_matches: ranges(&repo.repo_name_matches),
            })
        }
        "commit" => {
            let commit = spec.commit.as_ref().expect("commit spec");
            Match::Commit(CommitMatch {
                commit: GitCommit {
                    id: commit.id.clone(),
                    message: commit.message.clone(),
                    author: signature(&commit.author),
                    committer: signature(&commit.committer),
                },
                repo: minimal_repo(spec.repo.as_ref().expect("repo spec")),
                message_preview: matched_string(&spec.message_preview),
                diff_preview: matched_string(&spec.diff_preview),
            })
        }
        other => panic!("unknown result kind {other}"),
    }
}

fn event(case: &Case) -> SearchEvent {
    SearchEvent {
        results: case.results.iter().map(result_match).collect(),
        stats: Stats {
            is_limit_hit: case.is_limit_hit,
            ..Stats::default()
        },
    }
}

fn cases_with_error(case: &Case) -> (Vec<SearchEvent>, Option<JobError>) {
    let err = if case.child_error {
        Some(JobError("child failed".to_owned()))
    } else {
        None
    };
    (vec![event(case)], err)
}

// ---------------------------------------------------------------------------
// model -> normalized JSON
// ---------------------------------------------------------------------------

fn location_out(location: &Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn ranges_out(ranges: &[Range]) -> Value {
    Value::Array(
        ranges
            .iter()
            .map(|range| {
                json!({
                    "start": location_out(&range.start),
                    "end": location_out(&range.end),
                })
            })
            .collect(),
    )
}

fn file_out(file: &FileRef) -> Value {
    json!({
        "repo": { "id": file.repo.id, "name": file.repo.name },
        "commit_id": file.commit_id,
        "path": file.path,
        "input_rev": file.input_rev,
        "precise_language": file.precise_language,
    })
}

fn chunks_out(chunks: &[ChunkMatch]) -> Value {
    Value::Array(
        chunks
            .iter()
            .map(|chunk| {
                json!({
                    "content": chunk.content,
                    "content_start": location_out(&chunk.content_start),
                    "ranges": ranges_out(&chunk.ranges),
                })
            })
            .collect(),
    )
}

fn symbols_out(symbols: &[SymbolMatch]) -> Value {
    Value::Array(
        symbols
            .iter()
            .map(|symbol| {
                json!({
                    "file": file_out(&symbol.file),
                    "symbol": {
                        "name": symbol.symbol.name,
                        "path": symbol.symbol.path,
                        "line": symbol.symbol.line,
                        "character": symbol.symbol.character,
                        "kind": symbol.symbol.kind,
                        "language": symbol.symbol.language,
                        "parent": symbol.symbol.parent,
                        "parent_kind": symbol.symbol.parent_kind,
                    },
                })
            })
            .collect(),
    )
}

fn matched_string_out(value: &Option<MatchedString>) -> Value {
    match value {
        Some(value) => json!({
            "content": value.content,
            "matched_ranges": ranges_out(&value.matched_ranges),
        }),
        None => Value::Null,
    }
}

fn signature_out(signature: &Signature) -> Value {
    json!({
        "name": signature.name,
        "email": signature.email,
        "date": signature
            .date
            .to_offset(time::UtcOffset::UTC)
            .format(&Rfc3339)
            .expect("format date"),
    })
}

fn match_out(match_: &Match) -> Value {
    match match_ {
        Match::File(fm) => json!({
            "kind": "file",
            "file": file_out(&fm.file),
            "chunk_matches": chunks_out(&fm.chunk_matches),
            "symbols": symbols_out(&fm.symbols),
            "path_matches": ranges_out(&fm.path_matches),
            "limit_hit": fm.limit_hit,
            "debug": fm.debug,
        }),
        Match::Repo(repo) => json!({
            "kind": "repo",
            "name": repo.name,
            "id": repo.id,
            "rev": repo.rev,
            "description_matches": ranges_out(&repo.description_matches),
            "repo_name_matches": ranges_out(&repo.repo_name_matches),
        }),
        Match::Commit(cm) => json!({
            "kind": "commit",
            "commit": {
                "id": cm.commit.id,
                "message": cm.commit.message,
                "author": signature_out(&cm.commit.author),
                "committer": signature_out(&cm.commit.committer),
            },
            "repo": { "id": cm.repo.id, "name": cm.repo.name },
            "message_preview": matched_string_out(&cm.message_preview),
            "diff_preview": matched_string_out(&cm.diff_preview),
        }),
        Match::Owner(_) => json!({ "kind": "unknown" }),
    }
}

fn events_out(events: &[SearchEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "results": Value::Array(event.results.iter().map(match_out).collect()),
                    "stats": { "is_limit_hit": event.stats.is_limit_hit },
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// case runners
// ---------------------------------------------------------------------------

fn verbosity(name: &str) -> Verbosity {
    match name {
        "basic" => Verbosity::Basic,
        "max" => Verbosity::Max,
        _ => Verbosity::None,
    }
}

fn regexps(patterns: &[String]) -> Vec<Regex> {
    patterns
        .iter()
        .map(|pattern| Regex::new(pattern).expect("valid corpus regexp"))
        .collect()
}

fn run_job(job: &mut dyn SearchJob) -> (Vec<SearchEvent>, Option<JobError>) {
    let streamer = NoopStreamer;
    let lister = NoopLister;
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
    };
    let ctx = JobContext::default();
    let mut events = Vec::new();
    let result = job.run(&ctx, &clients, &mut |event| events.push(event));
    (events, result.err())
}

fn base(case: &Case) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    map.insert("name".to_owned(), json!(case.name));
    map.insert("kind".to_owned(), json!(case.kind));
    map
}

fn run_attributes(case: &Case) -> Value {
    let child = || StaticJob {
        events: Vec::new(),
        err: None,
    };
    let mut map = base(case);
    map.insert("job".to_owned(), json!(case.job));
    match case.job.as_str() {
        "contains" => {
            let original = case.original.as_ref().map(NodeSpec::to_node);
            let job = FilterFileContainsJob::new(
                case.include_patterns.clone(),
                original.as_ref(),
                case.case_sensitive,
                Box::new(child()),
            )
            .expect("valid corpus patterns");
            attributes(&mut map, &job, &case.verbosity);
        }
        "contributor" => {
            let job = FilterFileHasContributorsJob::new(
                Box::new(child()),
                regexps(&case.include),
                regexps(&case.exclude),
                Box::new(SpecFetcher {
                    contributors: BTreeMap::new(),
                    errors: HashSet::new(),
                }),
            );
            attributes(&mut map, &job, &case.verbosity);
        }
        "select" => {
            let job = SelectJob::new(case.select.clone(), Box::new(child()));
            attributes(&mut map, &job, &case.verbosity);
        }
        other => panic!("unknown job {other}"),
    }
    Value::Object(map)
}

fn attributes(map: &mut serde_json::Map<String, Value>, job: &dyn SearchJob, verbosity_name: &str) {
    map.insert("name_value".to_owned(), json!(job.name()));
    map.insert("children_count".to_owned(), json!(job.children().len()));
    map.insert(
        "attributes".to_owned(),
        Value::Array(
            job.attributes(verbosity(verbosity_name))
                .into_iter()
                .map(|(key, value)| json!([key, value]))
                .collect(),
        ),
    );
}

fn run_contains_constructor(case: &Case) -> Value {
    let original = case.original.as_ref().map(NodeSpec::to_node);
    let result = FilterFileContainsJob::new(
        case.include_patterns.clone(),
        original.as_ref(),
        case.case_sensitive,
        Box::new(StaticJob {
            events: Vec::new(),
            err: None,
        }),
    );
    let mut map = base(case);
    map.insert("job".to_owned(), json!("contains"));
    match result {
        Ok(_) => {
            map.insert("error".to_owned(), json!(false));
            map.insert("stage".to_owned(), json!(""));
        }
        Err(err) => {
            map.insert("error".to_owned(), json!(true));
            let stage = match err.stage {
                sg_service_frontend::search_jobutil_filters::ContainsPatternStage::Include => {
                    "include"
                }
                sg_service_frontend::search_jobutil_filters::ContainsPatternStage::Original => {
                    "original"
                }
            };
            map.insert("stage".to_owned(), json!(stage));
        }
    }
    Value::Object(map)
}

fn run_contains(case: &Case) -> Value {
    let original = case.original.as_ref().map(NodeSpec::to_node);
    let (events, err) = cases_with_error(case);
    let child = StaticJob { events, err };
    let mut job = FilterFileContainsJob::new(
        case.include_patterns.clone(),
        original.as_ref(),
        case.case_sensitive,
        Box::new(child),
    )
    .expect("valid corpus patterns");
    let (events, err) = run_job(&mut job);
    let mut map = base(case);
    map.insert("job".to_owned(), json!("contains"));
    map.insert("error".to_owned(), json!(err.is_some()));
    map.insert("events".to_owned(), events_out(&events));
    Value::Object(map)
}

fn run_diff_remove(case: &Case) -> Value {
    let job = FilterFileContainsJob::new(
        case.include_patterns.clone(),
        None,
        case.case_sensitive,
        Box::new(StaticJob {
            events: Vec::new(),
            err: None,
        }),
    )
    .expect("valid corpus patterns");
    let epoch = OffsetDateTime::UNIX_EPOCH;
    let mut cm = CommitMatch {
        commit: GitCommit {
            id: "c1".to_owned(),
            message: "msg".to_owned(),
            author: Signature {
                name: "A".to_owned(),
                email: "a@example.com".to_owned(),
                date: epoch,
            },
            committer: Signature {
                name: "A".to_owned(),
                email: "a@example.com".to_owned(),
                date: epoch,
            },
        },
        repo: MinimalRepo {
            id: 1,
            name: "repo".to_owned(),
        },
        message_preview: None,
        diff_preview: Some(MatchedString {
            content: case.content.clone(),
            matched_ranges: ranges(&case.matched_ranges),
        }),
    };
    let matched = job.filter_unmatched_file_diffs(&mut cm, &case.matched_file_counts);
    let result = if matched.is_some() {
        let preview = cm.diff_preview.as_ref().expect("preview");
        json!({
            "content": preview.content,
            "matched_ranges": ranges_out(&preview.matched_ranges),
        })
    } else {
        Value::Null
    };
    let mut map = base(case);
    map.insert("job".to_owned(), json!("contains"));
    map.insert("result".to_owned(), result);
    Value::Object(map)
}

fn run_contributor(case: &Case) -> Value {
    let (events, err) = cases_with_error(case);
    let child = StaticJob { events, err };
    let fetcher = SpecFetcher {
        contributors: case.contributors.clone(),
        errors: case.fetch_errors.iter().cloned().collect(),
    };
    let mut job = FilterFileHasContributorsJob::new(
        Box::new(child),
        regexps(&case.include),
        regexps(&case.exclude),
        Box::new(fetcher),
    );
    let deadline = if case.deadline == "expired" {
        Some(Instant::now() - Duration::from_secs(1))
    } else {
        None
    };
    let streamer = NoopStreamer;
    let lister = NoopLister;
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
    };
    let ctx = JobContext { deadline };
    let mut out = Vec::new();
    let result = job.run(&ctx, &clients, &mut |event| out.push(event));
    let mut map = base(case);
    map.insert("job".to_owned(), json!("contributor"));
    map.insert("error".to_owned(), json!(result.is_err()));
    map.insert("events".to_owned(), events_out(&out));
    Value::Object(map)
}

fn run_select(case: &Case) -> Value {
    let (events, err) = cases_with_error(case);
    let child = StaticJob { events, err };
    let mut job = SelectJob::new(case.select.clone(), Box::new(child));
    let (events, err) = run_job(&mut job);
    let mut map = base(case);
    map.insert("job".to_owned(), json!("select"));
    map.insert("error".to_owned(), json!(err.is_some()));
    map.insert("events".to_owned(), events_out(&events));
    Value::Object(map)
}

#[test]
fn search_jobutil_filters_differential() {
    if std::env::var("SG_M9HI_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HI_CORPUS").expect("M9HI_CORPUS must be set when SG_M9HI_DIFF=1");
    let out_path = std::env::var("M9HI_OUT").expect("M9HI_OUT must be set when SG_M9HI_DIFF=1");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "attributes" => run_attributes(case),
            "contains_constructor" => run_contains_constructor(case),
            "contains_run" => run_contains(case),
            "contains_diff_remove" => run_diff_remove(case),
            "contributor_run" => run_contributor(case),
            "select_run" => run_select(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();

    let encoded = serde_json::to_string(&out).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}
