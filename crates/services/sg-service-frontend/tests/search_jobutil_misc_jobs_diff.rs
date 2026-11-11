//! M9.8hm differential harness: `internal/search/job/jobutil/`
//! `sanitize_job.go`, `sub_repo_perms_job.go` and `log_job.go` against
//! [`sg_service_frontend::search_jobutil_misc_jobs`].
//!
//! Run with `SG_M9HM_DIFF=1 M9HM_CORPUS=<json> M9HM_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_search_query::node::Node;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::{init, run};
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
    SearchJob, Verbosity,
};
use sg_service_frontend::search_jobutil_misc_jobs::{
    apply_sub_repo_filtering, log_event, Actor, BackendEventLogger, CheckerError, LogActor,
    LogInputs, LogJob, NoopPermsLog, Perms, RepoContent, SanitizeJob, SubRepoPermissionChecker,
    SubRepoPermsFilterJob,
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
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
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
    #[serde(default)]
    modified_files: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ActorSpec {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    internal: bool,
    #[serde(default)]
    mock: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct CheckerSpec {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    enabled_for_repo_id: BTreeMap<String, String>,
    #[serde(default)]
    default_enabled: String,
    #[serde(default)]
    permissions: BTreeMap<String, String>,
    #[serde(default)]
    default_permissions: String,
    #[serde(default)]
    file_permissions_error: String,
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
    patterns: Vec<String>,
    #[serde(default)]
    results: Vec<ResultSpec>,
    #[serde(default)]
    is_limit_hit: bool,
    #[serde(default)]
    child_error: bool,
    #[serde(default)]
    checker: CheckerSpec,
    #[serde(default)]
    actor: ActorSpec,
    #[serde(default)]
    query: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    duration_ms: i64,
    #[serde(default)]
    fail_backend: Vec<String>,
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
        "m9hmChild"
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

#[derive(Default)]
struct Counts {
    enabled_for_repo_id: Cell<u32>,
    permissions: Cell<u32>,
    file_permissions: Cell<u32>,
    path_checks: Cell<u32>,
}

struct SpecChecker {
    spec: CheckerSpec,
    counts: Rc<Counts>,
}

impl SpecChecker {
    fn new(spec: CheckerSpec, counts: Rc<Counts>) -> Self {
        Self { spec, counts }
    }

    fn perm_for(spec: &CheckerSpec, path: &str) -> Result<Perms, CheckerError> {
        let raw = spec
            .permissions
            .get(path)
            .map(String::as_str)
            .unwrap_or(spec.default_permissions.as_str());
        parse_perms(raw)
    }
}

impl SubRepoPermissionChecker for SpecChecker {
    fn enabled(&self) -> bool {
        self.spec.enabled
    }

    fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, CheckerError> {
        self.counts
            .enabled_for_repo_id
            .set(self.counts.enabled_for_repo_id.get() + 1);
        let raw = self
            .spec
            .enabled_for_repo_id
            .get(&repo_id.to_string())
            .map(String::as_str)
            .unwrap_or(self.spec.default_enabled.as_str());
        parse_enabled(raw)
    }

    fn permissions(&self, _user_id: i32, content: &RepoContent) -> Result<Perms, CheckerError> {
        self.counts
            .permissions
            .set(self.counts.permissions.get() + 1);
        Self::perm_for(&self.spec, &content.path)
    }

    fn file_permissions_func(
        &self,
        _user_id: i32,
        _repo: &str,
    ) -> Result<Box<sg_service_frontend::search_jobutil_misc_jobs::FilePermissionFunc>, CheckerError>
    {
        self.counts
            .file_permissions
            .set(self.counts.file_permissions.get() + 1);
        if !self.spec.file_permissions_error.is_empty() {
            return Err(CheckerError::other(
                self.spec.file_permissions_error.clone(),
            ));
        }
        let spec = self.spec.clone();
        let counts = self.counts.clone();
        Ok(Box::new(move |path: &str| {
            counts.path_checks.set(counts.path_checks.get() + 1);
            SpecChecker::perm_for(&spec, path)
        }))
    }
}

fn parse_enabled(raw: &str) -> Result<bool, CheckerError> {
    match raw {
        "" | "true" => Ok(true),
        "false" => Ok(false),
        _ => {
            if let Some(message) = raw.strip_prefix("err:") {
                return Err(CheckerError::other(message));
            }
            if let Some(message) = raw.strip_prefix("ctx:") {
                return Err(CheckerError::context(message));
            }
            panic!("bad enabled value {raw}")
        }
    }
}

fn parse_perms(raw: &str) -> Result<Perms, CheckerError> {
    match raw {
        "" | "read" => Ok(Perms::READ),
        "write" => Ok(Perms::WRITE),
        "none" => Ok(Perms::NONE),
        _ => {
            if let Some(message) = raw.strip_prefix("err:") {
                return Err(CheckerError::other(message));
            }
            if let Some(message) = raw.strip_prefix("ctx:") {
                return Err(CheckerError::context(message));
            }
            panic!("bad permissions value {raw}")
        }
    }
}

struct FailingBackend {
    fail: HashSet<String>,
}

impl BackendEventLogger for FailingBackend {
    fn log_backend_event(
        &mut self,
        _user_id: i32,
        name: &str,
        _value: Option<&str>,
    ) -> Result<(), JobError> {
        if self.fail.contains(name) {
            Err(JobError("backend down".to_owned()))
        } else {
            Ok(())
        }
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
                    parents: Vec::new(),
                },
                repo: minimal_repo(spec.repo.as_ref().expect("repo spec")),
                message_preview: matched_string(&spec.message_preview),
                diff_preview: matched_string(&spec.diff_preview),
                refs: Vec::new(),
                source_refs: Vec::new(),
                diff: Vec::new(),
                modified_files: Vec::new(),
            })
        }
        "owner" => Match::Owner(sg_service_frontend::search_stream::convert::OwnerMatch {
            owner: sg_service_frontend::search_stream::convert::ResolvedOwner::Person {
                handle: "owner".to_owned(),
                email: "owner@example.com".to_owned(),
                user: None,
            },
            repo: minimal_repo(spec.repo.as_ref().expect("repo spec")),
        }),
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

fn modified_files(case: &Case) -> HashMap<(i32, String), Vec<String>> {
    let mut out = HashMap::new();
    for result in &case.results {
        if result.kind == "commit" {
            let commit = result.commit.as_ref().expect("commit spec");
            let repo = result.repo.as_ref().expect("repo spec");
            out.insert((repo.id, commit.id.clone()), result.modified_files.clone());
        }
    }
    out
}

fn actor(spec: &ActorSpec) -> Actor {
    Actor {
        uid: spec.uid,
        internal: spec.internal,
        mock_user: spec.mock,
    }
}

fn log_actor(spec: &ActorSpec) -> LogActor {
    LogActor {
        uid: spec.uid,
        mock_user: spec.mock,
    }
}

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "codycontext" => SearchType::CodyContext,
        _ => SearchType::Keyword,
    }
}

fn log_inputs(case: &Case) -> LogInputs {
    let pattern_type = search_type(&case.pattern_type);
    let query: Vec<Node> = run(init(case.query.clone(), pattern_type)).expect("valid corpus query");
    LogInputs {
        query,
        pattern_type,
    }
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

fn matches_out(matches: &[Match]) -> Value {
    Value::Array(matches.iter().map(match_out).collect())
}

fn events_out(events: &[SearchEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "results": matches_out(&event.results),
                    "stats": { "is_limit_hit": event.stats.is_limit_hit },
                })
            })
            .collect(),
    )
}

fn telemetry_out(
    events: &[sg_service_frontend::search_jobutil_misc_jobs::TelemetryEvent],
) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                let mut metadata = serde_json::Map::new();
                for (key, value) in &event.metadata {
                    metadata.insert(key.clone(), json!(format!("{value}")));
                }
                json!({
                    "feature": event.feature,
                    "action": event.action,
                    "metadata": Value::Object(metadata),
                })
            })
            .collect(),
    )
}

fn telemetry_names_out(
    events: &[sg_service_frontend::search_jobutil_misc_jobs::TelemetryEvent],
) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "feature": event.feature,
                    "action": event.action,
                })
            })
            .collect(),
    )
}

fn legacy_out(events: &[sg_service_frontend::search_jobutil_misc_jobs::LegacyEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "name": event.name,
                    "argument": event.argument,
                })
            })
            .collect(),
    )
}

fn legacy_names_out(
    events: &[sg_service_frontend::search_jobutil_misc_jobs::LegacyEvent],
) -> Value {
    Value::Array(events.iter().map(|event| json!(event.name)).collect())
}

fn error_out(error: &Option<JobError>) -> Value {
    match error {
        Some(error) => json!(error.to_string()),
        None => Value::Null,
    }
}

fn calls_out(counts: &Counts) -> Value {
    json!({
        "enabled_for_repo_id": counts.enabled_for_repo_id.get(),
        "permissions": counts.permissions.get(),
        "file_permissions": counts.file_permissions.get(),
        "path_checks": counts.path_checks.get(),
    })
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
        searcher: None,
        commit: None,
        repo_pager: None,
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

fn run_attributes(case: &Case) -> Value {
    let child = || StaticJob {
        events: Vec::new(),
        err: None,
    };
    let mut map = base(case);
    map.insert("job".to_owned(), json!(case.job));
    match case.job.as_str() {
        "sanitize" => {
            let job = SanitizeJob::new(regexps(&case.patterns), Box::new(child()));
            attributes(&mut map, &job, &case.verbosity);
        }
        "sub_repo" => {
            let counts = Rc::new(Counts::default());
            let job = SubRepoPermsFilterJob::new(
                Box::new(SpecChecker::new(case.checker.clone(), counts)),
                Actor::default(),
                HashMap::new(),
                Box::new(NoopPermsLog),
                Box::new(child()),
            );
            attributes(&mut map, &job, &case.verbosity);
        }
        "log" => {
            let job = LogJob::new(
                LogInputs {
                    query: Vec::new(),
                    pattern_type: SearchType::Keyword,
                },
                LogActor::default(),
                Box::new(FailingBackend {
                    fail: HashSet::new(),
                }),
                Box::new(child()),
            );
            attributes(&mut map, &job, &case.verbosity);
        }
        other => panic!("unknown job {other}"),
    }
    Value::Object(map)
}

fn run_sanitize(case: &Case) -> Value {
    let child = StaticJob {
        events: vec![event(case)],
        err: if case.child_error {
            Some(JobError("child failed".to_owned()))
        } else {
            None
        },
    };
    let mut job = SanitizeJob::new(regexps(&case.patterns), Box::new(child));
    let (events, err) = run_job(&mut job);
    let mut map = base(case);
    map.insert("error".to_owned(), json!(err.is_some()));
    map.insert("events".to_owned(), events_out(&events));
    Value::Object(map)
}

fn run_apply(case: &Case) -> Value {
    let counts = Rc::new(Counts::default());
    let checker = SpecChecker::new(case.checker.clone(), counts.clone());
    let matches: Vec<Match> = case.results.iter().map(result_match).collect();
    let (filtered, err) = apply_sub_repo_filtering(
        &checker,
        &actor(&case.actor),
        &modified_files(case),
        &mut NoopPermsLog,
        matches,
    );
    let mut map = base(case);
    map.insert("matches".to_owned(), matches_out(&filtered));
    map.insert("error".to_owned(), error_out(&err));
    map.insert("calls".to_owned(), calls_out(&counts));
    Value::Object(map)
}

fn run_sub_repo(case: &Case) -> Value {
    let counts = Rc::new(Counts::default());
    let checker = SpecChecker::new(case.checker.clone(), counts.clone());
    let child = StaticJob {
        events: vec![event(case)],
        err: if case.child_error {
            Some(JobError("child failed".to_owned()))
        } else {
            None
        },
    };
    let mut job = SubRepoPermsFilterJob::new(
        Box::new(checker),
        actor(&case.actor),
        modified_files(case),
        Box::new(NoopPermsLog),
        Box::new(child),
    );
    let (events, err) = run_job(&mut job);
    let mut map = base(case);
    map.insert("error".to_owned(), error_out(&err));
    map.insert("events".to_owned(), events_out(&events));
    map.insert("calls".to_owned(), calls_out(&counts));
    Value::Object(map)
}

fn fail_backend(case: &Case) -> FailingBackend {
    FailingBackend {
        fail: case.fail_backend.iter().cloned().collect(),
    }
}

fn run_log_event(case: &Case) -> Value {
    let inputs = log_inputs(case);
    let mut backend = fail_backend(case);
    let record = log_event(
        log_actor(&case.actor),
        &inputs,
        Duration::from_millis(case.duration_ms.max(0) as u64),
        &mut backend,
    );
    let mut map = base(case);
    map.insert("skipped".to_owned(), json!(record.skipped));
    map.insert("telemetry".to_owned(), telemetry_out(&record.telemetry));
    map.insert("legacy".to_owned(), legacy_out(&record.legacy));
    Value::Object(map)
}

fn run_log_run(case: &Case) -> Value {
    let inputs = log_inputs(case);
    let child = StaticJob {
        events: vec![event(case)],
        err: if case.child_error {
            Some(JobError("child failed".to_owned()))
        } else {
            None
        },
    };
    let mut job = LogJob::new(
        inputs,
        log_actor(&case.actor),
        Box::new(fail_backend(case)),
        Box::new(child),
    );
    let (events, err) = run_job(&mut job);
    let record = job.last_record().expect("log record");
    let mut map = base(case);
    map.insert("error".to_owned(), error_out(&err));
    map.insert("events".to_owned(), events_out(&events));
    map.insert("skipped".to_owned(), json!(record.skipped));
    map.insert(
        "telemetry".to_owned(),
        telemetry_names_out(&record.telemetry),
    );
    map.insert("legacy_names".to_owned(), legacy_names_out(&record.legacy));
    Value::Object(map)
}

#[test]
fn search_jobutil_misc_jobs_differential() {
    if std::env::var("SG_M9HM_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HM_CORPUS").expect("M9HM_CORPUS must be set when SG_M9HM_DIFF=1");
    let out_path = std::env::var("M9HM_OUT").expect("M9HM_OUT must be set when SG_M9HM_DIFF=1");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "attributes" => run_attributes(case),
            "sanitize_run" => run_sanitize(case),
            "sub_repo_apply" => run_apply(case),
            "sub_repo_run" => run_sub_repo(case),
            "log_event" => run_log_event(case),
            "log_run" => run_log_run(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();

    std::fs::write(&out_path, serde_json::to_vec(&out).expect("encode output"))
        .expect("write output");
}
