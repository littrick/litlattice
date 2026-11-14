//! M9.8if differential harness: execute `AlertJob` / `SelectJob` wrappers and
//! the commit leaf through `JobNode::run_with_alert` and compare events,
//! alerts and errors against the Go oracle
//! (`internal/search/job/jobutil/zz_m9if_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9IF_DIFF=1` enable the harness
//! * `M9IF_CORPUS` corpus JSON
//! * `M9IF_OUT` output JSON

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_proto::gitserver as pb;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_service_frontend::search_client::SearchMode;
use sg_service_frontend::search_commit_job::{
    query_to_git_query, repository_revisions, CommitRuntimeClients, CommitUserDB, CommitUserEmail,
    GitserverClient,
};
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, RuntimeClients,
};
use sg_service_frontend::search_jobutil_combinators::{Alert, AlertInputs, JobFailure};
use sg_service_frontend::search_jobutil_tree::{JobNode, StaticEvent};
use sg_service_frontend::search_searcher_client::{SearcherError, SearcherErrorKind};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    RepoMatch, Signature, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_zoekt::query::Query;
use sg_zoekt::types::{SearchOptions, SearchResult};

// ---------------------------------------------------------------------------
// corpus schema
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    inputs: InputsSpec,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    child_error: Option<ErrorSpec>,
    #[serde(default)]
    deadline: String,
    #[serde(default)]
    select: Vec<String>,
    // commit cases
    #[serde(default)]
    query: String,
    #[serde(default)]
    diff: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    users: Vec<UserSpec>,
    #[serde(default)]
    db_username_error: String,
    #[serde(default)]
    db_email_error: String,
    #[serde(default)]
    responses: Vec<ResponseSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct InputsSpec {
    #[serde(default)]
    query: String,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    search_mode: String,
}

#[derive(Debug, Default, Deserialize)]
struct EventSpec {
    #[serde(default)]
    results: Vec<ResultSpec>,
    #[serde(default)]
    is_limit_hit: bool,
    #[serde(default)]
    status: Vec<(i32, u8)>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ErrorSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct LocationSpec {
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    line: i64,
    #[serde(default)]
    column: i64,
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

#[derive(Debug, Default, Clone, Deserialize)]
struct SymbolSpec {
    #[serde(default)]
    file: FileSpec,
    #[serde(default)]
    symbol: SymbolInfoSpec,
}

#[derive(Debug, Default, Clone, Deserialize)]
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

#[derive(Debug, Default, Clone, Deserialize)]
struct MatchedStringSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    matched_ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct SignatureSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    date: String,
}

#[derive(Debug, Default, Clone, Deserialize)]
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

#[derive(Debug, Default, Clone, Deserialize)]
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

#[derive(Debug, Default, Clone, Deserialize)]
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

#[derive(Debug, Default, Clone, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    id: u32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct UserSpec {
    #[serde(default)]
    username: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    emails: Vec<MailSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct MailSpec {
    #[serde(default)]
    address: String,
    #[serde(default)]
    verified: bool,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ResponseSpec {
    #[serde(default)]
    repo: String,
    #[serde(default)]
    batches: Vec<Vec<MatchSpec>>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    error: Option<ErrorSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct CommitMatchedStringSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct MatchSpec {
    #[serde(default)]
    oid: String,
    #[serde(default)]
    author: SignatureSpec,
    #[serde(default)]
    committer: SignatureSpec,
    #[serde(default)]
    message: CommitMatchedStringSpec,
    #[serde(default)]
    diff: CommitMatchedStringSpec,
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
    ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct FakeDb {
    users: Vec<UserSpec>,
    username_error: Option<String>,
    email_error: Option<String>,
}

impl CommitUserDB for FakeDb {
    fn get_user_id_by_username(&self, username: &str) -> Result<Option<i32>, JobError> {
        if let Some(error) = &self.username_error {
            return Err(JobError(error.clone()));
        }
        Ok(self
            .users
            .iter()
            .find(|user| user.username == username)
            .map(|user| user.id))
    }

    fn list_emails_by_user(&self, user_id: i32) -> Result<Vec<CommitUserEmail>, JobError> {
        if let Some(error) = &self.email_error {
            return Err(JobError(error.clone()));
        }
        let mut emails = Vec::new();
        for user in &self.users {
            if user.id != user_id {
                continue;
            }
            for email in &user.emails {
                emails.push(CommitUserEmail {
                    email: email.address.clone(),
                    verified: email.verified,
                });
            }
        }
        Ok(emails)
    }
}

struct FakeGitserver {
    responses: RefCell<BTreeMap<String, VecDeque<ResponseSpec>>>,
    requests: RefCell<Vec<pb::SearchRequest>>,
}

impl GitserverClient for FakeGitserver {
    fn search(
        &self,
        request: &pb::SearchRequest,
        on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError> {
        self.requests.borrow_mut().push(request.clone());
        let mut responses = self.responses.borrow_mut();
        let queue = responses
            .get_mut(&request.repo)
            .unwrap_or_else(|| panic!("no scripted response for {}", request.repo));
        let spec = queue
            .pop_front()
            .unwrap_or_else(|| panic!("no scripted response left for {}", request.repo));

        for batch in &spec.batches {
            on_matches(batch.iter().map(pb_match).collect());
        }
        if let Some(error) = &spec.error {
            return Err(build_error(error, request));
        }
        Ok(spec.limit_hit)
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
        other => panic!("unknown result kind {other}"),
    }
}

fn static_event(spec: &EventSpec) -> StaticEvent {
    StaticEvent {
        results: spec.results.iter().map(result_match).collect(),
        is_limit_hit: spec.is_limit_hit,
        status: spec.status.clone(),
    }
}

fn child_failure(spec: &Option<ErrorSpec>) -> Option<JobFailure> {
    let spec = spec.as_ref()?;
    Some(match spec.kind.as_str() {
        "canceled" => JobFailure::Canceled,
        "deadline" => JobFailure::DeadlineExceeded,
        "multi" => JobFailure::Multi(
            spec.messages
                .iter()
                .map(|message| JobFailure::Message(message.clone()))
                .collect(),
        ),
        _ => JobFailure::Message(spec.message.clone()),
    })
}

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        _ => SearchType::Standard,
    }
}

fn search_mode(name: &str) -> SearchMode {
    if name == "smart" || name == "smart_search" {
        SearchMode::SmartSearch
    } else {
        SearchMode::Precise
    }
}

fn pb_location(spec: &LocationSpec) -> pb::commit_match::Location {
    pb::commit_match::Location {
        offset: spec.offset as u32,
        line: spec.line as u32,
        column: spec.column as u32,
    }
}

fn pb_matched_string(spec: &CommitMatchedStringSpec) -> pb::commit_match::MatchedString {
    pb::commit_match::MatchedString {
        content: spec.content.clone(),
        ranges: spec
            .ranges
            .iter()
            .map(|range| pb::commit_match::Range {
                start: Some(pb_location(&range.start)),
                end: Some(pb_location(&range.end)),
            })
            .collect(),
    }
}

fn pb_signature(spec: &SignatureSpec) -> pb::commit_match::Signature {
    let date = OffsetDateTime::parse(&spec.date, &Rfc3339).expect("valid corpus date");
    pb::commit_match::Signature {
        name: spec.name.clone(),
        email: spec.email.clone(),
        date: Some(prost_types::Timestamp {
            seconds: date.unix_timestamp(),
            nanos: date.nanosecond() as i32,
        }),
    }
}

fn pb_match(spec: &MatchSpec) -> pb::CommitMatch {
    pb::CommitMatch {
        oid: spec.oid.clone(),
        author: Some(pb_signature(&spec.author)),
        committer: Some(pb_signature(&spec.committer)),
        parents: Vec::new(),
        refs: Vec::new(),
        source_refs: Vec::new(),
        message: Some(pb_matched_string(&spec.message)),
        diff: Some(pb_matched_string(&spec.diff)),
        modified_files: Vec::new(),
    }
}

fn build_error(spec: &ErrorSpec, request: &pb::SearchRequest) -> SearcherError {
    let kind = match spec.kind.as_str() {
        "repo_not_found" => SearcherErrorKind::RepoNotFound {
            clone_in_progress: false,
        },
        "repo_cloning" => SearcherErrorKind::RepoNotFound {
            clone_in_progress: true,
        },
        "revision_not_found" => SearcherErrorKind::RevisionNotFound,
        "not_found" => SearcherErrorKind::NotFound,
        "timeout" => SearcherErrorKind::Timeout,
        "temporary" => SearcherErrorKind::Temporary,
        _ => SearcherErrorKind::Other,
    };
    let message = if spec.kind == "revision_not_found" {
        let rev = request
            .revisions
            .first()
            .map(|revision| revision.rev_spec.as_str())
            .unwrap_or("");
        format!("revision not found: {}@{rev}", request.repo)
    } else {
        spec.message.clone()
    };
    SearcherError { kind, message }
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
                let mut status: Vec<(i32, u8)> = event.stats.status.entries();
                status.sort_unstable();
                json!({
                    "results": Value::Array(event.results.iter().map(match_out).collect()),
                    "stats": {
                        "is_limit_hit": event.stats.is_limit_hit,
                        "status": Value::Array(
                            status
                                .iter()
                                .map(|(id, bits)| json!([id, bits]))
                                .collect(),
                        ),
                    },
                })
            })
            .collect(),
    )
}

fn pattern_type_out(search_type: SearchType) -> &'static str {
    match search_type {
        SearchType::Regex => "regex",
        SearchType::Literal => "literal",
        SearchType::Structural => "structural",
        SearchType::Standard => "standard",
        SearchType::CodyContext => "codycontext",
        SearchType::Keyword => "keyword",
    }
}

fn alert_out(alert: &Option<Alert>) -> Value {
    let Some(alert) = alert else {
        return Value::Null;
    };
    let proposed = alert
        .proposed_queries
        .iter()
        .map(|query| {
            let annotations: Map<String, Value> = query
                .annotations
                .iter()
                .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                .collect();
            json!({
                "description": query.description,
                "query": query.query,
                "pattern_type": pattern_type_out(query.pattern_type),
                "annotations": annotations,
            })
        })
        .collect();
    json!({
        "prometheus_type": alert.prometheus_type,
        "title": alert.title,
        "kind": alert.kind,
        "priority": alert.priority,
        "proposed_queries": Value::Array(proposed),
    })
}

fn error_out(error: &Option<JobFailure>) -> Value {
    let Some(error) = error else {
        return Value::Null;
    };
    let parts: Vec<Value> = error
        .parts()
        .into_iter()
        .map(|part| {
            if part.is_canceled() {
                json!({"kind": "canceled"})
            } else if part.is_deadline_exceeded() {
                json!({"kind": "deadline"})
            } else if let JobFailure::Message(message) = part {
                json!({"kind": "message", "message": message})
            } else {
                json!({"kind": "message", "message": part.to_string()})
            }
        })
        .collect();
    json!({ "parts": parts })
}

fn node_json(node: &pb::QueryNode) -> Value {
    use pb::query_node::Value as V;
    match node.value.as_ref() {
        Some(V::Boolean(node)) => json!({"type": "boolean", "value": node.value}),
        Some(V::AuthorMatches(node)) => json!({
            "type": "author_matches",
            "expr": node.expr,
            "ignore_case": node.ignore_case,
        }),
        Some(V::CommitterMatches(node)) => json!({
            "type": "committer_matches",
            "expr": node.expr,
            "ignore_case": node.ignore_case,
        }),
        Some(V::CommitBefore(node)) => json!({
            "type": "commit_before",
            "seconds": node.timestamp.as_ref().map_or(0, |t| t.seconds),
            "nanos": node.timestamp.as_ref().map_or(0, |t| t.nanos),
        }),
        Some(V::CommitAfter(node)) => json!({
            "type": "commit_after",
            "seconds": node.timestamp.as_ref().map_or(0, |t| t.seconds),
            "nanos": node.timestamp.as_ref().map_or(0, |t| t.nanos),
        }),
        Some(V::MessageMatches(node)) => json!({
            "type": "message_matches",
            "expr": node.expr,
            "ignore_case": node.ignore_case,
        }),
        Some(V::DiffMatches(node)) => json!({
            "type": "diff_matches",
            "expr": node.expr,
            "ignore_case": node.ignore_case,
        }),
        Some(V::DiffModifiesFile(node)) => json!({
            "type": "diff_modifies_file",
            "expr": node.expr,
            "ignore_case": node.ignore_case,
        }),
        Some(V::Operator(node)) => {
            let kind = match node.kind {
                x if x == pb::OperatorKind::And as i32 => "and",
                x if x == pb::OperatorKind::Or as i32 => "or",
                x if x == pb::OperatorKind::Not as i32 => "not",
                _ => "unspecified",
            };
            json!({
                "type": "operator",
                "kind": kind,
                "operands": Value::Array(node.operands.iter().map(node_json).collect()),
            })
        }
        None => Value::Null,
    }
}

fn request_out(request: &pb::SearchRequest) -> Value {
    json!({
        "repo": request.repo,
        "revisions": request
            .revisions
            .iter()
            .map(|revision| revision.rev_spec.clone())
            .collect::<Vec<_>>(),
        "limit": request.limit,
        "include_diff": request.include_diff,
        "include_modified_files": request.include_modified_files,
        "query": request.query.as_ref().map(node_json),
    })
}

// ---------------------------------------------------------------------------
// runners
// ---------------------------------------------------------------------------

fn deadline_instant(name: &str) -> Option<Instant> {
    match name {
        "expired" => Instant::now().checked_sub(Duration::from_secs(1)),
        "future" => Instant::now().checked_add(Duration::from_secs(30)),
        _ => None,
    }
}

fn run_node(
    node: &mut JobNode,
    case: &Case,
    clients: &RuntimeClients<'_>,
) -> (Vec<SearchEvent>, Option<Alert>, Option<JobFailure>) {
    let ctx = JobContext {
        deadline: deadline_instant(&case.deadline),
    };
    let mut events = Vec::new();
    let (alert, error) = node.run_with_alert(&ctx, clients, &mut |event| events.push(event));
    (events, alert, error)
}

fn base(case: &Case) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("name".to_owned(), json!(case.name));
    map.insert("kind".to_owned(), json!(case.kind));
    map
}

fn run_alert(case: &Case) -> Value {
    let inputs = AlertInputs {
        query: case.inputs.query.clone(),
        original_query: case.inputs.original_query.clone(),
        pattern_type: search_type(&case.inputs.pattern_type),
        search_mode: search_mode(&case.inputs.search_mode),
        ..AlertInputs::default()
    };
    let child = JobNode::static_events(
        case.events.iter().map(static_event).collect(),
        child_failure(&case.child_error),
    );
    let mut node = JobNode::alert(&inputs, child);

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
    let (events, alert, error) = run_node(&mut node, case, &clients);

    let mut map = base(case);
    map.insert("events".to_owned(), events_out(&events));
    map.insert("alert".to_owned(), alert_out(&alert));
    map.insert("error".to_owned(), error_out(&error));
    Value::Object(map)
}

fn run_select(case: &Case) -> Value {
    let child = JobNode::static_events(
        case.events.iter().map(static_event).collect(),
        child_failure(&case.child_error),
    );
    let mut node = JobNode::select(case.select.clone(), child);

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
    let (events, alert, error) = run_node(&mut node, case, &clients);

    let mut map = base(case);
    map.insert("events".to_owned(), events_out(&events));
    map.insert("alert".to_owned(), alert_out(&alert));
    map.insert("error".to_owned(), error_out(&error));
    Value::Object(map)
}

fn run_commit(case: &Case) -> Value {
    let plan = pipeline_query(&case.query, SearchType::Regex)
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query));
    assert_eq!(
        plan.len(),
        1,
        "{}: expected one basic query, got {}",
        case.name,
        plan.len()
    );
    let basic = plan.into_iter().next().unwrap();
    let query = query_to_git_query(&basic, case.diff);

    let repos = case
        .repos
        .iter()
        .map(|repo| repository_revisions(repo.id, &repo.name, repo.revs.clone()))
        .collect();

    let mut responses: BTreeMap<String, VecDeque<ResponseSpec>> = BTreeMap::new();
    for spec in &case.responses {
        responses
            .entry(spec.repo.clone())
            .or_default()
            .push_back(spec.clone());
    }

    let gitserver = FakeGitserver {
        responses: RefCell::new(responses),
        requests: RefCell::new(Vec::new()),
    };
    let db = FakeDb {
        users: case.users.clone(),
        username_error: (!case.db_username_error.is_empty())
            .then(|| case.db_username_error.clone()),
        email_error: (!case.db_email_error.is_empty()).then(|| case.db_email_error.clone()),
    };
    let commit_clients = CommitRuntimeClients {
        db: &db,
        gitserver: &gitserver,
        leaf: None,
    };

    let streamer = NoopStreamer;
    let lister = NoopLister;
    let clients = RuntimeClients {
        db: &lister,
        zoekt: &streamer,
        max_repos: None,
        searcher: None,
        commit: Some(&commit_clients),
        repo_pager: None,
    };

    let mut node = JobNode::commit(query, repos, case.diff, case.limit);
    let (events, alert, error) = run_node(&mut node, case, &clients);

    let requests: Vec<Value> = gitserver
        .requests
        .borrow()
        .iter()
        .map(request_out)
        .collect();

    let mut map = base(case);
    map.insert("events".to_owned(), events_out(&events));
    map.insert("alert".to_owned(), alert_out(&alert));
    map.insert("error".to_owned(), error_out(&error));
    map.insert("requests".to_owned(), Value::Array(requests));
    Value::Object(map)
}

fn main_run() {
    if std::env::var("SG_M9IF_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IF_CORPUS").expect("M9IF_CORPUS");
    let out_path = std::env::var("M9IF_OUT").expect("M9IF_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "alert" => run_alert(case),
            "select" => run_select(case),
            "commit" => run_commit(case),
            other => panic!("unknown case kind {other}"),
        })
        .collect();

    let encoded = serde_json::to_vec(&out).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}

#[test]
fn search_jobutil_alert_select_diff() {
    main_run();
}

#[test]
fn commit_leaf_without_clients_reports_not_ported() {
    let query = pb::QueryNode {
        value: Some(pb::query_node::Value::Boolean(pb::BooleanNode {
            value: true,
        })),
    };
    let mut node = JobNode::commit(query, Vec::new(), false, 10);
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
    let (alert, error) = node.run_with_alert(&ctx, &clients, &mut |_| {});
    assert!(alert.is_none());
    assert_eq!(
        error.expect("not-ported error").to_string(),
        "CommitSearchJob execution is not ported (M9.8if)"
    );
}
