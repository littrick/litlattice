//! M9.8im differential harness: assemble the production search runtime bundle
//! (`TreeRuntimeClients` → `TreeJobRunner::with_clients`) with fake clients and
//! run a fixed job tree, emitting normalized JSON for the Go oracle
//! (`internal/search/job/jobutil/zz_m9im_oracle_test.go`).
//!
//! Both sides read the same corpus and compare byte-for-byte.
//!
//! Env:
//!
//! * `SG_M9IM_DIFF=1` enable the harness
//! * `M9IM_CORPUS` corpus JSON
//! * `M9IM_OUT` output JSON

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use prost_types::Timestamp;
use serde::Deserialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_proto::gitserver as pb;
use sg_service_frontend::search_commit_job::{CommitUserDB, CommitUserEmail, GitserverClient};
use sg_service_frontend::search_job::{JobError, JobStreamer, PrivateRepoLister, ReposListOptions};
use sg_service_frontend::search_jobutil_tree::{JobNode, TreeRepoOptions};
use sg_service_frontend::search_runtime::{CommitRuntimeClientsOwned, SearcherRuntimeClients};
use sg_service_frontend::search_searcher_client::{
    SearcherClient, SearcherError, SearcherErrorKind, SearcherGitserver, SearcherLimiter,
    SymbolsClient, SymbolsParameters, SymbolsResponse,
};
use sg_service_frontend::search_stream::convert::Match;
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::SearchEvent;
use sg_service_frontend::search_stream::zoekt_search::{SelectPath, ZoektParameters};
use sg_service_frontend::search_stream::{JobRunner, TreeJobRunner, TreeRuntimeClients};
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

/// Go's zero `time.Time` in Unix seconds.
const GO_ZERO_SECONDS: i64 = -62_135_596_800;

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
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
    max_repos: Option<i64>,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    timeout_nanos: i64,
    #[serde(default)]
    commit: CommitSpec,
}

#[derive(Debug, Default, Deserialize, Clone)]
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

#[derive(Debug, Default, Deserialize)]
struct GlobalQuerySpec {
    #[serde(default)]
    query: QuerySpec,
    #[serde(default)]
    scope: Option<QuerySpec>,
    #[serde(default)]
    include_private: bool,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize, Clone)]
struct RepoRef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
struct BranchSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
}

#[derive(Debug, Default, Deserialize)]
struct LocationSpec {
    #[serde(default)]
    byte_offset: u32,
    #[serde(default)]
    line_number: u32,
    #[serde(default)]
    column: u32,
}

impl LocationSpec {
    fn to_zoekt(&self) -> ZoektLocation {
        ZoektLocation {
            byte_offset: self.byte_offset,
            line_number: self.line_number,
            column: self.column,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ZoektRangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
struct ChunkSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_start: LocationSpec,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    ranges: Vec<ZoektRangeSpec>,
    #[serde(default)]
    symbol_info: Vec<Option<SymbolSpec>>,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
struct ResultSpec {
    #[serde(default)]
    files: Vec<FileSpec>,
    #[serde(default)]
    stats: StatsSpec,
}

// ---------------------------------------------------------------------------
// commit corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct CommitSpec {
    #[serde(default)]
    query: CommitQuerySpec,
    #[serde(default)]
    repos: Vec<CommitRepoSpec>,
    #[serde(default)]
    diff: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    users: Vec<UserSpec>,
    #[serde(default)]
    db_username_error: String,
    #[serde(default)]
    db_email_error: String,
    #[serde(default)]
    responses: Vec<ResponseSpec>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct CommitQuerySpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    value: bool,
    #[serde(default)]
    expr: String,
    #[serde(default)]
    ignore_case: bool,
    #[serde(default)]
    children: Vec<CommitQuerySpec>,
}

#[derive(Debug, Default, Deserialize)]
struct CommitRepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UserSpec {
    #[serde(default)]
    username: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    emails: Vec<MailSpec>,
}

#[derive(Debug, Default, Deserialize)]
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
    batches: Vec<Vec<CommitMatchSpec>>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    error: Option<ErrorSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ErrorSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    clone_in_progress: bool,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct CommitMatchSpec {
    #[serde(default)]
    oid: String,
    #[serde(default)]
    author: SignatureSpec,
    #[serde(default)]
    committer: SignatureSpec,
    #[serde(default)]
    message: MatchedStringSpec,
    #[serde(default)]
    diff: MatchedStringSpec,
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
struct MatchedStringSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    ranges: Vec<ResultRangeSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ResultRangeSpec {
    #[serde(default)]
    start: ResultLocationSpec,
    #[serde(default)]
    end: ResultLocationSpec,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ResultLocationSpec {
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    line: i64,
    #[serde(default)]
    column: i64,
}

// ---------------------------------------------------------------------------
// corpus → zoekt
// ---------------------------------------------------------------------------

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

fn commit_leaf(case: &Case) -> JobNode {
    let repos = case
        .commit
        .repos
        .iter()
        .map(|repo| RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id: repo.id as u32,
                name: repo.name.clone(),
            },
            revs: repo.revs.clone(),
        })
        .collect();
    JobNode::commit(
        commit_query(&case.commit.query),
        repos,
        case.commit.diff,
        case.commit.limit,
    )
}

fn commit_query(spec: &CommitQuerySpec) -> pb::QueryNode {
    use pb::query_node::Value as V;
    let value = match spec.kind.as_str() {
        "boolean" => V::Boolean(pb::BooleanNode { value: spec.value }),
        "author_matches" => V::AuthorMatches(pb::AuthorMatchesNode {
            expr: spec.expr.clone(),
            ignore_case: spec.ignore_case,
        }),
        "committer_matches" => V::CommitterMatches(pb::CommitterMatchesNode {
            expr: spec.expr.clone(),
            ignore_case: spec.ignore_case,
        }),
        "message_matches" => V::MessageMatches(pb::MessageMatchesNode {
            expr: spec.expr.clone(),
            ignore_case: spec.ignore_case,
        }),
        "diff_matches" => V::DiffMatches(pb::DiffMatchesNode {
            expr: spec.expr.clone(),
            ignore_case: spec.ignore_case,
        }),
        "diff_modifies_file" => V::DiffModifiesFile(pb::DiffModifiesFileNode {
            expr: spec.expr.clone(),
            ignore_case: spec.ignore_case,
        }),
        "operator" => V::Operator(pb::OperatorNode {
            kind: match spec.expr.as_str() {
                "and" => pb::OperatorKind::And as i32,
                "or" => pb::OperatorKind::Or as i32,
                "not" => pb::OperatorKind::Not as i32,
                _ => 0,
            },
            operands: spec.children.iter().map(commit_query).collect(),
        }),
        other => panic!("unknown commit query kind {other}"),
    };
    pb::QueryNode { value: Some(value) }
}

fn build_tree(case: &Case) -> JobNode {
    match case.kind.as_str() {
        "noop" => JobNode::noop(),
        "text" => text_leaf(case),
        "symbol" => symbol_leaf(case),
        "global_text" => global_text_leaf(case),
        "log_text" => JobNode::log(text_leaf(case)),
        "limit_text" => JobNode::limit(case.limit, text_leaf(case)),
        "timeout_text" => JobNode::timeout(case.timeout_nanos, text_leaf(case)),
        "log_limit_text" => JobNode::log(JobNode::limit(case.limit, text_leaf(case))),
        "sequential_text_symbol" => {
            JobNode::sequential(false, vec![text_leaf(case), symbol_leaf(case)])
        }
        "commit" => commit_leaf(case),
        "diff" => commit_leaf(case),
        "log_commit" => JobNode::log(commit_leaf(case)),
        "limit_commit" => JobNode::limit(case.limit, commit_leaf(case)),
        "sequential_text_commit" => {
            JobNode::sequential(false, vec![text_leaf(case), commit_leaf(case)])
        }
        "sequential_commit_text" => {
            JobNode::sequential(false, vec![commit_leaf(case), text_leaf(case)])
        }
        other => panic!("unknown case kind {other}"),
    }
}

fn is_global(case: &Case) -> bool {
    case.kind.starts_with("global_")
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

struct FakeStreamer {
    batches: Vec<ZoektSearchResult>,
    error: bool,
    called: Mutex<bool>,
}

impl JobStreamer for FakeStreamer {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        sender: &mut dyn FnMut(ZoektSearchResult),
    ) -> Result<(), JobError> {
        *self.called.lock().expect("called lock") = true;
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
    options: Mutex<Option<ReposListOptions>>,
}

impl PrivateRepoLister for FakeLister {
    fn list_minimal_repos(&self, options: &ReposListOptions) -> Result<Vec<RepoMinimal>, JobError> {
        *self.options.lock().expect("options lock") = Some(options.clone());
        if self.error {
            return Err(JobError("list failed".to_owned()));
        }
        Ok(self.repos.clone())
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

struct FakeGitGitserver {
    responses: Mutex<BTreeMap<String, VecDeque<ResponseSpec>>>,
    requests: Mutex<Vec<pb::SearchRequest>>,
}

impl GitserverClient for FakeGitGitserver {
    fn search(
        &self,
        request: &pb::SearchRequest,
        on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.clone());
        let mut responses = self.responses.lock().expect("responses lock");
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

struct FakeSearcher;

impl SearcherClient for FakeSearcher {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        Ok(vec!["http://searcher.test:1".to_owned()])
    }

    fn search(
        &self,
        _request: &sg_service_frontend::search_searcher_client::protocol::Request,
        _on_match: &mut dyn FnMut(sg_service_frontend::search_searcher_client::protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        Ok(false)
    }
}

impl SearcherLimiter for FakeSearcher {
    fn set_limit(&self, _limit: usize) {}

    fn acquire(&self) -> Result<(), SearcherError> {
        Ok(())
    }
}

impl SearcherGitserver for FakeSearcher {
    fn resolve_revision(&self, _repo: &str, revspec: &str) -> Result<String, SearcherError> {
        Ok(revspec.to_owned())
    }

    fn default_branch(&self, repo: &str) -> Result<String, SearcherError> {
        Ok(format!("refs/heads/{repo}"))
    }
}

impl SymbolsClient for FakeSearcher {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Ok(SymbolsResponse::default())
    }
}

// ---------------------------------------------------------------------------
// commit helpers
// ---------------------------------------------------------------------------

fn timestamp(date: &str) -> Timestamp {
    if date.is_empty() {
        return Timestamp {
            seconds: GO_ZERO_SECONDS,
            nanos: 0,
        };
    }
    let parsed = OffsetDateTime::parse(date, &Rfc3339)
        .unwrap_or_else(|error| panic!("bad date {date}: {error}"));
    Timestamp {
        seconds: parsed.unix_timestamp(),
        nanos: parsed.nanosecond() as i32,
    }
}

fn pb_signature(spec: &SignatureSpec) -> pb::commit_match::Signature {
    pb::commit_match::Signature {
        name: spec.name.clone(),
        email: spec.email.clone(),
        date: Some(timestamp(&spec.date)),
    }
}

fn pb_location(spec: &ResultLocationSpec) -> pb::commit_match::Location {
    pb::commit_match::Location {
        offset: spec.offset as u32,
        line: spec.line as u32,
        column: spec.column as u32,
    }
}

fn pb_matched_string(spec: &MatchedStringSpec) -> pb::commit_match::MatchedString {
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

fn pb_match(spec: &CommitMatchSpec) -> pb::CommitMatch {
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
            clone_in_progress: spec.clone_in_progress,
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
// normalization
// ---------------------------------------------------------------------------

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let map: BTreeMap<String, Value> = pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn arr<T: Into<Value>>(values: Vec<T>) -> Value {
    Value::Array(values.into_iter().map(Into::into).collect())
}

fn lister_json(options: &ReposListOptions) -> Value {
    obj(vec![
        ("only_private", Value::Bool(options.only_private)),
        ("only_forks", Value::Bool(options.only_forks)),
        ("no_forks", Value::Bool(options.no_forks)),
        ("only_archived", Value::Bool(options.only_archived)),
        ("no_archived", Value::Bool(options.no_archived)),
        (
            "exclude_pattern",
            Value::String(options.exclude_pattern.clone()),
        ),
        ("limit", Value::from(options.limit.unwrap_or(0))),
    ])
}

fn node_json(node: &pb::QueryNode) -> Value {
    use pb::query_node::Value as V;
    match node.value.as_ref() {
        Some(V::Boolean(node)) => obj(vec![
            ("type", Value::String("boolean".to_owned())),
            ("value", Value::Bool(node.value)),
        ]),
        Some(V::AuthorMatches(node)) => obj(vec![
            ("type", Value::String("author_matches".to_owned())),
            ("expr", Value::String(node.expr.clone())),
            ("ignore_case", Value::Bool(node.ignore_case)),
        ]),
        Some(V::CommitterMatches(node)) => obj(vec![
            ("type", Value::String("committer_matches".to_owned())),
            ("expr", Value::String(node.expr.clone())),
            ("ignore_case", Value::Bool(node.ignore_case)),
        ]),
        Some(V::CommitBefore(node)) => obj(vec![
            ("type", Value::String("commit_before".to_owned())),
            (
                "seconds",
                Value::from(node.timestamp.as_ref().map_or(0, |t| t.seconds)),
            ),
            (
                "nanos",
                Value::from(node.timestamp.as_ref().map_or(0, |t| t.nanos)),
            ),
        ]),
        Some(V::CommitAfter(node)) => obj(vec![
            ("type", Value::String("commit_after".to_owned())),
            (
                "seconds",
                Value::from(node.timestamp.as_ref().map_or(0, |t| t.seconds)),
            ),
            (
                "nanos",
                Value::from(node.timestamp.as_ref().map_or(0, |t| t.nanos)),
            ),
        ]),
        Some(V::MessageMatches(node)) => obj(vec![
            ("type", Value::String("message_matches".to_owned())),
            ("expr", Value::String(node.expr.clone())),
            ("ignore_case", Value::Bool(node.ignore_case)),
        ]),
        Some(V::DiffMatches(node)) => obj(vec![
            ("type", Value::String("diff_matches".to_owned())),
            ("expr", Value::String(node.expr.clone())),
            ("ignore_case", Value::Bool(node.ignore_case)),
        ]),
        Some(V::DiffModifiesFile(node)) => obj(vec![
            ("type", Value::String("diff_modifies_file".to_owned())),
            ("expr", Value::String(node.expr.clone())),
            ("ignore_case", Value::Bool(node.ignore_case)),
        ]),
        Some(V::Operator(node)) => {
            let kind = match pb::OperatorKind::try_from(node.kind) {
                Ok(pb::OperatorKind::And) => "and",
                Ok(pb::OperatorKind::Or) => "or",
                Ok(pb::OperatorKind::Not) => "not",
                _ => "unspecified",
            };
            obj(vec![
                ("type", Value::String("operator".to_owned())),
                ("kind", Value::String(kind.to_owned())),
                (
                    "operands",
                    Value::Array(node.operands.iter().map(node_json).collect()),
                ),
            ])
        }
        None => Value::Null,
    }
}

fn request_json(request: &pb::SearchRequest) -> Value {
    obj(vec![
        ("repo", Value::String(request.repo.clone())),
        (
            "revisions",
            arr(request
                .revisions
                .iter()
                .map(|revision| revision.rev_spec.clone())
                .collect()),
        ),
        ("limit", Value::from(request.limit)),
        ("include_diff", Value::Bool(request.include_diff)),
        (
            "include_modified_files",
            Value::Bool(request.include_modified_files),
        ),
        (
            "query",
            request.query.as_ref().map_or(Value::Null, node_json),
        ),
    ])
}

fn date_json(value: OffsetDateTime) -> Value {
    obj(vec![
        ("seconds", Value::from(value.unix_timestamp())),
        ("nanos", Value::from(value.nanosecond())),
    ])
}

fn signature_json(signature: &sg_service_frontend::search_stream::convert::Signature) -> Value {
    obj(vec![
        ("name", Value::String(signature.name.clone())),
        ("email", Value::String(signature.email.clone())),
        ("date", date_json(signature.date)),
    ])
}

fn matched_string_json(
    matched: &Option<sg_service_frontend::search_stream::convert::MatchedString>,
) -> Value {
    match matched {
        None => Value::Null,
        Some(matched) => obj(vec![
            ("content", Value::String(matched.content.clone())),
            (
                "ranges",
                Value::Array(
                    matched
                        .matched_ranges
                        .iter()
                        .map(|range| {
                            obj(vec![
                                (
                                    "start",
                                    obj(vec![
                                        ("offset", Value::from(range.start.offset)),
                                        ("line", Value::from(range.start.line)),
                                        ("column", Value::from(range.start.column)),
                                    ]),
                                ),
                                (
                                    "end",
                                    obj(vec![
                                        ("offset", Value::from(range.end.offset)),
                                        ("line", Value::from(range.end.line)),
                                        ("column", Value::from(range.end.column)),
                                    ]),
                                ),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]),
    }
}

fn commit_json(commit: &sg_service_frontend::search_stream::convert::CommitMatch) -> Value {
    obj(vec![
        ("kind", Value::String("commit".to_owned())),
        (
            "commit",
            obj(vec![
                ("id", Value::String(commit.commit.id.clone())),
                ("message", Value::String(commit.commit.message.clone())),
                ("author", signature_json(&commit.commit.author)),
                ("committer", signature_json(&commit.commit.committer)),
            ]),
        ),
        (
            "repo",
            obj(vec![
                ("id", Value::from(commit.repo.id)),
                ("name", Value::String(commit.repo.name.clone())),
            ]),
        ),
        (
            "message_preview",
            matched_string_json(&commit.message_preview),
        ),
        ("diff_preview", matched_string_json(&commit.diff_preview)),
    ])
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
    obj(vec![
        ("results", Value::Array(results)),
        (
            "stats",
            obj(vec![
                ("is_limit_hit", Value::Bool(event.stats.is_limit_hit)),
                (
                    "backends_missing",
                    Value::from(event.stats.backends_missing),
                ),
                (
                    "status",
                    Value::Array(status.into_iter().map(|pair| arr(pair.to_vec())).collect()),
                ),
            ]),
        ),
    ])
}

fn match_json(match_: &Match) -> Value {
    match match_ {
        Match::File(file_match) => obj(vec![
            ("kind", Value::String("file".to_owned())),
            ("file", file_json(&file_match.file)),
            (
                "chunk_matches",
                Value::Array(file_match.chunk_matches.iter().map(chunk_json).collect()),
            ),
            (
                "symbols",
                Value::Array(file_match.symbols.iter().map(symbol_json).collect()),
            ),
            ("path_matches", ranges_json(&file_match.path_matches)),
            (
                "debug",
                match &file_match.debug {
                    Some(debug) => Value::String(debug.clone()),
                    None => Value::Null,
                },
            ),
        ]),
        Match::Repo(repo) => obj(vec![
            ("kind", Value::String("repo".to_owned())),
            ("name", Value::String(repo.name.clone())),
            ("id", Value::from(repo.id)),
        ]),
        Match::Commit(commit) => commit_json(commit),
        _ => obj(vec![("kind", Value::String("unknown".to_owned()))]),
    }
}

fn file_json(file: &sg_service_frontend::search_stream::convert::FileRef) -> Value {
    obj(vec![
        (
            "repo",
            obj(vec![
                ("id", Value::from(file.repo.id)),
                ("name", Value::String(file.repo.name.clone())),
            ]),
        ),
        ("commit_id", Value::String(file.commit_id.clone())),
        ("path", Value::String(file.path.clone())),
        (
            "input_rev",
            Value::String(file.input_rev.clone().unwrap_or_default()),
        ),
        (
            "precise_language",
            Value::String(file.precise_language.clone()),
        ),
    ])
}

fn chunk_json(chunk: &sg_service_frontend::search_stream::convert::ChunkMatch) -> Value {
    obj(vec![
        ("content", Value::String(chunk.content.clone())),
        ("content_start", location_json(&chunk.content_start)),
        ("ranges", ranges_json(&chunk.ranges)),
    ])
}

fn symbol_json(symbol_match: &sg_service_frontend::search_stream::convert::SymbolMatch) -> Value {
    let symbol = &symbol_match.symbol;
    obj(vec![
        ("file", file_json(&symbol_match.file)),
        (
            "symbol",
            obj(vec![
                ("name", Value::String(symbol.name.clone())),
                ("path", Value::String(symbol.path.clone())),
                ("line", Value::from(symbol.line)),
                ("character", Value::from(symbol.character)),
                ("kind", Value::String(symbol.kind.clone())),
                ("language", Value::String(symbol.language.clone())),
                ("parent", Value::String(symbol.parent.clone())),
                ("parent_kind", Value::String(symbol.parent_kind.clone())),
            ]),
        ),
    ])
}

fn location_json(location: &Location) -> Value {
    obj(vec![
        ("offset", Value::from(location.offset)),
        ("line", Value::from(location.line)),
        ("column", Value::from(location.column)),
    ])
}

fn ranges_json(ranges: &[Range]) -> Value {
    Value::Array(
        ranges
            .iter()
            .map(|range| {
                obj(vec![
                    ("start", location_json(&range.start)),
                    ("end", location_json(&range.end)),
                ])
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// driver
// ---------------------------------------------------------------------------

fn run_case(case: &Case) -> Value {
    let streamer = Arc::new(FakeStreamer {
        batches: case.batches.iter().map(to_result).collect(),
        error: case.stream_error,
        called: Mutex::new(false),
    });
    let lister = Arc::new(FakeLister {
        repos: case
            .private_repos
            .iter()
            .map(|repo| RepoMinimal {
                id: repo.id as u32,
                name: repo.name.clone(),
            })
            .collect(),
        error: case.private_repos_error,
        options: Mutex::new(None),
    });
    let db = Arc::new(FakeDb {
        users: case
            .commit
            .users
            .iter()
            .map(|user| UserSpec {
                username: user.username.clone(),
                id: user.id,
                emails: user
                    .emails
                    .iter()
                    .map(|email| MailSpec {
                        address: email.address.clone(),
                        verified: email.verified,
                    })
                    .collect(),
            })
            .collect(),
        username_error: (!case.commit.db_username_error.is_empty())
            .then(|| case.commit.db_username_error.clone()),
        email_error: (!case.commit.db_email_error.is_empty())
            .then(|| case.commit.db_email_error.clone()),
    });
    let mut responses: BTreeMap<String, VecDeque<ResponseSpec>> = BTreeMap::new();
    for spec in &case.commit.responses {
        responses
            .entry(spec.repo.clone())
            .or_default()
            .push_back(spec.clone());
    }
    let gitserver = Arc::new(FakeGitGitserver {
        responses: Mutex::new(responses),
        requests: Mutex::new(Vec::new()),
    });

    let runtime = TreeRuntimeClients {
        db: lister.clone(),
        zoekt: streamer.clone(),
        max_repos: case.max_repos,
        searcher: Some(Arc::new(SearcherRuntimeClients {
            client: Arc::new(FakeSearcher),
            limiter: Arc::new(FakeSearcher),
            gitserver: Arc::new(FakeSearcher),
            symbols: Arc::new(FakeSearcher),
        })),
        commit: Some(Arc::new(CommitRuntimeClientsOwned {
            db: db.clone(),
            gitserver: gitserver.clone(),
        })),
    };
    let runner = TreeJobRunner::with_clients(runtime);
    let mut tree = build_tree(case);
    let mut events: Vec<SearchEvent> = Vec::new();
    let output = runner.run(&mut tree, &mut |event| events.push(event));

    let lister_out = if is_global(case) {
        let guard = lister.options.lock().expect("options lock");
        match guard.as_ref() {
            Some(options) => lister_json(options),
            None => Value::Null,
        }
    } else {
        Value::Null
    };

    let requests: Vec<Value> = gitserver
        .requests
        .lock()
        .expect("requests lock")
        .iter()
        .map(request_json)
        .collect();

    let stream_called = *streamer.called.lock().expect("called lock");
    let alert = output.alert.is_some();

    obj(vec![
        ("name", Value::String(case.name.clone())),
        (
            "error",
            Value::String(output.error.map(|error| error.message).unwrap_or_default()),
        ),
        ("stream_called", Value::Bool(stream_called)),
        ("alert", Value::Bool(alert)),
        (
            "events",
            Value::Array(events.iter().map(event_json).collect()),
        ),
        ("lister", lister_out),
        ("commit_requests", Value::Array(requests)),
    ])
}

#[test]
fn search_runtime_assembly_diff() {
    if std::env::var("SG_M9IM_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9IM_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9IM_CORPUS").expect("M9IM_CORPUS must be set");
    let out_path = std::env::var("M9IM_OUT").expect("M9IM_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run_case).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
