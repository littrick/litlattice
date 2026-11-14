//! M9.8ia differential harness: run the ported commit/diff search job over a
//! corpus with fake database/gitserver clients and emit normalized JSON for a
//! byte-for-byte comparison with the Go oracle
//! (`internal/search/commit/zz_m9ia_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9IA_DIFF=1` enable the harness
//! * `M9IA_CORPUS` corpus JSON
//! * `M9IA_OUT` output JSON

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use prost_types::Timestamp;
use serde::Deserialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_proto::gitserver as pb;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_service_frontend::search_commit_job::{
    query_to_git_query, repository_revisions, CommitRuntimeClients, CommitUserDB, CommitUserEmail,
    GitserverClient, SearchJob,
};
use sg_service_frontend::search_job::{JobContext, JobError, Verbosity};
use sg_service_frontend::search_searcher_client::{SearcherError, SearcherErrorKind};
use sg_service_frontend::search_stream::convert::{CommitMatch, Match};
use sg_service_frontend::search_stream::progress::SearchEvent;

/// Go's zero `time.Time` in Unix seconds.
const GO_ZERO_SECONDS: i64 = -62_135_596_800;

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    diff: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    include_modified_files: bool,
    #[serde(default)]
    concurrency: usize,
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
struct RepoSpec {
    #[serde(default)]
    id: u32,
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
    batches: Vec<Vec<MatchSpec>>,
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
struct MatchSpec {
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
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RangeSpec {
    #[serde(default)]
    start: LocationSpec,
    #[serde(default)]
    end: LocationSpec,
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

fn pb_location(spec: &LocationSpec) -> pb::commit_match::Location {
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
            clone_in_progress: spec.clone_in_progress,
        },
        "revision_not_found" => SearcherErrorKind::RevisionNotFound,
        "not_found" => SearcherErrorKind::NotFound,
        "timeout" => SearcherErrorKind::Timeout,
        "temporary" => SearcherErrorKind::Temporary,
        _ => SearcherErrorKind::Other,
    };
    // The baseline's fatal error text is `RevisionNotFoundError.Error()`.
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
// Normalization
// ---------------------------------------------------------------------------

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

fn commit_json(commit: &CommitMatch) -> Value {
    obj(vec![
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
    let results: Vec<Value> = event
        .results
        .iter()
        .map(|matched| match matched {
            Match::Commit(commit) => commit_json(commit),
            other => panic!("unexpected match: {other:?}"),
        })
        .collect();
    let status: Vec<Value> = event
        .stats
        .status
        .entries()
        .into_iter()
        .map(|(id, status)| arr(vec![Value::from(id), Value::from(status)]))
        .collect();
    obj(vec![
        ("results", Value::Array(results)),
        (
            "stats",
            obj(vec![
                ("is_limit_hit", Value::Bool(event.stats.is_limit_hit)),
                ("status", Value::Array(status)),
            ]),
        ),
    ])
}

fn attributes_json(job: &SearchJob, verbosity: Verbosity) -> Value {
    let map: BTreeMap<String, Value> = job
        .attributes(verbosity)
        .into_iter()
        .map(|(key, value)| (key, Value::String(value)))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn run(case: &Case) -> Value {
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
        users: case
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
        username_error: (!case.db_username_error.is_empty())
            .then(|| case.db_username_error.clone()),
        email_error: (!case.db_email_error.is_empty()).then(|| case.db_email_error.clone()),
    };
    let clients = CommitRuntimeClients {
        db: &db,
        gitserver: &gitserver,
        leaf: None,
    };

    let mut job = SearchJob {
        query,
        repos,
        diff: case.diff,
        limit: case.limit,
        include_modified_files: case.include_modified_files,
        concurrency: if case.concurrency == 0 {
            1
        } else {
            case.concurrency
        },
        code_monitor_search_wrapper: None,
    };

    let mut events: Vec<SearchEvent> = Vec::new();
    let mut stream = |event: SearchEvent| events.push(event);
    let error = job
        .run(&JobContext::default(), &clients, &mut stream)
        .err()
        .map(|error| error.0);

    let requests: Vec<Value> = gitserver
        .requests
        .borrow()
        .iter()
        .map(request_json)
        .collect();

    obj(vec![
        ("name", Value::String(case.name.clone())),
        ("job_name", Value::String(job.name().to_owned())),
        ("attributes_basic", attributes_json(&job, Verbosity::Basic)),
        ("attributes_max", attributes_json(&job, Verbosity::Max)),
        ("requests", Value::Array(requests)),
        (
            "events",
            Value::Array(events.iter().map(event_json).collect()),
        ),
        ("error", error.map_or(Value::Null, Value::String)),
    ])
}

#[test]
fn search_commit_job_diff() {
    if std::env::var("SG_M9IA_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IA_CORPUS").expect("M9IA_CORPUS");
    let out_path = std::env::var("M9IA_OUT").expect("M9IA_OUT");
    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
