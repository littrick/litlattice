//! Commit/diff search job (`internal/search/commit/commit.go:27-354`).
//!
//! Ported pieces:
//!
//! * [`SearchJob`] fields and [`SearchJob::run`] (per-repo
//!   `gitserver.Search` request, `onMatches` → [`convert::CommitMatch`],
//!   `HandleRepoSearchResult` status/stats event, first-error propagation);
//! * [`SearchJob::name`] / [`SearchJob::attributes`]
//!   (`CommitSearchJob`/`DiffSearchJob`);
//! * [`SearchJob::expand_usernames`] and [`expand_usernames_to_emails`]
//!   (`@user` → verified emails, regex-quoted and unioned);
//! * [`query_to_git_query`] (reuses
//!   [`crate::search_jobutil_tree::query_to_git_query`] and translates the
//!   tree to the gitserver `QueryNode` proto);
//! * [`protocol_match_to_commit_match`].
//!
//! Not ported / injected:
//!
//! * the CodeMonitor enterprise hook — [`SearchJob::code_monitor_search_wrapper`]
//!   is `None` by default, which is the baseline's no-hook path;
//! * real database/gRPC adapters: [`CommitUserDB`] and [`GitserverClient`] are
//!   injectable traits (the differential harness uses fakes);
//! * `StartSpan`/tracing and the parallel `conc/pool` execution. `run` executes
//!   repositories serially and returns the first error; the baseline's
//!   `WithFirstError` reports the same error, but concurrent event ordering is
//!   intentionally not reproduced.
//! * `result.CommitMatch`'s `Parents`, structured `Diff`, `ModifiedFiles` and
//!   `Refs`: [`convert::CommitMatch`] has no such fields and this slice does
//!   not change that public struct.
//!
//! See `docs/overviews/m9_8ia_commit_search_job_overview.md`.

use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;

use prost_types::Timestamp;
use regex::Regex;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use sg_proto::gitserver as pb;
use sg_search_query::basic::Basic;
use sg_zoekt::indexed_repo_revs::{RepositoryRevisions, RepositoryRevisionsRepo};

use crate::search_job::{JobContext, JobError, Verbosity};
use crate::search_jobutil_filters::parse_diff_string;
use crate::search_jobutil_tree::{
    query_to_git_query as tree_query_to_git_query, GitNode, GitOperatorKind,
};
use crate::search_searcher_client::{SearcherError, SearcherErrorKind};
use crate::search_searcher_jobs::handle_repo_search_result;
use crate::search_stream::convert::{
    quote_meta, CommitMatch, GitCommit, Match, MatchedString, MinimalRepo, Signature,
};
use crate::search_stream::events::{Location, Range};
use crate::search_stream::progress::{SearchEvent, Stats};

/// `DoSearchFunc`: run one (possibly hook-wrapped) git search request.
pub type DoSearchFunc<'a> = dyn FnMut(&pb::SearchRequest) -> Result<(), JobError> + 'a;

/// `CodeMonitorHook` (`commit.go:40-45`). The enterprise wrapper is out of
/// scope; the type is kept so a caller can inject it. `None` on
/// [`SearchJob::code_monitor_search_wrapper`] means the default `doSearch`.
pub type CodeMonitorHook = dyn Fn(
        &JobContext,
        &dyn CommitUserDB,
        &dyn GitserverClient,
        &pb::SearchRequest,
        u32,
        &mut DoSearchFunc<'_>,
    ) -> Result<(), JobError>
    + Send
    + Sync;

/// Shared handle to a [`CodeMonitorHook`]. The job tree stores the hook on
/// `SearchJob.CodeMonitorSearchWrapper` (`commit.go:27-45`); an `Arc` lets the
/// same hook be attached to the tree while [`CodeMonitorHookHandle`] stays
/// `Debug`/`PartialEq` for [`crate::search_jobutil_tree::JobNode`].
#[derive(Clone)]
pub struct CodeMonitorHookHandle(Arc<CodeMonitorHook>);

impl CodeMonitorHookHandle {
    pub fn new(hook: Arc<CodeMonitorHook>) -> Self {
        Self(hook)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call(
        &self,
        ctx: &JobContext,
        db: &dyn CommitUserDB,
        gitserver: &dyn GitserverClient,
        request: &pb::SearchRequest,
        repo_id: u32,
        do_search: &mut DoSearchFunc<'_>,
    ) -> Result<(), JobError> {
        (self.0)(ctx, db, gitserver, request, repo_id, do_search)
    }
}

impl fmt::Debug for CodeMonitorHookHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CodeMonitorHook")
    }
}

impl PartialEq for CodeMonitorHookHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CodeMonitorHookHandle {}

/// `GitserverClient.Search` (`commit.go:42-45`). `on_matches` receives each
/// streamed batch of commit matches; the return value is `limitHit`.
///
/// Errors use [`SearcherError`]/[`SearcherErrorKind`], the same classification
/// [`handle_repo_search_result`] consumes; [`searcher_error_from_git`] adapts
/// the real [`sg_git::Error`].
pub trait GitserverClient {
    fn search(
        &self,
        request: &pb::SearchRequest,
        on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError>;

    /// `gitserver.Client.ResolveRevision` (`commit.go:42-45`), used by the
    /// code-monitor hook. `RevisionNotFound` is distinguished through
    /// [`SearcherErrorKind::RevisionNotFound`]; the default reports an
    /// explicit unsupported error so injected test clients keep working.
    fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
        ensure_revision: bool,
    ) -> Result<String, SearcherError> {
        let _ = (repo, rev, ensure_revision);
        Err(SearcherError::other(
            "gitserver ResolveRevision is not supported by this client",
        ))
    }
}

/// The subset of `db.Users()`/`db.UserEmails()` username expansion reads.
pub trait CommitUserDB {
    /// `Users().GetByUsername`; `None` is `errcode.IsNotFound`.
    fn get_user_id_by_username(&self, username: &str) -> Result<Option<i32>, JobError>;

    /// `UserEmails().ListByUser({UserID})`.
    fn list_emails_by_user(&self, user_id: i32) -> Result<Vec<CommitUserEmail>, JobError>;
}

/// `*database.UserEmail`, reduced to the fields expansion reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitUserEmail {
    pub email: String,
    /// `VerifiedAt != nil`.
    pub verified: bool,
}

/// `job.RuntimeClients` subset for this job.
pub struct CommitRuntimeClients<'a> {
    pub db: &'a dyn CommitUserDB,
    pub gitserver: &'a dyn GitserverClient,
}

/// `commit.SearchJob` (`commit.go:27-37`).
pub struct SearchJob {
    /// `Query` (a gitserver `QueryNode` tree).
    pub query: pb::QueryNode,
    /// `Repos`; the zoekt minimal `RepositoryRevisions` shape is reused.
    pub repos: Vec<RepositoryRevisions>,
    pub diff: bool,
    pub limit: i64,
    pub include_modified_files: bool,
    pub concurrency: usize,
    /// `CodeMonitorSearchWrapper`; `None` runs `doSearch` directly.
    pub code_monitor_search_wrapper: Option<CodeMonitorHookHandle>,
}

impl SearchJob {
    /// `Job.Run` (`commit.go:47-108`): expand usernames, then search each
    /// repository in order. Serial execution is documented in the module docs.
    pub fn run(
        &mut self,
        _ctx: &JobContext,
        clients: &CommitRuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        self.expand_usernames(clients.db)?;

        let mut first_error = None;
        for repo_rev in &self.repos {
            // Skip repos for which no revisions were resolved.
            if repo_rev.revs.is_empty() {
                continue;
            }

            let request = pb::SearchRequest {
                repo: repo_rev.repo.name.clone(),
                revisions: repo_rev
                    .revs
                    .iter()
                    .map(|rev| pb::RevisionSpecifier {
                        rev_spec: rev.clone(),
                    })
                    .collect(),
                limit: self.limit,
                include_diff: self.diff,
                include_modified_files: self.include_modified_files,
                query: Some(self.query.clone()),
            };
            let repo = MinimalRepo {
                id: repo_rev.repo.id as i32,
                name: repo_rev.repo.name.clone(),
            };
            let diff = self.diff;

            let result = if let Some(hook) = &self.code_monitor_search_wrapper {
                let mut do_search = |args: &pb::SearchRequest| {
                    do_search(args, repo_rev, &repo, diff, clients, stream)
                };
                hook.call(
                    _ctx,
                    clients.db,
                    clients.gitserver,
                    &request,
                    repo_rev.repo.id,
                    &mut do_search,
                )
            } else {
                do_search(&request, repo_rev, &repo, diff, clients, stream)
            };

            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// `SearchJob.Name` (`commit.go:110-115`).
    pub fn name(&self) -> &'static str {
        if self.diff {
            "DiffSearchJob"
        } else {
            "CommitSearchJob"
        }
    }

    /// `SearchJob.Attributes` (`commit.go:117-132`).
    pub fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if verbosity == Verbosity::Max {
            res.push((
                "includeModifiedFiles".to_owned(),
                self.include_modified_files.to_string(),
            ));
        }
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            res.push(("query".to_owned(), query_node_string(&self.query)));
            res.push(("diff".to_owned(), self.diff.to_string()));
            res.push(("limit".to_owned(), self.limit.to_string()));
        }
        res
    }

    /// `SearchJob.ExpandUsernames` (`commit.go:137-163`).
    pub fn expand_usernames(&mut self, db: &dyn CommitUserDB) -> Result<(), JobError> {
        expand_node(&mut self.query, db)
    }
}

/// One repository's `doSearch` (`commit.go:80-90`).
fn do_search(
    request: &pb::SearchRequest,
    repo_rev: &RepositoryRevisions,
    repo: &MinimalRepo,
    diff: bool,
    clients: &CommitRuntimeClients<'_>,
    stream: &mut dyn FnMut(SearchEvent),
) -> Result<(), JobError> {
    let mut on_matches = |batch: Vec<pb::CommitMatch>| {
        let results: Vec<Match> = batch
            .iter()
            .map(|protocol_match| {
                Match::Commit(protocol_match_to_commit_match(repo, diff, protocol_match))
            })
            .collect();
        stream(SearchEvent {
            results,
            stats: Stats::default(),
        });
    };

    let (limit_hit, error) = match clients.gitserver.search(request, &mut on_matches) {
        Ok(limit_hit) => (limit_hit, None),
        Err(error) => (false, Some(error)),
    };

    // `HandleRepoSearchResult` turns non-fatal errors into status bits and
    // returns a `fatal` error only for errors the job should propagate.
    let (status, fatal) =
        handle_repo_search_result(repo.id, &repo_rev.revs, limit_hit, false, error.as_ref());
    stream(SearchEvent {
        results: Vec::new(),
        stats: Stats {
            is_limit_hit: limit_hit,
            status,
            ..Stats::default()
        },
    });

    match fatal {
        Some(error) => Err(JobError(error.message)),
        None => Ok(()),
    }
}

/// Adapter from the real gitserver client error to the classification used by
/// [`handle_repo_search_result`].
pub fn searcher_error_from_git(error: sg_git::Error) -> SearcherError {
    let kind = match &error {
        sg_git::Error::RepoNotFound {
            clone_in_progress, ..
        } => SearcherErrorKind::RepoNotFound {
            clone_in_progress: *clone_in_progress,
        },
        sg_git::Error::RevisionNotFound { .. } => SearcherErrorKind::RevisionNotFound,
        sg_git::Error::NotFound(_) | sg_git::Error::FileNotExist { .. } => {
            SearcherErrorKind::NotFound
        }
        sg_git::Error::Message(_) => SearcherErrorKind::Other,
    };
    SearcherError {
        kind,
        message: error.to_string(),
    }
}

// ---------------------------------------------------------------------------
// ExpandUsernames
// ---------------------------------------------------------------------------

fn expand_node(node: &mut pb::QueryNode, db: &dyn CommitUserDB) -> Result<(), JobError> {
    let Some(value) = node.value.as_mut() else {
        return Ok(());
    };
    match value {
        pb::query_node::Value::AuthorMatches(matches) => expand_expr(&mut matches.expr, db),
        pb::query_node::Value::CommitterMatches(matches) => expand_expr(&mut matches.expr, db),
        pb::query_node::Value::Operator(operator) => {
            for operand in &mut operator.operands {
                expand_node(operand, db)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn expand_expr(expr: &mut String, db: &dyn CommitUserDB) -> Result<(), JobError> {
    let expanded = expand_usernames_to_emails(db, std::slice::from_ref(expr))?;
    *expr = format!("(?:{})", expanded.join(")|(?:"));
    Ok(())
}

/// `expandUsernamesToEmails` (`commit.go:165-211`).
pub fn expand_usernames_to_emails(
    db: &dyn CommitUserDB,
    values: &[String],
) -> Result<Vec<String>, JobError> {
    let mut expanded_values = Vec::with_capacity(values.len());
    for value in values {
        match expand_one(db, value)? {
            // Not a username reference, or the user does not exist.
            None => expanded_values.push(value.clone()),
            Some(emails) => expanded_values.extend(emails),
        }
    }
    Ok(expanded_values)
}

fn expand_one(db: &dyn CommitUserDB, value: &str) -> Result<Option<Vec<String>>, JobError> {
    let Some(username) = value.strip_prefix('@') else {
        return Ok(None);
    };
    let Some(user_id) = db.get_user_id_by_username(username)? else {
        return Ok(None);
    };
    let emails = db.list_emails_by_user(user_id)?;
    Ok(Some(
        emails
            .into_iter()
            .filter(|email| email.verified)
            .map(|email| quote_meta(&email.email))
            .collect(),
    ))
}

// ---------------------------------------------------------------------------
// QueryToGitQuery
// ---------------------------------------------------------------------------

/// `commit.QueryToGitQuery` (`commit.go:213-236`) translated to the gitserver
/// `QueryNode` proto. The predicate tree itself is built by
/// [`crate::search_jobutil_tree::query_to_git_query`].
pub fn query_to_git_query(basic: &Basic, diff: bool) -> pb::QueryNode {
    let node = tree_query_to_git_query(basic, diff);
    git_node_to_proto(&node)
}

fn git_node_to_proto(node: &GitNode) -> pb::QueryNode {
    use pb::query_node::Value;

    let value = match node {
        GitNode::Boolean(value) => Value::Boolean(pb::BooleanNode { value: *value }),
        GitNode::AuthorMatches { expr, ignore_case } => {
            Value::AuthorMatches(pb::AuthorMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitNode::CommitterMatches { expr, ignore_case } => {
            Value::CommitterMatches(pb::CommitterMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitNode::CommitBefore { time } => Value::CommitBefore(pb::CommitBeforeNode {
            timestamp: Some(parse_git_timestamp(time)),
        }),
        GitNode::CommitAfter { time } => Value::CommitAfter(pb::CommitAfterNode {
            timestamp: Some(parse_git_timestamp(time)),
        }),
        GitNode::MessageMatches { expr, ignore_case } => {
            Value::MessageMatches(pb::MessageMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitNode::DiffMatches { expr, ignore_case } => Value::DiffMatches(pb::DiffMatchesNode {
            expr: expr.clone(),
            ignore_case: *ignore_case,
        }),
        GitNode::DiffModifiesFile { expr, ignore_case } => {
            Value::DiffModifiesFile(pb::DiffModifiesFileNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitNode::Operator { kind, operands } => Value::Operator(pb::OperatorNode {
            kind: match kind {
                GitOperatorKind::And => pb::OperatorKind::And as i32,
                GitOperatorKind::Or => pb::OperatorKind::Or as i32,
                GitOperatorKind::Not => pb::OperatorKind::Not as i32,
            },
            operands: operands.iter().map(git_node_to_proto).collect(),
        }),
    };
    pb::QueryNode { value: Some(value) }
}

/// `gitprotocol.Node.String()` for the cases the commit job can produce.
pub fn query_node_string(node: &pb::QueryNode) -> String {
    use pb::query_node::Value;

    match node.value.as_ref() {
        Some(Value::Boolean(node)) => format!("*protocol.Boolean({})", node.value),
        Some(Value::AuthorMatches(node)) => {
            format!("*protocol.AuthorMatches({})", node.expr)
        }
        Some(Value::CommitterMatches(node)) => {
            format!("*protocol.CommitterMatches({})", node.expr)
        }
        Some(Value::CommitBefore(node)) => {
            format!(
                "*protocol.CommitBefore({})",
                go_time_string(node.timestamp.as_ref())
            )
        }
        Some(Value::CommitAfter(node)) => {
            format!(
                "*protocol.CommitAfter({})",
                go_time_string(node.timestamp.as_ref())
            )
        }
        Some(Value::MessageMatches(node)) => {
            format!("*protocol.MessageMatches({})", node.expr)
        }
        Some(Value::DiffMatches(node)) => format!("*protocol.DiffMatches({})", node.expr),
        Some(Value::DiffModifiesFile(node)) => {
            format!("*protocol.DiffModifiesFile({})", node.expr)
        }
        Some(Value::Operator(operator)) => {
            let (separator, prefix) = match pb::OperatorKind::try_from(operator.kind) {
                Ok(pb::OperatorKind::And) => (" AND ", ""),
                Ok(pb::OperatorKind::Or) => (" OR ", ""),
                Ok(pb::OperatorKind::Not) => (" AND NOT ", "NOT "),
                _ => (" AND ", ""),
            };
            let rendered: Vec<String> = operator.operands.iter().map(query_node_string).collect();
            format!("({prefix}{})", rendered.join(separator))
        }
        None => String::new(),
    }
}

// ---------------------------------------------------------------------------
// protocolMatchToCommitMatch
// ---------------------------------------------------------------------------

/// `protocolMatchToCommitMatch` (`commit.go:322-354`).
///
/// The full `result.CommitMatch` shape is modelled: `Parents`, `Refs`,
/// `SourceRefs`, the parsed structured [`Diff`](convert::CommitMatch::diff) and
/// `ModifiedFiles` are populated from the protocol match; the streaming event
/// conversion ignores them, so existing event payloads are unchanged.
pub fn protocol_match_to_commit_match(
    repo: &MinimalRepo,
    diff: bool,
    protocol_match: &pb::CommitMatch,
) -> CommitMatch {
    let message = matched_string(protocol_match.message.as_ref());
    let commit = GitCommit {
        id: protocol_match.oid.clone(),
        message: message.content.clone(),
        author: signature(protocol_match.author.as_ref()),
        committer: signature(protocol_match.committer.as_ref()),
        parents: protocol_match.parents.clone(),
    };

    let (message_preview, diff_preview, structured_diff) = if diff {
        let diff_preview = matched_string(protocol_match.diff.as_ref());
        let structured_diff = parse_diff_string(&diff_preview.content).unwrap_or_default();
        (None, Some(diff_preview), structured_diff)
    } else {
        (Some(message), None, Vec::new())
    };

    CommitMatch {
        commit,
        repo: repo.clone(),
        message_preview,
        diff_preview,
        refs: protocol_match.refs.clone(),
        source_refs: protocol_match.source_refs.clone(),
        diff: structured_diff,
        modified_files: protocol_match.modified_files.clone(),
    }
}

fn signature(signature: Option<&pb::commit_match::Signature>) -> Signature {
    match signature {
        Some(signature) => Signature {
            name: signature.name.clone(),
            email: signature.email.clone(),
            date: timestamp_to_time(signature.date.as_ref()),
        },
        None => Signature {
            name: String::new(),
            email: String::new(),
            date: timestamp_to_time(None),
        },
    }
}

/// `(*timestamppb.Timestamp)(nil).AsTime()` is the Unix epoch.
fn timestamp_to_time(timestamp: Option<&Timestamp>) -> OffsetDateTime {
    let (seconds, nanos) =
        timestamp.map_or((0, 0), |timestamp| (timestamp.seconds, timestamp.nanos));
    OffsetDateTime::from_unix_timestamp(seconds)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .saturating_add(Duration::nanoseconds(i64::from(nanos)))
}

fn matched_string(matched: Option<&pb::commit_match::MatchedString>) -> MatchedString {
    match matched {
        Some(matched) => MatchedString {
            content: matched.content.clone(),
            matched_ranges: matched.ranges.iter().map(range).collect(),
        },
        None => MatchedString {
            content: String::new(),
            matched_ranges: Vec::new(),
        },
    }
}

fn range(range: &pb::commit_match::Range) -> Range {
    Range {
        start: location(range.start.as_ref()),
        end: location(range.end.as_ref()),
    }
}

fn location(location: Option<&pb::commit_match::Location>) -> Location {
    Location {
        offset: location.map_or(0, |location| i64::from(location.offset)),
        line: location.map_or(0, |location| i64::from(location.line)),
        column: location.map_or(0, |location| i64::from(location.column)),
    }
}

/// Build a `RepositoryRevisions` from its parts (convenience for callers).
pub fn repository_revisions(id: u32, name: &str, revs: Vec<String>) -> RepositoryRevisions {
    RepositoryRevisions {
        repo: RepositoryRevisionsRepo {
            id,
            name: name.to_owned(),
        },
        revs,
    }
}

// ---------------------------------------------------------------------------
// gitdate.ParseGitDate (deterministic formats only)
// ---------------------------------------------------------------------------

/// Go's zero `time.Time` (`0001-01-01T00:00:00Z`) in Unix seconds.
const GO_ZERO_SECONDS: i64 = -62_135_596_800;

/// `gitdomain.ParseGitDate` (`internal/gitserver/gitdomain/date.go:17-77`),
/// restricted to the deterministic formats. Natural-language relative dates
/// are not ported: an unrecognized string yields Go's zero time, exactly like
/// any other parse failure.
fn parse_git_timestamp(value: &str) -> Timestamp {
    parse_git_date(value).map_or(
        Timestamp {
            seconds: GO_ZERO_SECONDS,
            nanos: 0,
        },
        timestamp_of,
    )
}

pub(crate) fn parse_git_date(value: &str) -> Option<OffsetDateTime> {
    parse_git_internal(value)
        .or_else(|| parse_date_only(value))
        .or_else(|| {
            OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()
        })
        .or_else(|| parse_naive(value, 'T'))
        .or_else(|| parse_offset_with_space(value))
        .or_else(|| parse_naive(value, ' '))
        .or_else(|| parse_rfc2822(value))
        .or_else(|| parse_dotted(value))
        .or_else(|| parse_slashed(value))
        .or_else(|| parse_simple_date(value))
}

fn timestamp_of(value: OffsetDateTime) -> Timestamp {
    Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

/// `parseGitInternalFormat`: `<epoch-seconds>[ ±hhmm]`. The wall clock of the
/// epoch in UTC is reinterpreted in the given fixed zone, matching Go's
/// `time.Date(..., time.FixedZone(...))`.
fn parse_git_internal(value: &str) -> Option<OffsetDateTime> {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    let pattern = PATTERN.get_or_init(|| {
        Regex::new(r"^([0-9]{5,})( ([+\-])([0-9]{2})([0-9]{2}))?$").expect("valid regexp")
    });
    let captures = pattern.captures(value)?;
    let epoch: i64 = captures.get(1)?.as_str().parse().ok()?;
    let offset_seconds = match captures.get(3) {
        Some(sign) => {
            let hours: i64 = captures.get(4)?.as_str().parse().ok()?;
            let minutes: i64 = captures.get(5)?.as_str().parse().ok()?;
            let offset = hours * 3600 + minutes * 60;
            if sign.as_str() == "-" {
                -offset
            } else {
                offset
            }
        }
        None => 0,
    };
    let utc = OffsetDateTime::from_unix_timestamp(epoch).ok()?;
    Some(utc - Duration::seconds(offset_seconds))
}

fn split_date(value: &str) -> Option<(i32, u8, u8)> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    Some((
        value[0..4].parse().ok()?,
        value[5..7].parse().ok()?,
        value[8..10].parse().ok()?,
    ))
}

fn utc_date(year: i32, month: u8, day: u8) -> Option<OffsetDateTime> {
    let date = Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()?;
    Some(PrimitiveDateTime::new(date, Time::MIDNIGHT).assume_utc())
}

fn parse_date_only(value: &str) -> Option<OffsetDateTime> {
    let (year, month, day) = split_date(value)?;
    utc_date(year, month, day)
}

/// `YYYY-MM-DDTHH:MM:SS` / `YYYY-MM-DD HH:MM:SS` (no zone → UTC).
fn parse_naive(value: &str, separator: char) -> Option<OffsetDateTime> {
    let bytes = value.as_bytes();
    if bytes.len() != 19 || bytes[10] != separator as u8 || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let (year, month, day) = split_date(&value[..10])?;
    let date = Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()?;
    let time = Time::from_hms(
        value[11..13].parse().ok()?,
        value[14..16].parse().ok()?,
        value[17..19].parse().ok()?,
    )
    .ok()?;
    Some(PrimitiveDateTime::new(date, time).assume_utc())
}

/// `YYYY-MM-DD HH:MM:SSZ07:00` (Go layout with timezone and space).
fn parse_offset_with_space(value: &str) -> Option<OffsetDateTime> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes[10] != b' ' {
        return None;
    }
    let (year, month, day) = split_date(&value[..10])?;
    let date = Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()?;
    let time = Time::from_hms(
        value[11..13].parse().ok()?,
        value[14..16].parse().ok()?,
        value[17..19].parse().ok()?,
    )
    .ok()?;
    let offset = parse_utc_offset(&value[19..])?;
    Some(PrimitiveDateTime::new(date, time).assume_offset(offset))
}

/// `Z` or `±hh:mm`.
fn parse_utc_offset(value: &str) -> Option<UtcOffset> {
    if value == "Z" {
        return Some(UtcOffset::UTC);
    }
    let bytes = value.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hours: i8 = value[1..3].parse().ok()?;
    let minutes: i8 = value[4..6].parse().ok()?;
    UtcOffset::from_hms(sign * hours, sign * minutes, 0).ok()
}

/// `Thu, 02 Jan 2006 15:04:05 -0700`.
fn parse_rfc2822(value: &str) -> Option<OffsetDateTime> {
    let tokens: Vec<&str> = value.split_whitespace().collect();
    if tokens.len() != 6 {
        return None;
    }
    let day: u8 = tokens[1].parse().ok()?;
    let month = month_from_name(tokens[2])?;
    let year: i32 = tokens[3].parse().ok()?;
    let time_parts: Vec<&str> = tokens[4].split(':').collect();
    if time_parts.len() != 3 {
        return None;
    }
    let time = Time::from_hms(
        time_parts[0].parse().ok()?,
        time_parts[1].parse().ok()?,
        time_parts[2].parse().ok()?,
    )
    .ok()?;
    let offset = parse_numeric_offset(tokens[5])?;
    let date = Date::from_calendar_date(year, month, day).ok()?;
    Some(PrimitiveDateTime::new(date, time).assume_offset(offset))
}

/// `±hhmm`.
fn parse_numeric_offset(value: &str) -> Option<UtcOffset> {
    let bytes = value.as_bytes();
    if bytes.len() != 5 {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hours: i8 = value[1..3].parse().ok()?;
    let minutes: i8 = value[3..5].parse().ok()?;
    UtcOffset::from_hms(sign * hours, sign * minutes, 0).ok()
}

/// `YYYY.MM.DD` (tried first) then `DD.MM.YYYY`.
fn parse_dotted(value: &str) -> Option<OffsetDateTime> {
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    if parts[0].len() == 4 {
        let year: i32 = parts[0].parse().ok()?;
        let month: u8 = parts[1].parse().ok()?;
        let day: u8 = parts[2].parse().ok()?;
        utc_date(year, month, day)
    } else if parts[2].len() == 4 {
        let day: u8 = parts[0].parse().ok()?;
        let month: u8 = parts[1].parse().ok()?;
        let year: i32 = parts[2].parse().ok()?;
        utc_date(year, month, day)
    } else {
        None
    }
}

/// `MM/DD/YYYY`.
fn parse_slashed(value: &str) -> Option<OffsetDateTime> {
    let parts: Vec<&str> = value.split('/').collect();
    if parts.len() != 3 || parts[2].len() != 4 {
        return None;
    }
    let month: u8 = parts[0].parse().ok()?;
    let day: u8 = parts[1].parse().ok()?;
    let year: i32 = parts[2].parse().ok()?;
    utc_date(year, month, day)
}

/// `1 november 2020` / `november 1, 2020`.
fn parse_simple_date(value: &str) -> Option<OffsetDateTime> {
    let tokens: Vec<&str> = value.split_whitespace().collect();
    if tokens.len() != 3 {
        return None;
    }
    let strip_comma = |token: &str| token.trim_end_matches(',').to_owned();
    let first = strip_comma(tokens[0]);
    let second = strip_comma(tokens[1]);
    let third = strip_comma(tokens[2]);

    if let Some(month) = month_from_name(&first) {
        let day: u8 = second.parse().ok()?;
        let year: i32 = third.parse().ok()?;
        return utc_date(year, month as u8, day);
    }
    if let Some(month) = month_from_name(&second) {
        let day: u8 = first.parse().ok()?;
        let year: i32 = third.parse().ok()?;
        return utc_date(year, month as u8, day);
    }
    None
}

fn month_from_name(name: &str) -> Option<Month> {
    match name.to_ascii_lowercase().as_str() {
        "january" | "jan" => Some(Month::January),
        "february" | "feb" => Some(Month::February),
        "march" | "mar" => Some(Month::March),
        "april" | "apr" => Some(Month::April),
        "may" => Some(Month::May),
        "june" | "jun" => Some(Month::June),
        "july" | "jul" => Some(Month::July),
        "august" | "aug" => Some(Month::August),
        "september" | "sep" => Some(Month::September),
        "october" | "oct" => Some(Month::October),
        "november" | "nov" => Some(Month::November),
        "december" | "dec" => Some(Month::December),
        _ => None,
    }
}

/// `time.Time.String()` for a UTC instant, e.g. `2020-01-01 00:00:00 +0000 UTC`.
fn go_time_string(timestamp: Option<&Timestamp>) -> String {
    let Some(timestamp) = timestamp else {
        return String::new();
    };
    let Ok(value) = OffsetDateTime::from_unix_timestamp(timestamp.seconds) else {
        return String::new();
    };
    let value = value.to_offset(UtcOffset::UTC);
    let nanos = timestamp.nanos.clamp(0, 999_999_999);
    let fraction = if nanos == 0 {
        String::new()
    } else {
        format!(".{:09}", nanos).trim_end_matches('0').to_owned()
    };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}{fraction} +0000 UTC",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Users;

    impl CommitUserDB for Users {
        fn get_user_id_by_username(&self, username: &str) -> Result<Option<i32>, JobError> {
            Ok((username == "alice").then_some(7))
        }

        fn list_emails_by_user(&self, user_id: i32) -> Result<Vec<CommitUserEmail>, JobError> {
            assert_eq!(user_id, 7);
            Ok(vec![
                CommitUserEmail {
                    email: "alice@example.com".to_owned(),
                    verified: true,
                },
                CommitUserEmail {
                    email: "unverified@example.com".to_owned(),
                    verified: false,
                },
            ])
        }
    }

    fn author_node(expr: &str) -> pb::QueryNode {
        pb::QueryNode {
            value: Some(pb::query_node::Value::AuthorMatches(
                pb::AuthorMatchesNode {
                    expr: expr.to_owned(),
                    ignore_case: true,
                },
            )),
        }
    }

    fn author_expr(node: &pb::QueryNode) -> &str {
        match node.value.as_ref() {
            Some(pb::query_node::Value::AuthorMatches(matches)) => &matches.expr,
            other => panic!("unexpected node {other:?}"),
        }
    }

    #[test]
    fn expand_usernames_quotes_verified_emails() {
        let mut node = author_node("@alice");
        expand_node(&mut node, &Users).unwrap();
        assert_eq!(author_expr(&node), "(?:alice@example\\.com)");
    }

    #[test]
    fn expand_usernames_keeps_unknown_users_and_plain_values() {
        // The baseline wraps every author/committer expression, expanded or not.
        let mut unknown = author_node("@bob");
        expand_node(&mut unknown, &Users).unwrap();
        assert_eq!(author_expr(&unknown), "(?:@bob)");

        let mut plain = author_node("alice");
        expand_node(&mut plain, &Users).unwrap();
        assert_eq!(author_expr(&plain), "(?:alice)");
    }

    #[test]
    fn query_node_string_matches_gitprotocol_format() {
        let node = pb::QueryNode {
            value: Some(pb::query_node::Value::Operator(pb::OperatorNode {
                kind: pb::OperatorKind::And as i32,
                operands: vec![
                    pb::QueryNode {
                        value: Some(pb::query_node::Value::DiffModifiesFile(
                            pb::DiffModifiesFileNode {
                                expr: "foo".to_owned(),
                                ignore_case: false,
                            },
                        )),
                    },
                    author_node("bar"),
                ],
            })),
        };
        assert_eq!(
            query_node_string(&node),
            "(*protocol.DiffModifiesFile(foo) AND *protocol.AuthorMatches(bar))"
        );
    }

    #[test]
    fn parse_git_internal_reinterprets_the_wall_clock() {
        // 1000000000 = 2001-09-09T01:46:40Z.
        let utc = parse_git_timestamp("1000000000");
        assert_eq!(utc.seconds, 1_000_000_000);
        let plus_two = parse_git_timestamp("1000000000 +0200");
        assert_eq!(plus_two.seconds, 1_000_000_000 - 7200);
    }

    #[test]
    fn name_and_attributes_follow_the_baseline() {
        let job = SearchJob {
            query: pb::QueryNode {
                value: Some(pb::query_node::Value::Boolean(pb::BooleanNode {
                    value: true,
                })),
            },
            repos: Vec::new(),
            diff: true,
            limit: 42,
            include_modified_files: true,
            concurrency: 1,
            code_monitor_search_wrapper: None,
        };
        assert_eq!(job.name(), "DiffSearchJob");
        assert_eq!(
            job.attributes(Verbosity::Basic),
            vec![
                ("query".to_owned(), "*protocol.Boolean(true)".to_owned()),
                ("diff".to_owned(), "true".to_owned()),
                ("limit".to_owned(), "42".to_owned()),
            ]
        );
        assert_eq!(
            job.attributes(Verbosity::Max)[0],
            ("includeModifiedFiles".to_owned(), "true".to_owned())
        );
    }
}
