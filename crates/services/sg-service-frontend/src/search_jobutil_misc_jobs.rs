//! jobutil sanitize / sub-repo permissions / log jobs
//! (`internal/search/job/jobutil/{sanitize_job,sub_repo_perms_job,log_job}.go`).
//!
//! * [`SanitizeJob`] (`sanitize_job.go`): drops chunk ranges, commit diffs and
//!   whole file matches whose text matches any sanitize pattern. Symbol file
//!   matches bypass chunk sanitization; repo matches pass through untouched and
//!   any other match type (including owner matches) is dropped.
//! * [`SubRepoPermsFilterJob`] (`sub_repo_perms_job.go`): the
//!   `applySubRepoFiltering` walk over file/commit/repo matches with the
//!   baseline's `errCache`, `Read` checks, `CanReadAnyPath` + `diffIsEmpty`
//!   rule and generic `subRepoFilterFunc` error.
//! * [`LogJob`] (`log_job.go`): records the search-latency, `file:has.owner`
//!   and `select:file.owners` events and forwards the child stream unchanged.
//!
//! The real dependencies do not exist in this port, so they are injected:
//! [`SubRepoPermissionChecker`] (authz DB/cache), [`Actor`], commit
//! `ModifiedFiles` (only populated by the backend when `IncludeModifiedFiles`
//! is requested) and a [`PermsLogSink`]; for the log job a
//! [`BackendEventLogger`] replaces `DB`/telemetry stores and [`log_event`]
//! returns a structured [`LogRecord`] instead of writing to an OTel/log
//! backend. `JobContext`/`RuntimeClients` are reused from
//! [`crate::search_job`] without modifying that module.
//!
//! See `docs/overviews/m9_8hm_jobutil_misc_jobs_overview.md`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use regex::Regex;
use sg_search_query::basic::{q_string_value, Parameters};
use sg_search_query::fields::{FIELD_FILE, FIELD_SELECT, FIELD_TYPE};
use sg_search_query::labels::Labels;
use sg_search_query::node::{visit_field, Node};
use sg_search_query::parser::SearchType;
use sg_search_query::predicate::parse_as_predicate;
use sg_search_query::transformer::to_basic_query;

use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_stream::convert::{ChunkMatch, CommitMatch, FileMatch, Match, MatchedString};
use crate::search_stream::progress::SearchEvent;

// ---------------------------------------------------------------------------
// `sanitize_job.go`
// ---------------------------------------------------------------------------

/// `sanitizeJob` (`sanitize_job.go:22-25`).
pub struct SanitizeJob {
    sanitize_patterns: Vec<Regex>,
    child: Box<dyn SearchJob>,
}

impl SanitizeJob {
    /// `NewSanitizeJob`.
    pub fn new(sanitize_patterns: Vec<Regex>, child: Box<dyn SearchJob>) -> Self {
        Self {
            sanitize_patterns,
            child,
        }
    }

    /// `sanitizeEvent`.
    pub fn sanitize_event(&self, event: SearchEvent) -> SearchEvent {
        sanitize_event(event, &self.sanitize_patterns)
    }
}

impl SearchJob for SanitizeJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Self {
            sanitize_patterns,
            child,
        } = self;
        let mut filtered = |event: SearchEvent| {
            stream(sanitize_event(event, sanitize_patterns.as_slice()));
        };
        child.run(ctx, clients, &mut filtered)
    }

    fn name(&self) -> &'static str {
        "SanitizeJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// `sanitizeEvent` (`sanitize_job.go:57-79`).
pub fn sanitize_event(event: SearchEvent, patterns: &[Regex]) -> SearchEvent {
    let mut event = event;
    let results = std::mem::take(&mut event.results);
    let mut sanitized = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Match::File(fm) => {
                if let Some(fm) = sanitize_file_match(fm, patterns) {
                    sanitized.push(Match::File(fm));
                }
            }
            Match::Commit(cm) => {
                if let Some(cm) = sanitize_commit_match(cm, patterns) {
                    sanitized.push(Match::Commit(cm));
                }
            }
            Match::Repo(rm) => sanitized.push(Match::Repo(rm)),
            // The baseline's switch has no OwnerMatch case, and its default
            // branch drops the result.
            Match::Owner(_) => {}
        }
    }
    event.results = sanitized;
    event
}

/// `sanitizeFileMatch` (`sanitize_job.go:81-100`).
fn sanitize_file_match(mut fm: FileMatch, patterns: &[Regex]) -> Option<FileMatch> {
    if !fm.symbols.is_empty() {
        return Some(fm);
    }

    let chunks = std::mem::take(&mut fm.chunk_matches);
    fm.chunk_matches = chunks
        .into_iter()
        .filter_map(|chunk| {
            let chunk = sanitize_chunk(chunk, patterns);
            if chunk.ranges.is_empty() {
                None
            } else {
                Some(chunk)
            }
        })
        .collect();

    if fm.chunk_matches.is_empty() {
        return None;
    }
    Some(fm)
}

/// `sanitizeChunk` (`sanitize_job.go:102-114`).
fn sanitize_chunk(mut chunk: ChunkMatch, patterns: &[Regex]) -> ChunkMatch {
    let matched = matched_content(&chunk);
    let mut ranges = Vec::with_capacity(chunk.ranges.len());
    for (i, value) in matched.iter().enumerate() {
        if matches_any(value, patterns) {
            continue;
        }
        if let Some(range) = chunk.ranges.get(i) {
            ranges.push(*range);
        }
    }
    chunk.ranges = ranges;
    chunk
}

/// `sanitizeCommitMatch` (`sanitize_job.go:116-124`).
fn sanitize_commit_match(cm: CommitMatch, patterns: &[Regex]) -> Option<CommitMatch> {
    if let Some(preview) = &cm.diff_preview {
        if matches_any(&preview.content, patterns) {
            return None;
        }
    }
    Some(cm)
}

/// `matchesAnySanitizePattern` (`sanitize_job.go:126-133`).
fn matches_any(value: &str, patterns: &[Regex]) -> bool {
    patterns.iter().any(|re| re.is_match(value))
}

/// `ChunkMatch.MatchedContent` (`internal/search/result/file.go:257-275`),
/// with the out-of-range offsets clamped to the content bounds. Go additionally
/// panics on negative offsets; that undefined input is not exercised.
fn matched_content(chunk: &ChunkMatch) -> Vec<String> {
    let content_start = chunk.content_start.offset;
    let len = chunk.content.len() as i64;
    chunk
        .ranges
        .iter()
        .map(|range| {
            let start = range.start.offset - content_start;
            let end = range.end.offset - content_start;
            let start = start.clamp(0, len) as usize;
            let end = end.clamp(0, len) as usize;
            if start <= end {
                chunk.content.get(start..end).unwrap_or_default().to_owned()
            } else {
                String::new()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// `sub_repo_perms_job.go`
// ---------------------------------------------------------------------------

/// `authz.Perms` (`internal/authz/perms.go:17-33`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Perms(pub u32);

impl Perms {
    pub const NONE: Perms = Perms(0);
    pub const READ: Perms = Perms(1);
    pub const WRITE: Perms = Perms(2);

    /// `Perms.Include`.
    pub fn include(self, other: Perms) -> bool {
        self.0 & other.0 == other.0
    }
}

/// `authz.RepoContent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoContent {
    pub repo: String,
    pub path: String,
}

/// An injected authz error. `is_context` models `err == ctx.Err()`: enabled
/// checks that return the context error are not logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckerError {
    pub message: String,
    pub is_context: bool,
}

impl CheckerError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            is_context: false,
        }
    }

    pub fn context(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            is_context: true,
        }
    }
}

impl fmt::Display for CheckerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CheckerError {}

/// `authz.FilePermissionFunc`.
pub type FilePermissionFunc = dyn Fn(&str) -> Result<Perms, CheckerError>;

/// `authz.SubRepoPermissionChecker`, reduced to the methods used by
/// `applySubRepoFiltering` and `CanReadAnyPath`.
pub trait SubRepoPermissionChecker {
    /// `Enabled`.
    fn enabled(&self) -> bool;

    /// `EnabledForRepoID`.
    fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, CheckerError>;

    /// `Permissions`.
    fn permissions(&self, user_id: i32, content: &RepoContent) -> Result<Perms, CheckerError>;

    /// `FilePermissionsFunc`.
    fn file_permissions_func(
        &self,
        user_id: i32,
        repo: &str,
    ) -> Result<Box<FilePermissionFunc>, CheckerError>;
}

/// `actor.Actor`, restricted to the fields these jobs read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Actor {
    pub uid: i32,
    pub internal: bool,
    pub mock_user: bool,
}

impl Actor {
    /// `Actor.IsAuthenticated`.
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }

    /// `Actor.IsInternal`.
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// `Actor.IsMockUser`.
    pub fn is_mock_user(&self) -> bool {
        self.mock_user
    }
}

/// `authz.SubRepoEnabled`.
pub fn sub_repo_enabled(checker: &dyn SubRepoPermissionChecker) -> bool {
    checker.enabled()
}

/// `actorSubRepoEnabled` (`internal/authz/sub_repo_perms.go:112-123`).
fn actor_sub_repo_enabled(
    checker: &dyn SubRepoPermissionChecker,
    actor: &Actor,
) -> Result<bool, CheckerError> {
    if !sub_repo_enabled(checker) {
        return Ok(false);
    }
    if actor.is_internal() {
        return Ok(false);
    }
    if !actor.is_authenticated() {
        return Err(CheckerError::other("request is unauthenticated"));
    }
    Ok(true)
}

/// `authz.ActorPermissions` (`internal/authz/sub_repo_perms.go:92-106`).
pub fn actor_permissions(
    checker: &dyn SubRepoPermissionChecker,
    actor: &Actor,
    content: &RepoContent,
) -> Result<Perms, CheckerError> {
    if !actor_sub_repo_enabled(checker, actor)? {
        return Ok(Perms::READ);
    }
    checker
        .permissions(actor.uid, content)
        .map_err(|err| CheckerError {
            message: format!(
                "getting actor permissions for actor: {}: {}",
                actor.uid, err.message
            ),
            is_context: err.is_context,
        })
}

/// `authz.CanReadAnyPath` (`internal/authz/sub_repo_perms.go:161-213`).
pub fn can_read_any_path(
    checker: &dyn SubRepoPermissionChecker,
    actor: &Actor,
    repo: &str,
    paths: &[String],
) -> Result<bool, CheckerError> {
    if !actor_sub_repo_enabled(checker, actor)? {
        return Ok(true);
    }
    let check_path = checker.file_permissions_func(actor.uid, repo)?;
    for path in paths {
        if check_path(path)?.include(Perms::READ) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `diffIsEmpty` (`sub_repo_perms_job.go:155-162`).
pub fn diff_is_empty(diff_preview: &Option<MatchedString>) -> bool {
    match diff_preview {
        Some(preview) => preview.content.is_empty() || preview.matched_ranges.is_empty(),
        None => false,
    }
}

/// Message used for `logger.Error` when `EnabledForRepoID` fails.
pub const ENABLED_REPO_ERROR_MESSAGE: &str =
    "Could not determine if sub-repo permissions are enabled for repo, skipping";
/// Message used for `logger.Warn` when permission checks fail.
pub const APPLY_WARN_MESSAGE: &str = "Applying sub-repo permissions to search results";

/// Structured logger seam for [`apply_sub_repo_filtering`].
pub trait PermsLogSink {
    /// `logger.Error(message, log.Error(err), log.String("repoName", name))`.
    fn enabled_error(&mut self, message: &str, error: &str, repo_name: &str);

    /// `logger.Warn(message, log.Error(err))`.
    fn warn(&mut self, message: &str, error: &str);
}

/// A deterministic logger that discards entries.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopPermsLog;

impl PermsLogSink for NoopPermsLog {
    fn enabled_error(&mut self, _message: &str, _error: &str, _repo_name: &str) {}
    fn warn(&mut self, _message: &str, _error: &str) {}
}

/// `applySubRepoFiltering` (`sub_repo_perms_job.go:77-153`).
///
/// `modified_files` replaces `result.CommitMatch.ModifiedFiles`, which
/// `convert::CommitMatch` does not model; a missing entry is the baseline's
/// empty list.
pub fn apply_sub_repo_filtering(
    checker: &dyn SubRepoPermissionChecker,
    actor: &Actor,
    modified_files: &HashMap<(i32, String), Vec<String>>,
    logger: &mut dyn PermsLogSink,
    matches: Vec<Match>,
) -> (Vec<Match>, Option<JobError>) {
    if !sub_repo_enabled(checker) {
        return (matches, None);
    }

    let mut errs: Vec<String> = Vec::new();
    let mut filtered = Vec::with_capacity(matches.len());
    let mut err_cache: HashSet<String> = HashSet::new();

    for m in matches {
        let repo = m.repo_name();
        if err_cache.contains(&repo.name) {
            continue;
        }
        match checker.enabled_for_repo_id(repo.id) {
            Err(err) => {
                if !err.is_context {
                    logger.enabled_error(ENABLED_REPO_ERROR_MESSAGE, &err.message, &repo.name);
                }
                err_cache.insert(repo.name);
                continue;
            }
            Ok(false) => {
                filtered.push(m);
                continue;
            }
            Ok(true) => {}
        }

        match m {
            Match::File(fm) => {
                let content = RepoContent {
                    repo: fm.file.repo.name.clone(),
                    path: fm.file.path.clone(),
                };
                match actor_permissions(checker, actor, &content) {
                    Err(err) => errs.push(err.message),
                    Ok(perms) => {
                        if perms.include(Perms::READ) {
                            filtered.push(Match::File(fm));
                        }
                    }
                }
            }
            Match::Commit(cm) => {
                let paths = modified_files
                    .get(&(cm.repo.id, cm.commit.id.clone()))
                    .cloned()
                    .unwrap_or_default();
                match can_read_any_path(checker, actor, &cm.repo.name, &paths) {
                    Err(err) => errs.push(err.message),
                    Ok(true) => {
                        if !diff_is_empty(&cm.diff_preview) {
                            filtered.push(Match::Commit(cm));
                        }
                    }
                    Ok(false) => {}
                }
            }
            Match::Repo(rm) => filtered.push(Match::Repo(rm)),
            // The baseline switch has no OwnerMatch case.
            Match::Owner(_) => {}
        }
    }

    if errs.is_empty() {
        return (filtered, None);
    }
    logger.warn(APPLY_WARN_MESSAGE, &format_multi_error(&errs));
    (filtered, Some(JobError("subRepoFilterFunc".to_owned())))
}

/// `errors.Append` formatting (`lib/errors/multi_error.go` +
/// `cockroach.go:131-154`): one error is printed verbatim; several are
/// `"N errors occurred:\n\t* e1\n\t* e2"`.
pub fn format_multi_error(errors: &[String]) -> String {
    match errors {
        [] => String::new(),
        [single] => single.clone(),
        _ => {
            let mut out = format!("{} errors occurred:", errors.len());
            for error in errors {
                out.push_str("\n\t* ");
                out.push_str(error);
            }
            out
        }
    }
}

/// `subRepoPermsFilterJob` (`sub_repo_perms_job.go:26-73`).
pub struct SubRepoPermsFilterJob {
    checker: Box<dyn SubRepoPermissionChecker>,
    actor: Actor,
    modified_files: HashMap<(i32, String), Vec<String>>,
    logger: Box<dyn PermsLogSink>,
    child: Box<dyn SearchJob>,
}

impl SubRepoPermsFilterJob {
    /// `NewFilterJob` with the injected checker/actor/logger.
    pub fn new(
        checker: Box<dyn SubRepoPermissionChecker>,
        actor: Actor,
        modified_files: HashMap<(i32, String), Vec<String>>,
        logger: Box<dyn PermsLogSink>,
        child: Box<dyn SearchJob>,
    ) -> Self {
        Self {
            checker,
            actor,
            modified_files,
            logger,
            child,
        }
    }

    /// The baseline reads the checker from the global
    /// `authz.DefaultSubRepoPermsChecker`; this accessor keeps the same shape
    /// for callers that only need a description.
    pub fn checker(&self) -> &dyn SubRepoPermissionChecker {
        self.checker.as_ref()
    }
}

impl SearchJob for SubRepoPermsFilterJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Self {
            checker,
            actor,
            modified_files,
            logger,
            child,
        } = self;

        let mut errs: Vec<String> = Vec::new();
        let result = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let results = std::mem::take(&mut event.results);
                let (results, err) = apply_sub_repo_filtering(
                    checker.as_ref(),
                    actor,
                    modified_files,
                    logger.as_mut(),
                    results,
                );
                event.results = results;
                if let Some(err) = err {
                    errs.push(err.to_string());
                }
                stream(event);
            };
            child.run(ctx, clients, &mut filtered)
        };

        if let Err(err) = &result {
            errs.push(err.to_string());
        }
        if errs.is_empty() {
            result
        } else {
            Err(JobError(format_multi_error(&errs)))
        }
    }

    fn name(&self) -> &'static str {
        "SubRepoPermsFilterJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// `log_job.go`
// ---------------------------------------------------------------------------

/// `search.Inputs` fields `logEvent` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogInputs {
    pub query: Vec<Node>,
    pub pattern_type: SearchType,
}

/// The actor fields `logEvent` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogActor {
    pub uid: i32,
    pub mock_user: bool,
}

impl LogActor {
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }

    pub fn is_mock_user(&self) -> bool {
        self.mock_user
    }
}

/// `usagestats.LogBackendEvent` seam. `value` is the raw JSON argument (None
/// for the ownership events), and `Err` produces the baseline warning.
pub trait BackendEventLogger {
    fn log_backend_event(
        &mut self,
        user_id: i32,
        name: &str,
        value: Option<&str>,
    ) -> Result<(), JobError>;
}

/// A `telemetry.EventRecorder.Record` call.
#[derive(Debug, Clone, PartialEq)]
pub struct TelemetryEvent {
    pub feature: String,
    pub action: String,
    pub metadata: BTreeMap<String, f64>,
}

/// A `usagestats.LogBackendEvent` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyEvent {
    pub name: String,
    pub argument: Option<String>,
}

/// A warning emitted by `logEvent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogWarning {
    pub message: String,
    pub error: String,
}

/// Structured result of `logEvent`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogRecord {
    pub skipped: bool,
    /// `"unauthenticated"` or `"mock_user"` when `skipped`.
    pub skip_reason: Option<String>,
    /// Trace span name (`"LogSearchDuration"`) when logging ran.
    pub trace: Option<String>,
    pub types: Vec<String>,
    pub action: String,
    pub telemetry: Vec<TelemetryEvent>,
    pub legacy: Vec<LegacyEvent>,
    pub warnings: Vec<LogWarning>,
    /// `query.ToBasicQuery` failed, so the ownership/select events were
    /// skipped (the latency events still fired).
    pub to_basic_query_error: bool,
}

impl LogRecord {
    /// Summary form used by the baseline's telemetry tests: `feature - action`.
    pub fn telemetry_summary(&self) -> Vec<String> {
        self.telemetry
            .iter()
            .map(|event| format!("{} - {}", event.feature, event.action))
            .collect()
    }
}

/// `LogJob` (`log_job.go:28-70`).
pub struct LogJob {
    inputs: LogInputs,
    actor: LogActor,
    backend: Box<dyn BackendEventLogger>,
    child: Box<dyn SearchJob>,
    duration_override: Option<Duration>,
    last_record: Option<LogRecord>,
}

impl LogJob {
    /// `NewLogJob` with the injected actor/backend.
    pub fn new(
        inputs: LogInputs,
        actor: LogActor,
        backend: Box<dyn BackendEventLogger>,
        child: Box<dyn SearchJob>,
    ) -> Self {
        Self {
            inputs,
            actor,
            backend,
            child,
            duration_override: None,
            last_record: None,
        }
    }

    /// Deterministic duration for differential testing; the baseline measures
    /// `time.Since(start)` around the child.
    pub fn with_duration_override(mut self, duration: Duration) -> Self {
        self.duration_override = Some(duration);
        self
    }

    /// The structured record produced by the last `Run`.
    pub fn last_record(&self) -> Option<&LogRecord> {
        self.last_record.as_ref()
    }

    /// `logEvent` (`log_job.go:75-155`).
    pub fn log_event(&self, duration: Duration, backend: &mut dyn BackendEventLogger) -> LogRecord {
        log_event(self.actor, &self.inputs, duration, backend)
    }
}

impl SearchJob for LogJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let start = Instant::now();
        let result = self.child.run(ctx, clients, stream);
        let duration = self.duration_override.unwrap_or_else(|| start.elapsed());
        let record = log_event(self.actor, &self.inputs, duration, self.backend.as_mut());
        self.last_record = Some(record);
        result
    }

    fn name(&self) -> &'static str {
        "LogJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// `logEvent` (`log_job.go:75-155`), pure over the injected backend.
pub fn log_event(
    actor: LogActor,
    inputs: &LogInputs,
    duration: Duration,
    backend: &mut dyn BackendEventLogger,
) -> LogRecord {
    let mut record = LogRecord::default();

    if !actor.is_authenticated() {
        record.skipped = true;
        record.skip_reason = Some("unauthenticated".to_owned());
        return record;
    }
    if actor.is_mock_user() {
        record.skipped = true;
        record.skip_reason = Some("mock_user".to_owned());
        return record;
    }

    record.trace = Some("LogSearchDuration".to_owned());

    // `Query.StringValues(query.FieldType)` (non-negated values only).
    let mut types: Vec<String> = Vec::new();
    visit_field(&inputs.query, FIELD_TYPE, &mut |value, negated, _| {
        if negated {
            return;
        }
        match value {
            "repo" | "symbol" | "diff" | "commit" => types.push(value.to_owned()),
            "path" => types.push("file".to_owned()),
            "file" => types.push(pattern_type_str(inputs.pattern_type).to_owned()),
            _ => {}
        }
    });

    // Prefer the result type if it is a single type, otherwise use the pattern
    // type.
    let action = if types.len() == 1 {
        types[0].clone()
    } else {
        pattern_type_str(inputs.pattern_type).to_owned()
    };
    record.types = types;
    record.action = action.clone();

    let duration_ms = duration.as_millis() as i64;
    let mut latency_metadata = BTreeMap::new();
    latency_metadata.insert("durationMs".to_owned(), duration_ms as f64);
    record.telemetry.push(TelemetryEvent {
        feature: "search.latencies".to_owned(),
        action: action.clone(),
        metadata: latency_metadata,
    });

    let value = format!("{{\"durationMs\": {duration_ms}}}");
    let event_name = format!("search.latencies.{action}");
    // Both the legacy event and its public argument carry the same payload.
    // A failed insert is not stored (matching `usagestats.LogBackendEvent`).
    match backend.log_backend_event(actor.uid, &event_name, Some(&value)) {
        Ok(()) => record.legacy.push(LegacyEvent {
            name: event_name,
            argument: Some(value),
        }),
        Err(err) => record.warnings.push(LogWarning {
            message: "Could not log search latency".to_owned(),
            error: err.to_string(),
        }),
    }

    let basic = match to_basic_query(&inputs.query) {
        Ok(basic) => basic,
        Err(_) => {
            record.to_basic_query_error = true;
            return record;
        }
    };

    // `file:has.owner` / `file:has.contributor` ownership search.
    if file_has_owner(&basic.parameters) {
        record.telemetry.push(TelemetryEvent {
            feature: "search".to_owned(),
            action: "file.hasOwners".to_owned(),
            metadata: BTreeMap::new(),
        });
        match backend.log_backend_event(actor.uid, "FileHasOwnerSearch", None) {
            Ok(()) => record.legacy.push(LegacyEvent {
                name: "FileHasOwnerSearch".to_owned(),
                argument: None,
            }),
            Err(err) => record.warnings.push(LogWarning {
                message: "Could not log use of file:has.owners".to_owned(),
                error: err.to_string(),
            }),
        }
    }

    let select = q_string_value(&basic.to_parse_tree(), FIELD_SELECT);
    if !select.is_empty() {
        if let Some(path) = select_path_from_string(&select) {
            if is_select_owners_search(&path) {
                record.telemetry.push(TelemetryEvent {
                    feature: "search".to_owned(),
                    action: "select.fileOwners".to_owned(),
                    metadata: BTreeMap::new(),
                });
                match backend.log_backend_event(actor.uid, "SelectFileOwnersSearch", None) {
                    Ok(()) => record.legacy.push(LegacyEvent {
                        name: "SelectFileOwnersSearch".to_owned(),
                        argument: None,
                    }),
                    Err(err) => record.warnings.push(LogWarning {
                        message: "Could not log use of select:file.owners".to_owned(),
                        error: err.to_string(),
                    }),
                }
            }
        }
    }

    record
}

/// `Parameters.FileHasOwner` (`internal/search/query/types.go:492-501`):
/// any `file:` parameter labelled `IsPredicate` whose parsed name is
/// `has.owner` (aliases included).
fn file_has_owner(parameters: &Parameters) -> bool {
    let nodes = parameters.to_nodes();
    let mut found = false;
    visit_field(&nodes, FIELD_FILE, &mut |value, _negated, annotation| {
        if !annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        if let Some((name, _args)) = parse_as_predicate(value) {
            if name == "has.owner" {
                found = true;
            }
        }
    });
    found
}

/// `filter.SelectPathFromString` (`internal/search/filter/select.go:80-90`).
pub fn select_path_from_string(value: &str) -> Option<Vec<String>> {
    let fields: Vec<&str> = value.split('.').collect();
    let first = fields.first()?;
    match *first {
        "commit" => match fields.as_slice() {
            ["commit"] => {}
            ["commit", "diff"] => {}
            ["commit", "diff", "added"] | ["commit", "diff", "removed"] => {}
            _ => return None,
        },
        "content" | "repo" if fields.len() == 1 => {}
        "file" => match fields.as_slice() {
            ["file"] => {}
            ["file", "directory"] | ["file", "path"] | ["file", "owners"] => {}
            _ => return None,
        },
        "symbol" => match fields.as_slice() {
            ["symbol"] => {}
            ["symbol", kind] if symbol_kinds().contains(kind) => {}
            _ => return None,
        },
        _ => return None,
    }
    Some(fields.into_iter().map(str::to_owned).collect())
}

/// `filter.validSelectors["symbol"]` keys (`select.go:38-66`).
fn symbol_kinds() -> &'static [&'static str] {
    &[
        "file",
        "module",
        "namespace",
        "package",
        "class",
        "method",
        "property",
        "field",
        "constructor",
        "enum",
        "interface",
        "function",
        "variable",
        "constant",
        "string",
        "number",
        "boolean",
        "array",
        "object",
        "key",
        "null",
        "enum-member",
        "struct",
        "event",
        "operator",
        "type-parameter",
    ]
}

/// `isSelectOwnersSearch` (`internal/search/job/jobutil/job.go:587-590`).
pub fn is_select_owners_search(path: &[String]) -> bool {
    path.first().is_some_and(|root| root.as_str() == "file")
        && path.len() == 2
        && path.get(1).is_some_and(|field| field.as_str() == "owners")
}

/// `SearchType.String()` via the already-ported helper.
fn pattern_type_str(search_type: SearchType) -> &'static str {
    crate::search_jobutil_combinators::pattern_type_str(search_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_stream::convert::{
        FileRef, GitCommit, MinimalRepo, RepoMatch, Signature, Symbol, SymbolMatch,
    };
    use crate::search_stream::events::{Location, Range};
    use crate::search_stream::progress::Stats;
    use sg_search_query::pipeline::{init, run};
    use time::OffsetDateTime;

    struct NoopStreamer;

    impl crate::search_job::JobStreamer for NoopStreamer {
        fn stream_search(
            &self,
            _query: &sg_zoekt::query::Query,
            _options: &sg_zoekt::types::SearchOptions,
            _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
        ) -> Result<(), JobError> {
            panic!("stream_search must not be called");
        }
    }

    struct NoopLister;

    impl crate::search_job::PrivateRepoLister for NoopLister {
        fn list_minimal_repos(
            &self,
            _options: &crate::search_job::ReposListOptions,
        ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    fn clients<'a>(db: &'a NoopLister, zoekt: &'a NoopStreamer) -> RuntimeClients<'a> {
        RuntimeClients {
            db,
            zoekt,
            max_repos: None,
            searcher: None,
        }
    }

    fn range(start: i64, end: i64) -> Range {
        Range {
            start: Location {
                offset: start,
                line: 0,
                column: start,
            },
            end: Location {
                offset: end,
                line: 0,
                column: end,
            },
        }
    }

    fn file(path: &str, chunks: Vec<ChunkMatch>) -> Match {
        Match::File(FileMatch {
            file: FileRef {
                repo: MinimalRepo {
                    id: 1,
                    name: "repo".to_owned(),
                },
                commit_id: "abc".to_owned(),
                path: path.to_owned(),
                input_rev: None,
                precise_language: String::new(),
            },
            chunk_matches: chunks,
            symbols: Vec::new(),
            path_matches: Vec::new(),
            limit_hit: false,
            debug: None,
        })
    }

    fn chunk(content: &str, ranges: Vec<Range>) -> ChunkMatch {
        ChunkMatch {
            content: content.to_owned(),
            content_start: Location::default(),
            ranges,
        }
    }

    fn commit(diff: Option<&str>) -> Match {
        let signature = Signature {
            name: "a".to_owned(),
            email: "a@example.com".to_owned(),
            date: OffsetDateTime::UNIX_EPOCH,
        };
        Match::Commit(CommitMatch {
            commit: GitCommit {
                id: "c1".to_owned(),
                message: "msg".to_owned(),
                author: signature.clone(),
                committer: signature,
                parents: Vec::new(),
            },
            repo: MinimalRepo {
                id: 1,
                name: "repo".to_owned(),
            },
            message_preview: None,
            diff_preview: diff.map(|content| MatchedString {
                content: content.to_owned(),
                matched_ranges: vec![range(0, 1)],
            }),
            refs: Vec::new(),
            source_refs: Vec::new(),
            diff: Vec::new(),
            modified_files: Vec::new(),
        })
    }

    fn sanitize_event_of(results: Vec<Match>, patterns: &[&str]) -> Vec<Match> {
        let patterns: Vec<Regex> = patterns.iter().map(|p| Regex::new(p).unwrap()).collect();
        let event = SearchEvent {
            results,
            stats: Stats::default(),
        };
        sanitize_event(event, &patterns).results
    }

    #[test]
    fn sanitize_filters_ranges_and_drops_empty_matches() {
        let out = sanitize_event_of(
            vec![
                file(
                    "a.txt",
                    vec![chunk(
                        "omitmeABcDe don't omit me",
                        vec![range(0, 13), range(14, 28)],
                    )],
                ),
                file("b.txt", vec![chunk("only", vec![range(0, 4)])]),
                commit(Some("patTErn2ABC")),
                commit(Some("good diff")),
                Match::Repo(RepoMatch {
                    name: "r".to_owned(),
                    id: 1,
                    rev: String::new(),
                    description_matches: Vec::new(),
                    repo_name_matches: Vec::new(),
                }),
            ],
            &["omitme[a-zA-z]{5}", "(?im)Pattern2[a-zA-Z]{3}"],
        );
        assert_eq!(out.len(), 4);
        let Match::File(fm) = &out[0] else {
            panic!("file");
        };
        assert_eq!(fm.chunk_matches[0].ranges.len(), 1);
        assert_eq!(fm.chunk_matches[0].ranges[0].start.offset, 14);
        assert!(matches!(&out[1], Match::File(_)));
        assert!(matches!(&out[2], Match::Commit(_)));
        assert!(matches!(&out[3], Match::Repo(_)));
    }

    #[test]
    fn sanitize_keeps_symbol_files_and_drops_owners() {
        let mut symbols_file = file("a.txt", vec![chunk("omit", vec![range(0, 4)])]);
        let Match::File(fm) = &mut symbols_file else {
            panic!("file");
        };
        fm.symbols.push(SymbolMatch {
            file: fm.file.clone(),
            symbol: Symbol {
                name: "s".to_owned(),
                path: "a.txt".to_owned(),
                line: 1,
                character: 0,
                kind: "function".to_owned(),
                language: "rust".to_owned(),
                parent: String::new(),
                parent_kind: String::new(),
            },
        });
        let out = sanitize_event_of(
            vec![
                symbols_file,
                Match::Owner(crate::search_stream::convert::OwnerMatch {
                    owner: crate::search_stream::convert::ResolvedOwner::Person {
                        handle: "u".to_owned(),
                        email: String::new(),
                        user: None,
                    },
                    repo: MinimalRepo {
                        id: 1,
                        name: "repo".to_owned(),
                    },
                }),
            ],
            &["omit"],
        );
        assert_eq!(out.len(), 1);
        let Match::File(fm) = &out[0] else {
            panic!("file");
        };
        assert_eq!(fm.chunk_matches[0].ranges.len(), 1);
    }

    struct FixedChecker {
        enabled: bool,
        enabled_ids: HashMap<i32, Result<bool, CheckerError>>,
        permissions: HashMap<String, Result<Perms, CheckerError>>,
    }

    impl SubRepoPermissionChecker for FixedChecker {
        fn enabled(&self) -> bool {
            self.enabled
        }
        fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, CheckerError> {
            self.enabled_ids.get(&repo_id).cloned().unwrap_or(Ok(true))
        }
        fn permissions(&self, _user_id: i32, content: &RepoContent) -> Result<Perms, CheckerError> {
            self.permissions
                .get(&content.path)
                .cloned()
                .unwrap_or(Ok(Perms::READ))
        }
        fn file_permissions_func(
            &self,
            _user_id: i32,
            _repo: &str,
        ) -> Result<Box<FilePermissionFunc>, CheckerError> {
            let permissions = self.permissions.clone();
            Ok(Box::new(move |path: &str| {
                permissions.get(path).cloned().unwrap_or(Ok(Perms::READ))
            }))
        }
    }

    fn checker() -> FixedChecker {
        FixedChecker {
            enabled: true,
            enabled_ids: HashMap::new(),
            permissions: HashMap::new(),
        }
    }

    #[test]
    fn sub_repo_filters_files_and_commits() {
        let mut checker = checker();
        checker
            .permissions
            .insert("README.md".to_owned(), Ok(Perms::NONE));
        let actor = Actor {
            uid: 42,
            ..Actor::default()
        };
        let mut modified_files = HashMap::new();
        modified_files.insert((1, "c1".to_owned()), vec!["README.md".to_owned()]);
        let mut log = NoopPermsLog;
        let (out, err) = apply_sub_repo_filtering(
            &checker,
            &actor,
            &modified_files,
            &mut log,
            vec![
                file("ok.txt", Vec::new()),
                file("README.md", Vec::new()),
                commit(Some("diff")),
            ],
        );
        assert!(err.is_none());
        assert_eq!(out.len(), 1);
        assert!(matches!(&out[0], Match::File(_)));
    }

    #[test]
    fn sub_repo_disabled_passes_everything_through() {
        let mut checker = checker();
        checker.enabled = false;
        let mut log = NoopPermsLog;
        let input = vec![file("README.md", Vec::new()), commit(Some("\"diff\""))];
        let (out, err) = apply_sub_repo_filtering(
            &checker,
            &Actor::default(),
            &HashMap::new(),
            &mut log,
            input,
        );
        assert!(err.is_none());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn sub_repo_errors_are_generic_and_logged() {
        let mut checker = checker();
        checker
            .permissions
            .insert("boom".to_owned(), Err(CheckerError::other("boom failed")));
        let actor = Actor {
            uid: 42,
            ..Actor::default()
        };
        let mut log = NoopPermsLog;
        let (out, err) = apply_sub_repo_filtering(
            &checker,
            &actor,
            &HashMap::new(),
            &mut log,
            vec![file("boom", Vec::new())],
        );
        assert!(out.is_empty());
        assert_eq!(err.unwrap().to_string(), "subRepoFilterFunc");
    }

    #[test]
    fn diff_empty_semantics() {
        assert!(!diff_is_empty(&None));
        assert!(diff_is_empty(&Some(MatchedString {
            content: String::new(),
            matched_ranges: vec![range(0, 1)],
        })));
        assert!(diff_is_empty(&Some(MatchedString {
            content: "x".to_owned(),
            matched_ranges: Vec::new(),
        })));
        assert!(!diff_is_empty(&Some(MatchedString {
            content: "x".to_owned(),
            matched_ranges: vec![range(0, 1)],
        })));
    }

    #[derive(Default)]
    struct RecordingBackend {
        calls: Vec<(i32, String, Option<String>)>,
        fail: HashSet<String>,
    }

    impl BackendEventLogger for RecordingBackend {
        fn log_backend_event(
            &mut self,
            user_id: i32,
            name: &str,
            value: Option<&str>,
        ) -> Result<(), JobError> {
            self.calls
                .push((user_id, name.to_owned(), value.map(str::to_owned)));
            if self.fail.contains(name) {
                Err(JobError("backend down".to_owned()))
            } else {
                Ok(())
            }
        }
    }

    fn log_inputs(query: &str, pattern_type: SearchType) -> LogInputs {
        LogInputs {
            query: run(init(query.to_owned(), pattern_type)).unwrap(),
            pattern_type,
        }
    }

    #[test]
    fn log_event_result_types_and_latency() {
        let inputs = log_inputs("sourcegraph type:repo", SearchType::Keyword);
        let mut backend = RecordingBackend::default();
        let record = log_event(
            LogActor {
                uid: 42,
                mock_user: false,
            },
            &inputs,
            Duration::from_millis(1234),
            &mut backend,
        );
        assert_eq!(record.types, vec!["repo"]);
        assert_eq!(record.action, "repo");
        assert_eq!(record.telemetry_summary(), vec!["search.latencies - repo"]);
        assert_eq!(record.legacy[0].name, "search.latencies.repo");
        assert_eq!(
            record.legacy[0].argument.as_deref(),
            Some("{\"durationMs\": 1234}")
        );
        assert_eq!(record.trace.as_deref(), Some("LogSearchDuration"));
    }

    #[test]
    fn log_event_maps_path_and_file_types() {
        let record = log_event(
            LogActor {
                uid: 42,
                mock_user: false,
            },
            &log_inputs("sourcegraph type:path", SearchType::Keyword),
            Duration::ZERO,
            &mut RecordingBackend::default(),
        );
        assert_eq!(record.action, "file");
        let record = log_event(
            LogActor {
                uid: 42,
                mock_user: false,
            },
            &log_inputs("sourcegraph type:file", SearchType::Regex),
            Duration::ZERO,
            &mut RecordingBackend::default(),
        );
        assert_eq!(record.action, "regex");
    }

    #[test]
    fn log_event_ownership_and_select_owners() {
        let mut record = log_event(
            LogActor {
                uid: 42,
                mock_user: false,
            },
            &log_inputs("file:has.owner(alice@example.com)", SearchType::Keyword),
            Duration::ZERO,
            &mut RecordingBackend::default(),
        );
        assert_eq!(
            record.telemetry_summary(),
            vec!["search.latencies - keyword", "search - file.hasOwners"]
        );
        assert_eq!(record.legacy[1].name, "FileHasOwnerSearch");
        assert_eq!(record.legacy[1].argument, None);

        record = log_event(
            LogActor {
                uid: 42,
                mock_user: false,
            },
            &log_inputs("select:file.owners", SearchType::Keyword),
            Duration::ZERO,
            &mut RecordingBackend::default(),
        );
        assert_eq!(
            record.telemetry_summary(),
            vec!["search.latencies - keyword", "search - select.fileOwners"]
        );
    }

    #[test]
    fn log_event_skips_anonymous_and_mock_users() {
        for (actor, reason) in [
            (LogActor::default(), "unauthenticated"),
            (
                LogActor {
                    uid: 42,
                    mock_user: true,
                },
                "mock_user",
            ),
        ] {
            let record = log_event(
                actor,
                &log_inputs("sourcegraph type:repo", SearchType::Keyword),
                Duration::ZERO,
                &mut RecordingBackend::default(),
            );
            assert!(record.skipped);
            assert_eq!(record.skip_reason.as_deref(), Some(reason));
            assert!(record.telemetry.is_empty());
            assert!(record.legacy.is_empty());
        }
    }

    struct EmitJob {
        events: Vec<SearchEvent>,
        error: Option<JobError>,
    }

    impl SearchJob for EmitJob {
        fn run(
            &mut self,
            _ctx: &JobContext,
            _clients: &RuntimeClients<'_>,
            stream: &mut dyn FnMut(SearchEvent),
        ) -> Result<(), JobError> {
            for event in &self.events {
                stream(event.clone());
            }
            match &self.error {
                Some(err) => Err(err.clone()),
                None => Ok(()),
            }
        }
        fn name(&self) -> &'static str {
            "EmitJob"
        }
        fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
            Vec::new()
        }
    }

    #[test]
    fn log_run_forwards_child_events_and_error() {
        let event = SearchEvent {
            results: vec![file("a", Vec::new())],
            stats: Stats::default(),
        };
        let mut job = LogJob::new(
            log_inputs("sourcegraph type:repo", SearchType::Keyword),
            LogActor {
                uid: 42,
                mock_user: false,
            },
            Box::new(RecordingBackend::default()),
            Box::new(EmitJob {
                events: vec![event.clone()],
                error: Some(JobError("child failed".to_owned())),
            }),
        )
        .with_duration_override(Duration::from_millis(5));

        let streamer = NoopStreamer;
        let lister = NoopLister;
        let clients = clients(&lister, &streamer);
        let mut events = Vec::new();
        let result = job.run(&JobContext::default(), &clients, &mut |event| {
            events.push(event)
        });
        assert_eq!(result.unwrap_err().to_string(), "child failed");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].results, event.results);
        assert!(events[0].stats.zero());
        let record = job.last_record().unwrap();
        assert_eq!(record.action, "repo");
    }
}
