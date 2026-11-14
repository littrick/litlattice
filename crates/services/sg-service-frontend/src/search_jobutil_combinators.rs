//! jobutil combinators, limits and alert collection
//! (`internal/search/job/jobutil/{combinators,limit,alert}.go`).
//!
//! Ported:
//!
//! * [`SequentialJob`] (`NewSequentialJob`): sequential children, optional
//!   `ensureUnique` result deduplication and the `ctx.Err()` early stop
//!   *before* recording the just-finished child's alert/error;
//! * [`ParallelJob`] (`NewParallelJob`): aggregate alerts and collect every
//!   child error (`errors.Join` in the baseline, no cancel-on-error);
//! * [`TimeoutJob`] (`NewTimeoutJob`), [`NoopJob`] and the 0/1-child/Noop
//!   constructor simplifications;
//! * [`LimitJob`] and [`LimitStream`] (`limit.go`): result-count based
//!   truncation, the `IsLimitHit` stats event, and swallowing
//!   `context.Canceled` caused by the limit;
//! * [`AlertJob`] (`alert.go`), [`longer`] and the alert model/helpers from
//!   `internal/search/alert.go` (`MaxPriorityAlert`, `MaxAlerter`,
//!   `AlertForSmartSearch`, `AlertForTimeout`).
//!
//! The module reuses [`crate::search_job::{SearchJob, Verbosity, JobContext,
//! RuntimeClients}`] but does not modify `search_job.rs`. `search_job::SearchJob`
//! has no alert or dynamic cancellation support, so this module defines a
//! local [`CombinatorJob`] trait plus [`SearchJobAdapter`] for existing jobs.
//!
//! Not ported: the job tree builders (`job.go`), expression jobs
//! (`expression_job.go`'s `AndJob`/`OrJob`), job registration,
//! GraphQL/HTTP wiring, real database/gRPC clients, OpenTelemetry spans,
//! `Job.MapChildren` tree rewriting, and `alert.Observer`'s DB/repo
//! resolution (its `Error`/`Done` contract is injected through
//! [`AlertObserver`]).
//!
//! See `docs/overviews/m9_8hh_jobutil_combinators_overview.md`.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use regex::Regex;
use sg_search_query::fields::FIELD_TIMEOUT;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::{init, run};
use sg_search_query::{mapper, string_human};

use crate::search_client::{Protocol, SearchMode};
use crate::search_job::{JobContext, RuntimeClients, SearchJob, Verbosity};
use crate::search_stream::convert::{Match, ResolvedOwner};
use crate::search_stream::progress::{repo_status, SearchEvent, Stats};

/// `search.Alert` (`internal/search/alert.go:14-22`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Alert {
    pub prometheus_type: String,
    pub title: String,
    pub description: String,
    pub proposed_queries: Vec<QueryDescription>,
    pub kind: String,
    pub priority: i32,
}

/// `search.QueryDescription` (`internal/search/alert.go:50-55`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDescription {
    pub description: String,
    pub query: String,
    pub pattern_type: SearchType,
    pub annotations: BTreeMap<String, String>,
}

/// `search.MaxPriorityAlert`: skip `None`, replace the current maximum only
/// on a *strictly* higher priority (ties keep the first alert).
pub fn max_priority_alert(alerts: &[Option<Alert>]) -> Option<Alert> {
    let mut max: Option<Alert> = None;
    for alert in alerts {
        let Some(alert) = alert else {
            continue;
        };
        if max
            .as_ref()
            .is_none_or(|current| alert.priority > current.priority)
        {
            max = Some(alert.clone());
        }
    }
    max
}

/// Thread-safe `search.MaxAlerter`.
#[derive(Debug, Default)]
pub struct MaxAlerter {
    alert: Mutex<Option<Alert>>,
}

impl MaxAlerter {
    pub fn new() -> Self {
        Self::default()
    }

    /// `MaxAlerter.Add`.
    pub fn add(&self, alert: Option<Alert>) {
        let mut current = self.alert.lock().expect("max alerter mutex poisoned");
        if let Some(alert) = alert {
            if current
                .as_ref()
                .is_none_or(|existing| alert.priority > existing.priority)
            {
                *current = Some(alert);
            }
        }
    }

    /// The aggregated alert (highest priority, first on ties).
    pub fn alert(&self) -> Option<Alert> {
        self.alert
            .lock()
            .expect("max alerter mutex poisoned")
            .clone()
    }
}

/// `search.AlertForSmartSearch` (`internal/search/alert.go:99-105`).
pub fn alert_for_smart_search() -> Alert {
    Alert {
        prometheus_type: "smart_search_no_results".to_owned(),
        title: "No results matched your search.".to_owned(),
        description: "To find more results, try your search again using the default \
                      `patterntype:keyword`."
            .to_owned(),
        ..Alert::default()
    }
}

/// `search.AlertForTimeout` (`internal/search/alert.go:107-132`).
///
/// `used_time` and `suggest_time` are non-negative (the baseline's
/// `time.Duration` may be negative; this port only models the search-timeout
/// path).
pub fn alert_for_timeout(
    used_time: Duration,
    suggest_time: Duration,
    query_string: &str,
    pattern_type: SearchType,
) -> Alert {
    let used = format_go_duration(round_to_second(used_time));
    // The baseline calls `query.ParseLiteral`, which is
    // `Run(Init(in, SearchTypeLiteral))`: the `For` step joins contiguous
    // literal patterns ("foo bar" is one pattern) before `OmitField`.
    match run(init(query_string.to_owned(), SearchType::Literal)) {
        Err(_) => Alert {
            prometheus_type: "timed_out".to_owned(),
            title: "Timed out while searching".to_owned(),
            description: format!(
                "We weren't able to find any results in {used}. Try adding timeout: with a higher value."
            ),
            ..Alert::default()
        },
        Ok(nodes) => {
            let without_timeout =
                mapper::map_field(&nodes, FIELD_TIMEOUT, &mut |_, _, _| None);
            let query = string_human(&without_timeout);
            Alert {
                prometheus_type: "timed_out".to_owned(),
                title: "Timed out while searching".to_owned(),
                description: format!("We weren't able to find any results in {used}."),
                proposed_queries: vec![QueryDescription {
                    description: "query with longer timeout".to_owned(),
                    query: format!("timeout:{} {}", format_go_duration(suggest_time), query),
                    pattern_type,
                    annotations: BTreeMap::new(),
                }],
                ..Alert::default()
            }
        }
    }
}

/// Go `comby.MatchHoleRegexp` (`internal/comby/translate.go:12-31`).
pub fn has_match_hole(query: &str) -> bool {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    let regex = REGEX.get_or_init(|| {
        Regex::new(r":\[(\w+)\]|:\[\[(\w+)\]\]|:\[(\w+)\.\]|:\[(\w+)\\n\]|:\[( +(\w+)?)?\]|\.\.\.")
            .expect("match-hole regex")
    });
    regex.is_match(query)
}

/// `search.AlertForStructuralSearchNotSet` (`internal/search/alert.go:152-165`).
pub fn alert_for_structural_search_not_set(query_string: &str) -> Alert {
    Alert {
        prometheus_type: "structural_search_not_set".to_owned(),
        title: "No results".to_owned(),
        description: "It looks like you're trying to run a structural search, but it is not \
                      enabled using the patterntype keyword or UI toggle."
            .to_owned(),
        proposed_queries: vec![QueryDescription {
            description: "Activate structural search".to_owned(),
            query: query_string.to_owned(),
            pattern_type: SearchType::Structural,
            annotations: BTreeMap::new(),
        }],
        kind: String::new(),
        priority: 0,
    }
}

/// `longer` (`alert.go:126-138`): suggest a round duration of at least
/// `n * dt` and at least 2s.
pub fn longer(n: i64, dt: Duration) -> Duration {
    const SECOND: u128 = 1_000_000_000;
    const MINUTE: u128 = 60 * SECOND;
    const HOUR: u128 = 60 * MINUTE;

    let n = n.max(0) as u128;
    let ndt = dt.as_nanos().saturating_mul(n);
    let rounded = if ndt >= HOUR {
        div_ceil(ndt, HOUR) * HOUR
    } else if ndt >= MINUTE {
        div_ceil(ndt, MINUTE) * MINUTE
    } else if ndt >= SECOND {
        div_ceil(ndt, SECOND) * SECOND
    } else {
        0
    };
    let result = rounded.max(2 * SECOND);
    Duration::from_nanos(u64::try_from(result).unwrap_or(u64::MAX))
}

fn div_ceil(value: u128, divisor: u128) -> u128 {
    value / divisor + u128::from(value % divisor != 0)
}

fn round_to_second(dt: Duration) -> Duration {
    const SECOND: u128 = 1_000_000_000;
    let nanos = dt.as_nanos();
    let rounded = ((nanos + SECOND / 2) / SECOND) * SECOND;
    Duration::from_nanos(u64::try_from(rounded).unwrap_or(u64::MAX))
}

/// Go `time.Duration.String()` for non-negative durations (`time/format.go`).
pub fn format_go_duration(dt: Duration) -> String {
    const MICRO: u128 = 1_000;
    const MILLI: u128 = 1_000_000;
    const SECOND: u128 = 1_000_000_000;

    let u = dt.as_nanos();
    if u == 0 {
        return "0s".to_owned();
    }
    if u < SECOND {
        let (whole, remainder, precision, unit) = if u < MICRO {
            (u, 0, 0, "ns")
        } else if u < MILLI {
            (u / MICRO, u % MICRO, 3, "µs")
        } else {
            (u / MILLI, u % MILLI, 6, "ms")
        };
        let mut out = whole.to_string();
        push_fraction(&mut out, remainder, precision);
        out.push_str(unit);
        return out;
    }

    let seconds = u / SECOND;
    let remainder = u % SECOND;
    let mut out = String::new();
    let secs = seconds % 60;
    let minutes_total = seconds / 60;
    if minutes_total == 0 {
        out.push_str(&secs.to_string());
        push_fraction(&mut out, remainder, 9);
        out.push('s');
        return out;
    }
    let minutes = minutes_total % 60;
    let hours = minutes_total / 60;
    if hours == 0 {
        out.push_str(&minutes.to_string());
        out.push('m');
        out.push_str(&secs.to_string());
        push_fraction(&mut out, remainder, 9);
        out.push('s');
    } else {
        out.push_str(&hours.to_string());
        out.push('h');
        out.push_str(&minutes.to_string());
        out.push('m');
        out.push_str(&secs.to_string());
        push_fraction(&mut out, remainder, 9);
        out.push('s');
    }
    out
}

/// `fmtFrac`: append `.` + padded fraction with trailing zeros trimmed, but
/// only when the fraction is non-zero.
fn push_fraction(out: &mut String, remainder: u128, precision: usize) {
    if remainder == 0 || precision == 0 {
        return;
    }
    let digits = format!("{remainder:0precision$}");
    let trimmed = digits.trim_end_matches('0');
    if !trimmed.is_empty() {
        out.push('.');
        out.push_str(trimmed);
    }
}

/// `errors.Append`/`errors.CombineErrors`: combine two optional errors,
/// flattening `Multi` in order.
fn combine_errors(first: Option<JobFailure>, second: Option<JobFailure>) -> Option<JobFailure> {
    match (first, second) {
        (None, None) => None,
        (Some(err), None) | (None, Some(err)) => Some(err),
        (Some(first), Some(second)) => Some(match (first, second) {
            (JobFailure::Multi(mut first), JobFailure::Multi(second)) => {
                first.extend(second);
                JobFailure::Multi(first)
            }
            (JobFailure::Multi(mut first), second) => {
                first.push(second);
                JobFailure::Multi(first)
            }
            (first, JobFailure::Multi(mut second)) => {
                let mut errors = vec![first];
                errors.append(&mut second);
                JobFailure::Multi(errors)
            }
            (first, second) => JobFailure::Multi(vec![first, second]),
        }),
    }
}

/// Error returned by a [`CombinatorJob`], keeping the identity checks the
/// baseline performs with `errors.Is`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobFailure {
    /// A regular error; `Message` is its `Error()` text.
    Message(String),
    /// `context.Canceled`.
    Canceled,
    /// `context.DeadlineExceeded`.
    DeadlineExceeded,
    /// `errors.MultiError`/`errors.Join`.
    Multi(Vec<JobFailure>),
    /// `*searchrepos.MissingRepoRevsError` (`repos.go:1224`): carries the
    /// missing repo/rev pairs so the alert can render the baseline
    /// description.
    MissingRepoRevs(Vec<crate::search_repos::RepoRevSpecs>),
    /// `*searchalert.errOverRepoLimit` (`observer.go:319`). The baseline type
    /// is unexported and had no producer at the pinned commit; the observer
    /// mapping is still ported verbatim.
    OverRepoLimit {
        proposed_queries: Vec<QueryDescription>,
        description: String,
    },
}

impl JobFailure {
    /// `errors.Is(err, context.Canceled)` (recursive through `Multi`).
    pub fn is_canceled(&self) -> bool {
        match self {
            JobFailure::Canceled => true,
            JobFailure::Multi(errors) => errors.iter().any(JobFailure::is_canceled),
            JobFailure::Message(_)
            | JobFailure::DeadlineExceeded
            | JobFailure::MissingRepoRevs(_)
            | JobFailure::OverRepoLimit { .. } => false,
        }
    }

    /// `errors.Is(err, context.DeadlineExceeded)` (recursive).
    pub fn is_deadline_exceeded(&self) -> bool {
        match self {
            JobFailure::DeadlineExceeded => true,
            JobFailure::Multi(errors) => errors.iter().any(JobFailure::is_deadline_exceeded),
            JobFailure::Message(_)
            | JobFailure::Canceled
            | JobFailure::MissingRepoRevs(_)
            | JobFailure::OverRepoLimit { .. } => false,
        }
    }

    /// Flatten nested `Multi` into the leaves; used by the differential
    /// normalizer, mirroring `MultiError.Errors()`/`errors.Join`.
    pub fn parts(&self) -> Vec<&JobFailure> {
        match self {
            JobFailure::Multi(errors) => errors
                .iter()
                .flat_map(JobFailure::parts)
                .collect::<Vec<_>>(),
            other => vec![other],
        }
    }
}

impl fmt::Display for JobFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobFailure::Message(message) => f.write_str(message),
            JobFailure::Canceled => f.write_str("context canceled"),
            JobFailure::DeadlineExceeded => f.write_str("context deadline exceeded"),
            // `MissingRepoRevsError.Error` / `errOverRepoLimit.Error`.
            JobFailure::MissingRepoRevs(_) => f.write_str("missing repo revs"),
            JobFailure::OverRepoLimit { .. } => f.write_str("Too many matching repositories"),
            JobFailure::Multi(errors) => {
                let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
                f.write_str(&messages.join("\n"))
            }
        }
    }
}

impl std::error::Error for JobFailure {}

/// A shared cancellation node. A derived context observes its parent chain,
/// like `context.WithCancel`.
#[derive(Debug)]
struct CancelNode {
    canceled: AtomicBool,
    parent: Option<Arc<CancelNode>>,
}

impl CancelNode {
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.is_canceled())
    }
}

/// The runtime context a combinator passes down. `JobContext` only models the
/// deadline; combinators additionally need the baseline's dynamic
/// cancellation, so this type wraps a `JobContext`-compatible deadline and a
/// parent-linked cancel flag.
#[derive(Debug, Clone)]
pub struct RunContext {
    deadline: Option<Instant>,
    cancel: Arc<CancelNode>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self {
            deadline: None,
            cancel: Arc::new(CancelNode {
                canceled: AtomicBool::new(false),
                parent: None,
            }),
        }
    }
}

impl RunContext {
    /// A fresh root context, optionally with a deadline.
    pub fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            ..Self::default()
        }
    }

    /// `context.WithCancel`: the returned handle cancels only the derived
    /// context (and its descendants).
    pub fn with_cancel(&self) -> (RunContext, CancelHandle) {
        let node = Arc::new(CancelNode {
            canceled: AtomicBool::new(false),
            parent: Some(self.cancel.clone()),
        });
        (
            RunContext {
                deadline: self.deadline,
                cancel: node.clone(),
            },
            CancelHandle { node },
        )
    }

    /// `context.WithTimeout` (earlier of the parent deadline and `now+timeout`).
    pub fn with_timeout(&self, timeout: Duration) -> RunContext {
        let target = Instant::now().checked_add(timeout);
        let deadline = match (self.deadline, target) {
            (Some(parent), Some(target)) => Some(parent.min(target)),
            (parent, target) => parent.or(target),
        };
        RunContext {
            deadline,
            cancel: self.cancel.clone(),
        }
    }

    /// Cancel this context explicitly (used by tests and [`CancelHandle`]).
    pub fn cancel(&self) {
        self.cancel.canceled.store(true, Ordering::SeqCst);
    }

    /// `ctx.Err() != nil`: an expired deadline or an explicit cancellation.
    pub fn is_canceled(&self) -> bool {
        self.deadline_exceeded() || self.cancel.is_canceled()
    }

    /// `time.Now().After(deadline)`.
    pub fn deadline_exceeded(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// The classified `ctx.Err()` (deadline takes precedence, as in Go).
    pub fn err(&self) -> Option<JobFailure> {
        if self.deadline_exceeded() {
            Some(JobFailure::DeadlineExceeded)
        } else if self.cancel.is_canceled() {
            Some(JobFailure::Canceled)
        } else {
            None
        }
    }

    /// The deadline view passed to existing [`SearchJob`] implementations.
    pub fn as_job_context(&self) -> JobContext {
        JobContext {
            deadline: self.deadline,
        }
    }
}

/// Handle returned by [`RunContext::with_cancel`].
#[derive(Debug, Clone)]
pub struct CancelHandle {
    node: Arc<CancelNode>,
}

impl CancelHandle {
    pub fn cancel(&self) {
        self.node.canceled.store(true, Ordering::SeqCst);
    }
}

/// A sender of search events. The baseline uses `streaming.Sender`; this
/// module uses the same closure shape as `search_job`.
pub type Sender<'a> = &'a mut dyn FnMut(SearchEvent);

/// Minimal object-safe job interface for the combinators.
///
/// Unlike [`SearchJob`], `run` also returns an optional [`Alert`] and a
/// classified [`JobFailure`]. Child jobs can be existing `SearchJob`
/// implementations through [`SearchJobAdapter`].
pub trait CombinatorJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>);

    /// `Describer.Name`.
    fn name(&self) -> &'static str;

    /// `Describer.Children`.
    fn children(&self) -> Vec<&dyn CombinatorJob> {
        Vec::new()
    }

    /// `Describer.Attributes`, normalized to `(key, value)` strings.
    fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Models the baseline's `*NoopJob` type assertion in constructors.
    fn is_noop(&self) -> bool {
        false
    }
}

/// Adapts an existing [`SearchJob`] to [`CombinatorJob`]. The baseline's
/// alert is always `nil` for the jobs in `search_job.rs`.
pub struct SearchJobAdapter<J> {
    pub job: J,
}

impl<J> SearchJobAdapter<J> {
    pub fn new(job: J) -> Self {
        Self { job }
    }
}

impl<J: SearchJob> CombinatorJob for SearchJobAdapter<J> {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let job_ctx = ctx.as_job_context();
        match self.job.run(&job_ctx, clients, stream) {
            Ok(()) => (None, None),
            Err(err) => (None, Some(JobFailure::Message(err.to_string()))),
        }
    }

    fn name(&self) -> &'static str {
        self.job.name()
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        self.job.attributes(verbosity)
    }
}

/// A fixed-event leaf used by the execution differential harness to drive
/// wrapper jobs (`Alert`, `Select`) without the zoekt/searcher stack.
pub struct StaticJob {
    events: Vec<SearchEvent>,
    error: Option<JobFailure>,
}

impl StaticJob {
    pub fn new(events: Vec<SearchEvent>, error: Option<JobFailure>) -> Self {
        Self { events, error }
    }
}

impl CombinatorJob for StaticJob {
    fn run(
        &mut self,
        _ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        for event in &self.events {
            stream(event.clone());
        }
        (None, self.error.clone())
    }

    fn name(&self) -> &'static str {
        "StaticEventsJob"
    }
}

/// `NoopJob` (`combinators.go:226-239`).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopJob;

impl CombinatorJob for NoopJob {
    fn run(
        &mut self,
        _ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        _stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        (None, None)
    }

    fn name(&self) -> &'static str {
        "NoopJob"
    }

    fn is_noop(&self) -> bool {
        true
    }
}

pub fn new_noop_job() -> Box<dyn CombinatorJob> {
    Box::new(NoopJob)
}

/// `SequentialJob` (`combinators.go:39-116`).
pub struct SequentialJob {
    ensure_unique: bool,
    children: Vec<Box<dyn CombinatorJob>>,
}

/// `NewSequentialJob` (`combinators.go:26-37`).
pub fn new_sequential_job(
    ensure_unique: bool,
    mut children: Vec<Box<dyn CombinatorJob>>,
) -> Box<dyn CombinatorJob> {
    match children.len() {
        0 => new_noop_job(),
        1 => children.pop().expect("one child"),
        _ => Box::new(SequentialJob {
            ensure_unique,
            children,
        }),
    }
}

/// `result.Deduper` keyed by `result.Match.Key()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DedupKey {
    File {
        repo: String,
        commit: String,
        path: String,
        /// `result.FileMatch.Key` maps a nil `InputRev` to `Rev == ""`, so
        /// `None` and `Some("")` are the same key.
        input_rev: String,
    },
    Repo {
        repo: String,
        rev: String,
    },
    Commit {
        repo: String,
        author_unix: i64,
        author_nanos: u32,
        commit: String,
        rank: u8,
    },
    Owner {
        repo: String,
        owner: String,
    },
}

fn dedup_key(match_: &Match) -> DedupKey {
    match match_ {
        Match::File(file) => DedupKey::File {
            repo: file.file.repo.name.clone(),
            commit: file.file.commit_id.clone(),
            path: file.file.path.clone(),
            input_rev: file.file.input_rev.clone().unwrap_or_default(),
        },
        Match::Repo(repo) => DedupKey::Repo {
            repo: repo.name.clone(),
            rev: repo.rev.clone(),
        },
        Match::Commit(commit) => DedupKey::Commit {
            repo: commit.repo.name.clone(),
            author_unix: commit.commit.author.date.unix_timestamp(),
            author_nanos: commit.commit.author.date.nanosecond(),
            commit: commit.commit.id.clone(),
            rank: if commit.diff_preview.is_some() { 2 } else { 1 },
        },
        Match::Owner(owner) => DedupKey::Owner {
            repo: owner.repo.name.clone(),
            owner: match &owner.owner {
                ResolvedOwner::Person { handle, .. } => format!("person{handle}"),
                ResolvedOwner::Team { handle, name, .. } => {
                    format!("team{handle}{name}")
                }
            },
        },
    }
}

#[derive(Debug, Default)]
struct Deduper {
    seen: HashSet<DedupKey>,
}

impl Deduper {
    fn filter(&mut self, matches: Vec<Match>) -> Vec<Match> {
        let mut kept = Vec::with_capacity(matches.len());
        for match_ in matches {
            let key = dedup_key(&match_);
            if self.seen.insert(key) {
                kept.push(match_);
            }
        }
        kept
    }
}

impl CombinatorJob for SequentialJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let max_alerter = MaxAlerter::new();
        let errs: Option<JobFailure> = None;

        if self.ensure_unique {
            let mut deduper = Deduper::default();
            let mut dedup_stream = |mut event: SearchEvent| {
                event.results = deduper.filter(event.results);
                stream(event);
            };
            return self.run_children(ctx, clients, &mut dedup_stream, max_alerter, errs);
        }

        self.run_children(ctx, clients, stream, max_alerter, errs)
    }

    fn name(&self) -> &'static str {
        "SequentialJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        self.children.iter().map(AsRef::as_ref).collect()
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        match verbosity {
            Verbosity::Basic | Verbosity::Max => {
                vec![("ensureUnique".to_owned(), self.ensure_unique.to_string())]
            }
            Verbosity::None => Vec::new(),
        }
    }
}

impl SequentialJob {
    fn run_children(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
        max_alerter: MaxAlerter,
        mut errs: Option<JobFailure>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        for child in &mut self.children {
            let (alert, err) = child.run(ctx, clients, stream);
            if ctx.is_canceled() {
                // Baseline returns before recording the just-finished child.
                return (max_alerter.alert(), errs);
            }
            max_alerter.add(alert);
            errs = combine_errors(errs, err);
        }
        (max_alerter.alert(), errs)
    }
}

/// `ParallelJob` (`combinators.go:124-165`).
pub struct ParallelJob {
    children: Vec<Box<dyn CombinatorJob>>,
}

/// `NewParallelJob` (`combinators.go:119-128`).
pub fn new_parallel_job(mut children: Vec<Box<dyn CombinatorJob>>) -> Box<dyn CombinatorJob> {
    match children.len() {
        0 => new_noop_job(),
        1 => children.pop().expect("one child"),
        _ => Box::new(ParallelJob { children }),
    }
}

impl CombinatorJob for ParallelJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let max_alerter = MaxAlerter::new();
        let mut errs: Option<JobFailure> = None;
        // Observable semantics preserved from the baseline: every child runs,
        // no cancel-on-error, all errors are joined, alerts pick the maximum
        // priority. The port launches children in order because the existing
        // `RuntimeClients`/sender cannot be shared across threads without
        // changing `search_job.rs`; see the overview §2.2.
        for child in &mut self.children {
            let (alert, err) = child.run(ctx, clients, stream);
            max_alerter.add(alert);
            errs = combine_errors(errs, err);
        }
        (max_alerter.alert(), errs)
    }

    fn name(&self) -> &'static str {
        "ParallelJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        self.children.iter().map(AsRef::as_ref).collect()
    }
}

/// `TimeoutJob` (`combinators.go:178-224`).
pub struct TimeoutJob {
    timeout: Duration,
    child: Box<dyn CombinatorJob>,
}

/// `NewTimeoutJob` (`combinators.go:173-180`).
pub fn new_timeout_job(timeout: Duration, child: Box<dyn CombinatorJob>) -> Box<dyn CombinatorJob> {
    if child.is_noop() {
        return child;
    }
    Box::new(TimeoutJob { timeout, child })
}

impl CombinatorJob for TimeoutJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let child_ctx = ctx.with_timeout(self.timeout);
        self.child.run(&child_ctx, clients, stream)
    }

    fn name(&self) -> &'static str {
        "TimeoutJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        match verbosity {
            Verbosity::Basic | Verbosity::Max => {
                vec![("timeout".to_owned(), format_go_duration(self.timeout))]
            }
            Verbosity::None => Vec::new(),
        }
    }
}

/// `LimitJob` (`limit.go:23-75`).
pub struct LimitJob {
    limit: i64,
    child: Box<dyn CombinatorJob>,
}

/// `NewLimitJob` (`limit.go:19-27`).
pub fn new_limit_job(limit: i64, child: Box<dyn CombinatorJob>) -> Box<dyn CombinatorJob> {
    if child.is_noop() {
        return child;
    }
    Box::new(LimitJob { limit, child })
}

impl CombinatorJob for LimitJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let (child_ctx, cancel) = ctx.with_cancel();
        let mut limit_stream = LimitStream::new(self.limit, stream, cancel);
        let mut send = |event: SearchEvent| limit_stream.send(event);
        let (alert, mut err) = self.child.run(&child_ctx, clients, &mut send);
        if err.as_ref().is_some_and(JobFailure::is_canceled) {
            // Ignore context canceled errors caused by the limit.
            err = None;
        }
        (alert, err)
    }

    fn name(&self) -> &'static str {
        "LimitJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        match verbosity {
            Verbosity::Basic | Verbosity::Max => {
                vec![("limit".to_owned(), self.limit.to_string())]
            }
            Verbosity::None => Vec::new(),
        }
    }
}

/// `limitStream` (`limit.go:84-129`).
pub struct LimitStream<'a> {
    parent: Sender<'a>,
    on_limit_hit: CancelHandle,
    remaining: i64,
}

impl<'a> LimitStream<'a> {
    fn new(limit: i64, parent: Sender<'a>, on_limit_hit: CancelHandle) -> Self {
        Self {
            parent,
            on_limit_hit,
            remaining: limit,
        }
    }

    /// `limitStream.Send`.
    pub fn send(&mut self, mut event: SearchEvent) {
        let count: i64 = event
            .results
            .iter()
            .fold(0i64, |sum, match_| sum.wrapping_add(match_.result_count()));

        // Avoid limit checks if no change to the result count.
        if count == 0 {
            (self.parent)(event);
            return;
        }

        // Remaining count before/after this event.
        let after = self.remaining.wrapping_sub(count);
        let before = after.wrapping_add(count);

        // Truncate before forwarding; always forward, even with zero results,
        // so stats are preserved.
        if after < 0 {
            let limit = if before < 0 { 0 } else { before };
            matches_limit(&mut event.results, limit);
        }
        (self.parent)(event);
        self.remaining = after;

        // Crossed the zero-remaining threshold exactly once.
        if after <= 0 && before > 0 {
            (self.parent)(SearchEvent {
                results: Vec::new(),
                stats: Stats {
                    is_limit_hit: true,
                    ..Stats::default()
                },
            });
            self.on_limit_hit.cancel();
        }
    }
}

/// `result.Matches.Limit` (`internal/search/result/match.go:130-140`): truncate
/// so that `ResultCount() == limit`, never calling `Match.Limit` with
/// `limit <= 0`.
fn matches_limit(results: &mut Vec<Match>, mut limit: i64) -> i64 {
    let mut index = 0;
    while index < results.len() {
        if limit <= 0 {
            results.truncate(index);
            return 0;
        }
        limit = results[index].limit(limit);
        index += 1;
    }
    limit
}

/// Alert observer injection point for `alert.Job`.
///
/// Mirrors `searchalert.Observer`'s state contract: `HasResults` is set before
/// `Error`, `Error(ctx, err)` updates the alert/error state, and `Done()`
/// returns the final `(alert, err)`.
pub trait AlertObserver {
    fn set_has_results(&mut self, has_results: bool);
    fn observe_error(&mut self, ctx: &RunContext, err: &JobFailure);

    /// `Observer.Error(ctx, err)` with the runtime clients the DB-backed alert
    /// branches read (`observer.go:177-197`). The default keeps observers that
    /// model only the reachable-from-this-slice parts working; the tree
    /// observer overrides it to port `ErrNoResolvedRepos`.
    fn observe_error_with_clients(
        &mut self,
        ctx: &RunContext,
        _clients: &RuntimeClients<'_>,
        err: &JobFailure,
    ) {
        self.observe_error(ctx, err);
    }

    fn done(&mut self) -> (Option<Alert>, Option<JobFailure>);
}

/// Observer factory; a new observer is created per `Run`, like the baseline's
/// `alert.Observer` struct literal.
pub type AlertObserverFactory = Box<dyn Fn() -> Box<dyn AlertObserver>>;

/// `search.Inputs` subset used by `alertJob`.
#[derive(Debug, Clone)]
pub struct AlertInputs {
    pub query: String,
    pub original_query: String,
    pub pattern_type: SearchType,
    pub search_mode: SearchMode,
    pub features: String,
    pub protocol: Protocol,
    pub on_sourcegraph_dot_com: bool,
}

impl Default for AlertInputs {
    fn default() -> Self {
        Self {
            query: String::new(),
            original_query: String::new(),
            pattern_type: SearchType::Standard,
            search_mode: SearchMode::Precise,
            features: "{}".to_owned(),
            protocol: Protocol::Streaming,
            on_sourcegraph_dot_com: false,
        }
    }
}

/// `alertJob` (`alert.go:30-90`).
pub struct AlertJob {
    inputs: AlertInputs,
    child: Box<dyn CombinatorJob>,
    observer_factory: AlertObserverFactory,
}

/// `NewAlertJob` (`alert.go:20-27`).
pub fn new_alert_job(
    inputs: AlertInputs,
    child: Box<dyn CombinatorJob>,
    observer_factory: AlertObserverFactory,
) -> Box<dyn CombinatorJob> {
    if child.is_noop() {
        return child;
    }
    Box::new(AlertJob {
        inputs,
        child,
        observer_factory,
    })
}

impl CombinatorJob for AlertJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        let start = Instant::now();
        let mut count = 0i64;
        let mut stats = Stats::default();
        let mut observer = (self.observer_factory)();

        let (job_alert, mut err) = {
            let mut observed_stream = |event: SearchEvent| {
                let event_count = event
                    .results
                    .iter()
                    .fold(0i64, |sum, match_| sum.wrapping_add(match_.result_count()));
                count = count.wrapping_add(event_count);
                stats.update(&event.stats);
                stream(event);
            };
            self.child.run(ctx, clients, &mut observed_stream)
        };

        observer.set_has_results(count > 0);
        if let Some(err) = &err {
            observer.observe_error_with_clients(ctx, clients, err);
        }
        let (observer_alert, done_err) = observer.done();
        err = done_err;

        // Timeouts are also reported through statistics. Prefer the stats
        // progress notification if any repo timed out.
        if err.as_ref().is_some_and(JobFailure::is_deadline_exceeded) {
            if !stats.status.any(repo_status::TIMED_OUT) {
                let used_time = start.elapsed();
                let suggest_time = longer(2, used_time);
                return (
                    Some(alert_for_timeout(
                        used_time,
                        suggest_time,
                        &self.inputs.original_query,
                        self.inputs.pattern_type,
                    )),
                    None,
                );
            }
            err = None;
        }

        if count == 0
            && self.inputs.search_mode == SearchMode::SmartSearch
            && matches!(
                self.inputs.pattern_type,
                SearchType::Literal | SearchType::Standard
            )
        {
            return (Some(alert_for_smart_search()), None);
        }

        (max_priority_alert(&[job_alert, observer_alert]), err)
    }

    fn name(&self) -> &'static str {
        "AlertJob"
    }

    fn children(&self) -> Vec<&dyn CombinatorJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut result = Vec::new();
        if verbosity == Verbosity::Max {
            result.push(("features".to_owned(), self.inputs.features.clone()));
            result.push((
                "protocol".to_owned(),
                self.inputs.protocol.as_str().to_owned(),
            ));
            result.push((
                "onSourcegraphDotCom".to_owned(),
                self.inputs.on_sourcegraph_dot_com.to_string(),
            ));
        }
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            result.push(("query".to_owned(), self.inputs.query.clone()));
            result.push((
                "originalQuery".to_owned(),
                self.inputs.original_query.clone(),
            ));
            result.push((
                "patternType".to_owned(),
                pattern_type_str(self.inputs.pattern_type).to_owned(),
            ));
        }
        result
    }
}

pub fn pattern_type_str(search_type: SearchType) -> &'static str {
    match search_type {
        SearchType::Regex => "regex",
        SearchType::Literal => "literal",
        SearchType::Structural => "structural",
        SearchType::Standard => "standard",
        SearchType::CodyContext => "codycontext",
        SearchType::Keyword => "keyword",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_stream::convert::{FileMatch, FileRef, RepoMatch};
    use crate::search_stream::events::Range;

    #[test]
    fn go_duration_formatting_matches_baseline() {
        let cases = [
            (0u64, "0s"),
            (1, "1ns"),
            (1_500, "1.5µs"),
            (1_000_000, "1ms"),
            (1_500_000, "1.5ms"),
            (1_000_000_000, "1s"),
            (1_500_000_000, "1.5s"),
            (60_000_000_000, "1m0s"),
            (90_000_000_000, "1m30s"),
            (3_600_000_000_000, "1h0m0s"),
        ];
        for (nanos, want) in cases {
            assert_eq!(format_go_duration(Duration::from_nanos(nanos)), want);
        }
    }

    #[test]
    fn longer_rounds_up_and_has_a_two_second_floor() {
        assert_eq!(longer(2, Duration::ZERO), Duration::from_secs(2));
        assert_eq!(
            longer(2, Duration::from_millis(100)),
            Duration::from_secs(2)
        );
        assert_eq!(longer(2, Duration::from_secs(2)), Duration::from_secs(4));
        assert_eq!(longer(2, Duration::from_secs(31)), Duration::from_secs(120));
        assert_eq!(
            longer(2, Duration::from_secs(3601)),
            Duration::from_secs(10800)
        );
    }

    #[test]
    fn alert_for_timeout_joins_literal_patterns_and_omits_timeout() {
        let alert = alert_for_timeout(
            Duration::from_secs(3),
            Duration::from_secs(6),
            "foo bar timeout:1s",
            SearchType::Literal,
        );
        assert_eq!(
            alert.description,
            "We weren't able to find any results in 3s."
        );
        assert_eq!(alert.proposed_queries[0].query, "timeout:6s foo bar");
    }

    #[test]
    fn max_priority_alert_keeps_first_on_ties_and_skips_none() {
        let low = Alert {
            priority: 1,
            title: "low".to_owned(),
            ..Alert::default()
        };
        let high = Alert {
            priority: 2,
            title: "high".to_owned(),
            ..Alert::default()
        };
        assert_eq!(
            max_priority_alert(&[None, Some(low.clone())])
                .unwrap()
                .title,
            "low"
        );
        assert_eq!(
            max_priority_alert(&[Some(low.clone()), Some(high.clone())])
                .unwrap()
                .title,
            "high"
        );
        assert_eq!(
            max_priority_alert(&[Some(high.clone()), Some(low)])
                .unwrap()
                .title,
            "high"
        );
        assert!(max_priority_alert(&[None, None]).is_none());
    }

    fn file(path: &str) -> Match {
        Match::File(FileMatch {
            file: FileRef {
                repo: crate::search_stream::convert::MinimalRepo {
                    id: 1,
                    name: "repo".to_owned(),
                },
                commit_id: "c".to_owned(),
                path: path.to_owned(),
                input_rev: None,
                precise_language: String::new(),
            },
            chunk_matches: vec![],
            symbols: vec![],
            path_matches: vec![],
            limit_hit: false,
            debug: None,
        })
    }

    fn repo(name: &str) -> Match {
        Match::Repo(RepoMatch {
            name: name.to_owned(),
            id: 1,
            rev: String::new(),
            description_matches: vec![],
            repo_name_matches: vec![],
        })
    }

    #[test]
    fn limit_stream_truncates_and_emits_limit_hit_once() {
        let (ctx, cancel) = RunContext::default().with_cancel();
        let mut events = Vec::new();
        let mut parent = |event: SearchEvent| events.push(event);
        let mut stream = LimitStream::new(5, &mut parent, cancel);
        for _ in 0..10 {
            stream.send(SearchEvent {
                results: vec![repo("r")],
                stats: Stats::default(),
            });
        }
        assert!(ctx.is_canceled());
        let results: i64 = events.iter().map(|event| event.results.len() as i64).sum();
        assert_eq!(results, 5);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.stats.is_limit_hit)
                .count(),
            1
        );
    }

    #[test]
    fn limit_stream_truncates_within_a_match() {
        let (_ctx, cancel) = RunContext::default().with_cancel();
        let mut events = Vec::new();
        let mut parent = |event: SearchEvent| events.push(event);
        let mut stream = LimitStream::new(2, &mut parent, cancel);
        stream.send(SearchEvent {
            results: vec![Match::File(FileMatch {
                file: match file("p") {
                    Match::File(file) => file.file,
                    _ => unreachable!(),
                },
                chunk_matches: vec![crate::search_stream::convert::ChunkMatch {
                    content: String::new(),
                    content_start: crate::search_stream::events::Location::default(),
                    ranges: vec![Range::default(), Range::default(), Range::default()],
                }],
                symbols: vec![],
                path_matches: vec![],
                limit_hit: false,
                debug: None,
            })],
            stats: Stats::default(),
        });
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].results[0].result_count(), 2);
        let Match::File(file) = &events[0].results[0] else {
            panic!("file match");
        };
        assert!(file.limit_hit);
    }

    #[test]
    fn negative_and_zero_limits_never_emit_limit_hit() {
        for limit in [-1, 0] {
            let (ctx, cancel) = RunContext::default().with_cancel();
            let mut events = Vec::new();
            let mut parent = |event: SearchEvent| events.push(event);
            let mut stream = LimitStream::new(limit, &mut parent, cancel);
            stream.send(SearchEvent {
                results: vec![repo("r")],
                stats: Stats::default(),
            });
            assert!(!ctx.is_canceled());
            assert_eq!(events.len(), 1);
            assert!(events[0].results.is_empty());
            assert!(!events[0].stats.is_limit_hit);
        }
    }

    #[test]
    fn sequential_dedups_by_match_key() {
        struct Emit(Vec<Match>);
        impl CombinatorJob for Emit {
            fn run(
                &mut self,
                _ctx: &RunContext,
                _clients: &RuntimeClients<'_>,
                stream: Sender<'_>,
            ) -> (Option<Alert>, Option<JobFailure>) {
                stream(SearchEvent {
                    results: std::mem::take(&mut self.0),
                    stats: Stats::default(),
                });
                (None, None)
            }
            fn name(&self) -> &'static str {
                "Emit"
            }
        }

        let mut job = new_sequential_job(
            true,
            vec![
                Box::new(Emit(vec![file("a"), file("b")])),
                Box::new(Emit(vec![file("a"), file("c")])),
            ],
        );
        let mut seen = Vec::new();
        let mut sink = |event: SearchEvent| {
            for match_ in event.results {
                seen.push(dedup_key(&match_));
            }
        };
        job.run(&RunContext::default(), &test_clients(), &mut sink);
        assert_eq!(seen.len(), 3);
    }

    fn test_clients() -> RuntimeClients<'static> {
        // These combinators never read RuntimeClients; the differential harness
        // uses a fake streamer/lister. Unit tests only need the shape.
        struct NullStreamer;
        impl crate::search_job::JobStreamer for NullStreamer {
            fn stream_search(
                &self,
                _query: &sg_zoekt::query::Query,
                _options: &sg_zoekt::types::SearchOptions,
                _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
            ) -> Result<(), crate::search_job::JobError> {
                Ok(())
            }
        }
        struct NullLister;
        impl crate::search_job::PrivateRepoLister for NullLister {
            fn list_minimal_repos(
                &self,
                _options: &crate::search_job::ReposListOptions,
            ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, crate::search_job::JobError>
            {
                Ok(Vec::new())
            }
        }
        static STREAMER: NullStreamer = NullStreamer;
        static LISTER: NullLister = NullLister;
        RuntimeClients {
            db: &LISTER,
            zoekt: &STREAMER,
            max_repos: None,
            searcher: None,
            commit: None,
            repo_pager: None,
        }
    }
}
