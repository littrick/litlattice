//! Code-monitor trigger search (`internal/codemonitors/search.go`).
//!
//! Ported entry points:
//!
//! * [`search`] — `codemonitors.Search`: `Plan("V3", Prevent, Precise,
//!   Streaming, contextLines=0)`, `NewPlanJob`, the per-repo code-monitor hook
//!   ([`add_code_monitor_hook`] + [`hook_with_id`]), and an
//!   `AggregatingStream` that only accepts `*result.CommitMatch`;
//! * [`snapshot`] — `codemonitors.Snapshot`: the dummy revision-resolving run
//!   that records the current commit hashes per repo;
//! * [`add_code_monitor_hook`], [`hook_with_id`], [`strings_equal`],
//!   [`ERR_INVALID_MONITOR_QUERY`] and the "all branches" error.
//!
//! The DB writes the hook performs (`GetLastSearched` / `UpsertLastSearched` /
//! `UpdateTriggerJobWithLogs`) are injected through [`TriggerSearchStore`] so
//! the worker can bridge them to Postgres while the differential harness uses
//! an in-memory fake. Search execution uses the runtime assembled by
//! [`crate::search_runtime`] (the same `TreeRuntimeClients` the frontend search
//! stream uses); the repo manager is still `None` in production (M9.8hw), which
//! the job tree reports as its existing `RepoPagerJob execution is not ported`
//! error.
//!
//! See `docs/overviews/m9_9f_codemonitors_trigger_search_overview.md`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sg_proto::gitserver as pb;

use crate::search_client::{Protocol, SearchClient, SearchMode};
use crate::search_commit_job::{
    CodeMonitorHook, CodeMonitorHookHandle, DoSearchFunc, GitserverClient,
};
use crate::search_job::{JobContext, JobError};
use crate::search_jobutil_combinators::JobFailure;
use crate::search_jobutil_tree::{new_plan_job, JobNode};
use crate::search_searcher_client::SearcherErrorKind;
use crate::search_stream::convert::{CommitMatch, Match};
use crate::search_stream::{
    default_plan_deps, inputs_from_plan, tree_inputs_with_context, TreeRuntimeClients,
};

/// `codemonitors.ErrInvalidMonitorQuery` (`search.go:148`).
pub const ERR_INVALID_MONITOR_QUERY: &str =
    "code monitor cannot use different patterns for different repos";

/// `addCodeMonitorHook`'s leaf error (`search.go:170`).
pub const ERR_ALL_BRANCHES: &str =
    "all branches of query must be of type:diff or type:commit. If you have an AND/OR operator in your query, ensure that both sides have type:commit or type:diff.";

/// `codemonitors.Search` failure: `errcode.MakeNonRetryable` is applied to the
/// planning and hook-wiring errors, and not to the execution errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerSearchFailure {
    pub message: String,
    pub non_retryable: bool,
}

impl TriggerSearchFailure {
    pub fn new(message: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            non_retryable,
        }
    }

    fn non_retryable(message: impl Into<String>) -> Self {
        Self::new(message, true)
    }

    fn retryable(message: impl Into<String>) -> Self {
        Self::new(message, false)
    }
}

impl fmt::Display for TriggerSearchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TriggerSearchFailure {}

/// The subset of `database.CodeMonitorStore` the hook reads/writes. The
/// search hook runs synchronously; production implementations bridge to the
/// async DB themselves (the worker's `PgTriggerSearchStore`).
pub trait TriggerSearchStore: Send + Sync {
    /// `GetLastSearched(monitorID, repoID)`; `None` is `sql.ErrNoRows`.
    fn get_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
    ) -> Result<Option<Vec<String>>, JobError>;

    /// `UpsertLastSearched(monitorID, repoID, commitOIDs)`.
    fn upsert_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
        commit_oids: &[String],
    ) -> Result<(), JobError>;

    /// `UpdateTriggerJobWithLogs(triggerID, TriggerJobLogs{Message})`.
    fn update_trigger_job_with_logs(
        &self,
        trigger_id: i32,
        message: &str,
    ) -> Result<(), JobError>;
}

/// Plan + build the job tree, the shared prefix of `Search` and `Snapshot`.
///
/// `request_context` is `actor.FromContext(ctx)` (`search.go:29`, `:86`): the
/// worker passes the monitor owner's user actor, the GraphQL `Snapshot` caller
/// passes the request actor, and both fall back to anonymous — never to an
/// internal actor that would leak repository visibility or search contexts.
fn plan_tree(
    query: &str,
    request_context: crate::search_contexts::RequestContext,
) -> Result<JobNode, JobError> {
    let client = SearchClient::new(default_plan_deps());
    let planned = client
        .plan_with_context(
            "V3",
            None,
            query,
            SearchMode::Precise,
            Protocol::Streaming,
            Some(0),
            &request_context,
        )
        .map_err(|error| JobError(error.to_string()))?;
    let inputs = inputs_from_plan(planned);
    let tree_inputs = tree_inputs_with_context(&inputs, request_context);
    new_plan_job(&tree_inputs, &inputs.plan)

}

fn code_monitor_hook(
    store: Arc<dyn TriggerSearchStore + Send + Sync>,
    monitor_id: i64,
    trigger_id: i32,
) -> Arc<CodeMonitorHook> {
    Arc::new(
        move |ctx: &JobContext,
              _db: &dyn crate::search_commit_job::CommitUserDB,
              gs: &dyn GitserverClient,
              args: &pb::SearchRequest,
              repo_id: u32,
              do_search: &mut DoSearchFunc<'_>| {
            hook_with_id(
                ctx,
                store.as_ref(),
                gs,
                monitor_id,
                trigger_id,
                repo_id,
                args,
                do_search,
            )
        },
    )
}

/// `codemonitors.Search` (`search.go:28-75`).
pub fn search(
    runtime: &TreeRuntimeClients,
    store: Arc<dyn TriggerSearchStore + Send + Sync>,
    query: &str,
    monitor_id: i64,
    trigger_id: i32,
    request_context: crate::search_contexts::RequestContext,
) -> Result<Vec<CommitMatch>, TriggerSearchFailure> {
    let tree =
        plan_tree(query, request_context).map_err(|error| TriggerSearchFailure::non_retryable(error.0))?;

    let hook = CodeMonitorHookHandle::new(code_monitor_hook(store, monitor_id, trigger_id));
    let mut tree = add_code_monitor_hook(&tree, hook)
        .map_err(|error| TriggerSearchFailure::non_retryable(error.0))?;

    let mut results = Vec::new();
    let failure = run_tree(runtime, &mut tree, request_context, &mut |event| {
        results.extend(event.results);
    });
    if let Some(failure) = failure {
        return Err(TriggerSearchFailure::retryable(failure.to_string()));
    }

    results
        .into_iter()
        .map(|result| match result {
            Match::Commit(commit) => Ok(commit),
            Match::File(_) => Err(TriggerSearchFailure::retryable(format!(
                "expected search to only return commit matches, but got type {}",
                "*result.FileMatch"
            ))),
            Match::Repo(_) => Err(TriggerSearchFailure::retryable(format!(
                "expected search to only return commit matches, but got type {}",
                "*result.RepoMatch"
            ))),
            Match::Owner(_) => Err(TriggerSearchFailure::retryable(format!(
                "expected search to only return commit matches, but got type {}",
                "*result.OwnerMatch"
            ))),
        })
        .collect()
}

/// `codemonitors.Snapshot` (`search.go:77-146`).
///
/// `in_transaction` stands in for `db.Handle().InTransaction()`: callers that
/// run inside the baseline's DB transaction must pass `true` to reproduce the
/// `"Snapshot cannot be run in a transaction"` guard. The returned map is keyed
/// by `api.RepoID`.
pub fn snapshot(
    runtime: &TreeRuntimeClients,
    query: &str,
    in_transaction: bool,
    request_context: crate::search_contexts::RequestContext,
) -> Result<BTreeMap<i32, Vec<String>>, JobError> {
    if in_transaction {
        return Err(JobError(
            "Snapshot cannot be run in a transaction".to_owned(),
        ));
    }

    let tree = plan_tree(query, request_context)?;
    let resolved = Arc::new(Mutex::new(BTreeMap::<i32, Vec<String>>::new()));

    let hook = {
        let resolved = Arc::clone(&resolved);
        CodeMonitorHookHandle::new(Arc::new(
            move |ctx: &JobContext,
                  _db: &dyn crate::search_commit_job::CommitUserDB,
                  gs: &dyn GitserverClient,
                  args: &pb::SearchRequest,
                  repo_id: u32,
                  _do_search: &mut DoSearchFunc<'_>| {
                for rev in &args.revisions {
                    if context_done(ctx) {
                        return Err(JobError("context deadline exceeded".to_owned()));
                    }
                    match gs.resolve_revision(&args.repo, &rev.rev_spec, false) {
                        Ok(hash) => resolved
                            .lock()
                            .expect("snapshot map poisoned")
                            .entry(repo_id as i32)
                            .or_default()
                            .push(hash),
                        // Missing revisions are logged when the job executes;
                        // the snapshot itself skips them.
                        Err(error) if error.kind == SearcherErrorKind::RevisionNotFound => {}
                        Err(error) => return Err(JobError(error.message)),
                    }
                }
                Ok(())
            },
        ))
    };

    let mut tree = add_code_monitor_hook(&tree, hook)?;
    let failure = run_tree(runtime, &mut tree, request_context, &mut |_event| {});
    if let Some(failure) = failure {
        return Err(JobError(failure.to_string()));
    }

    // The hook closure keeps its `Arc` alive after `run_tree`, so copy the
    // resolved map out instead of trying to take ownership of the `Arc`.
    let resolved = resolved
        .lock()
        .expect("snapshot map poisoned")
        .clone();
    Ok(resolved)
}

/// Run one tree against the injected runtime (`planJob.Run`). The baseline
/// passes an `AggregatingStream`/`NullStream`; the caller collects the events.
fn run_tree(
    runtime: &TreeRuntimeClients,
    tree: &mut JobNode,
    request_context: crate::search_contexts::RequestContext,
    stream: &mut dyn FnMut(crate::search_stream::progress::SearchEvent),
) -> Option<JobFailure> {
    let ctx = JobContext {
        deadline: None,
        request_context,
    };
    runtime.with_clients(|clients| tree.run_with_alert(&ctx, clients, stream).1)
}

/// `addCodeMonitorHook` (`search.go:150-176`).
///
/// Applies the `job.Map` traversal bottom-up: commit jobs get the hook and
/// concurrency 1, `ComputeExcluded`/`Noop` become a plain no-op, and any other
/// childless leaf records the "all branches" error. More than one commit job
/// records [`ERR_INVALID_MONITOR_QUERY`].
pub fn add_code_monitor_hook(
    tree: &JobNode,
    hook: CodeMonitorHookHandle,
) -> Result<JobNode, JobError> {
    let mut commit_search_job_count = 0usize;
    let mut error: Option<JobError> = None;

    let mapped = map_job(tree, &mut |node| {
        if node.is_commit_search_job() {
            commit_search_job_count += 1;
            if commit_search_job_count > 1 && error.is_none() {
                error = Some(JobError(ERR_INVALID_MONITOR_QUERY.to_owned()));
            }
            return Some(node.with_code_monitor_hook(hook.clone()));
        }
        if node.is_noop_or_compute_excluded_job() {
            return Some(node.as_noop());
        }
        if node.children().is_empty() && error.is_none() {
            error = Some(JobError(ERR_ALL_BRANCHES.to_owned()));
        }
        None
    });

    match error {
        Some(error) => Err(error),
        None => Ok(mapped),
    }
}

/// `job.Map` over [`JobNode`]: map children first, then rewrite the node.
fn map_job(node: &JobNode, f: &mut dyn FnMut(&JobNode) -> Option<JobNode>) -> JobNode {
    let children = node
        .children()
        .iter()
        .map(|child| map_job(child, f))
        .collect();
    let mapped = node.with_children(children);
    f(&mapped).unwrap_or(mapped)
}

/// `hookWithID` (`search.go:178-260`).
#[allow(clippy::too_many_arguments)]
pub fn hook_with_id(
    ctx: &JobContext,
    store: &dyn TriggerSearchStore,
    gs: &dyn GitserverClient,
    monitor_id: i64,
    trigger_id: i32,
    repo_id: u32,
    args: &pb::SearchRequest,
    do_search: &mut DoSearchFunc<'_>,
) -> Result<(), JobError> {
    let repo_id = repo_id as i32;

    // Resolve the requested revisions into a static set of commit hashes.
    let mut commit_hashes = Vec::with_capacity(args.revisions.len());
    for rev in &args.revisions {
        if context_done(ctx) {
            return Err(JobError("context deadline exceeded".to_owned()));
        }

        match gs.resolve_revision(&args.repo, &rev.rev_spec, false) {
            Ok(hash) => commit_hashes.push(hash),
            Err(error) if error.kind == SearcherErrorKind::RevisionNotFound => {
                // Missing revisions can happen for empty repos; log a warning
                // on the trigger job and continue. The "WARNING: " prefix keeps
                // the log from looking like a "successfully failed" status.
                let message = format!("WARNING: {}", error.message);
                if let Err(log_error) = store.update_trigger_job_with_logs(trigger_id, &message) {
                    tracing::error!(log = %message, error = %log_error, "Error updating trigger job with log");
                }
                continue;
            }
            Err(error) => return Err(JobError(error.message)),
        }
    }

    // Look up the previously searched set of commit hashes.
    let mut last_searched = store
        .get_last_searched(monitor_id, repo_id)?
        .unwrap_or_default();
    if strings_equal(&mut commit_hashes, &mut last_searched) {
        // The repo hasn't changed since the last search.
        return Ok(());
    }

    // Merge requested hashes and excluded hashes.
    let mut new_revs = Vec::with_capacity(commit_hashes.len() + last_searched.len());
    new_revs.extend(commit_hashes.iter().cloned());
    new_revs.extend(last_searched.iter().map(|hash| format!("^{hash}")));

    let mut args_copy = args.clone();
    args_copy.revisions = new_revs
        .into_iter()
        .map(|rev_spec| pb::RevisionSpecifier { rev_spec })
        .collect();

    // Execute the search.
    let search_error = do_search(&args_copy);

    // Always save the last-searched commits: streamed results were already
    // delivered, and re-searching would notify the user twice.
    if let Err(error) = store.upsert_last_searched(monitor_id, repo_id, &commit_hashes) {
        return Err(error);
    }

    // Still return the search error so it can be displayed.
    match search_error {
        Err(JobError(message)) => Err(JobError(format!(
            "search failed, some commits may be skipped: {message}"
        ))),
        Ok(()) => Ok(()),
    }
}

/// `stringsEqual` (`search.go:262-276`): sorts both slices in place.
pub fn strings_equal(left: &mut [String], right: &mut [String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.sort();
    right.sort();
    left.iter().zip(right.iter()).all(|(left, right)| left == right)
}

fn context_done(ctx: &JobContext) -> bool {
    ctx.deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
}

/// `result.CommitMatch.MarshalJSON` (`result/commit_json.go`), rendered as a
/// JSON object with Go's stable field names. `repoStars` is always `0`: the
/// ported `convert::MinimalRepo` does not carry stars (errata in the overview).
pub fn commit_match_json(m: &CommitMatch) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert("repoID".to_owned(), m.repo.id.into());
    object.insert("repoName".to_owned(), m.repo.name.clone().into());
    object.insert("repoStars".to_owned(), 0.into());
    object.insert("commitID".to_owned(), m.commit.id.clone().into());
    object.insert("author".to_owned(), signature_json(&m.commit.author));
    object.insert("committer".to_owned(), signature_json(&m.commit.committer));
    object.insert("message".to_owned(), m.commit.message.clone().into());
    if !m.commit.parents.is_empty() {
        object.insert("parents".to_owned(), strings_json(&m.commit.parents));
    }
    if !m.refs.is_empty() {
        object.insert("refs".to_owned(), strings_json(&m.refs));
    }
    if !m.source_refs.is_empty() {
        object.insert("sourceRefs".to_owned(), strings_json(&m.source_refs));
    }
    if let Some(preview) = &m.message_preview {
        object.insert("messagePreview".to_owned(), matched_string_json(preview));
    }
    if let Some(preview) = &m.diff_preview {
        object.insert("diffPreview".to_owned(), matched_string_json(preview));
    }
    if !m.modified_files.is_empty() {
        object.insert("modifiedFiles".to_owned(), strings_json(&m.modified_files));
    }
    serde_json::Value::Object(object)
}

/// Serialize a whole `[]*result.CommitMatch` result set (nil-safe: an empty
/// slice marshals as `[]`).
pub fn commit_matches_json(matches: &[CommitMatch]) -> serde_json::Value {
    serde_json::Value::Array(matches.iter().map(commit_match_json).collect())
}

fn strings_json(values: &[String]) -> serde_json::Value {
    serde_json::Value::Array(values.iter().cloned().map(Into::into).collect())
}

fn signature_json(signature: &crate::search_stream::convert::Signature) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert("name".to_owned(), signature.name.clone().into());
    object.insert("email".to_owned(), signature.email.clone().into());
    object.insert("date".to_owned(), go_time_string(signature.date).into());
    serde_json::Value::Object(object)
}

fn matched_string_json(
    matched: &crate::search_stream::convert::MatchedString,
) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert("content".to_owned(), matched.content.clone().into());
    object.insert(
        "matchedRanges".to_owned(),
        serde_json::Value::Array(matched.matched_ranges.iter().map(range_json).collect()),
    );
    serde_json::Value::Object(object)
}

fn range_json(range: &crate::search_stream::events::Range) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert("start".to_owned(), location_json(&range.start));
    object.insert("end".to_owned(), location_json(&range.end));
    serde_json::Value::Object(object)
}

fn location_json(location: &crate::search_stream::events::Location) -> serde_json::Value {
    // `result.Location.MarshalJSON` encodes `[offset, line, column]`.
    serde_json::Value::Array(vec![
        location.offset.into(),
        location.line.into(),
        location.column.into(),
    ])
}

/// `time.Time` JSON encoding (`time.RFC3339Nano`, trailing fractional zeros
/// trimmed, always `Z` for UTC).
pub fn go_time_string(time: time::OffsetDateTime) -> String {
    let time = time.to_offset(time::UtcOffset::UTC);
    let mut result = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second()
    );
    let nanos = time.nanosecond();
    if nanos != 0 {
        let fraction = format!("{nanos:09}");
        let fraction = fraction.trim_end_matches('0');
        result.push('.');
        result.push_str(fraction);
    }
    result.push('Z');
    result
}
