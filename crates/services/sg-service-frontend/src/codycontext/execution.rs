#![forbid(unsafe_code)]

//! The blocking execution half of `internal/search/codycontext/job.go`
//! (`searchJob.Run`, `doSearch`, `limitCodeResponse`, `Attributes`).
//!
//! The construction half (`NewSearchJob`) lives in the parent module. This
//! layer reuses the existing [`CombinatorJob`] executor and takes the child
//! jobs as boxed combinators, so production wiring converts the out-of-line
//! sub-job trees in `search_jobutil_tree`, while tests inject fakes.
//!
//! Observable semantics ported verbatim (see
//! `docs/overviews/m9_8jy_codycontext_execution_overview.md`):
//!
//! * `Run` collects from three sub-jobs and sends text → symbol → code;
//! * each `doSearch` collects only `*result.FileMatch` that pass the file
//!   matcher, stops at `limit` (including a non-positive limit) and cancels
//!   its own sub-context on reaching it;
//! * `limitCodeResponse` drops code results already present in the symbol
//!   results and applies `min(len(code), codeCount - len(symbol))`;
//! * alerts use `search.MaxPriorityAlert` (strictly higher priority wins,
//!   ties keep the earlier response); errors are appended in response order.
//!
//! Unlike the baseline's concurrent `conc/pool` fan-out, the three child jobs
//! run one at a time (text, then symbol, then code). Each `doSearch` owns an
//! independent cancel context and the response order is fixed, so the
//! observable output is identical; this matches how the ported `ParallelJob`
//! already models Go concurrency.

use std::collections::HashSet;

use crate::search_job::{RuntimeClients, Verbosity};
use crate::search_jobutil_combinators::{
    max_priority_alert, Alert, CombinatorJob, JobFailure, NoopJob, RunContext, Sender,
};
use crate::search_stream::convert::{FileMatch, Match};
use crate::search_stream::progress::{SearchEvent, Stats};

use super::{CodyFileMatcher, SYMBOL_RESULTS_COUNT};

/// `searchJob` (`job.go:125-135`), with the three child jobs handed over as
/// combinators. `symbol_job == None` models the baseline's `newNoopJob()`.
pub struct CodyContextExecutionJob {
    pub symbol_job: Option<Box<dyn CombinatorJob>>,
    pub code_job: Box<dyn CombinatorJob>,
    pub text_job: Box<dyn CombinatorJob>,
    pub patterns: Vec<String>,
    pub code_count: i32,
    pub text_count: i32,
    pub file_matcher: CodyFileMatcher,
}

impl CodyContextExecutionJob {
    pub fn new(
        symbol_job: Option<Box<dyn CombinatorJob>>,
        code_job: Box<dyn CombinatorJob>,
        text_job: Box<dyn CombinatorJob>,
        patterns: Vec<String>,
        code_count: i32,
        text_count: i32,
        file_matcher: CodyFileMatcher,
    ) -> Self {
        Self {
            symbol_job,
            code_job,
            text_job,
            patterns,
            code_count,
            text_count,
            file_matcher,
        }
    }

    /// `searchJob.MapChildren` (`job.go:263-265`): the map function is never
    /// called and the receiver itself is returned.
    pub fn map_children(
        &self,
        _map: &mut dyn FnMut(&dyn CombinatorJob) -> Box<dyn CombinatorJob>,
    ) -> &Self {
        self
    }
}

/// One `response` (`job.go:199-203`). `matches` only ever holds file matches,
/// exactly like the baseline collector.
struct Response {
    matches: Vec<Match>,
    err: Option<JobFailure>,
    alert: Option<Alert>,
}

/// `result.Key` for a file match: repo name, commit, path and `InputRev`
/// (nil and `""` collapse to the same key).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    repo: String,
    commit: String,
    path: String,
    rev: String,
}

fn file_key(file: &FileMatch) -> FileKey {
    FileKey {
        repo: file.file.repo.name.clone(),
        commit: file.file.commit_id.clone(),
        path: file.file.path.clone(),
        rev: file.file.input_rev.clone().unwrap_or_default(),
    }
}

/// `doSearch` (`job.go:205-239`).
fn do_search(
    ctx: &RunContext,
    clients: &RuntimeClients<'_>,
    job: &mut dyn CombinatorJob,
    limit: i32,
    file_matcher: &CodyFileMatcher,
) -> Response {
    let (child_ctx, cancel) = ctx.with_cancel();
    let mut collected: Vec<Match> = Vec::new();
    let mut collector = |event: SearchEvent| {
        if collected.len() as i64 >= i64::from(limit) {
            return;
        }

        for result in event.results {
            // Only `*result.FileMatch` is collected; everything else is
            // silently dropped (`job.go:222-229`).
            let Match::File(file) = &result else {
                continue;
            };
            if !file_matcher.accepts(file.file.repo.id, &file.file.path) {
                continue;
            }

            collected.push(result);
            if collected.len() as i64 >= i64::from(limit) {
                cancel.cancel();
                return;
            }
        }
    };

    let (alert, err) = job.run(&child_ctx, clients, &mut collector);
    drop(collector);
    Response {
        matches: collected,
        err,
        alert,
    }
}

/// `limitCodeResponse` (`job.go:179-197`).
fn limit_code_response(symbol_response: &Response, code_response: &mut Response, code_count: i32) {
    let mut symbol_keys: HashSet<FileKey> = HashSet::new();
    for result in &symbol_response.matches {
        if let Match::File(file) = result {
            symbol_keys.insert(file_key(file));
        }
    }

    let limit = (code_response.matches.len() as i64)
        .min(i64::from(code_count) - symbol_response.matches.len() as i64);

    let mut code_matches = Vec::with_capacity(code_response.matches.len());
    for result in code_response.matches.drain(..) {
        if code_matches.len() as i64 >= limit {
            break;
        }

        let duplicate = match &result {
            Match::File(file) => symbol_keys.contains(&file_key(file)),
            _ => false,
        };
        if !duplicate {
            code_matches.push(result);
        }
    }
    code_response.matches = code_matches;
}

/// `errors.Append`: skip nils, flatten multi errors in order.
fn append_error(current: Option<JobFailure>, next: Option<JobFailure>) -> Option<JobFailure> {
    match (current, next) {
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

/// Normalize `attribute.StringSlice` the way `attribute.Value.Emit()` does:
/// `json.Marshal` of the slice (`["a","b"]`, and `"[]"` when empty).
fn string_slice(values: &[String]) -> String {
    let values: Vec<&str> = values.iter().map(String::as_str).collect();
    serde_json::to_string(&values).expect("string slice is serializable")
}

impl CombinatorJob for CodyContextExecutionJob {
    fn run(
        &mut self,
        ctx: &RunContext,
        clients: &RuntimeClients<'_>,
        stream: Sender<'_>,
    ) -> (Option<Alert>, Option<JobFailure>) {
        // `Run` starts all three concurrently and waits text, symbol, code.
        // Independent contexts make the order unobservable; run them in the
        // same order the responses are sent.
        let text_response = do_search(
            ctx,
            clients,
            &mut *self.text_job,
            self.text_count,
            &self.file_matcher,
        );

        let symbol_limit = self.code_count.min(SYMBOL_RESULTS_COUNT);
        let mut noop = NoopJob;
        let symbol_job: &mut dyn CombinatorJob = match self.symbol_job.as_mut() {
            Some(job) => &mut **job,
            None => &mut noop,
        };
        let symbol_response = do_search(ctx, clients, symbol_job, symbol_limit, &self.file_matcher);

        let mut code_response = do_search(
            ctx,
            clients,
            &mut *self.code_job,
            self.code_count,
            &self.file_matcher,
        );

        limit_code_response(&symbol_response, &mut code_response, self.code_count);

        let mut alert: Option<Alert> = None;
        let mut err: Option<JobFailure> = None;
        // For consistency always send text, symbol, then code.
        for response in [text_response, symbol_response, code_response] {
            stream(SearchEvent {
                results: response.matches,
                stats: Stats::default(),
            });
            alert = max_priority_alert(&[alert, response.alert]);
            err = append_error(err, response.err);
        }
        (alert, err)
    }

    fn name(&self) -> &'static str {
        "CodyContextSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        match verbosity {
            Verbosity::Max | Verbosity::Basic => vec![
                ("patterns".to_owned(), string_slice(&self.patterns)),
                ("codeCount".to_owned(), self.code_count.to_string()),
                ("textCount".to_owned(), self.text_count.to_string()),
            ],
            Verbosity::None => Vec::new(),
        }
    }
}
