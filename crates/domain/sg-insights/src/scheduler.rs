#![forbid(unsafe_code)]

//! `internal/insights/scheduler`: the pure decision helpers used by the
//! backfill state machine.
//!
//! This module ports the parts of the scheduler that do not touch the
//! database: the [`BackfillState`] enum and its [`SeriesBackfill`] record,
//! [`calculate_error_threshold`], [`parse_query`] (which turns an insight
//! series query into a search plan, honouring the capture-group compute
//! path), and [`translate_incomplete_reasons`]. The `BackfillStore`,
//! worker wiring and the repo-iterator storage methods live in the database
//! layer and are left for the storage-wiring slice; the iterator's pure
//! state logic is in [`crate::iterator`].

use sg_insights_query::{compute, parse_query as parse_plan};
use sg_search_query::Plan;

/// `scheduler.BackfillState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackfillState {
    New,
    Processing,
    Completed,
    Failed,
}

impl BackfillState {
    /// The stored string value (`insight_series_backfill.state`).
    pub fn as_str(&self) -> &'static str {
        match self {
            BackfillState::New => "new",
            BackfillState::Processing => "processing",
            BackfillState::Completed => "completed",
            BackfillState::Failed => "failed",
        }
    }

    /// Maps a stored state string; unknown values map to `None` (the Go
    /// `BackfillState` is a string type, but only these four are produced).
    pub fn from_str_value(value: &str) -> Option<BackfillState> {
        match value {
            "new" => Some(BackfillState::New),
            "processing" => Some(BackfillState::Processing),
            "completed" => Some(BackfillState::Completed),
            "failed" => Some(BackfillState::Failed),
            _ => None,
        }
    }
}

/// `scheduler.SeriesBackfill` (the database-backed fields; the unexported
/// `repoIteratorId` is `repo_iterator_id` here, with 0 meaning NULL).
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesBackfill {
    pub id: i64,
    pub series_id: i64,
    pub repo_iterator_id: i64,
    pub estimated_cost: f64,
    pub state: BackfillState,
}

impl SeriesBackfill {
    /// `SeriesBackfill.IsTerminalState`.
    pub fn is_terminal_state(&self) -> bool {
        self.state == BackfillState::Completed || self.state == BackfillState::Failed
    }
}

/// `calculateErrorThreshold`: `int(float64(cardinality)*percent)`, floored at
/// `floor`.
pub fn calculate_error_threshold(percent: f64, floor: i64, cardinality: i64) -> i64 {
    let scaled = (cardinality as f64 * percent) as i64;
    if scaled <= floor {
        floor
    } else {
        scaled
    }
}

/// `parseQuery`: parse an insight series query into a search plan.
///
/// Capture-group series first parse the query as a compute query
/// (`internal/compute.Parse`), convert it back with `ToSearchQuery`, and
/// parse that as a `regexp` query; other series parse as `literal`. Errors
/// are wrapped with the baseline's prefixes.
pub fn parse_query(query: &str, generated_from_capture_groups: bool) -> Result<Plan, String> {
    if generated_from_capture_groups {
        let compute_query = compute::parse(query).map_err(|err| format!("compute.Parse: {err}"))?;
        let search_query = compute_query.to_search_query();
        parse_plan(&search_query, "regexp").map_err(|err| format!("ParseQuery: {err}"))
    } else {
        parse_plan(query, "literal").map_err(|err| format!("ParseQuery: {err}"))
    }
}

/// `store.IncompleteReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IncompleteReason {
    Timeout,
    Generic,
}

impl IncompleteReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncompleteReason::Timeout => "timeout",
            IncompleteReason::Generic => "generic",
        }
    }
}

/// Mirror of `queryrunner.SearchTimeoutError` (a sentinel created with
/// `errors.New`). The query-runner port currently classifies this error by
/// message (`is_search_timeout`); this type is the shared sentinel the
/// scheduler path will use once the two crates converge.
#[derive(Debug, Clone, Copy)]
pub struct SearchTimeoutError;

impl std::fmt::Display for SearchTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("search timeout")
    }
}

impl std::error::Error for SearchTimeoutError {}

/// `translateIncompleteReasons`: `errors.Is(err, SearchTimeoutError)` over
/// the error's source chain, else `generic`.
pub fn translate_incomplete_reasons(err: &(dyn std::error::Error + 'static)) -> IncompleteReason {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = current {
        if err.downcast_ref::<SearchTimeoutError>().is_some() {
            return IncompleteReason::Timeout;
        }
        current = err.source();
    }
    IncompleteReason::Generic
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_state_matches_baseline() {
        let backfill = |state| SeriesBackfill {
            id: 1,
            series_id: 2,
            repo_iterator_id: 0,
            estimated_cost: 0.0,
            state,
        };
        assert!(!backfill(BackfillState::New).is_terminal_state());
        assert!(!backfill(BackfillState::Processing).is_terminal_state());
        assert!(backfill(BackfillState::Completed).is_terminal_state());
        assert!(backfill(BackfillState::Failed).is_terminal_state());
        assert_eq!(BackfillState::Processing.as_str(), "processing");
    }

    #[test]
    fn error_threshold_matches_baseline() {
        // From `internal/insights/scheduler/backfill_state_inprogress_handler_test.go`.
        assert_eq!(calculate_error_threshold(0.05, 10, 100), 10);
        assert_eq!(calculate_error_threshold(0.10, 10, 150), 15);
        assert_eq!(calculate_error_threshold(0.05, 50, 0), 50);
        assert_eq!(calculate_error_threshold(0.05, 0, 1000), 50);
    }

    #[test]
    fn parse_query_literal_matches_baseline() {
        let plan = parse_query("repo:^github\\.com/foo/bar$ file:\\.go$ func main", false)
            .expect("valid query");
        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn parse_query_capture_groups_goes_through_compute() {
        let plan = parse_query(
            "repo:^github\\.com/sourcegraph/sourcegraph$ file:\\.go$ (?P<key>func main)",
            true,
        )
        .expect("valid compute query");
        assert_eq!(plan.len(), 1);
        // The compute path returns the rewritten search query, wrapped in
        // parentheses by StringHuman.
        assert!(plan[0].pattern_string().contains("func main"));
    }

    #[test]
    fn incomplete_reasons_follow_source_chain() {
        #[derive(Debug)]
        struct Wrapped(SearchTimeoutError);
        impl std::fmt::Display for Wrapped {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("searchHandler")
            }
        }
        impl std::error::Error for Wrapped {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        #[derive(Debug)]
        struct Boom;
        impl std::fmt::Display for Boom {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("boom")
            }
        }
        impl std::error::Error for Boom {}

        assert_eq!(
            translate_incomplete_reasons(&Wrapped(SearchTimeoutError)),
            IncompleteReason::Timeout
        );
        assert_eq!(
            translate_incomplete_reasons(&SearchTimeoutError),
            IncompleteReason::Timeout
        );
        assert_eq!(
            translate_incomplete_reasons(&Boom),
            IncompleteReason::Generic
        );
    }
}
