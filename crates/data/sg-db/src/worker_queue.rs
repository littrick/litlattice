#![forbid(unsafe_code)]

//! `internal/workerutil/dbworker/store`'s `CountByState`, the only dbworker
//! store method the worker's `executors-multiqueue-metrics-reporter` job uses.
//!
//! `CountByState` runs
//!
//! ```sql
//! SELECT COUNT(*)
//! FROM <ViewName>
//! WHERE state IN ('queued','errored','processing')
//! ```
//!
//! against the store's view (`Options.ViewName`; the view is referenced with
//! its SQL alias exactly as in the baseline). The state bitset is expanded in
//! the fixed order `queued, errored, processing`. The views are the ones the
//! two queue stores configure:
//!
//! * `autoindexing.IndexWorkerStoreOptions` → `lsif_indexes_with_repository_name u`
//!   (JOINs `repo`, so a soft-deleted repo's indexes are invisible);
//! * `batchSpecWorkspaceExecutionWorkerStoreOptions` →
//!   `batch_spec_workspace_execution_jobs_with_rank batch_spec_workspace_execution_jobs`.
//!
//! Migrations stay owned by the Go `migrator`; this module only reads.

use sqlx::postgres::PgPool;

/// `store.StateQueued`.
pub const STATE_QUEUED: u32 = 1 << 0;
/// `store.StateErrored`.
pub const STATE_ERRORED: u32 = 1 << 1;
/// `store.StateProcessing`.
pub const STATE_PROCESSING: u32 = 1 << 2;

/// The bitset the executor auto-scaling metric counts:
/// `StateQueued|StateErrored|StateProcessing`.
pub const QUEUED_ERRORED_PROCESSING: u32 = STATE_QUEUED | STATE_ERRORED | STATE_PROCESSING;

/// `autoindexing.IndexWorkerStoreOptions.ViewName`.
pub const LSIF_INDEXES_VIEW: &str = "lsif_indexes_with_repository_name u";

/// `batchSpecWorkspaceExecutionWorkerStoreOptions.ViewName`.
pub const BATCH_SPEC_WORKSPACE_EXECUTION_JOBS_VIEW: &str =
    "batch_spec_workspace_execution_jobs_with_rank batch_spec_workspace_execution_jobs";

/// `store.countByStateQuery`.
const COUNT_BY_STATE_QUERY: &str = "SELECT COUNT(*)\nFROM %s\nWHERE state IN (%s)\n";

/// `store.errAtLeastOneState`.
pub const ERR_AT_LEAST_ONE_STATE: &str =
    "pre-condition failure: statesBitset should contain at least one state";

/// `RecordState.toList`: the state names for a bitset, in the baseline's
/// fixed order.
pub fn state_list(states: u32) -> Vec<&'static str> {
    let mut names = Vec::with_capacity(3);
    if states & STATE_QUEUED != 0 {
        names.push("queued");
    }
    if states & STATE_ERRORED != 0 {
        names.push("errored");
    }
    if states & STATE_PROCESSING != 0 {
        names.push("processing");
    }
    names
}

/// `(*store[T]).CountByState`.
///
/// The baseline returns the raw database error; Rust returns `sqlx`'s display
/// text (only reachable when the query itself fails, which the differential
/// harness does not exercise).
pub async fn count_by_state(pool: &PgPool, view: &str, states: u32) -> Result<i64, String> {
    let names = state_list(states);
    if names.is_empty() {
        return Err(ERR_AT_LEAST_ONE_STATE.to_owned());
    }

    let in_list = names
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(",");
    let query = COUNT_BY_STATE_QUERY
        .replacen("%s", view, 1)
        .replacen("%s", &in_list, 1);

    sqlx::query_scalar::<_, i64>(&query)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_list_matches_go_order() {
        assert_eq!(state_list(0), Vec::<&str>::new());
        assert_eq!(state_list(STATE_QUEUED), vec!["queued"]);
        assert_eq!(state_list(STATE_ERRORED), vec!["errored"]);
        assert_eq!(state_list(STATE_PROCESSING), vec!["processing"]);
        assert_eq!(
            state_list(QUEUED_ERRORED_PROCESSING),
            vec!["queued", "errored", "processing"]
        );
        assert_eq!(
            state_list(STATE_ERRORED | STATE_PROCESSING),
            vec!["errored", "processing"]
        );
        // Unknown bits are ignored.
        assert_eq!(state_list(1 << 3 | STATE_QUEUED), vec!["queued"]);
    }

    #[test]
    fn query_renders_like_the_baseline() {
        let names = state_list(QUEUED_ERRORED_PROCESSING);
        let in_list = names
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(",");
        let query = COUNT_BY_STATE_QUERY
            .replacen("%s", LSIF_INDEXES_VIEW, 1)
            .replacen("%s", &in_list, 1);
        assert_eq!(
            query,
            "SELECT COUNT(*)\nFROM lsif_indexes_with_repository_name u\nWHERE state IN ('queued','errored','processing')\n"
        );
    }

    #[tokio::test]
    async fn empty_bitset_is_a_precondition_failure() {
        // The pool is never touched: the precondition is checked first.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        let err = count_by_state(&pool, LSIF_INDEXES_VIEW, 0)
            .await
            .expect_err("empty bitset");
        assert_eq!(err, ERR_AT_LEAST_ONE_STATE);
    }
}
