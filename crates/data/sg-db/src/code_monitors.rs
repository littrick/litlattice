#![forbid(unsafe_code)]

//! Code monitor store operations used by the `codemonitors-job` worker job
//! (`internal/database/code_monitor_trigger_jobs.go`):
//!
//! * `EnqueueQueryTriggerJobs` — the `due`/`busy` CTE that inserts a
//!   `cm_trigger_jobs` row for every enabled monitor query whose
//!   `next_run` is due (or unset) and that has no queued/processing job;
//! * `DeleteOldTriggerJobs` — deletes trigger jobs whose `finished_at` is
//!   older than the retention window; `cm_action_jobs` rows cascade.
//!
//! The `logs` (`json[]`) column is never read: freshly enqueued rows start
//! with `NULL` and the baseline's `ScanTriggerJob` only appends what it
//! scans. `search_results` is returned as JSON so callers can inspect it.
//!
//! Migrations stay owned by the Go `migrator`; every SQL string below is
//! copied from the baseline store.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `database.TriggerJob` minus `Logs` (see the module note).
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerJob {
    pub id: i32,
    pub query: i64,
    /// The query we ran including the `after:` filter.
    pub query_string: Option<String>,
    pub search_results: Option<serde_json::Value>,
    pub state: Option<String>,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
}

/// `database.TriggerJobsColumns` without `cm_trigger_jobs.logs`.
pub const TRIGGER_JOBS_COLUMNS: &str = "cm_trigger_jobs.id, cm_trigger_jobs.query, \
     cm_trigger_jobs.query_string, cm_trigger_jobs.search_results, \
     cm_trigger_jobs.state, cm_trigger_jobs.failure_message, \
     cm_trigger_jobs.started_at, cm_trigger_jobs.finished_at, \
     cm_trigger_jobs.process_after, cm_trigger_jobs.num_resets, \
     cm_trigger_jobs.num_failures";

/// `enqueueTriggerQueryFmtStr`, with the column list inlined.
pub const ENQUEUE_QUERY_TRIGGER_JOBS_SQL: &str = "
WITH due AS (
    SELECT cm_queries.id as id
    FROM cm_queries
    JOIN cm_monitors ON cm_queries.monitor = cm_monitors.id
    JOIN users ON cm_monitors.namespace_user_id = users.id
    WHERE (cm_queries.next_run <= clock_timestamp() OR cm_queries.next_run IS NULL)
        AND cm_monitors.enabled = true
        AND users.deleted_at IS NULL
),
busy AS (
    SELECT DISTINCT query as id FROM cm_trigger_jobs
    WHERE state = 'queued'
    OR state = 'processing'
)
INSERT INTO cm_trigger_jobs (query)
SELECT id from due EXCEPT SELECT id from busy ORDER BY id
RETURNING cm_trigger_jobs.id, cm_trigger_jobs.query, cm_trigger_jobs.query_string, \
     cm_trigger_jobs.search_results, cm_trigger_jobs.state, cm_trigger_jobs.failure_message, \
     cm_trigger_jobs.started_at, cm_trigger_jobs.finished_at, cm_trigger_jobs.process_after, \
     cm_trigger_jobs.num_resets, cm_trigger_jobs.num_failures
";

/// `deleteOldJobLogsFmtStr`.
pub const DELETE_OLD_TRIGGER_JOBS_SQL: &str =
    "DELETE FROM cm_trigger_jobs\nWHERE finished_at < (NOW() - ($1 * '1 day'::interval));";

/// `codeMonitorStore.EnqueueQueryTriggerJobs`.
pub async fn enqueue_query_trigger_jobs(pool: &PgPool) -> Result<Vec<TriggerJob>, sqlx::Error> {
    let rows = sqlx::query(ENQUEUE_QUERY_TRIGGER_JOBS_SQL)
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(TriggerJob {
                id: row.try_get("id")?,
                query: row.try_get("query")?,
                query_string: row.try_get("query_string")?,
                search_results: row.try_get("search_results")?,
                state: row.try_get("state")?,
                failure_message: row.try_get("failure_message")?,
                started_at: row.try_get("started_at")?,
                finished_at: row.try_get("finished_at")?,
                process_after: row.try_get("process_after")?,
                num_resets: row.try_get("num_resets")?,
                num_failures: row.try_get("num_failures")?,
            })
        })
        .collect()
}

/// `codeMonitorStore.DeleteOldTriggerJobs`.
pub async fn delete_old_trigger_jobs(
    pool: &PgPool,
    retention_in_days: i32,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(DELETE_OLD_TRIGGER_JOBS_SQL)
        .bind(retention_in_days)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_sql_matches_baseline_shape() {
        assert!(ENQUEUE_QUERY_TRIGGER_JOBS_SQL.contains("clock_timestamp()"));
        assert!(ENQUEUE_QUERY_TRIGGER_JOBS_SQL.contains("users.deleted_at IS NULL"));
        assert!(ENQUEUE_QUERY_TRIGGER_JOBS_SQL.contains("ORDER BY id"));
        assert!(ENQUEUE_QUERY_TRIGGER_JOBS_SQL.contains("RETURNING cm_trigger_jobs.id"));
        assert!(!ENQUEUE_QUERY_TRIGGER_JOBS_SQL.contains("cm_trigger_jobs.logs"));
    }

    #[test]
    fn delete_sql_uses_day_interval() {
        assert_eq!(
            DELETE_OLD_TRIGGER_JOBS_SQL,
            "DELETE FROM cm_trigger_jobs\nWHERE finished_at < (NOW() - ($1 * '1 day'::interval));"
        );
    }

    #[test]
    fn trigger_job_columns_skip_logs() {
        assert!(!TRIGGER_JOBS_COLUMNS.contains("logs"));
        assert!(TRIGGER_JOBS_COLUMNS.contains("cm_trigger_jobs.num_failures"));
    }
}
