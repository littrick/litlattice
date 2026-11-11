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
//! * `EnqueueActionJobsForMonitor` — the `due_emails`/`due_webhooks`/
//!   `due_slack_webhooks` CTE that creates an `cm_action_jobs` row per
//!   enabled action that has no queued/processing job (`UNION ... ORDER BY
//!   1, 2, 3`);
//! * `GetActionJobMetadata` — the description/query/monitorID/results/
//!   owner-name join an action handler needs before sending.
//!
//! The `logs` (`json[]`) and `execution_logs` (`json[]`) columns are never
//! read: freshly enqueued rows start with `NULL` and the baseline's
//! `ScanTriggerJob` only appends what it scans. `search_results` is returned
//! as JSON so callers can inspect it.
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

/// `database.ActionJob`. `execution_logs` (`json[]`) is omitted for the same
/// reason as `TriggerJob::logs`.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionJob {
    pub id: i32,
    pub email: Option<i64>,
    pub webhook: Option<i64>,
    pub slack_webhook: Option<i64>,
    pub trigger_event: i32,
    pub state: Option<String>,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub log_contents: Option<String>,
}

/// `database.ActionJobColumns`.
pub const ACTION_JOB_COLUMNS: &str = "cm_action_jobs.id, cm_action_jobs.email, \
     cm_action_jobs.webhook, cm_action_jobs.slack_webhook, \
     cm_action_jobs.trigger_event, cm_action_jobs.state, \
     cm_action_jobs.failure_message, cm_action_jobs.started_at, \
     cm_action_jobs.finished_at, cm_action_jobs.process_after, \
     cm_action_jobs.num_resets, cm_action_jobs.num_failures, \
     cm_action_jobs.log_contents";

/// `enqueueActionEmailFmtStr`, with the three monitor-id parameters bound as
/// `$1` and the three trigger-job parameters as `$2`.
pub const ENQUEUE_ACTION_JOBS_SQL: &str = "
WITH due_emails AS (
	SELECT id
	FROM cm_emails
	WHERE monitor = $1
		AND enabled = true
	EXCEPT
	SELECT DISTINCT email as id FROM cm_action_jobs
	WHERE state = 'queued'
		OR state = 'processing'
), due_webhooks AS (
	SELECT id
	FROM cm_webhooks
	WHERE monitor = $1
		AND enabled = true
	EXCEPT
	SELECT DISTINCT webhook as id FROM cm_action_jobs
	WHERE state = 'queued'
		OR state = 'processing'
), due_slack_webhooks AS (
	SELECT id
	FROM cm_slack_webhooks
	WHERE monitor = $1
		AND enabled = true
	EXCEPT
	SELECT DISTINCT slack_webhook as id FROM cm_action_jobs
	WHERE state = 'queued'
		OR state = 'processing'
)
INSERT INTO cm_action_jobs (email, webhook, slack_webhook, trigger_event)
SELECT id, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), $2::integer from due_emails
UNION
SELECT CAST(NULL AS BIGINT), id, CAST(NULL AS BIGINT), $2::integer from due_webhooks
UNION
SELECT CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), id, $2::integer from due_slack_webhooks
ORDER BY 1, 2, 3
RETURNING cm_action_jobs.id, cm_action_jobs.email, cm_action_jobs.webhook, \
     cm_action_jobs.slack_webhook, cm_action_jobs.trigger_event, \
     cm_action_jobs.state, cm_action_jobs.failure_message, \
     cm_action_jobs.started_at, cm_action_jobs.finished_at, \
     cm_action_jobs.process_after, cm_action_jobs.num_resets, \
     cm_action_jobs.num_failures, cm_action_jobs.log_contents
";

/// `getActionJobMetadataFmtStr`.
pub const GET_ACTION_JOB_METADATA_SQL: &str = "
SELECT
	cm.description,
	ctj.query_string,
	cm.id AS monitorID,
	ctj.search_results,
	CASE WHEN LENGTH(users.display_name) > 0 THEN users.display_name ELSE users.username END
FROM cm_action_jobs caj
INNER JOIN cm_trigger_jobs ctj on caj.trigger_event = ctj.id
INNER JOIN cm_queries cq on cq.id = ctj.query
INNER JOIN cm_monitors cm on cm.id = cq.monitor
INNER JOIN users on cm.namespace_user_id = users.id
WHERE caj.id = $1
";

fn scan_action_job(row: sqlx::postgres::PgRow) -> Result<ActionJob, sqlx::Error> {
    Ok(ActionJob {
        id: row.try_get("id")?,
        email: row.try_get("email")?,
        webhook: row.try_get("webhook")?,
        slack_webhook: row.try_get("slack_webhook")?,
        trigger_event: row.try_get("trigger_event")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        process_after: row.try_get("process_after")?,
        num_resets: row.try_get("num_resets")?,
        num_failures: row.try_get("num_failures")?,
        log_contents: row.try_get("log_contents")?,
    })
}

/// `codeMonitorStore.EnqueueActionJobsForMonitor`.
pub async fn enqueue_action_jobs_for_monitor(
    pool: &PgPool,
    monitor_id: i64,
    trigger_job_id: i32,
) -> Result<Vec<ActionJob>, sqlx::Error> {
    let rows = sqlx::query(ENQUEUE_ACTION_JOBS_SQL)
        .bind(monitor_id)
        .bind(trigger_job_id)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(scan_action_job).collect()
}

/// `database.ActionJobMetadata`.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionJobMetadata {
    pub description: String,
    /// The query with the `after:` filter.
    pub query: Option<String>,
    pub monitor_id: i64,
    pub results: serde_json::Value,
    pub owner_name: String,
}

/// `codeMonitorStore.GetActionJobMetadata`.
pub async fn get_action_job_metadata(
    pool: &PgPool,
    job_id: i32,
) -> Result<ActionJobMetadata, sqlx::Error> {
    let row = sqlx::query(GET_ACTION_JOB_METADATA_SQL)
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    // The baseline scans by position; the `monitorID` alias and the CASE
    // expression have no stable lowercase names.
    // The baseline scans `search_results` into `[]byte`, which accepts NULL;
    // `json.Unmarshal(nil, …)` then reports "unexpected end of JSON input".
    Ok(ActionJobMetadata {
        description: row.try_get(0)?,
        query: row.try_get(1)?,
        monitor_id: row.try_get(2)?,
        results: row
            .try_get::<Option<serde_json::Value>, _>(3)?
            .unwrap_or(serde_json::Value::Null),
        owner_name: row.try_get(4)?,
    })
}

/// `database.EmailAction` subset used by the action runner.
#[derive(Debug, Clone, PartialEq)]
pub struct EmailAction {
    pub id: i64,
    pub monitor: i64,
    pub priority: String,
    pub include_results: bool,
}

/// `emailsColumns` subset: `codeMonitorStore.GetEmailAction`. `priority` is
/// the `cm_email_priority` enum; it is cast to text so the scan yields the
/// same string the baseline's `database/sql` scan produces.
pub const GET_EMAIL_ACTION_SQL: &str = "SELECT cm_emails.id, cm_emails.monitor, \
     cm_emails.priority::text AS priority, cm_emails.include_results FROM cm_emails \
     WHERE cm_emails.id = $1";

/// `codeMonitorStore.GetEmailAction`.
pub async fn get_email_action(pool: &PgPool, email_id: i64) -> Result<EmailAction, sqlx::Error> {
    let row = sqlx::query(GET_EMAIL_ACTION_SQL)
        .bind(email_id)
        .fetch_one(pool)
        .await?;
    Ok(EmailAction {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        priority: row.try_get("priority")?,
        include_results: row.try_get("include_results")?,
    })
}

/// `database.WebhookAction` subset used by the action runner.
#[derive(Debug, Clone, PartialEq)]
pub struct WebhookAction {
    pub id: i64,
    pub monitor: i64,
    pub url: String,
    pub include_results: bool,
}

/// `getWebhookActionQuery` with `webhookActionColumns` narrowed to the fields
/// `handleWebhook` reads.
pub const GET_WEBHOOK_ACTION_SQL: &str = "SELECT cm_webhooks.id, cm_webhooks.monitor, \
     cm_webhooks.url, cm_webhooks.include_results FROM cm_webhooks WHERE cm_webhooks.id = $1";

/// `codeMonitorStore.GetWebhookAction`.
pub async fn get_webhook_action(
    pool: &PgPool,
    webhook_id: i64,
) -> Result<WebhookAction, sqlx::Error> {
    let row = sqlx::query(GET_WEBHOOK_ACTION_SQL)
        .bind(webhook_id)
        .fetch_one(pool)
        .await?;
    Ok(WebhookAction {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        url: row.try_get("url")?,
        include_results: row.try_get("include_results")?,
    })
}

/// `database.SlackWebhookAction` subset used by the action runner.
#[derive(Debug, Clone, PartialEq)]
pub struct SlackWebhookAction {
    pub id: i64,
    pub monitor: i64,
    pub url: String,
    pub include_results: bool,
}

/// `getSlackWebhookActionQuery` narrowed to the fields `handleSlackWebhook`
/// reads.
pub const GET_SLACK_WEBHOOK_ACTION_SQL: &str = "SELECT cm_slack_webhooks.id, \
     cm_slack_webhooks.monitor, cm_slack_webhooks.url, \
     cm_slack_webhooks.include_results FROM cm_slack_webhooks \
     WHERE cm_slack_webhooks.id = $1";

/// `codeMonitorStore.GetSlackWebhookAction`.
pub async fn get_slack_webhook_action(
    pool: &PgPool,
    slack_webhook_id: i64,
) -> Result<SlackWebhookAction, sqlx::Error> {
    let row = sqlx::query(GET_SLACK_WEBHOOK_ACTION_SQL)
        .bind(slack_webhook_id)
        .fetch_one(pool)
        .await?;
    Ok(SlackWebhookAction {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        url: row.try_get("url")?,
        include_results: row.try_get("include_results")?,
    })
}

/// `database.Recipient`.
#[derive(Debug, Clone, PartialEq)]
pub struct Recipient {
    pub id: i64,
    pub email: i64,
    pub namespace_user_id: Option<i32>,
    pub namespace_org_id: Option<i32>,
}

/// `recipientColumns`.
pub const RECIPIENT_COLUMNS: &str = "cm_recipients.id, cm_recipients.email, \
     cm_recipients.namespace_user_id, cm_recipients.namespace_org_id";

/// `readRecipientQueryFmtStr` with `ListRecipientsOpts{EmailID}`.
pub const LIST_RECIPIENTS_SQL: &str = "SELECT cm_recipients.id, cm_recipients.email, \
     cm_recipients.namespace_user_id, cm_recipients.namespace_org_id FROM cm_recipients \
     WHERE email = $1 ORDER BY id ASC LIMIT ALL";

/// `codeMonitorStore.ListRecipients` for one e-mail action (no pagination).
pub async fn list_recipients(pool: &PgPool, email_id: i64) -> Result<Vec<Recipient>, sqlx::Error> {
    let rows = sqlx::query(LIST_RECIPIENTS_SQL)
        .bind(email_id)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(Recipient {
                id: row.try_get("id")?,
                email: row.try_get("email")?,
                namespace_user_id: row.try_get("namespace_user_id")?,
                namespace_org_id: row.try_get("namespace_org_id")?,
            })
        })
        .collect()
}

/// `database.QueryTrigger` subset read by `queryRunner.Handle`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryTrigger {
    pub id: i64,
    pub monitor: i64,
    pub query_string: String,
    pub latest_result: Option<OffsetDateTime>,
}

/// `queryColumns` narrowed to the fields the trigger handler reads.
pub const GET_QUERY_TRIGGER_FOR_JOB_SQL: &str = "SELECT cm_queries.id, cm_queries.monitor, \
     cm_queries.query, cm_queries.latest_result FROM cm_queries \
     INNER JOIN cm_trigger_jobs j ON cm_queries.id = j.query WHERE j.id = $1";

/// `codeMonitorStore.GetQueryTriggerForJob`.
pub async fn get_query_trigger_for_job(
    pool: &PgPool,
    trigger_job: i32,
) -> Result<QueryTrigger, sqlx::Error> {
    let row = sqlx::query(GET_QUERY_TRIGGER_FOR_JOB_SQL)
        .bind(trigger_job)
        .fetch_one(pool)
        .await?;
    Ok(QueryTrigger {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        query_string: row.try_get("query")?,
        latest_result: row.try_get("latest_result")?,
    })
}

/// `database.Monitor` subset needed by `queryRunner.Handle` (`GetMonitor`
/// returns the full row; only the id is used afterwards).
#[derive(Debug, Clone, PartialEq)]
pub struct Monitor {
    pub id: i64,
}

/// `monitorByIDFmtStr` narrowed to the id.
pub const GET_MONITOR_SQL: &str =
    "SELECT cm_monitors.id FROM cm_monitors WHERE cm_monitors.id = $1";

/// `codeMonitorStore.GetMonitor`.
pub async fn get_monitor(pool: &PgPool, monitor_id: i64) -> Result<Monitor, sqlx::Error> {
    let row = sqlx::query(GET_MONITOR_SQL)
        .bind(monitor_id)
        .fetch_one(pool)
        .await?;
    Ok(Monitor {
        id: row.try_get("id")?,
    })
}

/// `setTriggerQueryNextRunFmtStr`.
pub const SET_QUERY_TRIGGER_NEXT_RUN_SQL: &str =
    "UPDATE cm_queries SET next_run = $1, latest_result = $2 WHERE id = $3";

/// `codeMonitorStore.SetQueryTriggerNextRun`.
pub async fn set_query_trigger_next_run(
    pool: &PgPool,
    trigger_query_id: i64,
    next: OffsetDateTime,
    latest_result: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    sqlx::query(SET_QUERY_TRIGGER_NEXT_RUN_SQL)
        .bind(next)
        .bind(latest_result)
        .bind(trigger_query_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `UpsertLastSearched` (`internal/database/code_monitor_last_searched.go`).
pub const UPSERT_LAST_SEARCHED_SQL: &str = "
INSERT INTO cm_last_searched (monitor_id, repo_id, commit_oids)
VALUES ($1, $2, $3)
ON CONFLICT (monitor_id, repo_id) DO UPDATE
SET commit_oids = $4
";

/// `codeMonitorStore.UpsertLastSearched`. A nil slice is stored as `[]`
/// (baseline "appease non-null constraint" behavior).
pub async fn upsert_last_searched(
    pool: &PgPool,
    monitor_id: i64,
    repo_id: i32,
    commit_oids: &[String],
) -> Result<(), sqlx::Error> {
    let commit_oids = commit_oids.to_vec();
    sqlx::query(UPSERT_LAST_SEARCHED_SQL)
        .bind(monitor_id)
        .bind(repo_id)
        .bind(commit_oids.clone())
        .bind(commit_oids)
        .execute(pool)
        .await?;
    Ok(())
}

/// `GetLastSearched` (`internal/database/code_monitor_last_searched.go`).
pub const GET_LAST_SEARCHED_SQL: &str = "
SELECT commit_oids
FROM cm_last_searched
WHERE monitor_id = $1
	AND repo_id = $2
LIMIT 1
";

/// `codeMonitorStore.GetLastSearched`. `sql.ErrNoRows` maps to `None`
/// (baseline returns `nil, nil`).
pub async fn get_last_searched(
    pool: &PgPool,
    monitor_id: i64,
    repo_id: i32,
) -> Result<Option<Vec<String>>, sqlx::Error> {
    let row: Option<(Vec<String>,)> = sqlx::query_as(GET_LAST_SEARCHED_SQL)
        .bind(monitor_id)
        .bind(repo_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| row.0))
}

/// `updateTriggerJobLogsFmtStr`
/// (`internal/database/code_monitor_trigger_jobs.go:113-117`).
pub const UPDATE_TRIGGER_JOB_WITH_LOGS_SQL: &str = "
UPDATE cm_trigger_jobs
SET logs = logs || $1::json
WHERE id = $2
";

/// `codeMonitorStore.UpdateTriggerJobWithLogs`; `entry` is the
/// `TriggerJobLogs` JSON object.
pub async fn update_trigger_job_with_logs(
    pool: &PgPool,
    trigger_job_id: i32,
    entry: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(UPDATE_TRIGGER_JOB_WITH_LOGS_SQL)
        .bind(entry)
        .bind(trigger_job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `logSearchFmtStr`.
pub const UPDATE_TRIGGER_JOB_WITH_RESULTS_SQL: &str =
    "UPDATE cm_trigger_jobs SET query_string = $1, search_results = $2::jsonb WHERE id = $3";

/// `codeMonitorStore.UpdateTriggerJobWithResults`. The baseline marshals a
/// nil slice as `[]`; callers therefore pass a JSON array.
pub async fn update_trigger_job_with_results(
    pool: &PgPool,
    trigger_job_id: i32,
    query_string: &str,
    results: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(UPDATE_TRIGGER_JOB_WITH_RESULTS_SQL)
        .bind(query_string)
        .bind(results)
        .bind(trigger_job_id)
        .execute(pool)
        .await?;
    Ok(())
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
    fn enqueue_action_sql_matches_baseline_shape() {
        assert!(ENQUEUE_ACTION_JOBS_SQL.contains("WITH due_emails AS"));
        assert!(ENQUEUE_ACTION_JOBS_SQL.contains("due_slack_webhooks AS"));
        assert!(ENQUEUE_ACTION_JOBS_SQL.contains("EXCEPT"));
        assert!(ENQUEUE_ACTION_JOBS_SQL.contains("ORDER BY 1, 2, 3"));
        assert!(ENQUEUE_ACTION_JOBS_SQL.contains("$2::integer"));
        assert!(!ENQUEUE_ACTION_JOBS_SQL.contains("execution_logs"));
    }

    #[test]
    fn action_job_columns_match_scan_order() {
        assert!(ACTION_JOB_COLUMNS.contains("cm_action_jobs.log_contents"));
        assert!(!ACTION_JOB_COLUMNS.contains("execution_logs"));
        assert!(ACTION_JOB_COLUMNS.contains("cm_action_jobs.trigger_event"));
    }

    #[test]
    fn trigger_job_columns_skip_logs() {
        assert!(!TRIGGER_JOBS_COLUMNS.contains("logs"));
        assert!(TRIGGER_JOBS_COLUMNS.contains("cm_trigger_jobs.num_failures"));
    }
}
