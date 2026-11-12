#![forbid(unsafe_code)]

//! Production Postgres adapter for the code-monitor GraphQL seam (M9.13a).
//!
//! Implements [`crate::code_monitors_graphql::CodeMonitorsGraphqlDatabase`]
//! by porting the SQL in `internal/database/code_monitor_*.go`. Only existing
//! tables are touched: `cm_monitors`, `cm_queries`, `cm_emails`,
//! `cm_webhooks`, `cm_slack_webhooks`, `cm_recipients`, `cm_trigger_jobs`,
//! `cm_action_jobs`, `cm_last_searched`, `users`, `orgs` and `org_members`.
//!
//! The Go store is synchronous while `sqlx` is async, so the adapter bridges
//! with the same `block_in_place`/`block_on` helper the search-contexts
//! adapter uses. The baseline's transaction (`WithTransact`) is not
//! reproduced: every write runs as its own statement. This is documented as
//! an M9.13a deferral; a failed later write leaves the earlier ones applied.
//!
//! The `namespaceScopeQuery(user)` predicate on `UpdateQueryTrigger` and the
//! action updates is rendered as `namespace_user_id = <actor uid>`: code
//! monitors only accept user namespaces (org namespaces are rejected by
//! `isAllowedToCreate`), so the org half of the baseline predicate is
//! unreachable.

use std::future::Future;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::code_monitors_graphql::{
    ActionJobFilter, ActionJobRow, CodeMonitorsGraphqlDatabase, EmailActionRow, MonitorRow,
    QueryTriggerRow, RecipientRow, SlackWebhookActionRow, TriggerJobRow, WebhookActionRow,
};
use crate::graphql_resolvers::{OrgData, UserData};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("code monitors blocking runtime")
            .block_on(future),
    }
}

/// `monitorColumns` (`code_monitor_monitors.go:26-34`).
const MONITOR_COLUMNS: &str = "cm_monitors.id, cm_monitors.created_by, \
     cm_monitors.created_at, cm_monitors.changed_by, cm_monitors.changed_at, \
     cm_monitors.description, cm_monitors.enabled, cm_monitors.namespace_user_id";

/// `queryColumns` minus the audit columns (`code_monitor_queries.go:26-35`).
const QUERY_COLUMNS: &str = "cm_queries.id, cm_queries.monitor, cm_queries.query, \
     cm_queries.next_run, cm_queries.latest_result";

/// `emailsColumns` minus the audit columns; `priority` is cast to text for the
/// same reason as `sg_db::code_monitors::GET_EMAIL_ACTION_SQL`.
const EMAIL_COLUMNS: &str = "cm_emails.id, cm_emails.monitor, cm_emails.enabled, \
     cm_emails.priority::text AS priority, cm_emails.header, cm_emails.include_results";

/// `webhookActionColumns` minus the audit columns.
const WEBHOOK_COLUMNS: &str = "cm_webhooks.id, cm_webhooks.monitor, cm_webhooks.enabled, \
     cm_webhooks.url, cm_webhooks.include_results";

/// `slackWebhookActionColumns` minus the audit columns.
const SLACK_COLUMNS: &str = "cm_slack_webhooks.id, cm_slack_webhooks.monitor, \
     cm_slack_webhooks.enabled, cm_slack_webhooks.url, cm_slack_webhooks.include_results";

/// `recipientColumns`.
const RECIPIENT_COLUMNS: &str = "cm_recipients.id, cm_recipients.email, \
     cm_recipients.namespace_user_id, cm_recipients.namespace_org_id";

/// `TriggerJobsColumns`; `logs` is converted from `json[]` to JSON so it can
/// be scanned as `serde_json::Value`.
const TRIGGER_JOB_COLUMNS: &str = "cm_trigger_jobs.id, cm_trigger_jobs.query, \
     cm_trigger_jobs.query_string, cm_trigger_jobs.search_results, \
     cm_trigger_jobs.state, cm_trigger_jobs.failure_message, \
     cm_trigger_jobs.started_at, cm_trigger_jobs.finished_at, \
     cm_trigger_jobs.process_after, cm_trigger_jobs.num_resets, \
     cm_trigger_jobs.num_failures, to_jsonb(cm_trigger_jobs.logs) AS logs";

/// `ActionJobColumns`.
const ACTION_JOB_COLUMNS: &str = "cm_action_jobs.id, cm_action_jobs.email, \
     cm_action_jobs.webhook, cm_action_jobs.slack_webhook, \
     cm_action_jobs.trigger_event, cm_action_jobs.state, \
     cm_action_jobs.failure_message, cm_action_jobs.started_at, \
     cm_action_jobs.finished_at, cm_action_jobs.process_after, \
     cm_action_jobs.num_resets, cm_action_jobs.num_failures, \
     cm_action_jobs.log_contents";

pub struct PgCodeMonitorsGraphqlStore {
    pool: PgPool,
}

impl PgCodeMonitorsGraphqlStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn scan_monitor(row: &PgRow) -> Result<MonitorRow, sqlx::Error> {
    Ok(MonitorRow {
        id: row.try_get("id")?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        changed_by: row.try_get("changed_by")?,
        changed_at: row.try_get("changed_at")?,
        description: row.try_get("description")?,
        enabled: row.try_get("enabled")?,
        user_id: row.try_get("namespace_user_id")?,
    })
}

fn scan_query_trigger(row: &PgRow) -> Result<QueryTriggerRow, sqlx::Error> {
    Ok(QueryTriggerRow {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        query_string: row.try_get("query")?,
        next_run: row.try_get("next_run")?,
        latest_result: row.try_get("latest_result")?,
    })
}

fn scan_email(row: &PgRow) -> Result<EmailActionRow, sqlx::Error> {
    Ok(EmailActionRow {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        enabled: row.try_get("enabled")?,
        priority: row.try_get("priority")?,
        header: row.try_get("header")?,
        include_results: row.try_get("include_results")?,
    })
}

fn scan_webhook(row: &PgRow) -> Result<WebhookActionRow, sqlx::Error> {
    Ok(WebhookActionRow {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        enabled: row.try_get("enabled")?,
        url: row.try_get("url")?,
        include_results: row.try_get("include_results")?,
    })
}

fn scan_slack(row: &PgRow) -> Result<SlackWebhookActionRow, sqlx::Error> {
    Ok(SlackWebhookActionRow {
        id: row.try_get("id")?,
        monitor: row.try_get("monitor")?,
        enabled: row.try_get("enabled")?,
        url: row.try_get("url")?,
        include_results: row.try_get("include_results")?,
    })
}

fn scan_recipient(row: &PgRow) -> Result<RecipientRow, sqlx::Error> {
    Ok(RecipientRow {
        id: row.try_get("id")?,
        email: row.try_get("email")?,
        namespace_user_id: row.try_get("namespace_user_id")?,
        namespace_org_id: row.try_get("namespace_org_id")?,
    })
}

fn scan_trigger_job(row: &PgRow) -> Result<TriggerJobRow, sqlx::Error> {
    Ok(TriggerJobRow {
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
        logs: row.try_get("logs")?,
    })
}

fn scan_action_job(row: &PgRow) -> Result<ActionJobRow, sqlx::Error> {
    Ok(ActionJobRow {
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

fn error(error: sqlx::Error) -> String {
    error.to_string()
}

impl CodeMonitorsGraphqlDatabase for PgCodeMonitorsGraphqlStore {
    fn list_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        first: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<Vec<MonitorRow>, String> {
        let mut sql = format!(
            "SELECT {MONITOR_COLUMNS} FROM cm_monitors JOIN users \
             ON cm_monitors.created_by = users.id WHERE TRUE"
        );
        let mut index = 0;
        let mut binds: Vec<Bind> = Vec::new();
        if let Some(user_id) = user_id {
            index += 1;
            sql.push_str(&format!(" AND cm_monitors.namespace_user_id = ${index}"));
            binds.push(Bind::I32(user_id));
        }
        if let Some(after) = after {
            index += 1;
            sql.push_str(&format!(" AND cm_monitors.id > ${index}"));
            binds.push(Bind::I64(after));
        }
        if skip_orphaned {
            sql.push_str(" AND users.deleted_at IS NULL");
        }
        sql.push_str(" ORDER BY id ASC LIMIT ");
        match first {
            Some(first) => {
                index += 1;
                sql.push_str(&format!("${index}"));
                binds.push(Bind::I64(first));
            }
            None => sql.push_str("ALL"),
        }
        let mut query = sqlx::query(&sql);
        for bind in &binds {
            query = bind.apply(query);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(error)?;
        rows.iter().map(scan_monitor).collect::<Result<_, _>>().map_err(error)
    }

    fn count_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<i32, String> {
        let mut sql = String::from(
            "SELECT COUNT(*) FROM cm_monitors JOIN users \
             ON cm_monitors.created_by = users.id WHERE TRUE",
        );
        let mut index = 0;
        let mut binds: Vec<Bind> = Vec::new();
        if let Some(user_id) = user_id {
            index += 1;
            sql.push_str(&format!(" AND cm_monitors.namespace_user_id = ${index}"));
            binds.push(Bind::I32(user_id));
        }
        if let Some(after) = after {
            index += 1;
            sql.push_str(&format!(" AND cm_monitors.id > ${index}"));
            binds.push(Bind::I64(after));
        }
        if skip_orphaned {
            sql.push_str(" AND users.deleted_at IS NULL");
        }
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for bind in &binds {
            query = bind.apply_scalar(query);
        }
        let count = block_on(query.fetch_one(&self.pool)).map_err(error)?;
        Ok(count as i32)
    }

    fn get_monitor(&self, id: i64) -> Result<Option<MonitorRow>, String> {
        let sql = format!("SELECT {MONITOR_COLUMNS} FROM cm_monitors WHERE id = $1");
        let row = block_on(
            sqlx::query(&sql)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_monitor).transpose().map_err(error)
    }

    fn create_monitor(
        &self,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<MonitorRow, String> {
        let sql = format!(
            "INSERT INTO cm_monitors \
             (created_at, created_by, changed_at, changed_by, description, enabled, \
              namespace_user_id, namespace_org_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {MONITOR_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(now)
                .bind(actor_uid)
                .bind(now)
                .bind(actor_uid)
                .bind(description)
                .bind(enabled)
                .bind(namespace_user_id)
                .bind(namespace_org_id)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_monitor(&row).map_err(error)
    }

    fn update_monitor(
        &self,
        id: i64,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String> {
        // Baseline keeps `AND namespace_user_id = <new namespace>`.
        let sql = format!(
            "UPDATE cm_monitors SET description = $1, enabled = $2, \
             namespace_user_id = $3, namespace_org_id = $4, changed_by = $5, \
             changed_at = $6 WHERE id = $7 AND namespace_user_id = $8 \
             RETURNING {MONITOR_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(description)
                .bind(enabled)
                .bind(namespace_user_id)
                .bind(namespace_org_id)
                .bind(actor_uid)
                .bind(now)
                .bind(id)
                .bind(namespace_user_id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_monitor).transpose().map_err(error)
    }

    fn update_monitor_enabled(
        &self,
        id: i64,
        enabled: bool,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String> {
        let sql = format!(
            "UPDATE cm_monitors SET enabled = $1, changed_by = $2, changed_at = $3 \
             WHERE id = $4 RETURNING {MONITOR_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(enabled)
                .bind(actor_uid)
                .bind(now)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_monitor).transpose().map_err(error)
    }

    fn delete_monitor(&self, id: i64) -> Result<(), String> {
        block_on(
            sqlx::query("DELETE FROM cm_monitors WHERE id = $1")
                .bind(id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn create_query_trigger(
        &self,
        monitor_id: i64,
        query: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<QueryTriggerRow, String> {
        let sql = format!(
            "INSERT INTO cm_queries \
             (monitor, query, created_by, created_at, changed_by, changed_at, \
              next_run, latest_result) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {QUERY_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .bind(query)
                .bind(actor_uid)
                .bind(now)
                .bind(actor_uid)
                .bind(now)
                .bind(now)
                .bind(now)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_query_trigger(&row).map_err(error)
    }

    fn update_query_trigger(
        &self,
        id: i64,
        query: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        // `updateTriggerQueryFmtStr` with `namespaceScopeQuery(user)`.
        let row = block_on(
            sqlx::query(
                "UPDATE cm_queries SET query = $1, changed_by = $2, changed_at = $3, \
                 latest_result = $4 WHERE id = $5 AND EXISTS \
                 (SELECT 1 FROM cm_monitors WHERE cm_monitors.id = cm_queries.monitor \
                  AND cm_monitors.namespace_user_id = $6) RETURNING id",
            )
            .bind(query)
            .bind(actor_uid)
            .bind(now)
            .bind(now)
            .bind(id)
            .bind(actor_uid)
            .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        match row {
            Some(_) => Ok(()),
            None => Err("sql: no rows in result set".to_owned()),
        }
    }

    fn get_query_trigger_for_monitor(
        &self,
        monitor_id: i64,
    ) -> Result<Option<QueryTriggerRow>, String> {
        let sql = format!("SELECT {QUERY_COLUMNS} FROM cm_queries WHERE monitor = $1");
        let row = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref()
            .map(scan_query_trigger)
            .transpose()
            .map_err(error)
    }

    fn reset_query_trigger_timestamps(
        &self,
        query_id: i64,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        block_on(
            sqlx::query(
                "UPDATE cm_queries SET latest_result = null, next_run = $1 WHERE id = $2",
            )
            .bind(now)
            .bind(query_id)
            .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn upsert_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
        commit_oids: &[String],
    ) -> Result<(), String> {
        let commit_oids = commit_oids.to_vec();
        block_on(
            sqlx::query(
                "INSERT INTO cm_last_searched (monitor_id, repo_id, commit_oids) \
                 VALUES ($1, $2, $3) ON CONFLICT (monitor_id, repo_id) \
                 DO UPDATE SET commit_oids = $4",
            )
            .bind(monitor_id)
            .bind(repo_id)
            .bind(commit_oids.clone())
            .bind(commit_oids)
            .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn list_email_actions(&self, monitor_id: i64) -> Result<Vec<EmailActionRow>, String> {
        let sql = format!(
            "SELECT {EMAIL_COLUMNS} FROM cm_emails WHERE monitor = $1 \
             ORDER BY id ASC LIMIT ALL"
        );
        let rows = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter().map(scan_email).collect::<Result<_, _>>().map_err(error)
    }

    fn list_webhook_actions(&self, monitor_id: i64) -> Result<Vec<WebhookActionRow>, String> {
        let sql = format!(
            "SELECT {WEBHOOK_COLUMNS} FROM cm_webhooks WHERE monitor = $1 \
             ORDER BY id ASC LIMIT ALL"
        );
        let rows = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter().map(scan_webhook).collect::<Result<_, _>>().map_err(error)
    }

    fn list_slack_webhook_actions(
        &self,
        monitor_id: i64,
    ) -> Result<Vec<SlackWebhookActionRow>, String> {
        let sql = format!(
            "SELECT {SLACK_COLUMNS} FROM cm_slack_webhooks WHERE monitor = $1 \
             ORDER BY id ASC LIMIT ALL"
        );
        let rows = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .fetch_all(&self.pool),
        )
        .map_err(error)?;
        rows.iter().map(scan_slack).collect::<Result<_, _>>().map_err(error)
    }

    fn create_email_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<EmailActionRow, String> {
        let sql = format!(
            "INSERT INTO cm_emails (monitor, enabled, include_results, priority, header, \
             created_by, created_at, changed_by, changed_at) \
             VALUES ($1, $2, $3, $4::cm_email_priority, $5, $6, $7, $8, $9) \
             RETURNING {EMAIL_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .bind(enabled)
                .bind(include_results)
                .bind(priority)
                .bind(header)
                .bind(actor_uid)
                .bind(now)
                .bind(actor_uid)
                .bind(now)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_email(&row).map_err(error)
    }

    fn create_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<WebhookActionRow, String> {
        let sql = format!(
            "INSERT INTO cm_webhooks (monitor, enabled, include_results, url, \
             created_by, created_at, changed_by, changed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {WEBHOOK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .bind(enabled)
                .bind(include_results)
                .bind(url)
                .bind(actor_uid)
                .bind(now)
                .bind(actor_uid)
                .bind(now)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_webhook(&row).map_err(error)
    }

    fn create_slack_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<SlackWebhookActionRow, String> {
        let sql = format!(
            "INSERT INTO cm_slack_webhooks (monitor, enabled, include_results, url, \
             created_by, created_at, changed_by, changed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {SLACK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(monitor_id)
                .bind(enabled)
                .bind(include_results)
                .bind(url)
                .bind(actor_uid)
                .bind(now)
                .bind(actor_uid)
                .bind(now)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_slack(&row).map_err(error)
    }

    fn delete_email_actions(&self, ids: &[i64], monitor_id: i64) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        block_on(
            sqlx::query("DELETE FROM cm_emails WHERE id = ANY($1) AND monitor = $2")
                .bind(ids.to_vec())
                .bind(monitor_id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn delete_webhook_actions(&self, monitor_id: i64, ids: &[i64]) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        block_on(
            sqlx::query("DELETE FROM cm_webhooks WHERE id = ANY($1) AND monitor = $2")
                .bind(ids.to_vec())
                .bind(monitor_id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn delete_slack_webhook_actions(&self, monitor_id: i64, ids: &[i64]) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        block_on(
            sqlx::query("DELETE FROM cm_slack_webhooks WHERE id = ANY($1) AND monitor = $2")
                .bind(ids.to_vec())
                .bind(monitor_id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn update_email_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<EmailActionRow>, String> {
        let sql = format!(
            "UPDATE cm_emails SET enabled = $1, include_results = $2, \
             priority = $3::cm_email_priority, header = $4, changed_by = $5, \
             changed_at = $6 WHERE id = $7 AND EXISTS (SELECT 1 FROM cm_monitors \
             WHERE cm_monitors.id = cm_emails.monitor \
             AND cm_monitors.namespace_user_id = $8) RETURNING {EMAIL_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(enabled)
                .bind(include_results)
                .bind(priority)
                .bind(header)
                .bind(actor_uid)
                .bind(now)
                .bind(id)
                .bind(actor_uid)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_email).transpose().map_err(error)
    }

    fn update_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<WebhookActionRow>, String> {
        let sql = format!(
            "UPDATE cm_webhooks SET enabled = $1, include_results = $2, url = $3, \
             changed_by = $4, changed_at = $5 WHERE id = $6 AND EXISTS \
             (SELECT 1 FROM cm_monitors WHERE cm_monitors.id = cm_webhooks.monitor \
              AND cm_monitors.namespace_user_id = $7) RETURNING {WEBHOOK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(enabled)
                .bind(include_results)
                .bind(url)
                .bind(actor_uid)
                .bind(now)
                .bind(id)
                .bind(actor_uid)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_webhook).transpose().map_err(error)
    }

    fn update_slack_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<SlackWebhookActionRow>, String> {
        let sql = format!(
            "UPDATE cm_slack_webhooks SET enabled = $1, include_results = $2, url = $3, \
             changed_by = $4, changed_at = $5 WHERE id = $6 AND EXISTS \
             (SELECT 1 FROM cm_monitors WHERE cm_monitors.id = cm_slack_webhooks.monitor \
              AND cm_monitors.namespace_user_id = $7) RETURNING {SLACK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(enabled)
                .bind(include_results)
                .bind(url)
                .bind(actor_uid)
                .bind(now)
                .bind(id)
                .bind(actor_uid)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref().map(scan_slack).transpose().map_err(error)
    }

    fn delete_recipients(&self, email_id: i64) -> Result<(), String> {
        block_on(
            sqlx::query("DELETE FROM cm_recipients WHERE email = $1")
                .bind(email_id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        Ok(())
    }

    fn create_recipient(
        &self,
        email_id: i64,
        user_id: Option<i32>,
        org_id: Option<i32>,
    ) -> Result<RecipientRow, String> {
        let sql = format!(
            "INSERT INTO cm_recipients (email, namespace_user_id, namespace_org_id) \
             VALUES ($1, $2, $3) RETURNING {RECIPIENT_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(email_id)
                .bind(user_id)
                .bind(org_id)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_recipient(&row).map_err(error)
    }

    fn list_recipients(
        &self,
        email_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<RecipientRow>, String> {
        let mut sql = format!(
            "SELECT {RECIPIENT_COLUMNS} FROM cm_recipients WHERE email = $1"
        );
        let mut index = 1;
        let mut binds = vec![Bind::I64(email_id)];
        if let Some(after) = after {
            index += 1;
            sql.push_str(&format!(" AND id > ${index}"));
            binds.push(Bind::I64(after));
        }
        sql.push_str(" ORDER BY id ASC LIMIT ");
        match first {
            Some(first) => {
                index += 1;
                sql.push_str(&format!("${index}"));
                binds.push(Bind::I64(first));
            }
            None => sql.push_str("ALL"),
        }
        let mut query = sqlx::query(&sql);
        for bind in &binds {
            query = bind.apply(query);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(error)?;
        rows.iter().map(scan_recipient).collect::<Result<_, _>>().map_err(error)
    }

    fn count_recipients(&self, email_id: i64) -> Result<i32, String> {
        let count = block_on(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM cm_recipients WHERE email = $1",
            )
            .bind(email_id)
            .fetch_one(&self.pool),
        )
        .map_err(error)?;
        Ok(count as i32)
    }

    fn list_trigger_jobs(
        &self,
        query_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<TriggerJobRow>, String> {
        let mut sql = format!(
            "SELECT {TRIGGER_JOB_COLUMNS} FROM cm_trigger_jobs WHERE query = $1"
        );
        let mut index = 1;
        let mut binds = vec![Bind::I64(query_id)];
        if let Some(after) = after {
            index += 1;
            sql.push_str(&format!(" AND id < ${index}"));
            binds.push(Bind::I64(after));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ");
        match first {
            Some(first) => {
                index += 1;
                sql.push_str(&format!("${index}"));
                binds.push(Bind::I64(first));
            }
            None => sql.push_str("ALL"),
        }
        let mut query = sqlx::query(&sql);
        for bind in &binds {
            query = bind.apply(query);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(error)?;
        rows.iter().map(scan_trigger_job).collect::<Result<_, _>>().map_err(error)
    }

    fn count_trigger_jobs(&self, query_id: i64) -> Result<i32, String> {
        let count = block_on(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM cm_trigger_jobs \
                 WHERE ((state = 'completed' AND jsonb_array_length(search_results) > 0) \
                 OR (state != 'completed')) AND query = $1",
            )
            .bind(query_id)
            .fetch_one(&self.pool),
        )
        .map_err(error)?;
        Ok(count as i32)
    }

    fn list_action_jobs(
        &self,
        filter: &ActionJobFilter,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<ActionJobRow>, String> {
        let (mut sql, binds) = action_job_filter_sql(filter, after, first, true);
        sql = format!("SELECT {ACTION_JOB_COLUMNS} {sql}");
        let mut query = sqlx::query(&sql);
        for bind in &binds {
            query = bind.apply(query);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(error)?;
        rows.iter().map(scan_action_job).collect::<Result<_, _>>().map_err(error)
    }

    fn count_action_jobs(&self, filter: &ActionJobFilter) -> Result<i32, String> {
        let (sql, binds) = action_job_filter_sql(filter, None, None, false);
        let sql = format!("SELECT COUNT(*) {sql}");
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for bind in &binds {
            query = bind.apply_scalar(query);
        }
        let count = block_on(query.fetch_one(&self.pool)).map_err(error)?;
        Ok(count as i32)
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let record = block_on(sg_db::users::get_session_user(&self.pool, id))
            .map_err(error)?;
        Ok(record.map(crate::user_store::to_user_data))
    }

    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String> {
        let record = block_on(sg_db::orgs::find_org_by_id(&self.pool, id))
            .map_err(error)?;
        Ok(record.map(|record| OrgData {
            id: record.id,
            name: record.name,
            display_name: record.display_name,
            created_at: record.created_at,
            viewer_can_administer: false,
            viewer_is_member: false,
        }))
    }

    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String> {
        self.user_by_id(uid)
    }

    fn is_site_admin(&self, uid: i32) -> Result<bool, String> {
        let record = block_on(sg_db::users::get_session_user(&self.pool, uid))
            .map_err(error)?;
        Ok(record.map(|record| record.site_admin).unwrap_or(false))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let orgs = block_on(sg_db::orgs::org_members_for_user(
            &self.pool,
            user_id,
            &[org_id],
        ))
        .map_err(error)?;
        Ok(orgs.contains(&org_id))
    }
}

/// Build the `FROM cm_action_jobs WHERE ...` fragment plus its binds.
fn action_job_filter_sql(
    filter: &ActionJobFilter,
    after: Option<i64>,
    first: Option<i64>,
    with_limit: bool,
) -> (String, Vec<Bind>) {
    let mut sql = String::from("FROM cm_action_jobs WHERE TRUE");
    let mut index = 0;
    let mut binds = Vec::new();
    if let Some(trigger_event_id) = filter.trigger_event_id {
        index += 1;
        sql.push_str(&format!(" AND trigger_event = ${index}"));
        binds.push(Bind::I32(trigger_event_id));
    }
    if let Some(email_id) = filter.email_id {
        index += 1;
        sql.push_str(&format!(" AND email = ${index}"));
        binds.push(Bind::I64(email_id));
    }
    if let Some(webhook_id) = filter.webhook_id {
        index += 1;
        sql.push_str(&format!(" AND webhook = ${index}"));
        binds.push(Bind::I64(webhook_id));
    }
    if let Some(slack_webhook_id) = filter.slack_webhook_id {
        index += 1;
        sql.push_str(&format!(" AND slack_webhook = ${index}"));
        binds.push(Bind::I64(slack_webhook_id));
    }
    if let Some(after) = after {
        index += 1;
        sql.push_str(&format!(" AND id > ${index}"));
        binds.push(Bind::I64(after));
    }
    if with_limit {
        sql.push_str(" ORDER BY id ASC LIMIT ");
        match first {
            Some(first) => {
                index += 1;
                sql.push_str(&format!("${index}"));
                binds.push(Bind::I64(first));
            }
            None => sql.push_str("ALL"),
        }
    }
    (sql, binds)
}

enum Bind {
    I32(i32),
    I64(i64),
}

impl Bind {
    fn apply<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        match self {
            Self::I32(value) => query.bind(*value),
            Self::I64(value) => query.bind(*value),
        }
    }

    fn apply_scalar<'q>(
        &self,
        query: sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
        match self {
            Self::I32(value) => query.bind(*value),
            Self::I64(value) => query.bind(*value),
        }
    }
}
