//! M9.13a differential of the code-monitor GraphQL surface against the Go
//! baseline (`cmd/frontend/internal/codemonitors/resolvers` + graph-gophers).
//!
//! Both sides execute the same query strings against an in-memory
//! `CodeMonitorStore` built from the corpus fixtures. The Rust side assembles
//! the vendored `code_monitors.graphql` on top of the main OSS schema and
//! executes through the production `FrontendRootResolver`.
//!
//! Env:
//!
//! * `SG_M9_13A_CM_DIFF=1`
//! * `M9_13A_CM_CORPUS` — corpus JSON path
//! * `M9_13A_CM_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Map;
use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, CODE_MONITORS_SCHEMA, MAIN_SCHEMA};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::code_monitors_graphql::{
    ActionJobFilter, ActionJobRow, CodeMonitorSnapshot, CodeMonitorsGraphqlDatabase,
    CodeMonitorsResolver, CodeMonitorTestSender, EmailActionRow, MonitorRow, QueryTriggerRow,
    RecipientRow, SlackWebhookActionRow, TriggerJobRow, WebhookActionRow,
};
use sg_service_frontend::graphql_resolvers::{FrontendRootResolver, OrgData, UserData, UserObject};

#[derive(Deserialize)]
struct Corpus {
    now: String,
    #[serde(default)]
    fixtures: Fixtures,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct Fixtures {
    #[serde(default)]
    users: Vec<UserFixture>,
    #[serde(default)]
    orgs: Vec<OrgFixture>,
    #[serde(default)]
    org_members: Vec<(i32, i32)>,
    #[serde(default)]
    monitors: Vec<MonitorFixture>,
    #[serde(default)]
    triggers: Vec<TriggerFixture>,
    #[serde(default)]
    emails: Vec<EmailFixture>,
    #[serde(default)]
    webhooks: Vec<WebhookFixture>,
    #[serde(default)]
    slacks: Vec<WebhookFixture>,
    #[serde(default)]
    recipients: Vec<RecipientFixture>,
    #[serde(default)]
    trigger_jobs: Vec<TriggerJobFixture>,
    #[serde(default)]
    action_jobs: Vec<ActionJobFixture>,
}

#[derive(Deserialize)]
struct UserFixture {
    id: i32,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default = "default_time")]
    updated_at: String,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    tos_accepted: bool,
    #[serde(default)]
    primary_email: String,
}

#[derive(Deserialize)]
struct OrgFixture {
    id: i32,
    name: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default = "default_time")]
    created_at: String,
}

#[derive(Deserialize)]
struct MonitorFixture {
    id: i64,
    created_by: i32,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default)]
    changed_by: i32,
    #[serde(default = "default_time")]
    changed_at: String,
    description: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    user_id: i32,
}

#[derive(Deserialize)]
struct TriggerFixture {
    id: i64,
    monitor: i64,
    query_string: String,
    #[serde(default = "default_time")]
    next_run: String,
    #[serde(default)]
    latest_result: Option<String>,
}

#[derive(Deserialize)]
struct EmailFixture {
    id: i64,
    monitor: i64,
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_priority")]
    priority: String,
    #[serde(default)]
    header: String,
    #[serde(default)]
    include_results: bool,
}

fn default_priority() -> String {
    "NORMAL".to_owned()
}

#[derive(Deserialize)]
struct WebhookFixture {
    id: i64,
    monitor: i64,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    url: String,
    #[serde(default)]
    include_results: bool,
}

#[derive(Deserialize)]
struct RecipientFixture {
    id: i64,
    email: i64,
    #[serde(default)]
    namespace_user_id: Option<i32>,
    #[serde(default)]
    namespace_org_id: Option<i32>,
}

#[derive(Deserialize)]
struct TriggerJobFixture {
    id: i32,
    query: i64,
    #[serde(default)]
    query_string: Option<String>,
    #[serde(default)]
    search_results: Option<serde_json::Value>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    failure_message: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    finished_at: Option<String>,
    #[serde(default)]
    process_after: Option<String>,
    #[serde(default)]
    num_resets: i32,
    #[serde(default)]
    num_failures: i32,
    #[serde(default)]
    logs: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ActionJobFixture {
    id: i32,
    #[serde(default)]
    email: Option<i64>,
    #[serde(default)]
    webhook: Option<i64>,
    #[serde(default)]
    slack_webhook: Option<i64>,
    #[serde(default)]
    trigger_event: i32,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    failure_message: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    finished_at: Option<String>,
    #[serde(default)]
    process_after: Option<String>,
    #[serde(default)]
    num_resets: i32,
    #[serde(default)]
    num_failures: i32,
    #[serde(default)]
    log_contents: Option<String>,
}

fn default_time() -> String {
    "2024-01-01T00:00:00Z".to_owned()
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    dotcom: bool,
    #[serde(default = "default_enabled")]
    code_monitors_enabled: bool,
}

fn default_enabled() -> bool {
    true
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn parse_opt_time(value: &Option<String>) -> Option<OffsetDateTime> {
    value.as_deref().map(parse_time)
}

fn user_data(fixture: &UserFixture) -> UserData {
    UserData {
        id: fixture.id,
        username: fixture.username.clone(),
        display_name: fixture.display_name.clone(),
        avatar_url: fixture.avatar_url.clone(),
        created_at: parse_time(&fixture.created_at),
        updated_at: parse_time(&fixture.updated_at),
        site_admin: fixture.site_admin,
        tos_accepted: fixture.tos_accepted,
        primary_email: fixture.primary_email.clone(),
    }
}

fn org_data(fixture: &OrgFixture) -> OrgData {
    OrgData {
        id: fixture.id,
        name: fixture.name.clone(),
        display_name: fixture.display_name.clone(),
        created_at: parse_time(&fixture.created_at),
        viewer_can_administer: false,
        viewer_is_member: false,
    }
}

fn monitor_row(fixture: &MonitorFixture) -> MonitorRow {
    MonitorRow {
        id: fixture.id,
        created_by: fixture.created_by,
        created_at: parse_time(&fixture.created_at),
        changed_by: fixture.changed_by,
        changed_at: parse_time(&fixture.changed_at),
        description: fixture.description.clone(),
        enabled: fixture.enabled,
        user_id: fixture.user_id,
    }
}

fn trigger_row(fixture: &TriggerFixture) -> QueryTriggerRow {
    QueryTriggerRow {
        id: fixture.id,
        monitor: fixture.monitor,
        query_string: fixture.query_string.clone(),
        next_run: parse_time(&fixture.next_run),
        latest_result: parse_opt_time(&fixture.latest_result),
    }
}

fn email_row(fixture: &EmailFixture) -> EmailActionRow {
    EmailActionRow {
        id: fixture.id,
        monitor: fixture.monitor,
        enabled: fixture.enabled,
        priority: fixture.priority.clone(),
        header: fixture.header.clone(),
        include_results: fixture.include_results,
    }
}

fn webhook_row(fixture: &WebhookFixture) -> WebhookActionRow {
    WebhookActionRow {
        id: fixture.id,
        monitor: fixture.monitor,
        enabled: fixture.enabled,
        url: fixture.url.clone(),
        include_results: fixture.include_results,
    }
}

fn slack_row(fixture: &WebhookFixture) -> SlackWebhookActionRow {
    SlackWebhookActionRow {
        id: fixture.id,
        monitor: fixture.monitor,
        enabled: fixture.enabled,
        url: fixture.url.clone(),
        include_results: fixture.include_results,
    }
}

fn recipient_row(fixture: &RecipientFixture) -> RecipientRow {
    RecipientRow {
        id: fixture.id,
        email: fixture.email,
        namespace_user_id: fixture.namespace_user_id,
        namespace_org_id: fixture.namespace_org_id,
    }
}

fn trigger_job_row(fixture: &TriggerJobFixture) -> TriggerJobRow {
    TriggerJobRow {
        id: fixture.id,
        query: fixture.query,
        query_string: fixture.query_string.clone(),
        search_results: fixture.search_results.clone(),
        state: fixture.state.clone(),
        failure_message: fixture.failure_message.clone(),
        started_at: parse_opt_time(&fixture.started_at),
        finished_at: parse_opt_time(&fixture.finished_at),
        process_after: parse_opt_time(&fixture.process_after),
        num_resets: fixture.num_resets,
        num_failures: fixture.num_failures,
        logs: fixture.logs.clone(),
    }
}

fn action_job_row(fixture: &ActionJobFixture) -> ActionJobRow {
    ActionJobRow {
        id: fixture.id,
        email: fixture.email,
        webhook: fixture.webhook,
        slack_webhook: fixture.slack_webhook,
        trigger_event: fixture.trigger_event,
        state: fixture.state.clone(),
        failure_message: fixture.failure_message.clone(),
        started_at: parse_opt_time(&fixture.started_at),
        finished_at: parse_opt_time(&fixture.finished_at),
        process_after: parse_opt_time(&fixture.process_after),
        num_resets: fixture.num_resets,
        num_failures: fixture.num_failures,
        log_contents: fixture.log_contents.clone(),
    }
}

#[derive(Default)]
struct State {
    monitors: Vec<MonitorRow>,
    triggers: Vec<QueryTriggerRow>,
    emails: Vec<EmailActionRow>,
    webhooks: Vec<WebhookActionRow>,
    slacks: Vec<SlackWebhookActionRow>,
    recipients: Vec<RecipientRow>,
    trigger_jobs: Vec<TriggerJobRow>,
    action_jobs: Vec<ActionJobRow>,
}

struct InMemoryCodeMonitors {
    users: Vec<UserData>,
    orgs: Vec<OrgData>,
    org_members: Vec<(i32, i32)>,
    state: Mutex<State>,
}

impl InMemoryCodeMonitors {
    fn new(corpus: &Corpus) -> Self {
        Self {
            users: corpus.fixtures.users.iter().map(user_data).collect(),
            orgs: corpus.fixtures.orgs.iter().map(org_data).collect(),
            org_members: corpus.fixtures.org_members.clone(),
            state: Mutex::new(State {
                monitors: corpus.fixtures.monitors.iter().map(monitor_row).collect(),
                triggers: corpus.fixtures.triggers.iter().map(trigger_row).collect(),
                emails: corpus.fixtures.emails.iter().map(email_row).collect(),
                webhooks: corpus.fixtures.webhooks.iter().map(webhook_row).collect(),
                slacks: corpus.fixtures.slacks.iter().map(slack_row).collect(),
                recipients: corpus
                    .fixtures
                    .recipients
                    .iter()
                    .map(recipient_row)
                    .collect(),
                trigger_jobs: corpus
                    .fixtures
                    .trigger_jobs
                    .iter()
                    .map(trigger_job_row)
                    .collect(),
                action_jobs: corpus
                    .fixtures
                    .action_jobs
                    .iter()
                    .map(action_job_row)
                    .collect(),
            }),
        }
    }

    fn user(&self, id: i32) -> Option<UserData> {
        self.users.iter().find(|user| user.id == id).cloned()
    }

    fn org(&self, id: i32) -> Option<OrgData> {
        self.orgs.iter().find(|org| org.id == id).cloned()
    }

    fn monitor_filter(&self, user_id: Option<i32>, after: Option<i64>, skip_orphaned: bool) -> Vec<MonitorRow> {
        let mut rows: Vec<MonitorRow> = self
            .state
            .lock()
            .unwrap()
            .monitors
            .iter()
            .filter(|row| user_id.map_or(true, |user_id| row.user_id == user_id))
            .filter(|row| after.map_or(true, |after| row.id > after))
            .filter(|row| !skip_orphaned || self.user(row.created_by).is_some())
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        rows
    }
}

impl CodeMonitorsGraphqlDatabase for InMemoryCodeMonitors {
    fn list_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        first: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<Vec<MonitorRow>, String> {
        let mut rows = self.monitor_filter(user_id, after, skip_orphaned);
        if let Some(first) = first {
            rows.truncate(first.max(0) as usize);
        }
        Ok(rows)
    }

    fn count_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<i32, String> {
        Ok(self.monitor_filter(user_id, after, skip_orphaned).len() as i32)
    }

    fn get_monitor(&self, id: i64) -> Result<Option<MonitorRow>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .monitors
            .iter()
            .find(|row| row.id == id)
            .cloned())
    }

    fn create_monitor(
        &self,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        _namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<MonitorRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state.monitors.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = MonitorRow {
            id,
            created_by: actor_uid,
            created_at: now,
            changed_by: actor_uid,
            changed_at: now,
            description: description.to_owned(),
            enabled,
            user_id: namespace_user_id.unwrap_or(0),
        };
        state.monitors.push(row.clone());
        Ok(row)
    }

    fn update_monitor(
        &self,
        id: i64,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        _namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String> {
        let mut state = self.state.lock().unwrap();
        let Some(row) = state.monitors.iter_mut().find(|row| row.id == id) else {
            return Ok(None);
        };
        row.description = description.to_owned();
        row.enabled = enabled;
        row.user_id = namespace_user_id.unwrap_or(row.user_id);
        row.changed_by = actor_uid;
        row.changed_at = now;
        Ok(Some(row.clone()))
    }

    fn update_monitor_enabled(
        &self,
        id: i64,
        enabled: bool,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String> {
        let mut state = self.state.lock().unwrap();
        let Some(row) = state.monitors.iter_mut().find(|row| row.id == id) else {
            return Ok(None);
        };
        row.enabled = enabled;
        row.changed_by = actor_uid;
        row.changed_at = now;
        Ok(Some(row.clone()))
    }

    fn delete_monitor(&self, id: i64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.monitors.retain(|row| row.id != id);
        state.triggers.retain(|row| row.monitor != id);
        state.emails.retain(|row| row.monitor != id);
        state.webhooks.retain(|row| row.monitor != id);
        state.slacks.retain(|row| row.monitor != id);
        Ok(())
    }

    fn create_query_trigger(
        &self,
        monitor_id: i64,
        query: &str,
        _actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<QueryTriggerRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state.triggers.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = QueryTriggerRow {
            id,
            monitor: monitor_id,
            query_string: query.to_owned(),
            next_run: now,
            latest_result: None,
        };
        state.triggers.push(row.clone());
        Ok(row)
    }

    fn update_query_trigger(
        &self,
        id: i64,
        query: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        let row = state
            .triggers
            .iter_mut()
            .find(|row| row.id == id)
            .ok_or_else(|| "sql: no rows in result set".to_owned())?;
        row.query_string = query.to_owned();
        Ok(())
    }

    fn get_query_trigger_for_monitor(
        &self,
        monitor_id: i64,
    ) -> Result<Option<QueryTriggerRow>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .triggers
            .iter()
            .find(|row| row.monitor == monitor_id)
            .cloned())
    }

    fn reset_query_trigger_timestamps(
        &self,
        query_id: i64,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        let row = state
            .triggers
            .iter_mut()
            .find(|row| row.id == query_id)
            .ok_or_else(|| "sql: no rows in result set".to_owned())?;
        row.next_run = now;
        row.latest_result = None;
        Ok(())
    }

    fn upsert_last_searched(
        &self,
        _monitor_id: i64,
        _repo_id: i32,
        _commit_oids: &[String],
    ) -> Result<(), String> {
        Ok(())
    }

    fn list_email_actions(&self, monitor_id: i64) -> Result<Vec<EmailActionRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .emails
            .iter()
            .filter(|row| row.monitor == monitor_id)
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    fn list_webhook_actions(&self, monitor_id: i64) -> Result<Vec<WebhookActionRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .webhooks
            .iter()
            .filter(|row| row.monitor == monitor_id)
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    fn list_slack_webhook_actions(
        &self,
        monitor_id: i64,
    ) -> Result<Vec<SlackWebhookActionRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .slacks
            .iter()
            .filter(|row| row.monitor == monitor_id)
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    fn create_email_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<EmailActionRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state.emails.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = EmailActionRow {
            id,
            monitor: monitor_id,
            enabled,
            priority: priority.to_owned(),
            header: header.to_owned(),
            include_results,
        };
        state.emails.push(row.clone());
        Ok(row)
    }

    fn create_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<WebhookActionRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state.webhooks.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = WebhookActionRow {
            id,
            monitor: monitor_id,
            enabled,
            url: url.to_owned(),
            include_results,
        };
        state.webhooks.push(row.clone());
        Ok(row)
    }

    fn create_slack_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<SlackWebhookActionRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state.slacks.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = SlackWebhookActionRow {
            id,
            monitor: monitor_id,
            enabled,
            url: url.to_owned(),
            include_results,
        };
        state.slacks.push(row.clone());
        Ok(row)
    }

    fn delete_email_actions(&self, ids: &[i64], _monitor_id: i64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.emails.retain(|row| !ids.contains(&row.id));
        Ok(())
    }

    fn delete_webhook_actions(&self, _monitor_id: i64, ids: &[i64]) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.webhooks.retain(|row| !ids.contains(&row.id));
        Ok(())
    }

    fn delete_slack_webhook_actions(
        &self,
        _monitor_id: i64,
        ids: &[i64],
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.slacks.retain(|row| !ids.contains(&row.id));
        Ok(())
    }

    fn update_email_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<Option<EmailActionRow>, String> {
        let mut state = self.state.lock().unwrap();
        let Some(row) = state.emails.iter_mut().find(|row| row.id == id) else {
            return Ok(None);
        };
        row.enabled = enabled;
        row.include_results = include_results;
        row.priority = priority.to_owned();
        row.header = header.to_owned();
        Ok(Some(row.clone()))
    }

    fn update_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<Option<WebhookActionRow>, String> {
        let mut state = self.state.lock().unwrap();
        let Some(row) = state.webhooks.iter_mut().find(|row| row.id == id) else {
            return Ok(None);
        };
        row.enabled = enabled;
        row.include_results = include_results;
        row.url = url.to_owned();
        Ok(Some(row.clone()))
    }

    fn update_slack_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        _actor_uid: i32,
        _now: OffsetDateTime,
    ) -> Result<Option<SlackWebhookActionRow>, String> {
        let mut state = self.state.lock().unwrap();
        let Some(row) = state.slacks.iter_mut().find(|row| row.id == id) else {
            return Ok(None);
        };
        row.enabled = enabled;
        row.include_results = include_results;
        row.url = url.to_owned();
        Ok(Some(row.clone()))
    }

    fn delete_recipients(&self, email_id: i64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.recipients.retain(|row| row.email != email_id);
        Ok(())
    }

    fn create_recipient(
        &self,
        email_id: i64,
        user_id: Option<i32>,
        org_id: Option<i32>,
    ) -> Result<RecipientRow, String> {
        let mut state = self.state.lock().unwrap();
        let id = state
            .recipients
            .iter()
            .map(|row| row.id)
            .max()
            .unwrap_or(0)
            + 1;
        let row = RecipientRow {
            id,
            email: email_id,
            namespace_user_id: user_id,
            namespace_org_id: org_id,
        };
        state.recipients.push(row.clone());
        Ok(row)
    }

    fn list_recipients(
        &self,
        email_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<RecipientRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .recipients
            .iter()
            .filter(|row| row.email == email_id)
            .filter(|row| after.map_or(true, |after| row.id > after))
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        if let Some(first) = first {
            rows.truncate(first.max(0) as usize);
        }
        Ok(rows)
    }

    fn count_recipients(&self, email_id: i64) -> Result<i32, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .recipients
            .iter()
            .filter(|row| row.email == email_id)
            .count() as i32)
    }

    fn list_trigger_jobs(
        &self,
        query_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<TriggerJobRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .trigger_jobs
            .iter()
            .filter(|row| row.query == query_id)
            .filter(|row| after.map_or(true, |after| i64::from(row.id) < after))
            .cloned()
            .collect();
        rows.sort_by(|left, right| right.id.cmp(&left.id));
        if let Some(first) = first {
            rows.truncate(first.max(0) as usize);
        }
        Ok(rows)
    }

    fn count_trigger_jobs(&self, query_id: i64) -> Result<i32, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .trigger_jobs
            .iter()
            .filter(|row| row.query == query_id)
            .filter(|row| {
                let completed_with_results = row.state.as_deref() == Some("completed")
                    && row
                        .search_results
                        .as_ref()
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|results| !results.is_empty());
                completed_with_results || row.state.as_deref() != Some("completed")
            })
            .count() as i32)
    }

    fn list_action_jobs(
        &self,
        filter: &ActionJobFilter,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<ActionJobRow>, String> {
        let mut rows: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .action_jobs
            .iter()
            .filter(|row| {
                filter
                    .trigger_event_id
                    .map_or(true, |id| row.trigger_event == id)
                    && filter.email_id.map_or(true, |id| row.email == Some(id))
                    && filter.webhook_id.map_or(true, |id| row.webhook == Some(id))
                    && filter
                        .slack_webhook_id
                        .map_or(true, |id| row.slack_webhook == Some(id))
                    && after.map_or(true, |after| i64::from(row.id) > after)
            })
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        if let Some(first) = first {
            rows.truncate(first.max(0) as usize);
        }
        Ok(rows)
    }

    fn count_action_jobs(&self, filter: &ActionJobFilter) -> Result<i32, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .action_jobs
            .iter()
            .filter(|row| {
                filter
                    .trigger_event_id
                    .map_or(true, |id| row.trigger_event == id)
                    && filter.email_id.map_or(true, |id| row.email == Some(id))
                    && filter.webhook_id.map_or(true, |id| row.webhook == Some(id))
                    && filter
                        .slack_webhook_id
                        .map_or(true, |id| row.slack_webhook == Some(id))
            })
            .count() as i32)
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        Ok(self.user(id))
    }

    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String> {
        Ok(self.org(id))
    }

    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String> {
        Ok(self.user(uid))
    }

    fn is_site_admin(&self, uid: i32) -> Result<bool, String> {
        Ok(self.user(uid).is_some_and(|user| user.site_admin))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        Ok(self.org(org_id).is_some() && self.org_members.contains(&(org_id, user_id)))
    }
}

struct EmptySnapshot;

impl CodeMonitorSnapshot for EmptySnapshot {
    fn snapshot(
        &self,
        _query: &str,
        _request_context: sg_service_frontend::search_contexts::RequestContext,
    ) -> Result<BTreeMap<i32, Vec<String>>, String> {
        Ok(BTreeMap::new())
    }
}

struct NoopTestSender;

impl CodeMonitorTestSender for NoopTestSender {
    fn send_test_email(&self, _user_id: i32, _description: &str) -> Result<(), String> {
        Ok(())
    }
    fn send_test_webhook(&self, _description: &str, _url: &str) -> Result<(), String> {
        Ok(())
    }
    fn send_test_slack_webhook(&self, _description: &str, _url: &str) -> Result<(), String> {
        Ok(())
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut entries: Vec<(&str, serde_json::Value)> = Vec::new();
    if !response.errors.is_empty() {
        entries.push((
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        ));
    }
    if let Some(data) = response.data_json() {
        entries.push(("data", serde_json::from_str(&data).unwrap()));
    }
    let map: BTreeMap<&str, serde_json::Value> = entries.into_iter().collect();
    serde_json::to_string(&map).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn code_monitors_harness() {
    if std::env::var("SG_M9_13A_CM_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_13A_CM_CORPUS").expect("M9_13A_CM_CORPUS");
    let out = std::env::var("M9_13A_CM_OUT").expect("M9_13A_CM_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[MAIN_SCHEMA, CODE_MONITORS_SCHEMA]).expect("schema");
    let now = parse_time(&corpus.now);

    for case in &corpus.cases {
        let db: Arc<dyn CodeMonitorsGraphqlDatabase + Send + Sync> =
            Arc::new(InMemoryCodeMonitors::new(&corpus));
        let resolver = Arc::new(
            CodeMonitorsResolver::new(
                db,
                Arc::new(EmptySnapshot),
                Arc::new(NoopTestSender),
                case.code_monitors_enabled,
                case.dotcom,
            )
            .with_clock(Arc::new(move || now)),
        );
        let (actor, user) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => {
                let fixture = corpus
                    .fixtures
                    .users
                    .iter()
                    .find(|user| user.id == case.uid)
                    .expect("viewer fixture");
                let data = user_data(fixture);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let roots = FrontendRootResolver::with_user(user).with_code_monitors(Some(resolver));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
