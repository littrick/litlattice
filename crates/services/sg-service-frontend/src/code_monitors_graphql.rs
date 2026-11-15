#![forbid(unsafe_code)]

//! Code-monitor GraphQL surface (M9.13a).
//!
//! Ports `cmd/frontend/internal/codemonitors/resolvers/resolvers.go` and
//! `cmd/frontend/graphqlbackend/code_monitors.go`:
//!
//! - `Query.monitors` (site-admin only) and `User.monitors` (self only);
//! - `Query.node` kind `CodeMonitor`;
//! - the mutations `createCodeMonitor`, `toggleCodeMonitor`,
//!   `deleteCodeMonitor`, `updateCodeMonitor`,
//!   `resetTriggerQueryTimestamps` and the three `triggerTest*Action` sends;
//! - the full object graph (`Monitor`, `MonitorQuery`, trigger/action events,
//!   email/webhook/Slack actions and email recipients) with the baseline's
//!   ad-hoc pagination (only monitors use the `+1` probe).
//!
//! Authorization, error strings and several quirks are copied verbatim from
//! the baseline, including:
//! - `UpdateCodeMonitor` ignoring the `splitActionIDs` error;
//! - the action connection panicking when a page ends on a non-email action;
//! - `Query.monitors`/`User.monitors` not checking `DISABLE_CODE_MONITORS`
//!   (only the `CodeMonitor` node resolver does).
//!
//! External seams: [`CodeMonitorSnapshot`] replaces `codemonitors.Snapshot`
//! (which needs a non-transaction DB plus the search planner/gitserver) and
//! [`CodeMonitorTestSender`] replaces the test e-mail/webhook/Slack sends.
//! No table, column or Redis key is added.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, OrgData, OrgObject, PageInfoObject, UserData, UserObject,
    MUST_BE_SITE_ADMIN_OR_SAME_USER,
};
use crate::saved_searches_graphql::unmarshal_namespace_to_ids;

/// Relay kinds (`resolvers.go:742-753`).
pub const MONITOR_KIND: &str = "CodeMonitor";
pub const MONITOR_TRIGGER_QUERY_KIND: &str = "CodeMonitorTriggerQuery";
pub const MONITOR_TRIGGER_EVENT_KIND: &str = "CodeMonitorTriggerEvent";
pub const MONITOR_ACTION_EMAIL_KIND: &str = "CodeMonitorActionEmail";
pub const MONITOR_ACTION_WEBHOOK_KIND: &str = "CodeMonitorActionWebhook";
pub const MONITOR_ACTION_SLACK_WEBHOOK_KIND: &str = "CodeMonitorActionSlackWebhook";
pub const MONITOR_ACTION_EMAIL_EVENT_KIND: &str = "CodeMonitorActionEmailEvent";
pub const MONITOR_ACTION_EMAIL_RECIPIENT_KIND: &str = "CodeMonitorActionEmailRecipient";

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// `database.Monitor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorRow {
    pub id: i64,
    pub created_by: i32,
    pub created_at: OffsetDateTime,
    pub changed_by: i32,
    pub changed_at: OffsetDateTime,
    pub description: String,
    pub enabled: bool,
    pub user_id: i32,
}

/// `database.QueryTrigger`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTriggerRow {
    pub id: i64,
    pub monitor: i64,
    pub query_string: String,
    pub next_run: OffsetDateTime,
    pub latest_result: Option<OffsetDateTime>,
}

/// `database.EmailAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailActionRow {
    pub id: i64,
    pub monitor: i64,
    pub enabled: bool,
    pub priority: String,
    pub header: String,
    pub include_results: bool,
}

/// `database.WebhookAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookActionRow {
    pub id: i64,
    pub monitor: i64,
    pub enabled: bool,
    pub url: String,
    pub include_results: bool,
}

/// `database.SlackWebhookAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackWebhookActionRow {
    pub id: i64,
    pub monitor: i64,
    pub enabled: bool,
    pub url: String,
    pub include_results: bool,
}

/// `database.Recipient`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecipientRow {
    pub id: i64,
    pub email: i64,
    pub namespace_user_id: Option<i32>,
    pub namespace_org_id: Option<i32>,
}

/// `database.TriggerJob` (the `logs`/`search_results` columns stay JSON).
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerJobRow {
    pub id: i32,
    pub query: i64,
    pub query_string: Option<String>,
    pub search_results: Option<serde_json::Value>,
    pub state: Option<String>,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub logs: Option<serde_json::Value>,
}

/// `database.ActionJob`.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionJobRow {
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

/// `database.ListActionJobsOpts` filters shared by list and count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActionJobFilter {
    pub trigger_event_id: Option<i32>,
    pub email_id: Option<i64>,
    pub webhook_id: Option<i64>,
    pub slack_webhook_id: Option<i64>,
}

/// The snapshot seam (`codemonitors.Snapshot`).
pub trait CodeMonitorSnapshot: Send + Sync {
    fn snapshot(
        &self,
        query: &str,
        request_context: crate::search_contexts::RequestContext,
    ) -> Result<BTreeMap<i32, Vec<String>>, String>;
}

/// An empty snapshot, used by unit/differential harnesses.
pub struct EmptySnapshot;

impl CodeMonitorSnapshot for EmptySnapshot {
    fn snapshot(
        &self,
        _query: &str,
        _request_context: crate::search_contexts::RequestContext,
    ) -> Result<BTreeMap<i32, Vec<String>>, String> {
        Ok(BTreeMap::new())
    }
}

/// Production `codemonitors.Snapshot` (`internal/codemonitors/search.go:77`):
/// plans the monitor query, resolves the current revisions per repo, and
/// returns the `map[api.RepoID][]string` written to `cm_last_searched`.
///
/// The search runtime is the same [`TreeRuntimeClients`] bundle the frontend
/// search stream uses; `None` means the service connections were unavailable
/// at assembly time, so snapshotting (and therefore create/update) reports an
/// explicit error instead of silently skipping the snapshot.
pub struct ProductionSnapshot {
    pub runtime: Option<crate::search_stream::TreeRuntimeClients>,
}

impl ProductionSnapshot {
    pub fn new(runtime: crate::search_stream::TreeRuntimeClients) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    /// Unavailable variant used when endpoint configuration could not be
    /// assembled.
    pub fn unavailable() -> Self {
        Self { runtime: None }
    }
}

impl CodeMonitorSnapshot for ProductionSnapshot {
    fn snapshot(
        &self,
        query: &str,
        request_context: crate::search_contexts::RequestContext,
    ) -> Result<BTreeMap<i32, Vec<String>>, String> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Err("code monitor snapshots are not available in this build".to_owned());
        };
        // `codemonitors.Snapshot` runs outside a transaction; the Rust adapter
        // is per-statement, so `in_transaction` is always false here.
        crate::codemonitors_search::snapshot(runtime, query, false, request_context)
            .map_err(|error| error.0)
    }
}

/// `background.SendEmailForNewSearchResult` / `SendTestWebhook` /
/// `SendTestSlackWebhook` seam. Tests must never hit the network.
pub trait CodeMonitorTestSender: Send + Sync {
    fn send_test_email(&self, user_id: i32, description: &str) -> Result<(), String>;
    fn send_test_webhook(&self, description: &str, url: &str) -> Result<(), String>;
    fn send_test_slack_webhook(&self, description: &str, url: &str) -> Result<(), String>;
}

/// Production has no e-mail/Slack transport wired in this slice; the test
/// sends report an explicit error rather than silently succeeding.
pub struct UnavailableTestSender;

impl CodeMonitorTestSender for UnavailableTestSender {
    fn send_test_email(&self, _user_id: i32, _description: &str) -> Result<(), String> {
        Err("sending test emails is not available in this build".to_owned())
    }
    fn send_test_webhook(&self, _description: &str, _url: &str) -> Result<(), String> {
        Err("sending test webhooks is not available in this build".to_owned())
    }
    fn send_test_slack_webhook(&self, _description: &str, _url: &str) -> Result<(), String> {
        Err("sending test Slack webhooks is not available in this build".to_owned())
    }
}

/// A sender that does nothing, for harnesses that only exercise the guard
/// paths.
pub struct NoopTestSender;

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

/// The synchronous `database.CodeMonitorStore` seam.
#[allow(clippy::too_many_arguments)]
pub trait CodeMonitorsGraphqlDatabase: Send + Sync {
    fn list_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        first: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<Vec<MonitorRow>, String>;
    fn count_monitors(
        &self,
        user_id: Option<i32>,
        after: Option<i64>,
        skip_orphaned: bool,
    ) -> Result<i32, String>;
    fn get_monitor(&self, id: i64) -> Result<Option<MonitorRow>, String>;
    fn create_monitor(
        &self,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<MonitorRow, String>;
    fn update_monitor(
        &self,
        id: i64,
        description: &str,
        enabled: bool,
        namespace_user_id: Option<i32>,
        namespace_org_id: Option<i32>,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String>;
    fn update_monitor_enabled(
        &self,
        id: i64,
        enabled: bool,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<MonitorRow>, String>;
    fn delete_monitor(&self, id: i64) -> Result<(), String>;

    fn create_query_trigger(
        &self,
        monitor_id: i64,
        query: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<QueryTriggerRow, String>;
    fn update_query_trigger(
        &self,
        id: i64,
        query: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn get_query_trigger_for_monitor(
        &self,
        monitor_id: i64,
    ) -> Result<Option<QueryTriggerRow>, String>;
    fn reset_query_trigger_timestamps(
        &self,
        query_id: i64,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn upsert_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
        commit_oids: &[String],
    ) -> Result<(), String>;

    fn list_email_actions(&self, monitor_id: i64) -> Result<Vec<EmailActionRow>, String>;
    fn list_webhook_actions(&self, monitor_id: i64) -> Result<Vec<WebhookActionRow>, String>;
    fn list_slack_webhook_actions(
        &self,
        monitor_id: i64,
    ) -> Result<Vec<SlackWebhookActionRow>, String>;
    fn create_email_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<EmailActionRow, String>;
    fn create_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<WebhookActionRow, String>;
    fn create_slack_webhook_action(
        &self,
        monitor_id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<SlackWebhookActionRow, String>;
    fn delete_email_actions(&self, ids: &[i64], monitor_id: i64) -> Result<(), String>;
    fn delete_webhook_actions(&self, monitor_id: i64, ids: &[i64]) -> Result<(), String>;
    fn delete_slack_webhook_actions(&self, monitor_id: i64, ids: &[i64]) -> Result<(), String>;
    fn update_email_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        priority: &str,
        header: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<EmailActionRow>, String>;
    fn update_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<WebhookActionRow>, String>;
    fn update_slack_webhook_action(
        &self,
        id: i64,
        enabled: bool,
        include_results: bool,
        url: &str,
        actor_uid: i32,
        now: OffsetDateTime,
    ) -> Result<Option<SlackWebhookActionRow>, String>;

    fn delete_recipients(&self, email_id: i64) -> Result<(), String>;
    fn create_recipient(
        &self,
        email_id: i64,
        user_id: Option<i32>,
        org_id: Option<i32>,
    ) -> Result<RecipientRow, String>;
    fn list_recipients(
        &self,
        email_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<RecipientRow>, String>;
    fn count_recipients(&self, email_id: i64) -> Result<i32, String>;

    fn list_trigger_jobs(
        &self,
        query_id: i64,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<TriggerJobRow>, String>;
    fn count_trigger_jobs(&self, query_id: i64) -> Result<i32, String>;

    fn list_action_jobs(
        &self,
        filter: &ActionJobFilter,
        first: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<ActionJobRow>, String>;
    fn count_action_jobs(&self, filter: &ActionJobFilter) -> Result<i32, String>;

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String>;
    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String>;
    fn is_site_admin(&self, uid: i32) -> Result<bool, String>;
    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;
}

/// The injected handle.
pub type CodeMonitorsHandle = Arc<CodeMonitorsResolver>;

/// Wrapper so `graphql_resolvers::NestedPrefetch` can keep its derives.
#[derive(Clone)]
pub struct CodeMonitorsHandleWrapper(pub CodeMonitorsHandle);

impl std::fmt::Debug for CodeMonitorsHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CodeMonitorsHandle")
    }
}

impl PartialEq for CodeMonitorsHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CodeMonitorsHandleWrapper {}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

/// `resolvers.Resolver`.
pub struct CodeMonitorsResolver {
    pub db: Arc<dyn CodeMonitorsGraphqlDatabase + Send + Sync>,
    pub snapshot: Arc<dyn CodeMonitorSnapshot>,
    pub test_sender: Arc<dyn CodeMonitorTestSender>,
    /// `codemonitors.IsEnabled()` (`DISABLE_CODE_MONITORS`).
    pub enabled: bool,
    /// `dotcom.SourcegraphDotComMode()`.
    pub dotcom: bool,
    /// `db.CodeMonitors().Now()`.
    pub now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

impl CodeMonitorsResolver {
    pub fn new(
        db: Arc<dyn CodeMonitorsGraphqlDatabase + Send + Sync>,
        snapshot: Arc<dyn CodeMonitorSnapshot>,
        test_sender: Arc<dyn CodeMonitorTestSender>,
        enabled: bool,
        dotcom: bool,
    ) -> Self {
        Self {
            db,
            snapshot,
            test_sender,
            enabled,
            dotcom,
            now: Arc::new(OffsetDateTime::now_utc),
        }
    }

    pub fn with_clock(
        mut self,
        now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    ) -> Self {
        self.now = now;
        self
    }

    fn now(&self) -> OffsetDateTime {
        (self.now)()
    }

    fn is_enabled(&self) -> Result<(), ExecError> {
        if self.enabled {
            Ok(())
        } else {
            Err(ExecError::new("code monitoring is not enabled"))
        }
    }
}

/// `relay.MarshalID(kind, id)`.
pub fn marshal_id(kind: &str, id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{id}"))
}

/// `relay.UnmarshalSpec` into an int64 (the kind is not inspected).
pub fn unmarshal_spec_i64(id: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `relay.UnmarshalSpec` into an int (the kind is not inspected).
pub fn unmarshal_spec_i32(id: &str) -> Result<i32, String> {
    unmarshal_spec_i64(id).map(|value| value as i32)
}

/// `unmarshalMonitorID` (`resolvers.go:755-762`).
pub fn unmarshal_monitor_id(id: &str) -> Result<i64, String> {
    if relay_kind(id).as_deref() != Some(MONITOR_KIND) {
        return Err(format!(
            "expected graphql ID kind {MONITOR_KIND}, got {}",
            relay_kind(id).unwrap_or_default()
        ));
    }
    unmarshal_spec_i64(id)
}

/// `unmarshalEmailID` (`resolvers.go:764-771`).
pub fn unmarshal_email_id(id: &str) -> Result<i64, String> {
    if relay_kind(id).as_deref() != Some(MONITOR_ACTION_EMAIL_KIND) {
        return Err(format!(
            "expected graphql ID kind {MONITOR_ACTION_EMAIL_KIND}, got {}",
            relay_kind(id).unwrap_or_default()
        ));
    }
    unmarshal_spec_i64(id)
}

/// `unmarshalAfter` (`resolvers.go:773-781`): a raw Relay ID whose spec is an
/// int; the kind is ignored.
fn unmarshal_after(after: Option<&str>) -> Result<Option<i32>, String> {
    match after {
        Some(raw) => unmarshal_spec_i32(raw).map(Some),
        None => Ok(None),
    }
}

fn sql_no_rows() -> String {
    "sql: no rows in result set".to_owned()
}

fn i32_arg(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments
        .i64(name)
        .and_then(|value| i32::try_from(value).ok())
}

/// `validateSlackURL` (`resolvers.go:1379-1390`).
pub fn validate_slack_url(url_string: &str) -> Result<(), String> {
    // Go's `url.Parse` rejects only malformed URLs (control characters,
    // invalid escapes, missing scheme separator). Reuse `url` through the
    // `http`-less manual subset we need: scheme and host.
    let parsed = parse_url(url_string).map_err(|error| error.to_string())?;
    if parsed.host.as_deref() != Some("hooks.slack.com") || parsed.scheme.as_deref() != Some("https")
    {
        return Err("slack webhook URL must begin with 'https://hooks.slack.com/".to_owned());
    }
    Ok(())
}

struct ParsedUrl {
    scheme: Option<String>,
    host: Option<String>,
}

/// The subset of `net/url.Parse` the Slack check needs. Query/fragment are
/// irrelevant; an absolute URL is `scheme://host...`.
fn parse_url(input: &str) -> Result<ParsedUrl, String> {
    for byte in input.bytes() {
        if byte < 0x20 {
            return Err("net/url: invalid control character in URL".to_owned());
        }
    }
    let Some((scheme, rest)) = input.split_once("://") else {
        return Ok(ParsedUrl {
            scheme: None,
            host: None,
        });
    };
    if scheme.is_empty()
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        return Ok(ParsedUrl {
            scheme: Some(scheme.to_owned()),
            host: None,
        });
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or("");
    let host = host.split(':').next().unwrap_or("").to_owned();
    Ok(ParsedUrl {
        scheme: Some(scheme.to_owned()),
        host: Some(host),
    })
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// `resolvers.isAllowedToCreate` (`resolvers.go:694-708`).
pub fn is_allowed_to_create(
    db: &dyn CodeMonitorsGraphqlDatabase,
    actor: Actor,
    owner: &str,
) -> Result<(), ExecError> {
    let owner_id = unmarshal_spec_i32(owner).map_err(ExecError::new)?;
    match relay_kind(owner).as_deref() {
        Some("User") => check_site_admin_or_same_user(db, actor, owner_id),
        Some("Org") => Err(ExecError::new(
            "creating a code monitor with an org namespace is no longer supported",
        )),
        _ => Err(ExecError::new("provided ID is not a namespace")),
    }
}

/// `auth.CheckCurrentUserIsSiteAdmin` (`site_admin.go:16-32`) over this
/// module's seam.
fn check_current_user_is_site_admin(
    db: &dyn CodeMonitorsGraphqlDatabase,
    actor: Actor,
) -> Result<(), ExecError> {
    if actor.is_internal() {
        return Ok(());
    }
    let Some(uid) = actor.uid() else {
        return Err(ExecError::new("not authenticated"));
    };
    let user = db
        .current_user(uid)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("not authenticated"))?;
    if !user.site_admin {
        return Err(ExecError::new("must be site admin"));
    }
    Ok(())
}

/// `check_site_admin_or_same_user` over the code-monitor seam.
pub fn check_site_admin_or_same_user(
    db: &dyn CodeMonitorsGraphqlDatabase,
    actor: Actor,
    subject: i32,
) -> Result<(), ExecError> {
    if actor.is_internal() || actor.uid() == Some(subject) {
        return Ok(());
    }
    match check_current_user_is_site_admin(db, actor) {
        Ok(()) => Ok(()),
        Err(_) => Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER)),
    }
}

impl CodeMonitorsResolver {
    fn is_allowed_to_edit(&self, actor: Actor, id: &str) -> Result<(), ExecError> {
        if self.dotcom {
            return Err(ExecError::new(
                "Code Monitors are disabled on sourcegraph.com",
            ));
        }
        let monitor_id = unmarshal_monitor_id(id).map_err(ExecError::new)?;
        let owner = self
            .db
            .get_monitor(monitor_id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(sql_no_rows()))?;
        is_allowed_to_create(self.db.as_ref(), actor, &marshal_id("User", owner.user_id as i64))
    }

    fn monitor_by_id(&self, actor: Actor, id: &str) -> Result<MonitorObject, ExecError> {
        self.is_allowed_to_edit(actor, id)?;
        let monitor_id = unmarshal_monitor_id(id).map_err(ExecError::new)?;
        let row = self
            .db
            .get_monitor(monitor_id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(sql_no_rows()))?;
        Ok(MonitorObject::from_ref(self.clone_like(), row))
    }

    /// The resolver is shared through `Arc`, so objects cannot borrow it;
    /// they hold a cheap pointer clone instead.
    fn clone_like(&self) -> ResolverRef {
        ResolverRef {
            db: Arc::clone(&self.db),
            snapshot: Arc::clone(&self.snapshot),
            test_sender: Arc::clone(&self.test_sender),
            enabled: self.enabled,
            dotcom: self.dotcom,
            now: Arc::clone(&self.now),
        }
    }
}

/// A cheap, object-embeddable view of a [`CodeMonitorsResolver`].
#[derive(Clone)]
pub struct ResolverRef {
    pub db: Arc<dyn CodeMonitorsGraphqlDatabase + Send + Sync>,
    pub snapshot: Arc<dyn CodeMonitorSnapshot>,
    pub test_sender: Arc<dyn CodeMonitorTestSender>,
    pub enabled: bool,
    pub dotcom: bool,
    pub now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

impl ResolverRef {
    fn now(&self) -> OffsetDateTime {
        (self.now)()
    }
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

fn user_object(db: &dyn CodeMonitorsGraphqlDatabase, user: UserData, actor: Actor) -> FieldValue {
    let viewer_site_admin = actor
        .uid()
        .and_then(|uid| db.is_site_admin(uid).ok())
        .unwrap_or(false);
    FieldValue::Object(Arc::new(UserObject::from_data_as_viewer(
        user,
        viewer_site_admin,
    )))
}

/// Computes `OrgData`'s viewer flags (`auth.CheckOrgAccessOrSiteAdmin`).
fn org_data_for_viewer(
    record: OrgData,
    actor: Actor,
    db: &dyn CodeMonitorsGraphqlDatabase,
) -> Result<OrgData, String> {
    let (is_admin, is_member) = match actor.uid() {
        Some(uid) => {
            let admin = db.is_site_admin(uid)?;
            let member = if admin {
                true
            } else {
                db.is_org_member(record.id, uid)?
            };
            (admin, member)
        }
        None => (false, false),
    };
    Ok(OrgData {
        viewer_can_administer: is_admin || is_member,
        viewer_is_member: is_member,
        ..record
    })
}

fn namespace_object(
    db: &dyn CodeMonitorsGraphqlDatabase,
    user_id: Option<i32>,
    org_id: Option<i32>,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    if let Some(org_id) = org_id {
        let org = db
            .org_by_id(org_id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(format!("org not found: {org_id}")))?;
        let org = org_data_for_viewer(org, actor, db).map_err(ExecError::new)?;
        return Ok(FieldValue::Object(Arc::new(OrgObject::from_data(org))));
    }
    let user_id = user_id.ok_or_else(|| ExecError::new("no namespace"))?;
    let user = db
        .user_by_id(user_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new(format!("user not found: userID {user_id}")))?;
    Ok(user_object(db, user, actor))
}

/// `monitor` (`resolvers.go:784-824`).
pub struct MonitorObject {
    resolver: ResolverRef,
    row: MonitorRow,
}

impl MonitorObject {
    fn from_ref(resolver: ResolverRef, row: MonitorRow) -> Self {
        Self { resolver, row }
    }

    fn relay_id(&self) -> String {
        marshal_id(MONITOR_KIND, self.row.id)
    }
}

impl ObjectValue for MonitorObject {
    fn type_name(&self) -> &'static str {
        "Monitor"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.relay_id())))),
            "createdBy" => {
                let user = self
                    .resolver
                    .db
                    .user_by_id(self.row.created_by)
                    .map_err(ExecError::new)?
                    .ok_or_else(|| {
                        ExecError::new(format!("user not found: userID {}", self.row.created_by))
                    })?;
                Ok(Some(user_object(self.resolver.db.as_ref(), user, actor)))
            }
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row.created_at,
            ))))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.row.description.clone(),
            )))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.row.enabled)))),
            "owner" => Ok(Some(namespace_object(
                self.resolver.db.as_ref(),
                Some(self.row.user_id),
                None,
                actor,
            )?)),
            "trigger" => {
                let trigger = self
                    .resolver
                    .db
                    .get_query_trigger_for_monitor(self.row.id)
                    .map_err(ExecError::new)?
                    .ok_or_else(|| ExecError::new(sql_no_rows()))?;
                Ok(Some(FieldValue::Object(Arc::new(MonitorQueryObject {
                    resolver: self.resolver.clone(),
                    row: trigger,
                    monitor_id: self.row.id,
                }))))
            }
            "actions" => {
                let connection = action_connection(
                    &self.resolver,
                    None,
                    self.row.id,
                    arguments,
                )?;
                Ok(Some(FieldValue::Object(Arc::new(connection))))
            }
            _ => Ok(None),
        }
    }
}

/// `monitorQuery` (`resolvers.go:900-939`).
pub struct MonitorQueryObject {
    resolver: ResolverRef,
    row: QueryTriggerRow,
    monitor_id: i64,
}

impl ObjectValue for MonitorQueryObject {
    fn type_name(&self) -> &'static str {
        "MonitorQuery"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_id(
                MONITOR_TRIGGER_QUERY_KIND,
                self.row.id,
            ))))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(
                self.row.query_string.clone(),
            )))),
            "events" => {
                let after = unmarshal_after(arguments.string("after")).map_err(ExecError::new)?;
                let first = arguments.i64("first");
                let events = self
                    .resolver
                    .db
                    .list_trigger_jobs(self.row.id, first, after.map(i64::from))
                    .map_err(ExecError::new)?;
                let total = self
                    .resolver
                    .db
                    .count_trigger_jobs(self.row.id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    TriggerEventConnection {
                        resolver: self.resolver.clone(),
                        monitor_id: self.monitor_id,
                        events,
                        total_count: total,
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

/// `monitorTriggerEventConnection` (`resolvers.go:942-961`).
pub struct TriggerEventConnection {
    resolver: ResolverRef,
    monitor_id: i64,
    events: Vec<TriggerJobRow>,
    total_count: i32,
}

impl ObjectValue for TriggerEventConnection {
    fn type_name(&self) -> &'static str {
        "MonitorTriggerEventConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .events
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(TriggerEventObject {
                            resolver: self.resolver.clone(),
                            row: row.clone(),
                            monitor_id: self.monitor_id,
                        }))
                    })
                    .collect(),
                element_type: Some("MonitorTriggerEvent"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count as i64)))),
            "pageInfo" => {
                let _ = actor;
                Ok(Some(match self.events.last() {
                    None => FieldValue::Object(Arc::new(PageInfoObject::new(false, None))),
                    Some(row) => FieldValue::Object(Arc::new(PageInfoObject::new(
                        true,
                        Some(marshal_id(MONITOR_TRIGGER_EVENT_KIND, row.id as i64)),
                    ))),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// `stateToStatus` (`resolvers.go:976-982`).
fn state_to_status(state: &str) -> Option<&'static str> {
    match state {
        "completed" => Some("SUCCESS"),
        "queued" | "processing" => Some("PENDING"),
        "errored" | "failed" => Some("ERROR"),
        _ => None,
    }
}

fn state(row: &Option<String>) -> &str {
    row.as_deref().unwrap_or("")
}

/// `(*CommitMatch).ResultCount` over the stored JSON.
fn result_count(value: &serde_json::Value) -> i32 {
    let Some(matches) = value.as_array() else {
        return 0;
    };
    let mut count = 0;
    for commit in matches {
        let preview = commit
            .get("DiffPreview")
            .or_else(|| commit.get("diffPreview"));
        let preview = preview.or_else(|| {
            commit
                .get("MessagePreview")
                .or_else(|| commit.get("messagePreview"))
        });
        let matched = preview
            .and_then(|preview| {
                preview
                    .get("matchedRanges")
                    .or_else(|| preview.get("MatchedRanges"))
            })
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        count += if matched > 0 { matched as i32 } else { 1 };
    }
    count
}

/// `monitorTriggerEvent` (`resolvers.go:963-1025`).
pub struct TriggerEventObject {
    resolver: ResolverRef,
    row: TriggerJobRow,
    monitor_id: i64,
}

impl TriggerEventObject {
    fn relay_id(&self) -> String {
        marshal_id(MONITOR_TRIGGER_EVENT_KIND, self.row.id as i64)
    }

    fn message(&self) -> String {
        let mut message = String::new();
        if let Some(failure) = &self.row.failure_message {
            message.push_str(failure);
            message.push('\n');
        }
        if let Some(logs) = self.row.logs.as_ref().and_then(|value| value.as_array()) {
            for log in logs {
                let text = log
                    .get("Message")
                    .or_else(|| log.get("message"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                message.push_str(text);
                message.push('\n');
            }
        }
        message.trim().to_owned()
    }
}

impl ObjectValue for TriggerEventObject {
    fn type_name(&self) -> &'static str {
        "MonitorTriggerEvent"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.relay_id())))),
            "status" => match state_to_status(state(&self.row.state)) {
                Some(status) => Ok(Some(FieldValue::Json(Json::Str(status.to_owned())))),
                None => Err(ExecError::new(format!(
                    "unknown status: {}",
                    state(&self.row.state)
                ))),
            },
            "message" => Ok(Some(FieldValue::Json(Json::Str(self.message())))),
            "timestamp" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row
                    .finished_at
                    .unwrap_or_else(|| self.resolver.now()),
            ))))),
            "query" => Ok(Some(match &self.row.query_string {
                Some(query) => FieldValue::Json(Json::Str(query.clone())),
                None => FieldValue::Null,
            })),
            "resultCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.row
                    .search_results
                    .as_ref()
                    .map(result_count)
                    .unwrap_or(0) as i64,
            )))),
            "actions" => {
                let connection = action_connection(
                    &self.resolver,
                    Some(self.row.id),
                    self.monitor_id,
                    arguments,
                )?;
                Ok(Some(FieldValue::Object(Arc::new(connection))))
            }
            _ => Ok(None),
        }
    }
}

/// `action` union member (`resolvers.go:1053-1082`).
#[derive(Clone)]
enum ActionKind {
    Email(EmailActionRow),
    Webhook(WebhookActionRow),
    Slack(SlackWebhookActionRow),
}

impl ActionKind {
    fn relay_id(&self) -> String {
        match self {
            Self::Email(row) => marshal_id(MONITOR_ACTION_EMAIL_KIND, row.id),
            Self::Webhook(row) => marshal_id(MONITOR_ACTION_WEBHOOK_KIND, row.id),
            Self::Slack(row) => marshal_id(MONITOR_ACTION_SLACK_WEBHOOK_KIND, row.id),
        }
    }

    fn object(&self, resolver: ResolverRef, trigger_event_id: Option<i32>) -> FieldValue {
        match self {
            Self::Email(row) => FieldValue::Object(Arc::new(EmailObject {
                resolver,
                row: row.clone(),
                trigger_event_id,
            })),
            Self::Webhook(row) => FieldValue::Object(Arc::new(WebhookObject {
                resolver,
                row: row.clone(),
                trigger_event_id,
            })),
            Self::Slack(row) => FieldValue::Object(Arc::new(SlackWebhookObject {
                resolver,
                row: row.clone(),
                trigger_event_id,
            })),
        }
    }
}

/// `monitorActionConnection` (`resolvers.go:1028-1050`).
pub struct ActionConnection {
    resolver: ResolverRef,
    actions: Vec<ActionKind>,
    trigger_event_id: Option<i32>,
    total_count: i32,
}

impl ObjectValue for ActionConnection {
    fn type_name(&self) -> &'static str {
        "MonitorActionConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .actions
                    .iter()
                    .map(|action| {
                        action.object(self.resolver.clone(), self.trigger_event_id)
                    })
                    .collect(),
                element_type: Some("MonitorAction"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total_count as i64,
            )))),
            "pageInfo" => {
                let Some(last) = self.actions.last() else {
                    return Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                        false, None,
                    )))));
                };
                match last {
                    ActionKind::Email(row) => Ok(Some(FieldValue::Object(Arc::new(
                        PageInfoObject::new(
                            true,
                            Some(marshal_id(MONITOR_ACTION_EMAIL_KIND, row.id)),
                        ),
                    )))),
                    // Baseline `panic("found non-email monitor action")`.
                    ActionKind::Webhook(_) | ActionKind::Slack(_) => {
                        panic!("found non-email monitor action")
                    }
                }
            }
            _ => Ok(None),
        }
    }
}

fn action_connection(
    resolver: &ResolverRef,
    trigger_event_id: Option<i32>,
    monitor_id: i64,
    arguments: &Arguments,
) -> Result<ActionConnection, ExecError> {
    let emails = resolver
        .db
        .list_email_actions(monitor_id)
        .map_err(ExecError::new)?;
    let webhooks = resolver
        .db
        .list_webhook_actions(monitor_id)
        .map_err(ExecError::new)?;
    let slacks = resolver
        .db
        .list_slack_webhook_actions(monitor_id)
        .map_err(ExecError::new)?;
    let mut actions: Vec<ActionKind> = Vec::with_capacity(emails.len() + webhooks.len() + slacks.len());
    actions.extend(emails.into_iter().map(ActionKind::Email));
    actions.extend(webhooks.into_iter().map(ActionKind::Webhook));
    actions.extend(slacks.into_iter().map(ActionKind::Slack));
    let total_count = actions.len() as i32;
    if let Some(after) = arguments.string("after") {
        if let Some(index) = actions
            .iter()
            .position(|action| action.relay_id() == after)
        {
            actions = actions[index + 1..].to_vec();
        }
    }
    if let Some(first) = i32_arg(arguments, "first") {
        if first > 0 && actions.len() > first as usize {
            actions.truncate(first as usize);
        }
    }
    Ok(ActionConnection {
        resolver: resolver.clone(),
        actions,
        trigger_event_id,
        total_count,
    })
}

/// `monitorEmail` (`resolvers.go:1084-1185`).
pub struct EmailObject {
    resolver: ResolverRef,
    row: EmailActionRow,
    trigger_event_id: Option<i32>,
}

impl ObjectValue for EmailObject {
    fn type_name(&self) -> &'static str {
        "MonitorEmail"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_id(
                MONITOR_ACTION_EMAIL_KIND,
                self.row.id,
            ))))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.row.enabled)))),
            "includeResults" => Ok(Some(FieldValue::Json(Json::Bool(
                self.row.include_results,
            )))),
            "priority" => Ok(Some(FieldValue::Json(Json::Str(self.row.priority.clone())))),
            "header" => Ok(Some(FieldValue::Json(Json::Str(self.row.header.clone())))),
            "recipients" => {
                let after = match arguments.string("after") {
                    Some(raw) => Some(unmarshal_spec_i64(raw).map_err(ExecError::new)?),
                    None => None,
                };
                let first = arguments.i64("first");
                let rows = self
                    .resolver
                    .db
                    .list_recipients(self.row.id, first, after)
                    .map_err(ExecError::new)?;
                let next_page_cursor = rows
                    .last()
                    .map(|row| marshal_id(MONITOR_ACTION_EMAIL_RECIPIENT_KIND, row.id))
                    .unwrap_or_default();
                let mut nodes = Vec::with_capacity(rows.len());
                for row in &rows {
                    nodes.push(namespace_object(
                        self.resolver.db.as_ref(),
                        row.namespace_user_id,
                        row.namespace_org_id,
                        actor,
                    )?);
                }
                let total = self
                    .resolver
                    .db
                    .count_recipients(self.row.id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    RecipientsConnection {
                        nodes,
                        total_count: total,
                        next_page_cursor,
                    },
                ))))
            }
            "events" => {
                let connection =
                    action_event_connection(&self.resolver, ActionJobFilter {
                        email_id: Some(self.row.id),
                        trigger_event_id: self.trigger_event_id,
                        ..ActionJobFilter::default()
                    }, arguments)?;
                Ok(Some(FieldValue::Object(Arc::new(connection))))
            }
            _ => Ok(None),
        }
    }
}

/// `monitorWebhook` (`resolvers.go:1187-1241`).
pub struct WebhookObject {
    resolver: ResolverRef,
    row: WebhookActionRow,
    trigger_event_id: Option<i32>,
}

impl ObjectValue for WebhookObject {
    fn type_name(&self) -> &'static str {
        "MonitorWebhook"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_id(
                MONITOR_ACTION_WEBHOOK_KIND,
                self.row.id,
            ))))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.row.enabled)))),
            "includeResults" => Ok(Some(FieldValue::Json(Json::Bool(
                self.row.include_results,
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.row.url.clone())))),
            "events" => {
                let connection =
                    action_event_connection(&self.resolver, ActionJobFilter {
                        webhook_id: Some(self.row.id),
                        trigger_event_id: self.trigger_event_id,
                        ..ActionJobFilter::default()
                    }, arguments)?;
                Ok(Some(FieldValue::Object(Arc::new(connection))))
            }
            _ => Ok(None),
        }
    }
}

/// `monitorSlackWebhook` (`resolvers.go:1243-1297`).
pub struct SlackWebhookObject {
    resolver: ResolverRef,
    row: SlackWebhookActionRow,
    trigger_event_id: Option<i32>,
}

impl ObjectValue for SlackWebhookObject {
    fn type_name(&self) -> &'static str {
        "MonitorSlackWebhook"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_id(
                MONITOR_ACTION_SLACK_WEBHOOK_KIND,
                self.row.id,
            ))))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.row.enabled)))),
            "includeResults" => Ok(Some(FieldValue::Json(Json::Bool(
                self.row.include_results,
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.row.url.clone())))),
            "events" => {
                let connection =
                    action_event_connection(&self.resolver, ActionJobFilter {
                        slack_webhook_id: Some(self.row.id),
                        trigger_event_id: self.trigger_event_id,
                        ..ActionJobFilter::default()
                    }, arguments)?;
                Ok(Some(FieldValue::Object(Arc::new(connection))))
            }
            _ => Ok(None),
        }
    }
}

/// `monitorActionEmailRecipientsConnection` (`resolvers.go:1307-1327`).
pub struct RecipientsConnection {
    nodes: Vec<FieldValue>,
    total_count: i32,
    next_page_cursor: String,
}

impl ObjectValue for RecipientsConnection {
    fn type_name(&self) -> &'static str {
        "MonitorActionEmailRecipientsConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self.nodes.clone(),
                element_type: Some("Namespace"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total_count as i64,
            )))),
            "pageInfo" => Ok(Some(if self.nodes.is_empty() {
                FieldValue::Object(Arc::new(PageInfoObject::new(false, None)))
            } else {
                FieldValue::Object(Arc::new(PageInfoObject::new(
                    true,
                    Some(self.next_page_cursor.clone()),
                )))
            })),
            _ => Ok(None),
        }
    }
}

/// `monitorActionEventConnection` (`resolvers.go:1329-1348`).
pub struct ActionEventConnection {
    resolver: ResolverRef,
    events: Vec<ActionJobRow>,
    total_count: i32,
}

impl ObjectValue for ActionEventConnection {
    fn type_name(&self) -> &'static str {
        "MonitorActionEventConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .events
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(ActionEventObject {
                            resolver: self.resolver.clone(),
                            row: row.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("MonitorActionEvent"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count as i64)))),
            "pageInfo" => Ok(Some(match self.events.last() {
                None => FieldValue::Object(Arc::new(PageInfoObject::new(false, None))),
                Some(row) => FieldValue::Object(Arc::new(PageInfoObject::new(
                    true,
                    Some(marshal_id(MONITOR_ACTION_EMAIL_EVENT_KIND, row.id as i64)),
                ))),
            })),
            _ => Ok(None),
        }
    }
}

fn action_event_connection(
    resolver: &ResolverRef,
    filter: ActionJobFilter,
    arguments: &Arguments,
) -> Result<ActionEventConnection, ExecError> {
    let after = unmarshal_after(arguments.string("after")).map_err(ExecError::new)?;
    let first = arguments.i64("first");
    let events = resolver
        .db
        .list_action_jobs(&filter, first, after.map(i64::from))
        .map_err(ExecError::new)?;
    let total = resolver
        .db
        .count_action_jobs(&filter)
        .map_err(ExecError::new)?;
    Ok(ActionEventConnection {
        resolver: resolver.clone(),
        events,
        total_count: total,
    })
}

/// `monitorActionEvent` (`resolvers.go:1350-1377`).
pub struct ActionEventObject {
    resolver: ResolverRef,
    row: ActionJobRow,
}

impl ObjectValue for ActionEventObject {
    fn type_name(&self) -> &'static str {
        "MonitorActionEvent"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_id(
                MONITOR_ACTION_EMAIL_EVENT_KIND,
                self.row.id as i64,
            ))))),
            "status" => match state_to_status(state(&self.row.state)) {
                Some(status) => Ok(Some(FieldValue::Json(Json::Str(status.to_owned())))),
                None => Err(ExecError::new(format!(
                    "unknown state: {}",
                    state(&self.row.state)
                ))),
            },
            "message" => Ok(Some(match &self.row.failure_message {
                Some(message) => FieldValue::Json(Json::Str(message.clone())),
                None => FieldValue::Null,
            })),
            "timestamp" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row
                    .finished_at
                    .unwrap_or_else(|| self.resolver.now()),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `monitorConnection` (`resolvers.go:719-740`).
pub struct MonitorConnectionObject {
    resolver: ResolverRef,
    monitors: Vec<MonitorRow>,
    total_count: i32,
    has_next_page: bool,
}

impl ObjectValue for MonitorConnectionObject {
    fn type_name(&self) -> &'static str {
        "MonitorConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .monitors
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(MonitorObject::from_ref(
                            self.resolver.clone(),
                            row.clone(),
                        )))
                    })
                    .collect(),
                element_type: Some("Monitor"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total_count as i64,
            )))),
            "pageInfo" => Ok(Some(if self.monitors.is_empty() || !self.has_next_page {
                FieldValue::Object(Arc::new(PageInfoObject::new(false, None)))
            } else {
                FieldValue::Object(Arc::new(PageInfoObject::new(
                    true,
                    Some(marshal_id(
                        MONITOR_KIND,
                        self.monitors.last().map(|row| row.id).unwrap_or(0),
                    )),
                )))
            })),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root queries
// ---------------------------------------------------------------------------

/// `Resolver.Monitors` (`resolvers.go:70-112`).
pub fn monitors(
    resolver: &CodeMonitorsResolver,
    _actor: Actor,
    user_id: Option<i32>,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let first = arguments
        .i64("first")
        .ok_or_else(|| ExecError::new("missing first"))?;
    // Request one extra to determine if there are more pages.
    let probe = first + 1;
    let after = unmarshal_after(arguments.string("after")).map_err(ExecError::new)?;
    let rows = resolver
        .db
        .list_monitors(user_id, after.map(i64::from), Some(probe), true)
        .map_err(ExecError::new)?;
    let total = resolver
        .db
        .count_monitors(user_id, after.map(i64::from), true)
        .map_err(ExecError::new)?;
    let mut monitors = rows;
    let has_next_page = monitors.len() == (first as usize) + 1;
    if has_next_page {
        monitors.pop();
    }
    Ok(FieldValue::Object(Arc::new(MonitorConnectionObject {
        resolver: resolver.clone_like(),
        monitors,
        total_count: total,
        has_next_page,
    })))
}

/// `schemaResolver.Monitors` (`code_monitors.go:13-20`): site-admin gate, then
/// list every user's monitors.
pub fn query_monitors(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(resolver.db.as_ref(), actor)?;
    monitors(resolver, actor, None, arguments)
}

/// `UserResolver.Monitors` (`code_monitors.go:24-29` via
/// `auth.CheckSameUserFromActor`).
pub fn user_monitors(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    user_id: i32,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    if !(actor.is_internal() || actor.uid() == Some(user_id)) {
        return Err(ExecError::new(format!(
            "must be authenticated as user with id {user_id}"
        )));
    }
    monitors(resolver, actor, Some(user_id), arguments)
}

/// `Query.node` kind `CodeMonitor` (`resolvers.go:46-68,114-131`).
pub fn monitor_node(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    id: &str,
) -> Result<Option<FieldValue>, ExecError> {
    resolver.is_enabled()?;
    let monitor = resolver.monitor_by_id(actor, id)?;
    Ok(Some(FieldValue::Object(Arc::new(monitor))))
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

fn input_object<'a>(
    arguments: &'a Arguments,
    name: &str,
) -> Result<&'a std::collections::BTreeMap<String, InputValue>, ExecError> {
    match arguments.get(name) {
        Some(InputValue::Object(value)) => Ok(value),
        _ => Err(ExecError::new(format!("missing {name}"))),
    }
}

fn string_input(input: &std::collections::BTreeMap<String, InputValue>, name: &str) -> String {
    input
        .get(name)
        .and_then(InputValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn bool_input(input: &std::collections::BTreeMap<String, InputValue>, name: &str) -> bool {
    input
        .get(name)
        .and_then(InputValue::as_bool)
        .unwrap_or(false)
}

fn list_input<'a>(
    input: &'a std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Vec<&'a str> {
    match input.get(name) {
        Some(InputValue::List(values)) => {
            values.iter().filter_map(InputValue::as_str).collect()
        }
        _ => Vec::new(),
    }
}

struct ActionInput {
    email: Option<EmailActionInput>,
    webhook: Option<WebhookActionInput>,
    slack: Option<SlackWebhookActionInput>,
}

struct EmailActionInput {
    enabled: bool,
    include_results: bool,
    priority: String,
    header: String,
    recipients: Vec<String>,
}

struct WebhookActionInput {
    enabled: bool,
    include_results: bool,
    url: String,
}

struct SlackWebhookActionInput {
    enabled: bool,
    include_results: bool,
    url: String,
}

fn parse_email_input(input: &std::collections::BTreeMap<String, InputValue>) -> EmailActionInput {
    EmailActionInput {
        enabled: bool_input(input, "enabled"),
        include_results: bool_input(input, "includeResults"),
        priority: string_input(input, "priority"),
        header: string_input(input, "header"),
        recipients: list_input(input, "recipients")
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}

fn parse_webhook_input(input: &std::collections::BTreeMap<String, InputValue>) -> WebhookActionInput {
    WebhookActionInput {
        enabled: bool_input(input, "enabled"),
        include_results: bool_input(input, "includeResults"),
        url: string_input(input, "url"),
    }
}

fn parse_slack_input(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> SlackWebhookActionInput {
    SlackWebhookActionInput {
        enabled: bool_input(input, "enabled"),
        include_results: bool_input(input, "includeResults"),
        url: string_input(input, "url"),
    }
}

fn parse_action_input(value: &InputValue) -> ActionInput {
    let InputValue::Object(input) = value else {
        return ActionInput {
            email: None,
            webhook: None,
            slack: None,
        };
    };
    ActionInput {
        email: match input.get("email") {
            Some(InputValue::Object(email)) => Some(parse_email_input(email)),
            _ => None,
        },
        webhook: match input.get("webhook") {
            Some(InputValue::Object(webhook)) => Some(parse_webhook_input(webhook)),
            _ => None,
        },
        slack: match input.get("slackWebhook") {
            Some(InputValue::Object(slack)) => Some(parse_slack_input(slack)),
            _ => None,
        },
    }
}

fn action_inputs(arguments: &Arguments) -> Vec<ActionInput> {
    match arguments.get("actions") {
        Some(InputValue::List(values)) => values.iter().map(parse_action_input).collect(),
        _ => Vec::new(),
    }
}

impl CodeMonitorsResolver {
    fn create_actions(
        &self,
        actor_uid: i32,
        monitor_id: i64,
        actions: &[ActionInput],
        now: OffsetDateTime,
    ) -> Result<(), ExecError> {
        for action in actions {
            if let Some(email) = &action.email {
                let row = self
                    .db
                    .create_email_action(
                        monitor_id,
                        email.enabled,
                        email.include_results,
                        &email.priority,
                        &email.header,
                        actor_uid,
                        now,
                    )
                    .map_err(ExecError::new)?;
                self.create_recipients(row.id, &email.recipients)?;
            } else if let Some(webhook) = &action.webhook {
                self.db
                    .create_webhook_action(
                        monitor_id,
                        webhook.enabled,
                        webhook.include_results,
                        &webhook.url,
                        actor_uid,
                        now,
                    )
                    .map_err(ExecError::new)?;
            } else if let Some(slack) = &action.slack {
                validate_slack_url(&slack.url).map_err(ExecError::new)?;
                self.db
                    .create_slack_webhook_action(
                        monitor_id,
                        slack.enabled,
                        slack.include_results,
                        &slack.url,
                        actor_uid,
                        now,
                    )
                    .map_err(ExecError::new)?;
            } else {
                return Err(ExecError::new(
                    "exactly one of Email, Webhook, or SlackWebhook must be set",
                ));
            }
        }
        Ok(())
    }

    fn create_recipients(
        &self,
        email_id: i64,
        recipients: &[String],
    ) -> Result<(), ExecError> {
        for recipient in recipients {
            let namespace = unmarshal_namespace_to_ids(recipient)
                .map_err(|error| ExecError::new(format!("UnmarshalNamespaceID: {error}")))?;
            self.db
                .create_recipient(email_id, namespace.user, namespace.org)
                .map_err(ExecError::new)?;
        }
        Ok(())
    }

    fn delete_actions(
        &self,
        monitor_id: i64,
        ids: &[String],
    ) -> Result<(), ExecError> {
        let mut email = Vec::new();
        let mut webhook = Vec::new();
        let mut slack = Vec::new();
        for id in ids {
            let int_id = unmarshal_spec_i64(id).map_err(ExecError::new)?;
            match relay_kind(id).as_deref() {
                Some(MONITOR_ACTION_EMAIL_KIND) => email.push(int_id),
                Some(MONITOR_ACTION_WEBHOOK_KIND) => webhook.push(int_id),
                Some(MONITOR_ACTION_SLACK_WEBHOOK_KIND) => slack.push(int_id),
                _ => {
                    return Err(ExecError::new(
                        "action IDs must be exactly one of email, webhook, or slack webhook",
                    ))
                }
            }
        }
        self.db
            .delete_email_actions(&email, monitor_id)
            .map_err(ExecError::new)?;
        self.db
            .delete_webhook_actions(monitor_id, &webhook)
            .map_err(ExecError::new)?;
        self.db
            .delete_slack_webhook_actions(monitor_id, &slack)
            .map_err(ExecError::new)?;
        Ok(())
    }

    fn action_ids_for_monitor(&self, monitor_id: i64) -> Result<Vec<String>, ExecError> {
        let emails = self
            .db
            .list_email_actions(monitor_id)
            .map_err(ExecError::new)?;
        let webhooks = self
            .db
            .list_webhook_actions(monitor_id)
            .map_err(ExecError::new)?;
        let slacks = self
            .db
            .list_slack_webhook_actions(monitor_id)
            .map_err(ExecError::new)?;
        let mut ids = Vec::with_capacity(emails.len() + webhooks.len() + slacks.len());
        ids.extend(
            emails
                .iter()
                .map(|row| marshal_id(MONITOR_ACTION_EMAIL_KIND, row.id)),
        );
        ids.extend(
            webhooks
                .iter()
                .map(|row| marshal_id(MONITOR_ACTION_WEBHOOK_KIND, row.id)),
        );
        ids.extend(
            slacks
                .iter()
                .map(|row| marshal_id(MONITOR_ACTION_SLACK_WEBHOOK_KIND, row.id)),
        );
        Ok(ids)
    }

    fn update_code_monitor(
        &self,
        actor: Actor,
        arguments: &Arguments,
        now: OffsetDateTime,
    ) -> Result<MonitorObject, ExecError> {
        let monitor_input = input_object(arguments, "monitor")?;
        let trigger_input = input_object(arguments, "trigger")?;
        let monitor_id = unmarshal_monitor_id(
            monitor_input
                .get("id")
                .and_then(InputValue::as_str)
                .unwrap_or_default(),
        )
        .map_err(ExecError::new)?;
        let update = match monitor_input.get("update") {
            Some(InputValue::Object(update)) => update,
            _ => return Err(ExecError::new("missing monitor.update")),
        };
        let namespace = unmarshal_namespace_to_ids(&string_input(update, "namespace"))
            .map_err(ExecError::new)?;

        // The baseline runs the snapshot inside the update transaction but
        // takes it against `rawDB`; an error rolls the transaction back. The
        // Rust adapter is per-statement (M9.13a deferral), so validate and
        // resolve the snapshot before any write to keep the same observable
        // error semantics: a failed snapshot leaves `cm_*` untouched.
        let trigger_id = unmarshal_spec_i64(
            trigger_input
                .get("id")
                .and_then(InputValue::as_str)
                .unwrap_or_default(),
        )
        .map_err(ExecError::new)?;
        let trigger_update = match trigger_input.get("update") {
            Some(InputValue::Object(update)) => update,
            _ => return Err(ExecError::new("missing trigger.update")),
        };
        let new_query = string_input(trigger_update, "query");
        let current = self
            .db
            .get_query_trigger_for_monitor(monitor_id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(sql_no_rows()))?;
        let resolved = if current.query_string != new_query {
            self.snapshot
                .snapshot(
                    &new_query,
                    crate::search_contexts::RequestContext::from_graphql_actor(actor, false),
                )
                .map_err(ExecError::new)?
        } else {
            BTreeMap::new()
        };

        let row = self
            .db
            .update_monitor(
                monitor_id,
                &string_input(update, "description"),
                bool_input(update, "enabled"),
                namespace.user,
                namespace.org,
                actor.uid().unwrap_or(0),
                now,
            )
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(sql_no_rows()))?;

        for (repo_id, commit_ids) in resolved {
            self.db
                .upsert_last_searched(monitor_id, repo_id, &commit_ids)
                .map_err(ExecError::new)?;
        }
        self.db
            .update_query_trigger(
                trigger_id,
                &new_query,
                actor.uid().unwrap_or(0),
                now,
            )
            .map_err(ExecError::new)?;

        let mut updated_actions = Vec::new();
        if let Some(InputValue::List(values)) = arguments.get("actions") {
            for value in values {
                let InputValue::Object(action) = value else {
                    continue;
                };
                if let Some(InputValue::Object(email)) = action.get("email") {
                    let email_id = unmarshal_email_id(
                        email
                            .get("id")
                            .and_then(InputValue::as_str)
                            .unwrap_or_default(),
                    )
                    .map_err(ExecError::new)?;
                    let Some(InputValue::Object(update)) = email.get("update") else {
                        continue;
                    };
                    let parsed = parse_email_input(update);
                    self.db
                        .delete_recipients(email_id)
                        .map_err(ExecError::new)?;
                    let row = self
                        .db
                        .update_email_action(
                            email_id,
                            parsed.enabled,
                            parsed.include_results,
                            &parsed.priority,
                            &parsed.header,
                            actor.uid().unwrap_or(0),
                            now,
                        )
                        .map_err(ExecError::new)?
                        .ok_or_else(|| ExecError::new(sql_no_rows()))?;
                    self.create_recipients(row.id, &parsed.recipients)?;
                    updated_actions.push(row.id);
                } else if let Some(InputValue::Object(webhook)) = action.get("webhook") {
                    let id = unmarshal_spec_i64(
                        webhook
                            .get("id")
                            .and_then(InputValue::as_str)
                            .unwrap_or_default(),
                    )
                    .map_err(ExecError::new)?;
                    let Some(InputValue::Object(update)) = webhook.get("update") else {
                        continue;
                    };
                    let parsed = parse_webhook_input(update);
                    self.db
                        .update_webhook_action(
                            id,
                            parsed.enabled,
                            parsed.include_results,
                            &parsed.url,
                            actor.uid().unwrap_or(0),
                            now,
                        )
                        .map_err(ExecError::new)?
                        .ok_or_else(|| ExecError::new(sql_no_rows()))?;
                    updated_actions.push(id);
                } else if let Some(InputValue::Object(slack)) = action.get("slackWebhook") {
                    let id = unmarshal_spec_i64(
                        slack
                            .get("id")
                            .and_then(InputValue::as_str)
                            .unwrap_or_default(),
                    )
                    .map_err(ExecError::new)?;
                    let Some(InputValue::Object(update)) = slack.get("update") else {
                        continue;
                    };
                    let parsed = parse_slack_input(update);
                    validate_slack_url(&parsed.url).map_err(ExecError::new)?;
                    self.db
                        .update_slack_webhook_action(
                            id,
                            parsed.enabled,
                            parsed.include_results,
                            &parsed.url,
                            actor.uid().unwrap_or(0),
                            now,
                        )
                        .map_err(ExecError::new)?
                        .ok_or_else(|| ExecError::new(sql_no_rows()))?;
                    updated_actions.push(id);
                } else {
                    return Err(ExecError::new(
                        "action must be one of email, webhook, or slack webhook",
                    ));
                }
            }
        }
        let _ = updated_actions;
        Ok(MonitorObject::from_ref(self.clone_like(), row))
    }
}

/// `splitActionIDs` (`resolvers.go:481-528`).
///
/// Returns the create/update/delete buckets. `Err` is the baseline's
/// "unknown ID" error, which `UpdateCodeMonitor` accidentally drops; the
/// caller preserves that.
fn split_action_ids(
    arguments: &Arguments,
    action_ids: &[String],
) -> Result<(Vec<ActionInput>, Vec<String>), ExecError> {
    let known: std::collections::HashSet<&str> =
        action_ids.iter().map(String::as_str).collect();
    let mut to_create = Vec::new();
    let mut to_update = Vec::new();
    let mut remaining = known.clone();
    if let Some(InputValue::List(values)) = arguments.get("actions") {
        for value in values {
            let InputValue::Object(action) = value else {
                continue;
            };
            if let Some(InputValue::Object(email)) = action.get("email") {
                let Some(id) = email.get("id").and_then(InputValue::as_str) else {
                    if let Some(InputValue::Object(update)) = email.get("update") {
                        to_create.push(ActionInput {
                            email: Some(parse_email_input(update)),
                            webhook: None,
                            slack: None,
                        });
                    }
                    continue;
                };
                if !known.contains(id) {
                    return Err(ExecError::new(format!("unknown ID={id} for action")));
                }
                to_update.push(action.clone());
                remaining.remove(id);
            } else if let Some(InputValue::Object(webhook)) = action.get("webhook") {
                let Some(id) = webhook.get("id").and_then(InputValue::as_str) else {
                    if let Some(InputValue::Object(update)) = webhook.get("update") {
                        to_create.push(ActionInput {
                            email: None,
                            webhook: Some(parse_webhook_input(update)),
                            slack: None,
                        });
                    }
                    continue;
                };
                if !known.contains(id) {
                    return Err(ExecError::new(format!("unknown ID={id} for action")));
                }
                to_update.push(action.clone());
                remaining.remove(id);
            } else if let Some(InputValue::Object(slack)) = action.get("slackWebhook") {
                let Some(id) = slack.get("id").and_then(InputValue::as_str) else {
                    if let Some(InputValue::Object(update)) = slack.get("update") {
                        to_create.push(ActionInput {
                            email: None,
                            webhook: None,
                            slack: Some(parse_slack_input(update)),
                        });
                    }
                    continue;
                };
                if !known.contains(id) {
                    return Err(ExecError::new(format!("unknown ID={id} for action")));
                }
                to_update.push(action.clone());
                remaining.remove(id);
            }
        }
    }
    let mut to_delete: Vec<String> = remaining.into_iter().map(str::to_owned).collect();
    to_delete.sort();
    // The caller ignores the error; returning the buckets computed so far
    // matches the Go control flow (nil buckets on the error path).
    let _ = &to_update;
    Ok((to_create, to_delete))
}

/// `CreateCodeMonitor` (`resolvers.go:133-196`).
pub fn create_code_monitor(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let monitor_input = input_object(arguments, "monitor")?;
    let namespace_id = string_input(monitor_input, "namespace");
    is_allowed_to_create(resolver.db.as_ref(), actor, &namespace_id)?;
    let namespace = unmarshal_namespace_to_ids(&namespace_id).map_err(ExecError::new)?;
    let trigger_input = input_object(arguments, "trigger")?;
    let query = string_input(trigger_input, "query");
    // `codemonitors.Snapshot(ctx, ...)` runs with the request actor.
    let resolved = resolver
        .snapshot
        .snapshot(
            &query,
            crate::search_contexts::RequestContext::from_graphql_actor(actor, false),
        )
        .map_err(ExecError::new)?;
    let now = resolver.now();
    let monitor = resolver
        .db
        .create_monitor(
            &string_input(monitor_input, "description"),
            bool_input(monitor_input, "enabled"),
            namespace.user,
            namespace.org,
            actor.uid().unwrap_or(0),
            now,
        )
        .map_err(ExecError::new)?;
    resolver
        .db
        .create_query_trigger(monitor.id, &query, actor.uid().unwrap_or(0), now)
        .map_err(ExecError::new)?;
    for (repo_id, commit_ids) in &resolved {
        resolver
            .db
            .upsert_last_searched(monitor.id, *repo_id, commit_ids)
            .map_err(ExecError::new)?;
    }
    resolver.create_actions(
        actor.uid().unwrap_or(0),
        monitor.id,
        &action_inputs(arguments),
        now,
    )?;
    Ok(FieldValue::Object(Arc::new(MonitorObject::from_ref(
        resolver.clone_like(),
        monitor,
    ))))
}

/// `ToggleCodeMonitor` (`resolvers.go:198-213`).
pub fn toggle_code_monitor(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    resolver
        .is_allowed_to_edit(actor, id)
        .map_err(|error| ExecError::new(format!("UpdateMonitorEnabled: {}", error.message)))?;
    let monitor_id = unmarshal_monitor_id(id).map_err(ExecError::new)?;
    let row = resolver
        .db
        .update_monitor_enabled(
            monitor_id,
            arguments.bool("enabled").unwrap_or(false),
            actor.uid().unwrap_or(0),
            resolver.now(),
        )
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new(sql_no_rows()))?;
    Ok(FieldValue::Object(Arc::new(MonitorObject::from_ref(
        resolver.clone_like(),
        row,
    ))))
}

/// `DeleteCodeMonitor` (`resolvers.go:215-230`).
pub fn delete_code_monitor(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    resolver
        .is_allowed_to_edit(actor, id)
        .map_err(|error| ExecError::new(format!("DeleteCodeMonitor: {}", error.message)))?;
    let monitor_id = unmarshal_monitor_id(id).map_err(ExecError::new)?;
    resolver
        .db
        .delete_monitor(monitor_id)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// `UpdateCodeMonitor` (`resolvers.go:232-283`).
pub fn update_code_monitor(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let monitor_input = input_object(arguments, "monitor")?;
    let monitor_id_raw = monitor_input
        .get("id")
        .and_then(InputValue::as_str)
        .unwrap_or_default();
    resolver
        .is_allowed_to_edit(actor, monitor_id_raw)
        .map_err(|error| ExecError::new(format!("UpdateCodeMonitor: {}", error.message)))?;
    let update = match monitor_input.get("update") {
        Some(InputValue::Object(update)) => update,
        _ => return Err(ExecError::new("missing monitor.update")),
    };
    let namespace_id = string_input(update, "namespace");
    is_allowed_to_create(resolver.db.as_ref(), actor, &namespace_id)
        .map_err(|error| ExecError::new(format!("update namespace: {}", error.message)))?;
    let monitor_id = unmarshal_monitor_id(monitor_id_raw).map_err(ExecError::new)?;
    let action_ids = resolver.action_ids_for_monitor(monitor_id)?;
    // `splitActionIDs` errors are intentionally ignored by the baseline.
    let (to_create, to_delete) = split_action_ids(arguments, &action_ids).unwrap_or_default();
    if to_delete.len() == action_ids.len() && to_create.is_empty() {
        return Err(ExecError::new(
            "you tried to delete all actions, but every monitor must be connected to at least 1 action",
        ));
    }
    let today = resolver.now();
    resolver.delete_actions(monitor_id, &to_delete)?;
    resolver.create_actions(actor.uid().unwrap_or(0), monitor_id, &to_create, today)?;
    let monitor = resolver.update_code_monitor(actor, arguments, today)?;
    Ok(FieldValue::Object(Arc::new(monitor)))
}

/// `ResetTriggerQueryTimestamps` (`resolvers.go:373-392`).
pub fn reset_trigger_query_timestamps(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(resolver.db.as_ref(), actor)?;
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    let query_id = unmarshal_spec_i64(id).map_err(ExecError::new)?;
    resolver
        .db
        .reset_query_trigger_timestamps(query_id, resolver.now())
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// `sendTestEmail` (`resolvers.go:435-450`).
fn send_test_email(
    resolver: &CodeMonitorsResolver,
    recipient: &str,
    description: &str,
) -> Result<(), ExecError> {
    let namespace = unmarshal_namespace_to_ids(recipient).map_err(ExecError::new)?;
    let _ = namespace.user;
    match namespace.org {
        Some(org) if org != 0 => Ok(()),
        _ => {
            let user_id = namespace
                .user
                .ok_or_else(|| ExecError::new("missing user"))?;
            resolver
                .test_sender
                .send_test_email(user_id, description)
                .map_err(ExecError::new)
        }
    }
}

/// `TriggerTestEmailAction` (`resolvers.go:394-407`).
pub fn trigger_test_email_action(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let namespace = arguments
        .string("namespace")
        .ok_or_else(|| ExecError::new("missing namespace"))?;
    is_allowed_to_create(resolver.db.as_ref(), actor, namespace)?;
    let description = arguments.string("description").unwrap_or_default();
    let email = input_object(arguments, "email")?;
    for recipient in list_input(email, "recipients") {
        send_test_email(resolver, recipient, description)?;
    }
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// `TriggerTestWebhookAction` (`resolvers.go:409-420`).
pub fn trigger_test_webhook_action(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let namespace = arguments
        .string("namespace")
        .ok_or_else(|| ExecError::new("missing namespace"))?;
    is_allowed_to_create(resolver.db.as_ref(), actor, namespace)?;
    let webhook = input_object(arguments, "webhook")?;
    resolver
        .test_sender
        .send_test_webhook(
            arguments.string("description").unwrap_or_default(),
            &string_input(webhook, "url"),
        )
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// `TriggerTestSlackWebhookAction` (`resolvers.go:422-433`).
pub fn trigger_test_slack_webhook_action(
    resolver: &CodeMonitorsResolver,
    actor: Actor,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let namespace = arguments
        .string("namespace")
        .ok_or_else(|| ExecError::new("missing namespace"))?;
    is_allowed_to_create(resolver.db.as_ref(), actor, namespace)?;
    let slack = input_object(arguments, "slackWebhook")?;
    resolver
        .test_sender
        .send_test_slack_webhook(
            arguments.string("description").unwrap_or_default(),
            &string_input(slack, "url"),
        )
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// Dispatch one root field owned by this surface.
pub fn root_field(
    resolver: &CodeMonitorsResolver,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "monitors" => query_monitors(resolver, actor, arguments)?,
        "createCodeMonitor" => create_code_monitor(resolver, actor, arguments)?,
        "toggleCodeMonitor" => toggle_code_monitor(resolver, actor, arguments)?,
        "deleteCodeMonitor" => delete_code_monitor(resolver, actor, arguments)?,
        "updateCodeMonitor" => update_code_monitor(resolver, actor, arguments)?,
        "resetTriggerQueryTimestamps" => {
            reset_trigger_query_timestamps(resolver, actor, arguments)?
        }
        "triggerTestEmailAction" => trigger_test_email_action(resolver, actor, arguments)?,
        "triggerTestWebhookAction" => trigger_test_webhook_action(resolver, actor, arguments)?,
        "triggerTestSlackWebhookAction" => {
            trigger_test_slack_webhook_action(resolver, actor, arguments)?
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// All root fields owned by this surface.
pub const ROOT_FIELDS: &[&str] = &[
    "monitors",
    "createCodeMonitor",
    "toggleCodeMonitor",
    "deleteCodeMonitor",
    "updateCodeMonitor",
    "resetTriggerQueryTimestamps",
    "triggerTestEmailAction",
    "triggerTestWebhookAction",
    "triggerTestSlackWebhookAction",
];

pub fn is_root_field(field: &str) -> bool {
    ROOT_FIELDS.contains(&field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slack_url_validation_matches_baseline() {
        assert!(validate_slack_url("https://hooks.slack.com/services/x").is_ok());
        assert!(validate_slack_url("http://hooks.slack.com/services/x").is_err());
        assert!(validate_slack_url("https://example.com/services/x").is_err());
        assert!(validate_slack_url("not a url").is_err());
        assert_eq!(
            validate_slack_url("https://example.com/x").unwrap_err(),
            "slack webhook URL must begin with 'https://hooks.slack.com/"
        );
    }

    #[test]
    fn result_count_counts_highlights() {
        assert_eq!(result_count(&serde_json::json!([])), 0);
        assert_eq!(
            result_count(&serde_json::json!([{"MessagePreview": {"content": "x"}}])),
            1
        );
        assert_eq!(
            result_count(&serde_json::json!([
                {"DiffPreview": {"content": "x", "matchedRanges": [1, 2]}}
            ])),
            2
        );
    }
}
