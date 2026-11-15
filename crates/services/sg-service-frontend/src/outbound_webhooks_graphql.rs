#![forbid(unsafe_code)]

//! Outbound webhook GraphQL surface (M9.27b).
//!
//! Ports `cmd/frontend/graphqlbackend/outbound_webhooks.go` on the vendored
//! `outbound_webhooks.graphql` document (already joined into `assemble_oss()`):
//!
//! - `Query.outboundWebhooks(first, after, eventType, scope)` — site-admin
//!   only; integer `after` cursor, event-type/scope filter;
//! - `Query.outboundWebhookEventTypes` — site-admin only, sorted by key;
//! - `Mutation.createOutboundWebhook` / `updateOutboundWebhook` /
//!   `deleteOutboundWebhook`;
//! - `Query.node` kind `OutboundWebhook`.
//!
//! Authorization, validation and error strings follow the baseline verbatim:
//!
//! - queries and `delete` use `auth.CheckCurrentUserIsSiteAdmin`
//!   (`not authenticated` / `must be site admin`);
//! - `create`/`update` use `auth.CurrentUser` and dereference `user.SiteAdmin`,
//!   so a missing user (anonymous or internal actor) reproduces the baseline's
//!   nil-pointer panic (`panic occurred: runtime error: invalid memory address
//!   or nil pointer dereference`);
//! - `create` validates the URL through `outbound.CheckURL` (scheme `http*`,
//!   non-empty host, DNS resolution, loopback / `169.254.169.254` denial);
//!   `update` deliberately does **not** validate it;
//! - `create`/`update` store at least one event type
//!   (`an outbound webhook must have at least one event type`) and never check
//!   the key against the registry;
//! - `node(OutboundWebhook)` resolves lazily: `{ id }` succeeds for a missing
//!   row, while `url`/`eventTypes` report
//!   `outbound webhook not found: [<id>]`.
//!
//! The database is injected through [`OutboundWebhooksGraphqlDatabase`], a
//! synchronous seam in the same style as the GitHub Apps resolver; the
//! production adapter ([`crate::outbound_webhooks_store`]) bridges to sqlx.
//! M9.46a adds `OutboundWebhook.stats` and `OutboundWebhook.logs` (with the
//! `OutboundWebhookLog` / `OutboundWebhookJob` nodes), reading the existing
//! `outbound_webhook_logs` / `outbound_webhook_jobs` tables. No table, column
//! or Redis key is added.

use std::net::{IpAddr, ToSocketAddrs};
use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{datetime_rfc3339, PageInfoObject};
use crate::webhook_logs::{decode_message, WebhookLogMessageObject, WebhookLogRequestObject};

/// `outboundWebhookIDKind` (`outbound_webhooks.go:22`).
pub const OUTBOUND_WEBHOOK_KIND: &str = "OutboundWebhook";
/// `outboundWebhookLogIDKind` (`outbound_webhook_logs.go:16`).
pub const OUTBOUND_WEBHOOK_LOG_KIND: &str = "OutboundWebhookLog";
/// `outboundWebhookJobIDKind` (`outbound_webhook_logs.go:15`).
pub const OUTBOUND_WEBHOOK_JOB_KIND: &str = "OutboundWebhookJob";
/// `types.OutboundWebhookLogUnsentStatusCode`.
pub const OUTBOUND_WEBHOOK_LOG_UNSENT_STATUS_CODE: i32 = 0;

/// `auth.ErrNotAuthenticated`.
const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.ErrMustBeSiteAdmin`.
const MUST_BE_SITE_ADMIN: &str = "must be site admin";
/// graph-gophers' default panic error for the baseline's `user.SiteAdmin`
/// dereference when `auth.CurrentUser` returns `(nil, nil)`.
const PANIC_NIL_USER: &str =
    "panic occurred: runtime error: invalid memory address or nil pointer dereference";
/// `errIllegalAddr` (`outbound.go:55`).
const ERR_ILLEGAL_ADDR: &str = "Address must not be private, link-local or loopback";

// ---------------------------------------------------------------------------
// Data and the database seam
// ---------------------------------------------------------------------------

/// `types.OutboundWebhookEventType` projected to the GraphQL fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedEventTypeData {
    pub event_type: String,
    pub scope: Option<String>,
}

/// `types.OutboundWebhook`: `url` stays raw (`bytea`); decryption is lazy,
/// exactly like the Go store's `NewEncrypted(...)` scanner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookData {
    pub id: i64,
    pub url: Vec<u8>,
    pub encryption_key_id: String,
    pub event_types: Vec<ScopedEventTypeData>,
}

/// `database.FilterEventType`, re-exported shape used by the seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterEventType {
    pub event_type: String,
    /// `None` matches any scope; [`FILTER_EVENT_TYPE_NO_SCOPE`] matches NULL.
    pub scope: Option<String>,
}

/// `database.FilterEventTypeNoScope`.
pub const FILTER_EVENT_TYPE_NO_SCOPE: &str = "RESERVED_KEYWORD_MATCH_NULL_SCOPE";

/// The operations the resolver needs (`database.OutboundWebhookStore`).
pub trait OutboundWebhooksGraphqlDatabase: Send + Sync {
    /// `auth.CurrentUser`'s `site_admin` flag: `None` when there is no user
    /// (unauthenticated actor or unknown/deleted UID).
    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String>;

    /// `OutboundWebhookStore.List` (`limit` includes the `+1` probe).
    fn list_webhooks(
        &self,
        opts: &ListOpts,
    ) -> Result<Vec<WebhookData>, String>;

    /// `OutboundWebhookStore.Count`.
    fn count_webhooks(&self, opts: &CountOpts) -> Result<i64, String>;

    /// `OutboundWebhookStore.GetByID`: `None` is `OutboundWebhookNotFoundErr`.
    fn get_webhook(&self, id: i64) -> Result<Option<WebhookData>, String>;

    /// `OutboundWebhookStore.Create`.
    fn create_webhook(
        &self,
        created_by: i32,
        updated_by: i32,
        url: &str,
        secret: &str,
        event_types: &[ScopedEventTypeData],
    ) -> Result<WebhookData, String>;

    /// `OutboundWebhookStore.Update`: `None` when the row does not exist.
    /// The secret is preserved from the stored row, as in the baseline.
    fn update_webhook(
        &self,
        id: i64,
        updated_by: i32,
        url: &str,
        event_types: &[ScopedEventTypeData],
    ) -> Result<Option<WebhookData>, String>;

    /// `OutboundWebhookStore.Delete`: a missing row is not an error.
    fn delete_webhook(&self, id: i64) -> Result<(), String>;

    /// `(*encryption.Encryptable).Decrypt` for `url` (`MaybeDecrypt`).
    fn decrypt_url(&self, raw: &[u8], encryption_key_id: &str) -> Result<String, String>;

    /// `OutboundWebhookLogStore.CountsForOutboundWebhook`: `(total, errored)`.
    fn counts_for_outbound_webhook(&self, id: i64) -> Result<(i64, i64), String>;

    /// `OutboundWebhookLogStore.ListForOutboundWebhook` (`limit` includes the
    /// `+1` probe).
    fn list_logs(&self, opts: &LogListOpts) -> Result<Vec<OutboundWebhookLogData>, String>;

    /// `OutboundWebhookJobStore.GetByID`: `None` is
    /// `OutboundWebhookJobNotFoundErr`.
    fn get_job(&self, id: i64) -> Result<Option<OutboundWebhookJobData>, String>;

    /// `(*encryption.Encryptable).Decrypt` for any encrypted `bytea` column on
    /// this surface (log request/response/error, job payload). They all use
    /// `keyring.Default().OutboundWebhookKey`, so this defaults to
    /// [`Self::decrypt_url`].
    fn decrypt_outbound_value(
        &self,
        raw: &[u8],
        encryption_key_id: &str,
    ) -> Result<String, String> {
        self.decrypt_url(raw, encryption_key_id)
    }
}

/// `database.OutboundWebhookCountOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CountOpts {
    pub event_types: Vec<FilterEventType>,
}

/// `database.OutboundWebhookListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListOpts {
    pub count: CountOpts,
    pub limit: i64,
    pub offset: i64,
}

/// `types.OutboundWebhookLog` with raw encrypted columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhookLogData {
    pub id: i64,
    pub job_id: i64,
    pub sent_at: OffsetDateTime,
    pub status_code: i32,
    pub encryption_key_id: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
    pub error: Vec<u8>,
}

/// `types.OutboundWebhookJob` projected to the GraphQL fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhookJobData {
    pub id: i64,
    pub event_type: String,
    pub scope: Option<String>,
    pub encryption_key_id: String,
    pub payload: Vec<u8>,
}

/// `database.OutboundWebhookLogListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogListOpts {
    pub limit: i64,
    pub offset: i64,
    pub only_errors: bool,
    pub outbound_webhook_id: i64,
}

pub type OutboundWebhooksHandle = Arc<dyn OutboundWebhooksGraphqlDatabase + Send + Sync>;

// ---------------------------------------------------------------------------
// Authorization helpers
// ---------------------------------------------------------------------------

/// `auth.CheckCurrentUserIsSiteAdmin` (`site_admin.go:19-34`).
pub fn check_current_user_is_site_admin(
    db: &dyn OutboundWebhooksGraphqlDatabase,
    actor: Actor,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    match db.current_user_site_admin(actor)? {
        None => Err(NOT_AUTHENTICATED.to_owned()),
        Some(false) => Err(MUST_BE_SITE_ADMIN.to_owned()),
        Some(true) => Ok(()),
    }
}

/// `auth.CurrentUser` + `if !user.SiteAdmin` in
/// `CreateOutboundWebhook`/`UpdateOutboundWebhook`: a nil user panics in the
/// baseline (graph-gophers converts the panic to an error).
pub fn require_current_user_is_site_admin(
    db: &dyn OutboundWebhooksGraphqlDatabase,
    actor: Actor,
) -> Result<(), String> {
    if actor.is_internal() {
        // `GetByCurrentAuthUser` returns `ErrNoCurrentUser` (internal actors
        // are not authenticated), so `CurrentUser` is nil.
        return Err(PANIC_NIL_USER.to_owned());
    }
    match db.current_user_site_admin(actor)? {
        None => Err(PANIC_NIL_USER.to_owned()),
        Some(false) => Err(MUST_BE_SITE_ADMIN.to_owned()),
        Some(true) => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Relay IDs
// ---------------------------------------------------------------------------

/// `relay.MarshalID(outboundWebhookIDKind, id)`.
pub fn marshal_outbound_webhook_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{OUTBOUND_WEBHOOK_KIND}:{id}"))
}

/// `marshalOutboundWebhookLogID` (`outbound_webhook_logs.go:252`).
pub fn marshal_outbound_webhook_log_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{OUTBOUND_WEBHOOK_LOG_KIND}:{id}"))
}

/// `marshalOutboundWebhookJobID` (`outbound_webhook_logs.go:248`).
pub fn marshal_outbound_webhook_job_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{OUTBOUND_WEBHOOK_JOB_KIND}:{id}"))
}

/// `unmarshalOutboundWebhookID` (`outbound_webhooks.go:232-239`): kind
/// validation first, then the JSON integer spec.
pub fn unmarshal_outbound_webhook_id(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(raw);
    let kind = decoded
        .as_ref()
        .ok()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .and_then(|text| text.split_once(':').map(|(kind, _)| kind.to_owned()))
        .unwrap_or_default();
    if kind != OUTBOUND_WEBHOOK_KIND {
        return Err(format!(
            "invalid outbound webhook id of kind {}",
            go_quote(&kind)
        ));
    }
    let text = String::from_utf8_lossy(decoded.as_ref().expect("checked above")).into_owned();
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str::<i64>(spec).map_err(|error| error.to_string())
}

/// `OutboundWebhookNotFoundErr.Error()`: `outbound webhook not found: [<id>]`.
pub fn not_found(id: i64) -> String {
    format!("outbound webhook not found: [{id}]")
}

/// Go's `fmt.Sprintf("%q", value)` for the short ASCII strings used here.
fn go_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 || ch == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// URL validation (`outbound.CheckURL`, `outbound.go:109-160`)
// ---------------------------------------------------------------------------

/// `outbound.CheckURL`: parse, require an `http*` scheme and a host, then
/// reject loopback / `169.254.169.254` literals (and hosts resolving to them).
pub fn check_url(dest: &str) -> Result<(), String> {
    let parsed = match url::Url::parse(dest) {
        Ok(parsed) => parsed,
        Err(_) => {
            // `url.Parse` only fails for genuinely malformed absolute URLs;
            // schemeless strings parse as relative paths (empty scheme/host)
            // and fall through to "Unsupported URL".
            if dest.contains(':') {
                return Err(format!("Could not parse provided URL {dest}"));
            }
            return Err(format!("Unsupported URL provided {dest}"));
        }
    };

    let scheme = parsed.scheme();
    let host = parsed.host_str().unwrap_or_default();
    if !scheme.starts_with("http") || host.is_empty() {
        return Err(format!("Unsupported URL provided {dest}"));
    }

    match parsed.host() {
        Some(url::Host::Ipv4(ip)) => check_ip(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => check_ip(IpAddr::V6(ip)),
        _ => {
            let addrs = (host, 0u16)
                .to_socket_addrs()
                .map_err(|_| "Could not resolve hostname".to_owned())?;
            let mut resolved = false;
            for addr in addrs {
                resolved = true;
                check_ip(addr.ip())?;
            }
            if !resolved {
                return Err("Could not resolve hostname".to_owned());
            }
            Ok(())
        }
    }
}

/// `isIllegalIP` with the baseline denylist (`loopback` builtin plus the
/// `169.254.169.254` pattern).
fn check_ip(ip: IpAddr) -> Result<(), String> {
    let denied = match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback() || ip == std::net::Ipv4Addr::new(169, 254, 169, 254)
        }
        IpAddr::V6(ip) => ip.is_loopback(),
    };
    if denied {
        Err(ERR_ILLEGAL_ADDR.to_owned())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Root fields
// ---------------------------------------------------------------------------

/// Whether `field` is owned by this surface.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "outboundWebhooks" | "outboundWebhookEventTypes")
}

/// `schemaResolver.OutboundWebhooks` / `OutboundWebhookEventTypes`
/// (`outbound_webhooks.go:89-128`).
pub fn root_field(
    db: &OutboundWebhooksHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "outboundWebhooks" => outbound_webhooks(db, arguments, actor)?,
        "outboundWebhookEventTypes" => outbound_webhook_event_types(db, actor)?,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn outbound_webhooks(
    db: &OutboundWebhooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;

    let first = arguments.i64("first").unwrap_or(50);
    let mut offset = 0i64;
    if let Some(after) = arguments.string("after") {
        offset = after.parse::<i64>().map_err(|_| {
            ExecError::new(format!("cannot parse offset {}", go_quote(after)))
        })?;
    }

    let mut count = CountOpts::default();
    if let Some(event_type) = arguments.string("eventType") {
        count.event_types = vec![FilterEventType {
            event_type: event_type.to_owned(),
            scope: arguments.string("scope").map(str::to_owned),
        }];
    }

    let listed = db
        .list_webhooks(&ListOpts {
            count: count.clone(),
            limit: first + 1,
            offset,
        })
        .map_err(ExecError::new)?;
    let total_count = db.count_webhooks(&count).map_err(ExecError::new)?;

    let has_next_page = listed.len() as i64 > first;
    let nodes = if has_next_page {
        listed[..first.max(0) as usize].to_vec()
    } else {
        listed
    };
    let end_cursor = has_next_page.then(|| (first + offset).to_string());

    Ok(FieldValue::Object(Arc::new(OutboundWebhookConnectionObject {
        nodes,
        total_count,
        has_next_page,
        end_cursor,
        db: db.clone(),
    })))
}

fn outbound_webhook_event_types(
    db: &OutboundWebhooksHandle,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;

    let mut event_types = sg_extsvc::batches_webhooks::EVENT_TYPES.to_vec();
    event_types.sort_by(|left, right| left.0.cmp(right.0));
    Ok(FieldValue::List {
        items: event_types
            .into_iter()
            .map(|(key, description)| {
                FieldValue::Object(Arc::new(OutboundWebhookEventTypeObject {
                    key: key.to_owned(),
                    description: description.to_owned(),
                }))
            })
            .collect(),
        element_type: Some("OutboundWebhookEventType"),
    })
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/// Whether `field` is one of the outbound webhook mutations.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createOutboundWebhook" | "updateOutboundWebhook" | "deleteOutboundWebhook"
    )
}

/// `schemaResolver.CreateOutboundWebhook` / `UpdateOutboundWebhook` /
/// `DeleteOutboundWebhook` (`outbound_webhooks.go:130-213`).
pub fn mutation_field(
    db: &OutboundWebhooksHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "createOutboundWebhook" => create_outbound_webhook(db, arguments, actor)?,
        "updateOutboundWebhook" => update_outbound_webhook(db, arguments, actor)?,
        "deleteOutboundWebhook" => delete_outbound_webhook(db, arguments, actor)?,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// `OutboundWebhookCreateInput` / `OutboundWebhookUpdateInput` bindings.
struct WebhookInput {
    url: String,
    secret: String,
    event_types: Vec<ScopedEventTypeData>,
}

fn parse_event_types(value: Option<&InputValue>) -> Vec<ScopedEventTypeData> {
    let Some(InputValue::List(items)) = value else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| match item {
            InputValue::Object(fields) => {
                let event_type = fields
                    .get("eventType")
                    .and_then(InputValue::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let scope = match fields.get("scope") {
                    Some(InputValue::Str(scope)) => Some(scope.clone()),
                    _ => None,
                };
                Some(ScopedEventTypeData { event_type, scope })
            }
            _ => None,
        })
        .collect()
}

fn parse_input(value: Option<&InputValue>) -> WebhookInput {
    let fields = match value {
        Some(InputValue::Object(fields)) => fields,
        _ => return WebhookInput {
            url: String::new(),
            secret: String::new(),
            event_types: Vec::new(),
        },
    };
    WebhookInput {
        url: fields
            .get("url")
            .and_then(InputValue::as_str)
            .unwrap_or_default()
            .to_owned(),
        secret: fields
            .get("secret")
            .and_then(InputValue::as_str)
            .unwrap_or_default()
            .to_owned(),
        event_types: parse_event_types(fields.get("eventTypes")),
    }
}

fn create_outbound_webhook(
    db: &OutboundWebhooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    require_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let input = parse_input(arguments.get("input"));

    check_url(&input.url)
        .map_err(|error| ExecError::new(format!("invalid webhook address: {error}")))?;

    let user_id = actor.uid().unwrap_or(0);
    let webhook = db
        .create_webhook(
            user_id,
            user_id,
            &input.url,
            &input.secret,
            &input.event_types,
        )
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(OutboundWebhookObject {
        id: webhook.id,
        data: Some(webhook),
        db: db.clone(),
    })))
}

fn update_outbound_webhook(
    db: &OutboundWebhooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    require_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let id = unmarshal_outbound_webhook_id(arguments.string("id").unwrap_or_default())
        .map_err(ExecError::new)?;
    let input = parse_input(arguments.get("input"));

    let user_id = actor.uid().unwrap_or(0);
    let webhook = db
        .update_webhook(id, user_id, &input.url, &input.event_types)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new(not_found(id)))?;
    Ok(FieldValue::Object(Arc::new(OutboundWebhookObject {
        id: webhook.id,
        data: Some(webhook),
        db: db.clone(),
    })))
}

fn delete_outbound_webhook(
    db: &OutboundWebhooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let id = unmarshal_outbound_webhook_id(arguments.string("id").unwrap_or_default())
        .map_err(ExecError::new)?;
    db.delete_webhook(id).map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

// ---------------------------------------------------------------------------
// `Query.node`
// ---------------------------------------------------------------------------

/// Whether `kind` is the outbound webhook node kind.
pub fn is_node_kind(kind: &str) -> bool {
    kind == OUTBOUND_WEBHOOK_KIND
}

/// `OutboundWebhookByID` (`outbound_webhooks.go:215-226`).
pub fn node_field(
    db: &OutboundWebhooksHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if kind != OUTBOUND_WEBHOOK_KIND {
        return Ok(None);
    }
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let node_id = unmarshal_outbound_webhook_id(id).map_err(ExecError::new)?;
    Ok(Some(FieldValue::Object(Arc::new(OutboundWebhookObject {
        id: node_id,
        data: None,
        db: db.clone(),
    }))))
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

pub struct OutboundWebhookConnectionObject {
    nodes: Vec<WebhookData>,
    total_count: i64,
    has_next_page: bool,
    end_cursor: Option<String>,
    db: OutboundWebhooksHandle,
}

impl ObjectValue for OutboundWebhookConnectionObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookConnection"
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
                    .nodes
                    .iter()
                    .map(|node| {
                        FieldValue::Object(Arc::new(OutboundWebhookObject {
                            id: node.id,
                            data: Some(node.clone()),
                            db: self.db.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("OutboundWebhook"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.has_next_page,
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

pub struct OutboundWebhookObject {
    id: i64,
    /// `None` reproduces `newOutboundWebhookResolverFromDatabase`'s lazy
    /// `sync.OnceValues(func() { store.GetByID(...) })`.
    data: Option<WebhookData>,
    db: OutboundWebhooksHandle,
}

impl OutboundWebhookObject {
    fn load(&self) -> Result<WebhookData, String> {
        match &self.data {
            Some(data) => Ok(data.clone()),
            None => self
                .db
                .get_webhook(self.id)?
                .ok_or_else(|| not_found(self.id)),
        }
    }

    /// `outboundWebhookResolver.Logs` + the connection resolver
    /// (`outbound_webhooks.go:374-402`, `outbound_webhook_logs.go:81-130`).
    fn logs_field(&self, arguments: &Arguments) -> Result<FieldValue, ExecError> {
        let first = arguments.i64("first").unwrap_or(50);
        let mut offset = 0i64;
        if let Some(after) = arguments.string("after") {
            offset = after.parse::<i64>().map_err(|_| {
                ExecError::new(format!("cannot parse offset {}", go_quote(after)))
            })?;
        }
        let only_errors = arguments.bool("onlyErrors").unwrap_or(false);

        // `newOutboundWebhookLogConnectionResolver` keeps the caller's limit
        // and bumps it only for the nodes probe.
        let listed = self
            .db
            .list_logs(&LogListOpts {
                limit: first + 1,
                offset,
                only_errors,
                outbound_webhook_id: self.id,
            })
            .map_err(ExecError::new)?;

        let (total, errored) = self
            .db
            .counts_for_outbound_webhook(self.id)
            .map_err(ExecError::new)?;
        let total_count = if only_errors { errored } else { total };

        let has_next_page = listed.len() as i64 > first;
        let nodes = if has_next_page {
            listed[..first.max(0) as usize].to_vec()
        } else {
            listed
        };
        let end_cursor = has_next_page.then(|| (first + offset).to_string());

        Ok(FieldValue::Object(Arc::new(
            OutboundWebhookLogConnectionObject {
                nodes,
                total_count,
                has_next_page,
                end_cursor,
                db: self.db.clone(),
            },
        )))
    }
}

impl ObjectValue for OutboundWebhookObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhook"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `func (r *outboundWebhookResolver) ID()` does not touch the
            // database.
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_outbound_webhook_id(self.id),
            )))),
            "url" => {
                let data = self.load().map_err(ExecError::new)?;
                let url = self
                    .db
                    .decrypt_url(&data.url, &data.encryption_key_id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Str(url))))
            }
            "eventTypes" => {
                let data = self.load().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: data
                        .event_types
                        .into_iter()
                        .map(|event_type| {
                            FieldValue::Object(Arc::new(OutboundWebhookScopedEventTypeObject {
                                event_type: event_type.event_type,
                                scope: event_type.scope,
                            }))
                        })
                        .collect(),
                    element_type: None,
                }))
            }
            // `func (r *outboundWebhookResolver) Stats()`.
            "stats" => {
                let (total, errored) = self
                    .db
                    .counts_for_outbound_webhook(self.id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    OutboundWebhookLogStatsObject { total, errored },
                ))))
            }
            // `func (r *outboundWebhookResolver) Logs()`.
            "logs" => Ok(Some(self.logs_field(arguments)?)),
            _ => Ok(None),
        }
    }
}

pub struct OutboundWebhookEventTypeObject {
    key: String,
    description: String,
}

impl ObjectValue for OutboundWebhookEventTypeObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookEventType"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "key" => Ok(Some(FieldValue::Json(Json::Str(self.key.clone())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.description.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct OutboundWebhookScopedEventTypeObject {
    event_type: String,
    scope: Option<String>,
}

impl ObjectValue for OutboundWebhookScopedEventTypeObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookScopedEventType"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "eventType" => Ok(Some(FieldValue::Json(Json::Str(self.event_type.clone())))),
            "scope" => Ok(Some(match &self.scope {
                Some(scope) => FieldValue::Json(Json::Str(scope.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `outboundWebhookLogStatsResolver` (`outbound_webhook_logs.go:55-66`).
pub struct OutboundWebhookLogStatsObject {
    total: i64,
    errored: i64,
}

impl ObjectValue for OutboundWebhookLogStatsObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookLogStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "total" => Ok(Some(FieldValue::Json(Json::Int(self.total)))),
            "errored" => Ok(Some(FieldValue::Json(Json::Int(self.errored)))),
            _ => Ok(None),
        }
    }
}

/// `outboundWebhookLogConnectionResolver` (`outbound_webhook_logs.go:68-130`).
pub struct OutboundWebhookLogConnectionObject {
    nodes: Vec<OutboundWebhookLogData>,
    total_count: i64,
    has_next_page: bool,
    end_cursor: Option<String>,
    db: OutboundWebhooksHandle,
}

impl ObjectValue for OutboundWebhookLogConnectionObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookLogConnection"
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
                    .nodes
                    .iter()
                    .map(|log| {
                        FieldValue::Object(Arc::new(OutboundWebhookLogObject {
                            log: log.clone(),
                            db: self.db.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("OutboundWebhookLog"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.has_next_page,
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `outboundWebhookLogResolver` (`outbound_webhook_logs.go:132-193`).
pub struct OutboundWebhookLogObject {
    log: OutboundWebhookLogData,
    db: OutboundWebhooksHandle,
}

impl OutboundWebhookLogObject {
    fn decrypt_message(
        &self,
        raw: &[u8],
    ) -> Result<crate::webhook_logs::WebhookLogMessageData, String> {
        let plaintext = self
            .db
            .decrypt_outbound_value(raw, &self.log.encryption_key_id)?;
        decode_message(&plaintext)
    }
}

impl ObjectValue for OutboundWebhookLogObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_outbound_webhook_log_id(self.log.id),
            )))),
            // `newOutboundWebhookJobResolver` is lazy: `job { id }` succeeds
            // even when the row is missing.
            "job" => Ok(Some(FieldValue::Object(Arc::new(
                OutboundWebhookJobObject {
                    id: self.log.job_id,
                    db: self.db.clone(),
                },
            )))),
            "sentAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.log.sent_at,
            ))))),
            "statusCode" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.log.status_code,
            ))))),
            "request" => {
                let message = self.decrypt_message(&self.log.request).map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    WebhookLogRequestObject::from_message(message),
                ))))
            }
            "response" => {
                if self.log.status_code == OUTBOUND_WEBHOOK_LOG_UNSENT_STATUS_CODE {
                    return Ok(Some(FieldValue::Null));
                }
                let message =
                    self.decrypt_message(&self.log.response).map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    WebhookLogMessageObject::from_message(message),
                ))))
            }
            "error" => {
                if self.log.status_code != OUTBOUND_WEBHOOK_LOG_UNSENT_STATUS_CODE {
                    return Ok(Some(FieldValue::Null));
                }
                let message = self
                    .db
                    .decrypt_outbound_value(&self.log.error, &self.log.encryption_key_id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Str(message))))
            }
            _ => Ok(None),
        }
    }
}

/// `outboundWebhookJobResolver` (`outbound_webhook_logs.go:195-243`).
pub struct OutboundWebhookJobObject {
    id: i64,
    db: OutboundWebhooksHandle,
}

impl OutboundWebhookJobObject {
    /// `OutboundWebhookJobStore.GetByID`; missing rows surface
    /// `OutboundWebhookJobNotFoundErr`.
    fn load(&self) -> Result<OutboundWebhookJobData, String> {
        self.db.get_job(self.id)?.ok_or_else(|| {
            format!("outbound webhook job with id {} not found", self.id)
        })
    }
}

impl ObjectValue for OutboundWebhookJobObject {
    fn type_name(&self) -> &'static str {
        "OutboundWebhookJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `func (r *outboundWebhookJobResolver) ID()` does not touch the
            // database. Baseline bug: `newOutboundWebhookJobResolver` never
            // copies its `id` argument into the resolver struct
            // (`outbound_webhook_logs.go:203-211`), so `ID()` always marshals
            // zero even though `job()` loads the real row. Reproduced verbatim.
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_outbound_webhook_job_id(0),
            )))),
            "eventType" => {
                let job = self.load().map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Str(job.event_type))))
            }
            "scope" => {
                let job = self.load().map_err(ExecError::new)?;
                Ok(Some(match job.scope {
                    Some(scope) => FieldValue::Json(Json::Str(scope)),
                    None => FieldValue::Null,
                }))
            }
            "payload" => {
                let job = self.load().map_err(ExecError::new)?;
                let payload = self
                    .db
                    .decrypt_outbound_value(&job.payload, &job.encryption_key_id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Str(payload))))
            }
            _ => Ok(None),
        }
    }
}
