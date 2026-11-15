#![forbid(unsafe_code)]

//! `Query.webhookLogs` and the read-only `WebhookLog` / `WebhookLogMessage`
//! types (`cmd/frontend/graphqlbackend/webhook_logs.go:18-290`,
//! `internal/database/webhook_logs.go`).
//!
//! The connection is site-admin only. It lists `webhook_logs` newest id first,
//! applies the baseline's `onlyErrors` / `onlyUnmatched` / `webhookID` /
//! `legacyOnly` / `since` / `until` filters, and pages with the `id <= cursor`
//! scheme. Node request/response blobs are decrypted with the keyring's
//! `webhookLogKey` (`internal/encryption/json_encryptable.go`) and decoded from
//! the `WebhookLogMessage` JSON (header map, base64 `Body`, optional
//! `Method`/`URL`/`Version`).
//!
//! M9.8gk adds `Query.node`'s `WebhookLog` branch (`webhook_logs.go:222-247`)
//! and the nested `ExternalService.webhookLogs` connection
//! (`external_service.go:287-293`).
//!
//! Not ported: every mutation.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use sg_db::webhook_logs::{WebhookLogListOpts, WebhookLogRecord, WebhookLogStore};
use sg_encryption::{maybe_decrypt, Key};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::external_services::{
    external_service_by_id, ExternalServiceConnectionStore, ExternalServiceNodeData,
    ExternalServiceObject, ExternalServicesArgs, ExternalServicesConnectionLookup,
};
use crate::graphql_resolvers::{
    check_current_user_is_site_admin, datetime_rfc3339, relay_kind, unmarshal_relay_id, NodeRoots,
    PageInfoObject, UserLookupStore,
};

/// `webhookLogsExternalServiceID` sentinels (`webhook_logs.go:43-53`).
pub const ALL_EXTERNAL_SERVICES: i64 = -1;
pub const UNMATCHED_EXTERNAL_SERVICE: i64 = 0;

/// `relay.MarshalID("WebhookLog", id)`.
pub fn marshal_webhook_log_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("WebhookLog:{id}"))
}

/// `unmarshalWebhookLogID` (`webhook_logs.go:222-247`): `relay.UnmarshalSpec`
/// into an int64; the Relay kind is **not** validated.
pub fn unmarshal_webhook_log_id(raw: &str) -> Result<i64, String> {
    crate::external_services::relay_spec_i64(raw)
}

/// The arguments shared by `Query.webhookLogs` and
/// `ExternalService.webhookLogs` (`webhook_logs.go:24-32`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct WebhookLogsArgs {
    pub first: Option<i32>,
    pub after: Option<String>,
    pub only_errors: Option<bool>,
    pub since: Option<String>,
    pub until: Option<String>,
    /// Raw relay `ID`; parsed lazily so invalid input is a resolver error.
    pub webhook_id: Option<String>,
    pub legacy_only: Option<bool>,
}

/// `globalWebhookLogsArgs`: the root field adds `onlyUnmatched`
/// (`webhook_logs.go:128-131`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct GlobalWebhookLogsArgs {
    pub logs: WebhookLogsArgs,
    pub only_unmatched: Option<bool>,
}

/// The argument set from the executor's resolved arguments.
pub fn webhook_logs_args(arguments: &Arguments) -> WebhookLogsArgs {
    WebhookLogsArgs {
        first: arguments
            .i64("first")
            .and_then(|value| i32::try_from(value).ok()),
        after: arguments.string("after").map(str::to_owned),
        only_errors: arguments.bool("onlyErrors"),
        since: arguments.string("since").map(str::to_owned),
        until: arguments.string("until").map(str::to_owned),
        webhook_id: arguments.string("webhookID").map(str::to_owned),
        legacy_only: arguments.bool("legacyOnly"),
    }
}

/// The root field's argument set (adds `onlyUnmatched`).
pub fn global_webhook_logs_args(arguments: &Arguments) -> GlobalWebhookLogsArgs {
    GlobalWebhookLogsArgs {
        logs: webhook_logs_args(arguments),
        only_unmatched: arguments.bool("onlyUnmatched"),
    }
}

/// `WebhookLogsArgs.toListOpts` (`webhook_logs.go:58-105`).
pub fn to_list_opts(
    args: &WebhookLogsArgs,
    external_service_id: i64,
) -> Result<WebhookLogListOpts, String> {
    let mut opts = WebhookLogListOpts {
        // `webhookLogsExternalServiceID.toListOpt`: -1 means "all".
        external_service_id: if external_service_id == ALL_EXTERNAL_SERVICES {
            None
        } else {
            Some(external_service_id)
        },
        since: parse_datetime(args.since.as_deref())?,
        until: parse_datetime(args.until.as_deref())?,
        limit: args.first.map(i64::from).unwrap_or(50),
        cursor: 0,
        webhook_id: None,
        only_errors: false,
    };

    if let Some(after) = &args.after {
        opts.cursor = after
            .parse::<i64>()
            .map_err(|error| format!("parsing the after cursor: {error}"))?;
    }

    if args.only_errors == Some(true) {
        opts.only_errors = true;
    }

    // Both nil and "-1" webhook IDs resolve to "no filter"; only positive ids
    // are applied (`webhook_logs.go:88-96`).
    if let Some(raw) = &args.webhook_id {
        let id = unmarshal_relay_id(raw)
            .map_err(|error| format!("unmarshalling webhook ID: {error}"))?;
        if id > 0 {
            opts.webhook_id = Some(id);
        }
    }

    // `legacyOnly` wins over `webhookID` and restricts to `webhook_id IS NULL`.
    if args.legacy_only == Some(true) {
        opts.webhook_id = Some(0);
    }

    Ok(opts)
}

fn parse_datetime(value: Option<&str>) -> Result<Option<OffsetDateTime>, String> {
    match value {
        None => Ok(None),
        Some(text) => OffsetDateTime::parse(text, &Rfc3339)
            .map(Some)
            .map_err(|error| error.to_string()),
    }
}

/// The database seam for `webhook_logs` (`db.WebhookLogs(key)`).
#[async_trait]
pub trait WebhookLogConnectionStore: Send + Sync {
    async fn list(&self, opts: &WebhookLogListOpts)
        -> Result<(Vec<WebhookLogRecord>, i64), String>;

    async fn count(&self, opts: &WebhookLogListOpts) -> Result<i64, String>;

    async fn get_by_id(&self, id: i64) -> Result<WebhookLogRecord, String>;
}

/// Production adapter over [`sg_db::webhook_logs::WebhookLogStore`].
pub struct PgWebhookLogConnectionStore {
    store: WebhookLogStore,
}

impl PgWebhookLogConnectionStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            store: WebhookLogStore::new(pool),
        }
    }
}

#[async_trait]
impl WebhookLogConnectionStore for PgWebhookLogConnectionStore {
    async fn list(
        &self,
        opts: &WebhookLogListOpts,
    ) -> Result<(Vec<WebhookLogRecord>, i64), String> {
        self.store
            .list(opts)
            .await
            .map_err(|error| error.to_string())
    }

    async fn count(&self, opts: &WebhookLogListOpts) -> Result<i64, String> {
        self.store
            .count(opts)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_by_id(&self, id: i64) -> Result<WebhookLogRecord, String> {
        self.store
            .get_by_id(id)
            .await
            .map_err(|error| error.to_string())
    }
}

/// `types.WebhookLogMessage`, decoded from the (possibly encrypted) JSON blob.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebhookLogMessageData {
    /// Sorted by header name (Go map iteration is unordered).
    pub headers: Vec<(String, Vec<String>)>,
    pub body: Vec<u8>,
    pub method: String,
    pub url: String,
    pub version: String,
}

#[derive(serde::Deserialize)]
struct RawWebhookLogMessage {
    // Go's `WebhookLogMessage` field names (no json tags on Header/Body;
    // `Method`/`URL`/`Version` use `,omitempty`, which keeps the field name).
    #[serde(rename = "Header", default)]
    header: Option<BTreeMap<String, Option<Vec<String>>>>,
    /// `[]byte` is base64 in Go's JSON encoding.
    #[serde(rename = "Body", default)]
    body: Option<String>,
    #[serde(rename = "Method", default)]
    method: Option<String>,
    #[serde(rename = "URL", default)]
    url: Option<String>,
    #[serde(rename = "Version", default)]
    version: Option<String>,
}

/// `EncryptableWebhookLogMessage.Decrypt` (`json_encryptable.go:31-44`).
async fn load_message(
    cipher: &[u8],
    key_id: &str,
    key: Option<&dyn Key>,
) -> Result<WebhookLogMessageData, String> {
    let cipher = String::from_utf8_lossy(cipher);
    let plaintext = maybe_decrypt(key, &cipher, key_id)
        .await
        .map_err(|error| error.to_string())?;
    decode_message(&plaintext)
}

/// Decode the `WebhookLogMessage` JSON shared by `webhook_logs` and
/// `outbound_webhook_logs` (`types.WebhookLogMessage`).
pub(crate) fn decode_message(plaintext: &str) -> Result<WebhookLogMessageData, String> {
    let raw: RawWebhookLogMessage =
        serde_json::from_str(plaintext).map_err(|error| error.to_string())?;
    let mut headers: Vec<(String, Vec<String>)> = raw
        .header
        .unwrap_or_default()
        .into_iter()
        .map(|(name, values)| (name, values.unwrap_or_default()))
        .collect();
    headers.sort_by(|left, right| left.0.cmp(&right.0));

    let body = match raw.body {
        None => Vec::new(),
        Some(encoded) => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| error.to_string())?,
    };

    Ok(WebhookLogMessageData {
        headers,
        body,
        method: raw.method.unwrap_or_default(),
        url: raw.url.unwrap_or_default(),
        version: raw.version.unwrap_or_default(),
    })
}

/// One `WebhookLog` node. Request/response decryption and the
/// `externalService` lookup happen during prefetch, but their errors surface
/// only when the corresponding field is selected (the baseline resolves them
/// lazily).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookLogNodeData {
    pub id: i64,
    pub received_at: OffsetDateTime,
    pub external_service: Result<Option<ExternalServiceNodeData>, String>,
    pub status_code: i32,
    pub request: Result<WebhookLogMessageData, String>,
    pub response: Result<WebhookLogMessageData, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookLogsConnectionData {
    pub nodes: Vec<WebhookLogNodeData>,
    pub total_count: i64,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

/// The result of resolving one `Query.webhookLogs` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookLogsConnectionLookup {
    Found(WebhookLogsConnectionData),
    Error(String),
}

/// `schemaResolver.WebhookLogs` (`webhook_logs.go:133-141`) plus
/// `NewWebhookLogConnectionResolver`'s gate and `compute`
/// (`webhook_logs.go:149-206`).
#[allow(clippy::too_many_arguments)]
pub async fn webhook_logs(
    args: &WebhookLogsArgs,
    external_service_id: i64,
    logs: &dyn WebhookLogConnectionStore,
    external_services: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    webhook_key: Option<Arc<dyn Key>>,
    external_service_key: Option<Arc<dyn Key>>,
    external_url: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> WebhookLogsConnectionLookup {
    // `auth.CheckCurrentUserIsSiteAdmin`.
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return WebhookLogsConnectionLookup::Error(message);
    }

    let opts = match to_list_opts(args, external_service_id) {
        Ok(opts) => opts,
        Err(message) => return WebhookLogsConnectionLookup::Error(message),
    };

    // `TotalCount` (`webhook_logs.go:163-172`).
    let total_count = match logs.count(&opts).await {
        Ok(count) => count,
        Err(message) => return WebhookLogsConnectionLookup::Error(message),
    };

    // `Nodes` -> `compute` (`webhook_logs.go:155-161,185-206`).
    let (records, next) = match logs.list(&opts).await {
        Ok(result) => result,
        Err(message) => return WebhookLogsConnectionLookup::Error(message),
    };

    let mut nodes = Vec::with_capacity(records.len());
    for record in records {
        nodes.push(
            build_webhook_log_node(
                record,
                external_services,
                users,
                webhook_key.as_deref(),
                external_service_key.clone(),
                external_url,
            )
            .await,
        );
    }

    // `PageInfo` (`webhook_logs.go:174-183`).
    let (has_next_page, end_cursor) = if next == 0 {
        (false, None)
    } else {
        (true, Some(next.to_string()))
    };

    WebhookLogsConnectionLookup::Found(WebhookLogsConnectionData {
        nodes,
        total_count,
        has_next_page,
        end_cursor,
    })
}

/// Build one `WebhookLog` node from its row (`webhookLogResolver`'s lazily
/// resolved fields are all computed eagerly here).
async fn build_webhook_log_node(
    record: WebhookLogRecord,
    external_services: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    webhook_key: Option<&dyn Key>,
    external_service_key: Option<Arc<dyn Key>>,
    external_url: &str,
) -> WebhookLogNodeData {
    let external_service = match record.external_service_id {
        None => Ok(None),
        Some(id) => external_service_by_id(
            external_services,
            users,
            external_service_key,
            external_url,
            id,
        )
        .await
        .map(Some),
    };
    let request = load_message(&record.request, &record.encryption_key_id, webhook_key).await;
    let response = load_message(&record.response, &record.encryption_key_id, webhook_key).await;
    WebhookLogNodeData {
        id: record.id,
        received_at: record.received_at,
        external_service,
        status_code: record.status_code,
        request,
        response,
    }
}

/// `webhookLogByID` (`webhook_logs.go:222-247`): site-admin gate,
/// `unmarshalWebhookLogID` (no kind validation) and `GetByID`.
#[allow(clippy::too_many_arguments)]
pub async fn webhook_log_node_by_id(
    logs: &dyn WebhookLogConnectionStore,
    external_services: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    webhook_key: Option<Arc<dyn Key>>,
    external_service_key: Option<Arc<dyn Key>>,
    external_url: &str,
    raw_id: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<WebhookLogNodeData, String> {
    check_current_user_is_site_admin(actor, viewer_site_admin)?;
    let id = unmarshal_webhook_log_id(raw_id)?;
    let record = logs.get_by_id(id).await?;
    Ok(build_webhook_log_node(
        record,
        external_services,
        users,
        webhook_key.as_deref(),
        external_service_key,
        external_url,
    )
    .await)
}

/// Prefetch every selected `Query.webhookLogs` field.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_webhook_logs(
    logs: &dyn WebhookLogConnectionStore,
    external_services: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    webhook_key: Option<Arc<dyn Key>>,
    external_service_key: Option<Arc<dyn Key>>,
    external_url: &str,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> std::collections::HashMap<GlobalWebhookLogsArgs, WebhookLogsConnectionLookup> {
    let mut connections = std::collections::HashMap::new();
    for field in fields {
        if field.name != "webhookLogs" {
            continue;
        }
        let args = global_webhook_logs_args(&field.arguments);
        if connections.contains_key(&args) {
            continue;
        }
        let external_service_id = if args.only_unmatched == Some(true) {
            UNMATCHED_EXTERNAL_SERVICE
        } else {
            ALL_EXTERNAL_SERVICES
        };
        let lookup = webhook_logs(
            &args.logs,
            external_service_id,
            logs,
            external_services,
            users,
            webhook_key.clone(),
            external_service_key.clone(),
            external_url,
            actor,
            viewer_site_admin,
        )
        .await;
        connections.insert(args, lookup);
    }
    connections
}

/// Find the `webhookLogs` selections under one object's selection set.
fn webhook_logs_selections<'a>(
    selections: &'a [RootField],
    type_conditions: &'a [(String, Vec<RootField>)],
    type_name: &str,
) -> Vec<&'a RootField> {
    let matching_conditions = type_conditions.iter().filter(|(condition, _)| {
        condition == type_name || condition == "Node" || condition == "ExternalService"
    });
    selections
        .iter()
        .chain(matching_conditions.flat_map(|(_, fields)| fields.iter()))
        .filter(|selection| selection.name == "webhookLogs")
        .collect()
}

/// Append every `webhookLogs` selection of one external service to the
/// `(external_service_id, selection)` work list.
fn collect_external_service_selections<'a>(
    pending: &mut Vec<(i64, &'a RootField)>,
    service: &ExternalServiceNodeData,
    selections: &[&'a RootField],
) {
    for selection in selections {
        pending.push((service.id, *selection));
    }
}

/// The fields selected on a `Query.node` result of runtime `kind`: direct
/// interface selections plus fragments on that type (or on `Node`).
fn selections_for_kind<'a>(field: &'a RootField, kind: &str) -> Vec<&'a RootField> {
    field
        .selections
        .iter()
        .chain(
            field
                .type_conditions
                .iter()
                .filter(|(condition, _)| condition == kind || condition == "Node")
                .flat_map(|(_, selections)| selections.iter()),
        )
        .collect()
}

/// `ExternalService.webhookLogs` for every `ExternalService` selected under
/// `Query.node`, `Query.externalServices.nodes`, `Query.webhookLogs.nodes`
/// and `Query.node(WebhookLog).externalService`, keyed by
/// `(external_service_id, args)`.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_external_service_webhook_logs(
    logs: &dyn WebhookLogConnectionStore,
    external_services: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    webhook_key: Option<Arc<dyn Key>>,
    external_service_key: Option<Arc<dyn Key>>,
    external_url: &str,
    fields: &[RootField],
    external_services_connections: &std::collections::HashMap<
        ExternalServicesArgs,
        ExternalServicesConnectionLookup,
    >,
    webhook_logs_connections: &std::collections::HashMap<
        GlobalWebhookLogsArgs,
        WebhookLogsConnectionLookup,
    >,
    nodes: &NodeRoots,
    actor: Actor,
    viewer_site_admin: bool,
) -> std::collections::HashMap<(i64, WebhookLogsArgs), WebhookLogsConnectionLookup> {
    let mut prefetched: std::collections::HashMap<
        (i64, WebhookLogsArgs),
        WebhookLogsConnectionLookup,
    > = std::collections::HashMap::new();
    if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        // The connection's gate reports the error when the field resolves;
        // nothing to prefetch for an unauthorized viewer.
        return prefetched;
    }

    // Collect `(external_service_id, selection)` pairs first so the lookups
    // can share `prefetched` without holding a closure across awaits.
    let mut pending: Vec<(i64, &RootField)> = Vec::new();

    for field in fields {
        match field.name.as_str() {
            "externalServices" => {
                let args = crate::external_services::external_services_args(&field.arguments);
                let Some(ExternalServicesConnectionLookup::Found(data)) =
                    external_services_connections.get(&args)
                else {
                    continue;
                };
                for nodes_selection in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    let selections = webhook_logs_selections(
                        &nodes_selection.selections,
                        &nodes_selection.type_conditions,
                        "ExternalService",
                    );
                    for service in &data.nodes {
                        collect_external_service_selections(&mut pending, service, &selections);
                    }
                }
            }
            "webhookLogs" => {
                let args = global_webhook_logs_args(&field.arguments);
                let Some(WebhookLogsConnectionLookup::Found(data)) =
                    webhook_logs_connections.get(&args)
                else {
                    continue;
                };
                for nodes_selection in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    let external_service_selections: Vec<&RootField> = nodes_selection
                        .selections
                        .iter()
                        .filter(|selection| selection.name == "externalService")
                        .collect();
                    for node in &data.nodes {
                        let Ok(Some(service)) = &node.external_service else {
                            continue;
                        };
                        for external_service_selection in &external_service_selections {
                            let selections = webhook_logs_selections(
                                &external_service_selection.selections,
                                &external_service_selection.type_conditions,
                                "ExternalService",
                            );
                            collect_external_service_selections(&mut pending, service, &selections);
                        }
                    }
                }
            }
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                let Some(kind) = relay_kind(id) else {
                    continue;
                };
                let selected = selections_for_kind(field, &kind);
                match kind.as_str() {
                    "ExternalService" => {
                        let Some(Ok(service)) = nodes.external_services.get(id) else {
                            continue;
                        };
                        let selections: Vec<&RootField> = selected
                            .iter()
                            .copied()
                            .filter(|selection| selection.name == "webhookLogs")
                            .collect();
                        collect_external_service_selections(&mut pending, service, &selections);
                    }
                    "WebhookLog" => {
                        let Some(Ok(node)) = nodes.webhook_logs.get(id) else {
                            continue;
                        };
                        let Ok(Some(service)) = &node.external_service else {
                            continue;
                        };
                        let external_service_selections: Vec<&RootField> = selected
                            .iter()
                            .copied()
                            .filter(|selection| selection.name == "externalService")
                            .collect();
                        for external_service_selection in &external_service_selections {
                            let selections = webhook_logs_selections(
                                &external_service_selection.selections,
                                &external_service_selection.type_conditions,
                                "ExternalService",
                            );
                            collect_external_service_selections(&mut pending, service, &selections);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    for (external_service_id, selection) in pending {
        let args = webhook_logs_args(&selection.arguments);
        if prefetched.contains_key(&(external_service_id, args.clone())) {
            continue;
        }
        let result = webhook_logs(
            &args,
            external_service_id,
            logs,
            external_services,
            users,
            webhook_key.clone(),
            external_service_key.clone(),
            external_url,
            actor,
            viewer_site_admin,
        )
        .await;
        prefetched.insert((external_service_id, args), result);
    }
    prefetched
}

/// `webhookLogConnectionResolver` (`webhook_logs.go:143-206`).
pub struct WebhookLogConnectionObject {
    data: WebhookLogsConnectionData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl WebhookLogConnectionObject {
    pub fn from_data(
        data: WebhookLogsConnectionData,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for WebhookLogConnectionObject {
    fn type_name(&self) -> &'static str {
        "WebhookLogConnection"
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
                    .data
                    .nodes
                    .iter()
                    .map(|node| {
                        FieldValue::Object(Arc::new(WebhookLogObject {
                            data: node.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("WebhookLog"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                self.data.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `webhookLogResolver` (`webhook_logs.go:208-275`).
pub struct WebhookLogObject {
    data: WebhookLogNodeData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl WebhookLogObject {
    /// Build the node object from pre-fetched data (used by `Query.node`).
    pub fn from_data(
        data: WebhookLogNodeData,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for WebhookLogObject {
    fn type_name(&self) -> &'static str {
        "WebhookLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_webhook_log_id(
                self.data.id,
            ))))),
            "receivedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.received_at,
            ))))),
            "externalService" => match &self.data.external_service {
                Ok(Some(data)) => Ok(Some(FieldValue::Object(Arc::new(
                    ExternalServiceObject::from_data(
                        data.clone(),
                        self.viewer_site_admin,
                        self.nested.clone(),
                    ),
                )))),
                Ok(None) => Ok(Some(FieldValue::Null)),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "statusCode" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.status_code,
            ))))),
            "request" => match &self.data.request {
                Ok(message) => Ok(Some(FieldValue::Object(Arc::new(
                    WebhookLogRequestObject {
                        message: message.clone(),
                    },
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "response" => match &self.data.response {
                Ok(message) => Ok(Some(FieldValue::Object(Arc::new(
                    WebhookLogMessageObject {
                        message: message.clone(),
                    },
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

fn headers_value(message: &WebhookLogMessageData) -> FieldValue {
    FieldValue::List {
        element_type: Some("HTTPHeader"),
        items: message
            .headers
            .iter()
            .map(|(name, values)| {
                FieldValue::Object(Arc::new(HttpHeaderObject {
                    name: name.clone(),
                    values: values.clone(),
                }))
            })
            .collect(),
    }
}

fn body_value(message: &WebhookLogMessageData) -> FieldValue {
    FieldValue::Json(Json::Str(
        String::from_utf8_lossy(&message.body).into_owned(),
    ))
}

/// `webhookLogMessageResolver` (`webhook_logs.go:277-287`), used for the
/// response.
pub struct WebhookLogMessageObject {
    message: WebhookLogMessageData,
}

impl WebhookLogMessageObject {
    /// Reuse the `WebhookLogResponse` object for outbound webhook logs.
    pub fn from_message(message: WebhookLogMessageData) -> Self {
        Self { message }
    }
}

impl ObjectValue for WebhookLogMessageObject {
    fn type_name(&self) -> &'static str {
        "WebhookLogResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "headers" => Ok(Some(headers_value(&self.message))),
            "body" => Ok(Some(body_value(&self.message))),
            _ => Ok(None),
        }
    }
}

/// `webhookLogRequestResolver` (`webhook_logs.go:289-307`).
pub struct WebhookLogRequestObject {
    message: WebhookLogMessageData,
}

impl WebhookLogRequestObject {
    /// Reuse the `WebhookLogRequest` object for outbound webhook logs.
    pub fn from_message(message: WebhookLogMessageData) -> Self {
        Self { message }
    }
}

impl ObjectValue for WebhookLogRequestObject {
    fn type_name(&self) -> &'static str {
        "WebhookLogRequest"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "headers" => Ok(Some(headers_value(&self.message))),
            "body" => Ok(Some(body_value(&self.message))),
            "method" => Ok(Some(FieldValue::Json(Json::Str(
                self.message.method.clone(),
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.message.url.clone())))),
            "version" => Ok(Some(FieldValue::Json(Json::Str(
                self.message.version.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

/// `outbound_requests.go:31-35,186-200` `HttpHeaders`.
pub struct HttpHeaderObject {
    name: String,
    values: Vec<String>,
}

impl ObjectValue for HttpHeaderObject {
    fn type_name(&self) -> &'static str {
        "HTTPHeader"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "values" => Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: self
                    .values
                    .iter()
                    .map(|value| FieldValue::Json(Json::Str(value.clone())))
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}
