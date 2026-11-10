//! M9.8gk differential of the remaining read-only `ExternalService` node
//! fields (`url`, `webhookURL`, `hasConnectionCheck`; `rateLimiterState` is
//! fixed to null on both sides), the `ExternalService.webhookLogs` connection
//! and the `Query.node` `ExternalService`/`WebhookLog` dispatch against the Go
//! oracle (`cmd/frontend/graphqlbackend/zz_m9gk_external_service_node_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9GK_DIFF=1`
//! * `M9GK_CORPUS` — JSON `[{name, mode, uid, esID?, logID?, rawID?,
//!   includeWebhookLogs?, externalServiceID?, first?, after?, onlyErrors?,
//!   legacyOnly?, since?, until?, webhookID?, webhookIDInt?}]`
//! * `M9GK_OUT` — output directory (one `<name>.json` per case)
//! * `M9GK_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::{OffsetDateTime, UtcOffset};

use sg_encryption::{Key, NoopKey};
use sg_graphql::exec::Actor;
use sg_service_frontend::external_services::{
    external_service_node_by_id, marshal_external_service_id, ExternalServiceNodeData,
    PgExternalServiceConnectionStore,
};
use sg_service_frontend::graphql_resolvers::{relay_kind, UserData, UserKey, UserLookupStore};
use sg_service_frontend::user_store::PgUserStore;
use sg_service_frontend::webhook_logs::{
    marshal_webhook_log_id, webhook_log_node_by_id, webhook_logs, PgWebhookLogConnectionStore,
    WebhookLogMessageData, WebhookLogNodeData, WebhookLogsArgs, WebhookLogsConnectionData,
    WebhookLogsConnectionLookup, ALL_EXTERNAL_SERVICES,
};

/// The same fixed value the Go oracle passes to `conf.Mock`.
const EXTERNAL_URL: &str = "https://sourcegraph.example.com";

#[derive(Deserialize)]
struct Case {
    name: String,
    mode: String,
    #[serde(default)]
    uid: i32,
    #[serde(default, rename = "esID")]
    es_id: Option<i64>,
    #[serde(default, rename = "logID")]
    log_id: Option<i64>,
    #[serde(default, rename = "rawID")]
    raw_id: Option<String>,
    #[serde(default, rename = "includeWebhookLogs")]
    include_webhook_logs: bool,
    #[serde(default, rename = "externalServiceID")]
    external_service_id: Option<i64>,
    #[serde(default)]
    first: Option<i32>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default, rename = "onlyErrors")]
    only_errors: Option<bool>,
    #[serde(default, rename = "legacyOnly")]
    legacy_only: Option<bool>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    until: Option<String>,
    #[serde(default, rename = "webhookID")]
    webhook_id: Option<String>,
    #[serde(default, rename = "webhookIDInt")]
    webhook_id_int: Option<i32>,
}

/// Field order matches the Go oracle's `m9gkEnvelope`.
#[derive(Serialize, Default)]
struct Envelope {
    #[serde(rename = "nodeError")]
    node_error: bool,
    #[serde(rename = "nodeErrorCategory")]
    node_error_category: String,
    #[serde(rename = "typeMismatch")]
    type_mismatch: bool,
    node: Option<Node>,
    #[serde(rename = "webhookLog")]
    webhook_log: Option<WebhookLogNode>,
    connection: Option<Connection>,
    #[serde(rename = "connectionError")]
    connection_error: bool,
    #[serde(rename = "connectionErrorCategory")]
    connection_error_category: String,
}

#[derive(Serialize)]
struct Node {
    id: String,
    url: String,
    #[serde(rename = "urlError")]
    url_error: bool,
    #[serde(rename = "webhookURL")]
    webhook_url: Option<String>,
    #[serde(rename = "webhookURLError")]
    webhook_url_error: bool,
    #[serde(rename = "hasConnectionCheck")]
    has_connection_check: bool,
    #[serde(rename = "rateLimiterState")]
    rate_limiter_state: Option<String>,
}

#[derive(Serialize)]
struct ExternalServiceBasic {
    id: String,
    kind: String,
    #[serde(rename = "displayName")]
    display_name: String,
}

#[derive(Serialize)]
struct WebhookLogNode {
    id: String,
    #[serde(rename = "receivedAt")]
    received_at: String,
    #[serde(rename = "statusCode")]
    status_code: i32,
    #[serde(rename = "externalService")]
    external_service: Option<ExternalServiceBasic>,
    #[serde(rename = "externalServiceError")]
    external_service_error: bool,
    request: Option<Message>,
    #[serde(rename = "requestError")]
    request_error: bool,
    response: Option<Message>,
    #[serde(rename = "responseError")]
    response_error: bool,
}

#[derive(Serialize)]
struct Message {
    headers: Vec<Header>,
    body: String,
    method: Option<String>,
    url: Option<String>,
    version: Option<String>,
}

#[derive(Serialize)]
struct Header {
    name: String,
    values: Vec<String>,
}

#[derive(Serialize)]
struct Connection {
    nodes: Vec<WebhookLogNode>,
    #[serde(rename = "totalCount")]
    total_count: i64,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Serialize, Default)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

/// `gqlutil.DateTime.MarshalJSON`: UTC RFC3339, seconds precision.
fn datetime(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// Same stable mapping as the Go oracle's `m9gkErrorCategory`.
fn error_category(message: &str) -> &'static str {
    if message.contains("not authenticated") || message.contains("must be site admin") {
        "auth"
    } else if message.contains("invalid id")
        || message.contains("invalid graphql.ID")
        || message.contains("expected graphql ID")
    {
        "invalid_id"
    } else if message.to_lowercase().contains("not found") || message.contains("no rows") {
        "not_found"
    } else {
        "other"
    }
}

fn node_from(data: &ExternalServiceNodeData) -> Node {
    Node {
        id: marshal_external_service_id(data.id),
        url: data.url.clone().unwrap_or_default(),
        url_error: data.url.is_err(),
        webhook_url: data.webhook_url.clone().ok().flatten(),
        webhook_url_error: data.webhook_url.is_err(),
        has_connection_check: data.has_connection_check,
        rate_limiter_state: None,
    }
}

fn basic_from(data: &ExternalServiceNodeData) -> ExternalServiceBasic {
    ExternalServiceBasic {
        id: marshal_external_service_id(data.id),
        kind: data.kind.clone(),
        display_name: data.display_name.clone(),
    }
}

fn request_message(message: &WebhookLogMessageData) -> Message {
    Message {
        headers: message
            .headers
            .iter()
            .map(|(name, values)| Header {
                name: name.clone(),
                values: values.clone(),
            })
            .collect(),
        body: String::from_utf8_lossy(&message.body).into_owned(),
        method: Some(message.method.clone()),
        url: Some(message.url.clone()),
        version: Some(message.version.clone()),
    }
}

fn response_message(message: &WebhookLogMessageData) -> Message {
    Message {
        headers: message
            .headers
            .iter()
            .map(|(name, values)| Header {
                name: name.clone(),
                values: values.clone(),
            })
            .collect(),
        body: String::from_utf8_lossy(&message.body).into_owned(),
        method: None,
        url: None,
        version: None,
    }
}

fn webhook_log_from(data: WebhookLogNodeData) -> WebhookLogNode {
    let (external_service, external_service_error) = match data.external_service {
        Ok(Some(service)) => (Some(basic_from(&service)), false),
        Ok(None) => (None, false),
        Err(_) => (None, true),
    };
    let (request, request_error) = match data.request {
        Ok(message) => (Some(request_message(&message)), false),
        Err(_) => (None, true),
    };
    let (response, response_error) = match data.response {
        Ok(message) => (Some(response_message(&message)), false),
        Err(_) => (None, true),
    };
    WebhookLogNode {
        id: marshal_webhook_log_id(data.id),
        received_at: datetime(data.received_at),
        status_code: data.status_code,
        external_service,
        external_service_error,
        request,
        request_error,
        response,
        response_error,
    }
}

fn connection_from(data: WebhookLogsConnectionData) -> Connection {
    Connection {
        nodes: data.nodes.into_iter().map(webhook_log_from).collect(),
        total_count: data.total_count,
        page_info: PageInfo {
            has_next_page: data.has_next_page,
            end_cursor: data.end_cursor,
        },
    }
}

/// `relay.MarshalID("Webhook", id)` for the `webhookID` argument.
fn marshal_webhook_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Webhook:{id}"))
}

fn build_args(case: &Case) -> WebhookLogsArgs {
    let webhook_id = match (&case.webhook_id, case.webhook_id_int) {
        (Some(raw), _) => Some(raw.clone()),
        (None, Some(id)) => Some(marshal_webhook_id(id)),
        (None, None) => None,
    };
    WebhookLogsArgs {
        first: case.first,
        after: case.after.clone(),
        only_errors: case.only_errors,
        since: case.since.clone(),
        until: case.until.clone(),
        webhook_id,
        legacy_only: case.legacy_only,
    }
}

fn build_node_id(case: &Case) -> Option<String> {
    match (&case.raw_id, case.log_id, case.es_id) {
        (Some(raw), _, _) => Some(raw.clone()),
        (None, Some(id), _) => Some(marshal_webhook_log_id(id)),
        (None, None, Some(id)) => Some(marshal_external_service_id(id)),
        (None, None, None) => None,
    }
}

#[tokio::test]
async fn external_service_node_harness() {
    if std::env::var("SG_M9GK_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GK_CORPUS").expect("M9GK_CORPUS");
    let out = std::env::var("M9GK_OUT").expect("M9GK_OUT");
    let dsn = std::env::var("M9GK_DSN").expect("M9GK_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let services = PgExternalServiceConnectionStore::new(pool.clone());
    let logs = PgWebhookLogConnectionStore::new(pool.clone());
    let users = PgUserStore::new(pool);
    let external_service_key: Arc<dyn Key> = Arc::new(NoopKey::default());
    let webhook_key: Arc<dyn Key> = Arc::new(NoopKey::default());

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let current_user: Option<UserData> = if case.uid == 0 {
            None
        } else {
            users
                .lookup(&UserKey::DatabaseId(case.uid))
                .await
                .expect("user lookup")
        };
        let viewer_site_admin = current_user
            .as_ref()
            .map(|user| user.site_admin)
            .unwrap_or(false);

        let mut envelope = Envelope::default();

        match case.mode.as_str() {
            "externalService" | "webhookLog" => {
                let id = build_node_id(&case).expect("externalService/webhookLog needs an id");
                let kind = relay_kind(&id);
                match (case.mode.as_str(), kind.as_deref()) {
                    ("externalService", Some("ExternalService")) => {
                        match external_service_node_by_id(
                            &services,
                            Some(&users),
                            Some(external_service_key.clone()),
                            EXTERNAL_URL,
                            &id,
                            actor,
                            viewer_site_admin,
                        )
                        .await
                        {
                            Ok(data) => {
                                envelope.node = Some(node_from(&data));
                                if case.include_webhook_logs {
                                    let args = build_args(&case);
                                    let lookup = webhook_logs(
                                        &args,
                                        data.id,
                                        &logs,
                                        &services,
                                        Some(&users),
                                        Some(webhook_key.clone()),
                                        Some(external_service_key.clone()),
                                        EXTERNAL_URL,
                                        actor,
                                        viewer_site_admin,
                                    )
                                    .await;
                                    match lookup {
                                        WebhookLogsConnectionLookup::Found(data) => {
                                            envelope.connection = Some(connection_from(data));
                                        }
                                        WebhookLogsConnectionLookup::Error(message) => {
                                            envelope.connection_error = true;
                                            envelope.connection_error_category =
                                                error_category(&message).to_owned();
                                        }
                                    }
                                }
                            }
                            Err(message) => {
                                envelope.node_error = true;
                                envelope.node_error_category = error_category(&message).to_owned();
                            }
                        }
                    }
                    ("webhookLog", Some("WebhookLog")) => {
                        match webhook_log_node_by_id(
                            &logs,
                            &services,
                            Some(&users),
                            Some(webhook_key.clone()),
                            Some(external_service_key.clone()),
                            EXTERNAL_URL,
                            &id,
                            actor,
                            viewer_site_admin,
                        )
                        .await
                        {
                            Ok(data) => envelope.webhook_log = Some(webhook_log_from(data)),
                            Err(message) => {
                                envelope.node_error = true;
                                envelope.node_error_category = error_category(&message).to_owned();
                            }
                        }
                    }
                    ("externalService", Some("WebhookLog")) => {
                        // `Query.node` resolves the WebhookLog node first; the
                        // `... on ExternalService` fragment then does not match.
                        match webhook_log_node_by_id(
                            &logs,
                            &services,
                            Some(&users),
                            Some(webhook_key.clone()),
                            Some(external_service_key.clone()),
                            EXTERNAL_URL,
                            &id,
                            actor,
                            viewer_site_admin,
                        )
                        .await
                        {
                            Ok(_) => envelope.type_mismatch = true,
                            Err(message) => {
                                envelope.node_error = true;
                                envelope.node_error_category = error_category(&message).to_owned();
                            }
                        }
                    }
                    ("webhookLog", Some("ExternalService")) => {
                        match external_service_node_by_id(
                            &services,
                            Some(&users),
                            Some(external_service_key.clone()),
                            EXTERNAL_URL,
                            &id,
                            actor,
                            viewer_site_admin,
                        )
                        .await
                        {
                            Ok(_) => envelope.type_mismatch = true,
                            Err(message) => {
                                envelope.node_error = true;
                                envelope.node_error_category = error_category(&message).to_owned();
                            }
                        }
                    }
                    _ => {
                        // Unregistered Relay kind (including undecodable IDs):
                        // `schemaResolver.Node` returns `invalid id`.
                        envelope.node_error = true;
                        envelope.node_error_category = "invalid_id".to_owned();
                    }
                }
            }
            "connection" => {
                let args = build_args(&case);
                let external_service_id = case.external_service_id.unwrap_or(ALL_EXTERNAL_SERVICES);
                let lookup = webhook_logs(
                    &args,
                    external_service_id,
                    &logs,
                    &services,
                    Some(&users),
                    Some(webhook_key.clone()),
                    Some(external_service_key.clone()),
                    EXTERNAL_URL,
                    actor,
                    viewer_site_admin,
                )
                .await;
                match lookup {
                    WebhookLogsConnectionLookup::Found(data) => {
                        envelope.connection = Some(connection_from(data));
                    }
                    WebhookLogsConnectionLookup::Error(message) => {
                        envelope.connection_error = true;
                        envelope.connection_error_category = error_category(&message).to_owned();
                    }
                }
            }
            other => panic!("unknown mode {other}"),
        }

        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
