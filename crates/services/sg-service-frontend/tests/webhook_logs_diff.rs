//! M9.8gi differential of `Query.webhookLogs`, the `WebhookLogConnection` and
//! the read-only `WebhookLog` / `WebhookLogMessage` fields against the Go
//! oracle (`cmd/frontend/graphqlbackend/zz_m9gi_webhook_logs_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9GI_DIFF=1`
//! * `M9GI_CORPUS` — JSON `[{name, uid, first?, after?, onlyErrors?,
//!   onlyUnmatched?, since?, until?, webhookID?, webhookIDInt?, legacyOnly?,
//!   externalServiceID?}]`
//! * `M9GI_OUT` — output directory (one `<name>.json` per case)
//! * `M9GI_DSN` — postgres DSN of the scratch database
//! * `M9GI_ENCRYPTION_KEY` — 32-byte mounted key secret for fixture row 8005

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use sg_encryption::mounted::MountedKey;
use sg_encryption::{Key, NoopKey};
use sg_graphql::exec::Actor;
use sg_service_frontend::external_services::PgExternalServiceConnectionStore;
use sg_service_frontend::graphql_resolvers::{UserData, UserKey, UserLookupStore};
use sg_service_frontend::user_store::PgUserStore;
use sg_service_frontend::webhook_logs::{
    marshal_webhook_log_id, webhook_logs, WebhookLogConnectionStore, WebhookLogMessageData,
    WebhookLogNodeData, WebhookLogsArgs, WebhookLogsConnectionLookup, ALL_EXTERNAL_SERVICES,
    UNMATCHED_EXTERNAL_SERVICE,
};

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    first: Option<i32>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default, rename = "onlyErrors")]
    only_errors: Option<bool>,
    #[serde(default, rename = "onlyUnmatched")]
    only_unmatched: Option<bool>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    until: Option<String>,
    #[serde(default, rename = "webhookID")]
    webhook_id: Option<String>,
    #[serde(default, rename = "webhookIDInt")]
    webhook_id_int: Option<i32>,
    #[serde(default, rename = "legacyOnly")]
    legacy_only: Option<bool>,
    #[serde(default, rename = "externalServiceID")]
    external_service_id: Option<i64>,
}

/// Field order matches the Go oracle's `m9giEnvelope`.
#[derive(Serialize)]
struct Envelope {
    error: bool,
    nodes: Vec<Node>,
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

#[derive(Serialize)]
struct Node {
    id: String,
    #[serde(rename = "receivedAt")]
    received_at: String,
    #[serde(rename = "externalService")]
    external_service: Option<ExternalService>,
    #[serde(rename = "externalServiceError")]
    external_service_error: bool,
    #[serde(rename = "statusCode")]
    status_code: i32,
    request: Message,
    response: Message,
}

#[derive(Serialize)]
struct ExternalService {
    id: String,
    kind: String,
    #[serde(rename = "displayName")]
    display_name: String,
}

#[derive(Serialize, Default)]
struct Message {
    headers: Vec<Header>,
    body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
}

#[derive(Serialize)]
struct Header {
    name: String,
    values: Vec<String>,
}

impl Envelope {
    fn error() -> Self {
        Self {
            error: true,
            nodes: Vec::new(),
            total_count: 0,
            page_info: PageInfo::default(),
        }
    }
}

fn header_message(message: &WebhookLogMessageData) -> Message {
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

fn request_message(message: &WebhookLogMessageData) -> Message {
    Message {
        method: Some(message.method.clone()),
        url: Some(message.url.clone()),
        version: Some(message.version.clone()),
        ..header_message(message)
    }
}

/// `externalServiceByID` (`relay.MarshalID("ExternalService", id)`).
fn marshal_external_service_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("ExternalService:{id}"))
}

/// `marshalWebhookID` (`relay.MarshalID("Webhook", id)`).
fn marshal_webhook_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Webhook:{id}"))
}

/// `gqlutil.DateTime.MarshalJSON`: UTC RFC3339, seconds precision.
fn datetime(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
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

fn node_from(data: WebhookLogNodeData) -> Result<Node, ()> {
    let request = data.request.map_err(|_| ())?;
    let response = data.response.map_err(|_| ())?;
    let (external_service, external_service_error) = match data.external_service {
        Ok(Some(service)) => (
            Some(ExternalService {
                id: marshal_external_service_id(service.id),
                kind: service.kind,
                display_name: service.display_name,
            }),
            false,
        ),
        // A nullable field's resolver error nulls only that field.
        Ok(None) => (None, false),
        Err(_) => (None, true),
    };
    Ok(Node {
        id: marshal_webhook_log_id(data.id),
        received_at: datetime(data.received_at),
        external_service,
        external_service_error,
        status_code: data.status_code,
        request: request_message(&request),
        response: header_message(&response),
    })
}

#[tokio::test]
async fn webhook_logs_harness() {
    if std::env::var("SG_M9GI_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GI_CORPUS").expect("M9GI_CORPUS");
    let out = std::env::var("M9GI_OUT").expect("M9GI_OUT");
    let dsn = std::env::var("M9GI_DSN").expect("M9GI_DSN");
    let secret = std::env::var("M9GI_ENCRYPTION_KEY").expect("M9GI_ENCRYPTION_KEY");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let logs = sg_service_frontend::webhook_logs::PgWebhookLogConnectionStore::new(pool.clone());
    let external_services = PgExternalServiceConnectionStore::new(pool.clone());
    let users = PgUserStore::new(pool);

    // The same fixed mounted key the Go oracle builds from M9GI_ENCRYPTION_KEY.
    let webhook_key: Arc<dyn Key> = Arc::new(
        MountedKey::from_secret("webhook-logs", "v1", secret.as_bytes()).expect("mounted key"),
    );
    let external_service_key: Arc<dyn Key> = Arc::new(NoopKey::default());

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

        let webhook_id = match (&case.webhook_id, case.webhook_id_int) {
            (Some(raw), _) => Some(raw.clone()),
            (None, Some(id)) => Some(marshal_webhook_id(id)),
            (None, None) => None,
        };
        let args = WebhookLogsArgs {
            first: case.first,
            after: case.after.clone(),
            only_errors: case.only_errors,
            since: case.since.clone(),
            until: case.until.clone(),
            webhook_id,
            legacy_only: case.legacy_only,
        };
        let mut external_service_id = ALL_EXTERNAL_SERVICES;
        if case.only_unmatched == Some(true) {
            external_service_id = UNMATCHED_EXTERNAL_SERVICE;
        }
        if let Some(id) = case.external_service_id {
            external_service_id = id;
        }

        let lookup = webhook_logs(
            &args,
            external_service_id,
            &logs as &dyn WebhookLogConnectionStore,
            &external_services,
            Some(&users),
            Some(webhook_key.clone()),
            Some(external_service_key.clone()),
            "",
            actor,
            viewer_site_admin,
        )
        .await;

        let envelope = match lookup {
            WebhookLogsConnectionLookup::Found(data) => {
                let mut nodes = Vec::with_capacity(data.nodes.len());
                let mut failed = false;
                for node in data.nodes {
                    match node_from(node) {
                        Ok(node) => nodes.push(node),
                        Err(()) => {
                            failed = true;
                            break;
                        }
                    }
                }
                if failed {
                    Envelope::error()
                } else {
                    Envelope {
                        error: false,
                        nodes,
                        total_count: data.total_count,
                        page_info: PageInfo {
                            has_next_page: data.has_next_page,
                            end_cursor: data.end_cursor,
                        },
                    }
                }
            }
            WebhookLogsConnectionLookup::Error(message) => {
                eprintln!("CASE {} error: {message}", case.name);
                Envelope::error()
            }
        };

        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
