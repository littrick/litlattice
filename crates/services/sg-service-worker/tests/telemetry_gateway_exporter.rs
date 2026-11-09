#![forbid(unsafe_code)]

//! M9.8bk differential harness (Rust side): the `telemetrygateway-exporter`
//! job's export queue store, identifier construction and `RecordEvents` export
//! client.
//!
//! Mirrors `cmd/worker/internal/telemetrygatewayexporter/tgx_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_telemetry_gateway_exporter_diff.sh`. Both harnesses seed
//! the same `telemetry_events_export_queue` rows into a scratch copy of the
//! frontend schema, drive the same scripted `TelemeteryGatewayService` server
//! (one response per export, containing the acknowledged event IDs) and print
//! canonical `tgx ` lines. Request IDs are UUIDv4 and are masked as
//! `<request-id>` on both sides.

use prost::Message;
use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_db::telemetry_events;
use sg_proto::telemetrygateway as tg;
use sg_service_worker::jobs::telemetry_gateway_exporter::{export_events, MAX_MESSAGE_SIZE};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

const SITE_ID: &str = "11111111-1111-4111-8111-111111111111";

/// 2024-01-01T00:00:00Z.
const BASE: i64 = 1_704_067_200;

fn event(id: &str, timestamp: i64, feature: &str) -> tg::Event {
    tg::Event {
        id: id.to_owned(),
        timestamp: Some(prost_types::Timestamp {
            seconds: timestamp,
            nanos: 0,
        }),
        feature: feature.to_owned(),
        action: "test".to_owned(),
        ..tg::Event::default()
    }
}

async fn clean(pool: &PgPool) {
    sqlx::query("DELETE FROM telemetry_events_export_queue")
        .execute(pool)
        .await
        .expect("clean export queue");
    sqlx::query("DELETE FROM global_state")
        .execute(pool)
        .await
        .expect("clean global state");
    sqlx::query("INSERT INTO global_state (site_id, initialized) VALUES ($1::uuid, TRUE)")
        .bind(SITE_ID)
        .execute(pool)
        .await
        .expect("seed global state");
}

async fn insert_event(pool: &PgPool, event: &tg::Event) {
    let timestamp = event
        .timestamp
        .as_ref()
        .map(|timestamp| timestamp.seconds)
        .unwrap_or(BASE);
    sqlx::query(
        "INSERT INTO telemetry_events_export_queue (id, timestamp, payload_pb) \
         VALUES ($1, to_timestamp($2), $3)",
    )
    .bind(&event.id)
    .bind(timestamp as f64)
    .bind(event.encode_to_vec())
    .execute(pool)
    .await
    .expect("insert queued event");
}

async fn queue_ids(pool: &PgPool) -> String {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT id FROM telemetry_events_export_queue ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("remaining ids");
    rows.into_iter()
        .map(|(id,)| id)
        .collect::<Vec<_>>()
        .join(",")
}

fn site(json: &str) -> SiteConfiguration {
    SiteConfiguration::parse(json).expect("site config")
}

fn live(json: &str) -> LiveConfig {
    LiveConfig::new(Unified {
        site: site(json),
        service_connections: Default::default(),
    })
}

// ---------------------------------------------------------------------------
// Scripted Telemetry Gateway

#[derive(Debug, Default)]
struct Recorded {
    request_id: Option<String>,
    identifier: Option<tg::Identifier>,
    chunks: Vec<Vec<String>>,
}

#[derive(Clone)]
struct Recorder {
    state: Arc<Mutex<Recorded>>,
    /// Acknowledge only the first N events of every payload.
    drop_last: bool,
}

#[tonic::async_trait]
impl tg::telemetery_gateway_service_server::TelemeteryGatewayService for Recorder {
    type RecordEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<tg::RecordEventsResponse, Status>>;

    async fn record_events(
        &self,
        request: Request<tonic::Streaming<tg::RecordEventsRequest>>,
    ) -> Result<Response<Self::RecordEventsStream>, Status> {
        let mut input = request.into_inner();
        let state = Arc::clone(&self.state);
        let drop_last = self.drop_last;
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            let mut succeeded = Vec::new();
            loop {
                match input.message().await {
                    Ok(Some(request)) => match request.payload {
                        Some(tg::record_events_request::Payload::Metadata(metadata)) => {
                            let mut state = state.lock().expect("state");
                            state.request_id = Some(metadata.request_id);
                            state.identifier = metadata.identifier;
                        }
                        Some(tg::record_events_request::Payload::Events(payload)) => {
                            let ids: Vec<String> = payload
                                .events
                                .iter()
                                .map(|event| event.id.clone())
                                .collect();
                            state.lock().expect("state").chunks.push(ids.clone());
                            let mut acknowledged = ids;
                            if drop_last {
                                acknowledged.pop();
                            }
                            succeeded.extend(acknowledged);
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
            let _ = tx
                .send(Ok(tg::RecordEventsResponse {
                    succeeded_events: succeeded,
                }))
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn record_event(
        &self,
        _request: Request<tg::RecordEventRequest>,
    ) -> Result<Response<tg::RecordEventResponse>, Status> {
        Err(Status::unimplemented("record_event"))
    }
}

struct ServerHandle {
    url: url::Url,
    state: Arc<Mutex<Recorded>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl ServerHandle {
    async fn start(drop_last: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake server");
        let address = listener.local_addr().expect("local address");
        let state = Arc::new(Mutex::new(Recorded::default()));
        let recorder = Recorder {
            state: Arc::clone(&state),
            drop_last,
        };
        let server = tonic::transport::Server::builder()
            .add_service(
                tg::telemetery_gateway_service_server::TelemeteryGatewayServiceServer::new(
                    recorder,
                ),
            )
            .serve_with_incoming(TcpListenerStream::new(listener));
        let task = tokio::spawn(server);
        Self {
            url: url::Url::parse(&format!("http://{address}/")).expect("server url"),
            state,
            task,
        }
    }

    fn recorded(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.state.lock().expect("state")
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// Scenarios

async fn run_queue_store(pool: &PgPool) {
    clean(pool).await;

    for (index, id) in ["ev-1", "ev-2", "ev-3", "ev-4", "ev-5"].iter().enumerate() {
        let event = event(id, BASE + 60 * index as i64, "feature");
        insert_event(pool, &event).await;
    }
    // A row whose payload is not a protobuf Event is skipped by the store.
    sqlx::query(
        "INSERT INTO telemetry_events_export_queue (id, timestamp, payload_pb) \
         VALUES ('corrupt', to_timestamp($1), $2)",
    )
    .bind((BASE + 360) as f64)
    .bind(b"x".to_vec())
    .execute(pool)
    .await
    .expect("insert corrupt row");

    let list = export_queue_json(pool, 10).await;
    println!("tgx queue list ids={list}");
    let list = export_queue_json(pool, 2).await;
    println!("tgx queue list_limit2 ids={list}");

    telemetry_events::mark_as_exported(pool, &["ev-1".to_owned()])
        .await
        .expect("mark ev-1");
    let (unexported, oldest) = telemetry_events::count_unexported(pool)
        .await
        .expect("count unexported");
    let recently = telemetry_events::count_recently_exported(pool)
        .await
        .expect("count recently exported");
    println!(
        "tgx queue after_mark unexported={unexported} oldest={} recently_exported={recently}",
        oldest.map(|value| value.unix_timestamp()).unwrap_or(0)
    );

    sqlx::query(
        "UPDATE telemetry_events_export_queue SET exported_at = '2020-01-01T00:00:00Z' \
         WHERE id = 'ev-1'",
    )
    .execute(pool)
    .await
    .expect("age ev-1");
    sqlx::query(
        "UPDATE telemetry_events_export_queue SET exported_at = '2025-01-01T00:00:00Z' \
         WHERE id = 'ev-2'",
    )
    .execute(pool)
    .await
    .expect("age ev-2");

    let before = time::OffsetDateTime::parse(
        "2021-01-01T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .expect("cutoff");
    let affected = telemetry_events::deleted_exported(pool, before)
        .await
        .expect("delete exported");
    let remaining = queue_ids(pool).await;
    println!("tgx queue after_delete affected={affected} remaining={remaining}");

    let (unexported, oldest) = telemetry_events::count_unexported(pool)
        .await
        .expect("count unexported");
    let recently = telemetry_events::count_recently_exported(pool)
        .await
        .expect("count recently exported");
    println!(
        "tgx queue final unexported={unexported} oldest={} recently_exported={recently}",
        oldest.map(|value| value.unix_timestamp()).unwrap_or(0)
    );
}

/// `Store.ListForExport` decodes payloads and skips corrupt rows.
async fn export_queue_json(pool: &PgPool, limit: i64) -> String {
    let rows = telemetry_events::list_for_export(pool, limit)
        .await
        .expect("list for export");
    rows.into_iter()
        .filter_map(|row| tg::Event::decode(row.payload_pb.as_slice()).ok())
        .map(|event| event.id)
        .collect::<Vec<_>>()
        .join(",")
}

async fn seed_export_queue(pool: &PgPool) {
    clean(pool).await;
    insert_event(pool, &event("ev-a", BASE, "cody")).await;
    insert_event(pool, &event("ev-b", BASE + 60, "cody")).await;
}

fn identifier_line(identifier: &tg::Identifier) -> String {
    match identifier.identifier.as_ref().expect("identifier") {
        tg::identifier::Identifier::LicensedInstance(identifier) => format!(
            "kind=licensed license_key={} instance_id={} external_url={}",
            identifier.license_key, identifier.instance_id, identifier.external_url
        ),
        tg::identifier::Identifier::UnlicensedInstance(identifier) => format!(
            "kind=unlicensed instance_id={} external_url={}",
            identifier.instance_id, identifier.external_url
        ),
        tg::identifier::Identifier::ManagedService(_) => "kind=managed_service".to_owned(),
    }
}

async fn run_export(pool: &PgPool) {
    // Unlicensed instance: the identifier carries the site ID.
    seed_export_queue(pool).await;
    let live_config = live(r#"{"externalURL":"https://example.com"}"#);
    let server = ServerHandle::start(false).await;
    let events = export_queue_events(pool).await;
    let (succeeded, error) = export_events(pool, &live_config, &server.url, &events).await;
    assert!(error.is_none(), "{error:?}");
    {
        let recorded = server.recorded();
        println!(
            "tgx export metadata {} request_id={}",
            identifier_line(recorded.identifier.as_ref().expect("identifier")),
            if recorded
                .request_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
            {
                "set"
            } else {
                "missing"
            }
        );
        for (index, chunk) in recorded.chunks.iter().enumerate() {
            println!("tgx export chunk chunk={index} ids={}", chunk.join(","));
        }
    }
    println!("tgx export succeeded ids={}", succeeded.join(","));

    telemetry_events::mark_as_exported(pool, &succeeded)
        .await
        .expect("mark exported");
    let (unexported, _) = telemetry_events::count_unexported(pool)
        .await
        .expect("count unexported");
    let recently = telemetry_events::count_recently_exported(pool)
        .await
        .expect("count recently exported");
    println!("tgx export queue unexported={unexported} exported={recently}");

    // A licensed instance identifies with the license key.
    seed_export_queue(pool).await;
    let live_config = live(r#"{"licenseKey":"lk","externalURL":"https://example.com"}"#);
    let server = ServerHandle::start(false).await;
    let events = export_queue_events(pool).await;
    let (succeeded, error) = export_events(pool, &live_config, &server.url, &events).await;
    assert!(error.is_none(), "{error:?}");
    println!(
        "tgx export licensed metadata {}",
        identifier_line(server.recorded().identifier.as_ref().expect("identifier"))
    );
    println!("tgx export licensed succeeded ids={}", succeeded.join(","));

    // The server acknowledges one event fewer than submitted.
    seed_export_queue(pool).await;
    let live_config = live(r#"{"externalURL":"https://example.com"}"#);
    let server = ServerHandle::start(true).await;
    let events = export_queue_events(pool).await;
    let (succeeded, error) = export_events(pool, &live_config, &server.url, &events).await;
    println!(
        "tgx export shortfall succeeded={} error={}",
        succeeded.join(","),
        mask_request_id(error.expect("expected error"))
    );
}

async fn export_queue_events(pool: &PgPool) -> Vec<tg::Event> {
    telemetry_events::list_for_export(pool, 1000)
        .await
        .expect("list for export")
        .into_iter()
        .filter_map(|row| tg::Event::decode(row.payload_pb.as_slice()).ok())
        .collect()
}

/// `request "<uuid>": ...` -> `request <request-id>: ...`.
fn mask_request_id(message: String) -> String {
    match (message.find("request \""), message.find("\": ")) {
        (Some(start), Some(end)) if start < end => {
            format!(
                "{}request <request-id>{}",
                &message[..start],
                &message[end + 1..]
            )
        }
        _ => message,
    }
}

#[test]
fn chunker_boundary_is_one_mebibyte() {
    assert_eq!(MAX_MESSAGE_SIZE, 1024 * 1024);
}

#[tokio::test]
async fn telemetry_gateway_exporter_harness() {
    let Ok(dsn) = std::env::var("SG_TGX_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2)
        .await
        .expect("connect scratch database");

    run_queue_store(&pool).await;
    run_export(&pool).await;
}
