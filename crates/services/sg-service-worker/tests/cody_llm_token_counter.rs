#![forbid(unsafe_code)]

//! M9.8y differential harness (Rust side): the `cody-llm-token-counter` job.
//!
//! Mirrors `cmd/worker/internal/completions/tokens_harness_test.go`.
//! `test_ws/p9-go/run_cody_token_diff.sh` seeds the same `v2:LLMUsage:*`
//! Redis keys and truncates `event_logs` + `telemetry_events_export_queue`
//! before each side and diffs the canonical lines:
//!
//! * `jjob`/`jconfig`: the job's description and config slice;
//! * `jname`/`jdesc`/`jinterval`: the routine contract;
//! * `run`: the outcome of the handler;
//! * `elog`: the `event_logs` row (random IDs and timestamps normalized);
//! * `qevent`: the export-queue row with its protobuf payload decoded.
//!
//! The handler under test is the production [`TokenUsageHandler`]: it reads
//! Redis through the real `sg_rcache` namespace and records through the real
//! [sg_service_worker::telemetry] recorder.

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message;
use sg_proto::telemetrygateway::Event;
use sg_redis::KeyValue;
use sg_service_worker::framework::periodic::Handler;
use sg_service_worker::jobs::completions::{CodyLlmTokenCounter, TokenUsageHandler};
use sg_service_worker::jobs::{Job, JobDeps};
use sg_service_worker::telemetry::json_number;
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn cody_llm_token_counter_matches_go_harness() {
    if std::env::var("SG_CODY_TOKEN_HARNESS_ON").ok().as_deref() != Some("1") {
        eprintln!("SG_CODY_TOKEN_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_CODY_TOKEN_DSN").expect("SG_CODY_TOKEN_DSN is not set");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    // 1. Job metadata.
    let job = CodyLlmTokenCounter;
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    println!("jname name={}", routines[0].name());
    println!("jdesc description={:?}", routines[0].description());
    println!("jinterval secs={}", routines[0].interval().as_secs());

    // 2. The handler body over the real Redis namespace and recorder.
    let kv = KeyValue::connect(&sg_redis::store_endpoint())
        .await
        .expect("connect to REDIS_STORE_ENDPOINT");
    let handler = TokenUsageHandler::with_cache(
        pool.clone(),
        Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
        kv,
    );
    match handler.handle(CancellationToken::new()).await {
        Ok(()) => println!("run err=ok"),
        Err(error) => println!("run err={}", error.message()),
    }

    // 3. The two stores' rows.
    print_event_logs(&pool).await;
    print_export_queue(&pool).await;
}

async fn print_event_logs(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT name, url, user_id, anonymous_user_id, source, argument::text AS argument, \
                public_argument::text AS public_argument, version, timestamp, \
                feature_flags::text AS feature_flags, \
                COALESCE(cohort_id::text, 'NULL') AS cohort_id, \
                COALESCE(first_source_url, 'NULL') AS first_source_url, \
                COALESCE(last_source_url, 'NULL') AS last_source_url, \
                COALESCE(referrer, 'NULL') AS referrer, \
                device_id, insert_id, COALESCE(client, 'NULL') AS client, \
                COALESCE(billing_product_category, 'NULL') AS billing_product_category, \
                COALESCE(billing_event_id, 'NULL') AS billing_event_id \
         FROM event_logs \
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("event_logs");

    for row in rows {
        println!(
            "elog name={} url=\"{}\" user={} anon={} source={} argument={} public={} version={} ts={} ff={} cohort={} first={} last={} referrer={} device={} insert={} client={} billing_product={} billing_event={}",
            row.get::<String, _>("name"),
            row.get::<String, _>("url"),
            row.get::<i32, _>("user_id"),
            row.get::<String, _>("anonymous_user_id"),
            row.get::<String, _>("source"),
            row.get::<String, _>("argument"),
            row.get::<String, _>("public_argument"),
            row.get::<String, _>("version"),
            normalize_time(row.get::<sg_db::OffsetDateTime, _>("timestamp")),
            row.get::<String, _>("feature_flags"),
            row.get::<String, _>("cohort_id"),
            row.get::<String, _>("first_source_url"),
            row.get::<String, _>("last_source_url"),
            row.get::<String, _>("referrer"),
            normalize_uuid(&row.get::<String, _>("device_id")),
            normalize_uuid(&row.get::<String, _>("insert_id")),
            row.get::<String, _>("client"),
            row.get::<String, _>("billing_product_category"),
            row.get::<String, _>("billing_event_id"),
        );
    }
}

async fn print_export_queue(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, timestamp, payload_pb FROM telemetry_events_export_queue ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("export queue");

    for row in rows {
        let id: String = row.get("id");
        let timestamp: sg_db::OffsetDateTime = row.get("timestamp");
        let payload: Vec<u8> = row.get("payload_pb");
        let event = Event::decode(payload.as_slice()).expect("decode payload_pb");

        let parameters = event.parameters.clone().unwrap_or_default();
        let mut metadata = serde_json::Map::new();
        let sorted: BTreeMap<&String, &f64> = parameters.metadata.iter().collect();
        for (key, value) in sorted {
            metadata.insert(key.clone(), json_number(*value));
        }
        let metadata = serde_json::Value::Object(metadata);

        let source_version = event
            .source
            .as_ref()
            .and_then(|source| source.server.as_ref())
            .map(|server| server.version.clone())
            .unwrap_or_else(|| "<nil>".to_owned());
        let source_client = event
            .source
            .as_ref()
            .and_then(|source| source.client.as_ref())
            .map(|client| {
                format!(
                    "{}:{}",
                    client.name,
                    client.version.clone().unwrap_or_default()
                )
            })
            .unwrap_or_else(|| "NULL".to_owned());
        let user = event
            .user
            .as_ref()
            .map(|user| {
                format!(
                    "{}:{}",
                    user.user_id.unwrap_or(0),
                    user.anonymous_user_id.clone().unwrap_or_default()
                )
            })
            .unwrap_or_else(|| "NULL".to_owned());
        let flags = event
            .feature_flags
            .as_ref()
            .map(|flags| {
                let sorted: BTreeMap<&String, &String> = flags.flags.iter().collect();
                let inner = sorted
                    .iter()
                    .map(|(key, value)| format!("{key}:{value}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("map[{inner}]")
            })
            .unwrap_or_else(|| "NULL".to_owned());
        let interaction = event
            .interaction
            .as_ref()
            .map(|interaction| {
                format!(
                    "{}:{}",
                    interaction.trace_id.clone().unwrap_or_default(),
                    interaction.interaction_id.clone().unwrap_or_default()
                )
            })
            .unwrap_or_else(|| "NULL".to_owned());
        let billing = parameters
            .billing_metadata
            .as_ref()
            .map(|billing| format!("{}:{}", billing.product, billing.category))
            .unwrap_or_else(|| "NULL".to_owned());

        println!(
            "qevent id={} ts={} feature={} action={} pver={} metadata={} private={} billing={} source_version={} source_client={} user={} flags={} interaction={}",
            normalize_uuid(&id),
            normalize_time(timestamp),
            event.feature,
            event.action,
            parameters.version,
            metadata,
            parameters.private_metadata.is_some(),
            billing,
            source_version,
            source_client,
            user,
            flags,
            interaction,
        );
    }
}

/// The baseline prints the UUID version (event IDs are v7, `device_id` and
/// `insert_id` are v4) so the random components never reach the diff.
fn normalize_uuid(value: &str) -> String {
    match Uuid::parse_str(value) {
        Ok(uuid) => format!("uuid{}", uuid.get_version_num()),
        Err(_) => value.to_owned(),
    }
}

fn normalize_time(value: sg_db::OffsetDateTime) -> String {
    let delta = sg_db::OffsetDateTime::now_utc() - value;
    if delta.abs() < time::Duration::minutes(10) {
        return "now".to_owned();
    }
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "invalid".to_owned())
}
