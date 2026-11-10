//! M9.8fz differential of `Mutation.logEvent` / `Mutation.logEvents` /
//! `Mutation.logUserEvent` (`cmd/frontend/graphqlbackend/user_usage_stats.go`)
//! against graph-gophers + the real resolvers, `usagestats.LogEvents` and
//! `eventLogs.BulkInsert`, backed by a scratch database and `redispool.Store`.
//!
//! Each case writes `<name>.json` as
//! `{"response": …, "rows": […], "redis": {"before": …, "after": …}}`.
//!
//! Nondeterministic data is normalized on both sides:
//!
//! * GraphQL error messages are masked (`{"message": "<error>"}`) because
//!   serde and Go `encoding/json` report different texts;
//! * `event_logs.id` and `timestamp` are omitted;
//! * `device_id`/`insert_id` are emitted as `"uuid"` when the stored value is
//!   a generated v4 UUID, and verbatim otherwise (the corpus uses non-UUID
//!   provided ids).
//!
//! Env:
//!
//! * `SG_M9FZ_DIFF=1`
//! * `M9FZ_CORPUS` — JSON `[{name, siteConfig, query, uid?, variables?}]`
//! * `M9FZ_OUT` — output directory (one `<name>.json` per case)
//! * `M9FZ_DSN` — postgres DSN of the scratch database
//! * `M9FZ_REDIS` — Redis store endpoint (defaults to `REDIS_STORE_ENDPOINT`)

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::{PgPool, Row};

use sg_config::SiteConfiguration;
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::log_events::prefetch_log_events;
use sg_service_frontend::settings_mutation::MutationRoot;

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query {
  noop: String
}

type Mutation {
  logUserEvent(event: UserEvent!, userCookieID: String!): EmptyResponse
  logEvent(
    event: String!
    userCookieID: String!
    firstSourceURL: String
    lastSourceURL: String
    url: String!
    source: EventSource!
    cohortID: String
    referrer: String
    originalReferrer: String
    sessionReferrer: String
    sessionFirstURL: String
    deviceSessionID: String
    argument: String
    publicArgument: String
    deviceID: String
    eventID: Int
    insertID: String
    client: String
    billingProductCategory: String
    billingEventID: String
    connectedSiteID: String
    hashedLicenseKey: String
  ): EmptyResponse
  logEvents(events: [Event!]): EmptyResponse
}

input Event {
  event: String!
  userCookieID: String!
  firstSourceURL: String
  lastSourceURL: String
  url: String!
  source: EventSource!
  cohortID: String
  referrer: String
  originalReferrer: String
  sessionReferrer: String
  sessionFirstURL: String
  deviceSessionID: String
  argument: String
  publicArgument: String
  deviceID: String
  eventID: Int
  insertID: String
  client: String
  billingProductCategory: String
  billingEventID: String
  connectedSiteID: String
  hashedLicenseKey: String
}

enum EventSource {
  WEB
  CODEHOSTINTEGRATION
  BACKEND
  STATICWEB
  IDEEXTENSION
  CODY
}

enum UserEvent {
  PAGEVIEW
  SEARCHQUERY
  CODEINTEL
  CODEINTELREFS
  CODEINTELINTEGRATION
  CODEINTELINTEGRATIONREFS
  STAGEMANAGE
  STAGEPLAN
  STAGECODE
  STAGEREVIEW
  STAGEVERIFY
  STAGEPACKAGE
  STAGEDEPLOY
  STAGECONFIGURE
  STAGEMONITOR
  STAGESECURE
}

type EmptyResponse {
  alwaysNil: String
}
";

/// Field order matches the Go oracle's `m9fzEnvelope` struct.
#[derive(Serialize)]
struct Envelope {
    response: Value,
    rows: Value,
    redis: Value,
}

const SEARCH_OCCURRED_KEY: &str = "user_activity:searchoccurred";
const FIND_REFS_OCCURRED_KEY: &str = "user_activity:findrefsoccurred";

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    uid: i32,
    #[serde(rename = "siteConfig")]
    site_config: Value,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
}

/// `{"message": "<error>"}` for every GraphQL error.
fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        for error in errors {
            *error = json!({"message": "error"});
        }
    }
    value
}

/// A generated UUID is replaced with `"uuid"`; provided (non-UUID) ids stay.
fn normalize_id(value: Option<String>) -> Value {
    match value {
        None => Value::Null,
        Some(text) if uuid::Uuid::parse_str(&text).is_ok() => Value::String("uuid".to_owned()),
        Some(text) => Value::String(text),
    }
}

fn dump_rows(rows: &[sqlx::postgres::PgRow]) -> Value {
    let mut encoded = Vec::with_capacity(rows.len());
    for row in rows {
        encoded.push(json!({
            "name": row.try_get::<String, _>("name").expect("name"),
            "url": row.try_get::<String, _>("url").expect("url"),
            "user_id": row.try_get::<i32, _>("user_id").expect("user_id"),
            "anonymous_user_id": row.try_get::<String, _>("anonymous_user_id").expect("anonymous_user_id"),
            "source": row.try_get::<String, _>("source").expect("source"),
            "argument": row.try_get::<String, _>("argument").expect("argument"),
            "public_argument": row.try_get::<String, _>("public_argument").expect("public_argument"),
            "version": row.try_get::<String, _>("version").expect("version"),
            "feature_flags": row.try_get::<String, _>("feature_flags").expect("feature_flags"),
            "cohort_id": row.try_get::<Option<String>, _>("cohort_id").expect("cohort_id"),
            "first_source_url": row.try_get::<Option<String>, _>("first_source_url").expect("first_source_url"),
            "last_source_url": row.try_get::<Option<String>, _>("last_source_url").expect("last_source_url"),
            "referrer": row.try_get::<Option<String>, _>("referrer").expect("referrer"),
            "device_id": normalize_id(row.try_get::<Option<String>, _>("device_id").expect("device_id")),
            "insert_id": normalize_id(row.try_get::<Option<String>, _>("insert_id").expect("insert_id")),
            "client": row.try_get::<Option<String>, _>("client").expect("client"),
            "billing_product_category": row.try_get::<Option<String>, _>("billing_product_category").expect("billing_product_category"),
            "billing_event_id": row.try_get::<Option<String>, _>("billing_event_id").expect("billing_event_id"),
        }));
    }
    Value::Array(encoded)
}

async fn redis_state(kv: &sg_redis::KeyValue) -> Value {
    let search = kv
        .get_string(SEARCH_OCCURRED_KEY)
        .await
        .expect("search flag");
    let find_refs = kv
        .get_string(FIND_REFS_OCCURRED_KEY)
        .await
        .expect("findrefs flag");
    json!({"searchoccurred": search, "findrefsoccurred": find_refs})
}

#[tokio::test]
async fn log_events_harness() {
    if std::env::var("SG_M9FZ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FZ_CORPUS").expect("M9FZ_CORPUS");
    let out = std::env::var("M9FZ_OUT").expect("M9FZ_OUT");
    let dsn = std::env::var("M9FZ_DSN").expect("M9FZ_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let redis_endpoint = std::env::var("M9FZ_REDIS")
        .ok()
        .filter(|endpoint| !endpoint.is_empty())
        .unwrap_or_else(sg_redis::store_endpoint);
    let kv = sg_redis::KeyValue::connect(&redis_endpoint)
        .await
        .expect("redis connect");

    for case in cases {
        let site = SiteConfiguration::parse(&case.site_config.to_string()).expect("site config");
        let before = redis_state(&kv).await;
        let max_id: Option<i64> = sqlx::query_scalar("SELECT MAX(id) FROM event_logs")
            .fetch_one(&pool)
            .await
            .expect("max id");

        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &case.variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let outcomes = prefetch_log_events(&pool, Some(&kv), &site, case.uid, &fields).await;
        let mut mutation = MutationRoot::default();
        for (field, outcome) in outcomes {
            mutation = mutation.with_log_events(field, outcome);
        }
        let roots = FrontendRootResolver::new().with_mutation(mutation);
        let response = execute_with(&schema, request, Some(&roots));
        let response_json = mask_response(&response.to_json());

        let after = redis_state(&kv).await;

        let rows = sqlx::query(
            "SELECT name, url, user_id, anonymous_user_id, source, argument::text AS argument, \
                    public_argument::text AS public_argument, version, feature_flags::text AS feature_flags, \
                    cohort_id::text AS cohort_id, first_source_url, last_source_url, referrer, \
                    device_id, insert_id, client, billing_product_category, billing_event_id \
             FROM event_logs WHERE id > $1 ORDER BY id",
        )
        .bind(max_id.unwrap_or(0))
        .fetch_all(&pool)
        .await
        .expect("event rows");

        let envelope = Envelope {
            response: response_json,
            rows: dump_rows(&rows),
            redis: json!({"before": before, "after": after}),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
