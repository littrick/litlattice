//! M9.27b differential of the outbound webhook GraphQL surface against the Go
//! baseline (`cmd/frontend/graphqlbackend/outbound_webhooks.go` + graph-gophers
//! + the real Postgres store).
//!
//! Both sides read the same scratch database (seeded by
//! `run_outbound_webhooks_diff.sh` immediately before each side runs) and
//! execute the same query strings from `data/outbound_webhooks/corpus.json`.
//! The Rust side assembles the default OSS schema (`MAIN_SCHEMA` +
//! `OUTBOUND_WEBHOOKS_SCHEMA` + `VIEWER_SCHEMA`, as `assemble_oss()` does) and
//! resolves through the production `FrontendRootResolver`.
//!
//! M9.46a extends the same corpus with `OutboundWebhook.stats` /
//! `.logs` cases (existing `outbound_webhook_logs` table).
//!
//! Env:
//!
//! * `SG_M946A_OUTBOUND_WEBHOOK_LOGS_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M946A_CORPUS` — corpus JSON path
//! * `M946A_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA};
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::outbound_webhooks_graphql::OutboundWebhooksHandle;
use sg_service_frontend::outbound_webhooks_store::OutboundWebhooksPgDatabase;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut envelope: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        );
    }
    if let Some(data) = response.data_json() {
        envelope.insert("data", serde_json::from_str(&data).unwrap());
    }
    serde_json::to_string(&envelope).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn outbound_webhooks_graphql_harness() {
    if std::env::var("SG_M946A_OUTBOUND_WEBHOOK_LOGS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M946A_CORPUS").expect("M946A_CORPUS");
    let out = std::env::var("M946A_OUT").expect("M946A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    // `NewSchema`'s always-present documents, in order.
    let schema = assemble(&[MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA])
        .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &corpus.cases {
        let actor = match case.viewer.as_str() {
            "anonymous" | "" => Actor::Anonymous,
            _ => Actor::User { uid: case.uid },
        };
        // The site-admin gate reads `users.site_admin` from the same database
        // the Go side reads; no prefetched viewer is involved.
        let database = Arc::new(OutboundWebhooksPgDatabase::new(
            pool.clone(),
            Arc::new(sg_keyring::Keyring::default()),
        ));
        let outbound_webhooks: OutboundWebhooksHandle = database;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
        .with_outbound_webhooks(Some(outbound_webhooks));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
