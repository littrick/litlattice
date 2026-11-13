//! M9.25c differential of the GitHub Apps GraphQL read surface against the Go
//! baseline (`cmd/frontend/internal/githubapp` + graph-gophers + the real
//! Postgres store).
//!
//! Both sides read the same scratch database (seeded by
//! `run_github_apps_diff.sh` immediately before each side runs) and execute the
//! same query strings from `data/github_apps/corpus.json`. The Rust side
//! assembles the vendored `githubapps.graphql` on top of the main OSS schema
//! and resolves through the production `FrontendRootResolver`.
//!
//! Env:
//!
//! * `SG_M925C_GITHUB_APPS_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M925C_CORPUS` — corpus JSON path
//! * `M925C_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{
    assemble, GITHUB_APPS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA,
};
use sg_service_frontend::github_apps_graphql::GitHubAppsHandle;
use sg_service_frontend::github_apps_graphql_store::GitHubAppsPgDatabase;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;

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
async fn github_apps_graphql_harness() {
    if std::env::var("SG_M925C_GITHUB_APPS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M925C_CORPUS").expect("M925C_CORPUS");
    let out = std::env::var("M925C_OUT").expect("M925C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    // Match `NewSchemaWithBatchChangesResolver(nil batches, githubApps)`'s
    // document order: the three OSS documents, then `githubapps.graphql`.
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        GITHUB_APPS_SCHEMA,
    ])
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
        let database = Arc::new(GitHubAppsPgDatabase::new(
            pool.clone(),
            Arc::new(sg_keyring::Keyring::default()),
        ));
        let github_apps: GitHubAppsHandle = database;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
        .with_github_apps(Some(github_apps));
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
