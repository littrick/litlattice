//! M9.25b differential of the Batch Changes GraphQL *write* surface against the
//! Go baseline (`cmd/frontend/internal/batches/resolvers` + graph-gophers +
//! the real batches Postgres store).
//!
//! Both sides share the scratch database seeded by
//! `run_batches_mutations_diff.sh` immediately before each side runs and execute
//! the mutations from `data/batches_mutations/corpus.json` in corpus order.
//! Each case records the normalized GraphQL response plus a deterministic
//! snapshot of `batch_changes` / `changesets` / `changeset_jobs`.
//!
//! Env:
//!
//! * `SG_M925B_BATCHES_MUTATIONS_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M925B_CORPUS` — corpus JSON path
//! * `M925B_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{
    assemble, BATCHES_SCHEMA, GITHUB_APPS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA,
    VIEWER_SCHEMA,
};
use sg_service_frontend::batches_graphql::BatchesHandle;
use sg_service_frontend::batches_graphql_store::PgBatchesGraphqlStore;
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, UserData, UserObject,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    state: Vec<StateQuery>,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct StateQuery {
    name: String,
    sql: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    mutation: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default = "default_enabled")]
    batch_changes_enabled: bool,
    #[serde(default)]
    restrict_to_admins: bool,
}

fn default_enabled() -> bool {
    true
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn user_data(id: i32) -> UserData {
    UserData {
        id,
        username: format!("m925b-user-{id}"),
        display_name: String::new(),
        avatar_url: String::new(),
        created_at: parse_time("2024-01-01T00:00:00Z"),
        updated_at: parse_time("2024-01-01T00:00:00Z"),
        site_admin: id == 925003,
        tos_accepted: true,
        builtin_auth: false,
        scim_controlled: false,
        primary_email: String::new(),
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> Value {
    let mut envelope: BTreeMap<String, Value> = BTreeMap::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors".to_owned(),
            Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        );
    }
    if let Some(data) = response.data_json() {
        envelope.insert("data".to_owned(), serde_json::from_str(&data).unwrap());
    }
    Value::Object(envelope.into_iter().collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_mutations_harness() {
    if std::env::var("SG_M925B_BATCHES_MUTATIONS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M925B_CORPUS").expect("M925B_CORPUS");
    let out = std::env::var("M925B_OUT").expect("M925B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        BATCHES_SCHEMA,
        GITHUB_APPS_SCHEMA,
    ])
    .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &corpus.cases {
        let (actor, viewer) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => {
                let data = user_data(case.uid);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let store = PgBatchesGraphqlStore::new(pool.clone())
            .with_batch_changes_enabled(case.batch_changes_enabled)
            .with_batch_changes_restricted_to_admins(case.restrict_to_admins);
        let batches: BatchesHandle = Arc::new(store);
        let roots = FrontendRootResolver::with_prefetched_organizations(
            viewer,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            case.uid == 925003,
        )
        .with_batches(Some(batches));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.mutation,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));

        let mut state: BTreeMap<String, Value> = BTreeMap::new();
        for query in &corpus.state {
            let rows = sqlx::query(&query.sql)
                .fetch_all(&pool)
                .await
                .expect("state query");
            let mut items = Vec::with_capacity(rows.len());
            for row in &rows {
                let columns = row.columns();
                let mut item = Map::new();
                for (index, column) in columns.iter().enumerate() {
                    let value: Option<String> = row.try_get(index).expect("text column");
                    item.insert(
                        column.name().to_owned(),
                        match value {
                            Some(value) => Value::String(value),
                            None => Value::Null,
                        },
                    );
                }
                items.push(Value::Object(item));
            }
            state.insert(query.name.clone(), Value::Array(items));
        }

        let mut envelope: BTreeMap<String, Value> = BTreeMap::new();
        envelope.insert("response".to_owned(), normalized(&response));
        envelope.insert("state".to_owned(), Value::Object(state.into_iter().collect()));
        let body = serde_json::to_string(&envelope).unwrap();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
