//! M9.27a differential of the Teams GraphQL surface against the Go baseline
//! (`cmd/frontend/graphqlbackend/teams.go` + graph-gophers + real Postgres).
//!
//! Both sides share the scratch database seeded by `run_teams_diff.sh`
//! immediately before each side runs and execute the queries/mutations from
//! `data/teams/corpus.json` in corpus order (writes accumulate). Each case
//! records the normalized GraphQL response plus a deterministic snapshot of
//! `teams` / `team_members` / `names`.
//!
//! Env: `SG_M927A_TEAMS_DIFF=1`, `SG_QTEST_DSN`, `M927A_CORPUS`, `M927A_OUT`.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Row};

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA};
use sg_service_frontend::graphql_resolvers::{FrontendRootResolver, UserObject};
use sg_service_frontend::teams_graphql::{TeamsGraphqlDatabase, TeamsHandle};
use sg_service_frontend::teams_store::PgTeamsGraphqlStore;

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    state: Vec<StateQuery>,
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
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    own_disabled: bool,
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
                    .map(|error| {
                        // `TeamNotFoundError` renders its `args` as a list of
                        // `*sqlf.Query` pointers on the Go side, which is
                        // non-deterministic; the oracle keeps only the stable
                        // prefix and so do we.
                        let message = match error.message.find("team not found: ") {
                            Some(idx) => format!("{}team not found", &error.message[..idx]),
                            None => error.message.clone(),
                        };
                        serde_json::json!({ "message": message })
                    })
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
async fn teams_harness() {
    if std::env::var("SG_M927A_TEAMS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M927A_CORPUS").expect("M927A_CORPUS");
    let out = std::env::var("M927A_OUT").expect("M927A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA])
        .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &corpus.cases {
        let store = Arc::new(
            PgTeamsGraphqlStore::new(pool.clone()).with_own_enabled(!case.own_disabled),
        );
        let (actor, viewer, viewer_site_admin) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None, false),
            _ => {
                let data = store
                    .user_by_id(case.uid)
                    .expect("viewer lookup")
                    .expect("viewer row");
                let site_admin = data.site_admin;
                let object = UserObject::from_data_as_viewer(data, site_admin);
                (Actor::User { uid: case.uid }, Some(object), site_admin)
            }
        };
        let handle: TeamsHandle = store;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            viewer,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_teams(Some(handle));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
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
                let mut item: BTreeMap<String, Value> = BTreeMap::new();
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
                items.push(Value::Object(item.into_iter().collect()));
            }
            state.insert(query.name.clone(), Value::Array(items));
        }

        let mut envelope: BTreeMap<String, Value> = BTreeMap::new();
        envelope.insert("response".to_owned(), normalized(&response));
        envelope.insert(
            "state".to_owned(),
            Value::Object(state.into_iter().collect()),
        );
        let body = serde_json::to_string(&envelope).unwrap();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
