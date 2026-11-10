//! M9.8gb differential of `Query.temporarySettings`,
//! `Mutation.overwriteTemporarySettings` and `Mutation.editTemporarySettings`
//! (`cmd/frontend/graphqlbackend/temporary_settings.go:10-52` and
//! `internal/database/temporary_settings.go:30-80`) against graph-gophers + the
//! real resolvers, backed by the same scratch database rows.
//!
//! Each case optionally seeds its actor's row, executes the query, then writes
//! `{"response": …, "row": …}` where `row` is `contents::text` (JSON null when
//! absent or SQL NULL). GraphQL error messages are masked.
//!
//! Env:
//!
//! * `SG_M9GB_DIFF=1`
//! * `M9GB_CORPUS` — JSON `[{name, query, variables?, uid?, row?, seed?}]`
//! * `M9GB_OUT` — output directory (one `<name>.json` per case)
//! * `M9GB_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::settings_mutation::MutationRoot;
use sg_service_frontend::temporary_settings::{
    prefetch_temporary_settings, prefetch_temporary_settings_mutations,
};

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query {
  temporarySettings: TemporarySettings!
}

type Mutation {
  overwriteTemporarySettings(contents: String!): EmptyResponse!
  editTemporarySettings(settingsToEdit: String!): EmptyResponse!
}

type TemporarySettings {
  contents: String!
}

type EmptyResponse {
  alwaysNil: String
}
";

/// Field order matches the Go oracle's `m9gbEnvelope` struct.
#[derive(Serialize)]
struct Envelope {
    response: Value,
    row: Option<String>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    uid: i32,
    #[serde(default, rename = "row")]
    seed_row: bool,
    #[serde(default)]
    seed: Option<String>,
}

/// `{"message": "error"}` for every GraphQL error.
fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        for error in errors {
            *error = json!({"message": "error"});
        }
    }
    value
}

#[tokio::test]
async fn temporary_settings_harness() {
    if std::env::var("SG_M9GB_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GB_CORPUS").expect("M9GB_CORPUS");
    let out = std::env::var("M9GB_OUT").expect("M9GB_OUT");
    let dsn = std::env::var("M9GB_DSN").expect("M9GB_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    for case in cases {
        sqlx::query("DELETE FROM temporary_settings WHERE user_id = $1")
            .bind(case.uid)
            .execute(&pool)
            .await
            .expect("delete row");
        if case.seed_row {
            sqlx::query(
                "INSERT INTO temporary_settings (user_id, contents) VALUES ($1, $2::jsonb)",
            )
            .bind(case.uid)
            .bind(case.seed.as_deref())
            .execute(&pool)
            .await
            .expect("seed row");
        }

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
        let read = prefetch_temporary_settings(&pool, case.uid, &fields).await;
        let mut mutation = MutationRoot::default();
        for (field, outcome) in
            prefetch_temporary_settings_mutations(&pool, case.uid, &fields).await
        {
            mutation = mutation.with_temporary_settings(field, outcome);
        }
        let roots = FrontendRootResolver::new()
            .with_temporary_settings(read)
            .with_mutation(mutation);
        let response = execute_with(&schema, request, Some(&roots));

        let row: Option<String> = sqlx::query_scalar(
            "SELECT contents::text FROM temporary_settings WHERE user_id = $1 LIMIT 1",
        )
        .bind(case.uid)
        .fetch_optional(&pool)
        .await
        .expect("dump row")
        .flatten();

        let envelope = Envelope {
            response: mask_response(&response.to_json()),
            row,
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
