//! M9.8ft differential of `Mutation.settingsMutation` /
//! `Mutation.configurationMutation` (editSettings / editConfiguration /
//! overwriteSettings) against graph-gophers + the real
//! `schemaResolver.SettingsMutation` (`settings_mutation.go`),
//! `jsonx.ComputePropertyEdit`/`ComputePropertyRemoval`+`ApplyEdits` and
//! `settingsCreateIfUpToDate` -> `settingsStore.CreateIfUpToDate`, backed by a
//! scratch database.
//!
//! Each case writes `<name>.json` as `<response JSON>\n<settings rows JSON>`.
//!
//! Env:
//!
//! * `SG_M9FT_DIFF=1`
//! * `M9FT_CORPUS` — JSON `[{name, query, uid?, variables?, env?}]`
//! * `M9FT_OUT` — output directory (one `<name>.json` per case)
//! * `M9FT_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::Map;
use sqlx::{PgPool, Row};

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::json::Json;
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::settings_mutation::prefetch_settings_mutation;

const SDL: &str = "
schema { query: Query mutation: Mutation }

scalar JSONValue

type Query {
  noop: String
}

type Mutation {
  settingsMutation(input: SettingsMutationGroupInput!): SettingsMutation
  configurationMutation(input: SettingsMutationGroupInput!): SettingsMutation
}

input SettingsMutationGroupInput {
  subject: ID!
  lastID: Int
}

type SettingsMutation {
  editSettings(edit: SettingsEdit!): UpdateSettingsPayload
  editConfiguration(edit: ConfigurationEdit!): UpdateSettingsPayload
  overwriteSettings(contents: String!): UpdateSettingsPayload
}

input SettingsEdit {
  keyPath: [KeyPathSegment!]!
  value: JSONValue
  valueIsJSONCEncodedString: Boolean = false
}

input ConfigurationEdit {
  keyPath: [KeyPathSegment!]!
  value: JSONValue
  valueIsJSONCEncodedString: Boolean = false
}

input KeyPathSegment {
  property: String
  index: Int
}

type UpdateSettingsPayload {
  empty: EmptyResponse
}

type EmptyResponse {
  alwaysNil: String
}
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    variables: Map<String, serde_json::Value>,
    #[serde(default)]
    env: HashMap<String, String>,
}

async fn site_admin(pool: &PgPool, uid: i32) -> bool {
    if uid == 0 {
        return false;
    }
    sqlx::query_scalar::<_, bool>(
        "SELECT site_admin FROM users WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await
    .expect("site_admin query")
    .unwrap_or(false)
}

fn opt_id(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

/// `settings` rows with id >= 9580 in id order, in Go struct-field order
/// (`id`, `org_id`, `user_id`, `author_user_id`, `contents`); `created_at` is
/// excluded because both runs create rows at different times.
fn dump_settings(rows: &[sqlx::postgres::PgRow]) -> String {
    let mut encoded = Vec::with_capacity(rows.len());
    for row in rows {
        let body = format!(
            "{{\"id\":{},\"org_id\":{},\"user_id\":{},\"author_user_id\":{},\"contents\":{}}}",
            row.try_get::<i32, _>("id").expect("id"),
            opt_id(row.try_get::<Option<i32>, _>("org_id").expect("org_id")),
            opt_id(row.try_get::<Option<i32>, _>("user_id").expect("user_id")),
            opt_id(
                row.try_get::<Option<i32>, _>("author_user_id")
                    .expect("author_user_id")
            ),
            Json::Str(row.try_get::<String, _>("contents").expect("contents")).to_compact_string(),
        );
        encoded.push(body);
    }
    format!("[{}]", encoded.join(","))
}

fn apply_env(env: &HashMap<String, String>) -> Vec<(String, Option<String>)> {
    let mut restore = Vec::with_capacity(env.len());
    for key in env.keys() {
        restore.push((key.clone(), std::env::var(key).ok()));
    }
    for (key, value) in env {
        std::env::set_var(key, value);
    }
    restore
}

fn restore_env(restore: Vec<(String, Option<String>)>) {
    for (key, value) in restore {
        match value {
            Some(value) => std::env::set_var(&key, value),
            None => std::env::remove_var(&key),
        }
    }
}

#[tokio::test]
async fn graphql_settings_mutation_harness() {
    if std::env::var("SG_M9FT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FT_CORPUS").expect("M9FT_CORPUS");
    let out = std::env::var("M9FT_OUT").expect("M9FT_OUT");
    let dsn = std::env::var("M9FT_DSN").expect("M9FT_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let org_store = PgOrgStore::new(pool.clone());

    for case in cases {
        let restore = apply_env(&case.env);

        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let admin = site_admin(&pool, case.uid).await;
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &case.variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let mutation = prefetch_settings_mutation(&pool, &org_store, &fields, actor, admin).await;
        let roots = FrontendRootResolver::new().with_mutation(mutation);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        let rows = sqlx::query(
            "SELECT id, org_id, user_id, author_user_id, contents \
             FROM settings WHERE id >= 9580 ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .expect("settings rows");
        let rows_json = dump_settings(&rows);
        let file = format!("{body}\n{rows_json}");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());

        restore_env(restore);
    }
}
