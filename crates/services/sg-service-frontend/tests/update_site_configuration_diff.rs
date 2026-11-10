//! M9.8gn differential of `Mutation.updateSiteConfiguration`
//! (`cmd/frontend/graphqlbackend/site.go:294-362`) against graph-gophers +
//! the real resolver. The configuration server is started from the same DB
//! source (M9.8ei/ej/ek) so `conf.Raw`, `Write` and `NeedServerRestart`
//! behave like the frontend.
//!
//! Normalized output per case: the masked GraphQL response (the
//! `Boolean!` restart flag is replaced by `"RESTART"` for the deliberately
//! racy restart case), the latest `critical_and_site_config` row, the
//! `security_event_logs` rows and the polled `NeedServerRestart()`.
//!
//! Env:
//!
//! * `SG_M9GN_DIFF=1`
//! * `M9GN_CORPUS` — JSON `[{name, phase, uid, input, last_id_offset, setup,
//!   hide_restart_response}]`
//! * `M9GN_OUT` — output directory (one `<name>.json` per case)
//! * `M9GN_DSN` — postgres DSN of the scratch database
//! * `M9GN_PHASE` — `A`, `B` or `C` (see the Go oracle)

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::configuration_server::FrontendConfigurationServer;
use sg_service_frontend::configuration_source::DbConfigurationSource;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::settings_mutation::MutationRoot;
use sg_service_frontend::site_configuration_mutation::{
    prefetch_update_site_configuration, SiteConfigurationDeps,
};

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query { noop: String }

type Mutation {
  updateSiteConfiguration(lastID: Int!, input: String!): Boolean!
}
";

const QUERY: &str =
    "mutation($lastID: Int!, $input: String!) { updateSiteConfiguration(lastID: $lastID, input: $input) }";

#[derive(Serialize)]
struct Envelope {
    response: Value,
    config: Config,
    security_events: Vec<Event>,
    restart_final: bool,
}

#[derive(Serialize)]
struct Config {
    id: Option<i32>,
    row_count: i64,
    author_user_id: Option<i32>,
    contents: Value,
}

#[derive(Serialize)]
struct Event {
    name: String,
    url: String,
    user_id: i32,
    anonymous_user_id: String,
    source: String,
    argument: Value,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    phase: String,
    #[serde(default)]
    uid: i32,
    input: String,
    #[serde(default)]
    last_id_offset: i32,
    #[serde(default)]
    setup: Vec<String>,
    #[serde(default)]
    hide_restart_response: bool,
}

async fn site_admin(pool: &PgPool, uid: i32) -> bool {
    if uid == 0 {
        return false;
    }
    sqlx::query_scalar::<_, bool>("SELECT site_admin FROM users WHERE id = $1")
        .bind(uid)
        .fetch_optional(pool)
        .await
        .expect("site_admin query")
        .unwrap_or(false)
}

fn normalize_error(message: &str) -> &str {
    if message.contains("error unredacting secrets:") {
        "error unredacting secrets"
    } else {
        message
    }
}

fn mask_response(response: &str, hide_restart: bool) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        for error in errors {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            *error = json!({"message": normalize_error(message)});
        }
    }
    if hide_restart {
        if let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) {
            if data.contains_key("updateSiteConfiguration") {
                data.insert("updateSiteConfiguration".to_owned(), json!("RESTART"));
            }
        }
    }
    value
}

async fn latest_id(pool: &PgPool) -> i32 {
    sqlx::query_scalar::<_, i32>(
        "SELECT COALESCE(MAX(id), 0) FROM critical_and_site_config WHERE type = 'site'",
    )
    .fetch_one(pool)
    .await
    .expect("latest config id")
}

async fn config_snapshot(pool: &PgPool) -> Config {
    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM critical_and_site_config WHERE type = 'site'")
            .fetch_one(pool)
            .await
            .expect("config row count");
    let row: Option<(i32, Option<i32>, String)> = sqlx::query_as(
        "SELECT id, author_user_id, contents FROM critical_and_site_config \
         WHERE type = 'site' ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .expect("latest config");
    let Some((id, author_user_id, contents)) = row else {
        return Config {
            id: None,
            row_count,
            author_user_id: None,
            contents: Value::Null,
        };
    };
    let contents = serde_json::from_str(&contents).unwrap_or(Value::String(contents));
    Config {
        id: Some(id),
        row_count,
        author_user_id,
        contents,
    }
}

async fn security_events(pool: &PgPool) -> Vec<Event> {
    let rows: Vec<(String, String, i32, String, String, Option<Value>)> = sqlx::query_as(
        "SELECT name, url, user_id, anonymous_user_id, source, argument \
         FROM security_event_logs ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("security events");
    rows.into_iter()
        .map(
            |(name, url, user_id, anonymous_user_id, source, argument)| Event {
                name,
                url,
                user_id,
                anonymous_user_id,
                source,
                argument: argument.unwrap_or(Value::Null),
            },
        )
        .collect()
}

async fn wait_restart(deps: &SiteConfigurationDeps) -> bool {
    for _ in 0..200 {
        if deps.server.need_server_restart() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn update_site_configuration_harness() {
    if std::env::var("SG_M9GN_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GN_CORPUS").expect("M9GN_CORPUS");
    let out = std::env::var("M9GN_OUT").expect("M9GN_OUT");
    let dsn = std::env::var("M9GN_DSN").expect("M9GN_DSN");
    let phase = std::env::var("M9GN_PHASE").expect("M9GN_PHASE");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let source = DbConfigurationSource::new(pool.clone()).expect("configuration source");
    let wiring = FrontendConfigurationServer::start_with_source(source, CancellationToken::new())
        .await
        .expect("start configuration server");
    let deps = SiteConfigurationDeps {
        store: Arc::clone(wiring.store()),
        server: Arc::clone(wiring.server()),
    };
    let schema = assemble_sdl(SDL).expect("schema");

    for case in cases {
        if case.phase != phase {
            continue;
        }
        for statement in &case.setup {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .unwrap_or_else(|err| panic!("{} setup: {err}", case.name));
        }

        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let admin = site_admin(&pool, case.uid).await;
        let last_id = latest_id(&pool).await + case.last_id_offset;
        let mut variables = Map::new();
        variables.insert("lastID".to_owned(), json!(last_id));
        variables.insert("input".to_owned(), json!(case.input));
        let request = ExecRequest {
            query: QUERY,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);

        let outcomes =
            prefetch_update_site_configuration(&pool, &deps, &fields, actor, admin).await;
        let mut mutation = MutationRoot::default();
        for (field, outcome) in outcomes {
            mutation = mutation.with_site_configuration(field, outcome);
        }
        let roots = FrontendRootResolver::new().with_mutation(mutation);
        let response = execute_with(&schema, request, Some(&roots));

        let restart_final = wait_restart(&deps).await;
        let envelope = Envelope {
            response: mask_response(&response.to_json(), case.hide_restart_response),
            config: config_snapshot(&pool).await,
            security_events: security_events(&pool).await,
            restart_final,
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }

    wiring.shutdown().await;
}
