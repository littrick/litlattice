//! M9.8fh differential of `Query.user` + the ported `User.email`,
//! `User.siteAdmin` and `User.viewerCanAdminister` fields against
//! graph-gophers + the real `graphqlbackend.schemaResolver.User` and
//! `auth.CheckSiteAdminOrSameUserFromActor` (backed by a scratch database).
//!
//! Env:
//!
//! * `SG_M9FH_DIFF=1`
//! * `M9FH_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FH_OUT` — output directory (one `<name>.json` per case)
//! * `M9FH_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::Map;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{prefetch_users, FrontendRootResolver};
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

type Query {
  user(username: String, email: String, databaseID: Int): User
}

type User {
  id: ID!
  databaseID: Int!
  username: String!
  email: String!
  siteAdmin: Boolean!
  viewerCanAdminister: Boolean!
}
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    uid: i32,
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

#[tokio::test]
async fn graphql_user_lookup_harness() {
    if std::env::var("SG_M9FH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FH_CORPUS").expect("M9FH_CORPUS");
    let out = std::env::var("M9FH_OUT").expect("M9FH_OUT");
    let dsn = std::env::var("M9FH_DSN").expect("M9FH_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgUserStore::new(pool.clone());

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let admin = site_admin(&pool, case.uid).await;
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let users = prefetch_users(&store, &fields).await;
        let roots = FrontendRootResolver::with_user_and_repositories_and_users(
            None,
            HashMap::new(),
            users,
            admin,
        );
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
