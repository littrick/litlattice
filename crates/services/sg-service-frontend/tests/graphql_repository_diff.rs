//! M9.8fg differential of `Query.repository` + the ported `Repository`
//! fields against graph-gophers + the real
//! `graphqlbackend.schemaResolver.Repository` (backed by a scratch database).
//!
//! Env:
//!
//! * `SG_M9FG_DIFF=1`
//! * `M9FG_CORPUS` — JSON `[{name, query, uid, variables?}]`
//! * `M9FG_OUT` — output directory (one `<name>.json` per case)
//! * `M9FG_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{prefetch_repositories, FrontendRootResolver};
use sg_service_frontend::repository_store::PgRepositoryStore;

const SDL: &str = "
schema { query: Query }

type Query {
  repository(name: String, uri: String, cloneURL: String): Repository
}

type Repository {
  id: ID!
  name: String!
  uri: String!
  description: String!
  isFork: Boolean!
  isArchived: Boolean!
  isPrivate: Boolean!
  stars: Int!
  url: String!
  updatedAt: DateTime
  enabled: Boolean!
  sourceType: RepositoryType!
  externalRepository: ExternalRepository!
}

type ExternalRepository {
  id: String!
  serviceType: String!
  serviceID: String!
}

enum RepositoryType {
  GIT_REPOSITORY
  PERFORCE_DEPOT
}

scalar DateTime
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    uid: i32,
    #[serde(default)]
    variables: Map<String, Value>,
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

#[tokio::test]
async fn graphql_repository_harness() {
    if std::env::var("SG_M9FG_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FG_CORPUS").expect("M9FG_CORPUS");
    let out = std::env::var("M9FG_OUT").expect("M9FG_OUT");
    let dsn = std::env::var("M9FG_DSN").expect("M9FG_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgRepositoryStore::new(pool.clone());

    for case in cases {
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
        let repositories = prefetch_repositories(&store, None, &fields, actor, admin).await;
        let roots = FrontendRootResolver::with_user_and_repositories(None, repositories);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
