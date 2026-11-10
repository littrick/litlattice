//! M9.8fl differential of `Org.members` against graph-gophers + the real
//! `graphqlbackend.OrgResolver.Members`, gqlutil's bidirectional connection
//! pagination, `db.Users().ListByOrg`/`Count` and `org_members`, backed by a
//! scratch database.
//!
//! Env:
//!
//! * `SG_M9FL_DIFF=1`
//! * `M9FL_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FL_OUT` — output directory (one `<name>.json` per case)
//! * `M9FL_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{
    prefetch_nested, prefetch_organizations, FrontendRootResolver, NodeRoots,
};
use sg_service_frontend::org_store::PgOrgStore;

const SDL: &str = "
schema { query: Query }

type Query {
  organization(name: String!): Org
}

type Org {
  id: ID!
  name: String!
  members(first: Int, last: Int, after: String, before: String, query: String): NewUsersConnection!
}

type NewUsersConnection {
  nodes: [User!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type ConnectionPageInfo {
  endCursor: String
  hasNextPage: Boolean!
  startCursor: String
  hasPreviousPage: Boolean!
}

type User {
  id: ID!
  databaseID: Int!
  username: String!
  displayName: String
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
async fn graphql_org_members_harness() {
    if std::env::var("SG_M9FL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FL_CORPUS").expect("M9FL_CORPUS");
    let out = std::env::var("M9FL_OUT").expect("M9FL_OUT");
    let dsn = std::env::var("M9FL_DSN").expect("M9FL_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgOrgStore::new(pool.clone());

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
        let organizations = prefetch_organizations(&store, &fields, actor, admin).await;
        let organizations_connections = HashMap::new();
        let nested = Arc::new(
            prefetch_nested(
                &store,
                &store,
                &store,
                &fields,
                None,
                &HashMap::new(),
                &HashMap::new(),
                &organizations,
                &organizations_connections,
                &NodeRoots::default(),
                actor,
                admin,
            )
            .await,
        );
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            organizations,
            organizations_connections,
            admin,
        )
        .with_nested(nested);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
