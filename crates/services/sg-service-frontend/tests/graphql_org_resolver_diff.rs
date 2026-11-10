//! M9.8fj differential of `Query.organization` / `Query.organizations` and the
//! `Org` fields against graph-gophers + the real
//! `graphqlbackend.schemaResolver.Organization/Organizations`,
//! `db.Orgs().GetByName` / `List` / `Count`, the site-admin gates and
//! `org_members` membership, backed by a scratch database.
//!
//! Env:
//!
//! * `SG_M9FJ_DIFF=1`
//! * `M9FJ_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FJ_OUT` — output directory (one `<name>.json` per case)
//! * `M9FJ_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::Map;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{
    prefetch_organizations, prefetch_organizations_connections, FrontendRootResolver,
};
use sg_service_frontend::org_store::PgOrgStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  organization(name: String!): Org
  organizations(first: Int, query: String): OrgConnection!
}

type OrgConnection {
  nodes: [Org!]!
  totalCount: Int!
}

type Org {
  id: ID!
  name: String!
  displayName: String
  createdAt: DateTime!
  url: String!
  settingsURL: String
  namespaceName: String!
  viewerCanAdminister: Boolean!
  viewerIsMember: Boolean!
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
async fn graphql_org_resolver_harness() {
    if std::env::var("SG_M9FJ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FJ_CORPUS").expect("M9FJ_CORPUS");
    let out = std::env::var("M9FJ_OUT").expect("M9FJ_OUT");
    let dsn = std::env::var("M9FJ_DSN").expect("M9FJ_DSN");
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
        let organizations_connections =
            prefetch_organizations_connections(&store, &fields, actor, admin).await;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            organizations,
            organizations_connections,
            admin,
        );
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
