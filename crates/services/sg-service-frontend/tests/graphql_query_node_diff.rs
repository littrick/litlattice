//! M9.8fn differential of `Query.node` against graph-gophers + the real
//! `relay.UnmarshalKind`/`nodeByIDFns` dispatch (`graphqlbackend/node.go`),
//! `UserByID`, `OrgByID`, `repositoryByID` (`repoStore.Get`) and the nested
//! resolvers of the returned nodes, backed by a scratch database.
//!
//! Env:
//!
//! * `SG_M9FN_DIFF=1`
//! * `M9FN_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FN_OUT` — output directory (one `<name>.json` per case)
//! * `M9FN_DSN` — postgres DSN of the scratch database

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
    prefetch_nested, prefetch_node_roots, prefetch_users, FrontendRootResolver, UserObject,
    UserProfiles,
};
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::repository_store::PgRepositoryStore;
use sg_service_frontend::session_auth::PgSessionUsers;
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  node(id: ID!): Node
}

interface Node {
  id: ID!
}

type User implements Node {
  id: ID!
  username: String!
  displayName: String
  email: String!
  viewerCanAdminister: Boolean!
  organizations: OrgConnection!
  organizationMemberships: OrganizationMembershipConnection!
}

type OrganizationMembershipConnection {
  nodes: [OrganizationMembership!]!
  totalCount: Int!
}

type OrganizationMembership {
  organization: Org!
  user: User!
  createdAt: DateTime!
  updatedAt: DateTime!
}

type Org implements Node {
  id: ID!
  name: String!
  displayName: String
  createdAt: DateTime!
  url: String!
  settingsURL: String
  namespaceName: String!
  viewerCanAdminister: Boolean!
  viewerIsMember: Boolean!
  members(first: Int, last: Int, after: String, before: String, query: String): NewUsersConnection!
}

type Repository implements Node {
  id: ID!
  name: String!
  uri: String!
  description: String!
  isFork: Boolean!
  isArchived: Boolean!
  isPrivate: Boolean!
  stars: Int!
}

type OrgConnection {
  nodes: [Org!]!
  totalCount: Int!
}

type NewUsersConnection {
  nodes: [User!]!
  totalCount: Int!
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
async fn graphql_query_node_harness() {
    if std::env::var("SG_M9FN_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FN_CORPUS").expect("M9FN_CORPUS");
    let out = std::env::var("M9FN_OUT").expect("M9FN_OUT");
    let dsn = std::env::var("M9FN_DSN").expect("M9FN_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let users_store = PgUserStore::new(pool.clone());
    let profiles = PgSessionUsers::new(pool.clone());
    let org_store = PgOrgStore::new(pool.clone());
    let repo_store = PgRepositoryStore::new(pool.clone());

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let admin = site_admin(&pool, case.uid).await;
        let current_user = if case.uid == 0 {
            None
        } else {
            profiles.profile(case.uid).await.expect("profile")
        };
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let users = prefetch_users(&users_store, &fields).await;
        let nodes = prefetch_node_roots(
            &org_store,
            &repo_store,
            None,
            None,
            None,
            None,
            None,
            "",
            &fields,
            actor,
            admin,
        )
        .await;
        let nested = Arc::new(
            prefetch_nested(
                &org_store,
                &org_store,
                &org_store,
                &fields,
                current_user.as_ref(),
                &users,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
                &nodes,
                actor,
                admin,
            )
            .await,
        );
        let roots = FrontendRootResolver::with_prefetched_organizations(
            current_user.map(UserObject::from_data),
            HashMap::new(),
            users,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            admin,
        )
        .with_nested(nested)
        .with_nodes(nodes);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
