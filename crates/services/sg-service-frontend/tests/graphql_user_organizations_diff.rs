//! M9.8fk differential of `User.organizations` against graph-gophers + the
//! real `graphqlbackend.UserResolver.Organizations`, `db.Orgs().GetByUserID`,
//! `auth.CheckSiteAdminOrSameUserFromActor` and `org_members` membership,
//! backed by a scratch database.
//!
//! Env:
//!
//! * `SG_M9FK_DIFF=1`
//! * `M9FK_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FK_OUT` — output directory (one `<name>.json` per case)
//! * `M9FK_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{
    prefetch_nested, prefetch_users, prefetch_users_connections, FrontendRootResolver, UserObject,
    UserProfiles,
};
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::session_auth::PgSessionUsers;
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  user(username: String): User
  currentUser: User
  users(first: Int, query: String): UserConnection!
}

type UserConnection {
  nodes: [User!]!
  totalCount: Int!
}

type User {
  id: ID!
  username: String!
  organizations: OrgConnection!
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
async fn graphql_user_organizations_harness() {
    if std::env::var("SG_M9FK_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FK_CORPUS").expect("M9FK_CORPUS");
    let out = std::env::var("M9FK_OUT").expect("M9FK_OUT");
    let dsn = std::env::var("M9FK_DSN").expect("M9FK_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let users_store = PgUserStore::new(pool.clone());
    let profiles = PgSessionUsers::new(pool.clone());
    let org_store = PgOrgStore::new(pool.clone());

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
        let users_connections = prefetch_users_connections(&users_store, &fields).await;
        let organizations = HashMap::new();
        let organizations_connections = HashMap::new();
        let nested = Arc::new(
            prefetch_nested(
                &org_store,
                &org_store,
                &fields,
                current_user.as_ref(),
                &users,
                &users_connections,
                &organizations,
                &organizations_connections,
                actor,
                admin,
            )
            .await,
        );
        let roots = FrontendRootResolver::with_prefetched_organizations(
            current_user.map(UserObject::from_data),
            HashMap::new(),
            users,
            users_connections,
            HashMap::new(),
            HashMap::new(),
            admin,
        )
        .with_nested(nested);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
