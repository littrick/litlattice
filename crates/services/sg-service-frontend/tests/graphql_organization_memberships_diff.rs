//! M9.8fm differential of `User.organizationMemberships` against
//! graph-gophers + the real `graphqlbackend.UserResolver.OrganizationMemberships`,
//! `db.OrgMembers().GetByUserID`, `OrgByIDInt32`/`UserByIDInt32` and
//! `auth.CheckSiteAdminOrSameUser`, backed by a scratch database.
//!
//! Env:
//!
//! * `SG_M9FM_DIFF=1`
//! * `M9FM_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FM_OUT` — output directory (one `<name>.json` per case)
//! * `M9FM_DSN` — postgres DSN of the scratch database

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
    prefetch_nested, prefetch_users, FrontendRootResolver, NodeRoots, UserObject, UserProfiles,
};
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::session_auth::PgSessionUsers;
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  user(username: String, email: String, databaseID: Int): User
  currentUser: User
}

type User {
  id: ID!
  username: String!
  email: String!
  organizations: OrgConnection!
  organizationMemberships: OrganizationMembershipConnection!
}

type OrgConnection {
  nodes: [Org!]!
  totalCount: Int!
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

type Org {
  id: ID!
  name: String!
  displayName: String
  viewerCanAdminister: Boolean!
  viewerIsMember: Boolean!
  members(first: Int, last: Int, after: String, before: String, query: String): NewUsersConnection!
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
async fn graphql_organization_memberships_harness() {
    if std::env::var("SG_M9FM_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FM_CORPUS").expect("M9FM_CORPUS");
    let out = std::env::var("M9FM_OUT").expect("M9FM_OUT");
    let dsn = std::env::var("M9FM_DSN").expect("M9FM_DSN");
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
                &NodeRoots::default(),
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
        .with_nested(nested);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
