//! M9.8fp differential of `Query.settingsSubject(id)` and the
//! `SettingsSubject` implementations on `Site`/`Org`/`User` against
//! graph-gophers + the real `schemaResolver.SettingsSubject`
//! (`settings_subject.go`), `Site.LatestSettings`/`Org.LatestSettings` and
//! `settingsStore.GetLatest`, backed by a scratch database.
//!
//! Env:
//!
//! * `SG_M9FP_DIFF=1`
//! * `M9FP_CORPUS` — JSON `[{name, query, uid?}]`
//! * `M9FP_OUT` — output directory (one `<name>.json` per case)
//! * `M9FP_DSN` — postgres DSN of the scratch database

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
    prefetch_nested, prefetch_node_roots, prefetch_organizations, prefetch_users,
    FrontendRootResolver, UserObject, UserProfiles,
};
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::repository_store::PgRepositoryStore;
use sg_service_frontend::session_auth::PgSessionUsers;
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime
scalar JSONCString

type Query {
  settingsSubject(id: ID!): SettingsSubject
  organization(name: String!): Org
  user(username: String, email: String, databaseID: Int): User
  currentUser: User
}

interface SettingsSubject {
  id: ID!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
}

type User implements SettingsSubject {
  id: ID!
  username: String!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
}

type Org implements SettingsSubject {
  id: ID!
  name: String!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
}

type Site implements SettingsSubject {
  id: ID!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
}

type Settings {
  id: Int!
  subject: SettingsSubject!
  author: User
  createdAt: DateTime!
  contents: JSONCString!
  configuration: Configuration!
}

type Configuration {
  contents: JSONCString!
  messages: [String!]!
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
async fn graphql_settings_subject_harness() {
    if std::env::var("SG_M9FP_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FP_CORPUS").expect("M9FP_CORPUS");
    let out = std::env::var("M9FP_OUT").expect("M9FP_OUT");
    let dsn = std::env::var("M9FP_DSN").expect("M9FP_DSN");
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
        let organizations = prefetch_organizations(&org_store, &fields, actor, admin).await;
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
                &organizations,
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
            organizations,
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
