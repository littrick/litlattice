//! M9.8ji differential of internal-actor penetration through the internal
//! `/.internal/graphql` actor middleware (`actor.HTTPMiddleware`,
//! `internal/actor/http.go:116`) into the executor and the authorization
//! helpers (`internal/auth/{site_admin,orgs}.go`, `database/repos_perm.go`).
//!
//! Each corpus case carries `X-Sourcegraph-Actor-UID` /
//! `X-Sourcegraph-Actor-Anonymous-UID` header values. The case goes through
//! [`apply_actor_header`] (the ported middleware decision) and the resulting
//! actor is fed to the Rust schema (`assemble_sdl`, the prefetch helpers and
//! the root resolvers), mirroring the Go oracle's real `schemaResolver`
//! methods.
//!
//! Output is `{"status":<int>,"body":<string>}`, `body` being the raw GraphQL
//! JSON or the 403 middleware text, byte-compatible with the Go oracle.
//!
//! Env:
//!
//! * `SG_M9JI_DIFF=1`
//! * `M9JI_CORPUS` — JSON `[{name, headers, query}]`
//! * `M9JI_OUT` — output directory (one `<name>.json` per case)
//! * `M9JI_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;

use http::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Map;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::json::Json;
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::actor_middleware::{apply_actor_header, ActorHeaderDecision};
use sg_service_frontend::graphql_resolvers::{
    prefetch_node_roots, prefetch_organizations, prefetch_users, FrontendRootResolver, UserKey,
    UserLookupStore, UserObject,
};
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::repository_store::PgRepositoryStore;
use sg_service_frontend::user_store::PgUserStore;

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  currentUser: User
  site: Site!
  user(username: String, email: String, databaseID: Int): User
  organization(name: String!): Org
  node(id: ID!): Node
}

interface Node {
  id: ID!
}

type User implements Node {
  id: ID!
  username: String!
  displayName: String
  siteAdmin: Boolean!
  email: String!
  viewerCanAdminister: Boolean!
}

type Org implements Node {
  id: ID!
  name: String!
  displayName: String
  viewerCanAdminister: Boolean!
  viewerIsMember: Boolean!
}

type Site {
  viewerCanAdminister: Boolean!
}
";

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    query: String,
}

fn header_map(headers: &HashMap<String, String>) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.to_ascii_lowercase().as_bytes())
            .expect("valid header name");
        let value = HeaderValue::from_str(value).expect("valid header value");
        map.insert(name, value);
    }
    map
}

fn output(status: u16, body: String) -> String {
    Json::obj(vec![
        ("status", Json::Int(i64::from(status))),
        ("body", Json::Str(body)),
    ])
    .to_compact_string()
}

#[tokio::test]
async fn internal_actor_harness() {
    if std::env::var("SG_M9JI_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9JI_CORPUS").expect("M9JI_CORPUS");
    let out = std::env::var("M9JI_OUT").expect("M9JI_OUT");
    let dsn = std::env::var("M9JI_DSN").expect("M9JI_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let users = PgUserStore::new(pool.clone());
    let orgs = PgOrgStore::new(pool.clone());
    let repositories = PgRepositoryStore::new(pool.clone());

    for case in cases {
        // `actor.HTTPMiddleware`: an invalid UID is rejected before the
        // handler runs.
        let chain_actor = match apply_actor_header(&header_map(&case.headers)) {
            ActorHeaderDecision::Proceed(actor) => actor,
            ActorHeaderDecision::Reject { status, body } => {
                let encoded = output(status, body);
                std::fs::write(
                    Path::new(&out).join(format!("{}.json", case.name)),
                    &encoded,
                )
                .expect("write");
                println!("RUST {} status={}", case.name, status);
                continue;
            }
        };
        let actor = chain_actor.to_graphql();

        // `internal/auth` treats internal actors as site admins; otherwise the
        // flag comes from the user row.
        let current_user_data = match actor {
            Actor::User { uid } => users
                .lookup(&UserKey::DatabaseId(uid))
                .await
                .expect("user lookup")
                .filter(|data| data.id == uid),
            Actor::Anonymous | Actor::Internal => None,
        };
        let site_admin = actor.is_internal()
            || current_user_data
                .as_ref()
                .is_some_and(|data| data.site_admin);
        let current_user = current_user_data.map(UserObject::from_data);

        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let users_lookup = prefetch_users(&users, &fields).await;
        let organizations = prefetch_organizations(&orgs, &fields, actor, site_admin).await;
        let nodes = prefetch_node_roots(
            &orgs,
            &repositories,
            None,
            None,
            Some(&users),
            None,
            None,
            "",
            &fields,
            actor,
            site_admin,
        )
        .await;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            current_user,
            HashMap::new(),
            users_lookup,
            HashMap::new(),
            organizations,
            HashMap::new(),
            site_admin,
        )
        .with_nodes(nodes);

        let response = execute_with(&schema, request, Some(&roots));
        let encoded = output(200, response.to_json());
        std::fs::write(
            Path::new(&out).join(format!("{}.json", case.name)),
            &encoded,
        )
        .expect("write");
        println!("RUST {} len={}", case.name, encoded.len());
    }
}
