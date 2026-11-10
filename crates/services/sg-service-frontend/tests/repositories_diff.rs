//! M9.8gl differential of `Query.repositories`, the
//! `NewRepositoryConnection` and its `nodes`/`totalCount`/`pageInfo` fields
//! against graph-gophers + the real resolver
//! (`cmd/frontend/graphqlbackend/repositories.go:105-292`), backed by the same
//! scratch database rows.
//!
//! Env:
//!
//! * `SG_M9GL_DIFF=1`
//! * `M9GL_CORPUS` — JSON `[{name, query, variables?, uid?}]`
//! * `M9GL_OUT` — output directory (one `<name>.json` per case)
//! * `M9GL_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::repositories::{
    prefetch_repositories_connections, PgRepositoriesConnectionStore,
};

const SDL: &str = "
schema { query: Query }

type Query {
  repositories(
    first: Int
    last: Int
    query: String
    after: String
    before: String
    names: [String!]
    cloned: Boolean = true
    cloneStatus: CloneStatus
    notCloned: Boolean = true
    indexed: Boolean = true
    notIndexed: Boolean = true
    embedded: Boolean = true
    notEmbedded: Boolean = true
    failedFetch: Boolean = false
    corrupted: Boolean = false
    externalService: ID
    orderBy: RepositoryOrderBy = REPOSITORY_NAME
    descending: Boolean = false
  ): NewRepositoryConnection!
}

type NewRepositoryConnection {
  nodes: [Repository!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type ConnectionPageInfo {
  endCursor: String
  startCursor: String
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
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
  externalRepository: ExternalRepository!
}

type ExternalRepository {
  id: String!
  serviceType: String!
  serviceID: String!
}

enum CloneStatus {
  NOT_CLONED
  CLONING
  CLONED
}

enum RepositoryOrderBy {
  REPOSITORY_NAME
  REPO_CREATED_AT
  REPOSITORY_CREATED_AT
  SIZE
}
";

/// Field order matches the Go oracle's `m9glEnvelope` struct.
#[derive(Serialize)]
struct Envelope {
    response: Value,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    uid: i32,
}

/// `{"message": "error"}` for every GraphQL error.
fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        if !errors.is_empty() {
            // Error multiplicity and paths differ between the executors; only
            // the presence of an error is compared.
            *errors = vec![json!({"message": "error"})];
        }
    }
    // graph-gophers always emits `data`, even when it is null; the Rust
    // executor omits it for some pre-execution failures. Normalize to null.
    if let Value::Object(object) = &mut value {
        object.entry("data".to_owned()).or_insert(Value::Null);
    }
    value
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
async fn repositories_harness() {
    if std::env::var("SG_M9GL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GL_CORPUS").expect("M9GL_CORPUS");
    let out = std::env::var("M9GL_OUT").expect("M9GL_OUT");
    let dsn = std::env::var("M9GL_DSN").expect("M9GL_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgRepositoriesConnectionStore::new(pool.clone());

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
        let connections = prefetch_repositories_connections(&store, &fields, actor, admin).await;
        let roots = FrontendRootResolver::new().with_repositories_connections(connections);
        let response = execute_with(&schema, request, Some(&roots));

        let envelope = Envelope {
            response: mask_response(&response.to_json()),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
