//! M9.8gc differential of `Query.repositoryRedirect`
//! (`graphqlbackend.go:943-1000`, `repoStore.GetByHashedName` and
//! `repoStore.GetByName`) against graph-gophers + the real resolver, backed by
//! the same scratch database rows.
//!
//! Env:
//!
//! * `SG_M9GC_DIFF=1`
//! * `M9GC_CORPUS` — JSON `[{name, query, variables?, uid?}]`
//! * `M9GC_OUT` — output directory (one `<name>.json` per case)
//! * `M9GC_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{prefetch_repository_redirects, FrontendRootResolver};
use sg_service_frontend::repository_store::PgRepositoryStore;

const SDL: &str = "
schema { query: Query }

type Query {
  repositoryRedirect(name: String, cloneURL: String, hashedName: String): RepositoryRedirect
}

union RepositoryRedirect = Repository | Redirect

type Repository {
  id: ID!
  name: String!
  uri: String!
  isPrivate: Boolean!
  stars: Int!
}

type Redirect {
  url: String!
}
";

/// Field order matches the Go oracle's `m9gcEnvelope` struct.
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
        for error in errors {
            *error = json!({"message": "error"});
        }
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
async fn repository_redirect_harness() {
    if std::env::var("SG_M9GC_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GC_CORPUS").expect("M9GC_CORPUS");
    let out = std::env::var("M9GC_OUT").expect("M9GC_OUT");
    let dsn = std::env::var("M9GC_DSN").expect("M9GC_DSN");
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
        let redirects = prefetch_repository_redirects(&store, &fields, actor, admin).await;
        let roots = FrontendRootResolver::new().with_repository_redirects(redirects);
        let response = execute_with(&schema, request, Some(&roots));

        let envelope = Envelope {
            response: mask_response(&response.to_json()),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
