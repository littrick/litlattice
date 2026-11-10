//! M9.8ga differential of `Query.namespace` / `Query.namespaceByName`
//! (`cmd/frontend/graphqlbackend/namespaces.go:24-133`) against graph-gophers +
//! the real resolver and `db.Namespaces().GetByName`, backed by the same
//! scratch database fixtures.
//!
//! Each case writes `{"response": …}`; GraphQL error messages are masked
//! (`{"message": "error"}`) because serde and Go `encoding/json` report
//! different texts.
//!
//! Env:
//!
//! * `SG_M9GA_DIFF=1`
//! * `M9GA_CORPUS` — JSON `[{name, query, variables?}]`
//! * `M9GA_OUT` — output directory (one `<name>.json` per case)
//! * `M9GA_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{prefetch_namespace_roots, FrontendRootResolver};
use sg_service_frontend::org_store::PgOrgStore;

const SDL: &str = "
schema { query: Query }

type Query {
  namespace(id: ID!): Namespace
  namespaceByName(name: String!): Namespace
}

interface Namespace {
  id: ID!
  namespaceName: String!
  url: String!
}

type User implements Namespace {
  id: ID!
  namespaceName: String!
  url: String!
  username: String!
}

type Org implements Namespace {
  id: ID!
  namespaceName: String!
  url: String!
  name: String!
}
";

/// Field order matches the Go oracle's `m9gaEnvelope` struct.
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

#[tokio::test]
async fn namespace_harness() {
    if std::env::var("SG_M9GA_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GA_CORPUS").expect("M9GA_CORPUS");
    let out = std::env::var("M9GA_OUT").expect("M9GA_OUT");
    let dsn = std::env::var("M9GA_DSN").expect("M9GA_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgOrgStore::new(pool);
    let actor = Actor::Anonymous;

    for case in cases {
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &case.variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let namespaces = prefetch_namespace_roots(&store, &fields, actor, false).await;
        let roots = FrontendRootResolver::new().with_namespaces(namespaces);
        let response = execute_with(&schema, request, Some(&roots));
        let envelope = Envelope {
            response: mask_response(&response.to_json()),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
