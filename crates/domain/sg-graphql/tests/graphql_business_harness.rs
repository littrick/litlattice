#![forbid(unsafe_code)]

//! Gated harness for the M9.8fd differential: run the shared business
//! resolver corpus through `sg_graphql::exec::execute_with` with the same
//! stand-in schema/resolvers as the graph-gophers oracle, and dump each
//! response for byte comparison.
//!
//! Gating: `SG_GRAPHQL_BUSINESS_HARNESS=1`.
//! Corpus: `GRAPHQL_BUSINESS_CORPUS` (JSON `[{name, query, operationName, variables}]`).
//! Output: `GRAPHQL_BUSINESS_OUT` (one `<name>.json` per entry).

use std::env;
use std::sync::Arc;

use sg_graphql::exec::{
    execute_with, Actor, Arguments, ExecError, ExecRequest, FieldValue, ObjectValue, RootResolver,
};
use sg_graphql::schema::assemble_sdl;
use sg_graphql::Json;
use sha2::{Digest, Sha256};

const SDL: &str = r#"
schema { query: Query }

type Query {
  user(username: String!): User
  users: [User!]!
  echo(value: String = "default"): String!
  currentUser: User
  requiredNull: String!
  requiredUser: User!
  requiredFailure: String!
  usersWithHole: [User!]!
  flagged(value: String! = "x"): String!
}

type User {
  id: ID!
  username: String!
  friends: [User!]!
  broken: String
}
"#;

struct UserObject {
    uid: i32,
}

impl ObjectValue for UserObject {
    fn type_name(&self) -> &'static str {
        "User"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "id" => FieldValue::Json(Json::Str(format!("User:{}", self.uid))),
            "username" => FieldValue::Json(Json::Str("alice".to_owned())),
            "friends" => FieldValue::List {
                items: vec![FieldValue::Object(Arc::new(UserObject { uid: 2 }))],
                element_type: Some("User"),
            },
            "broken" => return Err(ExecError::new("boom")),
            _ => return Ok(None),
        }))
    }
}

struct QueryObject;

impl ObjectValue for QueryObject {
    fn type_name(&self) -> &'static str {
        "Query"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "user" => match arguments.string("username") {
                Some("alice") => FieldValue::Object(Arc::new(UserObject { uid: 1 })),
                _ => FieldValue::Null,
            },
            "users" => FieldValue::List {
                items: vec![
                    FieldValue::Object(Arc::new(UserObject { uid: 1 })),
                    FieldValue::Object(Arc::new(UserObject { uid: 2 })),
                ],
                element_type: Some("User"),
            },
            "usersWithHole" => FieldValue::List {
                items: vec![
                    FieldValue::Object(Arc::new(UserObject { uid: 1 })),
                    FieldValue::Null,
                ],
                element_type: Some("User"),
            },
            "echo" | "flagged" => FieldValue::Json(Json::Str(
                arguments.string("value").unwrap_or_default().to_owned(),
            )),
            "currentUser" => FieldValue::Null,
            "requiredNull" => return Err(ExecError::new("null")),
            "requiredUser" => FieldValue::Null,
            "requiredFailure" => return Err(ExecError::new("boom")),
            _ => return Ok(None),
        }))
    }
}

struct Roots {
    query: QueryObject,
}

impl RootResolver for Roots {
    fn root(&self, root_type: &str) -> Option<&dyn ObjectValue> {
        (root_type == "Query").then_some(&self.query as &dyn ObjectValue)
    }
}

#[test]
fn dump_business_corpus() {
    if env::var("SG_GRAPHQL_BUSINESS_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = env::var("GRAPHQL_BUSINESS_CORPUS").expect("GRAPHQL_BUSINESS_CORPUS");
    let out_dir = env::var("GRAPHQL_BUSINESS_OUT").expect("GRAPHQL_BUSINESS_OUT");
    std::fs::create_dir_all(&out_dir).expect("create output directory");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: serde_json::Value = serde_json::from_str(&raw).expect("parse corpus");
    let schema = assemble_sdl(SDL).expect("schema");

    for entry in corpus.as_array().expect("corpus array") {
        let name = entry["name"].as_str().expect("name");
        let query = entry["query"].as_str().expect("query");
        let operation_name = entry["operationName"].as_str().unwrap_or("");
        let variables = entry["variables"].as_object().cloned().unwrap_or_default();
        let response = execute_with(
            &schema,
            ExecRequest {
                query,
                operation_name,
                variables: &variables,
                actor: Actor::Anonymous,
            },
            Some(&Roots { query: QueryObject }),
        );
        let body = response.to_json();
        std::fs::write(format!("{out_dir}/{name}.json"), body.as_bytes()).expect("write response");
        let digest = Sha256::digest(body.as_bytes());
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        println!("RUST {name} len={} sha256={hex}", body.len());
    }
}
