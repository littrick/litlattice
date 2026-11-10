//! M9.8fe differential of the DB-backed `User` resolvers against
//! graph-gophers + the real `graphqlbackend.UserResolver`
//! (`cmd/m9feuserharness`).
//!
//! Env:
//!
//! * `SG_GRAPHQL_USER_HARNESS=1`
//! * `GRAPHQL_USER_CORPUS` — JSON `[{name, query, user{id,username,displayName,avatarURL}}]`
//! * `GRAPHQL_USER_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::Deserialize;
use serde_json::Map;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{FrontendRootResolver, UserData, UserObject};

const SDL: &str = "
schema { query: Query }

type Query {
  currentUser: User
}

type User {
  id: ID!
  databaseID: Int!
  username: String!
  displayName: String
  avatarURL: String
}
";

#[derive(Deserialize)]
struct UserCase {
    id: i32,
    username: String,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "avatarURL")]
    avatar_url: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    user: UserCase,
}

#[test]
fn graphql_user_harness() {
    if std::env::var("SG_GRAPHQL_USER_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("GRAPHQL_USER_CORPUS").expect("GRAPHQL_USER_CORPUS");
    let out = std::env::var("GRAPHQL_USER_OUT").expect("GRAPHQL_USER_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    for case in cases {
        let user = UserObject::from_data(UserData {
            id: case.user.id,
            username: case.user.username,
            display_name: case.user.display_name,
            avatar_url: case.user.avatar_url,
        });
        let roots = FrontendRootResolver::with_user(Some(user));
        let variables = Map::new();
        let response = execute_with(
            &schema,
            ExecRequest {
                query: &case.query,
                operation_name: "",
                variables: &variables,
                actor: Actor::User { uid: 1 },
            },
            Some(&roots),
        );
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
