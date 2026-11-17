//! M9.48a differential of the authz permissions GraphQL surface against the
//! Go baseline (`cmd/frontend/internal/authz/resolvers` + graph-gophers + the
//! real authz Postgres stores).
//!
//! Both sides read the same scratch database (seeded by
//! `run_authz_graphql_diff.sh` immediately before each side runs) and execute
//! the same query strings from `data/authz_graphql/corpus.json`. The Rust side
//! assembles the vendored `authz.graphql` on top of the main OSS schema and
//! resolves through the production `FrontendRootResolver` with the
//! `PgAuthzStore` adapter.
//!
//! Env:
//!
//! * `SG_M948A_AUTHZ_GQL_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M948A_CORPUS` — corpus JSON path
//! * `M948A_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;
use time::OffsetDateTime;

use sg_config::{LiveConfig, Unified};
use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, AUTHZ_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA};
use sg_service_frontend::authz_graphql::AuthzHandle;
use sg_service_frontend::authz_graphql_store::PgAuthzStore;
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, RepoKey, RepoLookup, RepositoryData, UserData, UserKey, UserLookup,
    UserObject,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
}

fn user_data(id: i32, username: &str, display_name: &str, site_admin: bool) -> UserData {
    UserData {
        id,
        username: username.to_owned(),
        display_name: display_name.to_owned(),
        avatar_url: String::new(),
        created_at: OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap(),
        updated_at: OffsetDateTime::from_unix_timestamp(1_704_153_600).unwrap(),
        site_admin,
        tos_accepted: false,
        builtin_auth: false,
        scim_controlled: false,
        completions_quota: None,
        code_completions_quota: None,
        primary_email: String::new(),
    }
}

fn repo_data(id: i32, name: &str, private: bool) -> RepositoryData {
    RepositoryData {
        id,
        name: name.to_owned(),
        uri: format!("https://{name}"),
        description: String::new(),
        fork: false,
        archived: false,
        private,
        stars: 0,
        external_id: String::new(),
        external_service_type: "github".to_owned(),
        external_service_id: "https://github.com/".to_owned(),
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut envelope: std::collections::BTreeMap<&str, serde_json::Value> =
        std::collections::BTreeMap::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        );
    }
    if let Some(data) = response.data_json() {
        envelope.insert("data", serde_json::from_str(&data).unwrap());
    }
    serde_json::to_string(&envelope).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_graphql_harness() {
    if std::env::var("SG_M948A_AUTHZ_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M948A_CORPUS").expect("M948A_CORPUS");
    let out = std::env::var("M948A_OUT").expect("M948A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        AUTHZ_SCHEMA,
    ])
    .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let authz = AuthzHandle::new(Arc::new(
        PgAuthzStore::new(pool.clone(), Arc::new(LiveConfig::new(Unified::default())))
            .with_test_license(),
    ));

    let users: Vec<UserData> = vec![
        user_data(1, "alice", "Alice", true),
        user_data(2, "bob", "Bob", false),
        user_data(3, "carol", "Carol", false),
        user_data(4, "dave", "Dave", false),
        user_data(5, "erin", "Erin", false),
    ];
    let repositories: Vec<RepositoryData> = vec![
        repo_data(10, "github.com/acme/alpha", true),
        repo_data(11, "github.com/acme/beta", true),
        repo_data(12, "github.com/acme/public", false),
        repo_data(13, "github.com/acme/unrestricted", true),
    ];

    for case in &corpus.cases {
        let viewer_user = users.iter().find(|user| user.id == case.uid).cloned();
        let viewer_site_admin = viewer_user.as_ref().is_some_and(|user| user.site_admin);
        let (actor, viewer) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => (
                Actor::User { uid: case.uid },
                viewer_user
                    .clone()
                    .map(|user| UserObject::from_data_as_viewer(user, viewer_site_admin)),
            ),
        };

        let mut user_map = HashMap::new();
        for user in &users {
            user_map.insert(
                UserKey::Username(user.username.clone()),
                UserLookup::Found(user.clone()),
            );
        }
        let mut repo_map = HashMap::new();
        for repo in &repositories {
            repo_map.insert(RepoKey::Name(repo.name.clone()), RepoLookup::Found(repo.clone()));
        }

        let roots = FrontendRootResolver::with_prefetched_organizations(
            viewer,
            repo_map,
            user_map,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_authz(Some(authz.clone()), viewer_site_admin);

        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
