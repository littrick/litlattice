//! M9.17a differential of the frontend GraphQL business surface batch B
//! (`packageRepoReferences` / `packageRepoReferencesMatchingFilter` /
//! `indexedSearchInstances` / `outboundRequests` / `invitationByToken` and the
//! organization-invitation, repository-metadata, session-invalidation,
//! user-recovery and password mutations) against graph-gophers + the real
//! `schemaResolver`, backed by the same scratch database fixtures as the Go
//! oracle.
//!
//! Env:
//!
//! * `SG_M9_17A_DIFF=1`
//! * `M9_17A_CORPUS` — JSON `[{name, query, variables?, uid?}]`
//! * `M9_17A_OUT` — output directory (one `<name>.json` per case)
//! * `M9_17A_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};
use sg_service_frontend::graphql_inventory_b::OrgInvitationConfig;

/// Same base64 HMAC key the Go oracle installs through `conf.Mock`.
const SIGNING_KEY: &str = "bTktMTdhLXNpZ25pbmcta2V5LTAxMjM0NTY3ODlhYmNkZWY=";

/// `conf.ExternalURLParsed()` is `http://example.com` when the configured
/// external URL is empty (`computed.go:444-486`).
const EXTERNAL_URL: &str = "http://example.com";

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<Value>,
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

#[tokio::test]
async fn graphql_inventory_b_harness() {
    if std::env::var("SG_M9_17A_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9_17A_CORPUS").expect("M9_17A_CORPUS");
    let out = std::env::var("M9_17A_OUT").expect("M9_17A_OUT");
    let dsn = std::env::var("M9_17A_DSN").expect("M9_17A_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = Arc::new(assemble_oss().expect("oss schema"));
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let deps = SchemaGraphqlDeps::with_session(Arc::clone(&schema), pool)
        .with_org_invitation_config(OrgInvitationConfig {
            signing_key: SIGNING_KEY.to_owned(),
            external_url: EXTERNAL_URL.to_owned(),
        });

    for case in cases {
        let actor = if case.uid == 0 {
            sg_graphql::exec::Actor::Anonymous
        } else {
            sg_graphql::exec::Actor::User { uid: case.uid }
        };
        let response = deps
            .exec(GraphqlExecRequest {
                query: &case.query,
                operation_name: "",
                variables: &case.variables,
                actor,
                anonymous_uid: "",
            })
            .await;

        let data = response
            .data
            .as_ref()
            .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
            .unwrap_or(Value::Null);
        let errors = response
            .errors
            .iter()
            .map(|_| json!({"message": "error"}))
            .collect::<Vec<_>>();
        let envelope = Envelope { data, errors };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
