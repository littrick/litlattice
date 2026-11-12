//! M9.18a differential of the frontend GraphQL Query/object surface batch C
//! (`externalServiceNamespaces` / `externalServiceRepositories` /
//! `renderMarkdown` / `highlightCode` / `webhooks` and the `UserEmail`,
//! `AccessToken`, `Session` and `Webhook` object fields) against graph-gophers
//! + the real `schemaResolver`, backed by the same scratch database fixtures as
//! the Go oracle.
//!
//! Markdown uses the production default (`SubsetMarkdownRenderer`, which
//! matches goldmark for the corpus's plain paragraphs). External discovery and
//! syntax highlighting are injected fakes on both sides: the Go oracle replaces
//! `mockExternalServicesService` and `highlight.Mocks.Code` with the same
//! deterministic outputs.
//!
//! Env:
//!
//! * `SG_M9_18A_QUERY_DIFF=1`
//! * `M9_18A_CORPUS` — JSON `[{name, query, variables?, uid?}]`
//! * `M9_18A_OUT` — output directory (one `<name>.json` per case)
//! * `M9_18A_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};
use sg_service_frontend::graphql_inventory_c::{
    CodeHighlighter, ExternalNamespaceData, ExternalRepositoryData, ExternalServiceDiscovery,
    InventoryCDeps,
};

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

/// Mirrors the Go oracle's `mockExternalServicesService` fake: a fixed two-node
/// namespace list and a single repository whose `externalID` records the
/// arguments the resolver received (including the `first` clamp to 100), so a
/// dropped clamp or argument difference shows up in the compared data.
struct M9Discovery;

#[async_trait::async_trait]
impl ExternalServiceDiscovery for M9Discovery {
    async fn list_namespaces(
        &self,
        _external_service_id: Option<i64>,
        kind: &str,
        config: &str,
    ) -> Result<Vec<ExternalNamespaceData>, String> {
        Ok(vec![
            ExternalNamespaceData {
                id: 9101,
                name: "ns-one".to_owned(),
                external_id: format!("cfg={config}"),
            },
            ExternalNamespaceData {
                id: 9102,
                name: "ns-two".to_owned(),
                external_id: format!("kind={kind}"),
            },
        ])
    }

    async fn discover_repos(
        &self,
        _external_service_id: Option<i64>,
        _kind: &str,
        config: &str,
        first: i32,
        query: &str,
        exclude_repos: &[String],
    ) -> Result<Vec<ExternalRepositoryData>, String> {
        let first = first.min(100);
        Ok(vec![ExternalRepositoryData {
            id: 9201,
            name: "repo/one".to_owned(),
            external_id: format!(
                "cfg={config}|first={first}|query={query}|exclude={}",
                exclude_repos.join(",")
            ),
        }])
    }
}

/// Mirrors the Go oracle's `highlight.Mocks.Code`: a deterministic wrapper that
/// records the `filepath` (so `SyntectLanguageMap` differences surface) and the
/// `disableTimeout` flag, and fails for the sentinel code `boom`.
struct M9Highlighter;

impl CodeHighlighter for M9Highlighter {
    fn highlight(
        &self,
        code: &str,
        filepath: &str,
        disable_timeout: bool,
    ) -> Result<String, String> {
        if code == "boom" {
            return Err("highlight failed".to_owned());
        }
        Ok(format!("<m9>{filepath}|{code}|{disable_timeout}</m9>"))
    }
}

#[tokio::test]
async fn graphql_inventory_c_harness() {
    if std::env::var("SG_M9_18A_QUERY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9_18A_CORPUS").expect("M9_18A_CORPUS");
    let out = std::env::var("M9_18A_OUT").expect("M9_18A_OUT");
    let dsn = std::env::var("M9_18A_DSN").expect("M9_18A_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = Arc::new(assemble_oss().expect("oss schema"));
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let deps = SchemaGraphqlDeps::with_session(Arc::clone(&schema), pool).with_inventory_c_deps(
        InventoryCDeps {
            highlighter: Arc::new(M9Highlighter),
            external_service_discovery: Arc::new(M9Discovery),
            ..InventoryCDeps::default()
        },
    );

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
