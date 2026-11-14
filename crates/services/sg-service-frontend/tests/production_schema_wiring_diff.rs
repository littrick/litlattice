//! M9.42a differential of the production GraphQL schema assembly for the four
//! opt-in surfaces that `graphqlbackend.NewSchema` appends for non-nil
//! resolvers: Notebooks, Code Insights, GitHub Apps and Code Intel
//! autoindexing (`cmd/frontend/graphqlbackend/graphqlbackend.go:436-616`).
//!
//! The harness assembles the OSS schema and applies the same builders the
//! frontend production combination root uses in `service.rs`
//! (`with_notebooks`, `with_insights`, `with_github_apps`,
//! `with_codeintel_uploads` + `with_codeintel_autoindexing`) against the scratch
//! frontend / code-insights databases. It executes the corpus documents through
//! `SchemaGraphqlDeps::exec` (the production executor path) and writes one
//! normalized JSON document per case.
//!
//! Go oracle: `internal/codeintel/autoindexing/transport/graphql/`
//! `zz_m942a_production_schema_wiring_oracle_test.go` (UNTRACKED); it builds
//! `graphqlbackend.NewSchema` with the real Notebooks/Insights/GitHubApps
//! resolvers and a schema-appending `CodeIntelResolver` stub.
//!
//! Env: `SG_M942A_DIFF=1`, `SG_QTEST_DSN` (frontend), `M942A_INSIGHTS_DSN`
//! (code-insights), `M942A_CORPUS`, `M942A_OUT` (directory).

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;

use sg_config::SiteConfiguration;
use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    actor: String,
    #[serde(default)]
    uid: i32,
    /// When set, keep only these names in every introspection `fields` list
    /// (the Go enterprise schema has extra documents outside this slice).
    #[serde(default, rename = "fieldAllowlist")]
    field_allowlist: Option<Vec<String>>,
}

fn actor_for(case: &Case) -> Actor {
    match case.actor.as_str() {
        "" | "anonymous" => Actor::Anonymous,
        "internal" => Actor::Internal,
        "user" => Actor::User { uid: case.uid },
        other => panic!("unknown actor {other}"),
    }
}

/// Normalize a JSON value: sort introspection field lists and, when an
/// allowlist is supplied, drop names outside it. graph-gophers and sg-graphql
/// order the `fields` array differently, and the Go enterprise schema appends
/// documents this slice does not port, so the comparison is on the sorted
/// intersection specified by the corpus.
fn normalize_fields(value: &mut Value, allowlist: Option<&[String]>) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                normalize_fields(item, allowlist);
            }
        }
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "fields" || key == "inputFields" {
                    if let Value::Array(items) = child {
                        if let Some(allowlist) = allowlist {
                            items.retain(|item| {
                                item.get("name")
                                    .and_then(Value::as_str)
                                    .map(|name| allowlist.iter().any(|allowed| allowed == name))
                                    .unwrap_or(false)
                            });
                        }
                        items.sort_by(|a, b| {
                            a.get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .cmp(b.get("name").and_then(Value::as_str).unwrap_or(""))
                        });
                    }
                } else {
                    normalize_fields(child, allowlist);
                }
            }
        }
        _ => {}
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn production_schema_wiring_harness() {
    if std::env::var("SG_M942A_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M942A_CORPUS").expect("M942A_CORPUS");
    let out = std::env::var("M942A_OUT").expect("M942A_OUT");
    let frontend_dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let insights_dsn = std::env::var("M942A_INSIGHTS_DSN").unwrap_or_else(|_| frontend_dsn.clone());
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let frontend = PgPoolOptions::new()
        .max_connections(5)
        .connect(&frontend_dsn)
        .await
        .expect("connect frontend");
    let insights = PgPoolOptions::new()
        .max_connections(5)
        .connect(&insights_dsn)
        .await
        .expect("connect insights");

    // OSS base, then the same optional-document set the production
    // `service.rs` assembly appends. `with_codeintel_uploads` appends
    // `codeintel.autoindexing.graphql`; `with_codeintel_autoindexing` only
    // attaches its handle.
    let base = Arc::new(assemble_oss().expect("oss schema"));
    let deps = SchemaGraphqlDeps::new(base)
        .with_notebooks(Arc::new(
            sg_service_frontend::notebooks_store::PgNotebooksStore::new(frontend.clone()),
        ))
        .expect("notebooks surface")
        .with_insights(Arc::new(
            sg_service_frontend::insights_store::PgInsightsStore::new(insights.clone())
                .with_frontend_pool(frontend.clone()),
        ))
        .expect("insights surface")
        .with_github_apps(Arc::new(
            sg_service_frontend::github_apps_graphql_store::GitHubAppsPgDatabase::new(
                frontend.clone(),
                Arc::new(sg_keyring::Keyring::default()),
            ),
        ))
        .expect("github apps surface")
        .with_codeintel_uploads(Arc::new(
            sg_service_frontend::codeintel_uploads_graphql_store::PgCodeIntelUploadsStore::new(
                frontend.clone(),
            ),
        ))
        .expect("code intel uploads surface");
    let deps = deps.with_codeintel_autoindexing(Arc::new(
        sg_service_frontend::codeintel_autoindexing_graphql_store::PgCodeIntelAutoindexingStore::with_site_configuration(
            frontend.clone(),
            SiteConfiguration::default(),
            None,
        ),
    ));

    for case in &corpus.cases {
        let variables = Map::new();
        let response = deps
            .exec(GraphqlExecRequest {
                query: &case.query,
                operation_name: "",
                variables: &variables,
                actor: actor_for(case),
                anonymous_uid: "",
            })
            .await;

        let mut envelope = Map::new();
        if let Some(data) = response.data.as_ref() {
            let mut data: Value = serde_json::from_str(data).expect("data json");
            normalize_fields(&mut data, case.field_allowlist.as_deref());
            envelope.insert("data".to_owned(), data);
        }
        if !response.errors.is_empty() {
            let errors = response
                .errors
                .iter()
                .map(|error| Value::from(error.message.clone()))
                .collect();
            envelope.insert("errors".to_owned(), Value::Array(errors));
        }
        let body = serde_json::to_string(&Value::Object(envelope)).expect("json");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
