//! M9.26d differential of the GitHub Apps write surface against the Go
//! baseline (`cmd/frontend/internal/githubapp` + graph-gophers + the real
//! Postgres store, with a fake GitHub REST API).
//!
//! Both sides read the same scratch database (seeded by
//! `run_github_apps_mutations_diff.sh` immediately before each side runs) and
//! execute the same cases from `data/github_apps_mutations/corpus.json`. Each
//! case records the normalized GraphQL response and a snapshot of the
//! `github_apps` / `github_app_installs` rows the mutations can touch.
//!
//! `refreshGitHubApp` replays the scripted HTTP pages through the injectable
//! `GithubHttpTransport`, so no request leaves the process.
//!
//! Env:
//!
//! * `SG_M926D_GITHUB_APPS_MUTATIONS_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M926D_CORPUS` — corpus JSON path
//! * `M926D_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{
    assemble, GITHUB_APPS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA,
};
use sg_service_frontend::github_apps_graphql::GitHubAppsHandle;
use sg_service_frontend::github_apps_graphql_store::GitHubAppsPgDatabase;
use sg_service_frontend::github_oauth::{GithubHttpRequest, GithubHttpResponse, GithubHttpTransport};
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;

#[derive(Deserialize)]
struct Corpus {
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
    #[serde(default, rename = "externalUrl")]
    external_url: String,
    #[serde(default)]
    http: Vec<ScriptedPage>,
}

#[derive(Deserialize, Clone)]
struct ScriptedPage {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: String,
    #[serde(default)]
    link: String,
}

/// Replays the corpus's scripted pages by their 1-based `page` query value.
struct ScriptedTransport {
    pages: Vec<ScriptedPage>,
}

fn page_of(url: &str) -> usize {
    url.split("page=")
        .nth(1)
        .and_then(|rest| rest.split(['&', '#']).next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

#[async_trait]
impl GithubHttpTransport for ScriptedTransport {
    async fn execute(&self, request: &GithubHttpRequest) -> Result<GithubHttpResponse, String> {
        let page = page_of(&request.url);
        Ok(match self.pages.get(page.saturating_sub(1)) {
            Some(scripted) => GithubHttpResponse {
                status: if scripted.status == 0 { 200 } else { scripted.status },
                body: scripted.body.clone(),
                link: scripted.link.clone(),
            },
            None => GithubHttpResponse {
                status: 404,
                body: "{\"message\":\"Not Found\"}".to_owned(),
                link: String::new(),
            },
        })
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> serde_json::Value {
    let mut envelope: Map<String, serde_json::Value> = Map::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors".to_owned(),
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
        envelope.insert("data".to_owned(), serde_json::from_str(&data).unwrap());
    }
    serde_json::Value::Object(envelope)
}

/// Snapshot of the rows the mutations can touch, restricted to the fixture
/// ids so unrelated rows never leak into the comparison.
async fn snapshot(pool: &sqlx::PgPool) -> serde_json::Value {
    let app_rows = sqlx::query(
        "SELECT id, app_id, name, webhook_id FROM github_apps \
         WHERE id IN (1, 2, 3, 4, 5) ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("apps snapshot");
    let apps: Vec<serde_json::Value> = app_rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "id": row.get::<i32, _>("id"),
                "appId": row.get::<i32, _>("app_id"),
                "name": row.get::<String, _>("name"),
                "webhookId": row.get::<Option<i32>, _>("webhook_id"),
            })
        })
        .collect();

    let install_rows = sqlx::query(
        "SELECT app_id, installation_id, COALESCE(url, '') AS url, \
         COALESCE(account_login, '') AS account_login, \
         COALESCE(account_avatar_url, '') AS account_avatar_url, \
         COALESCE(account_url, '') AS account_url, \
         COALESCE(account_type, '') AS account_type \
         FROM github_app_installs WHERE app_id IN (1, 2, 3, 4, 5) \
         ORDER BY app_id, installation_id",
    )
    .fetch_all(pool)
    .await
    .expect("installs snapshot");
    let installs: Vec<serde_json::Value> = install_rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "accountAvatarUrl": row.get::<String, _>("account_avatar_url"),
                "accountLogin": row.get::<String, _>("account_login"),
                "accountType": row.get::<String, _>("account_type"),
                "accountUrl": row.get::<String, _>("account_url"),
                "appId": row.get::<i32, _>("app_id"),
                "installationId": row.get::<i32, _>("installation_id"),
                "url": row.get::<String, _>("url"),
            })
        })
        .collect();

    serde_json::json!({ "apps": apps, "installs": installs })
}

#[tokio::test(flavor = "multi_thread")]
async fn github_apps_mutations_harness() {
    if std::env::var("SG_M926D_GITHUB_APPS_MUTATIONS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M926D_CORPUS").expect("M926D_CORPUS");
    let out = std::env::var("M926D_OUT").expect("M926D_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    // Match `NewSchemaWithBatchChangesResolver(nil batches, githubApps)`'s
    // document order: the three OSS documents, then `githubapps.graphql`.
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        GITHUB_APPS_SCHEMA,
    ])
    .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &corpus.cases {
        let actor = match case.viewer.as_str() {
            "anonymous" | "" => Actor::Anonymous,
            _ => Actor::User { uid: case.uid },
        };
        let database = Arc::new(
            GitHubAppsPgDatabase::new(
                pool.clone(),
                Arc::new(sg_keyring::Keyring::default()),
            )
            .with_external_url(case.external_url.clone()),
        );
        let github_apps: GitHubAppsHandle = database;
        let transport: Arc<dyn GithubHttpTransport> = Arc::new(ScriptedTransport {
            pages: case.http.clone(),
        });
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            false,
        )
        .with_github_apps(Some(github_apps))
        .with_github_apps_http(Some(transport));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = serde_json::json!({
            "db": snapshot(&pool).await,
            "response": normalized(&response),
        });
        let body = serde_json::to_string(&body).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
