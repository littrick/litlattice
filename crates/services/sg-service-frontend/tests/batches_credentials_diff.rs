//! M9.30c differential of the Batch Changes *credential* GraphQL surface
//! against the Go baseline (`cmd/frontend/internal/batches/resolvers` +
//! graph-gophers + the real batches Postgres store).
//!
//! Both sides share the scratch database seeded by
//! `run_batches_credentials_diff.sh` immediately before each side runs and
//! execute the queries/mutations from `data/batches_credentials/corpus.json`
//! in corpus order. Each case records the normalized GraphQL response plus a
//! deterministic snapshot of `user_credentials` / `batch_changes_site_credentials`.
//!
//! Env:
//!
//! * `SG_M937D_BATCHES_CREDENTIALS_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M930C_CORPUS` — corpus JSON path
//! * `M930C_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Row};

use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{
    assemble, BATCHES_SCHEMA, GITHUB_APPS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA,
    VIEWER_SCHEMA,
};
use sg_service_frontend::batches_graphql::BatchesHandle;
use sg_service_frontend::batches_graphql_store::PgBatchesGraphqlStore;
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, NodeRoots, UserByIdLookup, UserData, UserObject,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    state: Vec<StateQuery>,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct StateQuery {
    name: String,
    sql: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default = "default_enabled")]
    batch_changes_enabled: bool,
    #[serde(default)]
    restrict_to_admins: bool,
    /// M9.37d: the scripted response `checkBatchChangesCredential` sees through
    /// the injected `HttpDoer` (the Go oracle serves it from `httptest`).
    #[serde(default)]
    http: Option<HttpCase>,
}

#[derive(Deserialize, Clone)]
struct HttpCase {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

fn default_enabled() -> bool {
    true
}

/// Serves one scripted response and records the request the credential
/// validation path sent. The GitHub base URL is the database placeholder, so
/// the host is never resolved.
struct ScriptedDoer {
    response: HttpResponse,
    seen: Mutex<Option<HttpRequest>>,
}

impl ScriptedDoer {
    fn new(case: &HttpCase) -> Self {
        let status = if case.status == 0 { 200 } else { case.status };
        Self {
            response: HttpResponse {
                status,
                headers: case
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                body: case.body.clone().into_bytes(),
            },
            seen: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl HttpDoer for ScriptedDoer {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        *self.seen.lock().unwrap() = Some(request);
        Ok(self.response.clone())
    }
}

fn default_http_case() -> HttpCase {
    HttpCase {
        status: 200,
        body: "{}".to_owned(),
        headers: HashMap::new(),
    }
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn username(id: i32) -> String {
    match id {
        930001 => "m930c-alice".to_owned(),
        930002 => "m930c-bob".to_owned(),
        930003 => "m930c-admin".to_owned(),
        other => format!("m930c-user-{other}"),
    }
}

fn user_data(id: i32) -> UserData {
    UserData {
        id,
        username: username(id),
        display_name: String::new(),
        avatar_url: String::new(),
        created_at: parse_time("2024-01-01T00:00:00Z"),
        updated_at: parse_time("2024-01-01T00:00:00Z"),
        site_admin: id == 930003,
        tos_accepted: true,
        primary_email: String::new(),
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> Value {
    let mut envelope: BTreeMap<String, Value> = BTreeMap::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors".to_owned(),
            Value::Array(
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
    Value::Object(envelope.into_iter().collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_credentials_harness() {
    if std::env::var("SG_M937D_BATCHES_CREDENTIALS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M930C_CORPUS").expect("M930C_CORPUS");
    let out = std::env::var("M930C_OUT").expect("M930C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        BATCHES_SCHEMA,
        GITHUB_APPS_SCHEMA,
    ])
    .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    // `Query.node` prefetch data: the three fixture users.
    let mut users: HashMap<i32, UserByIdLookup> = HashMap::new();
    for id in [930001, 930002, 930003] {
        users.insert(id, UserByIdLookup::Found(user_data(id)));
    }

    for case in &corpus.cases {
        let (actor, viewer) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => {
                let data = user_data(case.uid);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let http = ScriptedDoer::new(case.http.as_ref().unwrap_or(&default_http_case()));
        let store = PgBatchesGraphqlStore::new(pool.clone())
            .with_batch_changes_enabled(case.batch_changes_enabled)
            .with_batch_changes_restricted_to_admins(case.restrict_to_admins)
            .with_credential_http_doer(Arc::new(http));
        let batches: BatchesHandle = Arc::new(store);
        let nodes = NodeRoots {
            users: users.clone(),
            ..NodeRoots::default()
        };
        let roots = FrontendRootResolver::with_prefetched_organizations(
            viewer,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            case.uid == 930003,
        )
        .with_batches(Some(batches))
        .with_nodes(nodes);
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));

        let mut state: BTreeMap<String, Value> = BTreeMap::new();
        for query in &corpus.state {
            let rows = sqlx::query(&query.sql)
                .fetch_all(&pool)
                .await
                .expect("state query");
            let mut items = Vec::with_capacity(rows.len());
            for row in &rows {
                let columns = row.columns();
                let mut item = Map::new();
                for (index, column) in columns.iter().enumerate() {
                    let value: Option<String> = row.try_get(index).expect("text column");
                    item.insert(
                        column.name().to_owned(),
                        match value {
                            Some(value) => Value::String(value),
                            None => Value::Null,
                        },
                    );
                }
                items.push(Value::Object(item));
            }
            state.insert(query.name.clone(), Value::Array(items));
        }

        let mut envelope: BTreeMap<String, Value> = BTreeMap::new();
        envelope.insert("response".to_owned(), normalized(&response));
        envelope.insert("state".to_owned(), Value::Object(state.into_iter().collect()));
        let body = serde_json::to_string(&envelope).unwrap();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
