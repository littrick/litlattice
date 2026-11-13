//! M9.27d differential of the read-only Code Intel uploads GraphQL surface
//! (`Query.preciseIndexes` and the `PreciseIndex` read fields) against the Go
//! baseline (`internal/codeintel/uploads/transport/graphql` + the real
//! Postgres uploads store).
//!
//! Both sides read the same scratch database (seeded by
//! `run_codeintel_uploads_graphql_diff.sh` immediately before each side runs)
//! and consume the same corpus from `data/codeintel_uploads/corpus.json`.
//! Instead of executing a query string, both sides invoke the production
//! resolver with the corpus arguments and serialize the same field set; this
//! keeps the comparison focused on resolver semantics (filtering, pagination,
//! state mapping, dataloader pairing, step/log key matching).
//!
//! Env:
//!
//! * `SG_M927D_CODEINTEL_UPLOADS_GQL_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M927D_CORPUS` — corpus JSON path
//! * `M927D_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::schema::{
    assemble, assemble_oss, CODEINTEL_AUTOINDEXING_SCHEMA, CODEINTEL_POLICIES_SCHEMA, MAIN_SCHEMA,
    OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA,
};
use sg_graphql::Json;
use sg_service_frontend::codeintel_uploads_graphql::{
    precise_indexes, CodeIntelUploadsHandle,
};
use sg_service_frontend::codeintel_uploads_graphql_store::PgCodeIntelUploadsStore;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    states: Option<Vec<String>>,
    #[serde(default)]
    indexer_key: Option<String>,
    #[serde(default)]
    first: Option<i64>,
    #[serde(default)]
    after: Option<String>,
}

fn arguments_for(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let mut push = |name: &str, value: InputValue| {
        entries.push((name.to_owned(), value));
    };
    if let Some(repo) = &case.repo {
        push("repo", InputValue::Str(repo.clone()));
    }
    if let Some(query) = &case.query {
        push("query", InputValue::Str(query.clone()));
    }
    if let Some(states) = &case.states {
        push(
            "states",
            InputValue::List(states.iter().cloned().map(InputValue::Str).collect()),
        );
    }
    if let Some(indexer_key) = &case.indexer_key {
        push("indexerKey", InputValue::Str(indexer_key.clone()));
    }
    if let Some(first) = case.first {
        push("first", InputValue::Int(first));
    }
    if let Some(after) = &case.after {
        push("after", InputValue::Str(after.clone()));
    }
    Arguments::from_entries(entries)
}

/// Field set serialized by both sides, keyed by GraphQL type name. Mirrors the
/// Go oracle's `m927dNode` / `m927dSteps` helpers.
fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "PreciseIndexConnection" => Some(&["nodes", "totalCount", "pageInfo"]),
        "PageInfo" => Some(&["hasNextPage", "endCursor"]),
        "PreciseIndex" => Some(&[
            "id",
            "state",
            "inputCommit",
            "inputRoot",
            "inputIndexer",
            "queuedAt",
            "uploadedAt",
            "indexingStartedAt",
            "processingStartedAt",
            "indexingFinishedAt",
            "processingFinishedAt",
            "failure",
            "placeInQueue",
            "shouldReindex",
            "isLatestForRepo",
            "indexer",
            "steps",
        ]),
        "CodeIntelIndexer" => Some(&["key", "name", "url", "imageName"]),
        "IndexSteps" => Some(&["setup", "preIndex", "index", "upload", "teardown"]),
        "PreIndexStep" => Some(&["root", "image", "commands", "logEntry"]),
        "IndexStep" => Some(&[
            "commands",
            "indexerArgs",
            "outfile",
            "requestedEnvVars",
            "logEntry",
        ]),
        "ExecutionLogEntry" => Some(&[
            "key",
            "command",
            "exitCode",
            "startTime",
            "durationMilliseconds",
            "out",
        ]),
        _ => None,
    }
}

fn json_to_serde(value: &Json) -> Value {
    match value {
        Json::Null => Value::Null,
        Json::Bool(value) => Value::Bool(*value),
        Json::Int(value) => Value::from(*value),
        Json::UInt(value) => Value::from(*value),
        Json::Float(value) => Value::from(*value),
        Json::Str(value) => Value::String(value.clone()),
        Json::Raw(value) => serde_json::from_str(value).unwrap_or(Value::Null),
        Json::Arr(items) => Value::Array(items.iter().map(json_to_serde).collect()),
        Json::Obj(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect(),
        ),
    }
}

fn render_field(value: FieldValue) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_field(item)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => render_object(object.as_ref()),
    }
}

fn render_object(object: &dyn ObjectValue) -> Result<Value, String> {
    let type_name = object.type_name();
    let fields = fields_for(type_name)
        .ok_or_else(|| format!("codeintel diff: unhandled type {type_name}"))?;
    let arguments = Arguments::default();
    let mut map = Map::new();
    for field in fields {
        let value = object
            .field(field, &arguments, Actor::Internal)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("codeintel diff: unresolved {type_name}.{field}"))?;
        map.insert((*field).to_owned(), render_field(value)?);
    }
    Ok(Value::Object(map))
}

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_uploads_graphql_harness() {
    if std::env::var("SG_M927D_CODEINTEL_UPLOADS_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M927D_CORPUS").expect("M927D_CORPUS");
    let out = std::env::var("M927D_OUT").expect("M927D_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    // The code-intel document is opt-in: it must assemble together with the
    // policies document, and must not be part of the default OSS assembly.
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        CODEINTEL_AUTOINDEXING_SCHEMA,
        CODEINTEL_POLICIES_SCHEMA,
    ])
    .expect("codeintel schema");
    assert!(schema.types.contains_key("PreciseIndex"));
    assert!(!assemble_oss()
        .expect("oss schema")
        .types
        .contains_key("PreciseIndex"));

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let handle: CodeIntelUploadsHandle = Arc::new(PgCodeIntelUploadsStore::new(pool));

    for case in &corpus.cases {
        let arguments = arguments_for(case);
        let document = match precise_indexes(&handle, &arguments, Actor::Internal) {
            Ok(value) => render_field(value).unwrap_or_else(|error| {
                panic!("render {}: {error}", case.name);
            }),
            Err(error) => {
                let mut map = Map::new();
                map.insert("error".to_owned(), Value::String(error.message));
                Value::Object(map)
            }
        };
        let body = serde_json::to_string(&document).expect("serialize");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
