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
    code_intel_summary, delete_precise_index, delete_precise_indexes, indexer_keys,
    precise_index_by_id, precise_indexes, reindex_precise_index, reindex_precise_indexes,
    CodeIntelUploadsHandle,
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
    kind: Option<String>,
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
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    repository: Option<String>,
    #[serde(default)]
    is_latest_for_repo: Option<bool>,
    #[serde(default)]
    dependency_of: Option<String>,
    #[serde(default)]
    dependent_of: Option<String>,
    #[serde(default)]
    include_deleted: Option<bool>,
}

fn case_kind(case: &Case) -> &str {
    case.kind.as_deref().unwrap_or("preciseIndexes")
}

fn arguments_for(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let mut push = |name: &str, value: InputValue| {
        entries.push((name.to_owned(), value));
    };
    let kind = case_kind(case);
    match kind {
        "preciseIndexes" => {
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
            if let Some(dependency_of) = &case.dependency_of {
                push("dependencyOf", InputValue::Str(dependency_of.clone()));
            }
            if let Some(dependent_of) = &case.dependent_of {
                push("dependentOf", InputValue::Str(dependent_of.clone()));
            }
            if let Some(include_deleted) = case.include_deleted {
                push("includeDeleted", InputValue::Bool(include_deleted));
            }
        }
        "indexerKeys" => {
            if let Some(repo) = &case.repo {
                push("repo", InputValue::Str(repo.clone()));
            }
        }
        "codeIntelSummary" => {
            if let Some(first) = case.first {
                push("first", InputValue::Int(first));
            }
            if let Some(after) = &case.after {
                push("after", InputValue::Str(after.clone()));
            }
        }
        "preciseIndexByID" => {
            if let Some(id) = &case.id {
                push("id", InputValue::Str(id.clone()));
            }
        }
        "deletePreciseIndex" | "reindexPreciseIndex" => {
            if let Some(id) = &case.id {
                push("id", InputValue::Str(id.clone()));
            }
        }
        "deletePreciseIndexes" | "reindexPreciseIndexes" => {
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
            if let Some(is_latest) = case.is_latest_for_repo {
                push("isLatestForRepo", InputValue::Bool(is_latest));
            }
            if let Some(repository) = &case.repository {
                push("repository", InputValue::Str(repository.clone()));
            }
        }
        _ => {}
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
            "auditLogs",
        ]),
        "LSIFUploadAuditLog" => Some(&[
            "logTimestamp",
            "uploadDeletedAt",
            "reason",
            "changedColumns",
            "uploadId",
            "inputCommit",
            "inputRoot",
            "inputIndexer",
            "uploadedAt",
            "operation",
        ]),
        "AuditLogColumnChange" => Some(&["column", "old", "new"]),
        "CodeIntelSummary" => Some(&[
            "numRepositoriesWithCodeIntelligence",
            "repositoriesWithErrors",
        ]),
        "CodeIntelRepositoryWithErrorConnection" => Some(&["nodes", "totalCount", "pageInfo"]),
        "CodeIntelRepositoryWithError" => Some(&["repository", "count"]),
        "CodeIntelRepositoryWithConfigurationConnection" => {
            Some(&["nodes", "totalCount", "pageInfo"])
        }
        "CodeIntelRepositoryWithConfiguration" => Some(&["repository", "indexers"]),
        "IndexerWithCount" => Some(&["indexer", "count"]),
        "CodeIntelRepository" => Some(&["id", "name", "url"]),
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

/// Renders `Query.codeIntelSummary`, passing the case `first`/`after` through
/// to `repositoriesWithErrors` (the generic renderer uses empty arguments).
fn render_code_intel_summary(value: FieldValue, arguments: &Arguments) -> Result<Value, String> {
    let object = match value {
        FieldValue::Object(object) => object,
        other => return render_field(other),
    };
    let mut map = Map::new();
    let num = object
        .field(
            "numRepositoriesWithCodeIntelligence",
            &Arguments::default(),
            Actor::Internal,
        )
        .map_err(|error| error.message)?
        .ok_or_else(|| {
            "codeintel diff: unresolved CodeIntelSummary.numRepositoriesWithCodeIntelligence"
                .to_owned()
        })?;
    map.insert(
        "numRepositoriesWithCodeIntelligence".to_owned(),
        render_field(num)?,
    );
    let errors = object
        .field("repositoriesWithErrors", arguments, Actor::Internal)
        .map_err(|error| error.message)?
        .ok_or_else(|| {
            "codeintel diff: unresolved CodeIntelSummary.repositoriesWithErrors".to_owned()
        })?;
    map.insert("repositoriesWithErrors".to_owned(), render_field(errors)?);
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
    let handle: CodeIntelUploadsHandle = Arc::new(PgCodeIntelUploadsStore::new(pool.clone()));

    for case in &corpus.cases {
        let arguments = arguments_for(case);
        let kind = case_kind(case);
        let document = match kind {
            "preciseIndexes" => match precise_indexes(&handle, &arguments, Actor::Internal) {
                Ok(value) => render_field(value).unwrap_or_else(|error| {
                    panic!("render {}: {error}", case.name);
                }),
                Err(error) => error_document(&error),
            },
            "indexerKeys" => match indexer_keys(&handle, &arguments, Actor::Internal) {
                Ok(value) => {
                    let mut map = Map::new();
                    map.insert(
                        "keys".to_owned(),
                        render_field(value).unwrap_or_else(|error| {
                            panic!("render {}: {error}", case.name);
                        }),
                    );
                    Value::Object(map)
                }
                Err(error) => error_document(&error),
            },
            "preciseIndexByID" => match precise_index_by_id(
                &handle,
                case.id.as_deref().unwrap_or_default(),
                Actor::Internal,
            ) {
                Ok(Some(value)) => {
                    let mut map = Map::new();
                    map.insert(
                        "node".to_owned(),
                        render_field(value).unwrap_or_else(|error| {
                            panic!("render {}: {error}", case.name);
                        }),
                    );
                    Value::Object(map)
                }
                Ok(None) => {
                    let mut map = Map::new();
                    map.insert("node".to_owned(), Value::Null);
                    Value::Object(map)
                }
                Err(error) => error_document(&error),
            },
            "deletePreciseIndex" => {
                let outcome = delete_precise_index(&handle, &arguments, Actor::Internal);
                mutation_document(&pool, outcome.err()).await
            }
            "deletePreciseIndexes" => {
                let outcome = delete_precise_indexes(&handle, &arguments, Actor::Internal);
                mutation_document(&pool, outcome.err()).await
            }
            "reindexPreciseIndex" => {
                let outcome = reindex_precise_index(&handle, &arguments, Actor::Internal);
                mutation_document(&pool, outcome.err()).await
            }
            "reindexPreciseIndexes" => {
                let outcome = reindex_precise_indexes(&handle, &arguments, Actor::Internal);
                mutation_document(&pool, outcome.err()).await
            }
            "codeIntelSummary" => {
                match code_intel_summary(&handle, &Arguments::default(), Actor::Internal) {
                    Ok(value) => render_code_intel_summary(value, &arguments).unwrap_or_else(
                        |error| panic!("render {}: {error}", case.name),
                    ),
                    Err(error) => error_document(&error),
                }
            }
            other => panic!("unknown case kind {other}"),
        };
        let body = serde_json::to_string(&document).expect("serialize");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}

fn error_document(error: &sg_graphql::exec::ExecError) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(error.message.clone()));
    Value::Object(map)
}

async fn mutation_document(
    pool: &sqlx::PgPool,
    error: Option<sg_graphql::exec::ExecError>,
) -> Value {
    let mut map = Map::new();
    match error {
        Some(error) => {
            map.insert("error".to_owned(), Value::String(error.message));
        }
        None => {
            map.insert("result".to_owned(), Value::String("ok".to_owned()));
        }
    }
    map.insert("uploads".to_owned(), upload_snapshot(pool).await);
    map.insert("indexes".to_owned(), index_snapshot(pool).await);
    map.insert("dirty".to_owned(), dirty_snapshot(pool).await);
    Value::Object(map)
}

fn nullable_int(value: Option<i32>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

fn nullable_int64(value: Option<i64>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

fn nullable_string(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

async fn upload_snapshot(pool: &sqlx::PgPool) -> Value {
    use sqlx::Row as _;
    let rows = sqlx::query(
        "SELECT id, state, should_reindex, root, indexer, commit, repository_id, \
         associated_index_id, uploaded_at::text AS uploaded_at, \
         started_at::text AS started_at, finished_at::text AS finished_at \
         FROM lsif_uploads WHERE id BETWEEN 927000 AND 927099 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("upload snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                let mut map = Map::new();
                map.insert("id".to_owned(), Value::from(row.get::<i32, _>("id")));
                map.insert(
                    "state".to_owned(),
                    Value::String(row.get::<String, _>("state")),
                );
                map.insert(
                    "shouldReindex".to_owned(),
                    Value::Bool(row.get::<bool, _>("should_reindex")),
                );
                map.insert(
                    "root".to_owned(),
                    Value::String(row.get::<String, _>("root")),
                );
                map.insert(
                    "indexer".to_owned(),
                    Value::String(row.get::<String, _>("indexer")),
                );
                map.insert(
                    "commit".to_owned(),
                    Value::String(row.get::<String, _>("commit")),
                );
                map.insert(
                    "repositoryId".to_owned(),
                    Value::from(row.get::<i32, _>("repository_id")),
                );
                map.insert(
                    "associatedIndexId".to_owned(),
                    nullable_int64(row.get::<Option<i64>, _>("associated_index_id")),
                );
                map.insert(
                    "uploadedAt".to_owned(),
                    Value::String(row.get::<String, _>("uploaded_at")),
                );
                map.insert(
                    "startedAt".to_owned(),
                    nullable_string(row.get::<Option<String>, _>("started_at")),
                );
                map.insert(
                    "finishedAt".to_owned(),
                    nullable_string(row.get::<Option<String>, _>("finished_at")),
                );
                Value::Object(map)
            })
            .collect(),
    )
}

async fn index_snapshot(pool: &sqlx::PgPool) -> Value {
    use sqlx::Row as _;
    let rows = sqlx::query(
        "SELECT id, state, should_reindex, root, indexer, commit, repository_id, \
         (SELECT MAX(x.id) FROM lsif_uploads x WHERE x.associated_index_id = u.id) AS associated_upload_id, \
         queued_at::text AS queued_at, started_at::text AS started_at, \
         finished_at::text AS finished_at \
         FROM lsif_indexes u WHERE id BETWEEN 927100 AND 927199 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("index snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                let mut map = Map::new();
                map.insert("id".to_owned(), Value::from(row.get::<i64, _>("id")));
                map.insert(
                    "state".to_owned(),
                    Value::String(row.get::<String, _>("state")),
                );
                map.insert(
                    "shouldReindex".to_owned(),
                    Value::Bool(row.get::<bool, _>("should_reindex")),
                );
                map.insert(
                    "root".to_owned(),
                    Value::String(row.get::<String, _>("root")),
                );
                map.insert(
                    "indexer".to_owned(),
                    Value::String(row.get::<String, _>("indexer")),
                );
                map.insert(
                    "commit".to_owned(),
                    Value::String(row.get::<String, _>("commit")),
                );
                map.insert(
                    "repositoryId".to_owned(),
                    Value::from(row.get::<i32, _>("repository_id")),
                );
                map.insert(
                    "associatedUploadId".to_owned(),
                    nullable_int(row.get::<Option<i32>, _>("associated_upload_id")),
                );
                map.insert(
                    "queuedAt".to_owned(),
                    Value::String(row.get::<String, _>("queued_at")),
                );
                map.insert(
                    "startedAt".to_owned(),
                    nullable_string(row.get::<Option<String>, _>("started_at")),
                );
                map.insert(
                    "finishedAt".to_owned(),
                    nullable_string(row.get::<Option<String>, _>("finished_at")),
                );
                Value::Object(map)
            })
            .collect(),
    )
}

async fn dirty_snapshot(pool: &sqlx::PgPool) -> Value {
    use sqlx::Row as _;
    let rows = sqlx::query(
        "SELECT repository_id, dirty_token, update_token FROM lsif_dirty_repositories \
         WHERE repository_id IN (92700, 92701) ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("dirty snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                let mut map = Map::new();
                map.insert(
                    "repositoryId".to_owned(),
                    Value::from(row.get::<i32, _>("repository_id")),
                );
                map.insert(
                    "dirtyToken".to_owned(),
                    Value::from(row.get::<i32, _>("dirty_token")),
                );
                map.insert(
                    "updateToken".to_owned(),
                    Value::from(row.get::<i32, _>("update_token")),
                );
                Value::Object(map)
            })
            .collect(),
    )
}
