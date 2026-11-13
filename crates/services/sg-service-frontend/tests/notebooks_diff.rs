//! M9.29a differential of the Notebooks GraphQL surface against the Go
//! baseline (`cmd/frontend/internal/notebooks/resolvers` + the real notebook
//! store).
//!
//! Both sides read the scratch database seeded by `run_notebooks_diff.sh` and
//! consume `data/notebooks/corpus.json`. The Go oracle and this harness call
//! the production resolvers directly (not GraphQL strings) and serialize the
//! same documents. Timestamps that come from `now()` are excluded from the
//! mutation documents/snapshots so the two runs stay comparable.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::notebooks_graphql::{
    create_notebook, create_notebook_star, delete_notebook, delete_notebook_star, notebook_by_id,
    notebooks, update_notebook, NotebooksHandle,
};
use sg_service_frontend::notebooks_store::PgNotebooksStore;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    first: Option<i64>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default, rename = "creatorUserID")]
    creator_user_id: Option<String>,
    #[serde(default, rename = "starredByUserID")]
    starred_by_user_id: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default, rename = "orderBy")]
    order_by: Option<String>,
    #[serde(default)]
    descending: Option<bool>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    notebook: Option<Value>,
    #[serde(default)]
    args: Option<Value>,
}

fn actor_for(name: Option<&str>) -> Actor {
    match name.unwrap_or("anonymous") {
        "internal" => Actor::Internal,
        "anonymous" | "" => Actor::Anonymous,
        "alice" => Actor::User { uid: 7300 },
        "bob" => Actor::User { uid: 7301 },
        "carol" => Actor::User { uid: 7302 },
        "dave" => Actor::User { uid: 7303 },
        other => panic!("unknown actor {other}"),
    }
}

fn list_arguments(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let mut push = |name: &str, value: InputValue| entries.push((name.to_owned(), value));
    if let Some(first) = case.first {
        push("first", InputValue::Int(first));
    }
    if let Some(after) = &case.after {
        push("after", InputValue::Str(after.clone()));
    }
    if let Some(query) = &case.query {
        push("query", InputValue::Str(query.clone()));
    }
    if let Some(id) = &case.creator_user_id {
        push("creatorUserID", InputValue::Str(id.clone()));
    }
    if let Some(id) = &case.starred_by_user_id {
        push("starredByUserID", InputValue::Str(id.clone()));
    }
    if let Some(namespace) = &case.namespace {
        push("namespace", InputValue::Str(namespace.clone()));
    }
    if let Some(order_by) = &case.order_by {
        push("orderBy", InputValue::Enum(order_by.clone()));
    }
    if let Some(descending) = case.descending {
        push("descending", InputValue::Bool(descending));
    }
    Arguments::from_entries(entries)
}

fn stars_arguments(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    if let Some(first) = case.first {
        entries.push(("first".to_owned(), InputValue::Int(first)));
    }
    if let Some(after) = &case.after {
        entries.push(("after".to_owned(), InputValue::Str(after.clone())));
    }
    Arguments::from_entries(entries)
}

fn json_to_input(value: &Value) -> InputValue {
    match value {
        Value::Null => InputValue::Null,
        Value::Bool(value) => InputValue::Bool(*value),
        Value::Number(number) => match number.as_i64() {
            Some(value) => InputValue::Int(value),
            None => InputValue::Float(number.as_f64().unwrap_or_default()),
        },
        Value::String(value) => InputValue::Str(value.clone()),
        Value::Array(items) => InputValue::List(items.iter().map(json_to_input).collect()),
        Value::Object(entries) => InputValue::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_to_input(value)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

fn object_arguments(value: &Value) -> Arguments {
    let entries = match value {
        Value::Object(entries) => entries
            .iter()
            .map(|(key, value)| (key.clone(), json_to_input(value)))
            .collect(),
        _ => Vec::new(),
    };
    Arguments::from_entries(entries)
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

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "User" => Some(&["id", "username"]),
        "Org" => Some(&["id", "name"]),
        "PageInfo" => Some(&["hasNextPage", "endCursor"]),
        "MarkdownBlock" => Some(&["id", "markdownInput"]),
        "QueryBlock" => Some(&["id", "queryInput"]),
        "FileBlock" => Some(&["id", "fileInput"]),
        "SymbolBlock" => Some(&["id", "symbolInput"]),
        "FileBlockInput" => Some(&["repositoryName", "filePath", "revision", "lineRange"]),
        "FileBlockLineRange" => Some(&["startLine", "endLine"]),
        "SymbolBlockInput" => Some(&[
            "repositoryName",
            "filePath",
            "revision",
            "lineContext",
            "symbolName",
            "symbolContainerName",
            "symbolKind",
        ]),
        "NotebookStar" => Some(&["user", "createdAt"]),
        _ => None,
    }
}

fn render_generic(object: &dyn ObjectValue, actor: Actor) -> Result<Value, String> {
    let fields = fields_for(object.type_name())
        .ok_or_else(|| format!("notebooks diff: unhandled type {}", object.type_name()))?;
    render_fields(object, fields, &Arguments::default(), actor)
}

fn render_fields(
    object: &dyn ObjectValue,
    fields: &[&str],
    arguments: &Arguments,
    actor: Actor,
) -> Result<Value, String> {
    let mut map = Map::new();
    for field in fields {
        let value = object
            .field(field, arguments, actor)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("notebooks diff: unresolved {}.{field}", object.type_name()))?;
        map.insert((*field).to_owned(), render_simple(value, actor)?);
    }
    Ok(Value::Object(map))
}

fn render_simple(value: FieldValue, actor: Actor) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_simple(item, actor)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => render_generic(object.as_ref(), actor),
    }
}

fn render_namespace(value: FieldValue, actor: Actor) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Object(object) => {
            let type_name = object.type_name();
            let mut rendered = render_generic(object.as_ref(), actor)?;
            if let Value::Object(map) = &mut rendered {
                map.insert("type".to_owned(), Value::String(type_name.to_owned()));
            }
            Ok(rendered)
        }
        other => render_simple(other, actor),
    }
}

fn render_star(object: &dyn ObjectValue, actor: Actor, include_created_at: bool) -> Result<Value, String> {
    let fields: &[&str] = if include_created_at {
        &["user", "createdAt"]
    } else {
        &["user"]
    };
    render_fields(object, fields, &Arguments::default(), actor)
}

fn render_star_connection(
    value: FieldValue,
    actor: Actor,
    include_created_at: bool,
) -> Result<Value, String> {
    let FieldValue::Object(connection) = value else {
        return render_simple(value, actor);
    };
    let arguments = Arguments::default();
    let nodes = connection
        .field("nodes", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved stars.nodes".to_owned())?;
    let nodes = match nodes {
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    FieldValue::Object(star) => {
                        rendered.push(render_star(star.as_ref(), actor, include_created_at)?)
                    }
                    other => rendered.push(render_simple(other, actor)?),
                }
            }
            Value::Array(rendered)
        }
        other => render_simple(other, actor)?,
    };
    let total_count = connection
        .field("totalCount", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved stars.totalCount".to_owned())?;
    let page_info = connection
        .field("pageInfo", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved stars.pageInfo".to_owned())?;
    let mut map = Map::new();
    map.insert("nodes".to_owned(), nodes);
    map.insert("totalCount".to_owned(), render_simple(total_count, actor)?);
    map.insert("pageInfo".to_owned(), render_simple(page_info, actor)?);
    Ok(Value::Object(map))
}

fn render_block(value: FieldValue, actor: Actor) -> Result<Value, String> {
    match value {
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_block(item, actor)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => {
            // The union member name is the block's `__typename`.
            let type_name = object.type_name();
            let mut rendered = render_generic(object.as_ref(), actor)?;
            if let Value::Object(map) = &mut rendered {
                map.insert("type".to_owned(), Value::String(type_name.to_owned()));
            }
            Ok(rendered)
        }
        other => render_simple(other, actor),
    }
}

fn render_notebook(object: &dyn ObjectValue, actor: Actor, brief: bool) -> Result<Value, String> {
    // `stars(first: 50)` matches the schema default used by the Go oracle.
    let arguments = Arguments::from_entries(vec![("first".to_owned(), InputValue::Int(50))]);
    let fields: &[&str] = if brief {
        &[
            "id",
            "title",
            "blocks",
            "creator",
            "updater",
            "namespace",
            "public",
            "viewerCanManage",
            "viewerHasStarred",
            "stars",
            "patternType",
        ]
    } else {
        &[
            "id",
            "title",
            "blocks",
            "creator",
            "updater",
            "namespace",
            "public",
            "updatedAt",
            "createdAt",
            "viewerCanManage",
            "viewerHasStarred",
            "stars",
            "patternType",
        ]
    };
    let mut map = Map::new();
    for field in fields {
        let value = object
            .field(field, &arguments, actor)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("notebooks diff: unresolved Notebook.{field}"))?;
        let rendered = match *field {
            "namespace" => render_namespace(value, actor)?,
            "blocks" => render_block(value, actor)?,
            "stars" => render_star_connection(value, actor, true)?,
            _ => render_simple(value, actor)?,
        };
        map.insert((*field).to_owned(), rendered);
    }
    Ok(Value::Object(map))
}

fn render_notebook_connection(value: FieldValue, actor: Actor) -> Result<Value, String> {
    let FieldValue::Object(connection) = value else {
        return render_simple(value, actor);
    };
    let arguments = Arguments::default();
    let nodes = connection
        .field("nodes", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved notebooks.nodes".to_owned())?;
    let nodes = match nodes {
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    FieldValue::Object(notebook) => {
                        rendered.push(render_notebook(notebook.as_ref(), actor, false)?)
                    }
                    other => rendered.push(render_simple(other, actor)?),
                }
            }
            Value::Array(rendered)
        }
        other => render_simple(other, actor)?,
    };
    let total_count = connection
        .field("totalCount", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved notebooks.totalCount".to_owned())?;
    let page_info = connection
        .field("pageInfo", &arguments, actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| "notebooks diff: unresolved notebooks.pageInfo".to_owned())?;
    let mut map = Map::new();
    map.insert("nodes".to_owned(), nodes);
    map.insert("totalCount".to_owned(), render_simple(total_count, actor)?);
    map.insert("pageInfo".to_owned(), render_simple(page_info, actor)?);
    Ok(Value::Object(map))
}

fn error_document(error: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(error));
    Value::Object(map)
}

fn with_snapshot(mut document: Value, snapshot: Value) -> Value {
    if let Value::Object(map) = &mut document {
        map.insert("snapshot".to_owned(), snapshot);
    }
    document
}

async fn snapshot(pool: &sqlx::PgPool) -> Value {
    let rows = sqlx::query(
        "SELECT id, title, blocks, public, creator_user_id, updater_user_id, \
         namespace_user_id, namespace_org_id, pattern_type::text AS pattern_type \
         FROM notebooks WHERE id BETWEEN 7000 AND 7199 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot notebooks");
    let notebooks = rows
        .iter()
        .map(|row| {
            let blocks: Value = row.get("blocks");
            let mut map = Map::new();
            map.insert("id".to_owned(), Value::from(row.get::<i64, _>("id")));
            map.insert("title".to_owned(), Value::from(row.get::<String, _>("title")));
            map.insert("blocks".to_owned(), blocks);
            map.insert("public".to_owned(), Value::from(row.get::<bool, _>("public")));
            map.insert(
                "creatorUserID".to_owned(),
                Value::from(row.get::<Option<i32>, _>("creator_user_id").unwrap_or(0)),
            );
            map.insert(
                "updaterUserID".to_owned(),
                Value::from(row.get::<Option<i32>, _>("updater_user_id").unwrap_or(0)),
            );
            map.insert(
                "namespaceUserID".to_owned(),
                Value::from(row.get::<Option<i32>, _>("namespace_user_id").unwrap_or(0)),
            );
            map.insert(
                "namespaceOrgID".to_owned(),
                Value::from(row.get::<Option<i32>, _>("namespace_org_id").unwrap_or(0)),
            );
            map.insert(
                "patternType".to_owned(),
                Value::from(row.get::<String, _>("pattern_type")),
            );
            Value::Object(map)
        })
        .collect::<Vec<_>>();

    let star_rows = sqlx::query(
        "SELECT notebook_id, user_id FROM notebook_stars \
         WHERE notebook_id BETWEEN 7000 AND 7199 ORDER BY notebook_id, user_id",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot stars");
    let stars = star_rows
        .iter()
        .map(|row| {
            let mut map = Map::new();
            map.insert(
                "notebookID".to_owned(),
                Value::from(i64::from(row.get::<i32, _>("notebook_id"))),
            );
            map.insert("userID".to_owned(), Value::from(row.get::<i32, _>("user_id")));
            Value::Object(map)
        })
        .collect::<Vec<_>>();

    let mut map = Map::new();
    map.insert("notebooks".to_owned(), Value::Array(notebooks));
    map.insert("stars".to_owned(), Value::Array(stars));
    Value::Object(map)
}

#[tokio::test(flavor = "multi_thread")]
async fn notebooks_diff_harness() {
    if std::env::var("SG_M929A_NOTEBOOKS_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M929A_CORPUS").expect("M929A_CORPUS");
    let out = std::env::var("M929A_OUT").expect("M929A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let handle: NotebooksHandle = Arc::new(PgNotebooksStore::new(pool.clone()));

    for case in &corpus.cases {
        let actor = actor_for(case.actor.as_deref());
        let document = match case.op.as_str() {
            "notebooks" => {
                let arguments = list_arguments(case);
                match notebooks(&handle, &arguments, actor) {
                    Ok(value) => {
                        let rendered = render_notebook_connection(value, actor)
                            .expect("render connection");
                        let mut map = Map::new();
                        map.insert("notebooks".to_owned(), rendered);
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                }
            }
            "notebook" => {
                let id = case.id.as_deref().unwrap_or_default();
                match notebook_by_id(&handle, id, actor) {
                    Ok(Some(value)) => {
                        let FieldValue::Object(object) = value else {
                            panic!("notebook_by_id returned a non-object");
                        };
                        let rendered =
                            render_notebook(object.as_ref(), actor, false).expect("render notebook");
                        let mut map = Map::new();
                        map.insert("notebook".to_owned(), rendered);
                        Value::Object(map)
                    }
                    Ok(None) => {
                        let mut map = Map::new();
                        map.insert("notebook".to_owned(), Value::Null);
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                }
            }
            "stars" => {
                let id = case.id.as_deref().unwrap_or_default();
                match notebook_by_id(&handle, id, actor) {
                    Ok(Some(value)) => {
                        let FieldValue::Object(object) = value else {
                            panic!("notebook_by_id returned a non-object");
                        };
                        let arguments = stars_arguments(case);
                        match object.field("stars", &arguments, actor) {
                            Ok(Some(value)) => {
                                let rendered = render_star_connection(value, actor, true)
                                    .expect("render stars");
                                let mut map = Map::new();
                                map.insert("stars".to_owned(), rendered);
                                Value::Object(map)
                            }
                            Ok(None) => panic!("stars field unresolved"),
                            Err(error) => error_document(error.message),
                        }
                    }
                    Ok(None) => {
                        let mut map = Map::new();
                        map.insert("stars".to_owned(), Value::Null);
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                }
            }
            "create" => {
                let notebook = case.notebook.as_ref().expect("notebook input");
                let arguments = Arguments::from_entries(vec![(
                    "notebook".to_owned(),
                    json_to_input(notebook),
                )]);
                let result = create_notebook(&handle, &arguments, actor);
                let snapshot = snapshot(&pool).await;
                match result {
                    Ok(value) => {
                        let FieldValue::Object(object) = value else {
                            panic!("create_notebook returned a non-object");
                        };
                        let rendered =
                            render_notebook(object.as_ref(), actor, true).expect("render notebook");
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        map.insert("notebook".to_owned(), rendered);
                        with_snapshot(Value::Object(map), snapshot)
                    }
                    Err(error) => with_snapshot(error_document(error.message), snapshot),
                }
            }
            "update" => {
                let args = case.args.as_ref().expect("update args");
                let arguments = object_arguments(args);
                let result = update_notebook(&handle, &arguments, actor);
                let snapshot = snapshot(&pool).await;
                match result {
                    Ok(value) => {
                        let FieldValue::Object(object) = value else {
                            panic!("update_notebook returned a non-object");
                        };
                        let rendered =
                            render_notebook(object.as_ref(), actor, true).expect("render notebook");
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        map.insert("notebook".to_owned(), rendered);
                        with_snapshot(Value::Object(map), snapshot)
                    }
                    Err(error) => with_snapshot(error_document(error.message), snapshot),
                }
            }
            "delete" => {
                let args = case.args.as_ref().expect("delete args");
                let arguments = object_arguments(args);
                let result = delete_notebook(&handle, &arguments, actor);
                let snapshot = snapshot(&pool).await;
                match result {
                    Ok(_) => {
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        with_snapshot(Value::Object(map), snapshot)
                    }
                    Err(error) => with_snapshot(error_document(error.message), snapshot),
                }
            }
            "star" => {
                let args = case.args.as_ref().expect("star args");
                let arguments = object_arguments(args);
                let result = create_notebook_star(&handle, &arguments, actor);
                let snapshot = snapshot(&pool).await;
                match result {
                    Ok(value) => {
                        let FieldValue::Object(object) = value else {
                            panic!("create_notebook_star returned a non-object");
                        };
                        let rendered = render_star(object.as_ref(), actor, false).expect("render star");
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        map.insert("star".to_owned(), rendered);
                        with_snapshot(Value::Object(map), snapshot)
                    }
                    Err(error) => with_snapshot(error_document(error.message), snapshot),
                }
            }
            "unstar" => {
                let args = case.args.as_ref().expect("unstar args");
                let arguments = object_arguments(args);
                let result = delete_notebook_star(&handle, &arguments, actor);
                let snapshot = snapshot(&pool).await;
                match result {
                    Ok(_) => {
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        with_snapshot(Value::Object(map), snapshot)
                    }
                    Err(error) => with_snapshot(error_document(error.message), snapshot),
                }
            }
            other => panic!("unknown op {other}"),
        };
        let body = serde_json::to_string(&document).expect("serialize");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} op={} len={}", case.name, case.op, body.len());
    }
}
