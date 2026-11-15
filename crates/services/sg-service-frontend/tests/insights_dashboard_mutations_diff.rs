//! M9.30a differential of the Code Insights dashboard write surface against
//! the Go baseline (`cmd/frontend/internal/insights/resolvers`).
//!
//! Both sides reset the scratch databases from the same fixtures before each
//! case, consume `data/insights_dashboard_mutations/corpus.json`, call the
//! production resolvers directly, and serialize the returned document plus a
//! snapshot of `dashboard` / `dashboard_grants` / `dashboard_insight_view`.
//! The Go oracle is `zz_m930a_dashboard_mutations_oracle_test.go` (untracked).

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::insights_graphql::{mutation_field, InsightsHandle};
use sg_service_frontend::insights_store::PgInsightsStore;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct GrantsFixture {
    #[serde(default)]
    users: Option<Vec<String>>,
    #[serde(default)]
    organizations: Option<Vec<String>>,
    #[serde(default)]
    global: Option<bool>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default, rename = "actorUid")]
    actor_uid: i32,
    #[serde(default, rename = "limitedAccess")]
    limited_access: bool,
    /// M9.47c: extra SQL appended after the base fixtures.
    #[serde(default)]
    extra: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, rename = "titleSet")]
    title_set: bool,
    #[serde(default)]
    grants: Option<GrantsFixture>,
    #[serde(default, rename = "dashboardId")]
    dashboard_id: Option<String>,
    #[serde(default, rename = "insightViewId")]
    insight_view_id: Option<String>,
}

fn grants_object(fixture: &GrantsFixture) -> BTreeMap<String, InputValue> {
    let mut object = BTreeMap::new();
    if let Some(users) = &fixture.users {
        object.insert(
            "users".to_owned(),
            InputValue::List(users.iter().cloned().map(InputValue::Str).collect()),
        );
    }
    if let Some(orgs) = &fixture.organizations {
        object.insert(
            "organizations".to_owned(),
            InputValue::List(orgs.iter().cloned().map(InputValue::Str).collect()),
        );
    }
    if let Some(global) = fixture.global {
        object.insert("global".to_owned(), InputValue::Bool(global));
    }
    object
}

fn field_name(op: &str) -> &'static str {
    match op {
        "create" => "createInsightsDashboard",
        "update" => "updateInsightsDashboard",
        "delete" => "deleteInsightsDashboard",
        "add" => "addInsightViewToDashboard",
        "remove" => "removeInsightViewFromDashboard",
        other => panic!("unknown op {other}"),
    }
}

fn arguments_for(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    match case.op.as_str() {
        "create" => {
            let mut input = BTreeMap::new();
            input.insert(
                "title".to_owned(),
                InputValue::Str(case.title.clone().unwrap_or_default()),
            );
            let grants = case
                .grants
                .as_ref()
                .map(grants_object)
                .unwrap_or_default();
            input.insert("grants".to_owned(), InputValue::Object(grants));
            entries.push(("input".to_owned(), InputValue::Object(input)));
        }
        "update" => {
            entries.push((
                "id".to_owned(),
                InputValue::Str(case.id.clone().unwrap_or_default()),
            ));
            let mut input = BTreeMap::new();
            if case.title_set {
                if let Some(title) = &case.title {
                    input.insert("title".to_owned(), InputValue::Str(title.clone()));
                }
            }
            if let Some(grants) = &case.grants {
                input.insert("grants".to_owned(), InputValue::Object(grants_object(grants)));
            }
            entries.push(("input".to_owned(), InputValue::Object(input)));
        }
        "delete" => entries.push((
            "id".to_owned(),
            InputValue::Str(case.id.clone().unwrap_or_default()),
        )),
        "add" | "remove" => {
            let mut input = BTreeMap::new();
            input.insert(
                "insightViewId".to_owned(),
                InputValue::Str(case.insight_view_id.clone().unwrap_or_default()),
            );
            input.insert(
                "dashboardId".to_owned(),
                InputValue::Str(case.dashboard_id.clone().unwrap_or_default()),
            );
            entries.push(("input".to_owned(), InputValue::Object(input)));
        }
        other => panic!("unknown op {other}"),
    }
    Arguments::from_entries(entries)
}

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "InsightsDashboardPayload" => Some(&["dashboard"]),
        "InsightsDashboard" => Some(&["id", "title", "grants"]),
        "InsightsPermissionGrants" => Some(&["users", "organizations", "global"]),
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

fn render_value(value: FieldValue, actor: Actor) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_value(item, actor)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => render_object(object.as_ref(), actor),
    }
}

fn render_object(object: &dyn ObjectValue, actor: Actor) -> Result<Value, String> {
    let type_name = object.type_name();
    let fields = fields_for(type_name)
        .ok_or_else(|| format!("mutation diff: unhandled type {type_name}"))?;
    let arguments = Arguments::default();
    let mut map = Map::new();
    for field in fields {
        let value = object
            .field(field, &arguments, actor)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("mutation diff: unresolved {type_name}.{field}"))?;
        map.insert((*field).to_owned(), render_value(value, actor)?);
    }
    // `grants.users` / `grants.organizations` are arrays of relay IDs; sort for
    // a deterministic document independent of array aggregation order.
    if type_name == "InsightsPermissionGrants" {
        for key in ["users", "organizations"] {
            if let Some(Value::Array(items)) = map.get_mut(key) {
                items.sort_by_key(|item| item.as_str().unwrap_or_default().to_owned());
            }
        }
    }
    Ok(Value::Object(map))
}

fn error_document(message: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(message));
    Value::Object(map)
}

async fn snapshot(pool: &PgPool) -> Result<Value, String> {
    let mut dashboards = Vec::new();
    let rows = sqlx::query(
        "SELECT id, title, save, type, created_by_user_id, (deleted_at IS NOT NULL) AS deleted
         FROM dashboard ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        let id: i32 = row.try_get("id").map_err(|e| e.to_string())?;
        let title: String = row.try_get("title").map_err(|e| e.to_string())?;
        let save: bool = row.try_get("save").map_err(|e| e.to_string())?;
        let kind: String = row.try_get("type").map_err(|e| e.to_string())?;
        let created_by: Option<i32> = row.try_get("created_by_user_id").map_err(|e| e.to_string())?;
        let deleted: bool = row.try_get("deleted").map_err(|e| e.to_string())?;
        dashboards.push(Value::Array(vec![
            Value::from(id),
            Value::String(title),
            Value::Bool(save),
            Value::String(kind),
            created_by.map(Value::from).unwrap_or(Value::Null),
            Value::Bool(deleted),
        ]));
    }

    let mut grants = Vec::new();
    let rows = sqlx::query(
        "SELECT dashboard_id, user_id, org_id, global FROM dashboard_grants
         ORDER BY dashboard_id, user_id NULLS FIRST, org_id NULLS FIRST, global",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        let dashboard_id: i32 = row.try_get("dashboard_id").map_err(|e| e.to_string())?;
        let user_id: Option<i32> = row.try_get("user_id").map_err(|e| e.to_string())?;
        let org_id: Option<i32> = row.try_get("org_id").map_err(|e| e.to_string())?;
        let global: Option<bool> = row.try_get("global").map_err(|e| e.to_string())?;
        grants.push(Value::Array(vec![
            Value::from(dashboard_id),
            user_id.map(Value::from).unwrap_or(Value::Null),
            org_id.map(Value::from).unwrap_or(Value::Null),
            global.map(Value::Bool).unwrap_or(Value::Null),
        ]));
    }

    let mut views = Vec::new();
    let rows = sqlx::query(
        "SELECT dashboard_id, insight_view_id FROM dashboard_insight_view
         ORDER BY dashboard_id, insight_view_id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        let dashboard_id: i32 = row.try_get("dashboard_id").map_err(|e| e.to_string())?;
        let view_id: i32 = row.try_get("insight_view_id").map_err(|e| e.to_string())?;
        views.push(Value::Array(vec![Value::from(dashboard_id), Value::from(view_id)]));
    }

    let mut map = Map::new();
    map.insert("dashboards".to_owned(), Value::Array(dashboards));
    map.insert("grants".to_owned(), Value::Array(grants));
    map.insert("views".to_owned(), Value::Array(views));
    Ok(Value::Object(map))
}

#[tokio::test(flavor = "multi_thread")]
async fn insights_dashboard_mutations_harness() {
    if std::env::var("SG_M930A_DASHBOARD_MUT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M930A_CORPUS").expect("M930A_CORPUS");
    let out = std::env::var("M930A_OUT").expect("M930A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let frontend_dsn =
        std::env::var("SG_QTEST_FRONTEND_DSN").expect("SG_QTEST_FRONTEND_DSN");
    let fixtures_path = std::env::var("M930A_FIXTURES").expect("M930A_FIXTURES");
    let frontend_fixtures_path =
        std::env::var("M930A_FRONTEND_FIXTURES").expect("M930A_FRONTEND_FIXTURES");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let fixtures = std::fs::read_to_string(&fixtures_path).expect("fixtures");
    let frontend_fixtures =
        std::fs::read_to_string(&frontend_fixtures_path).expect("frontend fixtures");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect codeinsights");
    let frontend_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&frontend_dsn)
        .await
        .expect("connect frontend");

    for case in &corpus.cases {
        sqlx::raw_sql(&fixtures)
            .execute(&pool)
            .await
            .expect("seed codeinsights");
        if !case.extra.is_empty() {
            sqlx::raw_sql(&case.extra)
                .execute(&pool)
                .await
                .expect("seed codeinsights extra");
        }
        sqlx::raw_sql(&frontend_fixtures)
            .execute(&frontend_pool)
            .await
            .expect("seed frontend");

        let handle: InsightsHandle = Arc::new(
            PgInsightsStore::new(pool.clone())
                .with_frontend_pool(frontend_pool.clone())
                .with_license_provider(Arc::new(
                    sg_service_frontend::insights_store::FixedCodeInsightsLicense(
                        !case.limited_access,
                    ),
                )),
        );
        let actor = if case.actor_uid == 0 {
            Actor::Internal
        } else {
            Actor::User { uid: case.actor_uid }
        };
        let arguments = arguments_for(case);
        let field = field_name(&case.op);
        let result: Result<Value, String> = (async {
            let value = mutation_field(&handle, field, &arguments, actor)
                .map_err(|error| error.message)?
                .ok_or_else(|| "mutation unresolved".to_owned())?;
            match case.op.as_str() {
                "delete" => {
                    let mut map = Map::new();
                    let mut empty = Map::new();
                    empty.insert("alwaysNil".to_owned(), Value::Null);
                    map.insert("empty".to_owned(), Value::Object(empty));
                    Ok(Value::Object(map))
                }
                _ => {
                    let payload = match value {
                        FieldValue::Object(object) => object,
                        _ => return Err("payload is not an object".to_owned()),
                    };
                    let dashboard = payload
                        .field("dashboard", &Arguments::default(), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "payload.dashboard unresolved".to_owned())?;
                    let mut map = Map::new();
                    map.insert("dashboard".to_owned(), render_value(dashboard, actor)?);
                    Ok(Value::Object(map))
                }
            }
        })
        .await;

        let mut document = match result {
            Ok(document) => document,
            Err(message) => error_document(message),
        };
        let snapshot = snapshot(&pool).await.expect("snapshot");
        document
            .as_object_mut()
            .expect("document object")
            .insert("db".to_owned(), snapshot);

        let payload = serde_json::to_vec_pretty(&document).expect("serialize");
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            payload,
        )
        .expect("write document");
    }
}
