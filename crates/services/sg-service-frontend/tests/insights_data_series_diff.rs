//! M9.31a differential of the recorded Code Insights data-series read surface
//! against the Go baseline (`cmd/frontend/internal/insights/resolvers`).
//!
//! Both sides reset the scratch databases from the same fixtures before every
//! case, consume `data/insights_data_series/corpus.json`, call the production
//! `InsightView.dataSeries` / `seriesCount` resolvers, and serialize the
//! resulting series (points, diff/point-in-time queries, status).
//!
//! The Go oracle is `zz_m931a_data_series_oracle_test.go` (untracked); both
//! are compared by `run_insights_data_series_diff.sh`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::insights_graphql::{
    insight_views, marshal_insight_view_id, InsightsHandle,
};
use sg_service_frontend::insights_store::PgInsightsStore;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct FiltersFixture {
    #[serde(default, rename = "includeRepoRegex")]
    include_repo_regex: Option<String>,
    #[serde(default, rename = "excludeRepoRegex")]
    exclude_repo_regex: Option<String>,
    #[serde(default, rename = "searchContexts")]
    search_contexts: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct SortOptionsFixture {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    direction: Option<String>,
}

#[derive(Deserialize, Default)]
struct SeriesDisplayOptionsFixture {
    #[serde(default, rename = "sortOptions")]
    sort_options: Option<SortOptionsFixture>,
    #[serde(default)]
    limit: Option<i32>,
    #[serde(default, rename = "numSamples")]
    num_samples: Option<i32>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    view: String,
    #[serde(default)]
    filters: Option<FiltersFixture>,
    #[serde(default, rename = "seriesDisplayOptions")]
    series_display_options: Option<SeriesDisplayOptionsFixture>,
}

fn view_arguments(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    entries.push((
        "id".to_owned(),
        InputValue::Str(marshal_insight_view_id(&case.view)),
    ));
    if let Some(filters) = &case.filters {
        let mut object = BTreeMap::new();
        if let Some(value) = &filters.include_repo_regex {
            object.insert("includeRepoRegex".to_owned(), InputValue::Str(value.clone()));
        }
        if let Some(value) = &filters.exclude_repo_regex {
            object.insert("excludeRepoRegex".to_owned(), InputValue::Str(value.clone()));
        }
        if let Some(contexts) = &filters.search_contexts {
            object.insert(
                "searchContexts".to_owned(),
                InputValue::List(contexts.iter().cloned().map(InputValue::Str).collect()),
            );
        }
        entries.push(("filters".to_owned(), InputValue::Object(object)));
    }
    if let Some(options) = &case.series_display_options {
        let mut object = BTreeMap::new();
        if let Some(sort) = &options.sort_options {
            let mut sort_object = BTreeMap::new();
            if let Some(value) = &sort.mode {
                sort_object.insert("mode".to_owned(), InputValue::Str(value.clone()));
            }
            if let Some(value) = &sort.direction {
                sort_object.insert("direction".to_owned(), InputValue::Str(value.clone()));
            }
            object.insert("sortOptions".to_owned(), InputValue::Object(sort_object));
        }
        if let Some(value) = options.limit {
            object.insert("limit".to_owned(), InputValue::Int(i64::from(value)));
        }
        if let Some(value) = options.num_samples {
            object.insert("numSamples".to_owned(), InputValue::Int(i64::from(value)));
        }
        entries.push((
            "seriesDisplayOptions".to_owned(),
            InputValue::Object(object),
        ));
    }
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

fn object_from(value: FieldValue) -> Result<Arc<dyn ObjectValue>, String> {
    match value {
        FieldValue::Object(object) => Ok(object),
        _ => Err("expected an object".to_owned()),
    }
}

fn field(
    object: &dyn ObjectValue,
    name: &str,
    actor: Actor,
) -> Result<FieldValue, String> {
    object
        .field(name, &Arguments::default(), actor)
        .map_err(|error| error.message)?
        .ok_or_else(|| format!("unresolved {}.{}", object.type_name(), name))
}

fn scalar_string(value: FieldValue) -> Result<Value, String> {
    match value {
        FieldValue::Json(json) => Ok(json_to_serde(&json)),
        FieldValue::Null => Ok(Value::Null),
        _ => Err("expected a scalar".to_owned()),
    }
}

fn render_point(value: FieldValue, actor: Actor) -> Result<Value, String> {
    let object = object_from(value)?;
    let mut map = Map::new();
    map.insert(
        "dateTime".to_owned(),
        scalar_string(field(object.as_ref(), "dateTime", actor)?)?,
    );
    map.insert(
        "value".to_owned(),
        scalar_string(field(object.as_ref(), "value", actor)?)?,
    );
    map.insert(
        "diffQuery".to_owned(),
        scalar_string(field(object.as_ref(), "diffQuery", actor)?)?,
    );
    map.insert(
        "pointInTimeQuery".to_owned(),
        scalar_string(field(object.as_ref(), "pointInTimeQuery", actor)?)?,
    );
    Ok(Value::Object(map))
}

fn render_series(value: FieldValue, actor: Actor) -> Result<Value, String> {
    let object = object_from(value)?;
    let mut map = Map::new();
    map.insert(
        "seriesId".to_owned(),
        scalar_string(field(object.as_ref(), "seriesId", actor)?)?,
    );
    map.insert(
        "label".to_owned(),
        scalar_string(field(object.as_ref(), "label", actor)?)?,
    );

    let points = field(object.as_ref(), "points", actor)?;
    let FieldValue::List { items, .. } = points else {
        return Err("points is not a list".to_owned());
    };
    let mut rendered_points = Vec::with_capacity(items.len());
    for point in items {
        rendered_points.push(render_point(point, actor)?);
    }
    map.insert("points".to_owned(), Value::Array(rendered_points));

    let status = field(object.as_ref(), "status", actor)?;
    let status = object_from(status)?;
    let mut status_map = Map::new();
    for name in [
        "totalPoints",
        "pendingJobs",
        "completedJobs",
        "failedJobs",
        "backfillQueuedAt",
        "isLoadingData",
    ] {
        status_map.insert(
            name.to_owned(),
            scalar_string(field(status.as_ref(), name, actor)?)?,
        );
    }
    map.insert("status".to_owned(), Value::Object(status_map));

    Ok(Value::Object(map))
}

fn error_document(message: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(message));
    Value::Object(map)
}

#[tokio::test(flavor = "multi_thread")]
async fn insights_data_series_harness() {
    if std::env::var("SG_M931A_DATA_SERIES_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M931A_CORPUS").expect("M931A_CORPUS");
    let out = std::env::var("M931A_OUT").expect("M931A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let frontend_dsn =
        std::env::var("SG_QTEST_FRONTEND_DSN").expect("SG_QTEST_FRONTEND_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

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
    let handle: InsightsHandle = Arc::new(
        PgInsightsStore::new(pool).with_frontend_pool(frontend_pool),
    );

    for case in &corpus.cases {
        let actor = Actor::Internal;
        let document: Value = (|| {
            let value = insight_views(&handle, &view_arguments(case), actor)
                .map_err(|error| error.message)?;
            let connection = object_from(value)?;
            let nodes = field(connection.as_ref(), "nodes", actor)?;
            let FieldValue::List { items, .. } = nodes else {
                return Err("nodes is not a list".to_owned());
            };
            let view = match items.into_iter().next() {
                Some(view) => object_from(view)?,
                None => return Ok(error_document("insight view not found".to_owned())),
            };

            let mut map = Map::new();
            match view.field("dataSeries", &Arguments::default(), actor) {
                Err(error) => {
                    map.insert(
                        "dataSeriesError".to_owned(),
                        Value::String(error.message),
                    );
                }
                Ok(None) => {
                    map.insert(
                        "dataSeriesError".to_owned(),
                        Value::String("unresolved InsightView.dataSeries".to_owned()),
                    );
                }
                Ok(Some(FieldValue::List { items, .. })) => {
                    let mut series = Vec::with_capacity(items.len());
                    for item in items {
                        series.push(render_series(item, actor)?);
                    }
                    map.insert("dataSeries".to_owned(), Value::Array(series));
                }
                Ok(Some(other)) => {
                    return Err(format!(
                        "dataSeries is not a list: {:?}",
                        std::mem::discriminant(&other)
                    ))
                }
            }

            match view.field("seriesCount", &Arguments::default(), actor) {
                Err(error) => {
                    map.insert(
                        "seriesCountError".to_owned(),
                        Value::String(error.message),
                    );
                }
                Ok(Some(FieldValue::Json(Json::Int(value)))) => {
                    map.insert("seriesCount".to_owned(), Value::from(value));
                }
                Ok(Some(FieldValue::Json(Json::UInt(value)))) => {
                    map.insert("seriesCount".to_owned(), Value::from(value));
                }
                Ok(Some(FieldValue::Json(Json::Null))) | Ok(None) => {
                    map.insert("seriesCount".to_owned(), Value::Null);
                }
                Ok(Some(_)) => return Err("seriesCount is not an int".to_owned()),
            }

            Ok(Value::Object(map))
        })()
        .unwrap_or_else(|error: String| error_document(error));

        let payload = serde_json::to_vec_pretty(&document).expect("serialize");
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            payload,
        )
        .expect("write document");
    }
}
