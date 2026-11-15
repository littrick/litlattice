//! M9.29b differential of the read-only Code Insights GraphQL surface against
//! the Go baseline (`cmd/frontend/internal/insights/resolvers` + the real
//! code-insights store).
//!
//! Both sides read the scratch database seeded by
//! `run_insights_graphql_diff.sh` and consume `data/insights/corpus.json`.
//! The Go oracle (`zz_m929b_insights_oracle_test.go`, untracked) and this
//! harness call the production resolvers directly and serialize the same
//! documents.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::insights_graphql::{insight_views, root_field, InsightsHandle};
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

#[derive(Deserialize, Default)]
struct ArgumentsFixture {
    #[serde(default)]
    first: Option<i64>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "excludeIds")]
    exclude_ids: Vec<String>,
    #[serde(default)]
    find: Option<String>,
    #[serde(default, rename = "isFrozen")]
    is_frozen: Option<bool>,
    #[serde(default)]
    filters: Option<FiltersFixture>,
    #[serde(default, rename = "seriesDisplayOptions")]
    series_display_options: Option<SeriesDisplayOptionsFixture>,
    #[serde(default, rename = "dashboardId")]
    dashboard_id: Option<String>,
    #[serde(default, rename = "viewId")]
    view_id: Option<String>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default)]
    args: ArgumentsFixture,
    /// M9.47c: `licensing.Check(FeatureCodeInsights)` for the case (the Go
    /// oracle mocks the same way). Read-only fields do not consult it, but the
    /// store now always goes through the real seam.
    #[serde(default = "default_licensed")]
    licensed: bool,
}

fn default_licensed() -> bool {
    true
}

fn view_arguments(fixture: &ArgumentsFixture) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let mut push = |name: &str, value: InputValue| entries.push((name.to_owned(), value));
    if let Some(first) = fixture.first {
        push("first", InputValue::Int(first));
    }
    if let Some(value) = &fixture.after {
        push("after", InputValue::Str(value.clone()));
    }
    if let Some(value) = &fixture.id {
        push("id", InputValue::Str(value.clone()));
    }
    if !fixture.exclude_ids.is_empty() {
        push(
            "excludeIds",
            InputValue::List(
                fixture
                    .exclude_ids
                    .iter()
                    .cloned()
                    .map(InputValue::Str)
                    .collect(),
            ),
        );
    }
    if let Some(value) = &fixture.find {
        push("find", InputValue::Str(value.clone()));
    }
    if let Some(value) = fixture.is_frozen {
        push("isFrozen", InputValue::Bool(value));
    }
    if let Some(filters) = &fixture.filters {
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
        push("filters", InputValue::Object(object));
    }
    if let Some(options) = &fixture.series_display_options {
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
        push("seriesDisplayOptions", InputValue::Object(object));
    }
    Arguments::from_entries(entries)
}

fn dashboard_arguments(fixture: &ArgumentsFixture, id: Option<&String>) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    if let Some(first) = fixture.first {
        entries.push(("first".to_owned(), InputValue::Int(first)));
    }
    if let Some(value) = &fixture.after {
        entries.push(("after".to_owned(), InputValue::Str(value.clone())));
    }
    if let Some(value) = id {
        entries.push(("id".to_owned(), InputValue::Str(value.clone())));
    }
    Arguments::from_entries(entries)
}

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "InsightViewConnection" => Some(&["nodes", "totalCount", "pageInfo"]),
        "InsightView" => Some(&[
            "id",
            "defaultFilters",
            "appliedFilters",
            "presentation",
            "dataSeriesDefinitions",
            "dashboardReferenceCount",
            "isFrozen",
            "defaultSeriesDisplayOptions",
            "appliedSeriesDisplayOptions",
            "dashboards",
            "repositoryDefinition",
            "timeScope",
        ]),
        "InsightViewFilters" => Some(&["includeRepoRegex", "excludeRepoRegex", "searchContexts"]),
        "SeriesDisplayOptions" => Some(&["sortOptions", "limit", "numSamples"]),
        "SeriesSortOptions" => Some(&["mode", "direction"]),
        "LineChartInsightViewPresentation" => Some(&["title", "seriesPresentation"]),
        "LineChartDataSeriesPresentation" => Some(&["seriesId", "label", "color"]),
        "PieChartInsightViewPresentation" => Some(&["title", "otherThreshold"]),
        "SearchInsightDataSeriesDefinition" => Some(&[
            "seriesId",
            "query",
            "repositoryScope",
            "repositoryDefinition",
            "timeScope",
            "generatedFromCaptureGroups",
            "isCalculated",
            "groupBy",
        ]),
        "InsightRepositoryScope" => Some(&["repositories"]),
        "RepositorySearchScope" => Some(&["search", "allRepositories"]),
        "InsightIntervalTimeScope" => Some(&["unit", "value"]),
        "InsightsDashboardConnection" => Some(&["nodes", "pageInfo"]),
        "InsightsDashboard" => Some(&["id", "title", "views", "grants"]),
        "InsightsPermissionGrants" => Some(&["users", "organizations", "global"]),
        "PageInfo" => Some(&["hasNextPage", "endCursor"]),
        _ => None,
    }
}

/// Union-typed fields carry `__typename` in the Go document; mirror that
/// here. `SearchInsightDataSeriesDefinition.repositoryScope` is a plain
/// object field even though `InsightRepositoryScope` is a union member, so
/// it must not gain the key.
fn union_field(parent: &str, field: &str) -> bool {
    matches!(
        (parent, field),
        ("InsightView", "presentation")
            | ("InsightView", "repositoryDefinition")
            | ("InsightView", "timeScope")
            | ("InsightView", "dataSeriesDefinitions")
            | ("SearchInsightDataSeriesDefinition", "repositoryDefinition")
            | ("SearchInsightDataSeriesDefinition", "timeScope")
    )
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

fn render_field(
    value: FieldValue,
    actor: Actor,
    inject_typename: bool,
) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_field(item, actor, inject_typename)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => render_object(object.as_ref(), actor, inject_typename),
    }
}

fn render_object(
    object: &dyn ObjectValue,
    actor: Actor,
    inject_typename: bool,
) -> Result<Value, String> {
    let type_name = object.type_name();
    let fields = fields_for(type_name)
        .ok_or_else(|| format!("insights diff: unhandled type {type_name}"))?;
    let arguments = Arguments::default();
    let mut map = Map::new();
    if inject_typename {
        map.insert(
            "__typename".to_owned(),
            Value::String(type_name.to_owned()),
        );
    }
    for field in fields {
        // `InsightView.dashboards` and `InsightsDashboard.views` reference
        // each other, so expanding both would recurse forever. The Go oracle
        // renders nested dashboard `views` as an empty connection; the real
        // views document is compared through the `dashboardViews` case.
        if type_name == "InsightsDashboard" && *field == "views" {
            map.insert((*field).to_owned(), empty_view_connection());
            continue;
        }
        let value = object
            .field(field, &arguments, actor)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("insights diff: unresolved {type_name}.{field}"))?;
        let child_typename = union_field(type_name, field);
        map.insert(
            (*field).to_owned(),
            render_field(value, actor, child_typename)?,
        );
    }
    Ok(Value::Object(map))
}

/// Mirrors the oracle's cycle-breaking document for `InsightsDashboard.views`.
fn empty_view_connection() -> Value {
    let mut page_info = Map::new();
    page_info.insert("hasNextPage".to_owned(), Value::Bool(false));
    page_info.insert("endCursor".to_owned(), Value::Null);
    let mut connection = Map::new();
    connection.insert("nodes".to_owned(), Value::Array(Vec::new()));
    connection.insert("totalCount".to_owned(), Value::from(0));
    connection.insert("pageInfo".to_owned(), Value::Object(page_info));
    Value::Object(connection)
}

fn sort_document_nodes(document: &mut Value) {
    let Some(object) = document.as_object_mut() else {
        return;
    };
    let Some(Value::Array(nodes)) = object.get_mut("nodes") else {
        return;
    };
    nodes.sort_by(|left, right| {
        let left = left
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let right = right
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        left.cmp(&right)
    });
}

fn error_document(error: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(error));
    Value::Object(map)
}

fn object_from(value: FieldValue) -> Result<Arc<dyn ObjectValue>, String> {
    match value {
        FieldValue::Object(object) => Ok(object),
        _ => Err("expected an object".to_owned()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn insights_graphql_harness() {
    if std::env::var("SG_M929B_INSIGHTS_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M929B_CORPUS").expect("M929B_CORPUS");
    let out = std::env::var("M929B_OUT").expect("M929B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let base_pool = pool.clone();

    for case in &corpus.cases {
        let handle: InsightsHandle = Arc::new(
            PgInsightsStore::new(base_pool.clone()).with_license_provider(Arc::new(
                sg_service_frontend::insights_store::FixedCodeInsightsLicense(case.licensed),
            )),
        );
        let actor = Actor::Internal;
        let result: Result<Value, String> = (|| {
            match case.op.as_str() {
                "insightViews" => {
                    let value = insight_views(&handle, &view_arguments(&case.args), actor)
                        .map_err(|error| error.message)?;
                    render_document(value, actor)
                }
                "insightsDashboards" => {
                    let value = root_field(
                        &handle,
                        "insightsDashboards",
                        &dashboard_arguments(&case.args, case.args.id.as_ref()),
                        actor,
                    )
                    .map_err(|error| error.message)?
                    .ok_or_else(|| "insightsDashboards unresolved".to_owned())?;
                    render_document(value, actor)
                }
                "dashboardViews" => {
                    // The Go oracle selects the dashboard with an ID-only
                    // lookup; First/After belong to `Dashboard.views`.
                    let mut lookup: Vec<(String, InputValue)> = Vec::new();
                    if let Some(id) = &case.args.dashboard_id {
                        lookup.push(("id".to_owned(), InputValue::Str(id.clone())));
                    }
                    let connection = root_field(
                        &handle,
                        "insightsDashboards",
                        &Arguments::from_entries(lookup),
                        actor,
                    )
                    .map_err(|error| error.message)?
                    .ok_or_else(|| "insightsDashboards unresolved".to_owned())?;
                    let connection = object_from(connection)?;
                    let nodes = connection
                        .field("nodes", &Arguments::default(), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "dashboards.nodes unresolved".to_owned())?;
                    let FieldValue::List { items, .. } = nodes else {
                        return Err("dashboards.nodes is not a list".to_owned());
                    };
                    let dashboard = items
                        .into_iter()
                        .next()
                        .ok_or_else(|| "dashboard not found".to_owned())?;
                    let dashboard = object_from(dashboard)?;
                    let views = dashboard
                        .field("views", &dashboard_arguments(&case.args, None), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "dashboard.views unresolved".to_owned())?;
                    render_document(views, actor)
                }
                "viewDashboards" => {
                    let connection = insight_views(
                        &handle,
                        &view_arguments(&ArgumentsFixture {
                            id: case.args.view_id.clone(),
                            ..Default::default()
                        }),
                        actor,
                    )
                    .map_err(|error| error.message)?;
                    let connection = object_from(connection)?;
                    let nodes = connection
                        .field("nodes", &Arguments::default(), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "insightViews.nodes unresolved".to_owned())?;
                    let FieldValue::List { items, .. } = nodes else {
                        return Err("insightViews.nodes is not a list".to_owned());
                    };
                    let view = items
                        .into_iter()
                        .next()
                        .ok_or_else(|| "insight view not found".to_owned())?;
                    let view = object_from(view)?;
                    let dashboards = view
                        .field("dashboards", &dashboard_arguments(&case.args, case.args.id.as_ref()), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "insightView.dashboards unresolved".to_owned())?;
                    render_document(dashboards, actor)
                }
                other => Err(format!("unknown op {other}")),
            }
        })();
        let document = match result {
            Ok(mut document) => {
                sort_document_nodes(&mut document);
                document
            }
            Err(error) => error_document(error),
        };
        let payload = serde_json::to_vec_pretty(&document).expect("serialize");
        std::fs::write(std::path::Path::new(&out).join(format!("{}.json", case.name)), payload)
            .expect("write document");
    }
}

fn render_document(value: FieldValue, actor: Actor) -> Result<Value, String> {
    render_field(value, actor, false)
}
