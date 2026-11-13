//! M9.30b differential of the Code Insights view/series write surface against
//! the Go baseline (`cmd/frontend/internal/insights/resolvers`).
//!
//! Both sides reset the scratch databases from the same fixtures before each
//! case, consume `data/insights_view_mutations/corpus.json`, call the
//! production resolvers directly, and serialize the returned payload document
//! plus a snapshot of the insight view/series/grant and dashboard tables.
//! The Go oracle is `zz_m930b_view_mutations_oracle_test.go` (untracked).

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::insights_graphql::{
    insight_series_query_status, marshal_dashboard_id, mutation_field, InsightsHandle,
};
use sg_service_frontend::insights_store::PgInsightsStore;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct RepoScope {
    #[serde(default)]
    repositories: Vec<String>,
    #[serde(default, rename = "repositoryCriteria")]
    repository_criteria: Option<String>,
}

#[derive(Deserialize, Default)]
struct TimeScope {
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    value: Option<i32>,
}

#[derive(Deserialize, Default)]
struct Filters {
    #[serde(default, rename = "includeRepoRegex")]
    include_repo_regex: Option<String>,
    #[serde(default, rename = "excludeRepoRegex")]
    exclude_repo_regex: Option<String>,
    #[serde(default, rename = "searchContexts")]
    search_contexts: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct SortOptions {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    direction: Option<String>,
}

#[derive(Deserialize, Default)]
struct SeriesFixture {
    #[serde(default, rename = "seriesId")]
    series_id: Option<String>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default, rename = "lineColor")]
    line_color: Option<String>,
    #[serde(default, rename = "repositoryScope")]
    repository_scope: Option<RepoScope>,
    #[serde(default, rename = "timeScope")]
    time_scope: Option<TimeScope>,
    #[serde(default, rename = "generatedFromCaptureGroups")]
    generated_from_capture_groups: Option<bool>,
    #[serde(default, rename = "groupBy")]
    group_by: Option<String>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default, rename = "actorUid")]
    actor_uid: i32,
    #[serde(default, rename = "limitedAccess")]
    limited_access: bool,
    #[serde(default, rename = "viewUniqueId")]
    view_unique_id: Option<String>,
    #[serde(default, rename = "viewIdOverride")]
    view_id_override: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, rename = "dataSeries")]
    data_series: Vec<SeriesFixture>,
    #[serde(default, rename = "repositoryScope")]
    repository_scope: Option<RepoScope>,
    #[serde(default, rename = "timeScope")]
    time_scope: Option<TimeScope>,
    #[serde(default, rename = "viewControlsSet")]
    view_controls_set: bool,
    #[serde(default)]
    filters: Option<Filters>,
    #[serde(default, rename = "sortOptions")]
    sort_options: Option<SortOptions>,
    #[serde(default)]
    limit: Option<i32>,
    #[serde(default, rename = "numSamples")]
    num_samples: Option<i32>,
    #[serde(default)]
    dashboards: Option<Vec<i64>>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default, rename = "otherThreshold")]
    other_threshold: Option<f64>,
    #[serde(default)]
    repositories: Vec<String>,
    #[serde(default, rename = "insightViewId")]
    insight_view_id: Option<String>,
    #[serde(default)]
    dashboard: Option<i64>,
    #[serde(default, rename = "seriesId")]
    series_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

fn string_list(values: &[String]) -> InputValue {
    InputValue::List(values.iter().cloned().map(InputValue::Str).collect())
}

fn repo_scope_value(scope: &RepoScope) -> InputValue {
    let mut object = BTreeMap::new();
    object.insert("repositories".to_owned(), string_list(&scope.repositories));
    if let Some(criteria) = &scope.repository_criteria {
        object.insert(
            "repositoryCriteria".to_owned(),
            InputValue::Str(criteria.clone()),
        );
    }
    InputValue::Object(object)
}

fn time_scope_value(scope: &TimeScope) -> InputValue {
    let mut step = BTreeMap::new();
    if let Some(unit) = &scope.unit {
        step.insert("unit".to_owned(), InputValue::Str(unit.clone()));
    }
    if let Some(value) = scope.value {
        step.insert("value".to_owned(), InputValue::Int(i64::from(value)));
    }
    let mut object = BTreeMap::new();
    object.insert("stepInterval".to_owned(), InputValue::Object(step));
    InputValue::Object(object)
}

fn series_value(series: &SeriesFixture) -> InputValue {
    let mut object = BTreeMap::new();
    if let Some(series_id) = &series.series_id {
        object.insert("seriesId".to_owned(), InputValue::Str(series_id.clone()));
    }
    object.insert("query".to_owned(), InputValue::Str(series.query.clone()));
    let mut options = BTreeMap::new();
    if let Some(label) = &series.label {
        options.insert("label".to_owned(), InputValue::Str(label.clone()));
    }
    if let Some(color) = &series.line_color {
        options.insert("lineColor".to_owned(), InputValue::Str(color.clone()));
    }
    if !options.is_empty() {
        object.insert("options".to_owned(), InputValue::Object(options));
    }
    if let Some(scope) = &series.repository_scope {
        object.insert("repositoryScope".to_owned(), repo_scope_value(scope));
    }
    if let Some(scope) = &series.time_scope {
        object.insert("timeScope".to_owned(), time_scope_value(scope));
    }
    if let Some(generated) = series.generated_from_capture_groups {
        object.insert(
            "generatedFromCaptureGroups".to_owned(),
            InputValue::Bool(generated),
        );
    }
    if let Some(group_by) = &series.group_by {
        object.insert("groupBy".to_owned(), InputValue::Str(group_by.clone()));
    }
    InputValue::Object(object)
}

fn filters_value(filters: &Filters) -> InputValue {
    let mut object = BTreeMap::new();
    if let Some(value) = &filters.include_repo_regex {
        object.insert("includeRepoRegex".to_owned(), InputValue::Str(value.clone()));
    }
    if let Some(value) = &filters.exclude_repo_regex {
        object.insert("excludeRepoRegex".to_owned(), InputValue::Str(value.clone()));
    }
    if let Some(contexts) = &filters.search_contexts {
        object.insert("searchContexts".to_owned(), string_list(contexts));
    }
    InputValue::Object(object)
}

fn view_controls_value(case: &Case) -> InputValue {
    let mut controls = BTreeMap::new();
    if let Some(filters) = &case.filters {
        controls.insert("filters".to_owned(), filters_value(filters));
    }
    let mut options = BTreeMap::new();
    if let Some(sort) = &case.sort_options {
        let mut sort_object = BTreeMap::new();
        if let Some(mode) = &sort.mode {
            sort_object.insert("mode".to_owned(), InputValue::Str(mode.clone()));
        }
        if let Some(direction) = &sort.direction {
            sort_object.insert("direction".to_owned(), InputValue::Str(direction.clone()));
        }
        options.insert("sortOptions".to_owned(), InputValue::Object(sort_object));
    }
    if let Some(limit) = case.limit {
        options.insert("limit".to_owned(), InputValue::Int(i64::from(limit)));
    }
    if let Some(samples) = case.num_samples {
        options.insert("numSamples".to_owned(), InputValue::Int(i64::from(samples)));
    }
    controls.insert("seriesDisplayOptions".to_owned(), InputValue::Object(options));
    InputValue::Object(controls)
}

fn dashboards_value(case: &Case) -> Option<InputValue> {
    case.dashboards.as_ref().map(|ids| {
        InputValue::List(
            ids.iter()
                .map(|id| InputValue::Str(marshal_dashboard_id(*id)))
                .collect(),
        )
    })
}

fn line_input(case: &Case, update: bool) -> InputValue {
    let mut input = BTreeMap::new();
    let mut options = BTreeMap::new();
    if let Some(title) = &case.title {
        options.insert("title".to_owned(), InputValue::Str(title.clone()));
    }
    if update {
        input.insert("presentationOptions".to_owned(), InputValue::Object(options));
    } else {
        input.insert("options".to_owned(), InputValue::Object(options));
    }
    input.insert(
        "dataSeries".to_owned(),
        InputValue::List(case.data_series.iter().map(series_value).collect()),
    );
    if let Some(scope) = &case.repository_scope {
        input.insert("repositoryScope".to_owned(), repo_scope_value(scope));
    }
    if let Some(scope) = &case.time_scope {
        input.insert("timeScope".to_owned(), time_scope_value(scope));
    }
    if case.view_controls_set {
        input.insert("viewControls".to_owned(), view_controls_value(case));
    }
    if !update {
        if let Some(dashboards) = dashboards_value(case) {
            input.insert("dashboards".to_owned(), dashboards);
        }
    }
    InputValue::Object(input)
}

fn pie_input(case: &Case) -> InputValue {
    let mut input = BTreeMap::new();
    input.insert(
        "query".to_owned(),
        InputValue::Str(case.query.clone().unwrap_or_default()),
    );
    let mut scope = BTreeMap::new();
    scope.insert("repositories".to_owned(), string_list(&case.repositories));
    input.insert("repositoryScope".to_owned(), InputValue::Object(scope));
    let mut presentation = BTreeMap::new();
    presentation.insert(
        "title".to_owned(),
        InputValue::Str(case.title.clone().unwrap_or_default()),
    );
    presentation.insert(
        "otherThreshold".to_owned(),
        InputValue::Float(case.other_threshold.unwrap_or(0.0)),
    );
    input.insert(
        "presentationOptions".to_owned(),
        InputValue::Object(presentation),
    );
    if let Some(dashboards) = dashboards_value(case) {
        input.insert("dashboards".to_owned(), dashboards);
    }
    InputValue::Object(input)
}

fn relay_view_id(case: &Case) -> String {
    if let Some(raw) = &case.view_id_override {
        return raw.clone();
    }
    let unique_id = case.view_unique_id.clone().unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE.encode(format!("insight_view:\"{unique_id}\""))
}

fn arguments_for(case: &Case) -> (String, Arguments) {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let field = match case.op.as_str() {
        "createLine" => {
            entries.push(("input".to_owned(), line_input(case, false)));
            "createLineChartSearchInsight"
        }
        "updateLine" => {
            entries.push(("id".to_owned(), InputValue::Str(relay_view_id(case))));
            entries.push(("input".to_owned(), line_input(case, true)));
            "updateLineChartSearchInsight"
        }
        "createPie" => {
            entries.push(("input".to_owned(), pie_input(case)));
            "createPieChartSearchInsight"
        }
        "updatePie" => {
            entries.push(("id".to_owned(), InputValue::Str(relay_view_id(case))));
            entries.push(("input".to_owned(), pie_input(case)));
            "updatePieChartSearchInsight"
        }
        "delete" => {
            entries.push(("id".to_owned(), InputValue::Str(relay_view_id(case))));
            "deleteInsightView"
        }
        "save" => {
            let unique_id = case.insight_view_id.clone().unwrap_or_default();
            let view_id =
                base64::engine::general_purpose::URL_SAFE.encode(format!("insight_view:\"{unique_id}\""));
            let mut input = BTreeMap::new();
            input.insert("insightViewId".to_owned(), InputValue::Str(view_id));
            let mut options = BTreeMap::new();
            if let Some(title) = &case.title {
                options.insert("title".to_owned(), InputValue::Str(title.clone()));
            }
            input.insert("options".to_owned(), InputValue::Object(options));
            if let Some(dashboard) = case.dashboard {
                input.insert(
                    "dashboard".to_owned(),
                    InputValue::Str(marshal_dashboard_id(dashboard)),
                );
            }
            if case.view_controls_set {
                input.insert("viewControls".to_owned(), view_controls_value(case));
            }
            entries.push(("input".to_owned(), InputValue::Object(input)));
            "saveInsightAsNewView"
        }
        "updateSeries" => {
            let mut input = BTreeMap::new();
            input.insert(
                "seriesId".to_owned(),
                InputValue::Str(case.series_id.clone().unwrap_or_default()),
            );
            if let Some(enabled) = case.enabled {
                input.insert("enabled".to_owned(), InputValue::Bool(enabled));
            }
            entries.push(("input".to_owned(), InputValue::Object(input)));
            "updateInsightSeries"
        }
        "status" => "insightSeriesQueryStatus",
        other => panic!("unknown op {other}"),
    };
    (field.to_owned(), Arguments::from_entries(entries))
}

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "InsightViewPayload" => Some(&["view"]),
        "InsightView" => Some(&[
            "defaultFilters",
            "presentation",
            "dataSeriesDefinitions",
            "dashboardReferenceCount",
            "isFrozen",
            "defaultSeriesDisplayOptions",
            "repositoryDefinition",
            "timeScope",
        ]),
        "InsightViewFilters" => Some(&["includeRepoRegex", "excludeRepoRegex", "searchContexts"]),
        "SeriesDisplayOptions" => Some(&["sortOptions", "limit", "numSamples"]),
        "SeriesSortOptions" => Some(&["mode", "direction"]),
        "LineChartInsightViewPresentation" => Some(&["title", "seriesPresentation"]),
        "LineChartDataSeriesPresentation" => Some(&["label", "color"]),
        "PieChartInsightViewPresentation" => Some(&["title", "otherThreshold"]),
        "SearchInsightDataSeriesDefinition" => Some(&[
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
        "InsightSeriesMetadataPayload" => Some(&["series"]),
        "InsightSeriesMetadata" => Some(&["seriesId", "query", "enabled"]),
        "InsightSeriesQueryStatus" => Some(&[
            "seriesId",
            "query",
            "enabled",
            "errored",
            "completed",
            "processing",
            "failed",
            "queued",
        ]),
        _ => None,
    }
}

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

fn render_field(value: FieldValue, actor: Actor, inject_typename: bool) -> Result<Value, String> {
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
        .ok_or_else(|| format!("view mutations diff: unhandled type {type_name}"))?;
    let arguments = Arguments::default();
    let mut map = Map::new();
    if inject_typename {
        map.insert(
            "__typename".to_owned(),
            Value::String(type_name.to_owned()),
        );
    }
    for field in fields {
        let value = object
            .field(field, &arguments, actor)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("view mutations diff: unresolved {type_name}.{field}"))?;
        let child_typename = union_field(type_name, field);
        map.insert(
            (*field).to_owned(),
            render_field(value, actor, child_typename)?,
        );
    }
    Ok(Value::Object(map))
}

fn error_document(message: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(message));
    Value::Object(map)
}

fn nullable_int(value: Option<i32>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

fn nullable_bool(value: Option<bool>) -> Value {
    value.map(Value::Bool).unwrap_or(Value::Null)
}

fn array_value(value: Option<String>) -> Result<Value, String> {
    match value {
        None => Ok(Value::Null),
        Some(text) => serde_json::from_str(&text).map_err(|error| error.to_string()),
    }
}

async fn snapshot(pool: &PgPool) -> Result<Value, String> {
    let mut views = Vec::new();
    let rows = sqlx::query(
        "SELECT CASE WHEN id >= 100 THEN '<view:' || id || '>' ELSE unique_id END AS uid,
                title, description, default_filter_include_repo_regex, default_filter_exclude_repo_regex,
                other_threshold, presentation_type::text AS presentation, is_frozen,
                array_to_json(default_filter_search_contexts)::text AS contexts,
                series_sort_mode::text AS sort_mode, series_sort_direction::text AS sort_direction,
                series_limit, series_num_samples, false AS deleted
         FROM insight_view ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        views.push(Value::Array(vec![
            Value::String(row.try_get("uid").map_err(|e| e.to_string())?),
            row.try_get::<Option<String>, _>("title").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("description").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("default_filter_include_repo_regex").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("default_filter_exclude_repo_regex").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<f64>, _>("other_threshold").map_err(|e| e.to_string())?
                .map(Value::from).unwrap_or(Value::Null),
            Value::String(row.try_get("presentation").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("is_frozen").map_err(|e| e.to_string())?),
            array_value(row.try_get("contexts").map_err(|e| e.to_string())?)?,
            row.try_get::<Option<String>, _>("sort_mode").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("sort_direction").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            nullable_int(row.try_get("series_limit").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("series_num_samples").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("deleted").map_err(|e| e.to_string())?),
        ]));
    }

    let mut series = Vec::new();
    let rows = sqlx::query(
        "SELECT CASE WHEN id >= 200 THEN '<series:' || id || '>' ELSE series_id END AS sid,
                query, array_to_json(repositories)::text AS repositories,
                sample_interval_unit::text AS unit, sample_interval_value,
                generated_from_capture_groups, generation_method, just_in_time, group_by,
                repository_criteria, (deleted_at IS NOT NULL) AS deleted, needs_migration
         FROM insight_series ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        series.push(Value::Array(vec![
            Value::String(row.try_get("sid").map_err(|e| e.to_string())?),
            Value::String(row.try_get("query").map_err(|e| e.to_string())?),
            array_value(row.try_get("repositories").map_err(|e| e.to_string())?)?,
            Value::String(row.try_get("unit").map_err(|e| e.to_string())?),
            Value::from(row.try_get::<i32, _>("sample_interval_value").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("generated_from_capture_groups").map_err(|e| e.to_string())?),
            Value::String(row.try_get("generation_method").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("just_in_time").map_err(|e| e.to_string())?),
            row.try_get::<Option<String>, _>("group_by").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("repository_criteria").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            Value::Bool(row.try_get("deleted").map_err(|e| e.to_string())?),
            nullable_bool(row.try_get("needs_migration").map_err(|e| e.to_string())?),
        ]));
    }

    let mut attached = Vec::new();
    let rows = sqlx::query(
        "SELECT CASE WHEN v.id >= 100 THEN '<view:' || v.id || '>' ELSE v.unique_id END AS uid,
                CASE WHEN s.id >= 200 THEN '<series:' || s.id || '>' ELSE s.series_id END AS sid,
                ivs.label, ivs.stroke
         FROM insight_view_series ivs
         JOIN insight_view v ON v.id = ivs.insight_view_id
         JOIN insight_series s ON s.id = ivs.insight_series_id
         ORDER BY ivs.insight_view_id, ivs.insight_series_id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        attached.push(Value::Array(vec![
            Value::String(row.try_get("uid").map_err(|e| e.to_string())?),
            Value::String(row.try_get("sid").map_err(|e| e.to_string())?),
            row.try_get::<Option<String>, _>("label").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
            row.try_get::<Option<String>, _>("stroke").map_err(|e| e.to_string())?
                .map(Value::String).unwrap_or(Value::Null),
        ]));
    }

    let mut view_grants = Vec::new();
    let rows = sqlx::query(
        "SELECT CASE WHEN v.id >= 100 THEN '<view:' || v.id || '>' ELSE v.unique_id END AS uid,
                g.user_id, g.org_id, g.global
         FROM insight_view_grants g
         JOIN insight_view v ON v.id = g.insight_view_id
         ORDER BY g.insight_view_id, g.user_id NULLS FIRST, g.org_id NULLS FIRST, g.global",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        view_grants.push(Value::Array(vec![
            Value::String(row.try_get("uid").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("user_id").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("org_id").map_err(|e| e.to_string())?),
            nullable_bool(row.try_get("global").map_err(|e| e.to_string())?),
        ]));
    }

    let mut dashboards = Vec::new();
    let rows = sqlx::query(
        "SELECT id, title, save, type, created_by_user_id, (deleted_at IS NOT NULL) AS deleted
         FROM dashboard ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        dashboards.push(Value::Array(vec![
            Value::from(row.try_get::<i32, _>("id").map_err(|e| e.to_string())?),
            Value::String(row.try_get("title").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("save").map_err(|e| e.to_string())?),
            Value::String(row.try_get("type").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("created_by_user_id").map_err(|e| e.to_string())?),
            Value::Bool(row.try_get("deleted").map_err(|e| e.to_string())?),
        ]));
    }

    let mut dashboard_grants = Vec::new();
    let rows = sqlx::query(
        "SELECT dashboard_id, user_id, org_id, global FROM dashboard_grants
         ORDER BY dashboard_id, user_id NULLS FIRST, org_id NULLS FIRST, global",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        dashboard_grants.push(Value::Array(vec![
            Value::from(row.try_get::<i32, _>("dashboard_id").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("user_id").map_err(|e| e.to_string())?),
            nullable_int(row.try_get("org_id").map_err(|e| e.to_string())?),
            nullable_bool(row.try_get("global").map_err(|e| e.to_string())?),
        ]));
    }

    let mut dashboard_views = Vec::new();
    let rows = sqlx::query(
        "SELECT div.dashboard_id,
                CASE WHEN v.id >= 100 THEN '<view:' || v.id || '>' ELSE v.unique_id END AS uid
         FROM dashboard_insight_view div
         JOIN insight_view v ON v.id = div.insight_view_id
         ORDER BY div.dashboard_id, div.insight_view_id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    for row in rows {
        dashboard_views.push(Value::Array(vec![
            Value::from(row.try_get::<i32, _>("dashboard_id").map_err(|e| e.to_string())?),
            Value::String(row.try_get("uid").map_err(|e| e.to_string())?),
        ]));
    }

    let mut map = Map::new();
    map.insert("views".to_owned(), Value::Array(views));
    map.insert("series".to_owned(), Value::Array(series));
    map.insert("attached".to_owned(), Value::Array(attached));
    map.insert("viewGrants".to_owned(), Value::Array(view_grants));
    map.insert("dashboards".to_owned(), Value::Array(dashboards));
    map.insert("dashboardGrants".to_owned(), Value::Array(dashboard_grants));
    map.insert("dashboardViews".to_owned(), Value::Array(dashboard_views));
    Ok(Value::Object(map))
}

#[tokio::test(flavor = "multi_thread")]
async fn insights_view_mutations_harness() {
    if std::env::var("SG_M930B_VIEW_MUT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M930B_CORPUS").expect("M930B_CORPUS");
    let out = std::env::var("M930B_OUT").expect("M930B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let frontend_dsn =
        std::env::var("SG_QTEST_FRONTEND_DSN").expect("SG_QTEST_FRONTEND_DSN");
    let fixtures_path = std::env::var("M930B_FIXTURES").expect("M930B_FIXTURES");
    let frontend_fixtures_path =
        std::env::var("M930B_FRONTEND_FIXTURES").expect("M930B_FRONTEND_FIXTURES");
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
        sqlx::raw_sql(&frontend_fixtures)
            .execute(&frontend_pool)
            .await
            .expect("seed frontend");

        let handle: InsightsHandle = Arc::new(
            PgInsightsStore::new(pool.clone())
                .with_frontend_pool(frontend_pool.clone())
                .with_code_insights_licensed(!case.limited_access),
        );
        let actor = if case.actor_uid == 0 {
            Actor::Internal
        } else {
            Actor::User { uid: case.actor_uid }
        };
        let (field, arguments) = arguments_for(case);
        let result: Result<Value, String> = (|| {
            if case.op == "status" {
                let value =
                    insight_series_query_status(&handle, actor).map_err(|error| error.message)?;
                let rendered = render_field(value, actor, false)?;
                let mut map = Map::new();
                map.insert("statuses".to_owned(), rendered);
                return Ok(Value::Object(map));
            }
            let value = mutation_field(&handle, &field, &arguments, actor)
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
                "updateSeries" => {
                    let payload = match value {
                        FieldValue::Object(object) => object,
                        _ => return Err("payload is not an object".to_owned()),
                    };
                    let series = payload
                        .field("series", &Arguments::default(), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "payload.series unresolved".to_owned())?;
                    let mut map = Map::new();
                    map.insert("series".to_owned(), render_field(series, actor, false)?);
                    Ok(Value::Object(map))
                }
                _ => {
                    let payload = match value {
                        FieldValue::Object(object) => object,
                        _ => return Err("payload is not an object".to_owned()),
                    };
                    let view = payload
                        .field("view", &Arguments::default(), actor)
                        .map_err(|error| error.message)?
                        .ok_or_else(|| "payload.view unresolved".to_owned())?;
                    let mut map = Map::new();
                    map.insert("view".to_owned(), render_field(view, actor, false)?);
                    Ok(Value::Object(map))
                }
            }
        })();

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
