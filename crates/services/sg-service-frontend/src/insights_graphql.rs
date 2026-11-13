#![forbid(unsafe_code)]

//! Code Insights GraphQL surface, first (read-only) phase (M9.29b).
//!
//! Ports the read side of `internal/insights/resolvers`:
//!
//! - `Query.insightViews(first, after, id, excludeIds, find, isFrozen,
//!   filters, seriesDisplayOptions)` (`insight_view_resolvers.go`);
//! - `Query.insightsDashboards(first, after, id)` and
//!   `InsightView.dashboards(first, after, id)` (`dashboard_resolvers.go`);
//! - the `InsightView` / `InsightsDashboard` object fields backed by the
//!   persisted code-insights tables (`insight_view`, `insight_series`,
//!   `insight_view_series`, `insight_view_grants`, `dashboard`,
//!   `dashboard_grants`, `dashboard_insight_view`).
//!
//! Not covered (documented in
//! `dev/rs/docs/overviews/m9_29b_insights_overview.md`): the query engine and
//! aggregation computation (`InsightsSeries.points`, `InsightSeriesStatus`,
//! `InsightView.dataSeries`, `seriesCount`), live preview, every mutation, and
//! `node(InsightView)` (the baseline never registers the `insight_view` node
//! kind, so `node(id)` returns `invalid id`; we reproduce that).
//!
//! No table/column/Redis key is added.

use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;

pub type InsightsHandle = Arc<dyn InsightsDatabase>;

/// `insightKind` (`insight_view_resolvers.go:62`).
pub const INSIGHT_KIND: &str = "insight_view";
/// `dashboardKind` (`dashboard_id.go:26`).
pub const DASHBOARD_KIND: &str = "dashboard";

// ---------------------------------------------------------------------------
// Data and the database seam
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InsightViewFilters {
    pub include_repo_regex: Option<String>,
    pub exclude_repo_regex: Option<String>,
    /// `None` reproduces a SQL `NULL` array (Go nil slice → JSON null).
    pub search_contexts: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InsightSeriesData {
    pub series_id: String,
    pub query: String,
    pub label: String,
    pub stroke: String,
    pub repositories: Vec<String>,
    pub sample_interval_unit: String,
    pub sample_interval_value: i32,
    pub generated_from_capture_groups: bool,
    pub just_in_time: bool,
    pub generation_method: String,
    pub group_by: Option<String>,
    pub repository_criteria: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsightViewData {
    pub view_id: i32,
    pub unique_id: String,
    pub title: String,
    pub description: String,
    pub filters: InsightViewFilters,
    pub other_threshold: Option<f64>,
    pub presentation_type: String,
    pub is_frozen: bool,
    pub sort_mode: Option<String>,
    pub sort_direction: Option<String>,
    pub series_limit: Option<i32>,
    pub series_num_samples: Option<i32>,
    pub series: Vec<InsightSeriesData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardData {
    pub id: i32,
    pub title: String,
    pub insight_ids: Vec<String>,
    pub user_grants: Vec<i64>,
    pub org_grants: Vec<i64>,
    pub global_grant: bool,
}

#[derive(Debug, Clone, Default)]
pub struct InsightQueryOptions {
    pub after: Option<String>,
    pub limit: i32,
    pub is_frozen: Option<bool>,
    pub find: Option<String>,
    pub unique_id: Option<String>,
    pub exclude_ids: Vec<String>,
    pub dashboard_id: Option<i32>,
    pub user_ids: Vec<i32>,
    pub org_ids: Vec<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct DashboardQueryOptions {
    pub after: Option<i32>,
    pub limit: i32,
    pub ids: Vec<i32>,
    pub with_view_unique_id: Option<String>,
    pub user_ids: Vec<i32>,
    pub org_ids: Vec<i32>,
}

/// The read-only database seam. The production implementation is
/// [`crate::insights_store::PgInsightsStore`].
pub trait InsightsDatabase: InsightsWriteDatabase + Send + Sync + std::fmt::Debug {
    /// `getUserPermissions` (`resolver.go:132`): the actor's user id (if any)
    /// plus the ids of the orgs it belongs to. UID 0 (anonymous/internal)
    /// yields two empty lists, leaving only `global is true` grants.
    fn user_permission_ids(&self, actor: Actor) -> Result<(Vec<i32>, Vec<i32>), String>;

    /// `InsightStore.GetAllMapped` with `InsightQueryArgs`.
    fn list_views(&self, opts: &InsightQueryOptions) -> Result<Vec<InsightViewData>, String>;

    /// `InsightStore.GetAllOnDashboard` + `GroupByView`.
    fn list_dashboard_views(
        &self,
        dashboard_id: i32,
        after: Option<i32>,
        limit: i32,
    ) -> Result<Vec<InsightViewData>, String>;

    /// `DBDashboardStore.GetDashboards`.
    fn list_dashboards(&self, opts: &DashboardQueryOptions) -> Result<Vec<DashboardData>, String>;

    /// `InsightStore.GetReferenceCount`.
    fn dashboard_reference_count(&self, view_id: i32) -> Result<i64, String>;
}

// ---------------------------------------------------------------------------
// Relay IDs.
// ---------------------------------------------------------------------------

fn relay_encode(kind: &str, spec: &str) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}

/// `relay.MarshalID("insight_view", uniqueID)`.
pub fn marshal_insight_view_id(unique_id: &str) -> String {
    relay_encode(INSIGHT_KIND, &go_json_quote(unique_id))
}

/// `newRealDashboardID(id).marshal()`: `dashboard:{"IdType":"custom","Arg":<id>}`.
pub fn marshal_dashboard_id(id: i64) -> String {
    relay_encode(DASHBOARD_KIND, &format!(r#"{{"IdType":"custom","Arg":{id}}}"#))
}

/// `MarshalUserID` (`relay.MarshalID("User", id)`).
pub fn marshal_user_id(id: i64) -> String {
    relay_encode("User", &id.to_string())
}

/// `MarshalOrgID` (`relay.MarshalID("Org", id)`).
pub fn marshal_org_id(id: i64) -> String {
    relay_encode("Org", &id.to_string())
}

/// Go `encoding/json` string quoting (only differs from a naive quote for
/// control characters, which unique ids will not contain).
fn go_json_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// `relay.UnmarshalSpec` into a string (insight unique id / dashboard view
/// after cursor).
/// Go's `base64.CorruptInputError` message, used verbatim by relay IDs.
fn base64_message(error: base64::DecodeError) -> String {
    let offset = match error {
        base64::DecodeError::InvalidByte(offset, _)
        | base64::DecodeError::InvalidLength(offset)
        | base64::DecodeError::InvalidLastSymbol(offset, _) => offset,
        base64::DecodeError::InvalidPadding => 0,
    };
    format!("illegal base64 data at input byte {offset}")
}

pub fn unmarshal_string_spec(id: &str) -> Result<String, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(base64_message)?;
    let decoded = String::from_utf8(decoded).map_err(|err| err.to_string())?;
    let (_, spec) = decoded
        .split_once(':')
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let mut chars = spec.chars();
    let first = chars.next();
    let last = spec.chars().last();
    if first == Some('"') && last == Some('"') && spec.len() >= 2 {
        Ok(spec[1..spec.len() - 1].to_owned())
    } else {
        Err("invalid graphql.ID".to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DashboardId {
    id_type: &'static str,
    arg: i64,
}

/// `unmarshalDashboardID`: `relay.UnmarshalSpec` into
/// `dashboardID{IdType string, Arg int64}`.
fn unmarshal_dashboard_id(id: &str) -> Result<DashboardId, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(base64_message)?;
    let decoded = String::from_utf8(decoded).map_err(|err| err.to_string())?;
    let (_, spec) = decoded
        .split_once(':')
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let id_type = extract_json_field(spec, "IdType")
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let arg = extract_json_int(spec, "Arg").ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let id_type: &'static str = if id_type.eq_ignore_ascii_case("custom") {
        "custom"
    } else if id_type.eq_ignore_ascii_case("user") {
        "user"
    } else if id_type.eq_ignore_ascii_case("organization") {
        "organization"
    } else {
        return Err("invalid graphql.ID".to_owned());
    };
    Ok(DashboardId { id_type, arg })
}

fn extract_json_field(input: &str, name: &str) -> Option<String> {
    let needle = format!("\"{name}\"");
    let rest = input.split_once(&needle)?.1;
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

fn extract_json_int(input: &str, name: &str) -> Option<i64> {
    let needle = format!("\"{name}\"");
    let rest = input.split_once(&needle)?.1;
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let digits: String = rest
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect();
    digits.parse().ok()
}

// ---------------------------------------------------------------------------
// Argument helpers.
// ---------------------------------------------------------------------------

fn int_arg(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments.i64(name).map(|value| value as i32)
}

fn string_list(arguments: &Arguments, name: &str) -> Option<Vec<String>> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| match item {
                    InputValue::Str(value) => Some(value.clone()),
                    _ => None,
                })
                .collect(),
        ),
        Some(InputValue::Null) | None => None,
        _ => None,
    }
}

fn input_object<'a>(
    arguments: &'a Arguments,
    name: &str,
) -> Option<&'a std::collections::BTreeMap<String, InputValue>> {
    match arguments.get(name) {
        Some(InputValue::Object(value)) => Some(value),
        _ => None,
    }
}

fn object_str(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<String> {
    match object.get(name) {
        Some(InputValue::Str(value)) => Some(value.clone()),
        _ => None,
    }
}

fn object_int(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<i32> {
    match object.get(name) {
        Some(InputValue::Int(value)) => Some(*value as i32),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Root dispatch.
// ---------------------------------------------------------------------------

pub fn is_root_field(field: &str) -> bool {
    matches!(
        field,
        "insightViews" | "insightsDashboards" | "insightSeriesQueryStatus"
    )
}

pub fn root_field(
    database: &InsightsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "insightViews" => Ok(Some(insight_views(database, arguments, actor)?)),
        "insightsDashboards" => {
            let (user_ids, org_ids) = database
                .user_permission_ids(actor)
                .map_err(ExecError::new)?;
            Ok(Some(dashboards_connection(
                database,
                arguments,
                user_ids,
                org_ids,
                None,
            )?))
        }
        "insightSeriesQueryStatus" => {
            Ok(Some(insight_series_query_status(database, actor)?))
        }
        _ => Ok(None),
    }
}

/// `Resolver.InsightViews` + `InsightViewQueryConnectionResolver.computeViews`.
pub fn insight_views(
    database: &InsightsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let (user_ids, org_ids) = database
        .user_permission_ids(actor)
        .map_err(ExecError::new)?;

    let first = int_arg(arguments, "first");
    let mut opts = InsightQueryOptions {
        user_ids,
        org_ids,
        limit: first.map(|value| value + 1).unwrap_or(0),
        ..Default::default()
    };
    if let Some(after) = arguments.string("after") {
        opts.after = Some(unmarshal_string_spec(after).map_err(ExecError::new)?);
    }
    opts.is_frozen = arguments.bool("isFrozen");
    opts.find = arguments.string("find").map(str::to_owned);
    if let Some(id) = arguments.string("id") {
        opts.unique_id = Some(unmarshal_string_spec(id).map_err(ExecError::new)?);
    }
    if let Some(ids) = string_list(arguments, "excludeIds") {
        let mut unique_ids = Vec::with_capacity(ids.len());
        for id in &ids {
            unique_ids.push(unmarshal_string_spec(id).map_err(ExecError::new)?);
        }
        opts.exclude_ids = unique_ids;
    }

    let override_filters = arguments
        .get("filters")
        .and_then(|_| input_object(arguments, "filters"))
        .map(|object| InsightViewFilters {
            include_repo_regex: object_str(object, "includeRepoRegex"),
            exclude_repo_regex: object_str(object, "excludeRepoRegex"),
            search_contexts: match object.get("searchContexts") {
                Some(InputValue::List(items)) => Some(
                    items
                        .iter()
                        .filter_map(|item| match item {
                            InputValue::Str(value) => Some(value.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            },
        });

    let override_series_options = input_object(arguments, "seriesDisplayOptions").map(|object| {
        let sort_options = match object.get("sortOptions") {
            Some(InputValue::Object(sort)) => Some(SeriesSortOptionsData {
                mode: object_str(sort, "mode"),
                direction: object_str(sort, "direction"),
            }),
            _ => None,
        };
        let mut num_samples = object_int(object, "numSamples");
        if let Some(value) = num_samples {
            if value > 90 {
                num_samples = Some(90);
            }
        }
        SeriesDisplayOptionsData {
            sort_options,
            limit: object_int(object, "limit"),
            num_samples,
        }
    });

    let mut views = database
        .list_views(&opts)
        .map_err(ExecError::new)?;
    let mut next = None;
    if let Some(first) = first {
        if views.len() == (first + 1) as usize {
            if views.len() >= 2 {
                next = Some(views[views.len() - 2].unique_id.clone());
                views.truncate(views.len() - 1);
            }
        }
    }

    // `TotalCount` ignores every filter and counts all views the actor can see.
    let total_count = database
        .list_views(&InsightQueryOptions {
            user_ids: opts.user_ids.clone(),
            org_ids: opts.org_ids.clone(),
            ..Default::default()
        })
        .map_err(ExecError::new)?
        .len() as i32;

    Ok(FieldValue::Object(Arc::new(InsightViewConnectionObject {
        views,
        next,
        total_count,
        database: database.clone(),
        override_filters,
        override_series_options,
    })))
}

/// `InsightsDashboards` / `dashboardConnectionResolver`.
fn dashboards_connection(
    database: &InsightsHandle,
    arguments: &Arguments,
    user_ids: Vec<i32>,
    org_ids: Vec<i32>,
    with_view_unique_id: Option<String>,
) -> Result<FieldValue, ExecError> {
    let mut opts = DashboardQueryOptions {
        user_ids,
        org_ids,
        limit: int_arg(arguments, "first").unwrap_or(0),
        with_view_unique_id,
        ..Default::default()
    };
    if let Some(after) = arguments.string("after") {
        let id = unmarshal_dashboard_id(after).map_err(ExecError::new)?;
        opts.after = Some(id.arg as i32);
    }
    if let Some(id) = arguments.string("id") {
        let decoded = unmarshal_dashboard_id(id).map_err(ExecError::new)?;
        // `id.isVirtualized()` (user/org dashboards) leaves `IDs` empty.
        if decoded.id_type == "custom" {
            opts.ids = vec![decoded.arg as i32];
        }
    }

    let dashboards = database.list_dashboards(&opts).map_err(ExecError::new)?;
    let next = dashboards.iter().map(|dashboard| dashboard.id).max();
    Ok(FieldValue::Object(Arc::new(DashboardConnectionObject {
        dashboards,
        next,
        database: database.clone(),
    })))
}

// ---------------------------------------------------------------------------
// Objects.
// ---------------------------------------------------------------------------

struct InsightViewConnectionObject {
    views: Vec<InsightViewData>,
    next: Option<String>,
    total_count: i32,
    database: InsightsHandle,
    override_filters: Option<InsightViewFilters>,
    override_series_options: Option<SeriesDisplayOptionsData>,
}

impl ObjectValue for InsightViewConnectionObject {
    fn type_name(&self) -> &'static str {
        "InsightViewConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: None,
                items: self
                    .views
                    .iter()
                    .map(|view| {
                        FieldValue::Object(Arc::new(InsightViewObject {
                            view: view.clone(),
                            database: self.database.clone(),
                            override_filters: self.override_filters.clone(),
                            override_series_options: self.override_series_options.clone(),
                        }))
                    })
                    .collect(),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::PageInfoObject::new(
                    self.next.is_some(),
                    self.next
                        .as_ref()
                        .map(|next| marshal_insight_view_id(next)),
                ),
            )))),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            _ => Ok(None),
        }
    }
}

struct InsightViewObject {
    view: InsightViewData,
    database: InsightsHandle,
    override_filters: Option<InsightViewFilters>,
    override_series_options: Option<SeriesDisplayOptionsData>,
}

impl InsightViewObject {
    fn first_series(&self) -> Option<&InsightSeriesData> {
        self.view.series.first()
    }

    fn effective_filters(&self) -> InsightViewFilters {
        self.override_filters
            .clone()
            .unwrap_or_else(|| self.view.filters.clone())
    }

    fn effective_series_options(&self) -> SeriesDisplayOptionsData {
        self.override_series_options.clone().unwrap_or_else(|| {
            SeriesDisplayOptionsData {
                sort_options: match (&self.view.sort_mode, &self.view.sort_direction) {
                    (Some(mode), Some(direction)) => Some(SeriesSortOptionsData {
                        mode: Some(mode.clone()),
                        direction: Some(direction.clone()),
                    }),
                    _ => None,
                },
                limit: self.view.series_limit,
                num_samples: self.view.series_num_samples,
            }
        })
    }

    fn repository_definition(
        series: &InsightSeriesData,
    ) -> Result<FieldValue, ExecError> {
        if !series.repositories.is_empty() && series.repository_criteria.is_none() {
            Ok(FieldValue::Object(Arc::new(InsightRepositoryScopeObject {
                repositories: series.repositories.clone(),
            })))
        } else if series.repositories.is_empty() {
            let all_repositories = series.repository_criteria.is_none();
            Ok(FieldValue::Object(Arc::new(RepositorySearchScopeObject {
                search: series.repository_criteria.clone().unwrap_or_default(),
                all_repositories,
            })))
        } else {
            // Go returns a union member that satisfies neither `ToInsightRepositoryScope`
            // (criteria set) nor `ToRepositorySearchScope` (repositories set); resolving
            // the union then fails.
            Err(ExecError::new("unresolvable InsightRepositoryDefinition"))
        }
    }
}

impl ObjectValue for InsightViewObject {
    fn type_name(&self) -> &'static str {
        "InsightView"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_insight_view_id(&self.view.unique_id),
            )))),
            "defaultFilters" => Ok(Some(FieldValue::Object(Arc::new(
                InsightViewFiltersObject {
                    filters: self.view.filters.clone(),
                },
            )))),
            "appliedFilters" => Ok(Some(FieldValue::Object(Arc::new(
                InsightViewFiltersObject {
                    filters: self.effective_filters(),
                },
            )))),
            // Query-engine dependent; explicitly not ported (see module doc).
            "dataSeries" => Ok(None),
            "presentation" => {
                if self.view.presentation_type == "PIE" {
                    Ok(Some(FieldValue::Object(Arc::new(
                        PieChartInsightViewPresentationObject {
                            title: self.view.title.clone(),
                            other_threshold: self.view.other_threshold.unwrap_or(0.0),
                        },
                    ))))
                } else {
                    Ok(Some(FieldValue::Object(Arc::new(
                        LineChartInsightViewPresentationObject {
                            title: self.view.title.clone(),
                            series: self.view.series.clone(),
                        },
                    ))))
                }
            }
            "dataSeriesDefinitions" => Ok(Some(FieldValue::List {
                element_type: None,
                items: self
                    .view
                    .series
                    .iter()
                    .map(|series| {
                        FieldValue::Object(Arc::new(SearchInsightDataSeriesDefinitionObject {
                            series: series.clone(),
                        }))
                    })
                    .collect(),
            })),
            "dashboardReferenceCount" => {
                let count = self
                    .database
                    .dashboard_reference_count(self.view.view_id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Int(count))))
            }
            "isFrozen" => Ok(Some(FieldValue::Json(Json::Bool(self.view.is_frozen)))),
            "defaultSeriesDisplayOptions" => {
                let options = SeriesDisplayOptionsData {
                    sort_options: match (&self.view.sort_mode, &self.view.sort_direction) {
                        (Some(mode), Some(direction)) => Some(SeriesSortOptionsData {
                            mode: Some(mode.clone()),
                            direction: Some(direction.clone()),
                        }),
                        _ => None,
                    },
                    limit: self.view.series_limit,
                    num_samples: self.view.series_num_samples,
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    SeriesDisplayOptionsObject { options },
                ))))
            }
            "appliedSeriesDisplayOptions" => Ok(Some(FieldValue::Object(Arc::new(
                SeriesDisplayOptionsObject {
                    options: self.effective_series_options(),
                },
            )))),
            "dashboards" => {
                let (user_ids, org_ids) = self
                    .database
                    .user_permission_ids(actor)
                    .map_err(ExecError::new)?;
                Ok(Some(dashboards_connection(
                    &self.database,
                    arguments,
                    user_ids,
                    org_ids,
                    Some(self.view.unique_id.clone()),
                )?))
            }
            // Depends on the data-series generators (query engine).
            "seriesCount" => Ok(None),
            "repositoryDefinition" => match self.first_series() {
                None => Err(ExecError::new("no repository definitions available")),
                Some(series) => Ok(Some(Self::repository_definition(series)?)),
            },
            "timeScope" => match self.first_series() {
                None => Err(ExecError::new("no time scope available")),
                Some(series) => Ok(Some(FieldValue::Object(Arc::new(
                    InsightIntervalTimeScopeObject {
                        unit: series.sample_interval_unit.clone(),
                        value: series.sample_interval_value,
                    },
                )))),
            },
            _ => Ok(None),
        }
    }
}

struct InsightViewFiltersObject {
    filters: InsightViewFilters,
}

impl ObjectValue for InsightViewFiltersObject {
    fn type_name(&self) -> &'static str {
        "InsightViewFilters"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "includeRepoRegex" => Ok(Some(match &self.filters.include_repo_regex {
                Some(value) => FieldValue::Json(Json::Str(value.clone())),
                None => FieldValue::Null,
            })),
            "excludeRepoRegex" => Ok(Some(match &self.filters.exclude_repo_regex {
                Some(value) => FieldValue::Json(Json::Str(value.clone())),
                None => FieldValue::Null,
            })),
            "searchContexts" => Ok(Some(match &self.filters.search_contexts {
                Some(contexts) => FieldValue::List {
                    element_type: Some("String"),
                    items: contexts
                        .iter()
                        .map(|context| FieldValue::Json(Json::Str(context.clone())))
                        .collect(),
                },
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Default)]
struct SeriesSortOptionsData {
    mode: Option<String>,
    direction: Option<String>,
}

#[derive(Clone, Default)]
struct SeriesDisplayOptionsData {
    sort_options: Option<SeriesSortOptionsData>,
    limit: Option<i32>,
    num_samples: Option<i32>,
}

struct SeriesDisplayOptionsObject {
    options: SeriesDisplayOptionsData,
}

impl ObjectValue for SeriesDisplayOptionsObject {
    fn type_name(&self) -> &'static str {
        "SeriesDisplayOptions"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "sortOptions" => Ok(Some(FieldValue::Object(Arc::new(
                SeriesSortOptionsObject {
                    options: self.options.sort_options.clone(),
                },
            )))),
            "limit" => Ok(Some(match self.options.limit {
                Some(value) => FieldValue::Json(Json::Int(i64::from(value))),
                None => FieldValue::Null,
            })),
            "numSamples" => Ok(Some(match self.options.num_samples {
                Some(value) => FieldValue::Json(Json::Int(i64::from(value))),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct SeriesSortOptionsObject {
    options: Option<SeriesSortOptionsData>,
}

impl ObjectValue for SeriesSortOptionsObject {
    fn type_name(&self) -> &'static str {
        "SeriesSortOptions"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let (mode, direction) = match &self.options {
            Some(options) => (options.mode.clone(), options.direction.clone()),
            None => (None, None),
        };
        match field {
            "mode" => Ok(Some(match mode {
                Some(value) => FieldValue::Json(Json::Str(value)),
                None => FieldValue::Null,
            })),
            "direction" => Ok(Some(match direction {
                Some(value) => FieldValue::Json(Json::Str(value)),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct LineChartInsightViewPresentationObject {
    title: String,
    series: Vec<InsightSeriesData>,
}

impl ObjectValue for LineChartInsightViewPresentationObject {
    fn type_name(&self) -> &'static str {
        "LineChartInsightViewPresentation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str(self.title.clone())))),
            "seriesPresentation" => Ok(Some(FieldValue::List {
                element_type: Some("LineChartDataSeriesPresentation"),
                items: self
                    .series
                    .iter()
                    .map(|series| {
                        FieldValue::Object(Arc::new(LineChartDataSeriesPresentationObject {
                            series: series.clone(),
                        }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

struct LineChartDataSeriesPresentationObject {
    series: InsightSeriesData,
}

impl ObjectValue for LineChartDataSeriesPresentationObject {
    fn type_name(&self) -> &'static str {
        "LineChartDataSeriesPresentation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "seriesId" => Ok(Some(FieldValue::Json(Json::Str(
                self.series.series_id.clone(),
            )))),
            "label" => Ok(Some(FieldValue::Json(Json::Str(self.series.label.clone())))),
            "color" => Ok(Some(FieldValue::Json(Json::Str(self.series.stroke.clone())))),
            _ => Ok(None),
        }
    }
}

struct PieChartInsightViewPresentationObject {
    title: String,
    other_threshold: f64,
}

impl ObjectValue for PieChartInsightViewPresentationObject {
    fn type_name(&self) -> &'static str {
        "PieChartInsightViewPresentation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str(self.title.clone())))),
            "otherThreshold" => Ok(Some(FieldValue::Json(Json::Float(self.other_threshold)))),
            _ => Ok(None),
        }
    }
}

struct SearchInsightDataSeriesDefinitionObject {
    series: InsightSeriesData,
}

impl ObjectValue for SearchInsightDataSeriesDefinitionObject {
    fn type_name(&self) -> &'static str {
        "SearchInsightDataSeriesDefinition"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "seriesId" => Ok(Some(FieldValue::Json(Json::Str(
                self.series.series_id.clone(),
            )))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.series.query.clone())))),
            "repositoryScope" => Ok(Some(FieldValue::Object(Arc::new(
                InsightRepositoryScopeObject {
                    repositories: self.series.repositories.clone(),
                },
            )))),
            "repositoryDefinition" => {
                Ok(Some(InsightViewObject::repository_definition(&self.series)?))
            }
            "timeScope" => Ok(Some(FieldValue::Object(Arc::new(
                InsightIntervalTimeScopeObject {
                    unit: self.series.sample_interval_unit.clone(),
                    value: self.series.sample_interval_value,
                },
            )))),
            "generatedFromCaptureGroups" => Ok(Some(FieldValue::Json(Json::Bool(
                self.series.generated_from_capture_groups,
            )))),
            "isCalculated" => Ok(Some(FieldValue::Json(Json::Bool(
                if self.series.generated_from_capture_groups {
                    true
                } else {
                    !self.series.just_in_time
                },
            )))),
            "groupBy" => Ok(Some(match &self.series.group_by {
                Some(value) => FieldValue::Json(Json::Str(value.to_ascii_uppercase())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct InsightRepositoryScopeObject {
    repositories: Vec<String>,
}

impl ObjectValue for InsightRepositoryScopeObject {
    fn type_name(&self) -> &'static str {
        "InsightRepositoryScope"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repositories" => Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: self
                    .repositories
                    .iter()
                    .map(|repository| FieldValue::Json(Json::Str(repository.clone())))
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

struct RepositorySearchScopeObject {
    search: String,
    all_repositories: bool,
}

impl ObjectValue for RepositorySearchScopeObject {
    fn type_name(&self) -> &'static str {
        "RepositorySearchScope"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "search" => Ok(Some(FieldValue::Json(Json::Str(self.search.clone())))),
            "allRepositories" => Ok(Some(FieldValue::Json(Json::Bool(self.all_repositories)))),
            _ => Ok(None),
        }
    }
}

struct InsightIntervalTimeScopeObject {
    unit: String,
    value: i32,
}

impl ObjectValue for InsightIntervalTimeScopeObject {
    fn type_name(&self) -> &'static str {
        "InsightIntervalTimeScope"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "unit" => Ok(Some(FieldValue::Json(Json::Str(self.unit.clone())))),
            "value" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.value))))),
            _ => Ok(None),
        }
    }
}

struct DashboardConnectionObject {
    dashboards: Vec<DashboardData>,
    next: Option<i32>,
    database: InsightsHandle,
}

impl ObjectValue for DashboardConnectionObject {
    fn type_name(&self) -> &'static str {
        "InsightsDashboardConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: None,
                items: self
                    .dashboards
                    .iter()
                    .map(|dashboard| {
                        FieldValue::Object(Arc::new(InsightsDashboardObject {
                            dashboard: dashboard.clone(),
                            database: self.database.clone(),
                        }))
                    })
                    .collect(),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::PageInfoObject::new(
                    self.next.is_some(),
                    self.next.map(|id| marshal_dashboard_id(i64::from(id))),
                ),
            )))),
            _ => Ok(None),
        }
    }
}

struct InsightsDashboardObject {
    dashboard: DashboardData,
    database: InsightsHandle,
}

impl ObjectValue for InsightsDashboardObject {
    fn type_name(&self) -> &'static str {
        "InsightsDashboard"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_dashboard_id(
                i64::from(self.dashboard.id),
            ))))),
            "title" => Ok(Some(FieldValue::Json(Json::Str(
                self.dashboard.title.clone(),
            )))),
            "views" => Ok(Some(dashboard_views_connection(
                &self.database,
                self.dashboard.id,
                arguments,
            )?)),
            "grants" => Ok(Some(FieldValue::Object(Arc::new(
                InsightsPermissionGrantsObject {
                    user_grants: self.dashboard.user_grants.clone(),
                    org_grants: self.dashboard.org_grants.clone(),
                    global_grant: self.dashboard.global_grant,
                },
            )))),
            _ => Ok(None),
        }
    }
}

/// `DashboardInsightViewConnectionResolver`.
fn dashboard_views_connection(
    database: &InsightsHandle,
    dashboard_id: i32,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let first = int_arg(arguments, "first").unwrap_or(0);
    let after = match arguments.string("after") {
        Some(after) => Some(unmarshal_string_spec(after).map_err(ExecError::new)?),
        None => None,
    };
    // `InsightsOnDashboardQueryArgs.After` is a string; `GetAllOnDashboard`
    // compares `dbiv.id > $after`, so a non-numeric cursor is a scan error in
    // Go. The TypeScript client only ever sends the numeric dashboard view id.
    let after = match after {
        Some(value) => Some(
            value
                .parse::<i32>()
                .map_err(|err| ExecError::new(err.to_string()))?,
        ),
        None => None,
    };

    let views = database
        .list_dashboard_views(dashboard_id, after, first)
        .map_err(ExecError::new)?;
    // `sort.Slice(... DashboardViewId)`. Our SQL orders by `dbiv.id` already,
    // and `group_rows` keeps that order, so no extra sort is required.
    let count = database
        .list_dashboard_views(dashboard_id, None, 0)
        .map_err(ExecError::new)?
        .len() as i32;

    Ok(FieldValue::Object(Arc::new(InsightViewConnectionObject {
        views,
        next: None,
        total_count: count,
        database: database.clone(),
        override_filters: None,
        override_series_options: None,
    })))
}

struct InsightsPermissionGrantsObject {
    user_grants: Vec<i64>,
    org_grants: Vec<i64>,
    global_grant: bool,
}

impl ObjectValue for InsightsPermissionGrantsObject {
    fn type_name(&self) -> &'static str {
        "InsightsPermissionGrants"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "users" => Ok(Some(FieldValue::List {
                element_type: Some("ID"),
                items: self
                    .user_grants
                    .iter()
                    .map(|id| FieldValue::Json(Json::Str(marshal_user_id(*id))))
                    .collect(),
            })),
            "organizations" => Ok(Some(FieldValue::List {
                element_type: Some("ID"),
                items: self
                    .org_grants
                    .iter()
                    .map(|id| FieldValue::Json(Json::Str(marshal_org_id(*id))))
                    .collect(),
            })),
            "global" => Ok(Some(FieldValue::Json(Json::Bool(self.global_grant)))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Write surface (M9.30b).
// ---------------------------------------------------------------------------

/// `graphqlbackend.LineChartSearchInsightDataSeriesInput` after the
/// view-level scope fallback has been applied by the caller.
#[derive(Debug, Clone, Default)]
pub struct SeriesMutationInput {
    pub series_id: Option<String>,
    pub query: String,
    /// `emptyIfNil(options.label)`.
    pub label: String,
    /// `emptyIfNil(options.lineColor)`.
    pub stroke: String,
    pub repository_scope: Option<RepositoryScopeInput>,
    pub time_scope: Option<TimeScopeInput>,
    pub generated_from_capture_groups: Option<bool>,
    /// Raw `GroupByField` enum value; stored lowercase (`lowercaseGroupBy`).
    pub group_by: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RepositoryScopeInput {
    pub repositories: Vec<String>,
    pub repository_criteria: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TimeScopeInput {
    pub unit: String,
    pub value: i32,
}

impl SeriesMutationInput {
    /// `isValidSeriesInput` (`insight_view_resolvers.go:1608`), including the
    /// verbatim `critieria` typo.
    pub fn validate(&self) -> Result<(), String> {
        let Some(scope) = &self.repository_scope else {
            return Err("a repository scope is required".to_owned());
        };
        if self.time_scope.is_none() {
            return Err("a time scope is required".to_owned());
        }
        let repo_criteria_specified = scope.repository_criteria.is_some();
        let repo_list_specified = !scope.repositories.is_empty();
        if repo_list_specified && repo_criteria_specified {
            return Err(
                "series can not specify both a repository list and repository critieria".to_owned(),
            );
        }
        if !repo_list_specified && self.group_by.is_some() {
            return Err(
                "group by series require a list of repositories to be specified.".to_owned(),
            );
        }
        if let Some(criteria) = &scope.repository_criteria {
            let plan = sg_insights_query::parse_query(criteria, "literal")
                .map_err(|err| format!("ParseQuery: {err}"))?;
            let (message, valid) = sg_insights_query::is_valid_scope_query(&plan);
            if !valid {
                return Err(message);
            }
        }
        Ok(())
    }

    /// `existingSeriesHasChanged` (`insight_view_resolvers.go:743`) against a
    /// persisted series row.
    pub fn has_changed(&self, existing: &InsightSeriesData) -> bool {
        let scope = self.repository_scope.as_ref().expect("validated scope");
        if self.query != existing.query {
            return true;
        }
        let time = self.time_scope.as_ref().expect("validated time scope");
        if time.unit != existing.sample_interval_unit {
            return true;
        }
        if time.value != existing.sample_interval_value {
            return true;
        }
        if scope.repositories.len() != existing.repositories.len() {
            return true;
        }
        let mut new_repos = scope.repositories.clone();
        new_repos.sort();
        let mut old_repos = existing.repositories.clone();
        old_repos.sort();
        if new_repos != old_repos {
            return true;
        }
        if scope.repository_criteria.is_some() != existing.repository_criteria.is_some() {
            return true;
        }
        if let (Some(new_criteria), Some(old_criteria)) =
            (&scope.repository_criteria, &existing.repository_criteria)
        {
            if new_criteria != old_criteria {
                return true;
            }
        }
        self.group_by.clone().unwrap_or_default()
            != existing.group_by.clone().unwrap_or_default()
    }
}

/// `LineChartSearchInsightInput` / `UpdateLineChartSearchInsightInput`.
#[derive(Debug, Clone, Default)]
pub struct LineChartMutationInput {
    /// `emptyIfNil(options.title)` (create) / `presentationOptions.title`
    /// (update).
    pub title: String,
    /// Flattened `viewControls.filters` (`filtersFromInput`).
    pub filters: InsightViewFilters,
    pub sort_mode: Option<String>,
    pub sort_direction: Option<String>,
    pub limit: Option<i32>,
    pub num_samples: Option<i32>,
    /// `Input.Dashboards` (create only); `Some` iff the argument was provided.
    pub dashboards: Option<Vec<i64>>,
    pub series: Vec<SeriesMutationInput>,
}

#[derive(Debug, Clone, Default)]
pub struct PieChartCreateInput {
    pub title: String,
    pub other_threshold: f64,
    pub query: String,
    pub repositories: Vec<String>,
    pub dashboards: Option<Vec<i64>>,
}

#[derive(Debug, Clone, Default)]
pub struct PieChartUpdateInput {
    pub title: String,
    pub other_threshold: f64,
    pub query: String,
    pub repositories: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SaveAsNewViewInput {
    pub unique_id: String,
    /// `args.Input.ViewControls != nil`; `None` leaves filters empty.
    pub filters: Option<InsightViewFilters>,
    pub title: String,
    pub dashboard: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsightSeriesMetadataData {
    pub series_id: String,
    pub query: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsightSeriesQueryStatusData {
    pub series_id: String,
    pub query: String,
    pub enabled: bool,
    pub errored: i32,
    pub completed: i32,
    pub processing: i32,
    pub failed: i32,
    pub queued: i32,
}

/// The write half of the database seam. Implementations mirror the Go
/// resolver transaction boundaries documented in
/// `dev/rs/docs/overviews/m9_30b_insights_view_mutations_overview.md`.
pub trait InsightsWriteDatabase: Send + Sync {
    /// `licensing.Check(FeatureCodeInsights) == nil` (`false` reproduces
    /// Limited Access Mode).
    fn code_insights_licensed(&self) -> bool {
        true
    }

    /// `auth.CheckUserIsSiteAdmin`; `Err` carries the verbatim message.
    fn actor_is_site_admin(&self, actor: Actor) -> Result<bool, String>;

    /// `validateRepositoryList`: every name exists in the repo store.
    fn repositories_exist(&self, names: &[String]) -> Result<bool, String>;

    fn create_line_chart(
        &self,
        actor: Actor,
        input: &LineChartMutationInput,
    ) -> Result<String, String>;

    fn update_line_chart(
        &self,
        actor: Actor,
        unique_id: &str,
        input: &LineChartMutationInput,
    ) -> Result<String, String>;

    fn create_pie_chart(
        &self,
        actor: Actor,
        input: &PieChartCreateInput,
    ) -> Result<String, String>;

    fn update_pie_chart(
        &self,
        actor: Actor,
        unique_id: &str,
        input: &PieChartUpdateInput,
    ) -> Result<String, String>;

    /// No transaction (`DeleteInsightView` calls the non-tx stores directly).
    fn delete_insight_view(&self, actor: Actor, unique_id: &str) -> Result<(), String>;

    fn save_insight_as_new_view(
        &self,
        actor: Actor,
        input: &SaveAsNewViewInput,
    ) -> Result<String, String>;

    fn update_insight_series(
        &self,
        actor: Actor,
        series_id: &str,
        enabled: Option<bool>,
    ) -> Result<InsightSeriesMetadataData, String>;

    fn insight_series_query_status(
        &self,
        actor: Actor,
    ) -> Result<Vec<InsightSeriesQueryStatusData>, String>;
}

// ----- Argument parsing ----------------------------------------------------

fn object_bool(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<bool> {
    match object.get(name) {
        Some(InputValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn object_float(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<f64> {
    match object.get(name) {
        Some(InputValue::Float(value)) => Some(*value),
        Some(InputValue::Int(value)) => Some(*value as f64),
        _ => None,
    }
}

fn object_value<'a>(
    object: &'a std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<&'a std::collections::BTreeMap<String, InputValue>> {
    match object.get(name) {
        Some(InputValue::Object(value)) => Some(value),
        _ => None,
    }
}

fn object_string_list(
    object: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<Vec<String>> {
    match object.get(name) {
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
        ),
        Some(InputValue::Null) | None => None,
        _ => None,
    }
}

fn parse_repository_scope(
    object: &std::collections::BTreeMap<String, InputValue>,
) -> RepositoryScopeInput {
    RepositoryScopeInput {
        repositories: object_string_list(object, "repositories").unwrap_or_default(),
        repository_criteria: object_str(object, "repositoryCriteria"),
    }
}

fn parse_time_scope(
    object: &std::collections::BTreeMap<String, InputValue>,
) -> TimeScopeInput {
    let Some(step) = object_value(object, "stepInterval") else {
        return TimeScopeInput::default();
    };
    TimeScopeInput {
        unit: object_str(step, "unit").unwrap_or_default(),
        value: object_int(step, "value").unwrap_or(0),
    }
}

fn parse_series(
    object: &std::collections::BTreeMap<String, InputValue>,
    top_repository_scope: Option<&std::collections::BTreeMap<String, InputValue>>,
    top_time_scope: Option<&std::collections::BTreeMap<String, InputValue>>,
) -> Result<SeriesMutationInput, String> {
    let options = object_value(object, "options");
    let mut repository_scope = object_value(object, "repositoryScope").map(parse_repository_scope);
    let mut time_scope = object_value(object, "timeScope").map(parse_time_scope);
    if repository_scope.is_none() {
        repository_scope = top_repository_scope.map(parse_repository_scope);
    }
    if time_scope.is_none() {
        time_scope = top_time_scope.map(parse_time_scope);
    }
    Ok(SeriesMutationInput {
        series_id: match object.get("seriesId") {
            Some(InputValue::Str(value)) => Some(value.clone()),
            _ => None,
        },
        query: object_str(object, "query").unwrap_or_default(),
        label: options.and_then(|options| object_str(options, "label")).unwrap_or_default(),
        stroke: options
            .and_then(|options| object_str(options, "lineColor"))
            .unwrap_or_default(),
        repository_scope,
        time_scope,
        generated_from_capture_groups: object_bool(object, "generatedFromCaptureGroups"),
        group_by: match object.get("groupBy") {
            Some(InputValue::Enum(value)) => Some(value.clone()),
            Some(InputValue::Str(value)) => Some(value.clone()),
            _ => None,
        },
    })
}

/// `dataSeries: [LineChartSearchInsightDataSeriesInput!]!`.
fn parse_series_list(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> Result<Vec<SeriesMutationInput>, String> {
    let top_repository_scope = object_value(input, "repositoryScope");
    let top_time_scope = object_value(input, "timeScope");
    let Some(InputValue::List(items)) = input.get("dataSeries") else {
        return Ok(Vec::new());
    };
    let mut series = Vec::with_capacity(items.len());
    for item in items {
        let InputValue::Object(object) = item else {
            return Err("invalid data series".to_owned());
        };
        series.push(parse_series(object, top_repository_scope, top_time_scope)?);
    }
    Ok(series)
}

fn parse_filters(
    input: &std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> InsightViewFilters {
    let Some(filters) = object_value(input, name) else {
        return InsightViewFilters::default();
    };
    InsightViewFilters {
        include_repo_regex: object_str(filters, "includeRepoRegex"),
        exclude_repo_regex: object_str(filters, "excludeRepoRegex"),
        search_contexts: object_string_list(filters, "searchContexts"),
    }
}

/// `ViewControls.filters` when `ViewControls` is present.
fn parse_view_controls_filters(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> InsightViewFilters {
    match object_value(input, "viewControls") {
        Some(controls) => parse_filters(controls, "filters"),
        None => InsightViewFilters::default(),
    }
}

/// `ViewControls.seriesDisplayOptions`.
fn parse_series_display_options(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> (Option<String>, Option<String>, Option<i32>, Option<i32>) {
    let Some(controls) = object_value(input, "viewControls") else {
        return (None, None, None, None);
    };
    let Some(options) = object_value(controls, "seriesDisplayOptions") else {
        return (None, None, None, None);
    };
    let sort = object_value(options, "sortOptions");
    (
        sort.and_then(|sort| object_str(sort, "mode")),
        sort.and_then(|sort| object_str(sort, "direction")),
        object_int(options, "limit"),
        object_int(options, "numSamples"),
    )
}

fn parse_dashboards(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> Result<Option<Vec<i64>>, String> {
    match input.get("dashboards") {
        None | Some(InputValue::Null) => Ok(None),
        Some(InputValue::List(items)) => {
            let mut ids = Vec::with_capacity(items.len());
            for item in items {
                let Some(id) = item.as_str() else {
                    return Err("invalid graphql.ID".to_owned());
                };
                let id = unmarshal_dashboard_id(id)
                    .map_err(|err| format!("unmarshalDashboardID, id:{err}"))?;
                ids.push(id.arg);
            }
            Ok(Some(ids))
        }
        _ => Err("invalid graphql.ID".to_owned()),
    }
}

fn input_object_argument(
    arguments: &Arguments,
) -> Result<&std::collections::BTreeMap<String, InputValue>, ExecError> {
    input_object(arguments, "input")
        .ok_or_else(|| ExecError::new("missing input argument"))
}

fn parse_line_chart(
    input: &std::collections::BTreeMap<String, InputValue>,
    update: bool,
) -> Result<LineChartMutationInput, ExecError> {
    let series = parse_series_list(input).map_err(ExecError::new)?;
    let options = object_value(input, "options")
        .or_else(|| object_value(input, "presentationOptions"));
    let title = options
        .and_then(|options| object_str(options, "title"))
        .unwrap_or_default();
    let (sort_mode, sort_direction, limit, num_samples) =
        parse_series_display_options(input);
    let filters = parse_view_controls_filters(input);
    let dashboards = if update {
        None
    } else {
        parse_dashboards(input).map_err(ExecError::new)?
    };
    Ok(LineChartMutationInput {
        title,
        filters,
        sort_mode,
        sort_direction,
        limit,
        num_samples,
        dashboards,
        series,
    })
}

// ----- Mutation dispatch ---------------------------------------------------

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createLineChartSearchInsight"
            | "createPieChartSearchInsight"
            | "updateLineChartSearchInsight"
            | "updatePieChartSearchInsight"
            | "deleteInsightView"
            | "saveInsightAsNewView"
            | "updateInsightSeries"
    )
}

pub fn mutation_field(
    database: &InsightsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createLineChartSearchInsight" => {
            let input = input_object_argument(arguments)?;
            let input = parse_line_chart(input, false)?;
            if input.series.is_empty() {
                return Err(ExecError::new(
                    "At least one data series is required to create an insight view",
                ));
            }
            for series in &input.series {
                series.validate().map_err(ExecError::new)?;
                if let Some(scope) = &series.repository_scope {
                    validate_repository_list(database, &scope.repositories)?;
                }
            }
            let unique_id = database
                .create_line_chart(actor, &input)
                .map_err(ExecError::new)?;
            Ok(Some(payload_object(database.clone(), unique_id)))
        }
        "updateLineChartSearchInsight" => {
            let input = input_object_argument(arguments)?;
            let input = parse_line_chart(input, true)?;
            if input.series.is_empty() {
                return Err(ExecError::new(
                    "At least one data series is required to update an insight view",
                ));
            }
            for series in &input.series {
                series.validate().map_err(ExecError::new)?;
                if let Some(scope) = &series.repository_scope {
                    validate_repository_list(database, &scope.repositories)?;
                }
            }
            let id = arguments
                .string("id")
                .ok_or_else(|| ExecError::new("missing id argument"))?;
            let unique_id = unmarshal_string_spec(id).map_err(|err| {
                ExecError::new(format!("error unmarshalling the insight view id: {err}"))
            })?;
            let unique_id = database
                .update_line_chart(actor, &unique_id, &input)
                .map_err(ExecError::new)?;
            Ok(Some(payload_object(database.clone(), unique_id)))
        }
        "createPieChartSearchInsight" => {
            let input = input_object_argument(arguments)?;
            let presentation = object_value(input, "presentationOptions")
                .ok_or_else(|| ExecError::new("missing presentationOptions"))?;
            let repository_scope = object_value(input, "repositoryScope")
                .ok_or_else(|| ExecError::new("missing repositoryScope"))?;
            let input = PieChartCreateInput {
                title: object_str(presentation, "title").unwrap_or_default(),
                other_threshold: object_float(presentation, "otherThreshold")
                    .unwrap_or(0.0),
                query: object_str(input, "query").unwrap_or_default(),
                repositories: object_string_list(repository_scope, "repositories")
                    .unwrap_or_default(),
                dashboards: parse_dashboards(input).map_err(ExecError::new)?,
            };
            let unique_id = database
                .create_pie_chart(actor, &input)
                .map_err(ExecError::new)?;
            Ok(Some(payload_object(database.clone(), unique_id)))
        }
        "updatePieChartSearchInsight" => {
            let input = input_object_argument(arguments)?;
            let presentation = object_value(input, "presentationOptions")
                .ok_or_else(|| ExecError::new("missing presentationOptions"))?;
            let repository_scope = object_value(input, "repositoryScope")
                .ok_or_else(|| ExecError::new("missing repositoryScope"))?;
            let input = PieChartUpdateInput {
                title: object_str(presentation, "title").unwrap_or_default(),
                other_threshold: object_float(presentation, "otherThreshold")
                    .unwrap_or(0.0),
                query: object_str(input, "query").unwrap_or_default(),
                repositories: object_string_list(repository_scope, "repositories")
                    .unwrap_or_default(),
            };
            let id = arguments
                .string("id")
                .ok_or_else(|| ExecError::new("missing id argument"))?;
            let unique_id = unmarshal_string_spec(id).map_err(|err| {
                ExecError::new(format!("error unmarshalling the insight view id: {err}"))
            })?;
            let unique_id = database
                .update_pie_chart(actor, &unique_id, &input)
                .map_err(ExecError::new)?;
            Ok(Some(payload_object(database.clone(), unique_id)))
        }
        "deleteInsightView" => {
            let id = arguments
                .string("id")
                .ok_or_else(|| ExecError::new("missing id argument"))?;
            let unique_id = unmarshal_string_spec(id).map_err(|err| {
                ExecError::new(format!("error unmarshalling the insight view id: {err}"))
            })?;
            database
                .delete_insight_view(actor, &unique_id)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            ))))
        }
        "saveInsightAsNewView" => {
            let input = input_object_argument(arguments)?;
            let id = object_str(input, "insightViewId")
                .ok_or_else(|| ExecError::new("missing insightViewId"))?;
            let unique_id = unmarshal_string_spec(&id).map_err(|err| {
                ExecError::new(format!("error unmarshalling the insight view id: {err}"))
            })?;
            let options = object_value(input, "options");
            let filters = match object_value(input, "viewControls") {
                Some(controls) => Some(parse_filters(controls, "filters")),
                None => None,
            };
            let dashboard = match input.get("dashboard") {
                Some(InputValue::Str(value)) => Some(
                    unmarshal_dashboard_id(value)
                        .map_err(|err| {
                            ExecError::new(format!("unmarshalDashboardID, id:{err}"))
                        })?
                        .arg,
                ),
                _ => None,
            };
            let input = SaveAsNewViewInput {
                unique_id,
                filters,
                title: options
                    .and_then(|options| object_str(options, "title"))
                    .unwrap_or_default(),
                dashboard,
            };
            let unique_id = database
                .save_insight_as_new_view(actor, &input)
                .map_err(ExecError::new)?;
            Ok(Some(payload_object(database.clone(), unique_id)))
        }
        "updateInsightSeries" => {
            let input = input_object_argument(arguments)?;
            let series_id = object_str(input, "seriesId")
                .ok_or_else(|| ExecError::new("missing seriesId"))?;
            let enabled = object_bool(input, "enabled");
            let series = database
                .update_insight_series(actor, &series_id, enabled)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                InsightSeriesMetadataPayloadObject { series },
            ))))
        }
        _ => Ok(None),
    }
}

fn validate_repository_list(
    database: &InsightsHandle,
    repositories: &[String],
) -> Result<(), ExecError> {
    if repositories.is_empty() {
        return Ok(());
    }
    let all_found = database
        .repositories_exist(repositories)
        .map_err(ExecError::new)?;
    if !all_found {
        return Err(ExecError::new("repositories not found"));
    }
    Ok(())
}

fn payload_object(database: InsightsHandle, unique_id: String) -> FieldValue {
    FieldValue::Object(Arc::new(InsightViewPayloadObject {
        database,
        unique_id,
    }))
}

struct InsightViewPayloadObject {
    database: InsightsHandle,
    unique_id: String,
}

impl ObjectValue for InsightViewPayloadObject {
    fn type_name(&self) -> &'static str {
        "InsightViewPayload"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `insightPayloadResolver.View`: `GetAllMapped` restricted to the
            // view's unique id and the actor's grants.
            "view" => {
                let (user_ids, org_ids) = self
                    .database
                    .user_permission_ids(actor)
                    .map_err(ExecError::new)?;
                let views = self
                    .database
                    .list_views(&InsightQueryOptions {
                        unique_id: Some(self.unique_id.clone()),
                        user_ids,
                        org_ids,
                        ..Default::default()
                    })
                    .map_err(ExecError::new)?;
                if std::env::var("M930B_TRACE").as_deref() == Ok("1") {
                    eprintln!("M930B_TRACE payload views={}", views.len());
                };
                let Some(view) = views.into_iter().next() else {
                    return Err(ExecError::new("insight not found"));
                };
                Ok(Some(FieldValue::Object(Arc::new(InsightViewObject {
                    view,
                    database: self.database.clone(),
                    override_filters: None,
                    override_series_options: None,
                }))))
            }
            _ => Ok(None),
        }
    }
}

struct InsightSeriesMetadataPayloadObject {
    series: InsightSeriesMetadataData,
}

impl ObjectValue for InsightSeriesMetadataPayloadObject {
    fn type_name(&self) -> &'static str {
        "InsightSeriesMetadataPayload"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "series" => Ok(Some(FieldValue::Object(Arc::new(
                InsightSeriesMetadataObject {
                    series: self.series.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

struct InsightSeriesMetadataObject {
    series: InsightSeriesMetadataData,
}

impl ObjectValue for InsightSeriesMetadataObject {
    fn type_name(&self) -> &'static str {
        "InsightSeriesMetadata"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "seriesId" => Ok(Some(FieldValue::Json(Json::Str(
                self.series.series_id.clone(),
            )))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.series.query.clone())))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.series.enabled)))),
            _ => Ok(None),
        }
    }
}

/// `Query.insightSeriesQueryStatus` (`admin_resolver.go:56`).
pub fn insight_series_query_status(
    database: &InsightsHandle,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let statuses = database
        .insight_series_query_status(actor)
        .map_err(ExecError::new)?;
    Ok(FieldValue::List {
        element_type: Some("InsightSeriesQueryStatus"),
        items: statuses
            .into_iter()
            .map(|status| {
                FieldValue::Object(Arc::new(InsightSeriesQueryStatusObject { status }))
            })
            .collect(),
    })
}

struct InsightSeriesQueryStatusObject {
    status: InsightSeriesQueryStatusData,
}

impl ObjectValue for InsightSeriesQueryStatusObject {
    fn type_name(&self) -> &'static str {
        "InsightSeriesQueryStatus"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "seriesId" => Ok(Some(FieldValue::Json(Json::Str(
                self.status.series_id.clone(),
            )))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.status.query.clone())))),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(self.status.enabled)))),
            "errored" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.status.errored,
            ))))),
            "completed" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.status.completed,
            ))))),
            "processing" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.status.processing,
            ))))),
            "failed" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.status.failed,
            ))))),
            "queued" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.status.queued,
            ))))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;

    #[test]
    fn insights_sdl_assembles_opt_in() {
        // The default OSS schema must not contain the insights document; the
        // opt-in assembly appends it.
        let oss = sg_graphql::schema::assemble_oss().expect("oss schema");
        assert!(!oss.types.contains_key("InsightView"));
        let schema = sg_graphql::schema::assemble(&[
            oss.schema_string.as_str(),
            sg_graphql::schema::INSIGHTS_SCHEMA,
        ])
        .expect("insights schema");
        assert!(schema.types.contains_key("InsightView"));
        assert!(schema.types.contains_key("InsightsDashboard"));
    }

    #[test]
    fn relay_ids_match_go_json_encoding() {
        assert_eq!(
            super::marshal_dashboard_id(7),
            base64::engine::general_purpose::URL_SAFE.encode(
                r#"dashboard:{"IdType":"custom","Arg":7}"#
            )
        );
        assert_eq!(
            super::marshal_user_id(3),
            base64::engine::general_purpose::URL_SAFE.encode("User:3")
        );
    }
}
