#![forbid(unsafe_code)]

//! Production adapter for [`crate::insights_graphql::InsightsDatabase`]
//! (M9.29b). All SQL is read-only and targets the existing code-insights
//! tables (`insight_view`, `insight_series`, `insight_view_series`,
//! `insight_view_grants`, `dashboard`, `dashboard_grants`,
//! `dashboard_insight_view`). No table/column/Redis key is added.
//!
//! The queries mirror `internal/insights/store/insight_store.go`'s
//! `GetAll`/`GetAllOnDashboard` and `dashboard_store.go`'s `GetDashboards`:
//! the same visibility predicates, filters, ordering and grouping. The
//! query-engine reads (timeseries points / query status) are explicitly out
//! of scope; see `dev/rs/docs/overviews/m9_29b_insights_overview.md`.

use sqlx::{PgPool, Row};

use sg_graphql::exec::Actor;

use crate::codeintel_uploads_graphql_store::block_on;
use crate::insights_graphql::{
    DashboardData, DashboardQueryOptions, InsightQueryOptions, InsightSeriesData, InsightViewData,
    InsightViewFilters, InsightsDatabase,
};

#[derive(Clone)]
pub struct PgInsightsStore {
    /// The code-insights database (`sourcegraph-codeinsights`).
    pool: PgPool,
    /// The frontend database, used only for `getUserPermissions`' org lookup.
    /// `None` reproduces an anonymous/internal actor (no user/org grants).
    frontend_pool: Option<PgPool>,
}

impl std::fmt::Debug for PgInsightsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgInsightsStore")
            .field("frontend_pool", &self.frontend_pool.is_some())
            .finish_non_exhaustive()
    }
}

impl PgInsightsStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            frontend_pool: None,
        }
    }

    pub fn with_frontend_pool(mut self, pool: PgPool) -> Self {
        self.frontend_pool = Some(pool);
        self
    }
}

/// Column projection shared by `GetAll` and `GetAllOnDashboard`
/// (`getInsightsWithSeriesSql` / `getInsightsByDashboardSql`).
const SERIES_SELECT: &str = r#"
SELECT iv.id AS view_id,
       iv.unique_id,
       iv.title,
       iv.description,
       ivs.label,
       ivs.stroke,
       i.series_id,
       i.query,
       i.repositories,
       i.sample_interval_unit::text AS sample_interval_unit,
       i.sample_interval_value,
       iv.default_filter_include_repo_regex AS include_repo_regex,
       iv.default_filter_exclude_repo_regex AS exclude_repo_regex,
       iv.default_filter_search_contexts AS search_contexts,
       iv.other_threshold,
       iv.presentation_type::text AS presentation_type,
       i.generated_from_capture_groups,
       i.just_in_time,
       i.generation_method,
       iv.is_frozen,
       iv.series_sort_mode::text AS series_sort_mode,
       iv.series_sort_direction::text AS series_sort_direction,
       iv.series_limit,
       iv.series_num_samples,
       i.group_by,
       i.repository_criteria
"#;

struct SeriesRow {
    view_id: i32,
    unique_id: String,
    title: String,
    description: String,
    label: String,
    stroke: String,
    series_id: String,
    query: String,
    repositories: Option<Vec<String>>,
    sample_interval_unit: String,
    sample_interval_value: i32,
    include_repo_regex: Option<String>,
    exclude_repo_regex: Option<String>,
    search_contexts: Option<Vec<String>>,
    other_threshold: Option<f64>,
    presentation_type: String,
    generated_from_capture_groups: bool,
    just_in_time: bool,
    generation_method: String,
    is_frozen: bool,
    sort_mode: Option<String>,
    sort_direction: Option<String>,
    series_limit: Option<i32>,
    series_num_samples: Option<i32>,
    group_by: Option<String>,
    repository_criteria: Option<String>,
}

impl SeriesRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            view_id: row.try_get("view_id")?,
            unique_id: row.try_get("unique_id")?,
            title: row.try_get("title")?,
            description: row.try_get("description")?,
            label: row.try_get("label")?,
            stroke: row.try_get("stroke")?,
            series_id: row.try_get("series_id")?,
            query: row.try_get("query")?,
            repositories: row.try_get("repositories")?,
            sample_interval_unit: row.try_get("sample_interval_unit")?,
            sample_interval_value: row.try_get("sample_interval_value")?,
            include_repo_regex: row.try_get("include_repo_regex")?,
            exclude_repo_regex: row.try_get("exclude_repo_regex")?,
            search_contexts: row.try_get("search_contexts")?,
            other_threshold: row.try_get("other_threshold")?,
            presentation_type: row.try_get("presentation_type")?,
            generated_from_capture_groups: row.try_get("generated_from_capture_groups")?,
            just_in_time: row.try_get("just_in_time")?,
            generation_method: row.try_get("generation_method")?,
            is_frozen: row.try_get("is_frozen")?,
            sort_mode: row.try_get("series_sort_mode")?,
            sort_direction: row.try_get("series_sort_direction")?,
            series_limit: row.try_get("series_limit")?,
            series_num_samples: row.try_get("series_num_samples")?,
            group_by: row.try_get("group_by")?,
            repository_criteria: row.try_get("repository_criteria")?,
        })
    }

    fn series(&self) -> InsightSeriesData {
        InsightSeriesData {
            series_id: self.series_id.clone(),
            query: self.query.clone(),
            label: self.label.clone(),
            stroke: self.stroke.clone(),
            repositories: self.repositories.clone().unwrap_or_default(),
            sample_interval_unit: self.sample_interval_unit.clone(),
            sample_interval_value: self.sample_interval_value,
            generated_from_capture_groups: self.generated_from_capture_groups,
            just_in_time: self.just_in_time,
            generation_method: self.generation_method.clone(),
            group_by: self.group_by.clone(),
            repository_criteria: self.repository_criteria.clone(),
        }
    }

    fn into_view(self) -> InsightViewData {
        let series = self.series();
        InsightViewData {
            view_id: self.view_id,
            unique_id: self.unique_id,
            title: self.title,
            description: self.description,
            filters: InsightViewFilters {
                include_repo_regex: self.include_repo_regex,
                exclude_repo_regex: self.exclude_repo_regex,
                search_contexts: self.search_contexts,
            },
            other_threshold: self.other_threshold,
            presentation_type: self.presentation_type,
            is_frozen: self.is_frozen,
            sort_mode: self.sort_mode,
            sort_direction: self.sort_direction,
            series_limit: self.series_limit,
            series_num_samples: self.series_num_samples,
            series: vec![series],
        }
    }
}

fn group_rows(rows: Vec<SeriesRow>) -> Vec<InsightViewData> {
    let mut views: Vec<InsightViewData> = Vec::new();
    for row in rows {
        match views.last_mut() {
            Some(view) if view.unique_id == row.unique_id => view.series.push(row.series()),
            _ => views.push(row.into_view()),
        }
    }
    views
}

impl InsightsDatabase for PgInsightsStore {
    fn user_permission_ids(&self, actor: Actor) -> Result<(Vec<i32>, Vec<i32>), String> {
        // `getUserPermissions`: an actor with UID 0 (anonymous or internal)
        // gets neither user nor org grants, leaving only `global is true`.
        let Some(uid) = actor.uid() else {
            return Ok((Vec::new(), Vec::new()));
        };
        let Some(frontend) = &self.frontend_pool else {
            return Ok((Vec::new(), Vec::new()));
        };
        let org_ids = block_on(async {
            sqlx::query_scalar::<_, i32>("SELECT org_id FROM org_members WHERE user_id = $1")
                .bind(uid)
                .fetch_all(frontend)
                .await
        })
        .map_err(|err| err.to_string())?;
        Ok((vec![uid], org_ids))
    }

    fn list_views(&self, opts: &InsightQueryOptions) -> Result<Vec<InsightViewData>, String> {
        block_on(async {
            let limit = if opts.limit > 0 {
                Some(i64::from(opts.limit))
            } else {
                None
            };
            let unique_ids: Vec<String> = sqlx::query_scalar::<_, String>(
                r#"
WITH visible_dashboards AS (
    SELECT dashboard_id FROM dashboard_grants
    WHERE org_id = ANY($1) OR user_id = ANY($2) OR global IS TRUE
),
visible_views AS (
    SELECT insight_view_id FROM insight_view_grants
    WHERE org_id = ANY($1) OR user_id = ANY($2) OR global IS TRUE
)
SELECT DISTINCT iv.unique_id
FROM insight_view iv
JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
JOIN insight_series i ON ivs.insight_series_id = i.id
WHERE (iv.id IN (
           SELECT div.insight_view_id
           FROM dashboard_insight_view div
           JOIN dashboard db ON db.id = div.dashboard_id
           WHERE db.deleted_at IS NULL
             AND db.id IN (SELECT dashboard_id FROM visible_dashboards))
       OR iv.id IN (SELECT insight_view_id FROM visible_views))
  AND i.deleted_at IS NULL
  AND ($3::text IS NULL OR iv.unique_id > $3)
  AND ($4::bool IS NULL OR iv.is_frozen = $4)
  AND ($5::text IS NULL OR iv.title ILIKE '%' || $5 || '%' OR ivs.label ILIKE '%' || $5 || '%')
  AND ($6::text IS NULL OR iv.unique_id = $6)
  AND (COALESCE(array_length($7::text[], 1), 0) = 0 OR iv.unique_id <> ALL($7))
  AND ($8::int IS NULL OR iv.id IN (
           SELECT insight_view_id FROM dashboard_insight_view WHERE dashboard_id = $8))
ORDER BY iv.unique_id
LIMIT $9::int
"#,
            )
            .bind(&opts.user_ids)
            .bind(&opts.org_ids)
            .bind(&opts.after)
            .bind(opts.is_frozen)
            .bind(&opts.find)
            .bind(&opts.unique_id)
            .bind(&opts.exclude_ids)
            .bind(opts.dashboard_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.to_string())?;

            if unique_ids.is_empty() {
                return Ok(Vec::new());
            }

            let query = format!(
                "{SERIES_SELECT}\nFROM insight_view iv\nJOIN insight_view_series ivs ON iv.id = ivs.insight_view_id\nJOIN insight_series i ON ivs.insight_series_id = i.id\nWHERE iv.unique_id = ANY($1)\nORDER BY iv.unique_id"
            );
            let rows = sqlx::query(&query)
                .bind(&unique_ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|err| err.to_string())?;
            let rows = rows
                .iter()
                .map(SeriesRow::from_row)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| err.to_string())?;
            Ok(group_rows(rows))
        })
    }

    fn list_dashboard_views(
        &self,
        dashboard_id: i32,
        after: Option<i32>,
        limit: i32,
    ) -> Result<Vec<InsightViewData>, String> {
        block_on(async {
            let limit = if limit > 0 { Some(limit) } else { None };
            let query = format!(
                "{SERIES_SELECT}\nFROM dashboard_insight_view dbiv\nJOIN insight_view iv ON iv.id = dbiv.insight_view_id\nJOIN insight_view_series ivs ON iv.id = ivs.insight_view_id\nJOIN insight_series i ON ivs.insight_series_id = i.id\nWHERE dbiv.dashboard_id = $1\n  AND ($2::int IS NULL OR dbiv.id > $2)\nORDER BY dbiv.id\nLIMIT $3::int"
            );
            let rows = sqlx::query(&query)
                .bind(dashboard_id)
                .bind(after)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|err| err.to_string())?;
            let rows = rows
                .iter()
                .map(SeriesRow::from_row)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| err.to_string())?;
            Ok(group_rows(rows))
        })
    }

    fn list_dashboards(&self, opts: &DashboardQueryOptions) -> Result<Vec<DashboardData>, String> {
        block_on(async {
            let limit = if opts.limit > 0 {
                Some(i64::from(opts.limit))
            } else {
                None
            };
            let ids: Vec<i32> = opts.ids.clone();
            let rows = sqlx::query(
                r#"
SELECT db.id,
       db.title,
       t.uuid_array AS insight_view_unique_ids,
       ARRAY_REMOVE(ARRAY_AGG(dg.user_id), NULL)::bigint[] AS granted_users,
       ARRAY_REMOVE(ARRAY_AGG(dg.org_id), NULL)::bigint[] AS granted_orgs,
       BOOL_OR(dg.global IS TRUE) AS granted_global
FROM dashboard db
JOIN dashboard_grants dg ON db.id = dg.dashboard_id
LEFT JOIN (
    SELECT ARRAY_AGG(iv.unique_id) AS uuid_array, div.dashboard_id
    FROM insight_view iv
    JOIN dashboard_insight_view div ON iv.id = div.insight_view_id
    GROUP BY div.dashboard_id
) t ON t.dashboard_id = db.id
WHERE db.deleted_at IS NULL
  AND ($1::int IS NULL OR db.id > $1)
  AND (COALESCE(array_length($2::int[], 1), 0) = 0 OR db.id = ANY($2))
  AND ($3::text IS NULL OR $3 = ANY(t.uuid_array))
  AND db.id IN (
      SELECT dashboard_id FROM dashboard_grants
      WHERE org_id = ANY($4) OR user_id = ANY($5) OR global IS TRUE)
GROUP BY db.id, t.uuid_array
ORDER BY db.id
LIMIT $6::int
"#,
            )
            .bind(opts.after)
            .bind(&ids)
            .bind(&opts.with_view_unique_id)
            .bind(&opts.user_ids)
            .bind(&opts.org_ids)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
            let rows = rows
                .iter()
                .map(DashboardRow::from_row)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| err.to_string())?;
            Ok(rows
                .into_iter()
                .map(|row| DashboardData {
                    id: row.id,
                    title: row.title,
                    insight_ids: row.insight_view_unique_ids.unwrap_or_default(),
                    user_grants: row.granted_users,
                    org_grants: row.granted_orgs,
                    global_grant: row.granted_global,
                })
                .collect())
        })
    }

    fn dashboard_reference_count(&self, view_id: i32) -> Result<i64, String> {
        block_on(async {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM dashboard_insight_view WHERE insight_view_id = $1",
            )
            .bind(view_id)
            .fetch_one(&self.pool)
            .await
        })
        .map_err(|err| err.to_string())
    }
}

struct DashboardRow {
    id: i32,
    title: String,
    insight_view_unique_ids: Option<Vec<String>>,
    granted_users: Vec<i64>,
    granted_orgs: Vec<i64>,
    granted_global: bool,
}

impl DashboardRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            title: row.try_get("title")?,
            insight_view_unique_ids: row.try_get("insight_view_unique_ids")?,
            granted_users: row.try_get("granted_users")?,
            granted_orgs: row.try_get("granted_orgs")?,
            granted_global: row.try_get("granted_global")?,
        })
    }
}
