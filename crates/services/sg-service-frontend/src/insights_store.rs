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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use sqlx::{PgPool, Row};

use sg_graphql::exec::Actor;

use crate::codeintel_uploads_graphql_store::block_on;
use crate::insights_graphql::{
    DashboardData, DashboardGrant, DashboardQueryOptions, InsightQueryOptions,
    InsightSeriesData, InsightSeriesMetadataData, InsightSeriesQueryStatusData,
    InsightSeriesStatusData, InsightsDatabase, InsightsWriteDatabase, InsightViewData,
    InsightViewFilters, LineChartMutationInput, PieChartCreateInput, PieChartUpdateInput,
    SaveAsNewViewInput, SeriesMutationInput, SeriesPointData,
};
use sg_config::LiveConfig;
use sg_redis::KeyValue;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

/// `licensing.Check(licensing.FeatureCodeInsights)`: the single source of
/// truth for Limited Access Mode (M9.47c). `is_licensed() == false` makes
/// [`crate::insights_graphql::InsightsDatabase::limited_access_mode`] true and
/// drives the create-mutation rejections.
///
/// The baseline re-reads `conf.Get().LicenseKey` and
/// `licensing.IsLicenseValid()` on every check, so production builds wire
/// [`LiveCodeInsightsLicense`]; tests use [`FixedCodeInsightsLicense`].
pub trait CodeInsightsLicense: Send + Sync {
    /// `licensing.Check(licensing.FeatureCodeInsights) == nil`.
    fn is_licensed(&self) -> bool;
}

/// A fixed answer for tests/differentials (the Go oracle's
/// `licensing.MockCheckFeature`).
#[derive(Debug, Clone, Copy)]
pub struct FixedCodeInsightsLicense(pub bool);

impl CodeInsightsLicense for FixedCodeInsightsLicense {
    fn is_licensed(&self) -> bool {
        self.0
    }
}

/// Production adapter: `licensing.Check(FeatureCodeInsights)` over the live
/// site configuration's `licenseKey` plus the Redis validity flag
/// (`licensing:is_license_valid`, absent/unreadable means valid). Mirrors
/// `cmd/worker/internal/insights`'s license monitor assembly.
pub struct LiveCodeInsightsLicense {
    live: Arc<LiveConfig>,
    /// `redispool.Store`; `None` is a frontend without Redis, where the
    /// baseline's nil value also means valid.
    kv: Option<Arc<KeyValue>>,
}

impl LiveCodeInsightsLicense {
    pub fn new(live: Arc<LiveConfig>, kv: Option<Arc<KeyValue>>) -> Self {
        Self { live, kv }
    }
}

/// `licensing.IsLicenseValid` (`redispool.Store` `licensing:is_license_valid`):
/// nil/read error/unknown value all mean valid.
fn license_valid_flag(kv: &KeyValue) -> bool {
    match block_on(kv.get_string(sg_licensing::LICENSE_VALIDITY_STORE_KEY)) {
        Ok(None) => true,
        Ok(Some(raw)) => match raw.as_str() {
            "1" => true,
            "0" => false,
            _ => true,
        },
        Err(_) => true,
    }
}

impl CodeInsightsLicense for LiveCodeInsightsLicense {
    fn is_licensed(&self) -> bool {
        let license_key = self.live.current().site.license_key();
        let valid = match &self.kv {
            Some(kv) => license_valid_flag(kv),
            None => true,
        };
        sg_licensing::check_code_insights(&license_key, valid).is_ok()
    }
}

#[derive(Clone)]
pub struct PgInsightsStore {
    /// The code-insights database (`sourcegraph-codeinsights`).
    pool: PgPool,
    /// The frontend database, used only for `getUserPermissions`' org lookup.
    /// `None` reproduces an anonymous/internal actor (no user/org grants).
    frontend_pool: Option<PgPool>,
    /// `licensing.Check(FeatureCodeInsights)` (M9.47c). The default is
    /// *unlicensed*: an instance without a configured license key runs the
    /// free plan, which does not include Code Insights, so the baseline is in
    /// Limited Access Mode. Production wires [`LiveCodeInsightsLicense`].
    license: Arc<dyn CodeInsightsLicense>,
    /// M9.33a: the live-preview search/repository/gitserver seam assembled by
    /// the frontend combination root. `None` keeps the explicit
    /// `search execution is not connected` errors.
    preview_seam: Option<Arc<dyn crate::insights_preview::PreviewSearchSeam>>,
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
            license: Arc::new(FixedCodeInsightsLicense(false)),
            preview_seam: None,
        }
    }

    /// Replace the license seam (`licensing.Check(FeatureCodeInsights)`).
    pub fn with_license_provider(
        mut self,
        license: Arc<dyn CodeInsightsLicense>,
    ) -> Self {
        self.license = license;
        self
    }

    /// Wire the production check: live site configuration `licenseKey` plus
    /// the Redis validity flag.
    pub fn with_live_license(
        self,
        live: Arc<LiveConfig>,
        kv: Option<Arc<KeyValue>>,
    ) -> Self {
        self.with_license_provider(Arc::new(LiveCodeInsightsLicense::new(live, kv)))
    }

    /// Wire the production search combination root's live-preview seam
    /// (M9.33a). Without it the preview fields keep the explicit
    /// `search execution is not connected` errors.
    pub fn with_preview_seam(
        mut self,
        seam: Arc<dyn crate::insights_preview::PreviewSearchSeam>,
    ) -> Self {
        self.preview_seam = Some(seam);
        self
    }

    pub fn with_frontend_pool(mut self, pool: PgPool) -> Self {
        self.frontend_pool = Some(pool);
        self
    }

    /// Test seam: reproduce `licensing.Check(FeatureCodeInsights) != nil`.
    pub fn with_limited_access_mode(self, limited_access_mode: bool) -> Self {
        self.with_license_provider(Arc::new(FixedCodeInsightsLicense(!limited_access_mode)))
    }

    /// Test seam: reproduce `licensing.Check(FeatureCodeInsights) == nil`.
    pub fn with_code_insights_licensed(self, licensed: bool) -> Self {
        self.with_license_provider(Arc::new(FixedCodeInsightsLicense(licensed)))
    }

    /// `licensing.Check(FeatureCodeInsights)` evaluated now.
    fn is_code_insights_licensed(&self) -> bool {
        self.license.is_licensed()
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
       i.repository_criteria,
       i.id AS insight_series_id,
       i.backfill_queued_at,
       i.supports_augmentation
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
    insight_series_id: i32,
    backfill_queued_at: Option<PrimitiveDateTime>,
    supports_augmentation: bool,
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
            insight_series_id: row.try_get("insight_series_id")?,
            backfill_queued_at: row.try_get("backfill_queued_at")?,
            supports_augmentation: row.try_get("supports_augmentation")?,
        })
    }

    fn series(&self) -> InsightSeriesData {
        InsightSeriesData {
            insight_series_id: self.insight_series_id,
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
            backfill_queued_at: self.backfill_queued_at,
            supports_augmentation: self.supports_augmentation,
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
    fn preview_seam(&self) -> &dyn crate::insights_preview::PreviewSearchSeam {
        self.preview_seam
            .as_deref()
            .unwrap_or(&crate::insights_preview::UnavailablePreviewSeam)
    }

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
    WHERE user_id = ANY($1) OR org_id = ANY($2) OR global IS TRUE
),
visible_views AS (
    SELECT insight_view_id FROM insight_view_grants
    WHERE user_id = ANY($1) OR org_id = ANY($2) OR global IS TRUE
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
            // SQL uses `org_id = ANY($4) OR user_id = ANY($5)`.
            .bind(&opts.org_ids)
            .bind(&opts.user_ids)
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

    // -- M9.31a recorded data series. -------------------------------------

    fn load_series_points(
        &self,
        series: &InsightSeriesData,
        filters: &InsightViewFilters,
        num_samples: Option<i32>,
    ) -> Result<Vec<SeriesPointData>, String> {
        // `getRecordedSeriesPointOpts` sets at most `n < 90` samples.
        let num_samples = match num_samples {
            Some(value) if value > 0 && value < 90 => value,
            _ => 90,
        };
        block_on(async {
            // `GetOffsetNRecordingTime(..., excludeSnapshot=false)`: no
            // snapshot predicate; offset is `n` (not `n - 1`).
            let after: Option<OffsetDateTime> = sqlx::query_scalar(
                "SELECT recording_time FROM insight_series_recording_times \
                 WHERE insight_series_id = $1 \
                 ORDER BY recording_time DESC OFFSET $2 LIMIT 1",
            )
            .bind(series.insight_series_id)
            .bind(i64::from(num_samples))
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| err.to_string())?;

            let mut includes: Vec<String> = Vec::new();
            let mut excludes: Vec<String> = Vec::new();
            if let Some(regex) = &filters.include_repo_regex {
                if !regex.is_empty() {
                    includes.push(regex.clone());
                }
            }
            if let Some(regex) = &filters.exclude_repo_regex {
                if !regex.is_empty() {
                    excludes.push(regex.clone());
                }
            }
            let needs_repo_names = !includes.is_empty() || !excludes.is_empty();

            // `fullVectorSeriesAggregation`.
            let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "SELECT sub.series_id, sub.interval_time, SUM(sub.value) AS value, sub.capture FROM (\
                 SELECT sp.repo_name_id, sp.series_id, date_trunc('seconds', sp.time) AS interval_time, \
                 MAX(value) AS value, capture \
                 FROM (SELECT * FROM series_points UNION ALL SELECT * FROM series_points_snapshots) AS sp",
            );
            if needs_repo_names {
                query.push(" JOIN repo_names rn ON sp.repo_name_id = rn.id");
            }
            query.push(" WHERE sp.series_id = ");
            query.push_bind(series.series_id.clone());
            if let Some(after) = after {
                query.push(" AND sp.time > ");
                query.push_bind(after);
            }
            for include in &includes {
                query.push(" AND rn.name ~ ");
                query.push_bind(include.clone());
            }
            for exclude in &excludes {
                query.push(" AND rn.name !~ ");
                query.push_bind(exclude.clone());
            }
            query.push(
                " GROUP BY sp.series_id, interval_time, sp.repo_name_id, capture \
                 ORDER BY sp.series_id, interval_time, sp.repo_name_id) sub \
                 GROUP BY sub.series_id, sub.interval_time, sub.capture \
                 ORDER BY sub.series_id, sub.interval_time ASC",
            );

            let rows = query
                .build_query_as::<(String, OffsetDateTime, f64, Option<String>)>()
                .fetch_all(&self.pool)
                .await
                .map_err(|err| err.to_string())?;
            let mut points: Vec<SeriesPointData> = rows
                .into_iter()
                .map(|(series_id, time, value, capture)| SeriesPointData {
                    series_id,
                    time,
                    value,
                    capture,
                })
                .collect();

            if series.supports_augmentation && series.insight_series_id != 0 {
                // `augmentSeriesPoints` → `GetInsightSeriesRecordingTimes`
                // (respecting the same `after` bound).
                let mut times_query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                    "SELECT date_trunc('seconds', recording_time) FROM insight_series_recording_times \
                     WHERE insight_series_id = ",
                );
                times_query.push_bind(series.insight_series_id);
                if let Some(after) = after {
                    times_query.push(" AND recording_time > ");
                    times_query.push_bind(after);
                }
                times_query.push(" ORDER BY recording_time ASC");
                let recording_times = times_query
                    .build_query_scalar::<OffsetDateTime>()
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|err| err.to_string())?;

                if !recording_times.is_empty() {
                    // `coalesceZeroValues`. Capture values are enumerated in
                    // sorted order (the baseline uses randomized Go map order;
                    // callers/tests keep that observable input ≤ 1 capture).
                    let mut point_map: HashMap<(OffsetDateTime, String), SeriesPointData> =
                        HashMap::new();
                    let mut capture_values: BTreeSet<String> = BTreeSet::new();
                    for point in &points {
                        let capture = point.capture.clone().unwrap_or_default();
                        capture_values.insert(capture.clone());
                        point_map.insert((point.time, capture), point.clone());
                    }
                    let mut augmented = Vec::new();
                    for timestamp in &recording_times {
                        for capture_value in &capture_values {
                            if let Some(point) =
                                point_map.get(&(*timestamp, capture_value.clone()))
                            {
                                augmented.push(point.clone());
                            } else {
                                augmented.push(SeriesPointData {
                                    series_id: series.series_id.clone(),
                                    time: *timestamp,
                                    value: 0.0,
                                    capture: if capture_value.is_empty() {
                                        None
                                    } else {
                                        Some(capture_value.clone())
                                    },
                                });
                            }
                        }
                    }
                    if !augmented.is_empty() {
                        points = augmented;
                    }
                }
            }

            Ok(points)
        })
    }

    fn load_series_status(
        &self,
        series: &InsightSeriesData,
    ) -> Result<InsightSeriesStatusData, String> {
        let frontend = self
            .frontend_pool
            .as_ref()
            .ok_or_else(|| "frontend pool not configured".to_owned())?;
        block_on(async {
            let rows = sqlx::query(
                "SELECT state, COUNT(*) FROM insights_query_runner_jobs \
                 WHERE series_id = $1 GROUP BY state",
            )
            .bind(&series.series_id)
            .fetch_all(frontend)
            .await
            .map_err(|err| err.to_string())?;
            let mut queued = 0i32;
            let mut processing = 0i32;
            let mut completed = 0i32;
            let mut errored = 0i32;
            let mut failed = 0i32;
            for row in rows {
                let state: String = row.try_get("state").map_err(|err| err.to_string())?;
                let count: i64 = row.try_get("count").map_err(|err| err.to_string())?;
                let count = count as i32;
                match state.as_str() {
                    "queued" => queued = count,
                    "processing" => processing = count,
                    "completed" => completed = count,
                    "errored" => errored = count,
                    "failed" => failed = count,
                    _ => {}
                }
            }

            let backfill_states: Vec<String> = sqlx::query_scalar(
                "SELECT state FROM insight_series_backfill WHERE series_id = $1",
            )
            .bind(series.insight_series_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
            let backfill_in_progress = backfill_states
                .iter()
                .any(|state| state != "completed" && state != "failed");

            let pending_jobs = queued + processing + errored;
            let is_loading = series.backfill_queued_at.is_none()
                || pending_jobs > 0
                || backfill_in_progress;

            Ok(InsightSeriesStatusData {
                // The baseline never assigns `totalPoints`; stay at zero.
                total_points: 0,
                pending_jobs,
                completed_jobs: completed,
                failed_jobs: failed,
                backfill_queued_at: series.backfill_queued_at,
                is_loading,
            })
        })
    }

    // -- M9.30a dashboard writes. -----------------------------------------

    fn limited_access_mode(&self) -> bool {
        !self.is_code_insights_licensed()
    }

    fn create_dashboard(
        &self,
        title: &str,
        grants: &[DashboardGrant],
        user_ids: &[i32],
        org_ids: &[i32],
    ) -> Result<Option<DashboardData>, String> {
        let id = block_on(async {
            let mut tx = self.pool.begin().await.map_err(stringify)?;
            let id: i32 = sqlx::query_scalar(
                "INSERT INTO dashboard (title, save, type) VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(title)
            .bind(true)
            .bind("standard")
            .fetch_one(&mut *tx)
            .await
            .map_err(stringify)?;
            // `CreateDashboard` adds `Dashboard.InsightIDs`; the resolver passes
            // none, so `AddViewsToDashboard` is a no-op here.
            add_grants(&mut tx, id, grants).await?;
            tx.commit().await.map_err(stringify)?;
            Ok::<i32, String>(id)
        })?;
        self.dashboard_by_id(id, user_ids, org_ids)
    }

    fn update_dashboard(
        &self,
        id: i32,
        title: Option<&str>,
        grants: Option<&[DashboardGrant]>,
        user_ids: &[i32],
        org_ids: &[i32],
    ) -> Result<Option<DashboardData>, String> {
        block_on(async {
            let mut tx = self.pool.begin().await.map_err(stringify)?;
            if let Some(title) = title {
                sqlx::query("UPDATE dashboard SET title = $1 WHERE id = $2")
                    .bind(title)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(stringify)?;
            }
            if let Some(grants) = grants {
                sqlx::query("DELETE FROM dashboard_grants WHERE dashboard_id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(stringify)?;
                add_grants(&mut tx, id, grants).await?;
            }
            tx.commit().await.map_err(stringify)?;
            Ok::<(), String>(())
        })?;
        self.dashboard_by_id(id, user_ids, org_ids)
    }

    fn delete_dashboard(&self, id: i32) -> Result<(), String> {
        block_on(async {
            sqlx::query("UPDATE dashboard SET deleted_at = NOW() WHERE id = $1")
                .bind(id)
                .execute(&self.pool)
                .await
        })
        .map(|_| ())
        .map_err(stringify)
    }

    fn ensure_limited_access_mode_dashboard(&self) -> Result<i32, String> {
        block_on(async {
            let mut tx = self.pool.begin().await.map_err(stringify)?;
            let id = ensure_limited_access_mode(&mut tx).await?;
            tx.commit().await.map_err(stringify)?;
            Ok::<i32, String>(id)
        })
    }

    fn has_dashboard_permission(
        &self,
        dashboard_ids: &[i32],
        user_ids: &[i32],
        org_ids: &[i32],
    ) -> Result<bool, String> {
        block_on(async {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM dashboard
                 WHERE id = ANY($1)
                   AND id IN (
                       SELECT dashboard_id FROM dashboard_grants
                       WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE)",
            )
            .bind(dashboard_ids)
            .bind(org_ids)
            .bind(user_ids)
            .fetch_one(&self.pool)
            .await
            .map_err(stringify)?;
            Ok::<bool, String>(count == dashboard_ids.len() as i64)
        })
    }

    fn add_view_to_dashboard(
        &self,
        dashboard_id: i32,
        view_id: &str,
        user_ids: &[i32],
        org_ids: &[i32],
        limited_access_mode: bool,
    ) -> Result<DashboardData, String> {
        block_on(async {
            let mut tx = self.pool.begin().await.map_err(stringify)?;
            if limited_access_mode {
                let lam_id = ensure_limited_access_mode(&mut tx).await?;
                if lam_id == dashboard_id {
                    return Err(
                        "Cannot add insights to this dashboard while in Limited Access Mode"
                            .to_owned(),
                    );
                }
            }
            if !has_dashboard_permission_tx(&mut tx, dashboard_id, user_ids, org_ids).await? {
                return Err("dashboard not found".to_owned());
            }
            if !view_visible_tx(&mut tx, view_id, user_ids, org_ids).await? {
                return Err("insight not found".to_owned());
            }
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM dashboard_insight_view div
                 JOIN insight_view iv ON div.insight_view_id = iv.id
                 WHERE div.dashboard_id = $1 AND iv.unique_id = $2",
            )
            .bind(dashboard_id)
            .bind(view_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(stringify)?;
            if exists == 0 {
                sqlx::query(
                    "INSERT INTO dashboard_insight_view (dashboard_id, insight_view_id)
                     SELECT $1, iv.id FROM insight_view iv
                     WHERE iv.unique_id = $2
                     ON CONFLICT DO NOTHING",
                )
                .bind(dashboard_id)
                .bind(view_id)
                .execute(&mut *tx)
                .await
                .map_err(stringify)?;
            }
            // The baseline re-reads `GetDashboards(IDs, UserIDs, OrgIDs)` inside
            // the same transaction; when the (soft-deleted) dashboard is not
            // visible the resolver returns "dashboard not found" and `Done`
            // rolls the insert back.
            if !dashboard_visible_tx(&mut tx, dashboard_id, user_ids, org_ids).await? {
                return Err("dashboard not found".to_owned());
            }
            tx.commit().await.map_err(stringify)?;
            Ok::<(), String>(())
        })?;
        self.dashboard_by_id(dashboard_id, user_ids, org_ids)?
            .ok_or_else(|| "dashboard not found".to_owned())
    }

    fn remove_view_from_dashboard(
        &self,
        dashboard_id: i32,
        view_id: &str,
        user_ids: &[i32],
        org_ids: &[i32],
        limited_access_mode: bool,
    ) -> Result<DashboardData, String> {
        block_on(async {
            let mut tx = self.pool.begin().await.map_err(stringify)?;
            if limited_access_mode {
                let lam_id = ensure_limited_access_mode(&mut tx).await?;
                if lam_id == dashboard_id {
                    return Err(
                        "Cannot remove insights from this dashboard while in Limited Access Mode"
                            .to_owned(),
                    );
                }
            }
            if !has_dashboard_permission_tx(&mut tx, dashboard_id, user_ids, org_ids).await? {
                return Err("dashboard not found".to_owned());
            }
            sqlx::query(
                "DELETE FROM dashboard_insight_view
                 WHERE dashboard_id = $1
                   AND insight_view_id IN (
                       SELECT id FROM insight_view WHERE unique_id = $2)",
            )
            .bind(dashboard_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await
            .map_err(stringify)?;
            // Same transaction boundary as `AddInsightViewToDashboard`: the
            // post-removal `GetDashboards` failure rolls the delete back.
            if !dashboard_visible_tx(&mut tx, dashboard_id, user_ids, org_ids).await? {
                return Err("dashboard not found".to_owned());
            }
            tx.commit().await.map_err(stringify)?;
            Ok::<(), String>(())
        })?;
        self.dashboard_by_id(dashboard_id, user_ids, org_ids)?
            .ok_or_else(|| "dashboard not found".to_owned())
    }
}

fn stringify(err: sqlx::Error) -> String {
    err.to_string()
}

impl PgInsightsStore {
    /// `GetDashboards(IDs: [id], UserIDs, OrgIDs)`.
    fn dashboard_by_id(
        &self,
        id: i32,
        user_ids: &[i32],
        org_ids: &[i32],
    ) -> Result<Option<DashboardData>, String> {
        let opts = DashboardQueryOptions {
            ids: vec![id],
            user_ids: user_ids.to_vec(),
            org_ids: org_ids.to_vec(),
            ..Default::default()
        };
        Ok(self.list_dashboards(&opts)?.into_iter().next())
    }
}

/// `AddDashboardGrants`: one row per grant; empty grants is a no-op.
async fn add_grants(
    tx: &mut sqlx::PgConnection,
    dashboard_id: i32,
    grants: &[DashboardGrant],
) -> Result<(), String> {
    for grant in grants {
        sqlx::query(
            "INSERT INTO dashboard_grants (dashboard_id, user_id, org_id, global)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(dashboard_id)
        .bind(grant.user_id)
        .bind(grant.org_id)
        .bind(grant.global)
        .execute(&mut *tx)
        .await
        .map_err(stringify)?;
    }
    Ok(())
}

/// `EnsureLimitedAccessModeDashboard` in the caller's transaction: find,
/// create-with-global-grant if missing, then restore `deleted_at`.
async fn ensure_limited_access_mode(tx: &mut sqlx::PgConnection) -> Result<i32, String> {
    let existing: Option<i32> =
        sqlx::query_scalar("SELECT id FROM dashboard WHERE type = $1")
            .bind("limited_access_mode")
            .fetch_optional(&mut *tx)
            .await
            .map_err(stringify)?;
    let id = match existing {
        Some(id) => id,
        None => {
            let id: i32 = sqlx::query_scalar(
                "INSERT INTO dashboard (title, save, type)
                 VALUES ('Limited Access Mode Dashboard', true, 'limited_access_mode')
                 RETURNING id",
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(stringify)?;
            sqlx::query("INSERT INTO dashboard_grants (dashboard_id, global) VALUES ($1, true)")
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(stringify)?;
            id
        }
    };
    sqlx::query("UPDATE dashboard SET deleted_at = NULL WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(stringify)?;
    Ok(id)
}

/// `HasDashboardPermission` inside the Add/Remove transaction.
/// `GetDashboards(IDs: [id], UserIDs, OrgIDs)` visibility inside the caller's
/// transaction: the dashboard exists, is not soft-deleted and has a matching
/// user/org/global grant.
async fn dashboard_visible_tx(
    tx: &mut sqlx::PgConnection,
    dashboard_id: i32,
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<bool, String> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dashboard
         WHERE id = $1
           AND deleted_at IS NULL
           AND id IN (
               SELECT dashboard_id FROM dashboard_grants
               WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE)",
    )
    .bind(dashboard_id)
    .bind(org_ids)
    .bind(user_ids)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| err.to_string())?;
    Ok(count == 1)
}

async fn has_dashboard_permission_tx(
    tx: &mut sqlx::PgConnection,
    dashboard_id: i32,
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<bool, String> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dashboard
         WHERE id = $1
           AND id IN (
               SELECT dashboard_id FROM dashboard_grants
               WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE)",
    )
    .bind(dashboard_id)
    .bind(org_ids)
    .bind(user_ids)
    .fetch_one(&mut *tx)
    .await
    .map_err(stringify)?;
    Ok(count == 1)
}

/// `validateUserAccessForView` (`store.InsightStore.GetAll` with `UniqueID`):
/// a view is visible when a non-deleted series is granted to the caller or it
/// is attached to a visible (non-deleted) dashboard.
async fn view_visible_tx(
    tx: &mut sqlx::PgConnection,
    view_id: &str,
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<bool, String> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM insight_view iv
             JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
             JOIN insight_series i ON ivs.insight_series_id = i.id
             WHERE iv.unique_id = $1
               AND i.deleted_at IS NULL
               AND (iv.id IN (
                        SELECT div.insight_view_id
                        FROM dashboard_insight_view div
                        JOIN dashboard db ON db.id = div.dashboard_id
                        WHERE db.deleted_at IS NULL
                          AND db.id IN (
                              SELECT dashboard_id FROM dashboard_grants
                              WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE))
                    OR iv.id IN (
                        SELECT insight_view_id FROM insight_view_grants
                        WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE)))",
    )
    .bind(view_id)
    .bind(org_ids)
    .bind(user_ids)
    .fetch_one(&mut *tx)
    .await
    .map_err(stringify)
}

// ---------------------------------------------------------------------------
// Write surface (M9.30b).
// ---------------------------------------------------------------------------

fn now_primitive() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

fn zero_time() -> PrimitiveDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1, Month::January, 1).expect("year 1"),
        Time::MIDNIGHT,
    )
}

fn next_snapshot(now: PrimitiveDateTime) -> PrimitiveDateTime {
    PrimitiveDateTime::new(now.date() + Duration::days(1), Time::MIDNIGHT)
}

/// `timeseries.TimeInterval.StepForwards` (calendar semantics are not
/// observable through the mutation snapshot; every timestamp is normalized).
fn step_forwards(now: PrimitiveDateTime, unit: &str, value: i32) -> PrimitiveDateTime {
    let value = i64::from(value);
    let duration = match unit {
        "YEAR" => Duration::days(365 * value),
        "MONTH" => Duration::days(30 * value),
        "WEEK" => Duration::days(7 * value),
        "DAY" => Duration::days(value),
        "HOUR" => Duration::hours(value),
        _ => return now,
    };
    now + duration
}

fn series_fill_metadata(series: &SeriesMutationInput) -> (Option<String>, Option<String>) {
    (Some(series.label.clone()), Some(series.stroke.clone()))
}

struct MappedSeries {
    series_pk: i32,
    series_id: String,
    query: String,
    label: String,
    stroke: String,
    repositories: Vec<String>,
    sample_interval_unit: String,
    sample_interval_value: i32,
    generated_from_capture_groups: bool,
    just_in_time: bool,
    generation_method: String,
    group_by: Option<String>,
    repository_criteria: Option<String>,
}

impl MappedSeries {
    fn data(&self) -> InsightSeriesData {
        InsightSeriesData {
            insight_series_id: self.series_pk,
            series_id: self.series_id.clone(),
            query: self.query.clone(),
            label: self.label.clone(),
            stroke: self.stroke.clone(),
            repositories: self.repositories.clone(),
            sample_interval_unit: self.sample_interval_unit.clone(),
            sample_interval_value: self.sample_interval_value,
            generated_from_capture_groups: self.generated_from_capture_groups,
            just_in_time: self.just_in_time,
            generation_method: self.generation_method.clone(),
            group_by: self.group_by.clone(),
            repository_criteria: self.repository_criteria.clone(),
            backfill_queued_at: None,
            supports_augmentation: true,
        }
    }
}

fn mapped_series_from_row(row: &sqlx::postgres::PgRow) -> Result<MappedSeries, sqlx::Error> {
    Ok(MappedSeries {
        series_pk: row.try_get("series_pk")?,
        series_id: row.try_get("series_id")?,
        query: row.try_get("query")?,
        label: row.try_get("label")?,
        stroke: row.try_get("stroke")?,
        repositories: row.try_get("repositories")?,
        sample_interval_unit: row.try_get("sample_interval_unit")?,
        sample_interval_value: row.try_get("sample_interval_value")?,
        generated_from_capture_groups: row.try_get("generated_from_capture_groups")?,
        just_in_time: row.try_get("just_in_time")?,
        generation_method: row.try_get("generation_method")?,
        group_by: row.try_get("group_by")?,
        repository_criteria: row.try_get("repository_criteria")?,
    })
}

/// `InsightStore.GetMapped` (`getInsightByViewSql`): includes soft-deleted
/// series rows.
async fn get_mapped(
    conn: &mut sqlx::PgConnection,
    unique_id: &str,
) -> Result<Option<(i32, Vec<MappedSeries>)>, String> {
    let rows = sqlx::query(
        r#"
SELECT iv.id AS view_id, ivs.label, ivs.stroke,
       i.id AS series_pk, i.series_id, i.query, i.repositories,
       i.sample_interval_unit::text AS sample_interval_unit, i.sample_interval_value,
       i.generated_from_capture_groups, i.just_in_time, i.generation_method,
       i.group_by, i.repository_criteria
FROM insight_view iv
JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
JOIN insight_series i ON ivs.insight_series_id = i.id
WHERE iv.unique_id = $1
ORDER BY iv.id, i.series_id
"#,
    )
    .bind(unique_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    if rows.is_empty() {
        return Ok(None);
    }
    let view_id: i32 = rows[0]
        .try_get("view_id")
        .map_err(|err| err.to_string())?;
    let series = rows
        .iter()
        .map(mapped_series_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    Ok(Some((view_id, series)))
}

/// `InsightPermissionsValidator.validateUserAccessForView` (`GetAll` with the
/// actor's grants).
async fn visible_view_exists(
    conn: &mut sqlx::PgConnection,
    unique_id: &str,
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<bool, String> {
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(DISTINCT iv.unique_id)
FROM insight_view iv
JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
JOIN insight_series i ON ivs.insight_series_id = i.id
WHERE (iv.id IN (
           SELECT div.insight_view_id
           FROM dashboard_insight_view div
           JOIN dashboard db ON db.id = div.dashboard_id
           WHERE db.deleted_at IS NULL
             AND db.id IN (
                 SELECT dashboard_id FROM dashboard_grants
                 WHERE org_id = ANY($1) OR user_id = ANY($2) OR global IS TRUE))
       OR iv.id IN (
           SELECT insight_view_id FROM insight_view_grants
           WHERE org_id = ANY($1) OR user_id = ANY($2) OR global IS TRUE))
  AND iv.unique_id = $3
"#,
    )
    .bind(org_ids)
    .bind(user_ids)
    .bind(unique_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(count > 0)
}

async fn create_view(
    conn: &mut sqlx::PgConnection,
    title: &str,
    unique_id: &str,
    filters: &InsightViewFilters,
    other_threshold: Option<f64>,
    presentation_type: &str,
) -> Result<i32, String> {
    sqlx::query_scalar(
        r#"
INSERT INTO insight_view (title, description, unique_id, default_filter_include_repo_regex,
                          default_filter_exclude_repo_regex, default_filter_search_contexts,
                          other_threshold, presentation_type)
VALUES ($1, '', $2, $3, $4, $5, $6, $7::presentation_type_enum)
RETURNING id
"#,
    )
    .bind(title)
    .bind(unique_id)
    .bind(&filters.include_repo_regex)
    .bind(&filters.exclude_repo_regex)
    .bind(&filters.search_contexts)
    .bind(other_threshold)
    .bind(presentation_type)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn update_view(
    conn: &mut sqlx::PgConnection,
    unique_id: &str,
    title: &str,
    filters: &InsightViewFilters,
    other_threshold: Option<f64>,
    presentation_type: &str,
    sort_mode: Option<&str>,
    sort_direction: Option<&str>,
    limit: Option<i32>,
    num_samples: Option<i32>,
) -> Result<i32, String> {
    sqlx::query_scalar(
        r#"
UPDATE insight_view
SET title = $1, description = '', default_filter_include_repo_regex = $2,
    default_filter_exclude_repo_regex = $3, default_filter_search_contexts = $4,
    other_threshold = $5, presentation_type = $6::presentation_type_enum,
    series_sort_mode = $7::series_sort_mode_enum,
    series_sort_direction = $8::series_sort_direction_enum,
    series_limit = $9, series_num_samples = $10
WHERE unique_id = $11
RETURNING id
"#,
    )
    .bind(title)
    .bind(&filters.include_repo_regex)
    .bind(&filters.exclude_repo_regex)
    .bind(&filters.search_contexts)
    .bind(other_threshold)
    .bind(presentation_type)
    .bind(sort_mode)
    .bind(sort_direction)
    .bind(limit)
    .bind(num_samples)
    .bind(unique_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())
}

async fn add_view_grant(
    conn: &mut sqlx::PgConnection,
    view_id: i32,
    uid: i32,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO insight_view_grants (insight_view_id, org_id, user_id, global) VALUES ($1, NULL, $2, NULL)",
    )
    .bind(view_id)
    .bind(uid)
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

async fn set_series_enabled(
    conn: &mut sqlx::PgConnection,
    series_id: &str,
    enabled: bool,
) -> Result<(), String> {
    if enabled {
        sqlx::query("UPDATE insight_series SET deleted_at = NULL WHERE series_id = $1")
            .bind(series_id)
            .execute(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    } else {
        sqlx::query("UPDATE insight_series SET deleted_at = $2 WHERE series_id = $1")
            .bind(series_id)
            .bind(now_primitive())
            .execute(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

async fn attach_series_to_view(
    conn: &mut sqlx::PgConnection,
    series_pk: i32,
    series_id: &str,
    view_id: i32,
    label: &str,
    stroke: &str,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO insight_view_series (insight_series_id, insight_view_id, label, stroke) VALUES ($1, $2, $3, $4)",
    )
    .bind(series_pk)
    .bind(view_id)
    .bind(label)
    .bind(stroke)
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    set_series_enabled(conn, series_id, true).await
}

async fn update_view_series_metadata(
    conn: &mut sqlx::PgConnection,
    series_id: &str,
    view_id: i32,
    label: &str,
    stroke: &str,
) -> Result<(), String> {
    sqlx::query(
        r#"
UPDATE insight_view_series vs
SET label = $1, stroke = $2
FROM insight_series s
WHERE s.series_id = $3 AND vs.insight_series_id = s.id AND vs.insight_view_id = $4
"#,
    )
    .bind(label)
    .bind(stroke)
    .bind(series_id)
    .bind(view_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

async fn remove_series_from_view(
    conn: &mut sqlx::PgConnection,
    series_id: &str,
    view_id: i32,
) -> Result<(), String> {
    sqlx::query(
        r#"
DELETE FROM insight_view_series vs
USING insight_series s
WHERE s.series_id = $1 AND vs.insight_series_id = s.id AND vs.insight_view_id = $2
"#,
    )
    .bind(series_id)
    .bind(view_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    let references: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*) FROM insight_view_series viewSeries
INNER JOIN insight_series series ON viewSeries.insight_series_id = series.id
WHERE series.series_id = $1
"#,
    )
    .bind(series_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    if references == 0 {
        set_series_enabled(conn, series_id, false).await?;
    }
    Ok(())
}

async fn find_matching_series(
    conn: &mut sqlx::PgConnection,
    series: &SeriesMutationInput,
) -> Result<Option<MappedSeries>, String> {
    let group_by = series.group_by.as_ref().map(|value| value.to_lowercase());
    let time = series.time_scope.as_ref().expect("validated time scope");
    let dynamic = series.generated_from_capture_groups.unwrap_or(false);
    // NOTE: `FindMatchingSeries` does not filter `deleted_at`.
    let rows = sqlx::query(
        r#"
SELECT i.id AS series_pk, i.series_id, i.query, i.repositories,
       i.sample_interval_unit::text AS sample_interval_unit, i.sample_interval_value,
       i.generated_from_capture_groups, i.just_in_time, i.generation_method,
       i.group_by, i.repository_criteria,
       COALESCE(ivs.label, '') AS label, COALESCE(ivs.stroke, '') AS stroke
FROM insight_series i
LEFT JOIN insight_view_series ivs ON ivs.insight_series_id = i.id
WHERE (i.repositories = '{}' OR i.repositories IS NULL)
  AND i.query = $1
  AND i.sample_interval_unit = $2::time_unit
  AND i.sample_interval_value = $3
  AND i.generated_from_capture_groups = $4
  AND (($5::text IS NULL AND i.group_by IS NULL) OR i.group_by = $5)
ORDER BY i.id
LIMIT 1
"#,
    )
    .bind(&series.query)
    .bind(&time.unit)
    .bind(time.value)
    .bind(dynamic)
    .bind(&group_by)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    match rows.first() {
        Some(row) => mapped_series_from_row(row)
            .map(Some)
            .map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

async fn create_series(
    conn: &mut sqlx::PgConnection,
    series: &SeriesMutationInput,
) -> Result<MappedSeries, String> {
    let now = now_primitive();
    let time = series.time_scope.as_ref().expect("validated time scope");
    let repositories = series
        .repository_scope
        .as_ref()
        .map(|scope| scope.repositories.clone())
        .unwrap_or_default();
    let repository_criteria = series
        .repository_scope
        .as_ref()
        .and_then(|scope| scope.repository_criteria.clone());
    let dynamic = series.generated_from_capture_groups.unwrap_or(false);
    let group_by = series.group_by.as_ref().map(|value| value.to_lowercase());
    let (next_recording_after, oldest_historical_at) = if group_by.is_some() {
        (
            PrimitiveDateTime::new(
                Date::from_calendar_date(9999, Month::December, 31).expect("year 9999"),
                Time::MIDNIGHT,
            ),
            now,
        )
    } else {
        (
            step_forwards(now, &time.unit, time.value),
            now - Duration::days(7 * 26),
        )
    };
    let generation_method = if dynamic {
        if group_by.is_some() {
            "mapping-compute"
        } else {
            "search-compute"
        }
    } else {
        "search"
    };
    let series_id = uuid::Uuid::new_v4().to_string();
    let series_pk: i32 = sqlx::query_scalar(
        r#"
INSERT INTO insight_series (series_id, query, created_at, oldest_historical_at,
                            last_recorded_at, next_recording_after, last_snapshot_at,
                            next_snapshot_after, repositories, sample_interval_unit,
                            sample_interval_value, generated_from_capture_groups, just_in_time,
                            generation_method, group_by, needs_migration, repository_criteria)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::time_unit, $11, $12, false, $13, $14, false,
        $15)
RETURNING id
"#,
    )
    .bind(&series_id)
    .bind(&series.query)
    .bind(now)
    .bind(oldest_historical_at)
    .bind(zero_time())
    .bind(next_recording_after)
    .bind(zero_time())
    .bind(next_snapshot(now))
    .bind(&repositories)
    .bind(&time.unit)
    .bind(time.value)
    .bind(dynamic)
    .bind(generation_method)
    .bind(&group_by)
    .bind(&repository_criteria)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(MappedSeries {
        series_pk,
        series_id,
        query: series.query.clone(),
        label: series.label.clone(),
        stroke: series.stroke.clone(),
        repositories,
        sample_interval_unit: time.unit.clone(),
        sample_interval_value: time.value,
        generated_from_capture_groups: dynamic,
        just_in_time: false,
        generation_method: generation_method.to_owned(),
        group_by,
        repository_criteria,
    })
}

/// `createAndAttachSeries` (`insight_view_resolvers.go:1313`).
async fn create_and_attach_series(
    conn: &mut sqlx::PgConnection,
    view_id: i32,
    series: &SeriesMutationInput,
) -> Result<(), String> {
    if series.group_by.is_some() || series.generated_from_capture_groups.is_some() {
        sg_insights_query::compute::parse(&series.query)
            .map_err(|err| format!("query validation: compute.Parse: {err}"))?;
    } else {
        sg_insights_query::parse_query(&series.query, "literal")
            .map_err(|err| format!("query validation: {err}"))?;
    }

    let scope = series.repository_scope.as_ref().expect("validated scope");
    let mut found = None;
    if scope.repositories.is_empty() && scope.repository_criteria.is_none() {
        found = find_matching_series(conn, series).await?;
    }
    let (series_pk, series_id) = match found {
        Some(series) => (series.series_pk, series.series_id),
        None => {
            let created = create_series(conn, series).await?;
            (created.series_pk, created.series_id)
        }
    };
    let (label, stroke) = series_fill_metadata(series);
    attach_series_to_view(
        conn,
        series_pk,
        &series_id,
        view_id,
        label.as_deref().unwrap_or(""),
        stroke.as_deref().unwrap_or(""),
    )
    .await
}

async fn add_views_to_dashboard(
    conn: &mut sqlx::PgConnection,
    dashboard_id: i32,
    view_unique_id: &str,
) -> Result<(), String> {
    sqlx::query(
        r#"
INSERT INTO dashboard_insight_view (dashboard_id, insight_view_id)
SELECT $1, insight_view.id FROM insight_view WHERE unique_id = $2
ON CONFLICT DO NOTHING
"#,
    )
    .bind(dashboard_id)
    .bind(view_unique_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

async fn validate_dashboard_permissions(
    conn: &mut sqlx::PgConnection,
    dashboard_ids: &[i64],
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<(), String> {
    if dashboard_ids.is_empty() {
        return Ok(());
    }
    let ids: Vec<i32> = dashboard_ids.iter().map(|id| *id as i32).collect();
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(DISTINCT db.id)
FROM dashboard db
WHERE db.id = ANY($1)
  AND db.id IN (
      SELECT dashboard_id FROM dashboard_grants
      WHERE org_id = ANY($2) OR user_id = ANY($3) OR global IS TRUE)
"#,
    )
    .bind(&ids)
    .bind(org_ids)
    .bind(user_ids)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    if count == dashboard_ids.len() as i64 {
        Ok(())
    } else {
        Err("missing dashboard permission".to_owned())
    }
}

/// `createInsightLicenseCheck`.
async fn license_check(
    conn: &mut sqlx::PgConnection,
    licensed: bool,
    dashboard_ids: &[i64],
) -> Result<Option<i64>, String> {
    if licensed {
        return Ok(None);
    }
    let global_unfrozen: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(DISTINCT iv.id) FROM insight_view iv
JOIN dashboard_insight_view d ON iv.id = d.insight_view_id
JOIN dashboard_grants dg ON d.dashboard_id = dg.dashboard_id
WHERE iv.is_frozen = FALSE AND dg.global = TRUE
"#,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    if global_unfrozen >= 2 {
        return Err(
            "Cannot create more than 2 global insights in Limited Access Mode.".to_owned(),
        );
    }
    if !dashboard_ids.is_empty() {
        let ids: Vec<i32> = dashboard_ids.iter().map(|id| *id as i32).collect();
        let rows = sqlx::query(
            r#"
SELECT db.id, BOOL_OR(dg.global IS TRUE) AS global_grant
FROM dashboard db
JOIN dashboard_grants dg ON db.id = dg.dashboard_id
WHERE db.deleted_at IS NULL AND db.id = ANY($1)
GROUP BY db.id
"#,
        )
        .bind(&ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| err.to_string())?;
        for row in rows {
            let global: bool = row.try_get("global_grant").map_err(|err| err.to_string())?;
            if !global {
                return Err(
                    "Cannot create an insight on a non-global dashboard in Limited Access Mode."
                        .to_owned(),
                );
            }
        }
    }
    let existing: Option<i32> =
        sqlx::query_scalar("SELECT id FROM dashboard WHERE type = 'limited_access_mode' LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    let id = match existing {
        Some(id) => id,
        None => {
            let id: i32 = sqlx::query_scalar(
                "INSERT INTO dashboard (title, save, type) VALUES ('Limited Access Mode Dashboard', true, 'limited_access_mode') RETURNING id",
            )
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                "INSERT INTO dashboard_grants (dashboard_id, user_id, org_id, global) VALUES ($1, NULL, NULL, true)",
            )
            .bind(id)
            .execute(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            id
        }
    };
    sqlx::query("UPDATE dashboard SET deleted_at = NULL WHERE id = $1")
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(|err| err.to_string())?;
    Ok(Some(i64::from(id)))
}

async fn apply_dashboards(
    conn: &mut sqlx::PgConnection,
    dashboards_provided: bool,
    dashboard_ids: &[i64],
    view_unique_id: &str,
    user_ids: &[i32],
    org_ids: &[i32],
) -> Result<(), String> {
    if dashboard_ids.is_empty() {
        return Ok(());
    }
    if dashboards_provided {
        validate_dashboard_permissions(conn, dashboard_ids, user_ids, org_ids).await?;
    }
    for dashboard_id in dashboard_ids {
        add_views_to_dashboard(conn, *dashboard_id as i32, view_unique_id).await?;
    }
    Ok(())
}

impl InsightsWriteDatabase for PgInsightsStore {
    fn code_insights_licensed(&self) -> bool {
        self.is_code_insights_licensed()
    }

    fn actor_is_site_admin(&self, actor: Actor) -> Result<bool, String> {
        if actor.is_internal() {
            return Ok(true);
        }
        let uid = actor.uid().unwrap_or(0);
        let pool = self.frontend_pool.as_ref().unwrap_or(&self.pool);
        let site_admin: Option<bool> =
            block_on(async { sqlx::query_scalar("SELECT site_admin FROM users WHERE id = $1")
                .bind(uid)
                .fetch_optional(pool)
                .await })
            .map_err(|err| err.to_string())?;
        match site_admin {
            None => Err("not authenticated".to_owned()),
            Some(true) => Ok(true),
            Some(false) => Err("must be site admin".to_owned()),
        }
    }

    fn repositories_exist(&self, names: &[String]) -> Result<bool, String> {
        if names.is_empty() {
            return Ok(true);
        }
        let pool = self.frontend_pool.as_ref().unwrap_or(&self.pool);
        let found: Vec<String> = block_on(async {
            sqlx::query_scalar("SELECT name::text FROM repo WHERE name = ANY($1) AND deleted_at IS NULL")
                .bind(names)
                .fetch_all(pool)
                .await
        })
        .map_err(|err| err.to_string())?;
        Ok(found.len() == names.iter().collect::<std::collections::BTreeSet<_>>().len()
            && names.iter().all(|name| found.contains(name)))
    }

    fn create_line_chart(
        &self,
        actor: Actor,
        input: &LineChartMutationInput,
    ) -> Result<String, String> {
        let pool = self.pool.clone();
        let licensed = self.is_code_insights_licensed();
        let uid = actor.uid().unwrap_or(0);
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
            let conn = &mut *tx;
            let mut dashboard_ids = input.dashboards.clone().unwrap_or_default();
            if let Some(lam) = license_check(conn, licensed, &dashboard_ids)
                .await
                .map_err(|err| format!("createInsightLicenseCheck: {err}"))?
            {
                dashboard_ids.push(lam);
            }
            let unique_id = uuid::Uuid::new_v4().to_string();
            let view_id = create_view(
                conn,
                &input.title,
                &unique_id,
                &input.filters,
                None,
                "LINE",
            )
            .await?;
            add_view_grant(conn, view_id, uid).await?;
            for series in &input.series {
                create_and_attach_series(conn, view_id, series).await?;
            }
            apply_dashboards(
                conn,
                input.dashboards.is_some(),
                &dashboard_ids,
                &unique_id,
                &user_ids,
                &org_ids,
            )
            .await?;
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(unique_id)
        })
    }

    fn update_line_chart(
        &self,
        actor: Actor,
        unique_id: &str,
        input: &LineChartMutationInput,
    ) -> Result<String, String> {
        let pool = self.pool.clone();
        let unique_id = unique_id.to_owned();
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
            let conn = &mut *tx;
            if !visible_view_exists(conn, &unique_id, &user_ids, &org_ids).await? {
                return Err("insight not found".to_owned());
            }
            let Some((view_id, existing)) = get_mapped(conn, &unique_id).await? else {
                return Err("No insight view found with this id".to_owned());
            };
            update_view(
                conn,
                &unique_id,
                &input.title,
                &input.filters,
                None,
                "LINE",
                input.sort_mode.as_deref(),
                input.sort_direction.as_deref(),
                input.limit,
                input.num_samples,
            )
            .await?;

            let capture_group = input
                .series
                .iter()
                .any(|series| series.generated_from_capture_groups == Some(true));
            if capture_group {
                update_capture_group_insight(conn, view_id, &input.series[0], &existing).await?;
            } else {
                update_search_or_compute_insight(conn, view_id, &input.series, &existing).await?;
            }
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(unique_id)
        })
    }

    fn create_pie_chart(
        &self,
        actor: Actor,
        input: &PieChartCreateInput,
    ) -> Result<String, String> {
        let pool = self.pool.clone();
        let licensed = self.is_code_insights_licensed();
        let uid = actor.uid().unwrap_or(0);
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
            let conn = &mut *tx;
            let mut dashboard_ids = input.dashboards.clone().unwrap_or_default();
            if let Some(lam) = license_check(conn, licensed, &dashboard_ids)
                .await
                .map_err(|err| format!("createInsightLicenseCheck: {err}"))?
            {
                dashboard_ids.push(lam);
            }
            let unique_id = uuid::Uuid::new_v4().to_string();
            let filters = InsightViewFilters::default();
            let view_id = create_view(
                conn,
                &input.title,
                &unique_id,
                &filters,
                Some(input.other_threshold),
                "PIE",
            )
            .await?;
            add_view_grant(conn, view_id, uid).await?;

            let series_id = uuid::Uuid::new_v4().to_string();
            let now = now_primitive();
            let series_pk: i32 = sqlx::query_scalar(
                r#"
INSERT INTO insight_series (series_id, query, created_at, oldest_historical_at,
                            last_recorded_at, next_recording_after, last_snapshot_at,
                            next_snapshot_after, repositories, sample_interval_unit,
                            sample_interval_value, generated_from_capture_groups, just_in_time,
                            generation_method, group_by, needs_migration, repository_criteria)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'MONTH'::time_unit, 0, false, $10,
        'language-stats', NULL, false, NULL)
RETURNING id
"#,
            )
            .bind(&series_id)
            .bind(&input.query)
            .bind(now)
            .bind(now - Duration::days(7 * 26))
            .bind(zero_time())
            .bind(now)
            .bind(zero_time())
            .bind(next_snapshot(now))
            .bind(&input.repositories)
            .bind(!input.repositories.is_empty())
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            attach_series_to_view(conn, series_pk, &series_id, view_id, "", "").await?;

            apply_dashboards(
                conn,
                input.dashboards.is_some(),
                &dashboard_ids,
                &unique_id,
                &user_ids,
                &org_ids,
            )
            .await?;
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(unique_id)
        })
    }

    fn update_pie_chart(
        &self,
        actor: Actor,
        unique_id: &str,
        input: &PieChartUpdateInput,
    ) -> Result<String, String> {
        let pool = self.pool.clone();
        let unique_id = unique_id.to_owned();
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
            let conn = &mut *tx;
            if !visible_view_exists(conn, &unique_id, &user_ids, &org_ids).await? {
                return Err("insight not found".to_owned());
            }
            let Some((_view_id, existing)) = get_mapped(conn, &unique_id).await? else {
                return Err("No insight view found with this id".to_owned());
            };
            if existing.is_empty() {
                return Err(
                    "No matching series found for this view. The view data may be corrupted."
                        .to_owned(),
                );
            }
            let filters = InsightViewFilters::default();
            update_view(
                conn,
                &unique_id,
                &input.title,
                &filters,
                Some(input.other_threshold),
                "PIE",
                None,
                None,
                None,
                None,
            )
            .await?;
            // BUG parity: `UpdateFrontendSeriesArgs` leaves `StepIntervalValue`
            // zero and does not update `repository_criteria`.
            sqlx::query(
                r#"
UPDATE insight_series
SET query = $1, repositories = $2, sample_interval_unit = 'MONTH'::time_unit,
    sample_interval_value = 0, group_by = NULL
WHERE series_id = $3
"#,
            )
            .bind(&input.query)
            .bind(&input.repositories)
            .bind(&existing[0].series_id)
            .execute(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(unique_id)
        })
    }

    fn delete_insight_view(&self, actor: Actor, unique_id: &str) -> Result<(), String> {
        let pool = self.pool.clone();
        let unique_id = unique_id.to_owned();
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut connection = pool.acquire().await.map_err(|err| err.to_string())?;
            let conn = &mut *connection;
            if !visible_view_exists(conn, &unique_id, &user_ids, &org_ids).await? {
                return Err("insight not found".to_owned());
            }
            let Some((view_id, series)) = get_mapped(conn, &unique_id).await? else {
                return Err("Insight not found.".to_owned());
            };
            // `len(insights) != 1`: `GetMapped` returns at most one view row set.
            let _ = view_id;
            if series.is_empty() {
                return Err("Insight not found.".to_owned());
            }
            for item in &series {
                remove_series_from_view(conn, &item.series_id, view_id).await?;
            }
            sqlx::query("DELETE FROM insight_view WHERE unique_id = $1")
                .bind(&unique_id)
                .execute(&mut *conn)
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    }

    fn save_insight_as_new_view(
        &self,
        actor: Actor,
        input: &SaveAsNewViewInput,
    ) -> Result<String, String> {
        let pool = self.pool.clone();
        let licensed = self.is_code_insights_licensed();
        let uid = actor.uid().unwrap_or(0);
        let unique_id = input.unique_id.clone();
        let input = input.clone();
        let (user_ids, org_ids) = self.user_permission_ids(actor)?;
        block_on(async move {
            let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
            let conn = &mut *tx;
            if !visible_view_exists(conn, &unique_id, &user_ids, &org_ids).await? {
                return Err("insight not found".to_owned());
            }
            let mut dashboard_ids: Vec<i64> = input.dashboard.into_iter().collect();
            if let Some(lam) = license_check(conn, licensed, &dashboard_ids)
                .await
                .map_err(|err| format!("createInsightLicenseCheck: {err}"))?
            {
                dashboard_ids.push(lam);
            }
            let Some((_view_id, existing)) = get_mapped(conn, &unique_id).await? else {
                return Err("No insight view found with this id".to_owned());
            };
            let new_unique_id = uuid::Uuid::new_v4().to_string();
            let filters = input.filters.unwrap_or_default();
            let view_id = create_view(
                conn,
                &input.title,
                &new_unique_id,
                &filters,
                None,
                "LINE",
            )
            .await?;
            add_view_grant(conn, view_id, uid).await?;
            for series in &existing {
                attach_series_to_view(
                    conn,
                    series.series_pk,
                    &series.series_id,
                    view_id,
                    &series.label,
                    &series.stroke,
                )
                .await?;
            }
            apply_dashboards(
                conn,
                input.dashboard.is_some(),
                &dashboard_ids,
                &new_unique_id,
                &user_ids,
                &org_ids,
            )
            .await?;
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(new_unique_id)
        })
    }

    fn update_insight_series(
        &self,
        actor: Actor,
        series_id: &str,
        enabled: Option<bool>,
    ) -> Result<InsightSeriesMetadataData, String> {
        self.actor_is_site_admin(actor)?;
        let pool = self.pool.clone();
        let series_id = series_id.to_owned();
        block_on(async move {
            let mut connection = pool.acquire().await.map_err(|err| err.to_string())?;
            let conn = &mut *connection;
            if let Some(enabled) = enabled {
                set_series_enabled(conn, &series_id, enabled).await?;
            }
            let row = sqlx::query(
                r#"
SELECT series_id, query, (CASE WHEN deleted_at IS NULL THEN TRUE ELSE FALSE END) AS enabled
FROM insight_series
WHERE series_id = $1
"#,
            )
            .bind(&series_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            let Some(row) = row else {
                return Err(format!("unable to fetch series with series_id: {series_id}"));
            };
            Ok(InsightSeriesMetadataData {
                series_id: row.try_get("series_id").map_err(|err| err.to_string())?,
                query: row.try_get("query").map_err(|err| err.to_string())?,
                enabled: row.try_get("enabled").map_err(|err| err.to_string())?,
            })
        })
    }

    fn insight_series_query_status(
        &self,
        actor: Actor,
    ) -> Result<Vec<InsightSeriesQueryStatusData>, String> {
        self.actor_is_site_admin(actor)?;
        let pool = self.pool.clone();
        let queue_pool = self.frontend_pool.clone().unwrap_or_else(|| pool.clone());
        block_on(async move {
            let mut connection = pool.acquire().await.map_err(|err| err.to_string())?;
            let conn = &mut *connection;
            let statuses = sqlx::query(
                r#"
SELECT series_id,
       SUM(CASE WHEN state = 'errored' THEN 1 ELSE 0 END) AS errored,
       SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) AS processing,
       SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN state = 'queued' THEN 1 ELSE 0 END) AS queued
FROM insights_query_runner_jobs
GROUP BY series_id
ORDER BY series_id
"#,
            )
            .fetch_all(&queue_pool)
            .await
            .map_err(|err| err.to_string())?;
            let metadata = sqlx::query(
                r#"
SELECT series_id, query, (CASE WHEN deleted_at IS NULL THEN TRUE ELSE FALSE END) AS enabled
FROM insight_series
"#,
            )
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
            let mut by_id: std::collections::HashMap<String, (String, bool)> =
                std::collections::HashMap::new();
            for row in &metadata {
                by_id.insert(
                    row.try_get("series_id").map_err(|err| err.to_string())?,
                    (
                        row.try_get("query").map_err(|err| err.to_string())?,
                        row.try_get("enabled").map_err(|err| err.to_string())?,
                    ),
                );
            }
            let mut result = Vec::with_capacity(statuses.len());
            for row in &statuses {
                let series_id: String = row.try_get("series_id").map_err(|err| err.to_string())?;
                let (query, enabled) = by_id
                    .get(&series_id)
                    .cloned()
                    .unwrap_or_else(|| (String::new(), false));
                result.push(InsightSeriesQueryStatusData {
                    series_id,
                    query,
                    enabled,
                    errored: row.try_get::<i64, _>("errored").unwrap_or(0) as i32,
                    completed: row.try_get::<i64, _>("completed").unwrap_or(0) as i32,
                    processing: row.try_get::<i64, _>("processing").unwrap_or(0) as i32,
                    failed: row.try_get::<i64, _>("failed").unwrap_or(0) as i32,
                    queued: row.try_get::<i64, _>("queued").unwrap_or(0) as i32,
                });
            }
            Ok(result)
        })
    }
}

async fn update_capture_group_insight(
    conn: &mut sqlx::PgConnection,
    view_id: i32,
    input: &SeriesMutationInput,
    existing: &[MappedSeries],
) -> Result<(), String> {
    if existing.is_empty() {
        create_and_attach_series(conn, view_id, input).await
    } else if input.has_changed(&existing[0].data()) {
        remove_series_from_view(conn, &existing[0].series_id, view_id).await?;
        create_and_attach_series(conn, view_id, input).await
    } else {
        update_view_series_metadata(conn, &existing[0].series_id, view_id, &input.label, &input.stroke)
            .await
    }
}

async fn update_search_or_compute_insight(
    conn: &mut sqlx::PgConnection,
    view_id: i32,
    input: &[SeriesMutationInput],
    existing: &[MappedSeries],
) -> Result<(), String> {
    let input_ids: std::collections::BTreeSet<&str> = input
        .iter()
        .filter_map(|series| series.series_id.as_deref())
        .collect();
    let mut by_id: std::collections::HashMap<&str, &MappedSeries> =
        std::collections::HashMap::new();
    for old in existing {
        if input_ids.contains(old.series_id.as_str()) {
            by_id.insert(old.series_id.as_str(), old);
        } else {
            remove_series_from_view(conn, &old.series_id, view_id).await?;
        }
    }
    for series in input {
        match series.series_id.as_deref() {
            None => create_and_attach_series(conn, view_id, series).await?,
            Some(id) => match by_id.get(id) {
                Some(old) if series.has_changed(&old.data()) => {
                    remove_series_from_view(conn, id, view_id).await?;
                    create_and_attach_series(conn, view_id, series).await?;
                }
                Some(_) => {
                    update_view_series_metadata(conn, id, view_id, &series.label, &series.stroke)
                        .await?;
                }
                None => create_and_attach_series(conn, view_id, series).await?,
            },
        }
    }
    Ok(())
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

#[cfg(test)]
mod license_seam_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://sourcegraph@127.0.0.1:1/sourcegraph")
            .expect("lazy pool")
    }

    #[test]
    fn default_is_unlicensed_like_the_free_plan() {
        // `licensing.Check(FeatureCodeInsights)` against an empty license key
        // resolves the free-1 plan, which does not include Code Insights.
        assert!(!PgInsightsStore::new(pool()).code_insights_licensed());
        assert!(PgInsightsStore::new(pool()).limited_access_mode());
    }

    #[test]
    fn explicit_seams_drive_both_views() {
        let licensed = PgInsightsStore::new(pool()).with_code_insights_licensed(true);
        assert!(licensed.code_insights_licensed());
        assert!(!licensed.limited_access_mode());

        let limited = PgInsightsStore::new(pool()).with_limited_access_mode(true);
        assert!(!limited.code_insights_licensed());
        assert!(limited.limited_access_mode());

        let fixed = PgInsightsStore::new(pool())
            .with_license_provider(Arc::new(FixedCodeInsightsLicense(true)));
        assert!(fixed.code_insights_licensed());
    }

    #[test]
    fn check_code_insights_produces_the_expected_answers() {
        // The seam delegates to the licensing port; empty/unparseable keys are
        // Limited Access Mode, an invalidated license is an error too.
        assert!(sg_licensing::check_code_insights("", true).is_err());
        assert!(sg_licensing::check_code_insights("not a key", true).is_err());
        assert!(sg_licensing::check_code_insights("", false).is_err());
    }
}
