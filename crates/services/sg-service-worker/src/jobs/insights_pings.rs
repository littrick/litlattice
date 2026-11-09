#![forbid(unsafe_code)]

//! `insights-job` pings emitter (`internal/insights/background/pings`):
//! `insights_ping_emitter.go` + `insights_ping_aggregators.go`.
//!
//! Every hour the emitter runs eight aggregations against the **codeinsights**
//! database and records one `INSIGHT_*` event per aggregation in the
//! **frontend** `event_logs` table (actor `backend`, source `BACKEND`, user 0,
//! `public_argument = {}`), exactly like `InsightsPingEmitter.SaveEvent`.
//! Failures are logged and skipped — `emit` itself never fails — so one broken
//! aggregation does not hide the others.
//!
//! The baseline iterates a `map[string]emitter`, i.e. in random order; the
//! Rust port uses a fixed order and only guarantees the same set of emitters
//! runs. `regroupIntervalCounts` likewise iterates a Go map; this port returns
//! days ascending (callers must not depend on the order).
//!
//! No schema changes: the aggregations read the existing codeinsights tables
//! and the only write is the existing `event_logs` insert shared with the
//! telemetry recorder ([`sg_db::telemetry_events::bulk_insert_event_logs`]).

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use sg_db::telemetry_events::{bulk_insert_event_logs, EventLogRow};
use sg_insights::types::IntervalUnit;

/// `NewInsightsPingEmitterJob`'s `goroutine.WithName`.
pub const PINGS_EMITTER_NAME: &str = "insights.pings_emitter";
/// `NewInsightsPingEmitterJob`'s `goroutine.WithDescription`.
pub const PINGS_EMITTER_DESCRIPTION: &str = "emits enterprise telemetry pings";
/// `NewInsightsPingEmitterJob`'s interval.
pub const PINGS_EMITTER_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// `usagestats.InsightsTotalCountPingName`.
pub const INSIGHT_TOTAL_COUNTS: &str = "INSIGHT_TOTAL_COUNTS";
/// `usagestats.InsightsTotalCountCriticalPingName`.
pub const INSIGHT_TOTAL_COUNT_CRITICAL: &str = "INSIGHT_TOTAL_COUNT_CRITICAL";
/// `usagestats.InsightsIntervalCountsPingName`.
pub const INSIGHT_TIME_INTERVALS: &str = "INSIGHT_TIME_INTERVALS";
/// `usagestats.InsightsOrgVisibleInsightsPingName`.
pub const INSIGHT_ORG_VISIBLE_INSIGHTS: &str = "INSIGHT_ORG_VISIBLE_INSIGHTS";
/// `usagestats.InsightsTotalOrgsWithDashboardPingName`.
pub const INSIGHT_TOTAL_ORGS_WITH_DASHBOARD: &str = "INSIGHT_TOTAL_ORGS_WITH_DASHBOARD";
/// `usagestats.InsightsDashboardTotalCountPingName`.
pub const INSIGHT_DASHBOARD_TOTAL_COUNT: &str = "INSIGHT_DASHBOARD_TOTAL_COUNT";
/// `usagestats.InsightsPerDashboardPingName` (the baseline typo is preserved).
pub const INSIGHTS_PER_DASHBOARD: &str = "INSIGHTS_PER_DASHBORD_STATS";
/// `usagestats.InsightsBackfillTimePingName`.
pub const INSIGHTS_BACKFILL_TIME: &str = "INSIGHTS_BACKFILL_TIME";

/// `types.InsightViewsCountPing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightViewsCountPing {
    #[serde(rename = "ViewType")]
    pub view_type: String,
    #[serde(rename = "TotalCount")]
    pub total_count: i32,
}

/// `types.InsightSeriesCountPing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightSeriesCountPing {
    #[serde(rename = "GenerationType")]
    pub generation_type: String,
    #[serde(rename = "TotalCount")]
    pub total_count: i32,
}

/// `types.InsightViewSeriesCountPing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightViewSeriesCountPing {
    #[serde(rename = "ViewType")]
    pub view_type: String,
    #[serde(rename = "GenerationType")]
    pub generation_type: String,
    #[serde(rename = "TotalCount")]
    pub total_count: i32,
}

/// `types.InsightTotalCounts`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightTotalCounts {
    #[serde(rename = "ViewCounts")]
    pub view_counts: Vec<InsightViewsCountPing>,
    #[serde(rename = "SeriesCounts")]
    pub series_counts: Vec<InsightSeriesCountPing>,
    #[serde(rename = "ViewSeriesCounts")]
    pub view_series_counts: Vec<InsightViewSeriesCountPing>,
}

/// `types.InsightTimeIntervalPing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightTimeIntervalPing {
    #[serde(rename = "IntervalDays")]
    pub interval_days: i32,
    #[serde(rename = "TotalCount")]
    pub total_count: i32,
}

/// `types.OrgVisibleInsightPing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrgVisibleInsightPing {
    #[serde(rename = "Type")]
    pub kind: String,
    #[serde(rename = "TotalCount")]
    pub total_count: i32,
}

/// `types.InsightsBackfillTimePing`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsightsBackfillTimePing {
    #[serde(rename = "AllRepos")]
    pub all_repos: bool,
    #[serde(rename = "P99Seconds")]
    pub p99_seconds: i32,
    #[serde(rename = "P90Seconds")]
    pub p90_seconds: i32,
    #[serde(rename = "P50Seconds")]
    pub p50_seconds: i32,
    #[serde(rename = "Count")]
    pub count: i32,
}

/// `types.CodeInsightsCriticalTelemetry`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CodeInsightsCriticalTelemetry {
    #[serde(rename = "TotalInsights")]
    pub total_insights: i32,
}

/// One pending emitter: a boxed future borrowing the emitter.
type EmitterFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// One failed emitter: the baseline logs `errors.Wrap(err, name).Error()` and
/// keeps going.
#[derive(Debug, Clone, PartialEq)]
pub struct EmitterFailure {
    /// The map key in the baseline (`emitInsightTotalCounts`, …).
    pub emitter: &'static str,
    /// `"<emitter>: <database error>"`.
    pub message: String,
}

/// `pingSeriesType` from `insights_ping_aggregators.go`.
pub const PING_SERIES_TYPE: &str = r"CONCAT(
   CASE WHEN ((generation_method = 'search' or generation_method = 'search-compute') and generated_from_capture_groups) THEN 'capture-groups' ELSE generation_method END,
    '::',
   CASE WHEN (repositories IS NOT NULL AND cardinality(repositories) > 0) THEN 'scoped' WHEN repository_criteria IS NOT NULL THEN 'repo-search' ELSE 'global' END,
    '::',
   CASE WHEN (just_in_time = true) THEN 'jit' ELSE 'recorded' END
    ) as ping_series_type";

/// `insightViewSeriesTotalCountQuery` with `pingSeriesType` interpolation.
pub fn insight_view_series_total_count_query() -> String {
    format!(
        r"SELECT presentation_type::text AS presentation_type,
       {PING_SERIES_TYPE},
       COUNT(*)
FROM insight_series
         JOIN insight_view_series ivs ON insight_series.id = ivs.insight_series_id
         JOIN insight_view iv ON ivs.insight_view_id = iv.id
WHERE deleted_at IS NULL
GROUP BY presentation_type, ping_series_type;"
    )
}

/// `insightSeriesTotalCountQuery` with `pingSeriesType` interpolation.
pub fn insight_series_total_count_query() -> String {
    format!(
        r"SELECT {PING_SERIES_TYPE},
       COUNT(*)
FROM insight_series
WHERE deleted_at IS NULL
GROUP BY ping_series_type;"
    )
}

/// `insightViewTotalCountQuery`.
pub const INSIGHT_VIEW_TOTAL_COUNT_QUERY: &str = r"SELECT presentation_type::text AS presentation_type, COUNT(*)
FROM insight_view
GROUP BY presentation_type;";

/// `insightIntervalCountsQuery`.
pub const INSIGHT_INTERVAL_COUNTS_QUERY: &str = r"SELECT COUNT(DISTINCT(ivs.insight_view_id)), series.sample_interval_value, series.sample_interval_unit::text AS sample_interval_unit FROM insight_series AS series
JOIN insight_view_series AS ivs ON series.id = ivs.insight_series_id
WHERE series.sample_interval_value != 0
	AND series.sample_interval_value IS NOT NULL
	AND series.sample_interval_unit IS NOT NULL
GROUP BY series.sample_interval_value, series.sample_interval_unit;";

/// `orgVisibleInsightCountsQuery`.
pub const ORG_VISIBLE_INSIGHT_COUNTS_QUERY: &str = r"SELECT iv.presentation_type::text AS presentation_type, COUNT(iv.presentation_type) FROM insight_view AS iv
JOIN insight_view_grants AS ivg ON iv.id = ivg.insight_view_id
WHERE ivg.org_id IS NOT NULL
GROUP BY iv.presentation_type;";

/// `totalOrgsWithDashboardsQuery`.
pub const TOTAL_ORGS_WITH_DASHBOARDS_QUERY: &str =
    "SELECT COUNT(DISTINCT(org_id)) FROM dashboard_grants WHERE org_id IS NOT NULL;";

/// `totalDashboardsQuery`.
pub const TOTAL_DASHBOARDS_QUERY: &str = "SELECT COUNT(*) FROM dashboard WHERE deleted_at IS NULL;";

/// `insightsPerDashboardQuery`. The baseline scans `numeric` into `float32`;
/// the casts to `float8` keep the same value and let sqlx decode it.
pub const INSIGHTS_PER_DASHBOARD_QUERY: &str = r#"SELECT
	COALESCE(AVG(count), 0)::float8 AS average,
	COALESCE(MIN(count), 0)::int AS min,
	COALESCE(MAX(count), 0)::int AS max,
	COALESCE(STDDEV(count), 0)::float8 AS stddev,
	COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY count), 0)::float8 AS median FROM
	(
		SELECT DISTINCT(dashboard_id), COUNT(insight_view_id) FROM dashboard_insight_view GROUP BY dashboard_id
	) counts;"#;

/// `insightsCriticalCountQuery`.
pub const INSIGHTS_CRITICAL_COUNT_QUERY: &str =
    "SELECT COUNT(*) FROM insight_view WHERE is_frozen = false";

/// `backfillTimeQuery`; `$1` is the baseline's `time.Now()`.
pub const BACKFILL_TIME_QUERY: &str = r"WITH recent_backfills as (
	SELECT
		isb.series_id,
		SUM(runtime_duration)/1000000000 duration_seconds
	FROM insight_series_backfill isb
	  JOIN repo_iterator ri on isb.repo_iterator_id = ri.id
	WHERE isb.state = 'completed'
		AND ri.completed_at > date_trunc('week', $1::date)
	GROUP BY isb.series_id
)
SELECT
	COUNT(*),
	ROUND(COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP( ORDER BY duration_seconds), '0'))::INT AS p99_seconds,
	ROUND(COALESCE(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_seconds), '0'))::INT AS p90_seconds,
	ROUND(COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds), '0'))::INT AS p50_seconds
FROM recent_backfills;";

/// `getDays`: interval length in days, `0` for units finer than a day and for
/// units the baseline does not map (`YEAR` and anything unknown).
pub fn get_days(interval_value: i32, interval_unit: &IntervalUnit) -> i32 {
    match interval_unit {
        IntervalUnit::Month => interval_value * 30,
        IntervalUnit::Week => interval_value * 7,
        IntervalUnit::Day => interval_value,
        IntervalUnit::Hour => 0,
        _ => 0,
    }
}

/// `regroupIntervalCounts`: sum counts that resolve to the same number of
/// days. Go returns them in map iteration order; this port returns days
/// ascending.
pub fn regroup_interval_counts(
    from_groups: &[InsightTimeIntervalPing],
) -> Vec<InsightTimeIntervalPing> {
    let mut group_by_days: BTreeMap<i32, i32> = BTreeMap::new();
    for group in from_groups {
        *group_by_days.entry(group.interval_days).or_insert(0) += group.total_count;
    }
    group_by_days
        .into_iter()
        .map(|(interval_days, total_count)| InsightTimeIntervalPing {
            interval_days,
            total_count,
        })
        .collect()
}

/// `InsightsPingEmitter`.
pub struct InsightsPingEmitter {
    frontend: PgPool,
    insights: PgPool,
    /// `time.Now()` in the baseline (event timestamp and the backfill window).
    now: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

impl InsightsPingEmitter {
    pub fn new(
        frontend: PgPool,
        insights: PgPool,
        now: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        Self {
            frontend,
            insights,
            now: Box::new(now),
        }
    }

    /// `InsightsPingEmitter.emit`: run every emitter, collect the failures.
    /// The baseline logs and swallows them.
    pub async fn emit(&self) -> Vec<EmitterFailure> {
        let emitters: [(&'static str, EmitterFuture<'_>); 8] = [
            (
                "emitInsightTotalCounts",
                Box::pin(self.emit_insight_total_counts()),
            ),
            ("emitIntervalCounts", Box::pin(self.emit_interval_counts())),
            (
                "emitOrgVisibleInsightCounts",
                Box::pin(self.emit_org_visible_insight_counts()),
            ),
            (
                "emitTotalOrgsWithDashboard",
                Box::pin(self.emit_total_orgs_with_dashboard()),
            ),
            (
                "emitTotalDashboards",
                Box::pin(self.emit_total_dashboards()),
            ),
            (
                "emitInsightsPerDashboard",
                Box::pin(self.emit_insights_per_dashboard()),
            ),
            ("emitBackfillTime", Box::pin(self.emit_backfill_time())),
            (
                "emitTotalCountCritical",
                Box::pin(self.emit_total_count_critical()),
            ),
        ];
        let mut failures = Vec::new();
        for (emitter, result) in emitters {
            match result.await {
                Ok(()) => {}
                Err(err) => failures.push(EmitterFailure {
                    emitter,
                    message: format!("{emitter}: {err}"),
                }),
            }
        }
        failures
    }

    /// `emitInsightTotalCounts`.
    async fn emit_insight_total_counts(&self) -> Result<(), String> {
        let view_counts = self
            .get_total_count_by_view_type()
            .await
            .map_err(|err| format!("GetTotalCountByViewType: {err}"))?;
        let series_counts = self
            .get_total_count_by_series_type()
            .await
            .map_err(|err| format!("GetTotalCountBySeriesType: {err}"))?;
        let view_series_counts = self
            .get_total_count_by_view_series_type()
            .await
            .map_err(|err| format!("GetTotalCountByViewSeriesType: {err}"))?;
        let counts = InsightTotalCounts {
            view_counts,
            series_counts,
            view_series_counts,
        };
        let argument = serde_json::to_value(&counts).map_err(|err| format!("Marshal: {err}"))?;
        self.save_event(INSIGHT_TOTAL_COUNTS, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitTotalCountCritical`.
    async fn emit_total_count_critical(&self) -> Result<(), String> {
        let count = self
            .get_total_count_critical()
            .await
            .map_err(|err| format!("GetTotalCountCritical: {err}"))?;
        let argument = serde_json::to_value(CodeInsightsCriticalTelemetry {
            total_insights: count,
        })
        .map_err(|err| format!("Marshal: {err}"))?;
        self.save_event(INSIGHT_TOTAL_COUNT_CRITICAL, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitIntervalCounts`.
    async fn emit_interval_counts(&self) -> Result<(), String> {
        let counts = self
            .get_interval_counts()
            .await
            .map_err(|err| format!("GetIntervalCounts: {err}"))?;
        let argument = serde_json::to_value(&counts).map_err(|err| format!("Marshal: {err}"))?;
        self.save_event(INSIGHT_TIME_INTERVALS, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitOrgVisibleInsightCounts`.
    async fn emit_org_visible_insight_counts(&self) -> Result<(), String> {
        let counts = self
            .get_org_visible_insight_counts()
            .await
            .map_err(|err| format!("GetOrgVisibleInsightCounts: {err}"))?;
        let argument = serde_json::to_value(&counts).map_err(|err| format!("Marshal: {err}"))?;
        self.save_event(INSIGHT_ORG_VISIBLE_INSIGHTS, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitTotalOrgsWithDashboard`.
    async fn emit_total_orgs_with_dashboard(&self) -> Result<(), String> {
        let counts = self
            .get_total_orgs_with_dashboard()
            .await
            .map_err(|err| format!("GetTotalOrgsWithDashboard: {err}"))?;
        let argument = json!(counts);
        self.save_event(INSIGHT_TOTAL_ORGS_WITH_DASHBOARD, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitTotalDashboards`.
    async fn emit_total_dashboards(&self) -> Result<(), String> {
        let counts = self
            .get_total_dashboards()
            .await
            .map_err(|err| format!("GetTotalDashboards: {err}"))?;
        let argument = json!(counts);
        self.save_event(INSIGHT_DASHBOARD_TOTAL_COUNT, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitInsightsPerDashboard`.
    async fn emit_insights_per_dashboard(&self) -> Result<(), String> {
        let stats = self
            .get_insights_per_dashboard()
            .await
            .map_err(|err| format!("GetInsightsPerDashboard: {err}"))?;
        // `json.Marshal` prints a `float32` in its shortest form (`2`, `1.5`);
        // build the object by hand so integral values do not become `2.0`.
        let argument = json!({
            "Avg": go_float(stats.avg),
            "Max": stats.max,
            "Min": stats.min,
            "StdDev": go_float(stats.std_dev),
            "Median": go_float(stats.median),
        });
        self.save_event(INSIGHTS_PER_DASHBOARD, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `emitBackfillTime`.
    async fn emit_backfill_time(&self) -> Result<(), String> {
        let counts = self
            .get_backfill_time()
            .await
            .map_err(|err| format!("GetBackfillTime: {err}"))?;
        let argument = serde_json::to_value(&counts).map_err(|err| format!("Marshal: {err}"))?;
        self.save_event(INSIGHTS_BACKFILL_TIME, argument)
            .await
            .map_err(|err| format!("SaveEvent: {err}"))
    }

    /// `SaveEvent`: one `event_logs` row with the legacy backend actor
    /// (`user_id 0`, `anonymous_user_id "backend"`, source `BACKEND`,
    /// `public_argument {}`).
    async fn save_event(&self, name: &str, argument: Value) -> Result<(), String> {
        let row = EventLogRow {
            name: name.to_owned(),
            url: String::new(),
            user_id: 0,
            anonymous_user_id: "backend".to_owned(),
            source: "BACKEND".to_owned(),
            argument,
            public_argument: json!({}),
            version: sg_metrics::VERSION.to_owned(),
            timestamp: (self.now)(),
            feature_flags: Value::Null,
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: uuid::Uuid::new_v4().to_string(),
            insert_id: uuid::Uuid::new_v4().to_string(),
            client: None,
            billing_product_category: None,
            billing_event_id: None,
        };
        bulk_insert_event_logs(&self.frontend, &[row])
            .await
            .map_err(|err| err.to_string())
    }

    /// `GetTotalCountByViewType`.
    pub async fn get_total_count_by_view_type(&self) -> Result<Vec<InsightViewsCountPing>, String> {
        let rows = sqlx::query(INSIGHT_VIEW_TOTAL_COUNT_QUERY)
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(InsightViewsCountPing {
                view_type: row
                    .try_get("presentation_type")
                    .map_err(|e| e.to_string())?,
                total_count: count_i32(&row)?,
            });
        }
        Ok(results)
    }

    /// `GetTotalCountCritical`.
    pub async fn get_total_count_critical(&self) -> Result<i32, String> {
        let row = sqlx::query(INSIGHTS_CRITICAL_COUNT_QUERY)
            .fetch_one(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        count_i32(&row)
    }

    /// `GetTotalCountByViewSeriesType`.
    pub async fn get_total_count_by_view_series_type(
        &self,
    ) -> Result<Vec<InsightViewSeriesCountPing>, String> {
        let rows = sqlx::query(&insight_view_series_total_count_query())
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(InsightViewSeriesCountPing {
                view_type: row
                    .try_get("presentation_type")
                    .map_err(|e| e.to_string())?,
                generation_type: row.try_get("ping_series_type").map_err(|e| e.to_string())?,
                total_count: count_i32(&row)?,
            });
        }
        Ok(results)
    }

    /// `GetTotalCountBySeriesType`.
    pub async fn get_total_count_by_series_type(
        &self,
    ) -> Result<Vec<InsightSeriesCountPing>, String> {
        let rows = sqlx::query(&insight_series_total_count_query())
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(InsightSeriesCountPing {
                generation_type: row.try_get("ping_series_type").map_err(|e| e.to_string())?,
                total_count: count_i32(&row)?,
            });
        }
        Ok(results)
    }

    /// `GetIntervalCounts`.
    pub async fn get_interval_counts(&self) -> Result<Vec<InsightTimeIntervalPing>, String> {
        let rows = sqlx::query(INSIGHT_INTERVAL_COUNTS_QUERY)
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let count = count_i32(&row)?;
            let interval_value: i32 = row
                .try_get("sample_interval_value")
                .map_err(|e| e.to_string())?;
            let unit: String = row
                .try_get("sample_interval_unit")
                .map_err(|e| e.to_string())?;
            let unit = IntervalUnit::from_str_value(&unit);
            results.push(InsightTimeIntervalPing {
                interval_days: get_days(interval_value, &unit),
                total_count: count,
            });
        }
        Ok(regroup_interval_counts(&results))
    }

    /// `GetOrgVisibleInsightCounts`.
    pub async fn get_org_visible_insight_counts(
        &self,
    ) -> Result<Vec<OrgVisibleInsightPing>, String> {
        let rows = sqlx::query(ORG_VISIBLE_INSIGHT_COUNTS_QUERY)
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let presentation_type: String = row
                .try_get("presentation_type")
                .map_err(|e| e.to_string())?;
            results.push(OrgVisibleInsightPing {
                kind: if presentation_type == "LINE" {
                    "search".to_owned()
                } else {
                    "lang-stats".to_owned()
                },
                total_count: count_i32(&row)?,
            });
        }
        Ok(results)
    }

    /// `GetTotalOrgsWithDashboard`.
    pub async fn get_total_orgs_with_dashboard(&self) -> Result<i32, String> {
        let row = sqlx::query(TOTAL_ORGS_WITH_DASHBOARDS_QUERY)
            .fetch_one(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        count_i32(&row)
    }

    /// `GetTotalDashboards`.
    pub async fn get_total_dashboards(&self) -> Result<i32, String> {
        let row = sqlx::query(TOTAL_DASHBOARDS_QUERY)
            .fetch_one(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        count_i32(&row)
    }

    /// `GetInsightsPerDashboard`.
    pub async fn get_insights_per_dashboard(&self) -> Result<InsightsPerDashboardPing, String> {
        let row = sqlx::query(INSIGHTS_PER_DASHBOARD_QUERY)
            .fetch_one(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        Ok(InsightsPerDashboardPing {
            avg: row_f32(&row, "average")?,
            max: row
                .try_get::<i32, _>("max")
                .map_err(|e| format!("scan max: {e}"))?,
            min: row
                .try_get::<i32, _>("min")
                .map_err(|e| format!("scan min: {e}"))?,
            std_dev: row_f32(&row, "stddev")?,
            median: row_f32(&row, "median")?,
        })
    }

    /// `GetBackfillTime`.
    pub async fn get_backfill_time(&self) -> Result<Vec<InsightsBackfillTimePing>, String> {
        let rows = sqlx::query(BACKFILL_TIME_QUERY)
            .bind((self.now)().date())
            .fetch_all(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(InsightsBackfillTimePing {
                all_repos: false,
                p99_seconds: row.try_get("p99_seconds").map_err(|e| e.to_string())?,
                p90_seconds: row.try_get("p90_seconds").map_err(|e| e.to_string())?,
                p50_seconds: row.try_get("p50_seconds").map_err(|e| e.to_string())?,
                count: count_i32(&row)?,
            });
        }
        Ok(results)
    }
}

/// `types.InsightsPerDashboardPing` (`Avg`/`StdDev`/`Median` are `float32` in
/// the baseline; this port keeps the same precision).
#[derive(Debug, Clone, PartialEq)]
pub struct InsightsPerDashboardPing {
    pub avg: f32,
    pub max: i32,
    pub min: i32,
    pub std_dev: f32,
    pub median: f32,
}

fn count_i32(row: &sqlx::postgres::PgRow) -> Result<i32, String> {
    // `COUNT(*)` is `int8` in Postgres; the baseline scans it into `int`.
    let value: i64 = row.try_get("count").map_err(|e| e.to_string())?;
    i32::try_from(value).map_err(|e| e.to_string())
}

fn row_f32(row: &sqlx::postgres::PgRow, column: &str) -> Result<f32, String> {
    let value: f64 = row.try_get(column).map_err(|e| e.to_string())?;
    Ok(value as f32)
}

/// Format a float the way `json.Marshal` prints a `float32`: shortest
/// round-trip form, so `2.0` becomes the JSON number `2`.
fn go_float(value: f32) -> Value {
    serde_json::from_str(&format!("{value}")).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_metadata_matches_the_baseline() {
        assert_eq!(PINGS_EMITTER_NAME, "insights.pings_emitter");
        assert_eq!(
            PINGS_EMITTER_DESCRIPTION,
            "emits enterprise telemetry pings"
        );
        assert_eq!(PINGS_EMITTER_INTERVAL, Duration::from_secs(3600));
        assert_eq!(INSIGHTS_PER_DASHBOARD, "INSIGHTS_PER_DASHBORD_STATS");
    }

    #[test]
    fn get_days_matches_the_baseline() {
        assert_eq!(get_days(1, &IntervalUnit::Month), 30);
        assert_eq!(get_days(2, &IntervalUnit::Week), 14);
        assert_eq!(get_days(7, &IntervalUnit::Day), 7);
        assert_eq!(get_days(1, &IntervalUnit::Hour), 0);
        assert_eq!(get_days(1, &IntervalUnit::Year), 0);
        assert_eq!(get_days(3, &IntervalUnit::from_str_value("FORTNIGHT")), 0);
    }

    #[test]
    fn regroup_sums_matching_day_counts() {
        let groups = vec![
            InsightTimeIntervalPing {
                interval_days: 30,
                total_count: 1,
            },
            InsightTimeIntervalPing {
                interval_days: 14,
                total_count: 2,
            },
            InsightTimeIntervalPing {
                interval_days: 30,
                total_count: 3,
            },
        ];
        assert_eq!(
            regroup_interval_counts(&groups),
            vec![
                InsightTimeIntervalPing {
                    interval_days: 14,
                    total_count: 2,
                },
                InsightTimeIntervalPing {
                    interval_days: 30,
                    total_count: 4,
                },
            ]
        );
    }

    #[test]
    fn go_float_prints_integral_floats_without_a_fraction() {
        assert_eq!(serde_json::to_string(&go_float(2.0)).unwrap(), "2");
        assert_eq!(serde_json::to_string(&go_float(1.5)).unwrap(), "1.5");
    }
}
