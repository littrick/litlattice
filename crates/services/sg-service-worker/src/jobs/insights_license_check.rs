#![forbid(unsafe_code)]

//! `insights-job` license monitor (`internal/insights/background/license_check.go`).
//!
//! `NewLicenseCheckJob` runs every 15 minutes. When a Code Insights license is
//! present it unfreezes every insight view. Without a license it enforces the
//! limited access mode: insights are frozen unless at most two *global*
//! insights (views on a globally granted dashboard) are unfrozen; the two
//! surviving global insights are attached to the `limited_access_mode`
//! dashboard (created and globally granted on demand, restored if deleted).
//!
//! The licensing check itself is injected (`licensing.Check(
//! licensing.FeatureCodeInsights)` in the baseline) so the routine can be
//! driven deterministically; the real licensing port can be wired at
//! assembly time. All reads/writes use the existing codeinsights tables. No
//! schema changes.

use std::time::Duration;

use sqlx::postgres::PgPool;

/// `NewLicenseCheckJob`'s `goroutine.WithName`.
pub const LICENSE_CHECK_NAME: &str = "insights.license_check";
/// `NewLicenseCheckJob`'s `goroutine.WithDescription`.
pub const LICENSE_CHECK_DESCRIPTION: &str =
    "checks for code insights license and freezes insights when missing";
/// `NewLicenseCheckJob`'s `goroutine.WithInterval` (15 minutes).
pub const LICENSE_CHECK_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// `store.LimitedAccessMode` dashboard type.
pub const LIMITED_ACCESS_MODE: &str = "limited_access_mode";
/// The title used by `EnsureLimitedAccessModeDashboard`.
pub const LIMITED_ACCESS_MODE_TITLE: &str = "Limited Access Mode Dashboard";

/// `checkAndEnforceLicense`, with the license check injected.
pub struct LicenseCheck {
    insights: PgPool,
    /// `licensing.Check(licensing.FeatureCodeInsights)`: `Ok(())` when the
    /// instance is licensed for Code Insights.
    check_license: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
}

impl LicenseCheck {
    pub fn new(
        insights: PgPool,
        check_license: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            insights,
            check_license: Box::new(check_license),
        }
    }

    /// One periodic tick.
    pub async fn run(&self) -> Result<(), String> {
        self.check_and_enforce_license().await
    }

    /// `checkAndEnforceLicense`: one transaction over all view/dashboard
    /// mutations (the baseline nests the dashboard store inside the insight
    /// store transaction, which is equivalent here).
    pub async fn check_and_enforce_license(&self) -> Result<(), String> {
        let mut tx = self.insights.begin().await.map_err(|err| err.to_string())?;

        let license = (self.check_license)();
        if license.is_ok() {
            sqlx::query("UPDATE insight_view SET is_frozen = FALSE")
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("UnfreezeAllInsights: {err}"))?;
            tx.commit().await.map_err(|err| err.to_string())?;
            return Ok(());
        }

        tracing::info!(
            error = license.err().unwrap_or_default(),
            "No license found for Code Insights. Freezing insights for limited access mode"
        );

        let (global_unfrozen, total_unfrozen): (i64, i64) = sqlx::query_as(
            "SELECT unfrozenGlobal.total as unfrozenGlobal, unfrozenTotal.total as unfrozenTotal FROM ( \
                 SELECT COUNT(DISTINCT(iv.id)) as total from insight_view as iv \
                 JOIN dashboard_insight_view as d on iv.id = d.insight_view_id \
                 JOIN dashboard_grants as dg on d.dashboard_id = dg.dashboard_id \
                 WHERE iv.is_frozen = FALSE AND dg.global = TRUE \
             ) as unfrozenGlobal \
             CROSS JOIN \
             ( \
                 SELECT COUNT(DISTINCT(iv.id)) as total from insight_view as iv \
                 WHERE iv.is_frozen = FALSE \
             ) as unfrozenTotal;",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| format!("GetUnfrozenInsightCount: {err}"))?;

        // Insights need to be frozen if:
        // - more than 2 global insights are unfrozen
        // - any other insights are unfrozen
        let should_freeze = global_unfrozen > 2 || total_unfrozen != global_unfrozen;
        if should_freeze {
            sqlx::query("UPDATE insight_view SET is_frozen = TRUE")
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("FreezeAllInsights: {err}"))?;
            sqlx::query(
                "UPDATE insight_view SET is_frozen = FALSE \
                 WHERE id IN ( \
                     SELECT DISTINCT(iv.id) from insight_view as iv \
                     JOIN dashboard_insight_view as d on iv.id = d.insight_view_id \
                     JOIN dashboard_grants as dg on d.dashboard_id = dg.dashboard_id \
                     WHERE dg.global = TRUE \
                     ORDER BY iv.id ASC \
                     LIMIT 2 \
                 )",
            )
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("UnfreezeGlobalInsights: {err}"))?;

            // Attach the unfrozen insights to the limited access mode
            // dashboard.
            let dashboard_id = ensure_limited_access_mode_dashboard(&mut tx)
                .await
                .map_err(|err| format!("EnsureLimitedAccessModeDashboard: {err}"))?;
            let unique_ids: Vec<String> =
                sqlx::query_scalar("SELECT unique_id FROM insight_view WHERE is_frozen = FALSE;")
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|err| format!("GetUnfrozenInsightIds: {err}"))?;
            add_views_to_dashboard(&mut tx, dashboard_id, &unique_ids)
                .await
                .map_err(|err| format!("AddViewsToDashboard: {err}"))?;
        }

        tx.commit().await.map_err(|err| err.to_string())
    }
}

/// `EnsureLimitedAccessModeDashboard`: find or create the limited access mode
/// dashboard, restore it if deleted, and return its id. Grants a global access
/// grant only when the dashboard is first created.
async fn ensure_limited_access_mode_dashboard(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i32, String> {
    let id: Option<i32> = sqlx::query_scalar("SELECT id FROM dashboard WHERE type = $1 LIMIT 1")
        .bind(LIMITED_ACCESS_MODE)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| err.to_string())?;

    let id = match id {
        Some(id) => id,
        None => {
            let id: i32 = sqlx::query_scalar(
                "INSERT INTO dashboard (title, save, type) VALUES ($1, $2, $3) RETURNING id;",
            )
            .bind(LIMITED_ACCESS_MODE_TITLE)
            .bind(true)
            .bind(LIMITED_ACCESS_MODE)
            .fetch_one(&mut **tx)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                "INSERT INTO dashboard_grants (dashboard_id, user_id, org_id, global) \
                 VALUES ($1, NULL, NULL, TRUE);",
            )
            .bind(id)
            .execute(&mut **tx)
            .await
            .map_err(|err| err.to_string())?;
            id
        }
    };

    // The baseline calls `tx.RestoreDashboard(ctx, id)` but discards its
    // result (and then checks the stale `err`, which is nil), so restore
    // failures are swallowed. Mirror that.
    let _ = sqlx::query("UPDATE dashboard SET deleted_at = NULL WHERE id = $1;")
        .bind(id)
        .execute(&mut **tx)
        .await;

    Ok(id)
}

/// `AddViewsToDashboard`: insert `dashboard_insight_view` rows for the given
/// view unique ids, preserving the input order and ignoring conflicts.
async fn add_views_to_dashboard(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dashboard_id: i32,
    view_ids: &[String],
) -> Result<(), String> {
    if dashboard_id == 0 {
        return Err("unable to associate views to dashboard invalid dashboard ID".to_owned());
    }
    if view_ids.is_empty() {
        return Ok(());
    }

    // Rows for an inline table preserve the ordering of the view ids.
    let mut values = String::new();
    for index in 0..view_ids.len() {
        if index > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("(${}, ${})", 2 + index * 2, 3 + index * 2));
    }
    let any_placeholder = 2 + view_ids.len() * 2;
    let query = format!(
        "INSERT INTO dashboard_insight_view (dashboard_id, insight_view_id) ( \
             SELECT $1 AS dashboard_id, insight_view.id AS insight_view_id \
             FROM insight_view \
             JOIN ( VALUES {values}) as ids (id, ordering) \
             ON ids.id = insight_view.unique_id \
             WHERE unique_id = ANY(${any_placeholder}) \
             ORDER BY ids.ordering \
         ) ON CONFLICT DO NOTHING;"
    );

    let mut statement = sqlx::query(&query).bind(dashboard_id);
    for (index, view_id) in view_ids.iter().enumerate() {
        statement = statement.bind(view_id).bind(index as i64);
    }
    statement
        .bind(view_ids.to_vec())
        .execute(&mut **tx)
        .await
        .map_err(|err| err.to_string())?;

    Ok(())
}
