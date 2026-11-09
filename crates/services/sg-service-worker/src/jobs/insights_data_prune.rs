#![forbid(unsafe_code)]

//! `insights-job` data pruner (`internal/insights/background/data_prune.go`).
//!
//! `NewInsightsDataPrunerJob` runs hourly with a cutoff of `now + interval`
//! (one hour into the future) and hard-deletes every series that was
//! soft-deleted before that cutoff and is no longer linked to any insight
//! view. Per series, in order:
//!
//! 1. `insights_query_runner_jobs` rows are deleted from the frontend queue
//!    (`deleteQueuedRecords`);
//! 2. `insights_data_retention_jobs` rows are deleted from the codeinsights
//!    database (`deleteQueuedRetentionRecords`);
//! 3. a codeinsights transaction deletes `series_points` and
//!    `series_points_snapshots` for the series, then the `insight_series`
//!    row itself (which cascades to recording times, archived points and
//!    archived recording times as well as `insight_series_backfill`).
//!
//! The series definition is deleted last so a partial failure leaves a
//! re-attemptable state; the whole purge stops at the first error. No schema
//! changes.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// `NewInsightsDataPrunerJob`'s `goroutine.WithName`.
pub const DATA_PRUNER_NAME: &str = "insights.data_prune";
/// `NewInsightsDataPrunerJob`'s `goroutine.WithDescription`.
pub const DATA_PRUNER_DESCRIPTION: &str = "deletes series that have been marked as 'deleted'";
/// `NewInsightsDataPrunerJob`'s `goroutine.WithInterval` (60 minutes).
pub const DATA_PRUNER_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// The data pruner routine (`performPurge`).
pub struct DataPruner {
    /// Frontend database (`insights_query_runner_jobs`).
    frontend: PgPool,
    /// Codeinsights database (`insight_series` and friends).
    insights: PgPool,
    now: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

impl DataPruner {
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

    /// One periodic tick: `performPurge(ctx, ..., time.Now().Add(interval))`.
    pub async fn run(&self) -> Result<(), String> {
        self.perform_purge((self.now)() + DATA_PRUNER_INTERVAL)
            .await
    }

    /// `performPurge`: prune every eligible series, stopping at the first
    /// error.
    pub async fn perform_purge(&self, deleted_before: OffsetDateTime) -> Result<(), String> {
        let series_ids = self
            .get_soft_deleted_series(deleted_before)
            .await
            .map_err(|err| format!("GetSoftDeletedSeries: {err}"))?;

        for id in series_ids {
            // Always delete the series definition last so a partial state
            // remains re-attemptable.
            tracing::info!(series_id = %id, "pruning insight series");
            self.delete_queued_records(&id)
                .await
                .map_err(|err| format!("deleteQueuedRecords: {err}"))?;
            self.delete_queued_retention_records(&id)
                .await
                .map_err(|err| format!("deleteQueuedRetentionRecords: {err}"))?;
            self.delete_series_data(&id).await?;
        }

        Ok(())
    }

    /// `InsightStore.GetSoftDeletedSeries`: series soft-deleted before the
    /// cutoff that no view links to. No `ORDER BY`, like the baseline.
    pub async fn get_soft_deleted_series(
        &self,
        deleted_before: OffsetDateTime,
    ) -> Result<Vec<String>, String> {
        let rows = sqlx::query(
            "SELECT series_id \
             FROM insight_series i \
             LEFT JOIN insight_view_series ivs ON i.id = ivs.insight_series_id \
             WHERE i.deleted_at IS NOT NULL \
               AND i.deleted_at < $1::timestamptz \
               AND ivs.insight_series_id IS NULL;",
        )
        .bind(deleted_before)
        .fetch_all(&self.insights)
        .await
        .map_err(|err| err.to_string())?;

        rows.iter()
            .map(|row| {
                sqlx::Row::try_get::<String, _>(row, "series_id").map_err(|err| err.to_string())
            })
            .collect()
    }

    /// `deleteQueuedRecords`: frontend queue rows for the series.
    async fn delete_queued_records(&self, series_id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM insights_query_runner_jobs WHERE series_id = $1;")
            .bind(series_id)
            .execute(&self.frontend)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    /// `deleteQueuedRetentionRecords`: codeinsights retention queue rows.
    async fn delete_queued_retention_records(&self, series_id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM insights_data_retention_jobs WHERE series_id_string = $1;")
            .bind(series_id)
            .execute(&self.insights)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    /// `Store.Delete` + `InsightStore.HardDeleteSeries` in one transaction
    /// (the baseline nests `Delete`'s own transaction inside this one).
    async fn delete_series_data(&self, series_id: &str) -> Result<(), String> {
        let mut tx = self.insights.begin().await.map_err(|err| err.to_string())?;

        sqlx::query("DELETE FROM series_points WHERE series_id = $1;")
            .bind(series_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("Delete: DeleteForSeries: {err}"))?;
        sqlx::query("DELETE FROM series_points_snapshots WHERE series_id = $1;")
            .bind(series_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("Delete: DeleteForSeriesSnapshots: {err}"))?;
        sqlx::query("DELETE FROM insight_series WHERE series_id = $1;")
            .bind(series_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;

        tx.commit().await.map_err(|err| err.to_string())
    }
}
