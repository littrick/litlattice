#![forbid(unsafe_code)]

//! Port of `newBackfillHandler.Handle` from
//! `internal/insights/scheduler/backfill_state_new_handler.go`.
//!
//! The handler consumes a job from the `insights_jobs_backfill_new` view,
//! computes the repository scope and an approximate cost for the backfill,
//! writes the recording times for the series, moves the backfill to
//! `processing`, enqueues the next job and marks the current job complete.
//!
//! Transaction boundaries mirror the baseline:
//!
//! * the backfill load, `SetScope`, `setState` and `enqueueBackfill` run in one
//!   transaction;
//! * the series read and `SetInsightSeriesRecordingTimes` use separate
//!   connections (the Go handler uses the insight store directly), so they
//!   survive a later rollback exactly like in Go;
//! * `MarkComplete` runs on the worker store outside that transaction (the Go
//!   `workerStore` is built on `insightsDB`, not on the handler's tx).
//!
//! The repository iterator is injected as a [`SeriesRepoSelector`] so the
//! worker can pass the shared [`sg_insights::discovery::SeriesRepoIterator`]
//! and tests can substitute a deterministic one.
//!
//! Error wrapping reproduces the Go prefixes (`loadBackfill`,
//! `GetDataSeriesByID`, `repoIterator.SeriesRepoIterator`,
//! `reposIterator.ForEach`, `parseQuery`, `backfill.SetScope`,
//! `NewBackfillHandler.SetInsightSeriesRecordingTimes`, `backfill.setState`,
//! `backfill.enqueueBackfill`, `backfill.MarkComplete`). `sql.ErrNoRows` is
//! rendered as `sql: no rows in result set` to match the baseline text.

use std::sync::Arc;

use sqlx::postgres::{PgPool, Postgres};
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::{Row, Transaction};

use sg_db::insights_scheduler;
use sg_insights::discovery::{SeriesRepoSelector, SeriesScope};
use sg_insights::priority::{default_query_analyzer, QueryObject};
use sg_insights::scheduler::parse_query;
use sg_insights::timeseries::{build_sample_times, make_recordings_from_times, TimeInterval};
use sg_insights::types::IntervalUnit;

use crate::jobs::insights_backfill_store::{BackfillJobStore, NEW_STORE};
use crate::jobs::insights_query_runner::insights_store;

/// `timeseries.BuildSampleTimes(12, ...)` in the baseline.
pub const SAMPLE_POINTS: i64 = 12;

/// The subset of `types.InsightSeries` the new-state handler reads.
#[derive(Debug, Clone, PartialEq)]
pub struct InsightSeriesRecord {
    pub id: i32,
    pub series_id: String,
    pub query: String,
    /// `insight_series.created_at` is `timestamp without time zone`; the Go
    /// driver scans it as a UTC `time.Time`.
    pub created_at: PrimitiveDateTime,
    pub sample_interval_unit: String,
    pub sample_interval_value: i32,
    pub generated_from_capture_groups: bool,
    pub repositories: Vec<String>,
    pub repository_criteria: Option<String>,
    /// `insight_series.generation_method`: the backfill pipeline's handler
    /// selector (`search`, `search-compute`, `mapping-compute`).
    pub generation_method: String,
}

/// `InsightStore.GetDataSeriesByID` (`deleted_at IS NULL AND id = $1`).
///
/// The Go method returns `series not found` when no row matches and
/// `multiple series match id` if more than one does (impossible with the
/// primary key, but reproduced for parity). It also omits the id predicate
/// when the id is `0` (`if args.ID != 0`), so a zero id matches every live
/// series; this port keeps that quirk.
pub async fn get_data_series_by_id(pool: &PgPool, id: i32) -> Result<InsightSeriesRecord, String> {
    let rows = if id == 0 {
        sqlx::query(
            r#"
SELECT id, series_id, query, created_at,
       sample_interval_unit::text AS sample_interval_unit,
       sample_interval_value, generated_from_capture_groups, repositories,
       repository_criteria, generation_method
FROM insight_series
WHERE deleted_at IS NULL
"#,
        )
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?
    } else {
        sqlx::query(
            r#"
SELECT id, series_id, query, created_at,
       sample_interval_unit::text AS sample_interval_unit,
       sample_interval_value, generated_from_capture_groups, repositories,
       repository_criteria, generation_method
FROM insight_series
WHERE deleted_at IS NULL
  AND id = $1
"#,
        )
        .bind(id)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?
    };

    match rows.len() {
        0 => Err("series not found".to_owned()),
        1 => row_to_series(&rows[0]),
        _ => Err("multiple series match id".to_owned()),
    }
}

fn row_to_series(row: &sqlx::postgres::PgRow) -> Result<InsightSeriesRecord, String> {
    let repositories: Option<Vec<String>> = row
        .try_get("repositories")
        .map_err(|err: sqlx::Error| err.to_string())?;
    Ok(InsightSeriesRecord {
        id: row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        series_id: row
            .try_get("series_id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        query: row
            .try_get("query")
            .map_err(|err: sqlx::Error| err.to_string())?,
        created_at: row
            .try_get("created_at")
            .map_err(|err: sqlx::Error| err.to_string())?,
        sample_interval_unit: row
            .try_get("sample_interval_unit")
            .map_err(|err: sqlx::Error| err.to_string())?,
        sample_interval_value: row
            .try_get("sample_interval_value")
            .map_err(|err: sqlx::Error| err.to_string())?,
        generated_from_capture_groups: row
            .try_get("generated_from_capture_groups")
            .map_err(|err: sqlx::Error| err.to_string())?,
        repositories: repositories.unwrap_or_default(),
        repository_criteria: row
            .try_get("repository_criteria")
            .map_err(|err: sqlx::Error| err.to_string())?,
        generation_method: row
            .try_get("generation_method")
            .map_err(|err: sqlx::Error| err.to_string())?,
    })
}

/// `newBackfillHandler` (the two dependencies the handler drives).
pub struct NewBackfillHandler {
    pool: PgPool,
    repo_iterator: Arc<dyn SeriesRepoSelector>,
}

impl NewBackfillHandler {
    pub fn new(pool: PgPool, repo_iterator: Arc<dyn SeriesRepoSelector>) -> Self {
        Self {
            pool,
            repo_iterator,
        }
    }

    /// `newBackfillHandler.Handle`.
    pub async fn handle_new_backfill(&self, job_id: i32, backfill_id: i32) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|err| sql_error(&err))?;
        match self
            .handle_in_transaction(&mut tx, job_id, backfill_id)
            .await
        {
            Ok(()) => tx.commit().await.map_err(|err| sql_error(&err)),
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }

    async fn handle_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job_id: i32,
        backfill_id: i32,
    ) -> Result<(), String> {
        let backfill = insights_scheduler::load_backfill_in(tx, backfill_id)
            .await
            .map_err(|err| format!("loadBackfill: {}", sql_error(&err)))?;
        let series = get_data_series_by_id(&self.pool, backfill.series_id)
            .await
            .map_err(|err| format!("GetDataSeriesByID: {err}"))?;

        let scope = SeriesScope {
            repositories: series.repositories.clone(),
            repository_criteria: series.repository_criteria.clone(),
        };
        let repos_iterator = self
            .repo_iterator
            .for_series(&scope)
            .await
            .map_err(|err| format!("repoIterator.SeriesRepoIterator: {err}"))?;
        let mut repo_ids: Vec<i32> = Vec::new();
        repos_iterator
            .for_each(&mut |_name, id| {
                repo_ids.push(id as i32);
                Ok(())
            })
            .await
            .map_err(|err| format!("reposIterator.ForEach: {err}"))?;

        let query_plan = parse_query(&series.query, series.generated_from_capture_groups)
            .map_err(|err| format!("parseQuery: {err}"))?;
        let mut query_object = QueryObject::new(query_plan);
        query_object.number_of_repositories = i64::try_from(repo_ids.len()).unwrap_or(0);
        let cost = default_query_analyzer().cost(&mut query_object);

        let backfill = insights_scheduler::set_scope_in(
            tx,
            backfill_id,
            &repo_ids,
            cost,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(|err| format!("backfill.SetScope: {}", sql_error(&err)))?;

        let interval = TimeInterval {
            unit: IntervalUnit::from_str_value(&series.sample_interval_unit),
            value: i64::from(series.sample_interval_value),
        };
        let created_at = truncate_to_minute(series.created_at.assume_utc());
        let sample_times = build_sample_times(SAMPLE_POINTS, &interval, created_at);
        let recordings = make_recordings_from_times(&sample_times, false);
        let times: Vec<insights_store::RecordingTime> = recordings
            .iter()
            .map(|recording| insights_store::RecordingTime {
                time: recording.timestamp,
                snapshot: recording.snapshot,
            })
            .collect();
        {
            // The Go `timeseriesStore` is not the handler's transaction; keep
            // the same autocommit connection semantics.
            let mut conn = self.pool.acquire().await.map_err(|err| sql_error(&err))?;
            insights_store::set_recording_times(&mut conn, series.id, &times)
                .await
                .map_err(|err| {
                    format!("NewBackfillHandler.SetInsightSeriesRecordingTimes: {err}")
                })?;
        }

        insights_scheduler::set_state_in(tx, backfill.id, "processing")
            .await
            .map_err(|err| format!("backfill.setState: {}", sql_error(&err)))?;
        insights_scheduler::enqueue_backfill_in(tx, backfill.id)
            .await
            .map_err(|err| format!("backfill.enqueueBackfill: {}", sql_error(&err)))?;

        let worker_store = BackfillJobStore::new(self.pool.clone(), NEW_STORE);
        worker_store
            .mark_complete(job_id)
            .await
            .map_err(|err| format!("backfill.MarkComplete: {err}"))?;
        Ok(())
    }
}

/// Go's `time.Time.Truncate(time.Minute)`: floor the absolute instant to a
/// whole minute (the value is a UTC wall clock here).
pub(crate) fn truncate_to_minute(time: OffsetDateTime) -> OffsetDateTime {
    let seconds = time.unix_timestamp();
    let floored = seconds - seconds.rem_euclid(60);
    OffsetDateTime::from_unix_timestamp(floored).unwrap_or(time)
}

/// Render driver errors the way the Go baseline logs them.
pub(crate) fn sql_error(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::RowNotFound => "sql: no rows in result set".to_owned(),
        other => other.to_string(),
    }
}
