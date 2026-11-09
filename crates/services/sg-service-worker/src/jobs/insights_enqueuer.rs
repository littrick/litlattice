#![forbid(unsafe_code)]

//! `insights-job` enqueuers (`internal/insights/background`):
//! `insight_enqueuer.go` and `retention_enqueuer.go`.
//!
//! `InsightEnqueuer::discover_and_enqueue_insights` is the hourly routine
//! that turns due `insight_series` rows into `insights_query_runner_jobs`
//! (frontend database) and stamps the series (codeinsights database):
//!
//! 1. the *recording* pass selects series with `next_recording_after < now`
//!    and enqueues a `record` job per series, then stamps
//!    `last_recorded_at` / `next_recording_after`;
//! 2. the *snapshot* pass selects series with `next_snapshot_after < now` and
//!    enqueues a `snapshot` job, then stamps `last_snapshot_at` /
//!    `next_snapshot_after`.
//!
//! Both passes exclude just-in-time series. Jobs are deduplicated by
//! `series_id` within a pass (the table also has a unique index on it). The
//! queue insert commits before the stamp, so a crash between the two yields
//! at-least-once semantics, exactly like the baseline.
//!
//! `RetentionEnqueuer` is the 12-hour routine that queues every (non-JIT)
//! series for the `insights-data-retention-job` worker, which already lives
//! in [`crate::jobs::insights_data_retention`].
//!
//! Storage helpers are in
//! [`crate::jobs::insights_query_runner::insights_store`] (codeinsights) and
//! [`crate::jobs::insights_query_runner::store`] (frontend queue). No schema
//! changes.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

use sg_insights_query::{
    code_insights_query_defaults, compute_insight_command_query_with_map_type, global_query,
    make_query_with_repo_filters, multi_repo_query,
};

use crate::jobs::insights_data_retention;
use crate::jobs::insights_query_runner::insights_store::{
    get_data_series_with_args, stamp_recording, stamp_snapshot, GetDataSeriesArgs, PersistMode,
    Series,
};
use crate::jobs::insights_query_runner::store::{enqueue_job, NewJob};

/// `newInsightEnqueuer`'s `goroutine.WithName`.
pub const ENQUEUER_NAME: &str = "insights.enqueuer";
/// `newInsightEnqueuer`'s `goroutine.WithDescription`.
pub const ENQUEUER_DESCRIPTION: &str = "enqueues snapshot and current recording query jobs";
/// `newInsightEnqueuer`'s `goroutine.WithInterval`.
pub const ENQUEUER_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// `newRetentionEnqueuer`'s `goroutine.WithName`.
pub const RETENTION_ENQUEUER_NAME: &str = "insights.retention.enqueuer";
/// `newRetentionEnqueuer`'s `goroutine.WithDescription`.
pub const RETENTION_ENQUEUER_DESCRIPTION: &str = "enqueues series retention jobs";
/// `newRetentionEnqueuer`'s `goroutine.WithInterval`.
pub const RETENTION_ENQUEUER_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);

/// `int(priority.High)`.
pub const PRIORITY_HIGH: i32 = 10;
/// `int(priority.Indexed)`.
pub const COST_INDEXED: i32 = 500;

/// The queue write injected into [`InsightEnqueuer`] (the baseline's
/// `enqueueQueryRunnerJob` field), so tests can script failures.
#[async_trait]
pub trait JobEnqueuer: Send + Sync {
    async fn enqueue(&self, job: &NewJob) -> Result<i32, String>;
}

/// The real `enqueueQueryRunnerJob` closure: `queryrunner.EnqueueJob` against
/// the frontend database.
pub struct PgJobEnqueuer {
    pool: PgPool,
}

impl PgJobEnqueuer {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl JobEnqueuer for PgJobEnqueuer {
    async fn enqueue(&self, job: &NewJob) -> Result<i32, String> {
        enqueue_job(&self.pool, job).await
    }
}

/// Which stamp to apply after a job is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stamp {
    Recording,
    Snapshot,
}

impl Stamp {
    fn persist_mode(self) -> PersistMode {
        match self {
            Self::Recording => PersistMode::Record,
            Self::Snapshot => PersistMode::Snapshot,
        }
    }
}

/// `NewInsightEnqueuer`.
pub struct InsightEnqueuer<E>
where
    E: JobEnqueuer,
{
    /// The enqueuer clock (`NewInsightEnqueuer`'s `now`): selects due series.
    now: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
    /// `InsightStore.Now`, used by `StampRecording`/`StampSnapshot`. The
    /// baseline store keeps its own clock (default `time.Now`), which the
    /// harness overrides independently from the enqueuer clock.
    store_now: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
    enqueuer: E,
}

impl<E> InsightEnqueuer<E>
where
    E: JobEnqueuer,
{
    pub fn new(
        now: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
        store_now: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
        enqueuer: E,
    ) -> Self {
        Self {
            now: Box::new(now),
            store_now: Box::new(store_now),
            enqueuer,
        }
    }

    /// `discoverAndEnqueueInsights`.
    pub async fn discover_and_enqueue_insights(
        &self,
        insights_pool: &PgPool,
    ) -> Result<(), String> {
        let now = (self.now)();
        let recording_series = get_data_series_with_args(
            insights_pool,
            &GetDataSeriesArgs {
                next_recording_before: Some(now),
                exclude_just_in_time: true,
                ..GetDataSeriesArgs::default()
            },
        )
        .await
        .map_err(|err| {
            format!("indexed insight recorder: unable to fetch series for recordings: {err}")
        })?;
        let mut errors = self
            .enqueue(insights_pool, &recording_series, Stamp::Recording)
            .await;

        let snapshot_series = get_data_series_with_args(
            insights_pool,
            &GetDataSeriesArgs {
                next_snapshot_before: Some(now),
                exclude_just_in_time: true,
                ..GetDataSeriesArgs::default()
            },
        )
        .await
        .map_err(|err| {
            format!("indexed insight recorder: unable to fetch series for snapshots: {err}")
        })?;
        errors.extend(
            self.enqueue(insights_pool, &snapshot_series, Stamp::Snapshot)
                .await,
        );

        render_errors(&errors).map_or(Ok(()), Err)
    }

    /// `Enqueue`: deduplicate by `series_id`, keeping the first occurrence.
    pub async fn enqueue(
        &self,
        insights_pool: &PgPool,
        data_series: &[Series],
        stamp: Stamp,
    ) -> Vec<String> {
        let mut seen: Vec<&str> = Vec::with_capacity(data_series.len());
        let mut errors = Vec::new();
        for series in data_series {
            if seen.contains(&series.series_id.as_str()) {
                continue;
            }
            seen.push(series.series_id.as_str());
            if let Err(err) = self.enqueue_single(insights_pool, series, stamp).await {
                errors.push(err);
            }
        }
        errors
    }

    /// `EnqueueSingle`.
    pub async fn enqueue_single(
        &self,
        insights_pool: &PgPool,
        series: &Series,
        stamp: Stamp,
    ) -> Result<(), String> {
        let series_id = series.series_id.as_str();
        let default_params = code_insights_query_defaults(series.repositories.is_empty());
        let basic_query = series.query.as_str();

        let modified_query = if let Some(criteria) = series.repository_criteria.as_deref() {
            let criteria_defaults = code_insights_query_defaults(true);
            make_query_with_repo_filters(criteria, basic_query, true, &criteria_defaults.0)
        } else if !series.repositories.is_empty() {
            multi_repo_query(basic_query, &series.repositories, &default_params)
        } else {
            global_query(basic_query, &default_params)
        }
        .map_err(|err| format!("GlobalQuery series_id:{series_id}: {err}"))?;

        let final_query = match series.group_by.as_deref() {
            Some(group_by) => {
                compute_insight_command_query_with_map_type(&modified_query, group_by).map_err(
                    |err| format!("ComputeInsightCommandQuery series_id:{series_id}: {err}"),
                )?
            }
            None => modified_query,
        };

        let job = NewJob {
            series_id: series.series_id.clone(),
            search_query: final_query,
            record_time: None,
            persist_mode: stamp.persist_mode().as_str().to_owned(),
            dependent_frames: Vec::new(),
            state: "queued".to_owned(),
            process_after: None,
            cost: COST_INDEXED,
            priority: PRIORITY_HIGH,
        };
        self.enqueuer
            .enqueue(&job)
            .await
            .map_err(|err| format!("failed to enqueue insight series_id: {series_id}: {err}"))?;

        // The timestamp update cannot be transactional with the queue insert
        // (separate databases), so a failed stamp is reported after the job is
        // already queued — at-least-once semantics, like the baseline.
        match stamp {
            Stamp::Recording => stamp_recording(insights_pool, (self.store_now)(), series).await,
            Stamp::Snapshot => stamp_snapshot(insights_pool, (self.store_now)(), series).await,
        }
        .map_err(|err| format!("failed to stamp insight series_id: {series_id}: {err}"))
    }
}

/// `newRetentionEnqueuer`'s handler body: queue every non-JIT series for the
/// retention worker (codeinsights database).
pub struct RetentionEnqueuer {
    pool: PgPool,
}

impl RetentionEnqueuer {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue_all(&self) -> Result<(), String> {
        let all_series = get_data_series_with_args(
            &self.pool,
            &GetDataSeriesArgs {
                exclude_just_in_time: true,
                ..GetDataSeriesArgs::default()
            },
        )
        .await
        .map_err(|err| format!("unable to fetch series for retention: {err}"))?;

        let mut errors = Vec::new();
        for series in &all_series {
            if let Err(err) =
                insights_data_retention::enqueue_job(&self.pool, series.id, &series.series_id).await
            {
                errors.push(format!("seriesID: {}: {err}", series.id));
            }
        }
        render_errors(&errors).map_or(Ok(()), Err)
    }
}

/// `lib/errors.MultiError` rendering: one error is printed verbatim, several
/// use the cockroach form `N errors occurred:\n\t* a\n\t* b`.
pub fn render_errors(errors: &[String]) -> Option<String> {
    match errors {
        [] => None,
        [single] => Some(single.clone()),
        many => {
            let mut rendered = format!("{} errors occurred:", many.len());
            for error in many {
                rendered.push_str("\n\t* ");
                rendered.push_str(error);
            }
            Some(rendered)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_error_rendering_matches_the_baseline() {
        assert_eq!(render_errors(&[]), None);
        assert_eq!(render_errors(&["boom".to_owned()]), Some("boom".to_owned()));
        assert_eq!(
            render_errors(&["a".to_owned(), "b".to_owned()]),
            Some("2 errors occurred:\n\t* a\n\t* b".to_owned())
        );
    }

    #[test]
    fn routine_metadata_matches_the_baseline() {
        assert_eq!(ENQUEUER_NAME, "insights.enqueuer");
        assert_eq!(
            ENQUEUER_DESCRIPTION,
            "enqueues snapshot and current recording query jobs"
        );
        assert_eq!(ENQUEUER_INTERVAL, Duration::from_secs(3600));
        assert_eq!(RETENTION_ENQUEUER_NAME, "insights.retention.enqueuer");
        assert_eq!(
            RETENTION_ENQUEUER_DESCRIPTION,
            "enqueues series retention jobs"
        );
        assert_eq!(RETENTION_ENQUEUER_INTERVAL, Duration::from_secs(12 * 3600));
        assert_eq!(PRIORITY_HIGH, 10);
        assert_eq!(COST_INDEXED, 500);
    }
}
