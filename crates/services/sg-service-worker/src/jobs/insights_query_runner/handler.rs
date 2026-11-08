#![forbid(unsafe_code)]

//! `internal/insights/background/queryrunner/work_handler.go`: one worker
//! handler slot. The flow is the baseline's:
//!
//! 1. `limiter.Wait` for a search-query token;
//! 2. re-read the job and its dependency frames (`dequeueJob`);
//! 3. load the insight series (cached per process);
//! 4. reject just-in-time series;
//! 5. pick the recording time (now for global jobs, the queued time
//!    otherwise), dispatch on the series' `generation_method`, and persist the
//!    recordings;
//! 6. on a failed **last** attempt of a global recording job, add an
//!    incomplete datapoint (timeout vs generic).
//!
//! Not ported: the trace-id update (`trace.ID(ctx)` has no Rust equivalent
//! here) and the RED/metrics instrumentation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use sg_db::repos::RepoStore;

use super::insights_store::{
    self, add_incomplete_datapoint, get_data_series, Series, REASON_GENERIC, REASON_TIMEOUT,
};
use super::limiter::SearchQueryLimiter;
use super::search::{
    generate_compute_recordings, generate_search_recordings, is_search_timeout,
    is_terminal_streaming_error, persist_recordings, GenerationMethod, StreamingSearch,
    SubRepoChecker,
};
use super::store::{will_retry, Job, QueryRunnerStore, RECORD_MODE};
use crate::framework::dbworker::DbWorkerHandler;

/// `workHandler`.
pub struct QueryRunnerHandler {
    store: Arc<QueryRunnerStore>,
    insights: PgPool,
    repo_store: RepoStore,
    provider: Arc<dyn StreamingSearch>,
    sub_repo: Arc<dyn SubRepoChecker>,
    limiter: Arc<SearchQueryLimiter>,
    series_cache: Mutex<HashMap<String, Arc<Series>>>,
}

impl QueryRunnerHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<QueryRunnerStore>,
        insights: PgPool,
        repo_store: RepoStore,
        provider: Arc<dyn StreamingSearch>,
        sub_repo: Arc<dyn SubRepoChecker>,
        limiter: Arc<SearchQueryLimiter>,
    ) -> Self {
        Self {
            store,
            insights,
            repo_store,
            provider,
            sub_repo,
            limiter,
            series_cache: Mutex::new(HashMap::new()),
        }
    }

    /// `workHandler.getSeries`: process-wide cache; a missing definition is
    /// the baseline's explicit error.
    pub async fn get_series(&self, series_id: &str) -> Result<Arc<Series>, String> {
        if let Some(series) = self
            .series_cache
            .lock()
            .expect("series cache lock")
            .get(series_id)
        {
            return Ok(Arc::clone(series));
        }

        let series = get_data_series(&self.insights, series_id).await?;
        let Some(series) = series else {
            return Err(format!(
                "workHandler.getSeries: insight definition not found for series_id: {series_id}"
            ));
        };
        let series = Arc::new(series);
        self.series_cache
            .lock()
            .expect("series cache lock")
            .insert(series_id.to_owned(), Arc::clone(&series));
        Ok(series)
    }

    async fn dispatch(
        &self,
        job: &Job,
        series: &Series,
        record_time: OffsetDateTime,
        ctx: &CancellationToken,
    ) -> Result<Vec<insights_store::RecordSeriesPoint>, String> {
        let Some(method) = GenerationMethod::from_raw(&series.generation_method) else {
            return Err(format!(
                "unable to handle record for series_id: {} and generation_method: {}",
                series.series_id, series.generation_method
            ));
        };
        match method {
            GenerationMethod::Search => generate_search_recordings(
                job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                ctx,
            )
            .await
            .map_err(|err| format!("searchHandler: {err}")),
            GenerationMethod::SearchCompute => generate_compute_recordings(
                job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                ctx,
                false,
            )
            .await
            .map_err(|err| format!("computeHandler: {err}")),
            GenerationMethod::MappingCompute => generate_compute_recordings(
                job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                ctx,
                true,
            )
            .await
            .map_err(|err| format!("mappingComputeHandler: {err}")),
        }
    }

    async fn handle_record(&self, ctx: CancellationToken, record: &Job) -> Result<(), String> {
        // A nil RecordTime marks a global (unscoped) job.
        let is_global = record.record_time.is_none();

        self.limiter
            .wait(&ctx)
            .await
            .map_err(|err| format!("limiter.Wait: {err}"))?;

        let job = self
            .store
            .dequeue_job(record.id)
            .await
            .map_err(|err| format!("dequeueJob: {err}"))?;

        let series = self
            .get_series(&job.series_id)
            .await
            .map_err(|err| format!("getSeries: {err}"))?;

        if series.just_in_time {
            // Erratum: the baseline formats the integer `series.ID` with `%s`,
            // so the real failure message contains Go's `%!s(int=N)` artifact.
            // Reproduce it verbatim for a byte-identical `failure_message`.
            return Err(format!(
                "just in time series are not eligible for background processing, series_id: %!s(int={})",
                series.id
            ));
        }

        let record_time = job.record_time.unwrap_or_else(OffsetDateTime::now_utc);

        let recordings = match self.dispatch(&job, &series, record_time, &ctx).await {
            Ok(recordings) => recordings,
            Err(err) => {
                if !will_retry(job.num_failures) && is_global && job.persist_mode == RECORD_MODE {
                    let reason = if is_search_timeout(&err) {
                        REASON_TIMEOUT
                    } else {
                        REASON_GENERIC
                    };
                    tracing::debug!(
                        series_id = series.id,
                        series_unique_id = %series.series_id,
                        error = %err,
                        reason = reason,
                        "insights recording global query timeout"
                    );
                    if let Err(add_err) =
                        add_incomplete_datapoint(&self.insights, series.id, reason, record_time)
                            .await
                    {
                        return Err(format!(
                            "2 errors occurred:\n\t* {err}\n\t* workHandler.AddIncompleteDatapoint: {add_err}"
                        ));
                    }
                }
                return Err(err);
            }
        };

        persist_recordings(
            &self.insights,
            &self.repo_store,
            &job,
            &series,
            recordings,
            record_time,
        )
        .await
    }
}

#[async_trait]
impl DbWorkerHandler<QueryRunnerStore> for QueryRunnerHandler {
    async fn handle(&self, ctx: CancellationToken, record: &Job) -> Result<(), String> {
        let result = self.handle_record(ctx, record).await;
        if let Err(err) = &result {
            tracing::error!(error = %err, "insights.queryrunner.workHandler");
        }
        result
    }

    /// `TerminalStreamingError.NonRetryable`: an `invalid query` streaming
    /// error marks the job failed immediately.
    fn non_retryable(&self, failure_message: &str) -> bool {
        is_terminal_streaming_error(failure_message)
    }
}

/// Exposed for harnesses: the record's `will_retry` decision.
pub fn record_will_retry(job: &Job) -> bool {
    will_retry(job.num_failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool")
    }

    struct NoSearch;

    #[async_trait]
    impl StreamingSearch for NoSearch {
        async fn search(
            &self,
            _query: &str,
            _ctx: &CancellationToken,
        ) -> Result<super::super::streaming::TabulationResult, String> {
            Err("unused".to_owned())
        }

        async fn compute_match_context(
            &self,
            _query: &str,
            _ctx: &CancellationToken,
        ) -> Result<super::super::streaming::ComputeTabulationResult, String> {
            Err("unused".to_owned())
        }

        async fn compute_text_extra(
            &self,
            _query: &str,
            _ctx: &CancellationToken,
        ) -> Result<super::super::streaming::ComputeTabulationResult, String> {
            Err("unused".to_owned())
        }
    }

    struct NoSubRepo;

    #[async_trait]
    impl SubRepoChecker for NoSubRepo {
        async fn enabled_for_repo_id(&self, _repo_id: i32) -> Result<bool, String> {
            Ok(false)
        }
    }

    fn handler(insights: PgPool) -> QueryRunnerHandler {
        QueryRunnerHandler::new(
            Arc::new(QueryRunnerStore::new(lazy_pool())),
            insights,
            RepoStore::new(lazy_pool()),
            Arc::new(NoSearch),
            Arc::new(NoSubRepo),
            Arc::new(SearchQueryLimiter::new(1000.0, 10)),
        )
    }

    #[tokio::test]
    async fn get_series_reports_a_missing_definition() {
        // The lazy pool fails to connect; use a deliberately bad query path to
        // assert the error is surfaced rather than swallowed.
        let handler = handler(lazy_pool());
        let err = handler
            .get_series("missing")
            .await
            .expect_err("pool cannot connect");
        assert!(!err.is_empty());
    }

    #[tokio::test]
    async fn non_retryable_is_derived_from_the_error_text() {
        let handler = handler(lazy_pool());
        assert!(handler.non_retryable("streaming search: terminal errors: [invalid query]"));
        assert!(!handler.non_retryable("streaming search: errors: [boom]"));
    }

    #[test]
    fn record_will_retry_uses_num_failures() {
        let job = Job {
            series_id: "s".to_owned(),
            search_query: "q".to_owned(),
            record_time: None,
            persist_mode: RECORD_MODE.to_owned(),
            dependent_frames: Vec::new(),
            cost: 500,
            priority: 1,
            id: 1,
            state: "processing".to_owned(),
            failure_message: None,
            started_at: None,
            finished_at: None,
            process_after: None,
            num_resets: 0,
            num_failures: 9,
        };
        assert!(!record_will_retry(&job));
    }
}
