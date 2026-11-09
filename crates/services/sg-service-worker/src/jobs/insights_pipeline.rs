#![forbid(unsafe_code)]

//! `internal/insights/pipeline`: the historical backfill pipeline.
//!
//! A [`Backfiller::run`] executes three steps for one series/repository:
//!
//! 1. **generate_jobs** ([`make_search_jobs_func`]): resolve the repository's
//!    first commit, rate-limit compression, compress the sample times
//!    (usually [`sg_insights::compression::NoopFilter`] or
//!    [`sg_insights::compression::GitserverFilter`]), and turn every
//!    execution into a [`SearchJob`] (repository scoped search query with the
//!    nearest commit as revision). Series whose query already contains a
//!    `repo:` filter, and executions before the repository's first commit,
//!    produce no job.
//! 2. **run_searches** ([`make_run_search_func`]): run every job through the
//!    handler registered for the series' generation method, collecting the
//!    recorded points. Any failure discards all points.
//! 3. **save_results** ([`make_save_results_func`]): persist the points
//!    through the configured [`ResultsStore`].
//!
//! Not ported: the RED metrics and the `src_insights_backfill_searches_per_frame`
//! histogram (observability only), and the baseline's nil `requestContext`
//! and nil `RecordTime` dereferences (the typed port rejects them instead).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Timestamp;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use sg_git::Commit;
use sg_insights::compression::{DataFrameFilter, QueryExecution};
use sg_insights::gitserver::{self, CommitError, GitCommitClient};
use sg_insights_query::{
    code_insights_query_defaults, compute_insight_command_query_with_map_type, contains_field,
    single_repo_query,
};

use crate::jobs::insights_query_runner::insights_store::{RecordSeriesPoint, Series};
use crate::jobs::insights_query_runner::limiter::SearchQueryLimiter;

/// `query.FieldRepo`.
const FIELD_REPO: &str = "repo";

/// `queryrunner.SearchJob`.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchJob {
    pub series_id: String,
    pub search_query: String,
    pub record_time: Option<time::OffsetDateTime>,
    pub persist_mode: String,
    pub dependent_frames: Vec<time::OffsetDateTime>,
}

/// `itypes.MinimalRepo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinimalRepo {
    pub id: i32,
    pub name: String,
}

/// `pipeline.BackfillRequest`.
#[derive(Debug, Clone)]
pub struct BackfillRequest {
    pub series: Series,
    pub repo: MinimalRepo,
    pub sample_times: Vec<time::OffsetDateTime>,
}

/// `requestContext`: the carrier between the three steps.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub backfill_request: BackfillRequest,
}

/// `queryrunner.InsightsHandler`.
#[async_trait]
pub trait InsightsHandler: Send + Sync {
    async fn handle(
        &self,
        job: &SearchJob,
        series: &Series,
        record_time: time::OffsetDateTime,
    ) -> Result<Vec<RecordSeriesPoint>, String>;
}

/// `pipeline.GitCommitClient` (`FirstCommit` / `RecentCommits`).
#[async_trait]
pub trait CommitClient: Send + Sync {
    async fn first_commit(&self, repo: &str) -> Result<Commit, CommitError>;

    async fn recent_commits(
        &self,
        repo: &str,
        target: time::OffsetDateTime,
        revision: &str,
    ) -> Result<Vec<Commit>, CommitError>;
}

#[async_trait]
impl CommitClient for GitCommitClient {
    async fn first_commit(&self, repo: &str) -> Result<Commit, CommitError> {
        GitCommitClient::first_commit(self, repo).await
    }

    async fn recent_commits(
        &self,
        repo: &str,
        target: time::OffsetDateTime,
        revision: &str,
    ) -> Result<Vec<Commit>, CommitError> {
        GitCommitClient::recent_commits(self, repo, target, revision).await
    }
}

/// `store.Interface.RecordSeriesPoints`.
#[async_trait]
pub trait ResultsStore: Send + Sync {
    async fn record_series_points(&self, points: &[RecordSeriesPoint]) -> Result<(), String>;
}

/// `ratelimit.InstrumentedLimiter.Wait`.
#[async_trait]
pub trait RateLimiter: Send + Sync {
    async fn wait(&self, ctx: &CancellationToken) -> Result<(), String>;
}

#[async_trait]
impl RateLimiter for SearchQueryLimiter {
    async fn wait(&self, ctx: &CancellationToken) -> Result<(), String> {
        // `rate.Limiter.WaitN` always observes an already-canceled context,
        // even when a token is immediately available (the `limit == Inf`
        // branch in particular).
        if ctx.is_cancelled() {
            return Err("context canceled".to_owned());
        }
        SearchQueryLimiter::wait(self, ctx).await
    }
}

/// `pipeline.Backfiller`.
#[async_trait]
pub trait Backfiller: Send + Sync {
    async fn run(&self, ctx: &CancellationToken, request: BackfillRequest) -> Result<(), String>;
}

/// `pipeline.SearchJobGenerator`.
#[async_trait]
pub trait SearchJobGenerator: Send + Sync {
    async fn generate(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
    ) -> Result<(RequestContext, Vec<SearchJob>), String>;
}

/// `pipeline.SearchRunner`.
#[async_trait]
pub trait SearchRunner: Send + Sync {
    async fn run(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
        jobs: Vec<SearchJob>,
    ) -> Result<(RequestContext, Vec<RecordSeriesPoint>), String>;
}

/// `pipeline.ResultsPersister`.
#[async_trait]
pub trait ResultsPersister: Send + Sync {
    async fn persist(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
        points: Vec<RecordSeriesPoint>,
    ) -> Result<RequestContext, String>;
}

/// `pipeline.BackfillerConfig`.
pub struct BackfillerConfig {
    pub commit_client: Arc<dyn CommitClient>,
    pub compression_plan: Arc<dyn DataFrameFilter>,
    pub search_handlers: HashMap<String, Arc<dyn InsightsHandler>>,
    pub insight_store: Arc<dyn ResultsStore>,
    pub search_plan_worker_limit: usize,
    pub search_runner_worker_limit: usize,
    pub search_rate_limiter: Arc<dyn RateLimiter>,
    pub historic_rate_limiter: Arc<dyn RateLimiter>,
}

/// `pipeline.NewDefaultBackfiller`.
pub fn new_default_backfiller(config: BackfillerConfig) -> Box<dyn Backfiller> {
    let search_job_generator = make_search_jobs_func(
        config.commit_client,
        config.compression_plan,
        config.search_plan_worker_limit,
        config.historic_rate_limiter,
    );
    let search_runner = make_run_search_func(
        config.search_handlers,
        Some(config.search_runner_worker_limit),
        config.search_rate_limiter,
    );
    let persister = make_save_results_func(config.insight_store);
    Box::new(Backfill {
        search_job_generator,
        search_runner,
        persister,
    })
}

/// `newBackfiller`: the three injected steps (the baseline's clock is only
/// used for metrics).
pub fn new_backfiller(
    search_job_generator: Arc<dyn SearchJobGenerator>,
    search_runner: Arc<dyn SearchRunner>,
    persister: Arc<dyn ResultsPersister>,
) -> Box<dyn Backfiller> {
    Box::new(Backfill {
        search_job_generator,
        search_runner,
        persister,
    })
}

struct Backfill {
    search_job_generator: Arc<dyn SearchJobGenerator>,
    search_runner: Arc<dyn SearchRunner>,
    persister: Arc<dyn ResultsPersister>,
}

#[async_trait]
impl Backfiller for Backfill {
    async fn run(&self, ctx: &CancellationToken, request: BackfillRequest) -> Result<(), String> {
        let starting = RequestContext {
            backfill_request: request,
        };
        let (step1, jobs) = self.search_job_generator.generate(ctx, starting).await?;
        let (step2, points) = self.search_runner.run(ctx, step1, jobs).await?;
        let _ = self.persister.persist(ctx, step2, points).await?;
        Ok(())
    }
}

// `compressionSavingsMetric`/`backfillMetrics` are observability only and are
// not ported.

/// `makeSearchJobsFunc`.
pub fn make_search_jobs_func(
    commit_client: Arc<dyn CommitClient>,
    compression_plan: Arc<dyn DataFrameFilter>,
    search_job_worker_limit: usize,
    rate_limiter: Arc<dyn RateLimiter>,
) -> Arc<dyn SearchJobGenerator> {
    Arc::new(GenerateSearchJobs {
        commit_client,
        compression_plan,
        search_job_worker_limit,
        rate_limiter,
    })
}

struct GenerateSearchJobs {
    commit_client: Arc<dyn CommitClient>,
    compression_plan: Arc<dyn DataFrameFilter>,
    search_job_worker_limit: usize,
    rate_limiter: Arc<dyn RateLimiter>,
}

/// `buildSeriesContext`.
struct BuildSeriesContext {
    execution: QueryExecution,
    /// Carried like the baseline's `buildSeriesContext.id`; nothing reads it.
    #[allow(dead_code)]
    id: i32,
    repo_name: String,
    first_head_commit: Commit,
    series_id: String,
    series: Series,
}

#[async_trait]
impl SearchJobGenerator for GenerateSearchJobs {
    async fn generate(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
    ) -> Result<(RequestContext, Vec<SearchJob>), String> {
        let request = req.backfill_request.clone();
        let first_head_commit = match self.commit_client.first_commit(&request.repo.name).await {
            Ok(commit) => commit,
            Err(err) => {
                if gitserver::is_empty_repo_error(&err) {
                    // The repository is empty: there is no work to be done.
                    return Ok((req, Vec::new()));
                }
                return Err(err.to_string());
            }
        };

        self.rate_limiter.wait(ctx).await?;

        let search_plan = self
            .compression_plan
            .filter(request.sample_times.clone(), &request.repo.name)
            .await;

        let mut executions = search_plan.executions;
        executions.reverse();
        let commit_client = Arc::clone(&self.commit_client);
        let repo_id = request.repo.id;
        let repo_name = request.repo.name.clone();
        let series = request.series.clone();
        let jobs = run_bounded(
            ctx,
            self.search_job_worker_limit,
            executions,
            move |execution, _ctx| {
                let bctx = BuildSeriesContext {
                    execution,
                    id: repo_id,
                    repo_name: repo_name.clone(),
                    first_head_commit: first_head_commit.clone(),
                    series_id: series.series_id.clone(),
                    series: series.clone(),
                };
                let commit_client = Arc::clone(&commit_client);
                async move { make_historical_search_job(commit_client.as_ref(), &bctx).await }
            },
        )
        .await?;
        Ok((req, jobs.into_iter().flatten().collect()))
    }
}

/// `makeHistoricalSearchJobFunc`: one execution → at most one search job.
async fn make_historical_search_job(
    commit_client: &dyn CommitClient,
    bctx: &BuildSeriesContext,
) -> Result<Option<SearchJob>, String> {
    let raw_query = &bctx.series.query;
    let contains_repo = contains_field(raw_query, FIELD_REPO)?;
    if contains_repo {
        // Existing behavior: searches with a repo filter are ignored.
        return Ok(None);
    }

    // If the timeframe starts before the first commit, there are no results.
    // A missing author date is the zero `time.Time`, which no recording time
    // precedes.
    if let Some(first_commit_time) = commit_time(bctx.first_head_commit.author_date) {
        if bctx.execution.recording_time < first_commit_time {
            return Ok(None);
        }
    }

    let mut revision = bctx.execution.revision.clone();
    if revision.is_empty() {
        let recent_commits = match commit_client
            .recent_commits(&bctx.repo_name, bctx.execution.recording_time, "")
            .await
        {
            Ok(commits) => commits,
            Err(err) => {
                if err.is_revision_not_found() || err.is_repo_not_found() {
                    // No error: the repo may not be cloned or pushed yet.
                    return Ok(None);
                }
                return Err(format!("FindNearestCommit: {err}"));
            }
        };
        let Some(nearest_commit) = recent_commits.first() else {
            // The repository has no commits / is empty.
            return Ok(None);
        };
        revision = nearest_commit.oid.clone();
    }

    let defaults = code_insights_query_defaults(bctx.series.repositories.is_empty());
    let mut query = single_repo_query(raw_query, &bctx.repo_name, &revision, &defaults)
        .map_err(|err| format!("SingleRepoQuery: {err}"))?;
    if let Some(group_by) = &bctx.series.group_by {
        query = compute_insight_command_query_with_map_type(&query, group_by)
            .map_err(|err| format!("ComputeInsightCommandQuery: {err}"))?;
    }

    Ok(Some(SearchJob {
        series_id: bctx.series_id.clone(),
        search_query: query,
        record_time: Some(bctx.execution.recording_time),
        persist_mode: "record".to_owned(),
        dependent_frames: bctx.execution.shared_recordings.clone(),
    }))
}

/// The signature's date, or `None` for the zero `time.Time` (which no
/// recording time is ever before).
fn commit_time(value: Option<Timestamp>) -> Option<time::OffsetDateTime> {
    value.and_then(|value| {
        time::OffsetDateTime::from_unix_timestamp(value.seconds)
            .ok()
            .map(|time| {
                time.replace_nanosecond(value.nanos.max(0) as u32)
                    .unwrap_or(time)
            })
    })
}

/// `makeRunSearchFunc`.
pub fn make_run_search_func(
    search_handlers: HashMap<String, Arc<dyn InsightsHandler>>,
    search_worker_limit: Option<usize>,
    rate_limiter: Arc<dyn RateLimiter>,
) -> Arc<dyn SearchRunner> {
    Arc::new(RunSearch {
        search_handlers,
        search_worker_limit: search_worker_limit.unwrap_or(0),
        rate_limiter,
    })
}

struct RunSearch {
    search_handlers: HashMap<String, Arc<dyn InsightsHandler>>,
    search_worker_limit: usize,
    rate_limiter: Arc<dyn RateLimiter>,
}

#[async_trait]
impl SearchRunner for RunSearch {
    async fn run(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
        jobs: Vec<SearchJob>,
    ) -> Result<(RequestContext, Vec<RecordSeriesPoint>), String> {
        let series = req.backfill_request.series.clone();
        let Some(handler) = self.search_handlers.get(&series.generation_method) else {
            // The baseline indexes a nil map entry and panics on call; the
            // typed port reports the missing handler instead.
            return Err(format!(
                "unable to handle record for series_id: {} and generation_method: {}",
                series.series_id, series.generation_method
            ));
        };

        let search_worker_limit = self.search_worker_limit;
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let handler = Arc::clone(handler);
        let points = run_bounded(ctx, search_worker_limit, jobs, move |job, ctx| {
            let handler = Arc::clone(&handler);
            let series = series.clone();
            let rate_limiter = Arc::clone(&rate_limiter);
            async move {
                run_one_search(handler.as_ref(), rate_limiter.as_ref(), &job, &series, &ctx).await
            }
        })
        .await?;
        Ok((req, points.into_iter().flatten().collect()))
    }
}

async fn run_one_search(
    handler: &dyn InsightsHandler,
    rate_limiter: &dyn RateLimiter,
    job: &SearchJob,
    series: &Series,
    ctx: &CancellationToken,
) -> Result<Vec<RecordSeriesPoint>, String> {
    rate_limiter
        .wait(ctx)
        .await
        .map_err(|err| format!("rateLimiter.Wait: {err}"))?;
    let record_time = job
        .record_time
        .ok_or_else(|| "search job is missing its record time".to_owned())?;
    handler.handle(job, series, record_time).await
}

/// `makeSaveResultsFunc`.
pub fn make_save_results_func(insight_store: Arc<dyn ResultsStore>) -> Arc<dyn ResultsPersister> {
    Arc::new(SaveResults { insight_store })
}

struct SaveResults {
    insight_store: Arc<dyn ResultsStore>,
}

#[async_trait]
impl ResultsPersister for SaveResults {
    async fn persist(
        &self,
        ctx: &CancellationToken,
        req: RequestContext,
        points: Vec<RecordSeriesPoint>,
    ) -> Result<RequestContext, String> {
        if ctx.is_cancelled() {
            return Err("context canceled".to_owned());
        }
        self.insight_store.record_series_points(&points).await?;
        Ok(req)
    }
}

/// A bounded worker pool with cancel-on-first-error, the shape of
/// `conc/pool`'s `WithMaxGoroutines` + `WithCancelOnError`. `limit == 0`
/// means unbounded, like conc. The returned items are in completion order;
/// on any failure every item is discarded.
async fn run_bounded<T, R, F, Fut>(
    ctx: &CancellationToken,
    limit: usize,
    items: Vec<T>,
    f: F,
) -> Result<Vec<R>, String>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<R, String>> + Send + 'static,
{
    let semaphore = if limit > 0 {
        Some(Arc::new(Semaphore::new(limit)))
    } else {
        None
    };
    let f = Arc::new(f);
    let mut tasks = JoinSet::new();
    for item in items {
        // Like conc's pool: every queued task still runs, but observes the
        // canceled context through the per-job rate limiter.
        let permit = match &semaphore {
            Some(semaphore) => Some(
                semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| "worker pool closed".to_owned())?,
            ),
            None => None,
        };
        let f = Arc::clone(&f);
        let task_ctx = ctx.clone();
        tasks.spawn(async move {
            let _permit = permit;
            f(item, task_ctx).await
        });
    }

    let mut results = Vec::new();
    let mut first_error: Option<String> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(value)) => results.push(value),
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
                tasks.abort_all();
            }
            Err(err) => {
                if !err.is_cancelled() && first_error.is_none() {
                    first_error = Some(err.to_string());
                }
            }
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(results),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn series() -> Series {
        Series {
            id: 1,
            series_id: "abc".to_owned(),
            query: "test query".to_owned(),
            repositories: Vec::new(),
            sample_interval_unit: "WEEK".to_owned(),
            sample_interval_value: 1,
            group_by: None,
            repository_criteria: None,
            just_in_time: false,
            generation_method: "search".to_owned(),
        }
    }

    fn repo() -> MinimalRepo {
        MinimalRepo {
            id: 1,
            name: "testrepo".to_owned(),
        }
    }

    fn commit(oid: &str, author: Option<time::OffsetDateTime>) -> Commit {
        Commit {
            oid: oid.to_owned(),
            parents: Vec::new(),
            author_name: String::new(),
            author_email: String::new(),
            author_date: author.map(|value| Timestamp {
                seconds: value.unix_timestamp(),
                nanos: value.nanosecond() as i32,
            }),
            committer_date: None,
            message: String::new(),
        }
    }

    struct FakeCommitClient {
        first: Result<Commit, CommitError>,
        recents: Result<Vec<Commit>, CommitError>,
    }

    #[async_trait]
    impl CommitClient for FakeCommitClient {
        async fn first_commit(&self, _repo: &str) -> Result<Commit, CommitError> {
            self.first.clone()
        }

        async fn recent_commits(
            &self,
            _repo: &str,
            _target: time::OffsetDateTime,
            _revision: &str,
        ) -> Result<Vec<Commit>, CommitError> {
            self.recents.clone()
        }
    }

    struct Unlimited;

    #[async_trait]
    impl RateLimiter for Unlimited {
        async fn wait(&self, ctx: &CancellationToken) -> Result<(), String> {
            if ctx.is_cancelled() {
                return Err("context canceled".to_owned());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn generate_makes_one_job_per_execution_with_the_nearest_commit() {
        let generator = make_search_jobs_func(
            Arc::new(FakeCommitClient {
                first: Ok(commit("1", None)),
                recents: Ok(vec![commit("1", None)]),
            }),
            Arc::new(sg_insights::compression::NoopFilter),
            1,
            Arc::new(Unlimited),
        );
        let request = BackfillRequest {
            series: series(),
            repo: repo(),
            sample_times: vec![
                datetime!(2022-04-01 1:00 UTC),
                datetime!(2022-03-25 1:00 UTC),
            ],
        };
        let (_, jobs) = generator
            .generate(
                &CancellationToken::new(),
                RequestContext {
                    backfill_request: request,
                },
            )
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs
            .iter()
            .all(|job| job.search_query.contains("repo:^testrepo$@1")));
        assert!(jobs.iter().all(|job| job.persist_mode == "record"));
    }

    #[tokio::test]
    async fn generate_skips_series_with_a_repo_filter() {
        let mut request_series = series();
        request_series.query = "test query repo:repoA".to_owned();
        let generator = make_search_jobs_func(
            Arc::new(FakeCommitClient {
                first: Ok(commit("1", None)),
                recents: Ok(vec![commit("1", None)]),
            }),
            Arc::new(sg_insights::compression::NoopFilter),
            1,
            Arc::new(Unlimited),
        );
        let (_, jobs) = generator
            .generate(
                &CancellationToken::new(),
                RequestContext {
                    backfill_request: BackfillRequest {
                        series: request_series,
                        repo: repo(),
                        sample_times: vec![datetime!(2022-04-01 1:00 UTC)],
                    },
                },
            )
            .await
            .unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn generate_treats_an_empty_repository_as_no_work() {
        let generator = make_search_jobs_func(
            Arc::new(FakeCommitClient {
                first: Err(CommitError::RevisionNotFound {
                    repo: "testrepo".to_owned(),
                    spec: "HEAD".to_owned(),
                }),
                recents: Ok(Vec::new()),
            }),
            Arc::new(sg_insights::compression::NoopFilter),
            1,
            Arc::new(Unlimited),
        );
        let (_, jobs) = generator
            .generate(
                &CancellationToken::new(),
                RequestContext {
                    backfill_request: BackfillRequest {
                        series: series(),
                        repo: repo(),
                        sample_times: vec![datetime!(2022-04-01 1:00 UTC)],
                    },
                },
            )
            .await
            .unwrap();
        assert!(jobs.is_empty());
    }
}
