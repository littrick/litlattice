#![forbid(unsafe_code)]

// M9.8cj differential harness for `sg_service_worker::jobs::insights_pipeline`.
//
// Prints canonical `bp ` lines for the corpus in
// test_ws/p9-go/data/insights_pipeline_corpus.txt; the Go baseline prints the
// same lines from internal/insights/pipeline/parity_harness_test.go.
//
// Scenes:
//   orchestrate <numJobs>    -> the three connected steps with fake
//                               generator/runner/persister.
//   jobs <scene> <workers>   -> make_search_jobs_func (NoopFilter, unlimited
//                               limiter).
//   points <scene> <workers> -> make_run_search_func (constant / failing
//                               handler).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_git::Commit;
use sg_insights::compression::NoopFilter;
use sg_insights::gitserver::CommitError;
use sg_insights::timeseries::{build_sample_times, TimeInterval};
use sg_insights::types::IntervalUnit;
use sg_service_worker::jobs::insights_pipeline::{
    make_run_search_func, make_search_jobs_func, new_backfiller, BackfillRequest, CommitClient,
    InsightsHandler, MinimalRepo, RateLimiter, RequestContext, ResultsPersister, SearchJob,
    SearchJobGenerator, SearchRunner,
};
use sg_service_worker::jobs::insights_query_runner::insights_store::{RecordSeriesPoint, Series};
use time::format_description::well_known::Rfc3339;
use time::macros::datetime;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const CREATED_DATE: OffsetDateTime = datetime!(2022-04-01 01:00 UTC);

fn series(query: &str) -> Series {
    Series {
        id: 1,
        series_id: "abc".to_owned(),
        query: query.to_owned(),
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

fn sample_times() -> Vec<OffsetDateTime> {
    build_sample_times(
        12,
        &TimeInterval {
            unit: IntervalUnit::Week,
            value: 1,
        },
        CREATED_DATE,
    )
}

fn request(query: &str) -> BackfillRequest {
    BackfillRequest {
        series: series(query),
        repo: repo(),
        sample_times: sample_times(),
    }
}

fn commit(oid: &str, author: Option<OffsetDateTime>) -> Commit {
    Commit {
        oid: oid.to_owned(),
        parents: Vec::new(),
        author_name: String::new(),
        author_email: String::new(),
        author_date: author.map(|value| prost_types::Timestamp {
            seconds: value.unix_timestamp(),
            nanos: value.nanosecond() as i32,
        }),
        committer_date: None,
        message: String::new(),
    }
}

fn time_string(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).expect("format time")
}

// --- orchestrate: TestBackfillStepsConnected ---

struct ParityJobGenerator {
    num_jobs: usize,
}

#[async_trait]
impl SearchJobGenerator for ParityJobGenerator {
    async fn generate(
        &self,
        _ctx: &CancellationToken,
        req: RequestContext,
    ) -> Result<(RequestContext, Vec<SearchJob>), String> {
        let record_date = datetime!(2022-04-01 00:00 UTC);
        let jobs = (0..self.num_jobs)
            .map(|_| SearchJob {
                series_id: req.backfill_request.series.series_id.clone(),
                search_query: "test search".to_owned(),
                record_time: Some(record_date),
                persist_mode: String::new(),
                dependent_frames: Vec::new(),
            })
            .collect();
        Ok((req, jobs))
    }
}

struct ParitySearchRunner;

#[async_trait]
impl SearchRunner for ParitySearchRunner {
    async fn run(
        &self,
        _ctx: &CancellationToken,
        req: RequestContext,
        jobs: Vec<SearchJob>,
    ) -> Result<(RequestContext, Vec<RecordSeriesPoint>), String> {
        let points = jobs
            .iter()
            .map(|job| RecordSeriesPoint {
                series_id: String::new(),
                time: job.record_time.expect("record time"),
                value: 10.0,
                capture: None,
                repo_id: None,
                repo_name: None,
                persist_mode: String::new(),
            })
            .collect();
        Ok((req, points))
    }
}

struct ParityPersister {
    result_count: AtomicUsize,
    total_count: AtomicUsize,
}

#[async_trait]
impl ResultsPersister for ParityPersister {
    async fn persist(
        &self,
        _ctx: &CancellationToken,
        req: RequestContext,
        points: Vec<RecordSeriesPoint>,
    ) -> Result<RequestContext, String> {
        for point in &points {
            self.result_count.fetch_add(1, Ordering::SeqCst);
            self.total_count
                .fetch_add(point.value as usize, Ordering::SeqCst);
        }
        Ok(req)
    }
}

async fn orchestrate(label: &str, num_jobs: usize) {
    let persister = Arc::new(ParityPersister {
        result_count: AtomicUsize::new(0),
        total_count: AtomicUsize::new(0),
    });
    let backfiller = new_backfiller(
        Arc::new(ParityJobGenerator { num_jobs }),
        Arc::new(ParitySearchRunner),
        persister.clone(),
    );
    let err = backfiller
        .run(
            &CancellationToken::new(),
            BackfillRequest {
                series: Series {
                    id: 0,
                    series_id: "1".to_owned(),
                    query: String::new(),
                    repositories: Vec::new(),
                    sample_interval_unit: String::new(),
                    sample_interval_value: 0,
                    group_by: None,
                    repository_criteria: None,
                    just_in_time: false,
                    generation_method: String::new(),
                },
                repo: MinimalRepo {
                    id: 0,
                    name: String::new(),
                },
                sample_times: Vec::new(),
            },
        )
        .await;
    println!(
        "bp orchestrate {label} err={} results={} total={}",
        err.is_err(),
        persister.result_count.load(Ordering::SeqCst),
        persister.total_count.load(Ordering::SeqCst)
    );
}

// --- jobs: TestMakeSearchJobs ---

#[derive(Clone, Copy)]
enum Recents {
    Ok,
    ErrorAfter(usize),
}

struct ParityCommitClient {
    first: Result<Commit, CommitError>,
    recents: Recents,
    recent_commits: Vec<Commit>,
    recent_calls: AtomicUsize,
}

#[async_trait]
impl CommitClient for ParityCommitClient {
    async fn first_commit(&self, _repo: &str) -> Result<Commit, CommitError> {
        self.first.clone()
    }

    async fn recent_commits(
        &self,
        _repo: &str,
        _target: OffsetDateTime,
        _revision: &str,
    ) -> Result<Vec<Commit>, CommitError> {
        match self.recents {
            Recents::Ok => Ok(self.recent_commits.clone()),
            Recents::ErrorAfter(times) => {
                let called = self.recent_calls.fetch_add(1, Ordering::SeqCst);
                if called >= times {
                    Err(CommitError::Other("error hit".to_owned()))
                } else {
                    Ok(self.recent_commits.clone())
                }
            }
        }
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

async fn jobs_scene(label: &str, scene: &str, workers: usize) {
    let recent_commits = vec![commit("1", None), commit("2", None)];
    let base = ParityCommitClient {
        first: Ok(commit("1", None)),
        recents: Recents::Ok,
        recent_commits: recent_commits.clone(),
        recent_calls: AtomicUsize::new(0),
    };
    let mut req = request("test query");
    let mut canceled = false;
    let client = match scene {
        "base" => base,
        "first-commit" => ParityCommitClient {
            first: Ok(commit("1", Some(CREATED_DATE - time::Duration::weeks(3)))),
            ..base
        },
        "canceled" => {
            canceled = true;
            base
        }
        "first-commit-error" => ParityCommitClient {
            first: Err(CommitError::Other("somethings wrong".to_owned())),
            ..base
        },
        "empty-repo" => ParityCommitClient {
            // The Go scene uses gitserver.EmptyRepoErr, which the real
            // gitserver client never returns (see M9.8ci notes); the Rust
            // scene uses the RevisionNotFoundError the port treats as an
            // empty repository. Documented erratum.
            first: Err(CommitError::RevisionNotFound {
                repo: "testrepo".to_owned(),
                spec: "HEAD".to_owned(),
            }),
            ..base
        },
        "recent-error" => ParityCommitClient {
            recents: Recents::ErrorAfter(6),
            ..base
        },
        "invalid-query" => {
            // Same scene as the Go harness: unrecognized field (the
            // baseline unit test's "i++" relies on regexp-syntax validation,
            // which sg-search-query has not ported).
            req = request("g:foo a");
            base
        }
        "repo-filter" => {
            req = request("test query repo:repoA");
            base
        }
        other => panic!("unknown jobs scene {other}"),
    };

    let token = CancellationToken::new();
    if canceled {
        token.cancel();
    }
    let generator = make_search_jobs_func(
        Arc::new(client),
        Arc::new(NoopFilter),
        workers,
        Arc::new(Unlimited),
    );
    let result = generator
        .generate(
            &token,
            RequestContext {
                backfill_request: req,
            },
        )
        .await;
    match result {
        Ok((_, mut jobs)) => {
            jobs.sort_by_key(|job| std::cmp::Reverse(job.record_time));
            for job in &jobs {
                println!(
                    "bp job {label} recordtime={} query={}",
                    time_string(job.record_time.expect("record time")),
                    job.search_query
                );
            }
            println!("bp jobs {label} error=false");
        }
        Err(_) => println!("bp jobs {label} error=true"),
    }
}

// --- points: TestMakeRunSearch ---

struct ConstHandler;

#[async_trait]
impl InsightsHandler for ConstHandler {
    async fn handle(
        &self,
        job: &SearchJob,
        _series: &Series,
        _record_time: OffsetDateTime,
    ) -> Result<Vec<RecordSeriesPoint>, String> {
        Ok(vec![RecordSeriesPoint {
            series_id: String::new(),
            time: job.record_time.expect("record time"),
            value: 10.0,
            capture: None,
            repo_id: None,
            repo_name: None,
            persist_mode: String::new(),
        }])
    }
}

struct ErrorAfterHandler {
    called: Mutex<usize>,
}

#[async_trait]
impl InsightsHandler for ErrorAfterHandler {
    async fn handle(
        &self,
        job: &SearchJob,
        _series: &Series,
        _record_time: OffsetDateTime,
    ) -> Result<Vec<RecordSeriesPoint>, String> {
        let mut called = self.called.lock().unwrap();
        if *called >= 2 {
            return Err("search error".to_owned());
        }
        *called += 1;
        Ok(vec![RecordSeriesPoint {
            series_id: String::new(),
            time: job.record_time.expect("record time"),
            value: 10.0,
            capture: None,
            repo_id: None,
            repo_name: None,
            persist_mode: String::new(),
        }])
    }
}

async fn points_scene(label: &str, scene: &str, workers: usize) {
    let series = Series {
        generation_method: "search".to_owned(),
        ..series("test query")
    };
    let req = BackfillRequest {
        series: series.clone(),
        repo: repo(),
        sample_times: Vec::new(),
    };
    let record_times = [
        datetime!(2022-04-21 00:00 UTC),
        datetime!(2022-04-14 00:00 UTC),
        datetime!(2022-04-07 00:00 UTC),
        datetime!(2022-04-01 00:00 UTC),
    ];
    let jobs: Vec<SearchJob> = record_times
        .iter()
        .map(|time| SearchJob {
            series_id: String::new(),
            search_query: String::new(),
            record_time: Some(*time),
            persist_mode: String::new(),
            dependent_frames: Vec::new(),
        })
        .collect();

    let mut handlers: std::collections::HashMap<String, Arc<dyn InsightsHandler>> =
        std::collections::HashMap::new();
    let mut canceled = false;
    match scene {
        "base" => {
            handlers.insert("search".to_owned(), Arc::new(ConstHandler));
        }
        "canceled" => {
            canceled = true;
            handlers.insert("search".to_owned(), Arc::new(ConstHandler));
        }
        "fail" => {
            handlers.insert(
                "search".to_owned(),
                Arc::new(ErrorAfterHandler {
                    called: Mutex::new(0),
                }),
            );
        }
        other => panic!("unknown points scene {other}"),
    }

    let token = CancellationToken::new();
    if canceled {
        token.cancel();
    }
    let runner = make_run_search_func(handlers, Some(workers), Arc::new(Unlimited));
    let result = runner
        .run(
            &token,
            RequestContext {
                backfill_request: req,
            },
            jobs,
        )
        .await;
    match result {
        Ok((_, mut points)) => {
            points.sort_by_key(|point| std::cmp::Reverse(point.time));
            for point in &points {
                println!(
                    "bp point {label} pointtime={} value={}",
                    time_string(point.time),
                    point.value as i64
                );
            }
            println!("bp points {label} error=false");
        }
        Err(_) => println!("bp points {label} error=true"),
    }
}

#[tokio::test]
async fn insights_pipeline_parity() {
    if std::env::var("SG_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("set SG_HARNESS_ON=1 to run the insights pipeline parity harness");
        return;
    }
    let corpus = std::env::var("SG_INSIGHTS_PIPELINE_CORPUS")
        .expect("SG_INSIGHTS_PIPELINE_CORPUS is required");
    let contents = std::fs::read_to_string(&corpus)
        .unwrap_or_else(|err| panic!("read corpus {corpus}: {err}"));
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let label = fields.next().expect("label");
        let kind = fields.next().expect("kind");
        match kind {
            "orchestrate" => {
                let num_jobs = fields.next().unwrap_or("0").parse().expect("numJobs");
                orchestrate(label, num_jobs).await;
            }
            "jobs" => {
                let scene = fields.next().expect("scene");
                let workers = fields.next().unwrap_or("1").parse().expect("workers");
                jobs_scene(label, scene, workers).await;
            }
            "points" => {
                let scene = fields.next().expect("scene");
                let workers = fields.next().unwrap_or("1").parse().expect("workers");
                points_scene(label, scene, workers).await;
            }
            other => panic!("unknown scene kind {other}"),
        }
    }
}
