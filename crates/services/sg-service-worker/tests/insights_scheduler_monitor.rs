#![forbid(unsafe_code)]

//! Differential harness for the scheduler monitor composition (M9.8ck).
//!
//! Mirror of the Go harness
//! `internal/insights/scheduler/parity_monitor_harness_test.go`: both sides
//! build the two backfill workers with the real constructors, print their
//! metadata and drain the seeded queues through the stores the constructors
//! return.
//!
//! The handlers never run, so the repository selector/store and the backfiller
//! are inert doubles. Go's resetter exposes no Description/Interval, so both
//! harnesses print only the resetter name (and the Go harness normalises
//! `dbworker.Resetter[<name>]`).
//!
//! Gated by `SG_QTEST_INSIGHTS_DSN` (a scratch codeinsights database seeded by
//! `insights_scheduler_monitor_fixtures.sql`).

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::time::OffsetDateTime;

use sg_insights::discovery::{RepoIter, SeriesRepoSelector, SeriesScope};
use sg_service_worker::jobs::insights_backfill_inprogress::{
    BackfillError, BackfillRequest, Backfiller, HandlerConfig, InsightRepo, InsightRepoStore,
    RepoGetError, SystemClock,
};
use sg_service_worker::jobs::insights_scheduler::{new_background_job_monitor, MonitorConfig};

struct InertSeriesSelector;

#[async_trait]
impl SeriesRepoSelector for InertSeriesSelector {
    async fn for_series(&self, _series: &SeriesScope) -> Result<Box<dyn RepoIter>, String> {
        Err("inert series selector".to_owned())
    }
}

struct InertRepoStore;

#[async_trait]
impl InsightRepoStore for InertRepoStore {
    async fn get(&self, _id: i32) -> Result<InsightRepo, RepoGetError> {
        Err(RepoGetError::NotFound)
    }
}

struct InertBackfiller;

#[async_trait]
impl Backfiller for InertBackfiller {
    async fn run(&self, _request: BackfillRequest) -> Result<(), BackfillError> {
        Ok(())
    }
}

#[tokio::test]
async fn insights_scheduler_monitor_differential() {
    let dsn = match std::env::var("SG_QTEST_INSIGHTS_DSN") {
        Ok(dsn) if !dsn.is_empty() => dsn,
        _ => return,
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect codeinsights");

    let monitor = new_background_job_monitor(MonitorConfig {
        insights_pool: pool,
        series_repo_iterator: Arc::new(InertSeriesSelector),
        insight_repo_store: Arc::new(InertRepoStore),
        backfiller: Arc::new(InertBackfiller),
        clock: Arc::new(SystemClock),
        handler_config: HandlerConfig::default(),
    });

    let routines = monitor.routines();
    assert_eq!(routines.len(), 4);
    let [in_progress_worker, in_progress_resetter, new_worker, new_resetter] =
        [&routines[0], &routines[1], &routines[2], &routines[3]];

    println!(
        "mon in_progress worker name={:?} description={:?} interval={}",
        in_progress_worker.name(),
        in_progress_worker.description(),
        in_progress_worker.interval().as_secs()
    );
    println!(
        "mon in_progress resetter name={:?}",
        in_progress_resetter.name()
    );
    println!(
        "mon new worker name={:?} description={:?} interval={}",
        new_worker.name(),
        new_worker.description(),
        new_worker.interval().as_secs()
    );
    println!("mon new resetter name={:?}", new_resetter.name());

    for (side, store) in [
        ("in_progress", &monitor.in_progress_store),
        ("new", &monitor.new_backfill_store),
    ] {
        loop {
            let record = store
                .dequeue("harness-host", OffsetDateTime::now_utc())
                .await
                .expect("dequeue");
            let Some(record) = record else {
                println!("mon dequeue store={side} found=0");
                break;
            };
            println!(
                "mon dequeue store={side} found=1 id={} backfill={}",
                record.id,
                record.backfill_id.unwrap_or(0)
            );
        }
    }
}
