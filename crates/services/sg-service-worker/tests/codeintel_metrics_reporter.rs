#![forbid(unsafe_code)]

//! M9.8s differential harness (Rust side): the `codeintel-metrics-reporter`
//! job.
//!
//! Mirrors `cmd/worker/internal/codeintel/metrics_reporter_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_metrics_reporter_diff.sh` applies the shared
//! queue fixture to a scratch frontend database immediately before each side
//! and diffs the canonical lines: job metadata, the embedded
//! `executorqueue.Config`, the `executors.autoscaler-metrics` routine
//! metadata, and `CountByState` over the auto-indexing view.

use std::sync::Arc;

use sg_config::{LiveConfig, Unified};
use sg_core::EnvReader;
use sg_service_worker::framework::BackgroundRoutine;
use sg_service_worker::jobs::codeintel_metrics_reporter::{
    CodeIntelMetricsReporter, JOB_DESCRIPTION, QUEUE_NAME, ROUTINE_DESCRIPTION, ROUTINE_INTERVAL,
    ROUTINE_NAME,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use sg_db::worker_queue::{
    count_by_state, LSIF_INDEXES_VIEW, QUEUED_ERRORED_PROCESSING, STATE_ERRORED, STATE_PROCESSING,
    STATE_QUEUED,
};

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_metrics_reporter_matches_go_harness() {
    if std::env::var("SG_METRICS_HARNESS_ON").ok().as_deref() != Some("1") {
        eprintln!("SG_METRICS_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_METRICS_DSN").expect("SG_METRICS_DSN is not set");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    // 1./2. Job metadata and embedded config.
    let job = CodeIntelMetricsReporter::new();
    println!("jmeta description={:?}", job.description());
    assert_eq!(job.description(), JOB_DESCRIPTION);

    let configs = job.configs();
    println!("jconfig count={}", configs.len());
    for config in &configs {
        config.load(&mut EnvReader::new());
    }
    let values = job.config().snapshot();
    println!("jconfig environment={:?}", values.environment_label);
    let allocation = values
        .allocations
        .get(QUEUE_NAME)
        .copied()
        .unwrap_or_default();
    println!(
        "jconfig allocation {QUEUE_NAME} aws={:.2} gcp={:.2}",
        allocation.percentage_aws, allocation.percentage_gcp
    );
    match job.config().validate() {
        Ok(()) => println!("jconfig validate=ok"),
        Err(err) => println!("jconfig validate={err}"),
    }

    // 3. Routine metadata.
    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    let routine: &BackgroundRoutine = &routines[0];
    println!("jroutine name={}", routine.name());
    println!("jroutine description={}", routine.description());
    println!("jroutine interval_ms={}", routine.interval().as_millis());
    let shared = Arc::new(routine.clone());
    println!(
        "jroutine type={}",
        shared
            .recordable()
            .expect("periodic routine")
            .routine_type()
            .as_str()
    );
    assert_eq!(routine.name(), ROUTINE_NAME);
    assert_eq!(routine.description(), ROUTINE_DESCRIPTION);
    assert_eq!(routine.interval(), ROUTINE_INTERVAL);

    // 4. CountByState over `lsif_indexes_with_repository_name u`.
    for (name, states) in [
        ("queued_errored_processing", QUEUED_ERRORED_PROCESSING),
        ("queued", STATE_QUEUED),
        ("errored_processing", STATE_ERRORED | STATE_PROCESSING),
    ] {
        match count_by_state(&pool, LSIF_INDEXES_VIEW, states).await {
            Ok(count) => println!("jcount states={name} count={count}"),
            Err(err) => println!("jcount error states={name} = {err}"),
        }
    }
    if let Err(err) = count_by_state(&pool, LSIF_INDEXES_VIEW, 0).await {
        println!("jcount error states=empty = {err}");
    }
}
