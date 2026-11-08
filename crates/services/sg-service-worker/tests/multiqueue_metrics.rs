#![forbid(unsafe_code)]

//! M9.8k differential harness (Rust side): the
//! `executors-multiqueue-metrics-reporter` job.
//!
//! Mirrors `cmd/worker/internal/executorqueue/multiqueue_harness_test.go`:
//!
//! 1. allocation normalization for the same case table;
//! 2. the [`ExternalEmitter`] with fixed count sources and recording
//!    reporters (allocation multiplication and error wrapping);
//! 3. the routine metadata the job hands to the framework;
//! 4. `sg_db::worker_queue::count_by_state` against the fixture applied by
//!    `test_ws/p9-go/run_multiqueue_metrics_diff.sh`.
//!
//! `SG_QTEST_DSN` points at the scratch frontend database. The output line
//! order is the Go harness's.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::{LiveConfig, Unified};
use sg_core::EnvReader;
use sg_service_worker::framework::BackgroundRoutine;
use sg_service_worker::jobs::executors_multiqueue_metrics::{
    normalize_allocations, CountSource, ExecutorsMultiqueueMetricsReporter, ExternalEmitter,
    QueueAllocation, Reporter,
};
use sg_service_worker::jobs::{Job as _, JobConfig as _, JobDeps};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

type AllocationCase = (
    &'static str,
    HashMap<String, HashMap<String, f64>>,
    bool,
    bool,
);

#[tokio::test(flavor = "multi_thread")]
async fn multiqueue_metrics_matches_go_harness() {
    // 1. Allocation normalization.
    let cases: Vec<AllocationCase> = vec![
        ("empty_none", HashMap::new(), false, false),
        ("empty_aws", HashMap::new(), true, false),
        ("empty_gcp", HashMap::new(), false, true),
        ("empty_both", HashMap::new(), true, true),
        (
            "half",
            map(&[("batches", &[("aws", 0.5), ("gcp", 0.5)])]),
            true,
            true,
        ),
        (
            "quarter_aws",
            map(&[("batches", &[("aws", 0.25)])]),
            true,
            true,
        ),
        ("gcp_only", map(&[("batches", &[("gcp", 1.0)])]), true, true),
        (
            "aws_unconfigured",
            map(&[("batches", &[("aws", 1.0)])]),
            false,
            true,
        ),
        ("badqueue", map(&[("badqueue", &[])]), false, false),
        (
            "badprovider",
            map(&[("batches", &[("azure", 0.5)])]),
            false,
            false,
        ),
        ("range", map(&[("batches", &[("aws", 1.5)])]), false, false),
        (
            "negative",
            map(&[("batches", &[("gcp", -0.1)])]),
            false,
            false,
        ),
    ];

    for (name, m, aws, gcp) in &cases {
        match normalize_allocations(m, *aws, *gcp) {
            Err(err) => println!("alloc_err {name} = {err}"),
            Ok(allocations) => {
                let batches = allocations.get("batches").copied().unwrap_or_default();
                println!(
                    "alloc_ok {name} aws={:.2} gcp={:.2}",
                    batches.percentage_aws, batches.percentage_gcp
                );
            }
        }
    }

    // 2. Emitter with fake sources and reporters.
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let emitter = ExternalEmitter::new(
        "batches_codeintel".to_owned(),
        vec![
            Arc::new(FixedCount(4)) as Arc<dyn CountSource>,
            Arc::new(FixedCount(5)) as Arc<dyn CountSource>,
        ],
        vec![
            Arc::new(RecordingReporter::new("aws", Arc::clone(&lines))) as Arc<dyn Reporter>,
            Arc::new(RecordingReporter::new("gcp", Arc::clone(&lines))) as Arc<dyn Reporter>,
        ],
        QueueAllocation {
            percentage_aws: 0.5,
            percentage_gcp: 1.0,
        },
    );
    emitter
        .emit(&CancellationToken::new())
        .await
        .expect("emitter");
    let mut collected = lines.lock().unwrap().clone();
    collected.sort();
    for line in collected {
        println!("{line}");
    }

    let failing = ExternalEmitter::new(
        "batches_codeintel".to_owned(),
        vec![Arc::new(FailingCount) as Arc<dyn CountSource>],
        Vec::new(),
        QueueAllocation::default(),
    );
    let err = failing
        .emit(&CancellationToken::new())
        .await
        .expect_err("count error");
    println!("emit_error = {err}");

    // 3. Routine metadata.
    let dsn = std::env::var("SG_QTEST_DSN").ok();
    let pool = match &dsn {
        Some(dsn) => PgPool::connect(dsn).await.expect("connect"),
        None => sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
    };
    let deps = JobDeps {
        frontend: pool,
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };

    let job = ExecutorsMultiqueueMetricsReporter::new();
    job.config().load(&mut EnvReader::new());
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    let routine: &BackgroundRoutine = &routines[0];
    println!("routine name = {}", routine.name());
    println!("routine description = {}", routine.description());
    println!("routine interval_ms = {}", routine.interval().as_millis());
    let shared = Arc::new(routine.clone());
    println!(
        "routine type = {}",
        shared
            .recordable()
            .expect("periodic routine")
            .routine_type()
            .as_str()
    );

    // 4. CountByState for the real views.
    let Some(dsn) = dsn else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");

    for (name, states) in [
        ("7", 1u32 | 2 | 4),
        ("queued", 1),
        ("errored_processing", 2 | 4),
    ] {
        match sg_db::worker_queue::count_by_state(
            &pool,
            sg_db::worker_queue::LSIF_INDEXES_VIEW,
            states,
        )
        .await
        {
            Err(err) => println!("count_error codeintel states={name} = {err}"),
            Ok(count) => println!("count codeintel states={name} = {count}"),
        }
        match sg_db::worker_queue::count_by_state(
            &pool,
            sg_db::worker_queue::BATCH_SPEC_WORKSPACE_EXECUTION_JOBS_VIEW,
            states,
        )
        .await
        {
            Err(err) => println!("count_error batches states={name} = {err}"),
            Ok(count) => println!("count batches states={name} = {count}"),
        }
    }

    match sg_db::worker_queue::count_by_state(&pool, sg_db::worker_queue::LSIF_INDEXES_VIEW, 0)
        .await
    {
        Err(err) => println!("count_error codeintel states=0 = {err}"),
        Ok(count) => println!("count codeintel states=0 = {count}"),
    }
}

fn map(entries: &[(&str, &[(&str, f64)])]) -> HashMap<String, HashMap<String, f64>> {
    entries
        .iter()
        .map(|(queue, allocation)| {
            (
                (*queue).to_owned(),
                allocation
                    .iter()
                    .map(|(provider, value)| ((*provider).to_owned(), *value))
                    .collect(),
            )
        })
        .collect()
}

struct FixedCount(i64);

#[async_trait]
impl CountSource for FixedCount {
    async fn count(&self) -> Result<i64, String> {
        Ok(self.0)
    }
}

struct FailingCount;

#[async_trait]
impl CountSource for FailingCount {
    async fn count(&self) -> Result<i64, String> {
        Err("boom".to_owned())
    }
}

struct RecordingReporter {
    name: &'static str,
    lines: Arc<Mutex<Vec<String>>>,
}

impl RecordingReporter {
    fn new(name: &'static str, lines: Arc<Mutex<Vec<String>>>) -> Self {
        Self { name, lines }
    }
}

#[async_trait]
impl Reporter for RecordingReporter {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get_allocation(&self, allocation: QueueAllocation) -> f64 {
        if self.name == "aws" {
            allocation.percentage_aws
        } else {
            allocation.percentage_gcp
        }
    }

    async fn report_count(&self, _ctx: &CancellationToken, queue_name: &str, count: i64) {
        self.lines.lock().unwrap().push(format!(
            "emit reporter={} queue={queue_name} count={count}",
            self.name
        ));
    }
}
