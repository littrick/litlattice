#![forbid(unsafe_code)]

//! M9.8g differential harness (Rust side): the `background-job-logger`
//! recorder against a real Redis server.
//!
//! Exercises the same sequence as `internal/goroutine/recorder/harness_test.go`
//! and prints the canonical lines both sides must agree on. Timestamps are
//! normalized by `test_ws/p9-go/run_recorder_diff.sh`; everything else (key
//! names, JSON shape, ordering, TTL presence) must match byte for byte.
//!
//! `SG_QTEST_REDIS` is the endpoint; the cache prefix `__diff_recorder` is
//! cleared before and after the run.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_rcache::Cache;
use sg_redis::KeyValue;
use sg_service_worker::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use sg_service_worker::framework::recorder::{Recordable, Recorder};
use tokio_util::sync::CancellationToken;

const DIFF_CACHE_PREFIX: &str = "__diff_recorder";
const DIFF_HOST_NAME: &str = "diff-host";

struct NoopHandler;

#[async_trait]
impl Handler for NoopHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test]
async fn recorder_matches_go_harness() {
    let Ok(endpoint) = std::env::var("SG_QTEST_REDIS") else {
        return;
    };

    let kv = KeyValue::connect(&endpoint).await.expect("connect redis");
    clear_diff_keys(&kv).await;
    let result = run(&kv).await;
    clear_diff_keys(&kv).await;
    result.expect("run differential");
}

async fn run(kv: &KeyValue) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::new_with_ttl(kv.clone(), DIFF_CACHE_PREFIX, 604800);
    let recorder = Recorder::new(cache.clone(), DIFF_HOST_NAME);
    let routine = Arc::new(
        PeriodicRoutine::new(Arc::new(NoopHandler))
            .with_name("diff.routine")
            .with_description("diff description")
            .with_interval(Duration::from_secs(90)),
    );
    routine.set_job_name("diff-job");
    routine.register_recorder(Arc::clone(&recorder));
    let recordable: Arc<dyn Recordable> = routine.clone();
    recorder.register(recordable);
    recorder.registration_done().await;
    recorder.log_start(routine.as_ref()).await;
    recorder
        .log_run(routine.as_ref(), Duration::from_millis(1500), Some("boom"))
        .await;
    recorder
        .log_run(routine.as_ref(), Duration::from_millis(500), None)
        .await;
    recorder.log_stop(routine.as_ref()).await;

    println!(
        "routine = {}",
        cache
            .get_hash_item("knownRoutines", "diff-job:diff.routine")
            .await?
            .expect("knownRoutines item")
    );
    println!(
        "knownJobNames = {}",
        cache
            .get_hash_item("knownJobNames", "diff-job")
            .await?
            .expect("knownJobNames item")
    );
    println!(
        "knownHostNames = {}",
        cache
            .get_hash_item("knownHostNames", DIFF_HOST_NAME)
            .await?
            .expect("knownHostNames item")
    );

    let last_start_key = format!("diff-job:diff.routine:{DIFF_HOST_NAME}:lastStart");
    let last_start = cache.get(&last_start_key).await?.expect("lastStart");
    println!("lastStart = {}", String::from_utf8_lossy(&last_start));
    let last_stop_key = format!("diff-job:diff.routine:{DIFF_HOST_NAME}:lastStop");
    let last_stop = cache.get(&last_stop_key).await?.expect("lastStop");
    println!("lastStop = {}", String::from_utf8_lossy(&last_stop));

    let runs = cache
        .fifo_list(
            &format!("diff-job:diff.routine:{DIFF_HOST_NAME}:recentRuns"),
            100,
        )
        .all()
        .await?;
    println!("recentRuns size = {}", runs.len());
    for (index, run) in runs.iter().enumerate() {
        println!("run[{index}] = {}", String::from_utf8_lossy(run));
    }

    let stats_key = format!("diff-job:diff.routine:runStats:{}", today());
    let stats = cache.get_string(&stats_key).await?.expect("runStats");
    println!("runStats = {stats}");

    println!(
        "ttl knownRoutines = {}",
        ttl_state(cache.key_ttl("knownRoutines").await?)
    );
    println!(
        "ttl lastStart = {}",
        ttl_state(cache.key_ttl(&last_start_key).await?)
    );
    println!(
        "ttl recentRuns = {}",
        ttl_state(
            cache
                .key_ttl(&format!(
                    "diff-job:diff.routine:{DIFF_HOST_NAME}:recentRuns"
                ))
                .await?
        )
    );

    Ok(())
}

fn today() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    )
}

fn ttl_state(ttl: i64) -> &'static str {
    if ttl > 0 {
        "positive"
    } else {
        "none"
    }
}

async fn clear_diff_keys(kv: &KeyValue) {
    for pattern in ["v2:__diff_recorder*", "v2:v2:__diff_recorder*"] {
        for key in kv.keys(pattern).await.expect("scan keys") {
            kv.del(&key).await.expect("delete key");
        }
    }
}
