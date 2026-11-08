#![forbid(unsafe_code)]

//! M9.8h differential harness (Rust side): the `executors-janitor` job.
//!
//! `executors_janitor_config` prints the four resolved durations and the
//! validation error for the environment of the current process; the script
//! runs it once per case (`defaults`, `custom`, `invalid`).
//!
//! `executors_janitor_db_and_cache` mirrors the Go harness: it inserts the
//! same cache fixture under `__diff_executors_dequeues`, runs the cleaner and
//! the heartbeat delete against `SG_QTEST_DSN`, and prints the surviving cache
//! values, the scrape-time cache sizes and the surviving heartbeat hostnames.
//!
//! The Go counterparts are in
//! `cmd/worker/internal/executors/harness_test.go`; both are gated by
//! `SG_HARNESS_ON=1` so a plain `cargo test` skips them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sg_core::EnvReader;
use sg_rcache::Cache;
use sg_redis::KeyValue;
use sg_service_worker::jobs::executors::{
    register_cache_size_metrics, timeutil_now, ExecutorsJanitor, MultiqueueCacheCleaner,
    CLEANUP_INTERVAL, DEQUEUE_CACHE_PREFIX, DEQUEUE_TTL, VALID_QUEUE_NAMES,
};
use sg_service_worker::jobs::Job;

const HEARTBEAT_MAX_AGE: Duration = Duration::from_secs(168 * 3600);

fn harness_enabled() -> bool {
    std::env::var("SG_HARNESS_ON").as_deref() == Ok("1")
}

#[test]
fn executors_janitor_config() {
    if !harness_enabled() {
        return;
    }

    let job = ExecutorsJanitor::new();
    let configs = job.configs();
    let mut env = EnvReader::new();
    for config in &configs {
        config.load(&mut env);
    }
    let values = job.config().snapshot();

    println!(
        "cleanup_task_interval_secs={}",
        values.cleanup_task_interval.as_secs()
    );
    println!(
        "heartbeat_records_max_age_secs={}",
        values.heartbeat_records_max_age.as_secs()
    );
    println!(
        "cache_cleanup_interval_secs={}",
        values.cache_cleanup_interval.as_secs()
    );
    println!(
        "cache_dequeue_ttl_secs={}",
        values.cache_dequeue_ttl.as_secs()
    );
    match configs.first().map(|config| config.validate()) {
        Some(Err(err)) => println!("validation_error={err}"),
        _ => println!("validation_error="),
    }
}

async fn reset_cache(cache: &Cache) {
    cache.delete("batches").await.expect("reset batches");
    cache.delete("codeintel").await.expect("reset codeintel");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executors_janitor_db_and_cache() {
    if !harness_enabled() {
        return;
    }
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set");
        return;
    };
    let redis = std::env::var("SG_QTEST_REDIS").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    std::env::set_var("REDIS_ENDPOINT", &redis);

    println!("dequeue_cache_prefix={DEQUEUE_CACHE_PREFIX}");
    println!("dequeue_ttl_secs={}", DEQUEUE_TTL.as_secs());
    println!("cleanup_interval_secs={}", CLEANUP_INTERVAL.as_secs());
    println!("valid_queues={}", VALID_QUEUE_NAMES.join(","));

    let kv = KeyValue::connect(&redis).await.expect("connect redis");
    let cache = Cache::new(kv, "__diff_executors_dequeues");
    reset_cache(&cache).await;

    let now = SystemTime::now();
    let nanos = |time: SystemTime| -> String {
        let value = match time.duration_since(UNIX_EPOCH) {
            Ok(after) => after.as_nanos() as i128,
            Err(before) => -(before.duration().as_nanos() as i128),
        };
        value.to_string()
    };
    let fresh = nanos(now - Duration::from_secs(60));
    let future = nanos(now + Duration::from_secs(3600));
    for (queue, field, value) in [
        ("batches", "1700000000000000000".to_owned(), "old-batches"),
        ("batches", fresh.clone(), "fresh-batches"),
        (
            "codeintel",
            "1700000000000000000".to_owned(),
            "old-codeintel",
        ),
        ("codeintel", fresh, "fresh-codeintel-1"),
        ("codeintel", future, "fresh-codeintel-2"),
    ] {
        cache
            .set_hash_item(queue, &field, value)
            .await
            .expect("insert cache fixture");
    }

    // `NewMultiqueueCacheCleaner` registers the scrape-time gauges against the
    // cache it is given; mirror that with the diff-only cache.
    register_cache_size_metrics(&cache);

    let pool = sg_db::connect(&dsn, 2).await.expect("connect database");

    let cleaner = MultiqueueCacheCleaner::new(
        VALID_QUEUE_NAMES
            .iter()
            .map(|queue| (*queue).to_owned())
            .collect(),
        cache.clone(),
        DEQUEUE_TTL,
    );
    cleaner.clean().await.expect("clean");

    for queue in VALID_QUEUE_NAMES {
        let all = cache.get_hash_all(queue).await.expect("get hash all");
        let mut values: Vec<&str> = all.values().map(String::as_str).collect();
        values.sort_unstable();
        println!("remaining {queue}=[{}]", values.join(","));
    }

    let mut metric_lines = Vec::new();
    for line in sg_metrics::render_default().lines() {
        let Some(rest) = line.strip_prefix("multiqueue_executor_dequeue_cache_size{") else {
            continue;
        };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        let Some(queue) = labels.strip_prefix("queue=\"") else {
            continue;
        };
        let Some(queue) = queue.strip_suffix('"') else {
            continue;
        };
        metric_lines.push(format!("metric queue={queue} value={value}"));
    }
    metric_lines.sort();
    for line in metric_lines {
        println!("{line}");
    }

    sg_db::executor_heartbeats::delete_inactive_heartbeats(
        &pool,
        timeutil_now(),
        HEARTBEAT_MAX_AGE,
    )
    .await
    .expect("delete inactive heartbeats");

    let hostnames: Vec<String> = sqlx::query_scalar(
        "SELECT hostname FROM executor_heartbeats WHERE hostname LIKE 'difftest-%' ORDER BY hostname",
    )
    .fetch_all(&pool)
    .await
    .expect("select remaining heartbeats");
    println!("heartbeat_remaining=[{}]", hostnames.join(","));

    reset_cache(&cache).await;
}
